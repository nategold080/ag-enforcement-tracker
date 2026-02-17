"""CLI interface for the AG Enforcement Tracker.

Uses Click for command parsing and Rich for output formatting.
"""

import asyncio
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from sqlalchemy import select

from src.scrapers.registry import get_scraper, get_active_states, state_key_from_code, load_state_configs
from src.storage.database import Database
from src.storage.models import EnforcementAction, ScrapeRun

console = Console()


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging.")
@click.option("--db", "db_path", default="data/ag_enforcement.db", help="Database path.")
@click.pass_context
def cli(ctx, verbose, db_path):
    """AG Enforcement Tracker — Scrape, extract, and query state AG enforcement actions."""
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["db"] = Database(db_path)
    ctx.obj["db"].create_tables()


@cli.command()
@click.option("--state", "state_code", required=False, help="Two-letter state code (e.g., CA).")
@click.option("--all", "scrape_all", is_flag=True, help="Scrape all active states.")
@click.option("--since", "since_date", required=False, help="Only scrape press releases on or after this date (YYYY-MM-DD).")
@click.option("--max-pages", default=50, help="Maximum number of listing pages to scrape.")
@click.option("--save-raw", is_flag=True, help="Save raw HTML to data/raw/.")
@click.pass_context
def scrape(ctx, state_code, scrape_all, since_date, max_pages, save_raw):
    """Scrape press releases from state AG websites."""
    db: Database = ctx.obj["db"]

    since = None
    if since_date:
        since = date.fromisoformat(since_date)

    if not state_code and not scrape_all:
        console.print("[red]Error:[/red] Provide --state or --all.")
        sys.exit(1)

    if scrape_all:
        state_keys = get_active_states()
    else:
        state_key = state_key_from_code(state_code)
        if not state_key:
            console.print(f"[red]Error:[/red] Unknown state code: {state_code}")
            sys.exit(1)
        state_keys = [state_key]

    for state_key in state_keys:
        asyncio.run(_scrape_state(db, state_key, since, max_pages, save_raw))


async def _scrape_state(
    db: Database,
    state_key: str,
    since: date | None,
    max_pages: int,
    save_raw: bool,
) -> None:
    """Scrape a single state and store results."""
    # Import here to trigger registration
    import src.scrapers.states  # noqa: F401

    scraper = get_scraper(state_key)
    console.print(f"\n[bold blue]Scraping {scraper.state_name} ({scraper.state_code})[/bold blue]")

    # Create scrape run record
    run = ScrapeRun(state=scraper.state_code)
    with db.get_session() as session:
        session.add(run)
        session.commit()
        run_id = run.id

    try:
        # Phase 1: Scrape listing pages
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(f"Fetching listing pages for {scraper.state_code}...", total=None)
            items = await scraper.scrape_listing(since=since, max_pages=max_pages)
            progress.update(task, description=f"Found {len(items)} press releases")

        console.print(f"  Found [green]{len(items)}[/green] press release listing items.")

        # Filter out already-scraped URLs
        new_items = []
        for item in items:
            if not db.action_exists(item.url):
                new_items.append(item)
        skipped = len(items) - len(new_items)
        if skipped:
            console.print(f"  Skipping [yellow]{skipped}[/yellow] already-scraped URLs.")

        # Phase 2: Fetch detail pages
        press_releases = []
        errors = 0

        if new_items:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Fetching detail pages...", total=len(new_items),
                )
                for item in new_items:
                    try:
                        pr = await scraper.scrape_detail(item)
                        press_releases.append(pr)

                        # Save raw HTML if requested
                        if save_raw:
                            raw_dir = Path("data/raw") / scraper.state_code.lower()
                            raw_dir.mkdir(parents=True, exist_ok=True)
                            import re
                            from urllib.parse import urlparse
                            slug = re.sub(r"[^\w\-]", "_", urlparse(item.url).path.strip("/"))[:80]
                            (raw_dir / f"{slug}.html").write_text(pr.body_html, encoding="utf-8")

                    except Exception as e:
                        errors += 1
                        logging.getLogger(__name__).error(
                            "Failed to fetch %s: %s", item.url, e,
                        )
                    progress.advance(task)

        # Phase 3: Store in database (basic storage — full extraction in Phase 2)
        stored = 0
        for pr in press_releases:
            if db.action_exists(pr.url):
                continue
            with db.get_session() as session:
                action = EnforcementAction(
                    state=pr.state,
                    date_announced=pr.date or since or date.today(),
                    action_type="other",
                    status="announced",
                    headline=pr.title,
                    source_url=pr.url,
                    raw_text=pr.body_text,
                )
                session.add(action)
                session.commit()
                stored += 1

        # Update scrape run
        with db.get_session() as session:
            db_run = session.get(ScrapeRun, run_id)
            if db_run:
                db_run.completed_at = datetime.now(timezone.utc)
                db_run.press_releases_found = len(items)
                db_run.actions_extracted = stored
                db_run.errors = errors
                session.commit()

        console.print(f"  Stored [green]{stored}[/green] new records. [red]{errors}[/red] errors.")

    except Exception as e:
        console.print(f"  [red]Scrape failed:[/red] {e}")
        logging.getLogger(__name__).exception("Scrape failed for %s", state_key)
    finally:
        await scraper.close()


@cli.command()
@click.pass_context
def stats(ctx):
    """Show database statistics."""
    db: Database = ctx.obj["db"]
    s = db.stats()

    table = Table(title="AG Enforcement Tracker — Database Stats")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")

    table.add_row("Total enforcement actions", str(s["total_actions"]))
    table.add_row("Total defendants", str(s["total_defendants"]))
    table.add_row("States with data", str(s["states_with_data"]))
    table.add_row("Total scrape runs", str(s["total_scrape_runs"]))

    console.print(table)


@cli.command()
@click.option("--state", "state_code", required=False, help="Filter by state code.")
@click.option("--limit", default=20, help="Number of records to show.")
@click.pass_context
def list_actions(ctx, state_code, limit):
    """List enforcement actions in the database."""
    db: Database = ctx.obj["db"]

    with db.get_session() as session:
        stmt = select(EnforcementAction).order_by(EnforcementAction.date_announced.desc())
        if state_code:
            stmt = stmt.where(EnforcementAction.state == state_code.upper())
        stmt = stmt.limit(limit)
        actions = session.execute(stmt).scalars().all()

    if not actions:
        console.print("[yellow]No enforcement actions found.[/yellow]")
        return

    table = Table(title=f"Enforcement Actions (showing {len(actions)})")
    table.add_column("Date", style="cyan", width=12)
    table.add_column("State", style="green", width=5)
    table.add_column("Type", width=12)
    table.add_column("Headline", max_width=60)

    for a in actions:
        table.add_row(
            str(a.date_announced),
            a.state,
            a.action_type,
            a.headline[:60] + ("..." if len(a.headline) > 60 else ""),
        )

    console.print(table)


@cli.command()
@click.pass_context
def list_states(ctx):
    """Show configured states and their status."""
    configs = load_state_configs()
    db: Database = ctx.obj["db"]

    table = Table(title="Configured States")
    table.add_column("Key", style="cyan")
    table.add_column("Code", style="green")
    table.add_column("Name")
    table.add_column("Active", justify="center")
    table.add_column("Records", justify="right")

    for key, cfg in sorted(configs.items()):
        code = cfg.get("code", "??")
        active = "[green]Yes[/green]" if cfg.get("active") else "[dim]No[/dim]"
        count = db.get_action_count(code)
        table.add_row(key, code, cfg.get("name", ""), active, str(count))

    console.print(table)


@cli.command()
@click.option("--state", "state_code", required=False, help="Extract for a specific state code.")
@click.option("--all", "extract_all", is_flag=True, help="Extract for all states with scraped data.")
@click.option("--reprocess", is_flag=True, help="Re-extract even if already processed.")
@click.pass_context
def extract(ctx, state_code, extract_all, reprocess):
    """Run extraction pipeline on scraped press releases."""
    import yaml
    from pathlib import Path
    from sqlalchemy import delete, select

    from src.extractors.filter import is_enforcement_action
    from src.extractors.press_release import PressReleaseExtractor
    from src.storage.models import (
        ActionDefendant,
        Defendant,
        MonetaryTerms,
        StatuteCited,
        ViolationCategory,
    )
    from src.validation.schemas import PressRelease

    db: Database = ctx.obj["db"]

    # Load taxonomy
    taxonomy_path = Path("config/taxonomy.yaml")
    with open(taxonomy_path) as f:
        taxonomy = yaml.safe_load(f)

    extractor = PressReleaseExtractor(taxonomy)

    # Find actions to process (those with raw_text but action_type='other' and quality_score=0)
    with db.get_session() as session:
        stmt = select(EnforcementAction).where(EnforcementAction.raw_text != "")
        if state_code:
            stmt = stmt.where(EnforcementAction.state == state_code.upper())
        if not reprocess:
            stmt = stmt.where(EnforcementAction.quality_score == 0.0)
        actions = session.execute(stmt).scalars().all()

    if not actions:
        console.print("[yellow]No unprocessed actions found.[/yellow]")
        return

    console.print(f"Found [green]{len(actions)}[/green] actions to process.")

    extracted = 0
    filtered_out = 0
    errors = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Extracting...", total=len(actions))

        for action in actions:
            try:
                # Run non-enforcement filter
                filter_result = is_enforcement_action(action.headline, action.raw_text)

                if not filter_result.is_enforcement:
                    filtered_out += 1
                    # Mark as processed but low quality
                    with db.get_session() as session:
                        db_action = session.get(EnforcementAction, action.id)
                        if db_action:
                            db_action.quality_score = 0.1
                            db_action.action_type = "other"
                            session.commit()
                    progress.advance(task)
                    continue

                # Build a PressRelease for extraction
                pr = PressRelease(
                    title=action.headline,
                    url=action.source_url,
                    date=action.date_announced,
                    state=action.state,
                    body_text=action.raw_text,
                )

                result = extractor.extract(pr, date_announced=action.date_announced)

                # Update the database record with all extracted data
                with db.get_session() as session:
                    db_action = session.get(EnforcementAction, action.id)
                    if db_action:
                        db_action.action_type = result.action_type.value
                        db_action.status = result.status.value
                        db_action.quality_score = result.quality_score
                        db_action.is_multistate = result.is_multistate
                        db_action.summary = result.summary or ""
                        if result.date_filed:
                            db_action.date_filed = result.date_filed
                        if result.date_resolved:
                            db_action.date_resolved = result.date_resolved

                        # Store defendants
                        if result.defendants:
                            # Clear existing defendants for this action
                            session.execute(
                                delete(ActionDefendant).where(
                                    ActionDefendant.action_id == action.id
                                )
                            )
                            for def_schema in result.defendants:
                                # Find or create defendant
                                existing = session.execute(
                                    select(Defendant).where(
                                        Defendant.raw_name == def_schema.raw_name
                                    )
                                ).scalars().first()
                                if not existing:
                                    existing = Defendant(raw_name=def_schema.raw_name)
                                    session.add(existing)
                                    session.flush()
                                # Create junction record (skip if already exists)
                                exists_already = session.execute(
                                    select(ActionDefendant).where(
                                        ActionDefendant.action_id == action.id,
                                        ActionDefendant.defendant_id == existing.id,
                                    )
                                ).first()
                                if not exists_already:
                                    session.add(ActionDefendant(
                                        action_id=action.id,
                                        defendant_id=existing.id,
                                    ))

                        # Store violation categories
                        if result.violation_categories:
                            session.execute(
                                delete(ViolationCategory).where(
                                    ViolationCategory.action_id == action.id
                                )
                            )
                            for vc in result.violation_categories:
                                session.add(ViolationCategory(
                                    action_id=action.id,
                                    category=vc.category,
                                    subcategory=vc.subcategory,
                                    confidence=vc.confidence,
                                ))

                        # Store monetary terms (always clear old ones first)
                        session.execute(
                            delete(MonetaryTerms).where(
                                MonetaryTerms.action_id == action.id
                            )
                        )
                        if result.monetary_terms:
                            mt = result.monetary_terms
                            session.add(MonetaryTerms(
                                action_id=action.id,
                                total_amount=mt.total_amount,
                                civil_penalty=mt.civil_penalty,
                                consumer_restitution=mt.consumer_restitution,
                                fees_and_costs=mt.fees_and_costs,
                                amount_is_estimated=mt.amount_is_estimated,
                            ))

                        # Store statute citations (always clear old ones first)
                        session.execute(
                            delete(StatuteCited).where(
                                StatuteCited.action_id == action.id
                            )
                        )
                        if result.statutes_cited:
                            for sc in result.statutes_cited:
                                session.add(StatuteCited(
                                    action_id=action.id,
                                    statute_raw=sc.statute_raw,
                                    statute_normalized=sc.statute_normalized,
                                    statute_name=sc.statute_name or "",
                                    is_state_statute=sc.is_state_statute,
                                    is_federal_statute=sc.is_federal_statute,
                                ))

                        session.commit()

                extracted += 1

            except Exception as e:
                errors += 1
                logging.getLogger(__name__).error(
                    "Extraction failed for %s: %s", action.source_url, e,
                )
            progress.advance(task)

    console.print(
        f"\nExtracted [green]{extracted}[/green] actions. "
        f"Filtered [yellow]{filtered_out}[/yellow] non-enforcement. "
        f"[red]{errors}[/red] errors."
    )


@cli.command()
@click.option("--format", "fmt", type=click.Choice(["csv", "json"]), default="csv", help="Output format.")
@click.option("--output", "-o", "output_path", required=True, help="Output file path.")
@click.option("--state", "state_code", required=False, help="Filter by state code.")
@click.option("--since", "since_date", required=False, help="Actions on or after this date (YYYY-MM-DD).")
@click.pass_context
def export(ctx, fmt, output_path, state_code, since_date):
    """Export enforcement actions to CSV or JSON."""
    from scripts.export import load_actions, export_csv, export_json

    db: Database = ctx.obj["db"]
    since = date.fromisoformat(since_date) if since_date else None
    actions = load_actions(db, state=state_code, since=since)

    out = Path(output_path)
    if fmt == "csv":
        export_csv(actions, out)
    elif fmt == "json":
        export_json(actions, out)


@cli.command()
@click.option("--output", "-o", "output_path", default="data/processed/insights_report.md", help="Output file path.")
@click.pass_context
def analyze(ctx, output_path):
    """Generate analytics insights report."""
    from scripts.analyze import generate_report

    db: Database = ctx.obj["db"]
    report = generate_report(db)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")
    console.print(f"Report written to [cyan]{out}[/cyan] ({len(report.splitlines())} lines)")


@cli.command("resolve-entities")
@click.pass_context
def resolve_entities(ctx):
    """Run entity resolution on all defendants in the database."""
    from src.normalization.entities import EntityResolver
    from src.storage.models import Defendant

    db: Database = ctx.obj["db"]
    resolver = EntityResolver()

    with db.get_session() as session:
        defendants = session.execute(
            select(Defendant).where(Defendant.canonical_name == "")
        ).scalars().all()

    if not defendants:
        console.print("[yellow]No unresolved defendants found.[/yellow]")
    else:
        resolved = 0
        for d in defendants:
            canonical, confidence = resolver.resolve(d.raw_name)
            with db.get_session() as session:
                db_d = session.get(Defendant, d.id)
                if db_d:
                    db_d.canonical_name = canonical
                    session.commit()
                    resolved += 1

        console.print(f"Resolved [green]{resolved}[/green] defendant names.")

    review = resolver.get_review_queue()
    if review:
        table = Table(title=f"Entity Resolution Review Queue ({len(review)} items)")
        table.add_column("Raw Name", style="cyan")
        table.add_column("Best Match", style="green")
        table.add_column("Score", justify="right")
        for raw, candidate, score in review:
            table.add_row(raw, candidate, f"{score*100:.0f}%")
        console.print(table)


@cli.command()
@click.option("--port", default=8501, help="Port for the dashboard.")
def dashboard(port):
    """Launch the Streamlit dashboard."""
    import subprocess
    dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
    console.print(f"Launching dashboard at [cyan]http://localhost:{port}[/cyan]")
    import sys
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(dashboard_path), "--server.port", str(port)],
        check=True,
    )


if __name__ == "__main__":
    cli()
