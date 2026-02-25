"""Export enforcement action data to CSV, JSON, or Excel.

Run: python scripts/export.py --format csv --output data/processed/actions.csv
"""

from __future__ import annotations

import csv
import io
import json
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import click
from rich.console import Console

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select, desc
from sqlalchemy.orm import joinedload

from src.storage.database import Database
from src.storage.models import (
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ViolationCategory,
    MonetaryTerms,
    StatuteCited,
)

console = Console()


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


def load_actions(db: Database, state: str | None = None, since: date | None = None) -> list[dict]:
    """Load all enforcement actions with related data, returned as flat dicts."""
    with db.get_session() as session:
        stmt = (
            select(EnforcementAction)
            .options(
                joinedload(EnforcementAction.action_defendants).joinedload(ActionDefendant.defendant),
                joinedload(EnforcementAction.violation_categories),
                joinedload(EnforcementAction.monetary_terms),
                joinedload(EnforcementAction.statutes_cited),
            )
            .order_by(desc(EnforcementAction.date_announced))
        )
        if state:
            stmt = stmt.where(EnforcementAction.state == state.upper())
        if since:
            stmt = stmt.where(EnforcementAction.date_announced >= since)

        actions = session.execute(stmt).unique().scalars().all()
        # Convert to dicts inside the session to avoid DetachedInstanceError
        return [action_to_row(a) for a in actions]


def action_to_row(a: EnforcementAction) -> dict:
    """Convert an enforcement action to a flat dict for export."""
    defendants = "; ".join(
        ad.defendant.canonical_name or ad.defendant.raw_name
        for ad in a.action_defendants
    )
    categories = "; ".join(vc.category for vc in a.violation_categories)
    statutes = "; ".join(sc.statute_raw for sc in a.statutes_cited)
    amount = float(a.monetary_terms.total_amount) if a.monetary_terms else None
    civil_penalty = float(a.monetary_terms.civil_penalty) if a.monetary_terms and a.monetary_terms.civil_penalty else None
    restitution = float(a.monetary_terms.consumer_restitution) if a.monetary_terms and a.monetary_terms.consumer_restitution else None

    return {
        "id": a.id,
        "state": a.state,
        "date_announced": str(a.date_announced),
        "date_filed": str(a.date_filed) if a.date_filed else "",
        "date_resolved": str(a.date_resolved) if a.date_resolved else "",
        "action_type": a.action_type,
        "status": a.status,
        "headline": a.headline,
        "summary": a.summary or "",
        "defendants": defendants,
        "total_amount": amount,
        "civil_penalty": civil_penalty,
        "consumer_restitution": restitution,
        "categories": categories,
        "statutes_cited": statutes,
        "is_multistate": a.is_multistate,
        "quality_score": a.quality_score,
        "source_url": a.source_url,
    }


def export_csv(rows: list[dict], output_path: Path):
    """Export to CSV."""
    if not rows:
        console.print("[yellow]No actions to export.[/yellow]")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    console.print(f"Exported [green]{len(rows)}[/green] actions to [cyan]{output_path}[/cyan]")


def export_json(rows: list[dict], output_path: Path):
    """Export to JSON."""
    if not rows:
        console.print("[yellow]No actions to export.[/yellow]")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"count": len(rows), "actions": rows}, f, indent=2, cls=DecimalEncoder)

    console.print(f"Exported [green]{len(rows)}[/green] actions to [cyan]{output_path}[/cyan]")


@click.command()
@click.option("--format", "fmt", type=click.Choice(["csv", "json"]), default="csv", help="Output format.")
@click.option("--output", "-o", "output_path", required=True, help="Output file path.")
@click.option("--state", required=False, help="Filter by state code.")
@click.option("--since", required=False, help="Only include actions on or after this date (YYYY-MM-DD).")
@click.option("--db", "db_path", default="data/ag_enforcement.db", help="Database path.")
def main(fmt, output_path, state, since, db_path):
    """Export enforcement action data."""
    db = Database(db_path)
    since_date = date.fromisoformat(since) if since else None
    rows = load_actions(db, state=state, since=since_date)

    out = Path(output_path)
    if fmt == "csv":
        export_csv(rows, out)
    elif fmt == "json":
        export_json(rows, out)


if __name__ == "__main__":
    main()
