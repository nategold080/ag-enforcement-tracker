"""Bulk scrape all reachable states in parallel.

Runs each state as a separate asyncio task so different domains
are scraped concurrently (rate limiting is per-domain).

Run: python scripts/bulk_scrape.py --since 2022-01-01
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click

from src.scrapers.registry import get_scraper, load_state_configs
from src.storage.database import Database
from src.storage.models import EnforcementAction, ScrapeRun

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk_scrape")

# States that are reachable for live scraping
LIVE_STATES = ["california", "ohio", "texas", "oregon", "virginia", "washington"]


async def scrape_state(
    state_key: str,
    db: Database,
    since: date,
    max_pages: int,
) -> dict:
    """Scrape a single state: listing pages → detail pages → database."""
    stats = {"state": state_key, "listings": 0, "details": 0, "stored": 0, "skipped": 0, "errors": 0}

    try:
        scraper = get_scraper(state_key)
        logger.info("[%s] Starting scrape (since=%s, max_pages=%d)",
                     scraper.state_code, since, max_pages)

        # Phase 1: Listing pages
        items = await scraper.scrape_listing(since=since, max_pages=max_pages)
        stats["listings"] = len(items)
        logger.info("[%s] Found %d listing items", scraper.state_code, len(items))

        # Filter already-scraped URLs
        new_items = [item for item in items if not db.action_exists(item.url)]
        stats["skipped"] = len(items) - len(new_items)
        if stats["skipped"]:
            logger.info("[%s] Skipping %d already-scraped URLs", scraper.state_code, stats["skipped"])

        # Phase 2: Detail pages
        for i, item in enumerate(new_items):
            try:
                pr = await scraper.scrape_detail(item)
                stats["details"] += 1

                # Store in database
                if not db.action_exists(pr.url):
                    with db.get_session() as session:
                        action = EnforcementAction(
                            state=pr.state,
                            date_announced=pr.date or since,
                            action_type="other",
                            status="announced",
                            headline=pr.title,
                            source_url=pr.url,
                            raw_text=pr.body_text[:10000] if pr.body_text else "",
                        )
                        session.add(action)
                        session.commit()
                        stats["stored"] += 1

                if (i + 1) % 25 == 0:
                    logger.info("[%s] Progress: %d/%d detail pages fetched, %d stored",
                                scraper.state_code, i + 1, len(new_items), stats["stored"])

            except Exception as e:
                stats["errors"] += 1
                logger.warning("[%s] Failed detail page %s: %s", scraper.state_code, item.url, e)

        await scraper.close()

    except Exception as e:
        logger.error("[%s] Scrape failed: %s", state_key, e, exc_info=True)

    logger.info(
        "[%s] DONE: %d listings, %d detail pages, %d stored, %d skipped, %d errors",
        stats["state"], stats["listings"], stats["details"],
        stats["stored"], stats["skipped"], stats["errors"],
    )
    return stats


async def run_all(since: date, max_pages: int, states: list[str]):
    """Run all state scrapers concurrently."""
    db = Database()
    db.create_tables()

    tasks = [
        scrape_state(state_key, db, since, max_pages)
        for state_key in states
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("\n" + "=" * 60)
    logger.info("BULK SCRAPE SUMMARY")
    logger.info("=" * 60)
    total_stored = 0
    total_errors = 0
    for r in results:
        if isinstance(r, Exception):
            logger.error("State failed with exception: %s", r)
            continue
        logger.info(
            "  %-12s: %4d listings, %4d stored, %3d errors",
            r["state"], r["listings"], r["stored"], r["errors"],
        )
        total_stored += r["stored"]
        total_errors += r["errors"]
    logger.info("TOTAL: %d new records stored, %d errors", total_stored, total_errors)


@click.command()
@click.option("--since", default="2022-01-01", help="Scrape since this date (YYYY-MM-DD)")
@click.option("--max-pages", default=200, help="Max listing pages per state")
@click.option("--states", default=None, help="Comma-separated state keys (default: all reachable)")
def main(since, max_pages, states):
    since_date = date.fromisoformat(since)
    state_list = states.split(",") if states else LIVE_STATES
    logger.info("Bulk scraping %d states since %s (max %d pages each)", len(state_list), since_date, max_pages)
    start = time.time()
    asyncio.run(run_all(since_date, max_pages, state_list))
    elapsed = time.time() - start
    logger.info("Total time: %.1f minutes", elapsed / 60)


if __name__ == "__main__":
    main()
