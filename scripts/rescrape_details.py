"""Re-scrape detail pages for TX/OR/VA with corrected body selectors.

For TX: Re-fetch existing URLs and update body text with corrected selector.
For OR/VA: URLs may have changed, so re-scrape from listing pages first,
           then match by headline similarity to update existing records.

Run: python scripts/rescrape_details.py --states tx,or,va
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
import httpx
from selectolax.parser import HTMLParser
from sqlalchemy import select, update

from src.scrapers.base import BaseScraper
from src.scrapers.registry import get_scraper, load_state_configs
from src.storage.database import Database
from src.storage.models import EnforcementAction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("rescrape")

USER_AGENT = "AGEnforcementTracker/1.0 (research project; contact@example.com)"


async def rescrape_tx(db: Database) -> dict:
    """Re-fetch TX detail pages and update body text with corrected selector."""
    stats = {"state": "TX", "total": 0, "updated": 0, "errors": 0, "empty": 0}

    with db.get_session() as session:
        actions = session.execute(
            select(EnforcementAction.id, EnforcementAction.source_url)
            .where(EnforcementAction.state == "TX")
            .where(EnforcementAction.source_url.like("https://www.texasattorneygeneral.gov/%"))
        ).all()
        stats["total"] = len(actions)
    logger.info("[TX] Found %d real records to re-scrape (excluding fixtures)", stats["total"])

    scraper = get_scraper("texas")
    body_sel = ".main-content-wysiwyg-container"

    for i, (action_id, source_url) in enumerate(actions):
        try:
            html = await scraper.fetch(source_url)
            tree = HTMLParser(html)

            body_node = tree.css_first(body_sel)
            if body_node:
                body_text = body_node.text(separator="\n", strip=True)
                if body_text and len(body_text) > 50:
                    # Extract date from body text
                    pr_date = scraper._extract_date_from_detail(tree, body_text)

                    with db.get_session() as session:
                        updates = {"raw_text": body_text[:10000]}
                        if pr_date:
                            updates["date_announced"] = pr_date
                        session.execute(
                            update(EnforcementAction)
                            .where(EnforcementAction.id == action_id)
                            .values(**updates)
                        )
                        session.commit()
                        stats["updated"] += 1
                else:
                    stats["empty"] += 1
            else:
                stats["empty"] += 1

            if (i + 1) % 50 == 0:
                logger.info("[TX] Progress: %d/%d, %d updated",
                            i + 1, stats["total"], stats["updated"])

        except Exception as e:
            stats["errors"] += 1
            logger.warning("[TX] Error re-scraping %s: %s", source_url, e)

    await scraper.close()
    return stats


async def rescrape_or(db: Database, since: date) -> dict:
    """Re-scrape OR from listing pages — old URLs are dead.

    Deletes old OR records and re-scrapes fresh from the current listing.
    """
    stats = {"state": "OR", "old_count": 0, "deleted": 0, "new_found": 0, "stored": 0, "errors": 0}

    with db.get_session() as session:
        old_count = session.execute(
            select(EnforcementAction.id).where(EnforcementAction.state == "OR")
        ).all()
        stats["old_count"] = len(old_count)

        # Delete old OR records (URLs are dead)
        for (action_id,) in old_count:
            action = session.get(EnforcementAction, action_id)
            if action:
                session.delete(action)
                stats["deleted"] += 1
        session.commit()
    logger.info("[OR] Deleted %d old records with dead URLs", stats["deleted"])

    # Re-scrape from listing
    scraper = get_scraper("oregon")
    press_releases = await scraper.scrape(since=since, max_pages=200)
    stats["new_found"] = len(press_releases)

    for pr in press_releases:
        if not db.action_exists(pr.url):
            with db.get_session() as session:
                action = EnforcementAction(
                    state="OR",
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

    await scraper.close()
    return stats


async def rescrape_va(db: Database, since: date) -> dict:
    """Re-scrape VA from listing pages — old short-numeric URLs are dead.

    Deletes old VA records and re-scrapes fresh from the current listing.
    """
    stats = {"state": "VA", "old_count": 0, "deleted": 0, "new_found": 0, "stored": 0, "errors": 0}

    with db.get_session() as session:
        old_count = session.execute(
            select(EnforcementAction.id).where(EnforcementAction.state == "VA")
        ).all()
        stats["old_count"] = len(old_count)

        for (action_id,) in old_count:
            action = session.get(EnforcementAction, action_id)
            if action:
                session.delete(action)
                stats["deleted"] += 1
        session.commit()
    logger.info("[VA] Deleted %d old records with dead URLs", stats["deleted"])

    # Re-scrape from listing
    scraper = get_scraper("virginia")
    press_releases = await scraper.scrape(since=since, max_pages=200)
    stats["new_found"] = len(press_releases)

    for pr in press_releases:
        if not db.action_exists(pr.url):
            with db.get_session() as session:
                action = EnforcementAction(
                    state="VA",
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

    await scraper.close()
    return stats


async def run_all(since: date, states: list[str]):
    db = Database()
    db.create_tables()

    results = []
    if "tx" in states:
        results.append(await rescrape_tx(db))
    if "or" in states:
        results.append(await rescrape_or(db, since))
    if "va" in states:
        results.append(await rescrape_va(db, since))

    logger.info("\n" + "=" * 60)
    logger.info("RE-SCRAPE SUMMARY")
    logger.info("=" * 60)
    for r in results:
        if r["state"] == "TX":
            logger.info("  TX: %d total, %d updated, %d empty, %d errors",
                        r["total"], r["updated"], r["empty"], r["errors"])
        else:
            logger.info("  %s: %d old deleted, %d new found, %d stored, %d errors",
                        r["state"], r["deleted"], r["new_found"], r["stored"], r["errors"])


@click.command()
@click.option("--since", default="2022-01-01", help="Since date for re-scraping (YYYY-MM-DD)")
@click.option("--states", default="tx,or,va", help="Comma-separated state codes")
def main(since, states):
    since_date = date.fromisoformat(since)
    state_list = [s.strip().lower() for s in states.split(",")]
    logger.info("Re-scraping states: %s since %s", state_list, since_date)
    start = time.time()
    asyncio.run(run_all(since_date, state_list))
    elapsed = time.time() - start
    logger.info("Total time: %.1f minutes", elapsed / 60)


if __name__ == "__main__":
    main()
