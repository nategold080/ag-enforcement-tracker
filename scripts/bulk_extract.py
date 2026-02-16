"""Run the extraction pipeline on all unprocessed enforcement action records.

Processes records in batches: filter → extract → entity resolution → update.

Run: python scripts/bulk_extract.py
     python scripts/bulk_extract.py --reprocess   # Re-extract ALL records (clears old data)
"""

from __future__ import annotations

import logging
import re
import sys
import time
from datetime import date
from pathlib import Path

import click
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import delete, select, func, update

from src.extractors.filter import is_enforcement_action
from src.extractors.patterns import extract_announced_date
from src.extractors.press_release import PressReleaseExtractor
from src.normalization.entities import EntityResolver
from src.storage.database import Database
from src.storage.models import (
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ViolationCategory,
    MonetaryTerms,
    StatuteCited,
)
from src.validation.schemas import PressRelease

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk_extract")

# Sentinel date used as fallback when scraper couldn't find a real date
FALLBACK_DATE = date(2022, 1, 1)

# Maximum plausible settlement amount — anything above is flagged
MAX_PLAUSIBLE_AMOUNT = 50_000_000_000  # $50B


def backfill_dates(db: Database) -> int:
    """Fix records that have the fallback date (2022-01-01) by extracting from body text."""
    fixed = 0
    batch_size = 500

    with db.get_session() as session:
        total = session.execute(
            select(func.count(EnforcementAction.id))
            .where(EnforcementAction.date_announced == FALLBACK_DATE)
            .where(EnforcementAction.raw_text != "")
        ).scalar_one()

    if total == 0:
        return 0

    logger.info("Backfilling dates for %d records with fallback date %s", total, FALLBACK_DATE)

    # Load ALL fallback-date records at once (they're lightweight — just id, url, first 2000 chars of text)
    with db.get_session() as session:
        all_actions = session.execute(
            select(EnforcementAction.id, EnforcementAction.raw_text, EnforcementAction.source_url)
            .where(EnforcementAction.date_announced == FALLBACK_DATE)
            .where(EnforcementAction.raw_text != "")
        ).all()

    updates = []
    for action_id, raw_text, source_url in all_actions:
        extracted_date = extract_announced_date(raw_text)

        # Try extracting from URL patterns
        if not extracted_date and source_url:
            # NY pattern with month: /press-release/YYYY/MM/slug
            m = re.search(r'/press-release/(\d{4})/(\d{1,2})/', source_url)
            if m:
                try:
                    extracted_date = date(int(m.group(1)), int(m.group(2)), 15)
                except ValueError:
                    pass
            if not extracted_date:
                # Year-only URL pattern (NY): /press-release/YYYY/slug
                m = re.search(r'/press-release/(\d{4})/', source_url)
                if m:
                    year = int(m.group(1))
                    if 2020 <= year <= 2030:
                        extracted_date = date(year, 7, 1)  # mid-year approximation

        if extracted_date and extracted_date != FALLBACK_DATE:
            updates.append({"id": action_id, "date_announced": extracted_date})

    # Apply all updates in batches
    if updates:
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            with db.get_session() as session:
                for upd in batch:
                    session.execute(
                        update(EnforcementAction)
                        .where(EnforcementAction.id == upd["id"])
                        .values(date_announced=upd["date_announced"])
                    )
                session.commit()
            fixed = i + len(batch)
            logger.info("  Backfilled %d/%d dates so far", fixed, total)

    fixed = len(updates)
    logger.info("Date backfill complete: %d dates fixed out of %d", fixed, total)
    return fixed


def reset_extraction_data(db: Database) -> None:
    """Clear all extraction-derived data for a full reprocess."""
    logger.info("Clearing old extraction data for reprocess...")
    with db.get_session() as session:
        session.execute(delete(ActionDefendant))
        session.execute(delete(Defendant))
        session.execute(delete(ViolationCategory))
        session.execute(delete(MonetaryTerms))
        session.execute(delete(StatuteCited))
        # Reset quality scores to 0.0 so all records get reprocessed
        session.execute(
            update(EnforcementAction)
            .where(EnforcementAction.raw_text != "")
            .values(quality_score=0.0)
        )
        session.commit()
    logger.info("Old extraction data cleared.")


def run_extraction(reprocess: bool = False):
    db = Database()
    db.create_tables()

    # Step 0: Backfill dates first (before extraction uses them)
    backfill_dates(db)

    if reprocess:
        reset_extraction_data(db)

    # Load taxonomy
    with open(Path("config/taxonomy.yaml")) as f:
        taxonomy = yaml.safe_load(f)

    extractor = PressReleaseExtractor(taxonomy)
    resolver = EntityResolver()

    # Find unprocessed records (quality_score = 0.0 means not yet extracted)
    with db.get_session() as session:
        total_unprocessed = session.execute(
            select(func.count(EnforcementAction.id))
            .where(EnforcementAction.quality_score == 0.0)
            .where(EnforcementAction.raw_text != "")
        ).scalar_one()

        # Also count by state
        by_state = session.execute(
            select(EnforcementAction.state, func.count())
            .where(EnforcementAction.quality_score == 0.0)
            .where(EnforcementAction.raw_text != "")
            .group_by(EnforcementAction.state)
        ).all()

    logger.info("Found %d unprocessed records to extract:", total_unprocessed)
    for state, count in sorted(by_state, key=lambda x: -x[1]):
        logger.info("  %s: %d", state, count)

    if total_unprocessed == 0:
        logger.info("No unprocessed records. Done.")
        return

    # Process in batches
    stats = {
        "extracted": 0,
        "filtered_out": 0,
        "errors": 0,
        "low_quality": 0,
        "defendants_resolved": 0,
        "defendants_new": 0,
        "monetary_capped": 0,
    }

    batch_size = 100

    while True:
        with db.get_session() as session:
            actions = session.execute(
                select(EnforcementAction)
                .where(EnforcementAction.quality_score == 0.0)
                .where(EnforcementAction.raw_text != "")
                .limit(batch_size)
                .offset(0)  # Always 0 because processed ones get quality_score > 0
            ).scalars().all()

        if not actions:
            break

        for action in actions:
            try:
                process_action(db, action, extractor, resolver, stats)
            except Exception as e:
                stats["errors"] += 1
                logger.warning("Error processing %s: %s", action.source_url, e)
                # Mark as processed to avoid infinite loop
                with db.get_session() as session:
                    db_action = session.get(EnforcementAction, action.id)
                    if db_action:
                        db_action.quality_score = 0.1
                        session.commit()

        processed = stats["extracted"] + stats["filtered_out"] + stats["errors"]
        logger.info(
            "Progress: %d/%d processed (%d extracted, %d filtered, %d errors)",
            processed, total_unprocessed,
            stats["extracted"], stats["filtered_out"], stats["errors"],
        )

    # Show results
    logger.info("\n" + "=" * 60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info("  Extracted:       %d", stats["extracted"])
    logger.info("  Filtered out:    %d (non-enforcement)", stats["filtered_out"])
    logger.info("  Errors:          %d", stats["errors"])
    logger.info("  Low quality (<0.5): %d", stats["low_quality"])
    logger.info("  Defendants resolved: %d", stats["defendants_resolved"])
    logger.info("  Defendants new:     %d", stats["defendants_new"])
    logger.info("  Monetary amounts capped: %d", stats["monetary_capped"])

    # Show entity resolution review queue
    review = resolver.get_review_queue()
    if review:
        logger.info("\nEntity resolution review queue (%d items):", len(review))
        for raw, candidate, score in review[:20]:
            logger.info("  %s → %s (%.0f%%)", raw, candidate, score * 100)

    # Final DB stats
    with db.get_session() as session:
        total = session.execute(select(func.count(EnforcementAction.id))).scalar_one()
        extracted = session.execute(
            select(func.count(EnforcementAction.id))
            .where(EnforcementAction.quality_score > 0.0)
        ).scalar_one()
        below_05 = session.execute(
            select(func.count(EnforcementAction.id))
            .where(EnforcementAction.quality_score > 0.0)
            .where(EnforcementAction.quality_score < 0.5)
        ).scalar_one()
        defendants = session.execute(select(func.count(Defendant.id))).scalar_one()
        categories = session.execute(select(func.count(ViolationCategory.id))).scalar_one()
        monetary = session.execute(
            select(func.count(MonetaryTerms.id))
            .where(MonetaryTerms.total_amount > 0)
        ).scalar_one()
        fallback_dates = session.execute(
            select(func.count(EnforcementAction.id))
            .where(EnforcementAction.date_announced == FALLBACK_DATE)
        ).scalar_one()

    logger.info("\nFINAL DATABASE STATS:")
    logger.info("  Total records:      %d", total)
    logger.info("  Extracted:          %d", extracted)
    logger.info("  Below 0.5 quality:  %d", below_05)
    logger.info("  Defendants:         %d", defendants)
    logger.info("  Violation categories: %d", categories)
    logger.info("  Records with monetary terms: %d", monetary)
    logger.info("  Records still with fallback date: %d", fallback_dates)


def process_action(
    db: Database,
    action: EnforcementAction,
    extractor: PressReleaseExtractor,
    resolver: EntityResolver,
    stats: dict,
):
    """Process a single enforcement action through the extraction pipeline."""
    # Step 1: Non-enforcement filter
    filter_result = is_enforcement_action(action.headline, action.raw_text)

    if not filter_result.is_enforcement:
        stats["filtered_out"] += 1
        with db.get_session() as session:
            db_action = session.get(EnforcementAction, action.id)
            if db_action:
                db_action.quality_score = 0.15
                db_action.action_type = "other"
                session.commit()
        return

    # Step 2: Extract structured data
    pr = PressRelease(
        title=action.headline,
        url=action.source_url,
        date=action.date_announced,
        state=action.state,
        body_text=action.raw_text,
    )
    result = extractor.extract(pr, date_announced=action.date_announced)

    # Step 3: Update the main record
    with db.get_session() as session:
        db_action = session.get(EnforcementAction, action.id)
        if not db_action:
            return

        db_action.action_type = result.action_type.value
        db_action.status = result.status.value
        db_action.summary = result.summary
        db_action.is_multistate = result.is_multistate
        db_action.quality_score = result.quality_score
        db_action.extraction_method = result.extraction_method.value

        if result.date_filed:
            db_action.date_filed = result.date_filed
        if result.date_resolved:
            db_action.date_resolved = result.date_resolved

        # Step 4: Add monetary terms (with sanity cap)
        if result.monetary_terms:
            total = result.monetary_terms.total_amount
            is_estimated = result.monetary_terms.amount_is_estimated

            # Sanity cap: flag amounts > $50B as estimated
            if total and total > MAX_PLAUSIBLE_AMOUNT:
                stats["monetary_capped"] += 1
                is_estimated = True

            mt = MonetaryTerms(
                action_id=action.id,
                total_amount=total,
                civil_penalty=result.monetary_terms.civil_penalty,
                consumer_restitution=result.monetary_terms.consumer_restitution,
                fees_and_costs=result.monetary_terms.fees_and_costs,
                amount_is_estimated=is_estimated,
            )
            session.add(mt)

        # Step 5: Add violation categories
        for vc in result.violation_categories:
            session.add(ViolationCategory(
                action_id=action.id,
                category=vc.category,
                subcategory=vc.subcategory,
                confidence=vc.confidence,
            ))

        # Step 6: Add statutes
        for sc in result.statutes_cited:
            session.add(StatuteCited(
                action_id=action.id,
                statute_raw=sc.statute_raw,
                statute_normalized=sc.statute_normalized,
                statute_name=sc.statute_name,
                is_state_statute=sc.is_state_statute,
                is_federal_statute=sc.is_federal_statute,
            ))

        # Step 7: Add defendants with entity resolution
        for d_schema in result.defendants:
            canonical, confidence = resolver.resolve(d_schema.raw_name)
            metadata = resolver.get_metadata(canonical)

            if confidence >= 0.7:
                stats["defendants_resolved"] += 1
            else:
                stats["defendants_new"] += 1

            defendant = Defendant(
                raw_name=d_schema.raw_name,
                canonical_name=canonical,
                entity_type=metadata.get("entity_type", "corporation"),
                industry=metadata.get("industry"),
                sec_cik=metadata.get("sec_cik"),
            )
            session.add(defendant)
            session.flush()

            session.add(ActionDefendant(
                action_id=action.id,
                defendant_id=defendant.id,
                role="primary",
            ))

        session.commit()

    stats["extracted"] += 1
    if result.quality_score < 0.5:
        stats["low_quality"] += 1


@click.command()
@click.option("--reprocess", is_flag=True, help="Clear all extraction data and re-extract everything")
def main(reprocess):
    start = time.time()
    run_extraction(reprocess=reprocess)
    elapsed = time.time() - start
    logger.info("Total extraction time: %.1f minutes", elapsed / 60)


if __name__ == "__main__":
    main()
