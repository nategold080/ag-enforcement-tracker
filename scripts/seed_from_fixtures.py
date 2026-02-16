"""Seed the database by running the extraction pipeline on all fixture detail pages.

This creates a realistic dataset from real press release HTML without network access.
Run: python scripts/seed_from_fixtures.py
"""

from __future__ import annotations

import glob
import logging
import sys
import uuid
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import yaml

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.extractors.press_release import PressReleaseExtractor
from src.extractors.filter import is_enforcement_action
from src.normalization.entities import EntityResolver
from src.scrapers.registry import get_scraper
from src.storage.database import Database
from src.storage.models import (
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ViolationCategory,
    MonetaryTerms,
    StatuteCited,
)
from src.validation.schemas import PressReleaseListItem

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"

# Map fixture files to their real titles and dates (from listing pages)
FIXTURE_METADATA = {
    "california": {
        "detail_000_settlement_dollar": {
            "title": "Attorney General Bonta Announces $10 Million Settlement with DaVita for Medi-Cal Overbilling",
            "date": date(2024, 12, 20),
        },
        "detail_001_injunctive_only": {
            "title": "Attorney General Bonta Secures Injunction Against MV Realty for Deceptive Home Equity Scheme",
            "date": date(2024, 12, 18),
        },
        "detail_002_multistate": {
            "title": "Attorney General Bonta Announces Multistate Settlement with Purdue Pharma and the Sackler Family",
            "date": date(2024, 12, 16),
        },
        "detail_003_multiple_defendants": {
            "title": "Attorney General Bonta Files Charges Against Mortgage Fraud Ring Targeting Elderly Homeowners",
            "date": date(2024, 12, 12),
        },
        "detail_004_statute_citation": {
            "title": "Attorney General Bonta Sues ExxonMobil for Decades of Deception Around Plastic Recycling",
            "date": date(2024, 12, 10),
        },
        "detail_005_healthcare_fraud": {
            "title": "Attorney General Bonta Reaches $7.7 Million Settlement Over Health Clinic False Claims",
            "date": date(2024, 12, 6),
        },
        "detail_006_environmental": {
            "title": "Attorney General Bonta Announces $237 Million Settlement with Hino Motors for Emissions Fraud",
            "date": date(2024, 12, 4),
        },
        "detail_007_data_privacy": {
            "title": "Attorney General Bonta Settles CCPA Enforcement Action Against DoorDash for Data Privacy Violations",
            "date": date(2024, 12, 2),
        },
        "detail_010_wage_theft": {
            "title": "Attorney General Bonta Secures $8.5 Million in Wage Theft Settlement for Agricultural Workers",
            "date": date(2024, 11, 15),
        },
        "detail_011_auto_dealer": {
            "title": "Attorney General Bonta Announces $65 Million Settlement with Ford for Fuel Economy Misrepresentation",
            "date": date(2024, 11, 12),
        },
        "detail_012_opioid": {
            "title": "Attorney General Bonta Announces Settlement with Purdue Pharma Over Opioid Crisis",
            "date": date(2024, 11, 8),
        },
        "detail_013_antitrust": {
            "title": "Attorney General Bonta Files Antitrust Lawsuit Against Major Real Estate Brokerages",
            "date": date(2024, 11, 5),
        },
        "detail_014_telecom": {
            "title": "Attorney General Bonta Secures $5.5 Million Settlement for Illegal Robocall Scheme",
            "date": date(2024, 11, 1),
        },
    },
    "new_york": {
        "detail_000_recovers_47m_truck_rental": {
            "title": "Attorney General James Recovers Over $4.7 Million From NYC Truck Rental Companies and Their Accountant for Evading Taxes",
            "date": date(2026, 2, 6),
        },
        "detail_001_conviction_nypd_sergeant": {
            "title": "Attorney General James Announces Conviction of NYPD Sergeant on Manslaughter Charge",
            "date": date(2026, 2, 4),
        },
        "detail_002_stops_gun_accessory": {
            "title": "Attorney General James Stops Sale of Gun Accessory that Aided Buffalo Shooter",
            "date": date(2026, 2, 3),
        },
    },
    "ohio": {
        "detail_000_human_trafficking_guilty": {
            "title": "Eight People Plead Guilty in Mahoning Valley Human Trafficking Case",
            "date": date(2026, 2, 11),
        },
        "detail_001_ponzi_guilty": {
            "title": "Fourth Defendant Pleads Guilty in Decade-long Ponzi Scheme",
            "date": date(2026, 2, 7),
        },
        "detail_002_cannabis_antitrust": {
            "title": "Yost Sues Multistate Cannabis Operators for Anti-Competitive Conduct",
            "date": date(2026, 2, 5),
        },
    },
    "virginia": {
        "detail_000_pharmaceutical": {
            "title": "Attorney General Jones Announces Major Legal Actions to Hold Pharmaceutical Companies Accountable and Put Money Back in Virginians' Pockets",
            "date": date(2026, 2, 10),
        },
        "detail_001_big_polluters": {
            "title": "Attorney General Jones Moves to Hold Big Polluters Accountable",
            "date": date(2026, 2, 7),
        },
    },
    "oregon": {
        "detail_000_generic_drug_settlement": {
            "title": "AG Rayfield Announces Settlements with Lannett and Bausch Totalling $17.85 Million Over Conspiracies to Inflate Prices and Limit Competition",
            "date": date(2026, 2, 5),
        },
        "detail_001_charity_scheme_guilty": {
            "title": "Former Orangetheory Fitness Instructor Pleads Guilty in Charity Scheme",
            "date": date(2026, 1, 29),
        },
    },
    "pennsylvania": {
        "detail_000_gambling_conviction": {
            "title": "Office of Attorney General Secures Felony Conviction, $3M Asset Payment from Central Pa.-Based Company That Installed Hundreds of Illegal Video Gambling Machines",
            "date": date(2026, 2, 6),
        },
        "detail_001_gang_murders_guilty": {
            "title": "VERDICT: Two Philadelphia Men Found Guilty in Multiple Gang-Related Murders and Shootings",
            "date": date(2026, 2, 4),
        },
    },
    "texas": {
        "detail_000_human_trafficking_sentence": {
            "title": "Attorney General Ken Paxton Secures 50-Year Sentence Against Human Trafficker in North Texas Child Trafficking Case",
            "date": date(2026, 2, 10),
        },
        "detail_001_sues_snapchat": {
            "title": "Attorney General Paxton Sues Snapchat for Deceiving Parents, Endangering Texas Kids by Exposing Them to Addictive Features, and Serving as a Breeding Ground for Obscene Conduct",
            "date": date(2026, 2, 7),
        },
        "detail_002_sues_bastrop": {
            "title": "Attorney General Ken Paxton Sues Bastrop Factory for Harming Local Residents by Illegally Emitting Putrid Odors",
            "date": date(2026, 2, 5),
        },
    },
}


def seed():
    """Run the full extraction pipeline on all fixture detail pages and store results."""
    db = Database()
    db.create_tables()

    with open(Path(__file__).parent.parent / "config" / "taxonomy.yaml") as f:
        taxonomy = yaml.safe_load(f)

    extractor = PressReleaseExtractor(taxonomy)
    resolver = EntityResolver()

    total_inserted = 0
    total_skipped = 0
    total_rejected = 0

    for state_key, fixtures in FIXTURE_METADATA.items():
        scraper = get_scraper(state_key)
        logger.info("Processing %s (%s) — %d fixtures", state_key, scraper.state_code, len(fixtures))

        for fixture_stem, meta in fixtures.items():
            fixture_path = FIXTURES_DIR / state_key / f"{fixture_stem}.html"
            if not fixture_path.exists():
                logger.warning("Fixture not found: %s", fixture_path)
                continue

            html = fixture_path.read_text(encoding="utf-8")
            source_url = f"https://fixture/{state_key}/{fixture_stem}"

            # Check idempotency
            if db.action_exists(source_url):
                total_skipped += 1
                continue

            # Parse detail page
            dummy_item = PressReleaseListItem(
                title=meta["title"],
                url=source_url,
                date=meta["date"],
                state=scraper.state_code,
            )
            pr = scraper._parse_detail_page(html, dummy_item)

            # Check enforcement filter
            filter_result = is_enforcement_action(meta["title"], pr.body_text)
            if not filter_result.is_enforcement:
                logger.info("  REJECTED (not enforcement): %s", meta["title"][:60])
                total_rejected += 1
                continue

            # Extract structured data
            result = extractor.extract(pr, date_announced=meta["date"])

            # Store in database
            with db.get_session() as session:
                action = EnforcementAction(
                    id=str(result.id),
                    state=result.state,
                    date_announced=result.date_announced,
                    date_filed=result.date_filed,
                    date_resolved=result.date_resolved,
                    action_type=result.action_type.value,
                    status=result.status.value,
                    headline=result.headline,
                    summary=result.summary,
                    source_url=source_url,
                    is_multistate=result.is_multistate,
                    quality_score=result.quality_score,
                    extraction_method=result.extraction_method.value,
                    raw_text=result.raw_text[:5000],  # Truncate for DB
                )
                session.add(action)

                # Add monetary terms
                if result.monetary_terms:
                    mt = MonetaryTerms(
                        action_id=str(result.id),
                        total_amount=result.monetary_terms.total_amount,
                        civil_penalty=result.monetary_terms.civil_penalty,
                        consumer_restitution=result.monetary_terms.consumer_restitution,
                        fees_and_costs=result.monetary_terms.fees_and_costs,
                        amount_is_estimated=result.monetary_terms.amount_is_estimated,
                    )
                    session.add(mt)

                # Add violation categories
                for vc in result.violation_categories:
                    session.add(ViolationCategory(
                        action_id=str(result.id),
                        category=vc.category,
                        subcategory=vc.subcategory,
                        confidence=vc.confidence,
                    ))

                # Add statutes
                for sc in result.statutes_cited:
                    session.add(StatuteCited(
                        action_id=str(result.id),
                        statute_raw=sc.statute_raw,
                        statute_normalized=sc.statute_normalized,
                        statute_name=sc.statute_name,
                        is_state_statute=sc.is_state_statute,
                        is_federal_statute=sc.is_federal_statute,
                    ))

                # Add defendants with entity resolution
                for d_schema in result.defendants:
                    canonical, confidence = resolver.resolve(d_schema.raw_name)
                    metadata = resolver.get_metadata(canonical)

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
                        action_id=str(result.id),
                        defendant_id=defendant.id,
                        role="primary",
                    ))

                session.commit()
                total_inserted += 1
                logger.info(
                    "  OK: %s | type=%s amount=%s defendants=%d cats=%s",
                    meta["title"][:50],
                    result.action_type.value,
                    f"${result.monetary_terms.total_amount:,.0f}" if result.monetary_terms else "N/A",
                    len(result.defendants),
                    [vc.category for vc in result.violation_categories],
                )

    logger.info(
        "\nSeed complete: %d inserted, %d skipped (existing), %d rejected (non-enforcement)",
        total_inserted, total_skipped, total_rejected,
    )

    # Show review queue from entity resolver
    review = resolver.get_review_queue()
    if review:
        logger.info("\nEntity resolution review queue (%d items):", len(review))
        for raw, candidate, score in review:
            logger.info("  %s → %s (%.0f%%)", raw, candidate, score * 100)


if __name__ == "__main__":
    seed()
