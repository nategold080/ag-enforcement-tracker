"""Export a clean, human-readable CSV of enforcement actions.

Standalone script using sqlite3 directly (no SQLAlchemy dependency).
Filters to quality_score >= 0.5, joins related tables, and formats
columns with human-readable headers and display names.

Run:
    python scripts/export_clean.py
"""

from __future__ import annotations

import csv
import os
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "ag_enforcement.db"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "ag_enforcement_export.csv"

CATEGORY_DISPLAY = {
    "consumer_protection": "Consumer Protection",
    "data_privacy": "Data Privacy & Security",
    "antitrust": "Antitrust",
    "healthcare": "Healthcare Fraud",
    "environmental": "Environmental",
    "securities": "Securities Fraud",
    "housing_lending": "Housing & Lending",
    "employment": "Wage & Employment",
    "telecommunications": "Telecommunications",
    "charitable": "Charitable / Nonprofit",
    "tobacco_vaping": "Tobacco & Vaping",
    "tech_platform": "Tech Platform Accountability",
    "other": "Other",
}

ACTION_TYPE_DISPLAY = {
    "settlement": "Settlement",
    "lawsuit_filed": "Lawsuit Filed",
    "judgment": "Judgment",
    "injunction": "Injunction",
    "consent_decree": "Consent Decree",
    "assurance_of_discontinuance": "Assurance of Discontinuance",
    "other": "Other",
}

COLUMNS = [
    "State",
    "Date",
    "Headline",
    "Defendant",
    "Action Type",
    "Violation Category",
    "Settlement Amount",
    "Statute Cited",
    "Source URL",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fetch_actions(conn: sqlite3.Connection) -> list[dict]:
    """Fetch enforcement actions with quality_score >= 0.5, sorted by date descending."""
    cur = conn.execute(
        """
        SELECT id, state, date_announced, headline, action_type, source_url
        FROM enforcement_actions
        WHERE quality_score >= 0.5
          AND is_federal_litigation = 0
        ORDER BY date_announced DESC
        """
    )
    actions = []
    for row in cur.fetchall():
        actions.append({
            "id": row[0],
            "state": row[1],
            "date_announced": row[2],
            "headline": row[3],
            "action_type": row[4],
            "source_url": row[5],
        })
    return actions


def fetch_defendants(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """Return a mapping of action_id -> list of canonical defendant names."""
    cur = conn.execute(
        """
        SELECT ad.action_id, d.canonical_name
        FROM action_defendants ad
        JOIN defendants d ON d.id = ad.defendant_id
        ORDER BY ad.action_id, d.canonical_name
        """
    )
    result: dict[str, list[str]] = {}
    for action_id, canonical_name in cur.fetchall():
        result.setdefault(action_id, []).append(canonical_name)
    return result


def fetch_categories(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """Return a mapping of action_id -> list of category keys."""
    cur = conn.execute(
        """
        SELECT action_id, category
        FROM violation_categories
        ORDER BY action_id, category
        """
    )
    result: dict[str, list[str]] = {}
    for action_id, category in cur.fetchall():
        result.setdefault(action_id, []).append(category)
    return result


def fetch_monetary_terms(conn: sqlite3.Connection) -> dict[str, float]:
    """Return a mapping of action_id -> total_amount."""
    cur = conn.execute(
        """
        SELECT action_id, total_amount
        FROM monetary_terms
        """
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def fetch_statutes(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """Return a mapping of action_id -> list of raw statute citations."""
    cur = conn.execute(
        """
        SELECT action_id, statute_raw
        FROM statutes_cited
        ORDER BY action_id, statute_raw
        """
    )
    result: dict[str, list[str]] = {}
    for action_id, statute_raw in cur.fetchall():
        result.setdefault(action_id, []).append(statute_raw)
    return result


def format_amount(amount: float | None) -> str:
    """Format a dollar amount as a plain number string, or empty if None."""
    if amount is None or amount == 0:
        return ""
    # Remove trailing zeros after decimal; show as integer if whole number
    if amount == int(amount):
        return str(int(amount))
    return f"{amount:.2f}"


def format_action_type(raw: str) -> str:
    """Map internal action_type key to display name."""
    return ACTION_TYPE_DISPLAY.get(raw, raw.replace("_", " ").title())


def format_categories(cats: list[str]) -> str:
    """Map internal category keys to display names and join with '; '."""
    # Deduplicate while preserving order
    seen: set[str] = set()
    display_names: list[str] = []
    for cat in cats:
        display = CATEGORY_DISPLAY.get(cat, cat.replace("_", " ").title())
        if display not in seen:
            seen.add(display)
            display_names.append(display)
    return "; ".join(display_names)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        raise SystemExit(1)

    conn = sqlite3.connect(str(DB_PATH))
    try:
        # Fetch all data
        actions = fetch_actions(conn)
        defendants_map = fetch_defendants(conn)
        categories_map = fetch_categories(conn)
        monetary_map = fetch_monetary_terms(conn)
        statutes_map = fetch_statutes(conn)
    finally:
        conn.close()

    # Build rows
    rows: list[dict[str, str]] = []
    for action in actions:
        aid = action["id"]
        row = {
            "State": action["state"] or "",
            "Date": action["date_announced"] or "",
            "Headline": action["headline"] or "",
            "Defendant": "; ".join(defendants_map.get(aid, [])),
            "Action Type": format_action_type(action["action_type"]),
            "Violation Category": format_categories(categories_map.get(aid, [])),
            "Settlement Amount": format_amount(monetary_map.get(aid)),
            "Statute Cited": "; ".join(statutes_map.get(aid, [])),
            "Source URL": action["source_url"] or "",
        }
        rows.append(row)

    # Write CSV
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
