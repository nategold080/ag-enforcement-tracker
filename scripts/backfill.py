"""Backfill historical data from state AG websites.

Run: python scripts/backfill.py --state CA --since 2020-01-01

Phase 5 component — not yet implemented.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@click.command()
@click.option("--state", required=False, help="State code to backfill (default: all active).")
@click.option("--since", required=False, default="2020-01-01", help="Start date for backfill (YYYY-MM-DD).")
@click.option("--db", "db_path", default="data/ag_enforcement.db", help="Database path.")
def main(state, since, db_path):
    """Backfill historical enforcement action data."""
    raise NotImplementedError(
        "Historical backfill is not yet implemented (Phase 5). "
        "Use 'python -m src.cli scrape --state XX --since YYYY-MM-DD' for current scraping."
    )


if __name__ == "__main__":
    main()
