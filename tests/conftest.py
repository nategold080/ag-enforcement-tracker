"""Shared pytest fixtures for the AG Enforcement Tracker test suite."""

from pathlib import Path

import pytest

from src.scrapers.registry import get_scraper
from src.storage.database import Database

FIXTURES_DIR = Path(__file__).parent / "fixtures"
CA_FIXTURES_DIR = FIXTURES_DIR / "california"


@pytest.fixture
def ca_scraper():
    """Return a CaliforniaScraper instance."""
    return get_scraper("california")


@pytest.fixture
def ca_listing_html() -> str:
    """Return the raw HTML of the California listing page fixture."""
    return (CA_FIXTURES_DIR / "listing_page_0.html").read_text(encoding="utf-8")


@pytest.fixture
def ca_detail_htmls() -> dict[str, str]:
    """Return a dict of fixture name → HTML for all California detail pages."""
    result = {}
    for path in sorted(CA_FIXTURES_DIR.glob("detail_*.html")):
        result[path.stem] = path.read_text(encoding="utf-8")
    return result


@pytest.fixture
def in_memory_db():
    """Return a Database instance backed by an in-memory SQLite database."""
    db = Database(":memory:")
    db.create_tables()
    return db
