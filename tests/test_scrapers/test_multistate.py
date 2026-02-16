"""Tests for all non-California state scrapers against saved HTML fixtures.

These tests run against real HTML snapshots downloaded from live AG websites.
Each state's listing page parsing and detail page body extraction are verified.
"""

from datetime import date
from pathlib import Path

import pytest

from src.scrapers.registry import get_scraper
from src.validation.schemas import PressReleaseListItem


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ── Helper to get scraper + listing HTML ──────────────────────────────────


def _get_listing_items(state_key: str) -> list[PressReleaseListItem]:
    """Parse the listing page fixture and return items."""
    scraper = get_scraper(state_key)
    html_path = FIXTURES_DIR / state_key / "listing_page_0.html"
    if not html_path.exists():
        pytest.skip(f"No listing fixture for {state_key}")
    html = html_path.read_text(encoding="utf-8")
    return scraper._parse_listing_page(html)


def _parse_detail(state_key: str, fixture_name: str):
    """Parse a detail page fixture and return the PressRelease."""
    scraper = get_scraper(state_key)
    html_path = FIXTURES_DIR / state_key / f"{fixture_name}.html"
    if not html_path.exists():
        pytest.skip(f"No detail fixture: {html_path}")
    html = html_path.read_text(encoding="utf-8")
    dummy = PressReleaseListItem(
        title="test", url="https://example.com", date=None, state=scraper.state_code,
    )
    return scraper._parse_detail_page(html, dummy)


# ── New York (NY) ────────────────────────────────────────────────────────


class TestNewYorkListing:
    """Test NY AG listing page parsing."""

    def test_finds_items(self):
        items = _get_listing_items("new_york")
        assert len(items) >= 20

    def test_items_have_titles(self):
        items = _get_listing_items("new_york")
        for item in items:
            assert item.title and len(item.title) > 10

    def test_items_have_urls(self):
        items = _get_listing_items("new_york")
        for item in items:
            assert item.url.startswith("https://ag.ny.gov/")

    def test_items_have_dates(self):
        items = _get_listing_items("new_york")
        for item in items:
            assert item.date is not None

    def test_items_have_state_code(self):
        items = _get_listing_items("new_york")
        for item in items:
            assert item.state == "NY"


class TestNewYorkDetail:
    """Test NY AG detail page body extraction."""

    def test_recovers_47m_has_body(self):
        pr = _parse_detail("new_york", "detail_000_recovers_47m_truck_rental")
        assert len(pr.body_text) > 500
        assert "$4.7 million" in pr.body_text.lower() or "$4,700,000" in pr.body_text

    def test_conviction_has_body(self):
        pr = _parse_detail("new_york", "detail_001_conviction_nypd_sergeant")
        assert len(pr.body_text) > 500
        assert "conviction" in pr.body_text.lower() or "guilty" in pr.body_text.lower()

    def test_gun_accessory_has_body(self):
        pr = _parse_detail("new_york", "detail_002_stops_gun_accessory")
        assert len(pr.body_text) > 500


# ── Ohio (OH) ─────────────────────────────────────────────────────────────


class TestOhioListing:
    """Test OH AG listing page parsing."""

    def test_finds_items(self):
        items = _get_listing_items("ohio")
        assert len(items) >= 5

    def test_items_have_titles(self):
        items = _get_listing_items("ohio")
        for item in items:
            assert item.title and len(item.title) > 10

    def test_items_have_urls(self):
        items = _get_listing_items("ohio")
        for item in items:
            assert "ohioattorneygeneral.gov" in item.url

    def test_items_have_dates(self):
        items = _get_listing_items("ohio")
        for item in items:
            assert item.date is not None

    def test_items_have_state_code(self):
        items = _get_listing_items("ohio")
        for item in items:
            assert item.state == "OH"


class TestOhioDetail:
    """Test OH AG detail page body extraction."""

    def test_human_trafficking_has_body(self):
        pr = _parse_detail("ohio", "detail_000_human_trafficking_guilty")
        assert len(pr.body_text) > 500
        assert "trafficking" in pr.body_text.lower()

    def test_ponzi_has_body(self):
        pr = _parse_detail("ohio", "detail_001_ponzi_guilty")
        assert len(pr.body_text) > 200

    def test_cannabis_antitrust_has_body(self):
        pr = _parse_detail("ohio", "detail_002_cannabis_antitrust")
        assert len(pr.body_text) > 500
        assert "antitrust" in pr.body_text.lower()


# ── Virginia (VA) ─────────────────────────────────────────────────────────


class TestVirginiaListing:
    """Test VA AG listing page parsing."""

    def test_finds_items(self):
        items = _get_listing_items("virginia")
        assert len(items) >= 5

    def test_items_have_titles(self):
        items = _get_listing_items("virginia")
        for item in items:
            assert item.title and len(item.title) > 15

    def test_items_have_urls(self):
        items = _get_listing_items("virginia")
        for item in items:
            assert "oag.state.va.us" in item.url

    def test_items_filter_non_press_releases(self):
        items = _get_listing_items("virginia")
        for item in items:
            assert "/news-releases/" in item.url

    def test_items_have_state_code(self):
        items = _get_listing_items("virginia")
        for item in items:
            assert item.state == "VA"


class TestVirginiaDetail:
    """Test VA AG detail page body extraction."""

    def test_pharmaceutical_has_body(self):
        pr = _parse_detail("virginia", "detail_000_pharmaceutical")
        assert len(pr.body_text) > 500
        assert "pharmaceutical" in pr.body_text.lower()

    def test_polluters_has_body(self):
        pr = _parse_detail("virginia", "detail_001_big_polluters")
        assert len(pr.body_text) > 200


# ── Oregon (OR) ───────────────────────────────────────────────────────────


class TestOregonListing:
    """Test OR DOJ listing page parsing."""

    def test_finds_items(self):
        items = _get_listing_items("oregon")
        assert len(items) >= 3

    def test_items_have_titles(self):
        items = _get_listing_items("oregon")
        for item in items:
            assert item.title and len(item.title) > 10

    def test_items_have_urls(self):
        items = _get_listing_items("oregon")
        for item in items:
            assert "doj.state.or.us" in item.url

    def test_items_have_state_code(self):
        items = _get_listing_items("oregon")
        for item in items:
            assert item.state == "OR"


class TestOregonDetail:
    """Test OR DOJ detail page body extraction."""

    def test_generic_drug_settlement_has_body(self):
        pr = _parse_detail("oregon", "detail_000_generic_drug_settlement")
        assert len(pr.body_text) > 500
        assert "settlement" in pr.body_text.lower() or "drug" in pr.body_text.lower()

    def test_charity_scheme_has_body(self):
        pr = _parse_detail("oregon", "detail_001_charity_scheme_guilty")
        assert len(pr.body_text) > 500


# ── Pennsylvania (PA) ─────────────────────────────────────────────────────


class TestPennsylvaniaListing:
    """Test PA AG listing page parsing."""

    def test_finds_items(self):
        items = _get_listing_items("pennsylvania")
        assert len(items) >= 20

    def test_items_have_titles(self):
        items = _get_listing_items("pennsylvania")
        for item in items:
            assert item.title and len(item.title) > 5

    def test_items_have_urls(self):
        items = _get_listing_items("pennsylvania")
        for item in items:
            assert "attorneygeneral.gov" in item.url

    def test_items_have_dates(self):
        items = _get_listing_items("pennsylvania")
        for item in items:
            assert item.date is not None

    def test_no_pagination_links(self):
        """Pagination links like /taking-action/page/N/ should be filtered."""
        items = _get_listing_items("pennsylvania")
        for item in items:
            assert "/page/" not in item.url

    def test_items_have_state_code(self):
        items = _get_listing_items("pennsylvania")
        for item in items:
            assert item.state == "PA"


class TestPennsylvaniaDetail:
    """Test PA AG detail page body extraction."""

    def test_gambling_conviction_has_body(self):
        pr = _parse_detail("pennsylvania", "detail_000_gambling_conviction")
        assert len(pr.body_text) > 500
        assert "gambling" in pr.body_text.lower()

    def test_gang_murders_has_body(self):
        pr = _parse_detail("pennsylvania", "detail_001_gang_murders_guilty")
        assert len(pr.body_text) > 500


# ── Texas (TX) ─────────────────────────────────────────────────────────────


class TestTexasListing:
    """Test TX AG listing page parsing."""

    def test_finds_items(self):
        items = _get_listing_items("texas")
        assert len(items) >= 5

    def test_items_have_titles(self):
        items = _get_listing_items("texas")
        for item in items:
            assert item.title and len(item.title) > 10

    def test_items_have_urls(self):
        items = _get_listing_items("texas")
        for item in items:
            assert "texasattorneygeneral.gov" in item.url

    def test_titles_no_soft_hyphens(self):
        """Soft hyphens (\\xad) should be stripped from TX titles."""
        items = _get_listing_items("texas")
        for item in items:
            assert "\xad" not in item.title

    def test_items_have_state_code(self):
        items = _get_listing_items("texas")
        for item in items:
            assert item.state == "TX"


class TestTexasDetail:
    """Test TX AG detail page body extraction."""

    def test_sues_snapchat_has_body(self):
        pr = _parse_detail("texas", "detail_001_sues_snapchat")
        assert len(pr.body_text) > 500
        assert "snap" in pr.body_text.lower()

    def test_sues_bastrop_has_body(self):
        pr = _parse_detail("texas", "detail_002_sues_bastrop")
        assert len(pr.body_text) > 500
        assert "darling" in pr.body_text.lower()


# ── Scraper registry tests for new states ─────────────────────────────────


class TestNewStateRegistry:
    """Test that all new state scrapers are registered and loadable."""

    @pytest.mark.parametrize("state_key,code", [
        ("new_york", "NY"),
        ("ohio", "OH"),
        ("virginia", "VA"),
        ("oregon", "OR"),
        ("pennsylvania", "PA"),
        ("texas", "TX"),
    ])
    def test_scraper_registered(self, state_key, code):
        scraper = get_scraper(state_key)
        assert scraper.state_code == code

    def test_active_states_include_new(self):
        from src.scrapers.registry import get_active_states
        active = get_active_states()
        for state in ["california", "new_york", "ohio", "virginia", "oregon", "pennsylvania", "texas"]:
            assert state in active, f"{state} should be active"
