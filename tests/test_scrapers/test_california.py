"""Tests for the California AG scraper against saved HTML fixtures.

These tests are deterministic — they run against real HTML snapshots
from the Wayback Machine (web.archive.org) of oag.ca.gov, not live websites.
"""

from datetime import date
from pathlib import Path

import pytest

from src.scrapers.registry import get_scraper
from src.validation.schemas import PressReleaseListItem


# ── Fixtures directory ──────────────────────────────────────────────────

CA_FIXTURES = Path(__file__).parent.parent / "fixtures" / "california"


# ── Listing page tests ──────────────────────────────────────────────────

class TestCaliforniaListingPage:
    """Tests for parsing the California AG listing page."""

    def test_parse_listing_finds_all_items(self, ca_scraper, ca_listing_html):
        items = ca_scraper._parse_listing_page(ca_listing_html)
        assert len(items) == 30

    def test_listing_items_have_titles(self, ca_scraper, ca_listing_html):
        items = ca_scraper._parse_listing_page(ca_listing_html)
        for item in items:
            assert item.title, f"Item missing title: {item}"
            assert len(item.title) > 10

    def test_listing_items_have_urls(self, ca_scraper, ca_listing_html):
        items = ca_scraper._parse_listing_page(ca_listing_html)
        for item in items:
            assert item.url.startswith("https://oag.ca.gov/"), f"Bad URL: {item.url}"

    def test_listing_items_have_dates(self, ca_scraper, ca_listing_html):
        items = ca_scraper._parse_listing_page(ca_listing_html)
        for item in items:
            assert item.date is not None, f"Missing date for: {item.title}"
            assert isinstance(item.date, date)

    def test_listing_items_have_state_code(self, ca_scraper, ca_listing_html):
        items = ca_scraper._parse_listing_page(ca_listing_html)
        for item in items:
            assert item.state == "CA"

    def test_listing_dates_are_in_expected_range(self, ca_scraper, ca_listing_html):
        items = ca_scraper._parse_listing_page(ca_listing_html)
        for item in items:
            assert item.date.year in (2024, 2025)
            assert 1 <= item.date.month <= 12

    def test_listing_items_sorted_by_date_descending(self, ca_scraper, ca_listing_html):
        items = ca_scraper._parse_listing_page(ca_listing_html)
        dates = [item.date for item in items]
        assert dates == sorted(dates, reverse=True)

    def test_listing_contains_medi_cal_settlement(self, ca_scraper, ca_listing_html):
        """The listing page contains a real Medi-Cal fraud settlement press release."""
        items = ca_scraper._parse_listing_page(ca_listing_html)
        titles = [item.title for item in items]
        assert any("$10 Million" in t and "Settlement" in t for t in titles)

    def test_date_filtering_since(self, ca_scraper, ca_listing_html):
        """The scraper's date filtering works on listing items."""
        items = ca_scraper._parse_listing_page(ca_listing_html)
        since = date(2024, 12, 20)
        filtered = [i for i in items if i.date and i.date >= since]
        # Items from Dec 20 2024 through Jan 2 2025
        assert len(filtered) >= 5
        for item in filtered:
            assert item.date >= since


# ── Detail page tests ───────────────────────────────────────────────────

class TestCaliforniaDetailPages:
    """Tests for parsing individual California press release detail pages."""

    def _parse_detail(self, ca_scraper, fixture_name: str) -> dict:
        """Helper to parse a detail fixture and return the PressRelease."""
        html = (CA_FIXTURES / f"{fixture_name}.html").read_text(encoding="utf-8")
        dummy_item = PressReleaseListItem(
            title="test", url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA",
        )
        return ca_scraper._parse_detail_page(html, dummy_item)

    def test_settlement_has_body_text(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_000_settlement_dollar")
        assert len(pr.body_text) > 500
        assert "OAKLAND" in pr.body_text

    def test_settlement_contains_dollar_amount(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_000_settlement_dollar")
        assert "$10 million" in pr.body_text

    def test_settlement_mentions_medi_cal(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_000_settlement_dollar")
        assert "Medi-Cal" in pr.body_text

    def test_injunctive_has_body_text(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_001_injunctive_only")
        assert len(pr.body_text) > 300
        text_lower = pr.body_text.lower()
        assert "injunction" in text_lower

    def test_injunctive_mentions_mv_realty(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_001_injunctive_only")
        assert "MV Realty" in pr.body_text

    def test_multistate_mentions_states(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_002_multistate")
        text_lower = pr.body_text.lower()
        assert "states" in text_lower or "multistate" in text_lower

    def test_multistate_purdue_settlement(self, ca_scraper):
        """The multistate fixture is the Purdue Pharma opioid settlement."""
        pr = self._parse_detail(ca_scraper, "detail_002_multistate")
        assert "Purdue" in pr.body_text
        assert "Sackler" in pr.body_text

    def test_multiple_defendants_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_003_multiple_defendants")
        assert len(pr.body_text) > 300
        text_lower = pr.body_text.lower()
        assert "defendant" in text_lower

    def test_multiple_defendants_mortgage_fraud(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_003_multiple_defendants")
        text_lower = pr.body_text.lower()
        assert "mortgage" in text_lower

    def test_statute_citation_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_004_statute_citation")
        assert len(pr.body_text) > 300
        assert "ExxonMobil" in pr.body_text

    def test_statute_citation_mentions_plastic(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_004_statute_citation")
        text_lower = pr.body_text.lower()
        assert "plastic" in text_lower
        assert "recycl" in text_lower

    def test_healthcare_fraud_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_005_healthcare_fraud")
        text_lower = pr.body_text.lower()
        assert "false claims" in text_lower or "clinics" in text_lower or "health" in text_lower

    def test_healthcare_settlement_amount(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_005_healthcare_fraud")
        assert "$7.7 million" in pr.body_text

    def test_environmental_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_006_environmental")
        text_lower = pr.body_text.lower()
        assert "emissions" in text_lower or "environmental" in text_lower or "pollution" in text_lower

    def test_environmental_hino_motors(self, ca_scraper):
        """Environmental fixture is the $237M Hino Motors emissions settlement."""
        pr = self._parse_detail(ca_scraper, "detail_006_environmental")
        assert "Hino" in pr.body_text

    def test_data_privacy_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_007_data_privacy")
        text_lower = pr.body_text.lower()
        assert "privacy" in text_lower or "ccpa" in text_lower or "data" in text_lower

    def test_data_privacy_ccpa(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_007_data_privacy")
        assert "CCPA" in pr.body_text or "California Consumer Privacy Act" in pr.body_text

    def test_consumer_alert_no_enforcement(self, ca_scraper):
        """Consumer alert pages should parse but are NOT enforcement actions."""
        pr = self._parse_detail(ca_scraper, "detail_008_consumer_alert")
        assert len(pr.body_text) > 200
        text_lower = pr.body_text.lower()
        # This is a consumer tips/alert page — should NOT contain enforcement language
        assert "settlement" not in text_lower
        assert "judgment" not in text_lower
        assert "defendant" not in text_lower

    def test_policy_statement_no_enforcement(self, ca_scraper):
        """Policy statement pages should parse but are NOT enforcement actions."""
        pr = self._parse_detail(ca_scraper, "detail_009_policy_statement")
        assert len(pr.body_text) > 200
        text_lower = pr.body_text.lower()
        assert "statement" in text_lower or "legislation" in text_lower

    def test_wage_theft_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_010_wage_theft")
        text_lower = pr.body_text.lower()
        assert "wage" in text_lower

    def test_auto_dealer_page(self, ca_scraper):
        """Auto/vehicle fixture is the Ford fuel economy misrepresentation settlement."""
        pr = self._parse_detail(ca_scraper, "detail_011_auto_dealer")
        text_lower = pr.body_text.lower()
        assert "vehicle" in text_lower or "ford" in text_lower

    def test_opioid_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_012_opioid")
        text_lower = pr.body_text.lower()
        assert "opioid" in text_lower

    def test_opioid_mentions_purdue(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_012_opioid")
        assert "Purdue" in pr.body_text

    def test_antitrust_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_013_antitrust")
        text_lower = pr.body_text.lower()
        assert "antitrust" in text_lower

    def test_telecom_page(self, ca_scraper):
        pr = self._parse_detail(ca_scraper, "detail_014_telecom")
        text_lower = pr.body_text.lower()
        assert "robocall" in text_lower or "telecom" in text_lower

    def test_all_detail_pages_have_body_html(self, ca_scraper, ca_detail_htmls):
        """Every detail fixture produces non-empty body_html."""
        dummy_item = PressReleaseListItem(
            title="test", url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA",
        )
        for name, html in ca_detail_htmls.items():
            pr = ca_scraper._parse_detail_page(html, dummy_item)
            assert pr.body_html, f"Empty body_html for {name}"
            assert pr.body_text, f"Empty body_text for {name}"


# ── Database idempotency tests ──────────────────────────────────────────

class TestDatabaseIdempotency:
    """Tests that the database correctly prevents duplicate records."""

    def test_action_exists_check(self, in_memory_db):
        assert not in_memory_db.action_exists("https://oag.ca.gov/test")

    def test_stats_empty_db(self, in_memory_db):
        stats = in_memory_db.stats()
        assert stats["total_actions"] == 0
        assert stats["total_defendants"] == 0

    def test_insert_and_check_action(self, in_memory_db):
        from src.storage.models import EnforcementAction
        with in_memory_db.get_session() as session:
            action = EnforcementAction(
                state="CA",
                date_announced=date(2024, 11, 15),
                action_type="settlement",
                headline="Test Settlement",
                source_url="https://oag.ca.gov/test-settlement",
            )
            session.add(action)
            session.commit()

        assert in_memory_db.action_exists("https://oag.ca.gov/test-settlement")
        assert not in_memory_db.action_exists("https://oag.ca.gov/other")
        assert in_memory_db.get_action_count() == 1
        assert in_memory_db.get_action_count("CA") == 1
        assert in_memory_db.get_action_count("NY") == 0


# ── Scraper registry tests ──────────────────────────────────────────────

class TestScraperRegistry:
    """Tests for the scraper registry and factory."""

    def test_get_california_scraper(self):
        scraper = get_scraper("california")
        assert scraper.state_code == "CA"
        assert scraper.state_name == "California"

    def test_unknown_state_raises(self):
        with pytest.raises(ValueError, match="Unknown state"):
            get_scraper("nonexistent_state")

    def test_active_states(self):
        from src.scrapers.registry import get_active_states
        active = get_active_states()
        assert "california" in active

    def test_state_code_lookup(self):
        from src.scrapers.registry import state_key_from_code
        assert state_key_from_code("CA") == "california"
        assert state_key_from_code("ZZ") is None
