"""Tests for the non-enforcement press release filter.

Tests verify that the filter correctly distinguishes:
- Enforcement actions (settlements, lawsuits, etc.) → KEEP
- Non-enforcement (consumer alerts, policy statements) → REJECT
"""

from datetime import date
from pathlib import Path

import pytest

from src.extractors.filter import is_enforcement_action


CA_FIXTURES = Path(__file__).parent.parent / "fixtures" / "california"


class TestKeywordScreen:
    """Test the keyword-based first pass."""

    def test_settlement_is_enforcement(self):
        result = is_enforcement_action(
            "AG Announces $10 Million Settlement with Healthcare Provider",
            "The settlement requires the company to pay civil penalties.",
        )
        assert result.is_enforcement is True

    def test_lawsuit_is_enforcement(self):
        result = is_enforcement_action(
            "AG Sues ExxonMobil for Deceiving the Public",
            "The Attorney General filed a complaint in Superior Court.",
        )
        assert result.is_enforcement is True

    def test_injunction_is_enforcement(self):
        result = is_enforcement_action(
            "AG Secures Preliminary Injunction Against MV Realty",
            "The court granted a preliminary injunction preventing the company.",
        )
        assert result.is_enforcement is True

    def test_consumer_alert_is_not_enforcement(self):
        result = is_enforcement_action(
            "AG Warns Consumers of Surge in Text-Based Toll Scam Activity",
            "The Attorney General reminds consumers to be aware of scam texts.",
        )
        assert result.is_enforcement is False

    def test_policy_statement_is_not_enforcement(self):
        result = is_enforcement_action(
            "AG Issues Statement on Legislation Authorizing Additional Civil Penalties",
            "The Attorney General issues statement in response to new legislation.",
        )
        assert result.is_enforcement is False

    def test_personnel_announcement_is_not_enforcement(self):
        result = is_enforcement_action(
            "AG Announces Appointment of New Solicitor General",
            "Attorney General Bonta announced the appointment of a new solicitor general.",
        )
        assert result.is_enforcement is False

    def test_investigation_is_not_enforcement(self):
        result = is_enforcement_action(
            "Department of Justice Investigating Officer-Involved Shooting",
            "The DOJ is investigating an officer-involved shooting under AB 1506.",
        )
        assert result.is_enforcement is False

    # P5: Wayback Machine leak fixes
    def test_murder_case_is_not_enforcement(self):
        result = is_enforcement_action(
            "Ex-Husband Found Guilty of Murder in 2001 Cold Case",
            "The jury convicted the defendant of first-degree murder.",
        )
        assert result.is_enforcement is False

    def test_highlights_wins_is_not_enforcement(self):
        result = is_enforcement_action(
            "AG Yost Highlights 2025 Human Trafficking Wins",
            "The AG celebrated the work of his office over the past year.",
        )
        assert result.is_enforcement is False

    def test_referendum_is_not_enforcement(self):
        result = is_enforcement_action(
            "Title and Summary Language Certified for Proposed Referendum",
            "The AG certified the ballot measure language.",
        )
        assert result.is_enforcement is False

    def test_passing_of_officer_is_not_enforcement(self):
        result = is_enforcement_action(
            "AG Yost's Statement on Passing of Sheriff's Lieutenant",
            "The AG mourns the loss of a dedicated law enforcement officer.",
        )
        assert result.is_enforcement is False

    def test_serial_murder_guilty_plea_is_not_enforcement(self):
        result = is_enforcement_action(
            "Statement from AG Yost on Guilty Plea in Serial Murder Case",
            "The defendant pled guilty to three counts of aggravated murder.",
        )
        assert result.is_enforcement is False


class TestRealFixtures:
    """Test against real CA AG press release fixtures."""

    def _get_text(self, ca_scraper, ca_detail_htmls, fixture_name: str, title: str) -> tuple[str, str]:
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get(fixture_name, "")
        if not html:
            pytest.skip(f"Fixture {fixture_name} not found")
        dummy = PressReleaseListItem(title=title, url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA")
        pr = ca_scraper._parse_detail_page(html, dummy)
        return pr.title, pr.body_text

    # ── Enforcement actions (should be KEPT) ──

    def test_settlement_dollar_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Combats Medi-Cal Fraud, Securing a $10 Million Settlement"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_000_settlement_dollar", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_injunctive_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Secures Preliminary Injunction Against MV Realty"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_001_injunctive_only", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_multistate_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Helps Secure $7.4 Billion from Purdue Pharma"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_002_multistate", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_multiple_defendants_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta: 12 Defendants Held Accountable for $15 Million Scheme"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_003_multiple_defendants", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_exxonmobil_lawsuit_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Sues ExxonMobil for Deceiving the Public"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_004_statute_citation", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_healthcare_settlement_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Secures $7.7 Million Settlement with Healthcare Provider"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_005_healthcare_fraud", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_environmental_settlement_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Announces Nearly $237 Million Settlement with Hino Motors"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_006_environmental", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_wage_theft_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Strikes at Wage Theft"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_010_wage_theft", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    def test_telecom_lawsuit_is_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Announces Lawsuit Against Telecommunications Company"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_014_telecom", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is True, f"Expected enforcement: {result.reason}"

    # ── Non-enforcement (should be REJECTED) ──

    def test_consumer_alert_is_not_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Tell Everyone: Attorney General Bonta Warns Consumers of Surge in Text-Based Toll Scam"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_008_consumer_alert", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is False, f"Expected non-enforcement: {result.reason}"

    def test_policy_statement_is_not_enforcement(self, ca_scraper, ca_detail_htmls):
        title = "Attorney General Bonta Issues Statement on Legislation"
        _, body = self._get_text(ca_scraper, ca_detail_htmls, "detail_009_policy_statement", title)
        result = is_enforcement_action(title, body)
        assert result.is_enforcement is False, f"Expected non-enforcement: {result.reason}"
