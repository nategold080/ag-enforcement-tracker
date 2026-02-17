"""Tests for regex-based extraction patterns.

Tests run against both synthetic examples and real CA AG press release fixtures.
"""

from datetime import date
from decimal import Decimal

import pytest

from src.extractors.patterns import (
    classify_action_type,
    classify_monetary_components,
    extract_defendants_from_body,
    extract_defendants_from_headline,
    extract_dollar_amounts,
    extract_filed_date,
    extract_largest_dollar_amount,
    extract_resolved_date,
    extract_statutes,
    is_multistate_action,
)


# ---------------------------------------------------------------------------
# Dollar Amount Extraction
# ---------------------------------------------------------------------------

class TestDollarAmountExtraction:

    def test_simple_millions(self):
        amounts = extract_dollar_amounts("paid $3.5 million in penalties")
        assert len(amounts) == 1
        assert amounts[0].amount == Decimal("3500000")

    def test_simple_billions(self):
        amounts = extract_dollar_amounts("a $7.4 billion settlement")
        assert len(amounts) == 1
        assert amounts[0].amount == Decimal("7400000000")

    def test_raw_number(self):
        amounts = extract_dollar_amounts("totaling $3,500,000 in fines")
        assert len(amounts) == 1
        assert amounts[0].amount == Decimal("3500000")

    def test_multiple_amounts(self):
        text = "The $10 million settlement includes $3 million in penalties and $7 million in restitution"
        amounts = extract_dollar_amounts(text)
        assert len(amounts) == 3
        values = sorted([a.amount for a in amounts])
        assert values == [Decimal("3000000"), Decimal("7000000"), Decimal("10000000")]

    def test_approximate_amount(self):
        amounts = extract_dollar_amounts("approximately $2.5 million in damages")
        assert len(amounts) == 1
        assert amounts[0].is_estimated is True

    def test_nearly_amount(self):
        amounts = extract_dollar_amounts("nearly $237 million settlement")
        assert len(amounts) == 1
        assert amounts[0].is_estimated is True
        assert amounts[0].amount == Decimal("237000000")

    def test_exact_amount(self):
        amounts = extract_dollar_amounts("required to pay $500,000")
        assert len(amounts) == 1
        assert amounts[0].is_estimated is False

    def test_no_amounts(self):
        amounts = extract_dollar_amounts("The AG filed an injunction against the company.")
        assert len(amounts) == 0

    def test_largest_amount(self):
        text = "The $10 million settlement includes $3 million in civil penalties"
        largest = extract_largest_dollar_amount(text)
        assert largest is not None
        assert largest.amount == Decimal("10000000")

    def test_real_fixture_settlement_dollar(self, ca_scraper, ca_detail_htmls):
        """Test against real CA fixture: $10M Medi-Cal settlement."""
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get("detail_000_settlement_dollar", "")
        if not html:
            pytest.skip("Fixture not found")
        dummy = PressReleaseListItem(title="test", url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA")
        pr = ca_scraper._parse_detail_page(html, dummy)
        largest = extract_largest_dollar_amount(pr.body_text)
        assert largest is not None
        assert largest.amount >= Decimal("1000000")

    def test_real_fixture_hino_environmental(self, ca_scraper, ca_detail_htmls):
        """Test against real CA fixture: $237M Hino Motors settlement."""
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get("detail_006_environmental", "")
        if not html:
            pytest.skip("Fixture not found")
        dummy = PressReleaseListItem(title="test", url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA")
        pr = ca_scraper._parse_detail_page(html, dummy)
        largest = extract_largest_dollar_amount(pr.body_text)
        assert largest is not None
        assert largest.amount >= Decimal("100000000")


# ---------------------------------------------------------------------------
# Action Type Classification
# ---------------------------------------------------------------------------

class TestActionTypeClassification:

    def test_settlement_from_headline(self):
        assert classify_action_type("AG Announces Settlement with Company X", "") == "settlement"

    def test_lawsuit_from_headline(self):
        assert classify_action_type("AG Sues Company X for Fraud", "") == "lawsuit_filed"

    def test_files_lawsuit(self):
        assert classify_action_type("AG Files Lawsuit Against Tech Giant", "") == "lawsuit_filed"

    def test_injunction(self):
        assert classify_action_type("AG Secures Preliminary Injunction Against MV Realty", "") == "injunction"

    def test_judgment(self):
        assert classify_action_type("12 Defendants Sentenced in Fraud Scheme", "") == "judgment"

    def test_consent_decree(self):
        assert classify_action_type("AG Enters Consent Decree with Polluter", "") == "consent_decree"

    def test_other_fallback(self):
        assert classify_action_type("AG Highlights Work in 2024", "A summary of this year.") == "other"

    def test_body_text_fallback(self):
        result = classify_action_type(
            "AG Announces Action",
            "The settlement requires the company to pay $5 million.",
        )
        assert result == "settlement"

    def test_real_fixture_settlement(self, ca_scraper, ca_detail_htmls):
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get("detail_000_settlement_dollar", "")
        if not html:
            pytest.skip("Fixture not found")
        dummy = PressReleaseListItem(
            title="Attorney General Bonta Combats Medi-Cal Fraud, Securing a $10 Million Settlement",
            url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA",
        )
        pr = ca_scraper._parse_detail_page(html, dummy)
        action_type = classify_action_type(pr.title, pr.body_text)
        assert action_type == "settlement"

    def test_real_fixture_injunction(self, ca_scraper, ca_detail_htmls):
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get("detail_001_injunctive_only", "")
        if not html:
            pytest.skip("Fixture not found")
        dummy = PressReleaseListItem(
            title="Attorney General Bonta Secures Preliminary Injunction Against MV Realty",
            url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA",
        )
        pr = ca_scraper._parse_detail_page(html, dummy)
        action_type = classify_action_type(pr.title, pr.body_text)
        assert action_type == "injunction"

    def test_real_fixture_lawsuit(self, ca_scraper, ca_detail_htmls):
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get("detail_004_statute_citation", "")
        if not html:
            pytest.skip("Fixture not found")
        dummy = PressReleaseListItem(
            title="Attorney General Bonta Sues ExxonMobil for Deceiving the Public",
            url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA",
        )
        pr = ca_scraper._parse_detail_page(html, dummy)
        action_type = classify_action_type(pr.title, pr.body_text)
        assert action_type == "lawsuit_filed"


# ---------------------------------------------------------------------------
# Multistate Detection
# ---------------------------------------------------------------------------

class TestMultistateDetection:

    def test_multistate_keyword(self):
        assert is_multistate_action("", "This multistate settlement involves 42 states.")

    def test_coalition_pattern(self):
        assert is_multistate_action("AG Joins Coalition of 15 State Attorneys General", "")

    def test_not_multistate(self):
        assert not is_multistate_action("AG Settles with Local Company", "The settlement requires payment.")

    def test_real_fixture_purdue(self, ca_scraper, ca_detail_htmls):
        """The Purdue Pharma settlement is a multistate action."""
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get("detail_002_multistate", "")
        if not html:
            pytest.skip("Fixture not found")
        dummy = PressReleaseListItem(
            title="AG Helps Secure $7.4 Billion from Purdue Pharma",
            url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA",
        )
        pr = ca_scraper._parse_detail_page(html, dummy)
        assert is_multistate_action(pr.title, pr.body_text)


# ---------------------------------------------------------------------------
# Defendant Extraction
# ---------------------------------------------------------------------------

class TestDefendantExtraction:

    def test_sues_pattern(self):
        names = extract_defendants_from_headline("AG Sues ExxonMobil for Deceiving the Public")
        assert len(names) >= 1
        # _fix_headline_spacing converts "ExxonMobil" → "Exxon Mobil"
        assert any("Exxon" in n for n in names)

    def test_settlement_with_pattern(self):
        names = extract_defendants_from_headline("AG Secures Settlement with Ford for Misrepresenting Fuel Economy")
        assert len(names) >= 1
        assert any("Ford" in n for n in names)

    def test_body_defendant_pattern(self):
        text = "The complaint names defendant Acme Corporation, a Delaware-based company."
        names = extract_defendants_from_body(text)
        assert len(names) >= 1

    def test_body_settlement_pattern(self):
        text = "The settlement with R&B Medical Group, Inc. requires payment of $10 million."
        names = extract_defendants_from_body(text)
        assert len(names) >= 1


# ---------------------------------------------------------------------------
# Statute Extraction
# ---------------------------------------------------------------------------

class TestStatuteExtraction:

    def test_california_code(self):
        statutes = extract_statutes("in violation of the Business and Professions Code section 17200")
        assert len(statutes) >= 1
        assert any("Business and Professions" in s.raw_citation for s in statutes)

    def test_ccpa_common_name(self):
        statutes = extract_statutes("violated the CCPA by failing to honor opt-out requests")
        assert len(statutes) >= 1
        assert any(s.common_name == "CCPA" for s in statutes)

    def test_false_claims_act(self):
        statutes = extract_statutes("in violation of the False Claims Act")
        assert len(statutes) >= 1
        assert any("False Claims Act" in s.raw_citation for s in statutes)

    def test_real_fixture_data_privacy(self, ca_scraper, ca_detail_htmls):
        """The CCPA page should contain CCPA citations."""
        from src.validation.schemas import PressReleaseListItem
        html = ca_detail_htmls.get("detail_007_data_privacy", "")
        if not html:
            pytest.skip("Fixture not found")
        dummy = PressReleaseListItem(title="test", url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA")
        pr = ca_scraper._parse_detail_page(html, dummy)
        statutes = extract_statutes(pr.body_text)
        assert any("CCPA" in s.raw_citation or s.common_name == "CCPA" for s in statutes)


# ---------------------------------------------------------------------------
# Monetary Components
# ---------------------------------------------------------------------------

class TestMonetaryComponents:

    def test_civil_penalty(self):
        components = classify_monetary_components("The company will pay civil penalties of $2 million")
        assert "civil_penalty" in components
        assert components["civil_penalty"] == Decimal("2000000")

    def test_restitution(self):
        components = classify_monetary_components("consumer restitution of $5 million")
        assert "consumer_restitution" in components

    def test_no_components(self):
        components = classify_monetary_components("The AG filed an injunction.")
        assert len(components) == 0
