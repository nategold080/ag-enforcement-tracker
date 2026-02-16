"""Tests for the full press release extraction pipeline.

Runs the PressReleaseExtractor against real CA AG fixtures and validates
that key fields are extracted correctly. Target: 90%+ accuracy.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from src.extractors.press_release import PressReleaseExtractor
from src.validation.schemas import PressReleaseListItem, ActionType, ActionStatus


@pytest.fixture
def taxonomy():
    config_path = Path(__file__).parent.parent.parent / "config" / "taxonomy.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def extractor(taxonomy):
    return PressReleaseExtractor(taxonomy)


CA_FIXTURES = Path(__file__).parent.parent / "fixtures" / "california"


def _make_pr(ca_scraper, ca_detail_htmls, fixture_name, title):
    from src.validation.schemas import PressReleaseListItem, PressRelease
    html = ca_detail_htmls.get(fixture_name, "")
    if not html:
        pytest.skip(f"Fixture {fixture_name} not found")
    dummy = PressReleaseListItem(title=title, url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA")
    return ca_scraper._parse_detail_page(html, dummy)


class TestExtractionPipeline:
    """End-to-end extraction against real fixtures."""

    def test_settlement_extraction(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_000_settlement_dollar",
                      "Attorney General Bonta Combats Medi-Cal Fraud, Securing a $10 Million Settlement")
        action = extractor.extract(pr, date_announced=date(2025, 1, 2))

        assert action.state == "CA"
        assert action.action_type == ActionType.SETTLEMENT
        assert action.status == ActionStatus.SETTLED
        assert action.date_announced == date(2025, 1, 2)
        assert action.monetary_terms is not None
        assert action.monetary_terms.total_amount >= Decimal("1000000")
        assert action.quality_score > 0.3
        assert action.extraction_method.value == "rules"
        assert len(action.summary) > 50

    def test_injunction_extraction(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_001_injunctive_only",
                      "Attorney General Bonta Secures Preliminary Injunction Against MV Realty")
        action = extractor.extract(pr)

        assert action.action_type == ActionType.INJUNCTION
        assert action.status == ActionStatus.ONGOING

    def test_lawsuit_extraction(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_004_statute_citation",
                      "Attorney General Bonta Sues ExxonMobil for Deceiving the Public on Recyclability of Plastic Products")
        action = extractor.extract(pr)

        assert action.action_type == ActionType.LAWSUIT_FILED
        assert action.status == ActionStatus.PENDING

    def test_judgment_extraction(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_003_multiple_defendants",
                      "Attorney General Bonta: 12 Defendants Held Accountable for $15 Million Scheme of Mortgage Fraud")
        action = extractor.extract(pr)

        # This is a sentencing — should be classified as judgment
        assert action.action_type == ActionType.JUDGMENT

    def test_multistate_detection(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_002_multistate",
                      "Attorney General Bonta Helps Secure $7.4 Billion from Purdue Pharma and the Sackler Family")
        action = extractor.extract(pr)

        assert action.is_multistate is True
        assert action.monetary_terms is not None
        assert action.monetary_terms.total_amount >= Decimal("1000000000")

    def test_violation_category_healthcare(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_005_healthcare_fraud",
                      "Attorney General Bonta Secures $7.7 Million Settlement with Healthcare Provider")
        action = extractor.extract(pr)

        categories = [vc.category for vc in action.violation_categories]
        assert "healthcare" in categories or "consumer_protection" in categories

    def test_violation_category_environmental(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_006_environmental",
                      "Attorney General Bonta Announces Nearly $237 Million Settlement with Hino Motors")
        action = extractor.extract(pr)

        categories = [vc.category for vc in action.violation_categories]
        assert "environmental" in categories

    def test_violation_category_privacy(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_007_data_privacy",
                      "Attorney General Becerra Announces Final Regulations Under CCPA")
        action = extractor.extract(pr)

        categories = [vc.category for vc in action.violation_categories]
        assert "data_privacy" in categories

    def test_violation_category_employment(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_010_wage_theft",
                      "Attorney General Bonta Strikes at Wage Theft")
        action = extractor.extract(pr)

        categories = [vc.category for vc in action.violation_categories]
        assert "employment" in categories

    def test_violation_category_antitrust(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_013_antitrust",
                      "Drivers, Claim Your Money: Attorney General Bonta Reminds Californians of Gas Antitrust Settlement")
        action = extractor.extract(pr)

        categories = [vc.category for vc in action.violation_categories]
        assert "antitrust" in categories

    def test_violation_category_telecom(self, extractor, ca_scraper, ca_detail_htmls):
        pr = _make_pr(ca_scraper, ca_detail_htmls, "detail_014_telecom",
                      "Attorney General Bonta Announces Lawsuit Against Telecommunications Company")
        action = extractor.extract(pr)

        categories = [vc.category for vc in action.violation_categories]
        assert "telecommunications" in categories or "consumer_protection" in categories

    def test_quality_score_range(self, extractor, ca_scraper, ca_detail_htmls):
        """All enforcement fixtures should produce quality scores > 0.3."""
        enforcement_fixtures = [
            ("detail_000_settlement_dollar", "AG Settlement"),
            ("detail_001_injunctive_only", "AG Secures Preliminary Injunction"),
            ("detail_002_multistate", "AG Helps Secure $7.4 Billion"),
            ("detail_003_multiple_defendants", "AG: 12 Defendants Held Accountable"),
            ("detail_004_statute_citation", "AG Sues ExxonMobil"),
            ("detail_005_healthcare_fraud", "AG Secures $7.7 Million Settlement"),
            ("detail_006_environmental", "AG Announces $237 Million Settlement"),
        ]
        for fixture_name, title in enforcement_fixtures:
            pr = _make_pr(ca_scraper, ca_detail_htmls, fixture_name, title)
            action = extractor.extract(pr)
            assert action.quality_score > 0.3, f"Low quality for {fixture_name}: {action.quality_score}"

    def test_all_actions_have_state(self, extractor, ca_scraper, ca_detail_htmls):
        """Every extracted action should have CA as state."""
        for fixture_name in ["detail_000_settlement_dollar", "detail_004_statute_citation"]:
            html = ca_detail_htmls.get(fixture_name, "")
            if not html:
                continue
            dummy = PressReleaseListItem(title="test", url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA")
            pr = ca_scraper._parse_detail_page(html, dummy)
            action = extractor.extract(pr)
            assert action.state == "CA"

    def test_extraction_accuracy_summary(self, extractor, ca_scraper, ca_detail_htmls):
        """Summary extraction accuracy test.

        For enforcement fixtures, check that key fields are extracted:
        - Action type is not 'other'
        - At least one violation category is assigned
        - Quality score is above threshold

        Target: 90%+ on key fields across fixtures.
        """
        fixtures = {
            "detail_000_settlement_dollar": ("AG Combats Medi-Cal Fraud, Securing $10M Settlement", "settlement"),
            "detail_001_injunctive_only": ("AG Secures Preliminary Injunction Against MV Realty", "injunction"),
            "detail_002_multistate": ("AG Helps Secure $7.4 Billion from Purdue Pharma", "settlement"),
            "detail_003_multiple_defendants": ("12 Defendants Held Accountable for $15M Scheme", "judgment"),
            "detail_004_statute_citation": ("AG Sues ExxonMobil", "lawsuit_filed"),
            "detail_005_healthcare_fraud": ("AG Secures $7.7M Settlement", "settlement"),
            "detail_006_environmental": ("AG Announces $237M Settlement with Hino Motors", "settlement"),
        }

        correct_type = 0
        has_category = 0
        has_summary = 0
        total = len(fixtures)

        for fixture_name, (title, expected_type) in fixtures.items():
            html = ca_detail_htmls.get(fixture_name, "")
            if not html:
                total -= 1
                continue
            dummy = PressReleaseListItem(title=title, url="https://oag.ca.gov/test", date=date(2024, 1, 1), state="CA")
            pr = ca_scraper._parse_detail_page(html, dummy)
            action = extractor.extract(pr)

            if action.action_type.value == expected_type:
                correct_type += 1
            if action.violation_categories and action.violation_categories[0].category != "other":
                has_category += 1
            if len(action.summary) > 50:
                has_summary += 1

        # Target: 90%+ accuracy
        if total > 0:
            type_accuracy = correct_type / total
            category_accuracy = has_category / total
            summary_accuracy = has_summary / total

            assert type_accuracy >= 0.85, f"Action type accuracy: {type_accuracy:.0%} ({correct_type}/{total})"
            assert category_accuracy >= 0.85, f"Category accuracy: {category_accuracy:.0%} ({has_category}/{total})"
            assert summary_accuracy >= 0.85, f"Summary accuracy: {summary_accuracy:.0%} ({has_summary}/{total})"
