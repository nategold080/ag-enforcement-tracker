"""Tests for Pydantic schemas."""

import datetime as _dt
import uuid
from decimal import Decimal

import pytest

from src.validation.schemas import (
    ActionType,
    ActionStatus,
    DefendantSchema,
    EnforcementActionSchema,
    MonetaryTermsSchema,
    PressRelease,
    PressReleaseListItem,
    ScrapeRunSchema,
    StatuteCitedSchema,
    ViolationCategorySchema,
)


class TestPressReleaseListItem:

    def test_creates_with_date(self):
        item = PressReleaseListItem(
            title="Test Title",
            url="https://example.com/test",
            date=_dt.date(2024, 1, 1),
            state="CA",
        )
        assert item.date == _dt.date(2024, 1, 1)

    def test_creates_without_date(self):
        item = PressReleaseListItem(
            title="Test Title",
            url="https://example.com/test",
            state="CA",
        )
        assert item.date is None

    def test_state_uppercased(self):
        item = PressReleaseListItem(
            title="Test", url="https://example.com", state="ca",
        )
        assert item.state == "CA"

    def test_state_too_short_fails(self):
        with pytest.raises(Exception):
            PressReleaseListItem(
                title="Test", url="https://example.com", state="C",
            )


class TestEnforcementActionSchema:

    def test_creates_with_required_fields(self):
        action = EnforcementActionSchema(
            state="CA",
            date_announced=_dt.date(2024, 6, 15),
            headline="AG Files Lawsuit",
            source_url="https://example.com/press-release",
        )
        assert action.state == "CA"
        assert action.action_type == ActionType.OTHER
        assert action.status == ActionStatus.ANNOUNCED
        assert action.quality_score == 0.0
        assert isinstance(action.id, uuid.UUID)

    def test_defaults_for_optional_fields(self):
        action = EnforcementActionSchema(
            state="NY",
            date_announced=_dt.date(2024, 1, 1),
            headline="Test",
            source_url="https://example.com",
        )
        assert action.date_filed is None
        assert action.date_resolved is None
        assert action.settlement_doc_url is None
        assert action.is_multistate is False
        assert action.defendants == []
        assert action.statutes_cited == []

    def test_state_uppercased(self):
        action = EnforcementActionSchema(
            state="ca",
            date_announced=_dt.date(2024, 1, 1),
            headline="Test",
            source_url="https://example.com",
        )
        assert action.state == "CA"

    def test_quality_score_range(self):
        with pytest.raises(Exception):
            EnforcementActionSchema(
                state="CA",
                date_announced=_dt.date(2024, 1, 1),
                headline="Test",
                source_url="https://example.com",
                quality_score=1.5,
            )


class TestDefendantSchema:

    def test_creates_with_raw_name(self):
        d = DefendantSchema(raw_name="Acme Corp")
        assert d.raw_name == "Acme Corp"
        assert d.canonical_name == ""
        assert isinstance(d.id, uuid.UUID)

    def test_empty_raw_name_fails(self):
        with pytest.raises(Exception):
            DefendantSchema(raw_name="")


class TestMonetaryTermsSchema:

    def test_creates_with_amount(self):
        mt = MonetaryTermsSchema(
            action_id=uuid.uuid4(),
            total_amount=Decimal("3500000"),
            civil_penalty=Decimal("1000000"),
        )
        assert mt.total_amount == Decimal("3500000")
        assert mt.amount_is_estimated is False
