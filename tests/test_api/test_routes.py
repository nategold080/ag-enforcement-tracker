"""Tests for the FastAPI API routes.

Tests the action listing, detail, stats, and export endpoints using
FastAPI's TestClient with a temporary SQLite database.
"""

import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.api.server import app
from src.api.routes import configure_db
from src.storage.database import Database
from src.storage.models import (
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ViolationCategory,
    MonetaryTerms,
)


@pytest.fixture
def test_db(tmp_path):
    """Create a temp file database with sample data."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)
    db.create_tables()

    with db.get_session() as session:
        action = EnforcementAction(
            id="test-action-1",
            state="CA",
            date_announced=date(2024, 6, 15),
            action_type="settlement",
            status="settled",
            headline="AG Settles with Test Corp for $5 Million",
            source_url="https://example.com/test-action-1",
            quality_score=0.8,
            is_multistate=False,
            raw_text="The AG settled with Test Corp for $5 million.",
        )
        session.add(action)

        defendant = Defendant(
            id="test-defendant-1",
            raw_name="Test Corp Inc.",
            canonical_name="Test Corp",
            entity_type="corporation",
        )
        session.add(defendant)
        session.flush()

        session.add(ActionDefendant(
            action_id="test-action-1",
            defendant_id="test-defendant-1",
            role="primary",
        ))

        session.add(ViolationCategory(
            action_id="test-action-1",
            category="consumer_protection",
            subcategory="Deceptive Business Practices",
            confidence=0.9,
        ))

        session.add(MonetaryTerms(
            action_id="test-action-1",
            total_amount=Decimal("5000000"),
            civil_penalty=Decimal("2000000"),
        ))

        action2 = EnforcementAction(
            id="test-action-2",
            state="NY",
            date_announced=date(2024, 7, 20),
            action_type="lawsuit_filed",
            status="announced",
            headline="AG Sues Another Company for Fraud",
            source_url="https://example.com/test-action-2",
            quality_score=0.6,
            is_multistate=False,
        )
        session.add(action2)
        session.commit()

    return db


@pytest.fixture
def client(test_db):
    """Create a test client with the test database injected."""
    configure_db(test_db)
    with TestClient(app) as c:
        yield c
    configure_db(None)


class TestListActions:
    def test_list_returns_200(self, client):
        resp = client.get("/api/v1/actions")
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert data["count"] == 2

    def test_filter_by_state(self, client):
        resp = client.get("/api/v1/actions?state=CA")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["results"][0]["state"] == "CA"

    def test_filter_by_action_type(self, client):
        resp = client.get("/api/v1/actions?action_type=settlement")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1

    def test_filter_by_date(self, client):
        resp = client.get("/api/v1/actions?since=2024-07-01")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["results"][0]["state"] == "NY"

    def test_search_headline(self, client):
        resp = client.get("/api/v1/actions?q=Fraud")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1

    def test_pagination(self, client):
        resp = client.get("/api/v1/actions?limit=1&offset=0")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["limit"] == 1
        assert data["offset"] == 0


class TestGetAction:
    def test_existing_action_returns_200(self, client):
        resp = client.get("/api/v1/actions/test-action-1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == "test-action-1"
        assert data["state"] == "CA"

    def test_nonexistent_action_returns_404(self, client):
        """P7 regression test: should return 404, not 200."""
        resp = client.get("/api/v1/actions/nonexistent-id")
        assert resp.status_code == 404

    def test_action_includes_defendants(self, client):
        resp = client.get("/api/v1/actions/test-action-1")
        data = resp.json()
        assert len(data["defendants"]) == 1
        assert data["defendants"][0]["canonical_name"] == "Test Corp"

    def test_action_includes_monetary_terms(self, client):
        resp = client.get("/api/v1/actions/test-action-1")
        data = resp.json()
        assert data["monetary_terms"] is not None
        assert data["monetary_terms"]["total_amount"] == 5000000.0


class TestStats:
    def test_stats_returns_200(self, client):
        resp = client.get("/api/v1/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_actions"] == 2
        assert data["total_defendants"] == 1


class TestExportCSV:
    def test_csv_export_returns_200(self, client):
        resp = client.get("/api/v1/export/csv")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        lines = resp.text.strip().split("\n")
        assert len(lines) >= 2  # Header + at least 1 data row

    def test_csv_export_filter_by_state(self, client):
        resp = client.get("/api/v1/export/csv?state=CA")
        assert resp.status_code == 200
        lines = resp.text.strip().split("\n")
        assert len(lines) == 2  # Header + 1 CA record
