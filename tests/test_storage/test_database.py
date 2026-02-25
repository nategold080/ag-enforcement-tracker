"""Tests for database operations.

Tests upsert idempotency, search, and dedup key (source_url uniqueness).
"""

from datetime import date

import pytest
from sqlalchemy.exc import IntegrityError

from src.storage.database import Database
from src.storage.models import EnforcementAction, Defendant, ActionDefendant, ScrapeRun


@pytest.fixture
def db():
    """Return a fresh in-memory database."""
    d = Database(":memory:")
    d.create_tables()
    return d


class TestTableCreation:
    def test_tables_created(self, db):
        """All tables should be created without error."""
        stats = db.stats()
        assert stats["total_actions"] == 0
        assert stats["total_defendants"] == 0
        assert stats["total_scrape_runs"] == 0


class TestActionExists:
    def test_nonexistent_url(self, db):
        assert not db.action_exists("https://example.com/does-not-exist")

    def test_existing_url(self, db):
        with db.get_session() as session:
            session.add(EnforcementAction(
                state="CA",
                date_announced=date(2024, 1, 1),
                headline="Test",
                source_url="https://example.com/test-1",
            ))
            session.commit()

        assert db.action_exists("https://example.com/test-1")


class TestIdempotency:
    def test_duplicate_source_url_rejected(self, db):
        """Inserting a second action with the same source_url should fail (unique constraint)."""
        with db.get_session() as session:
            session.add(EnforcementAction(
                state="CA",
                date_announced=date(2024, 1, 1),
                headline="Test",
                source_url="https://example.com/test-dup",
            ))
            session.commit()

        with pytest.raises(IntegrityError):
            with db.get_session() as session:
                session.add(EnforcementAction(
                    state="NY",
                    date_announced=date(2024, 2, 1),
                    headline="Different",
                    source_url="https://example.com/test-dup",  # same URL
                ))
                session.commit()


class TestGetActionCount:
    def test_empty_db(self, db):
        assert db.get_action_count() == 0

    def test_count_all(self, db):
        with db.get_session() as session:
            session.add(EnforcementAction(
                state="CA", date_announced=date(2024, 1, 1),
                headline="A", source_url="https://a.com",
            ))
            session.add(EnforcementAction(
                state="NY", date_announced=date(2024, 1, 1),
                headline="B", source_url="https://b.com",
            ))
            session.commit()

        assert db.get_action_count() == 2

    def test_count_by_state(self, db):
        with db.get_session() as session:
            session.add(EnforcementAction(
                state="CA", date_announced=date(2024, 1, 1),
                headline="A", source_url="https://a.com",
            ))
            session.add(EnforcementAction(
                state="NY", date_announced=date(2024, 1, 1),
                headline="B", source_url="https://b.com",
            ))
            session.commit()

        assert db.get_action_count("CA") == 1
        assert db.get_action_count("NY") == 1
        assert db.get_action_count("TX") == 0


class TestScrapeRun:
    def test_create_and_retrieve(self, db):
        with db.get_session() as session:
            run = ScrapeRun(state="CA", press_releases_found=10, errors=2)
            session.add(run)
            session.commit()
            run_id = run.id

        result = db.get_scrape_run(run_id)
        assert result is not None
        assert result.state == "CA"
        assert result.press_releases_found == 10

    def test_stats_counts_runs(self, db):
        with db.get_session() as session:
            session.add(ScrapeRun(state="CA"))
            session.add(ScrapeRun(state="NY"))
            session.commit()

        stats = db.stats()
        assert stats["total_scrape_runs"] == 2


class TestRelationships:
    def test_action_defendant_relationship(self, db):
        with db.get_session() as session:
            action = EnforcementAction(
                id="act-1", state="CA", date_announced=date(2024, 1, 1),
                headline="Test", source_url="https://test.com",
            )
            defendant = Defendant(
                id="def-1", raw_name="Acme Corp", canonical_name="Acme",
            )
            session.add(action)
            session.add(defendant)
            session.flush()
            session.add(ActionDefendant(
                action_id="act-1", defendant_id="def-1", role="primary",
            ))
            session.commit()

        with db.get_session() as session:
            from sqlalchemy import select
            from sqlalchemy.orm import joinedload
            act = session.execute(
                select(EnforcementAction)
                .options(joinedload(EnforcementAction.action_defendants).joinedload(ActionDefendant.defendant))
                .where(EnforcementAction.id == "act-1")
            ).unique().scalar_one()
            assert len(act.action_defendants) == 1
            assert act.action_defendants[0].defendant.canonical_name == "Acme"
