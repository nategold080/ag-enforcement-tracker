"""Database interface for the AG Enforcement Tracker.

Provides a simple interface for creating the database, getting sessions,
and performing common queries. Uses SQLAlchemy 2.0 with SQLite by default.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session, sessionmaker

from src.storage.models import (
    Base,
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ScrapeRun,
)

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("data/ag_enforcement.db")


class Database:
    """Manages the SQLite database connection and provides query helpers."""

    def __init__(self, db_path: Path | str | None = None, echo: bool = False):
        if db_path is None:
            db_path = DEFAULT_DB_PATH
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}", echo=echo)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self) -> None:
        """Create all tables if they don't exist."""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created at %s", self.db_path)

    def get_session(self) -> Session:
        """Return a new SQLAlchemy session."""
        return self.SessionLocal()

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def action_exists(self, source_url: str) -> bool:
        """Check if an enforcement action with this source URL already exists.

        This is the primary idempotency check — running the scraper twice
        should not create duplicate records.
        """
        with self.get_session() as session:
            result = session.execute(
                select(EnforcementAction.id).where(
                    EnforcementAction.source_url == source_url
                )
            ).scalar_one_or_none()
            return result is not None

    def get_action_count(self, state: Optional[str] = None) -> int:
        """Return the total number of enforcement actions, optionally filtered by state."""
        with self.get_session() as session:
            stmt = select(func.count(EnforcementAction.id))
            if state:
                stmt = stmt.where(EnforcementAction.state == state.upper())
            return session.execute(stmt).scalar_one()

    def get_scrape_run(self, run_id: str) -> ScrapeRun | None:
        """Look up a scrape run by ID."""
        with self.get_session() as session:
            return session.get(ScrapeRun, run_id)

    def stats(self) -> dict:
        """Return summary statistics about the database."""
        with self.get_session() as session:
            total_actions = session.execute(
                select(func.count(EnforcementAction.id))
            ).scalar_one()
            total_defendants = session.execute(
                select(func.count(Defendant.id))
            ).scalar_one()
            states_with_data = session.execute(
                select(func.count(func.distinct(EnforcementAction.state)))
            ).scalar_one()
            total_scrape_runs = session.execute(
                select(func.count(ScrapeRun.id))
            ).scalar_one()
            return {
                "total_actions": total_actions,
                "total_defendants": total_defendants,
                "states_with_data": states_with_data,
                "total_scrape_runs": total_scrape_runs,
            }
