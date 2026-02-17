"""SQLAlchemy 2.0 ORM models for the AG Enforcement Tracker.

These map 1:1 to the schema defined in CLAUDE.md.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Helper for UUID primary keys stored as strings in SQLite
# ---------------------------------------------------------------------------

def _uuid_default() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Core tables
# ---------------------------------------------------------------------------

class MultistateAction(Base):
    __tablename__ = "multistate_actions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid_default)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    lead_state: Mapped[str | None] = mapped_column(String(2), nullable=True)
    participating_states: Mapped[dict | list] = mapped_column(JSON, default=list)
    total_settlement: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)

    actions: Mapped[list[EnforcementAction]] = relationship(back_populates="multistate_action")


class EnforcementAction(Base):
    __tablename__ = "enforcement_actions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid_default)
    state: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    date_announced: Mapped[date] = mapped_column(Date, nullable=False)
    date_filed: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_resolved: Mapped[date | None] = mapped_column(Date, nullable=True)
    action_type: Mapped[str] = mapped_column(
        String(40), nullable=False, default="other",
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="announced",
    )
    headline: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="")
    source_url: Mapped[str] = mapped_column(String(2048), nullable=False, unique=True)
    settlement_doc_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    is_multistate: Mapped[bool] = mapped_column(Boolean, default=False)
    is_federal_litigation: Mapped[bool] = mapped_column(Boolean, default=False)
    multistate_action_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("multistate_actions.id"), nullable=True,
    )
    quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    extraction_method: Mapped[str] = mapped_column(String(10), default="rules")
    raw_text: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    multistate_action: Mapped[MultistateAction | None] = relationship(back_populates="actions")
    action_defendants: Mapped[list[ActionDefendant]] = relationship(
        back_populates="action", cascade="all, delete-orphan",
    )
    violation_categories: Mapped[list[ViolationCategory]] = relationship(
        back_populates="action", cascade="all, delete-orphan",
    )
    monetary_terms: Mapped[MonetaryTerms | None] = relationship(
        back_populates="action", uselist=False, cascade="all, delete-orphan",
    )
    statutes_cited: Mapped[list[StatuteCited]] = relationship(
        back_populates="action", cascade="all, delete-orphan",
    )


class Defendant(Base):
    __tablename__ = "defendants"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid_default)
    raw_name: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_name: Mapped[str] = mapped_column(Text, default="")
    entity_type: Mapped[str] = mapped_column(
        String(30), nullable=False, default="corporation",
    )
    industry: Mapped[str | None] = mapped_column(String(200), nullable=True)
    parent_company: Mapped[str | None] = mapped_column(String(500), nullable=True)
    sec_cik: Mapped[str | None] = mapped_column(String(20), nullable=True)

    action_defendants: Mapped[list[ActionDefendant]] = relationship(back_populates="defendant")


class ActionDefendant(Base):
    __tablename__ = "action_defendants"

    action_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("enforcement_actions.id"), primary_key=True,
    )
    defendant_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("defendants.id"), primary_key=True,
    )
    role: Mapped[str] = mapped_column(
        String(20), nullable=False, default="primary",
    )

    action: Mapped[EnforcementAction] = relationship(back_populates="action_defendants")
    defendant: Mapped[Defendant] = relationship(back_populates="action_defendants")


class ViolationCategory(Base):
    __tablename__ = "violation_categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("enforcement_actions.id"), nullable=False, index=True,
    )
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    subcategory: Mapped[str | None] = mapped_column(String(200), nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)

    action: Mapped[EnforcementAction] = relationship(back_populates="violation_categories")


class MonetaryTerms(Base):
    __tablename__ = "monetary_terms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("enforcement_actions.id"), nullable=False, unique=True,
    )
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0"))
    civil_penalty: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    consumer_restitution: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    fees_and_costs: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    other_monetary: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    amount_is_estimated: Mapped[bool] = mapped_column(Boolean, default=False)

    action: Mapped[EnforcementAction] = relationship(back_populates="monetary_terms")


class StatuteCited(Base):
    __tablename__ = "statutes_cited"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("enforcement_actions.id"), nullable=False, index=True,
    )
    statute_raw: Mapped[str] = mapped_column(Text, nullable=False)
    statute_normalized: Mapped[str] = mapped_column(Text, default="")
    statute_name: Mapped[str] = mapped_column(String(200), default="")
    is_state_statute: Mapped[bool] = mapped_column(Boolean, default=False)
    is_federal_statute: Mapped[bool] = mapped_column(Boolean, default=False)

    action: Mapped[EnforcementAction] = relationship(back_populates="statutes_cited")


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid_default)
    state: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    press_releases_found: Mapped[int] = mapped_column(Integer, default=0)
    actions_extracted: Mapped[int] = mapped_column(Integer, default=0)
    errors: Mapped[int] = mapped_column(Integer, default=0)
    error_details: Mapped[dict | list] = mapped_column(JSON, default=list)
