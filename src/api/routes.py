"""API endpoints for searching, filtering, and exporting enforcement actions."""

from __future__ import annotations

import csv
import io
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import Integer, select, func, desc, and_, case
from sqlalchemy.orm import Session, joinedload

from src.storage.database import Database
from src.storage.models import (
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ViolationCategory,
    MonetaryTerms,
    StatuteCited,
)

router = APIRouter()

# Module-level database reference, set via configure_db() or overridden in tests.
_db: Database | None = None


def _escape_like(value: str) -> str:
    """Escape special SQL LIKE characters."""
    return value.replace("%", r"\%").replace("_", r"\_")


def configure_db(db: Database | None) -> None:
    """Set the database instance used by all routes."""
    global _db
    _db = db


def _get_db() -> Database:
    """Return the Database singleton, creating it if needed."""
    global _db
    if _db is None:
        _db = Database()
        _db.create_tables()
    return _db


def get_db_session() -> Generator[Session, None, None]:
    """FastAPI dependency: yield a database session, close it after the request."""
    session = _get_db().get_session()
    try:
        yield session
    finally:
        session.close()


# ── Actions endpoints ─────────────────────────────────────────────────────


@router.get("/actions")
def list_actions(
    session: Session = Depends(get_db_session),
    state: Optional[str] = Query(None, description="Filter by state code (e.g., CA)"),
    category: Optional[str] = Query(None, description="Filter by violation category"),
    action_type: Optional[str] = Query(None, description="Filter by action type"),
    defendant: Optional[str] = Query(None, description="Search defendant name"),
    since: Optional[date] = Query(None, description="Actions on or after this date"),
    until: Optional[date] = Query(None, description="Actions on or before this date"),
    min_amount: Optional[float] = Query(None, description="Minimum settlement amount"),
    q: Optional[str] = Query(None, description="Full-text search in headlines"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List enforcement actions with filtering."""
    stmt = (
        select(EnforcementAction)
        .options(
            joinedload(EnforcementAction.action_defendants).joinedload(ActionDefendant.defendant),
            joinedload(EnforcementAction.violation_categories),
            joinedload(EnforcementAction.monetary_terms),
        )
    )

    if state:
        stmt = stmt.where(EnforcementAction.state == state.upper())
    if action_type:
        stmt = stmt.where(EnforcementAction.action_type == action_type)
    if since:
        stmt = stmt.where(EnforcementAction.date_announced >= since)
    if until:
        stmt = stmt.where(EnforcementAction.date_announced <= until)
    if q:
        stmt = stmt.where(EnforcementAction.headline.ilike(f"%{_escape_like(q)}%", escape="\\"))
    if category:
        stmt = stmt.join(ViolationCategory).where(ViolationCategory.category == category)
    if defendant:
        stmt = (
            stmt.join(ActionDefendant)
            .join(Defendant)
            .where(
                Defendant.canonical_name.ilike(f"%{_escape_like(defendant)}%", escape="\\")
                | Defendant.raw_name.ilike(f"%{_escape_like(defendant)}%", escape="\\")
            )
        )
    if min_amount:
        stmt = stmt.join(MonetaryTerms).where(
            MonetaryTerms.total_amount >= Decimal(str(min_amount))
        )

    stmt = stmt.order_by(desc(EnforcementAction.date_announced))
    stmt = stmt.offset(offset).limit(limit)

    actions = session.execute(stmt).unique().scalars().all()

    return {
        "count": len(actions),
        "offset": offset,
        "limit": limit,
        "results": [_serialize_action(a) for a in actions],
    }


@router.get("/actions/{action_id}")
def get_action(action_id: str, session: Session = Depends(get_db_session)):
    """Get a single enforcement action by ID."""
    action = session.execute(
        select(EnforcementAction)
        .options(
            joinedload(EnforcementAction.action_defendants).joinedload(ActionDefendant.defendant),
            joinedload(EnforcementAction.violation_categories),
            joinedload(EnforcementAction.monetary_terms),
            joinedload(EnforcementAction.statutes_cited),
        )
        .where(EnforcementAction.id == action_id)
    ).unique().scalar_one_or_none()

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    return _serialize_action(action, include_body=True)


# ── Analytics endpoints ───────────────────────────────────────────────────


@router.get("/stats")
def get_stats(session: Session = Depends(get_db_session)):
    """Summary statistics for the entire dataset."""
    total = session.execute(select(func.count(EnforcementAction.id))).scalar_one()
    total_defendants = session.execute(select(func.count(Defendant.id))).scalar_one()

    # By state
    by_state = session.execute(
        select(
            EnforcementAction.state,
            func.count(EnforcementAction.id),
        )
        .group_by(EnforcementAction.state)
        .order_by(desc(func.count(EnforcementAction.id)))
    ).all()

    # By action type
    by_type = session.execute(
        select(
            EnforcementAction.action_type,
            func.count(EnforcementAction.id),
        )
        .group_by(EnforcementAction.action_type)
    ).all()

    # By category
    by_category = session.execute(
        select(
            ViolationCategory.category,
            func.count(ViolationCategory.id),
        )
        .group_by(ViolationCategory.category)
        .order_by(desc(func.count(ViolationCategory.id)))
    ).all()

    # Total monetary
    total_monetary = session.execute(
        select(func.sum(MonetaryTerms.total_amount))
    ).scalar_one() or 0

    # Top defendants by action count
    top_defendants = session.execute(
        select(
            Defendant.canonical_name,
            func.count(ActionDefendant.action_id).label("count"),
        )
        .join(ActionDefendant)
        .where(Defendant.canonical_name != "")
        .group_by(Defendant.canonical_name)
        .order_by(desc("count"))
        .limit(20)
    ).all()

    return {
        "total_actions": total,
        "total_defendants": total_defendants,
        "total_monetary_value": float(total_monetary),
        "by_state": [{"state": s, "count": c} for s, c in by_state],
        "by_action_type": [{"type": t, "count": c} for t, c in by_type],
        "by_category": [{"category": cat, "count": c} for cat, c in by_category],
        "top_defendants": [{"name": n, "count": c} for n, c in top_defendants],
    }


@router.get("/stats/timeline")
def get_timeline(
    session: Session = Depends(get_db_session),
    state: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    granularity: str = Query("month", pattern="^(month|quarter|year)$"),
):
    """Actions over time, grouped by month/quarter/year."""
    if granularity == "month":
        date_expr = func.strftime("%Y-%m", EnforcementAction.date_announced)
    elif granularity == "quarter":
        # SQLite has no %Q — compute quarter from month
        month_expr = func.cast(
            func.strftime("%m", EnforcementAction.date_announced), Integer
        )
        quarter_expr = case(
            (month_expr.in_([1, 2, 3]), "Q1"),
            (month_expr.in_([4, 5, 6]), "Q2"),
            (month_expr.in_([7, 8, 9]), "Q3"),
            else_="Q4",
        )
        date_expr = func.strftime("%Y", EnforcementAction.date_announced) + "-" + quarter_expr
    else:
        date_expr = func.strftime("%Y", EnforcementAction.date_announced)

    stmt = select(
        date_expr.label("period"),
        func.count(EnforcementAction.id).label("count"),
    )

    if state:
        stmt = stmt.where(EnforcementAction.state == state.upper())
    if category:
        stmt = stmt.join(ViolationCategory).where(ViolationCategory.category == category)

    stmt = stmt.group_by("period").order_by("period")
    rows = session.execute(stmt).all()

    return [{"period": p, "count": c} for p, c in rows]


@router.get("/states")
def list_states(session: Session = Depends(get_db_session)):
    """List all states with data."""
    rows = session.execute(
        select(
            EnforcementAction.state,
            func.count(EnforcementAction.id).label("count"),
            func.sum(MonetaryTerms.total_amount).label("total_amount"),
        )
        .outerjoin(MonetaryTerms)
        .group_by(EnforcementAction.state)
        .order_by(desc("count"))
    ).all()

    return [
        {"state": s, "count": c, "total_amount": float(a or 0)}
        for s, c, a in rows
    ]


# ── Export endpoint ───────────────────────────────────────────────────────


@router.get("/export/csv")
def export_csv(
    session: Session = Depends(get_db_session),
    state: Optional[str] = Query(None),
    since: Optional[date] = Query(None),
):
    """Export enforcement actions as CSV."""
    stmt = (
        select(EnforcementAction)
        .options(
            joinedload(EnforcementAction.action_defendants).joinedload(ActionDefendant.defendant),
            joinedload(EnforcementAction.violation_categories),
            joinedload(EnforcementAction.monetary_terms),
        )
        .order_by(desc(EnforcementAction.date_announced))
    )
    if state:
        stmt = stmt.where(EnforcementAction.state == state.upper())
    if since:
        stmt = stmt.where(EnforcementAction.date_announced >= since)

    actions = session.execute(stmt).unique().scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "state", "date_announced", "action_type", "status",
        "headline", "defendants", "total_amount", "categories",
        "is_multistate", "quality_score", "source_url",
    ])

    for a in actions:
        defendants = ", ".join(
            ad.defendant.canonical_name or ad.defendant.raw_name
            for ad in a.action_defendants
        )
        cats = ", ".join(vc.category for vc in a.violation_categories)
        amount = float(a.monetary_terms.total_amount) if a.monetary_terms else ""

        writer.writerow([
            a.id, a.state, a.date_announced, a.action_type, a.status,
            a.headline, defendants, amount, cats,
            a.is_multistate, a.quality_score, a.source_url,
        ])

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ag_enforcement_actions.csv"},
    )


# ── Serialization helpers ─────────────────────────────────────────────────


def _serialize_action(action: EnforcementAction, include_body: bool = False) -> dict:
    """Serialize an EnforcementAction to a JSON-friendly dict."""
    result = {
        "id": action.id,
        "state": action.state,
        "date_announced": str(action.date_announced),
        "action_type": action.action_type,
        "status": action.status,
        "headline": action.headline,
        "summary": action.summary,
        "source_url": action.source_url,
        "is_multistate": action.is_multistate,
        "quality_score": action.quality_score,
        "defendants": [
            {
                "raw_name": ad.defendant.raw_name,
                "canonical_name": ad.defendant.canonical_name,
                "role": ad.role,
            }
            for ad in action.action_defendants
        ],
        "categories": [
            {
                "category": vc.category,
                "subcategory": vc.subcategory,
                "confidence": vc.confidence,
            }
            for vc in action.violation_categories
        ],
        "monetary_terms": None,
    }

    if action.monetary_terms:
        mt = action.monetary_terms
        result["monetary_terms"] = {
            "total_amount": float(mt.total_amount),
            "civil_penalty": float(mt.civil_penalty) if mt.civil_penalty else None,
            "consumer_restitution": float(mt.consumer_restitution) if mt.consumer_restitution else None,
            "fees_and_costs": float(mt.fees_and_costs) if mt.fees_and_costs else None,
            "amount_is_estimated": mt.amount_is_estimated,
        }

    if include_body:
        result["raw_text"] = action.raw_text
        result["statutes_cited"] = [
            {
                "statute_raw": s.statute_raw,
                "statute_name": s.statute_name,
                "is_state_statute": s.is_state_statute,
                "is_federal_statute": s.is_federal_statute,
            }
            for s in action.statutes_cited
        ]

    return result
