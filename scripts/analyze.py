"""Generate an analytics/insights report from the enforcement action database.

Run: python scripts/analyze.py --output data/processed/insights_report.md
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import click
from rich.console import Console

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select, func, desc
from sqlalchemy.orm import joinedload

from src.storage.database import Database
from src.storage.models import (
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ViolationCategory,
    MonetaryTerms,
    StatuteCited,
)

console = Console()

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

CATEGORY_DISPLAY = {
    "consumer_protection": "Consumer Protection",
    "data_privacy": "Data Privacy & Security",
    "antitrust": "Antitrust",
    "healthcare": "Healthcare Fraud",
    "environmental": "Environmental",
    "securities": "Securities Fraud",
    "housing_lending": "Housing & Lending",
    "employment": "Wage & Employment",
    "telecommunications": "Telecommunications",
    "charitable": "Charitable / Nonprofit",
    "tobacco_vaping": "Tobacco & Vaping",
    "tech_platform": "Tech Platform",
    "other": "Other",
}


def generate_report(db: Database) -> str:
    """Generate a markdown insights report."""
    sections = []

    # ── Header ──
    sections.append("# AG Enforcement Tracker — Insights Report")
    sections.append(f"\n*Generated: {date.today().isoformat()}*\n")

    # ── Overview stats ──
    with db.get_session() as session:
        total_actions = session.execute(
            select(func.count(EnforcementAction.id))
        ).scalar_one()

        total_defendants = session.execute(
            select(func.count(Defendant.id))
        ).scalar_one()

        total_monetary = session.execute(
            select(func.sum(MonetaryTerms.total_amount))
        ).scalar_one() or Decimal("0")

        states_with_data = session.execute(
            select(func.count(func.distinct(EnforcementAction.state)))
        ).scalar_one()

        multistate_count = session.execute(
            select(func.count(EnforcementAction.id)).where(EnforcementAction.is_multistate == True)
        ).scalar_one()

    sections.append("## Overview\n")
    sections.append(f"| Metric | Value |")
    sections.append(f"|--------|-------|")
    sections.append(f"| Total Enforcement Actions | {total_actions:,} |")
    sections.append(f"| Total Defendants | {total_defendants:,} |")
    sections.append(f"| Total Monetary Value | ${float(total_monetary):,.0f} |")
    sections.append(f"| States Tracked | {states_with_data} |")
    sections.append(f"| Multistate Actions | {multistate_count} |")

    # ── Insight 1: Enforcement by State ──
    sections.append("\n## Insight 1: Enforcement Activity by State\n")

    with db.get_session() as session:
        by_state = session.execute(
            select(
                EnforcementAction.state,
                func.count(EnforcementAction.id).label("count"),
            )
            .group_by(EnforcementAction.state)
            .order_by(desc("count"))
        ).all()

    sections.append("| State | Actions | Share |")
    sections.append("|-------|---------|-------|")
    for state_code, count in by_state:
        name = STATE_NAMES.get(state_code, state_code)
        pct = (count / total_actions * 100) if total_actions else 0
        sections.append(f"| {name} ({state_code}) | {count} | {pct:.1f}% |")

    top_state = by_state[0] if by_state else None
    if top_state:
        sections.append(
            f"\n**{STATE_NAMES.get(top_state[0], top_state[0])}** leads with "
            f"{top_state[1]} enforcement actions ({top_state[1]/total_actions*100:.0f}% of total)."
        )

    # ── Insight 2: Settlement Amounts ──
    sections.append("\n## Insight 2: Largest Settlements\n")

    with db.get_session() as session:
        top_settlements = session.execute(
            select(
                EnforcementAction.headline,
                EnforcementAction.state,
                EnforcementAction.date_announced,
                MonetaryTerms.total_amount,
            )
            .join(MonetaryTerms)
            .where(MonetaryTerms.total_amount > 0)
            .order_by(desc(MonetaryTerms.total_amount))
            .limit(10)
        ).all()

    sections.append("| # | Settlement | State | Date | Amount |")
    sections.append("|---|-----------|-------|------|--------|")
    for i, (headline, state, dt, amount) in enumerate(top_settlements, 1):
        amt = float(amount)
        if amt >= 1e9:
            amt_str = f"${amt/1e9:.2f}B"
        elif amt >= 1e6:
            amt_str = f"${amt/1e6:.1f}M"
        else:
            amt_str = f"${amt:,.0f}"
        sections.append(f"| {i} | {headline[:60]}{'...' if len(headline)>60 else ''} | {state} | {dt} | {amt_str} |")

    if top_settlements:
        top_amt = float(top_settlements[0][3])
        median_idx = len(top_settlements) // 2
        median_amt = float(top_settlements[median_idx][3]) if top_settlements else 0
        sections.append(
            f"\nThe largest single settlement is **{_format_amount(top_amt)}**. "
            f"The median of the top 10 is **{_format_amount(median_amt)}**."
        )

    # ── Insight 3: Violation Categories ──
    sections.append("\n## Insight 3: Most Common Violation Categories\n")

    with db.get_session() as session:
        by_category = session.execute(
            select(
                ViolationCategory.category,
                func.count(ViolationCategory.id).label("count"),
            )
            .group_by(ViolationCategory.category)
            .order_by(desc("count"))
        ).all()

    sections.append("| Category | Actions | Share |")
    sections.append("|----------|---------|-------|")
    total_cats = sum(c for _, c in by_category)
    for cat, count in by_category:
        display = CATEGORY_DISPLAY.get(cat, cat)
        pct = (count / total_cats * 100) if total_cats else 0
        sections.append(f"| {display} | {count} | {pct:.1f}% |")

    if by_category:
        top_cat = CATEGORY_DISPLAY.get(by_category[0][0], by_category[0][0])
        sections.append(
            f"\n**{top_cat}** is the most common enforcement category, appearing in "
            f"{by_category[0][1]} categorizations ({by_category[0][1]/total_cats*100:.0f}% of all labels)."
        )

    # ── Insight 4: Action Types ──
    sections.append("\n## Insight 4: Action Type Distribution\n")

    with db.get_session() as session:
        by_type = session.execute(
            select(
                EnforcementAction.action_type,
                func.count(EnforcementAction.id).label("count"),
            )
            .group_by(EnforcementAction.action_type)
            .order_by(desc("count"))
        ).all()

    sections.append("| Action Type | Count | Share |")
    sections.append("|-------------|-------|-------|")
    for atype, count in by_type:
        pct = (count / total_actions * 100) if total_actions else 0
        sections.append(f"| {atype.replace('_', ' ').title()} | {count} | {pct:.1f}% |")

    settlements = next((c for t, c in by_type if t == "settlement"), 0)
    lawsuits = next((c for t, c in by_type if t == "lawsuit_filed"), 0)
    if settlements and lawsuits:
        ratio = settlements / lawsuits if lawsuits else 0
        sections.append(
            f"\nSettlements outnumber lawsuits filed by **{ratio:.1f}x**, "
            f"suggesting most AG enforcement resolves through negotiated settlements."
        )

    # ── Insight 5: Cross-State Defendant Activity ──
    sections.append("\n## Insight 5: Defendants Facing Actions in Multiple States\n")

    with db.get_session() as session:
        multi_state_defendants = session.execute(
            select(
                Defendant.canonical_name,
                func.count(func.distinct(EnforcementAction.state)).label("state_count"),
                func.count(ActionDefendant.action_id).label("action_count"),
                func.group_concat(func.distinct(EnforcementAction.state)).label("states"),
            )
            .select_from(Defendant)
            .join(ActionDefendant, ActionDefendant.defendant_id == Defendant.id)
            .join(EnforcementAction, EnforcementAction.id == ActionDefendant.action_id)
            .where(Defendant.canonical_name != "")
            .group_by(Defendant.canonical_name)
            .having(func.count(func.distinct(EnforcementAction.state)) > 1)
            .order_by(desc("state_count"), desc("action_count"))
        ).all()

    if multi_state_defendants:
        sections.append("| Defendant | States | Actions | States Involved |")
        sections.append("|-----------|--------|---------|-----------------|")
        for name, state_count, action_count, states in multi_state_defendants:
            sections.append(f"| {name} | {state_count} | {action_count} | {states} |")
        sections.append(
            f"\n**{len(multi_state_defendants)} defendants** face enforcement actions in multiple states, "
            f"indicating cross-jurisdictional enforcement patterns."
        )
    else:
        sections.append("No defendants currently appear in actions across multiple states in our dataset.")

    # ── Insight 6: Monetary Recovery by Category ──
    sections.append("\n## Insight 6: Monetary Recovery by Violation Category\n")

    with db.get_session() as session:
        cat_amounts = session.execute(
            select(
                ViolationCategory.category,
                func.sum(MonetaryTerms.total_amount).label("total"),
                func.count(func.distinct(EnforcementAction.id)).label("count"),
            )
            .select_from(ViolationCategory)
            .join(EnforcementAction, EnforcementAction.id == ViolationCategory.action_id)
            .join(MonetaryTerms, MonetaryTerms.action_id == EnforcementAction.id)
            .where(MonetaryTerms.total_amount > 0)
            .group_by(ViolationCategory.category)
            .order_by(desc("total"))
        ).all()

    if cat_amounts:
        sections.append("| Category | Total Amount | Actions | Avg per Action |")
        sections.append("|----------|-------------|---------|----------------|")
        for cat, total, count in cat_amounts:
            display = CATEGORY_DISPLAY.get(cat, cat)
            avg = float(total) / count if count else 0
            sections.append(
                f"| {display} | {_format_amount(float(total))} | {count} | {_format_amount(avg)} |"
            )

    # ── Insight 7: Quality Distribution ──
    sections.append("\n## Insight 7: Data Quality Distribution\n")

    with db.get_session() as session:
        quality_dist = session.execute(
            select(
                func.round(EnforcementAction.quality_score, 1).label("bucket"),
                func.count(EnforcementAction.id).label("count"),
            )
            .group_by("bucket")
            .order_by("bucket")
        ).all()

        avg_quality = session.execute(
            select(func.avg(EnforcementAction.quality_score))
        ).scalar_one() or 0

    sections.append("| Quality Score | Count |")
    sections.append("|--------------|-------|")
    for bucket, count in quality_dist:
        sections.append(f"| {bucket:.1f} | {count} |")

    sections.append(f"\nAverage quality score: **{float(avg_quality):.2f}** (1.0 = perfect extraction)")

    # ── Footer ──
    sections.append("\n---")
    sections.append(
        "\n*Report generated by AG Enforcement Tracker analytics pipeline. "
        "Data sourced from official state Attorney General websites.*"
    )

    return "\n".join(sections)


def _format_amount(amount: float) -> str:
    if amount >= 1e9:
        return f"${amount/1e9:.2f}B"
    elif amount >= 1e6:
        return f"${amount/1e6:.1f}M"
    elif amount >= 1e3:
        return f"${amount:,.0f}"
    else:
        return f"${amount:,.2f}"


@click.command()
@click.option("--output", "-o", "output_path", default="data/processed/insights_report.md", help="Output file path.")
@click.option("--db", "db_path", default="data/ag_enforcement.db", help="Database path.")
def main(output_path, db_path):
    """Generate analytics insights report."""
    db = Database(db_path)
    db.create_tables()

    report = generate_report(db)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")

    console.print(f"Report written to [cyan]{out}[/cyan]")
    console.print(f"({len(report.splitlines())} lines)")


if __name__ == "__main__":
    main()
