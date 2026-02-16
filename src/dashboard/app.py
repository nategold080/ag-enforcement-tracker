"""Streamlit dashboard for the AG Enforcement Tracker.

Polished, screenshot-ready dashboard for outreach and demos.

Run: streamlit run src/dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from decimal import Decimal

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import select, func, desc, distinct, and_

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.storage.database import Database
from src.storage.models import (
    EnforcementAction,
    Defendant,
    ActionDefendant,
    ViolationCategory,
    MonetaryTerms,
    StatuteCited,
)

# ── Constants ─────────────────────────────────────────────────────────────

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
    "tech_platform": "Tech Platform Accountability",
}

ACTION_TYPE_DISPLAY = {
    "settlement": "Settlement",
    "lawsuit_filed": "Lawsuit Filed",
    "judgment": "Judgment",
    "injunction": "Injunction",
    "consent_decree": "Consent Decree",
    "assurance_of_discontinuance": "Assurance of Discontinuance",
}

BRAND_BLUE = "#1B2A4A"
BRAND_PURPLE = "#6C5CE7"
ACCENT_BLUE = "#0984E3"
LIGHT_BG = "#F8FAFC"

PALETTE = [
    "#0984E3", "#6C5CE7", "#00B894", "#E17055", "#FDCB6E",
    "#74B9FF", "#A29BFE", "#55EFC4", "#FF7675", "#DFE6E9",
]


# ── Data loading ──────────────────────────────────────────────────────────

MIN_QUALITY = 0.1  # Exclude non-enforcement filtered records

# Common filter: enforcement-only records (not filtered, not federal litigation)
def _enforcement_filter():
    return and_(
        EnforcementAction.quality_score > MIN_QUALITY,
        EnforcementAction.is_federal_litigation == False,
    )


@st.cache_resource
def get_database():
    db = Database()
    db.create_tables()
    return db


@st.cache_data(ttl=120)
def load_actions_df() -> pd.DataFrame:
    db = get_database()
    with db.get_session() as session:
        actions = session.execute(
            select(EnforcementAction).where(_enforcement_filter())
        ).scalars().all()

        rows = []
        for a in actions:
            rows.append({
                "id": a.id,
                "state": a.state,
                "state_name": STATE_NAMES.get(a.state, a.state),
                "date": a.date_announced,
                "year": a.date_announced.year,
                "month": a.date_announced.strftime("%Y-%m"),
                "action_type": a.action_type,
                "headline": a.headline,
                "source_url": a.source_url,
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=120)
def load_monetary_df() -> pd.DataFrame:
    db = get_database()
    with db.get_session() as session:
        rows = session.execute(
            select(
                EnforcementAction.id,
                EnforcementAction.state,
                EnforcementAction.date_announced,
                EnforcementAction.headline,
                MonetaryTerms.total_amount,
                MonetaryTerms.amount_is_estimated,
            )
            .join(MonetaryTerms)
            .where(
                and_(
                    MonetaryTerms.total_amount > 0,
                    EnforcementAction.quality_score > MIN_QUALITY,
                    EnforcementAction.is_federal_litigation == False,
                )
            )
            .order_by(desc(MonetaryTerms.total_amount))
        ).all()

        return pd.DataFrame(
            [{"id": r[0], "state": r[1], "date": r[2], "headline": r[3],
              "amount": float(r[4]), "is_estimated": r[5]} for r in rows]
        ) if rows else pd.DataFrame()


@st.cache_data(ttl=120)
def load_categories_df() -> pd.DataFrame:
    db = get_database()
    with db.get_session() as session:
        rows = session.execute(
            select(
                ViolationCategory.category,
                ViolationCategory.subcategory,
                EnforcementAction.state,
                EnforcementAction.date_announced,
            )
            .join(EnforcementAction)
            .where(
                and_(
                    EnforcementAction.quality_score > MIN_QUALITY,
                    EnforcementAction.is_federal_litigation == False,
                    ViolationCategory.category != "other",
                )
            )
        ).all()

        return pd.DataFrame(
            [{"category": r[0], "subcategory": r[1], "state": r[2],
              "date": r[3], "year": r[3].year}
             for r in rows]
        ) if rows else pd.DataFrame()


@st.cache_data(ttl=120)
def load_defendants_df() -> pd.DataFrame:
    db = get_database()
    with db.get_session() as session:
        rows = session.execute(
            select(
                Defendant.canonical_name,
                func.count(distinct(ActionDefendant.action_id)).label("action_count"),
                func.count(distinct(EnforcementAction.state)).label("state_count"),
                func.group_concat(distinct(EnforcementAction.state)).label("states"),
            )
            .select_from(Defendant)
            .join(ActionDefendant, ActionDefendant.defendant_id == Defendant.id)
            .join(EnforcementAction, EnforcementAction.id == ActionDefendant.action_id)
            .where(
                and_(
                    Defendant.canonical_name != "",
                    Defendant.canonical_name.isnot(None),
                    func.length(Defendant.canonical_name) > 3,
                    EnforcementAction.quality_score > MIN_QUALITY,
                    EnforcementAction.is_federal_litigation == False,
                )
            )
            .group_by(Defendant.canonical_name)
            .order_by(desc("action_count"))
        ).all()

        return pd.DataFrame(
            [{"Company": r[0], "Actions": r[1], "States": r[2],
              "State List": r[3]} for r in rows]
        ) if rows else pd.DataFrame()


@st.cache_data(ttl=120)
def load_multistate_df() -> pd.DataFrame:
    """Load defendants targeted by 3+ states — the 'wow' table."""
    db = get_database()
    with db.get_session() as session:
        subq = (
            select(
                Defendant.canonical_name.label("company"),
                func.count(distinct(EnforcementAction.state)).label("state_count"),
                func.count(distinct(ActionDefendant.action_id)).label("action_count"),
                func.group_concat(distinct(EnforcementAction.state)).label("states"),
            )
            .select_from(Defendant)
            .join(ActionDefendant, ActionDefendant.defendant_id == Defendant.id)
            .join(EnforcementAction, EnforcementAction.id == ActionDefendant.action_id)
            .where(
                and_(
                    Defendant.canonical_name != "",
                    Defendant.canonical_name.isnot(None),
                    func.length(Defendant.canonical_name) > 3,
                    EnforcementAction.quality_score > MIN_QUALITY,
                    EnforcementAction.is_federal_litigation == False,
                )
            )
            .group_by(Defendant.canonical_name)
            .having(func.count(distinct(EnforcementAction.state)) >= 3)
            .order_by(desc("state_count"), desc("action_count"))
        )
        rows = session.execute(subq).all()

        # Also get settlement amounts per company
        settle_map: dict[str, float] = {}
        for r in rows:
            company = r[0]
            amt = session.execute(
                select(func.sum(MonetaryTerms.total_amount))
                .select_from(MonetaryTerms)
                .join(EnforcementAction, EnforcementAction.id == MonetaryTerms.action_id)
                .join(ActionDefendant, ActionDefendant.action_id == EnforcementAction.id)
                .join(Defendant, Defendant.id == ActionDefendant.defendant_id)
                .where(
                    and_(
                        Defendant.canonical_name == company,
                        MonetaryTerms.total_amount > 0,
                        MonetaryTerms.amount_is_estimated == False,
                        EnforcementAction.quality_score > MIN_QUALITY,
                    )
                )
            ).scalar()
            settle_map[company] = float(amt) if amt else 0.0

        data = []
        for r in rows:
            total = settle_map.get(r[0], 0)
            data.append({
                "Company": r[0],
                "States Targeted": r[1],
                "Total Actions": r[2],
                "States": r[3],
                "Settlements ($M)": round(total / 1e6, 1) if total else None,
            })

    return pd.DataFrame(data) if data else pd.DataFrame()


@st.cache_data(ttl=120)
def load_company_search_data() -> pd.DataFrame:
    """Load full company detail data for search."""
    db = get_database()
    with db.get_session() as session:
        rows = session.execute(
            select(
                Defendant.canonical_name,
                EnforcementAction.state,
                EnforcementAction.date_announced,
                EnforcementAction.headline,
                EnforcementAction.action_type,
                EnforcementAction.source_url,
                MonetaryTerms.total_amount,
            )
            .select_from(ActionDefendant)
            .join(Defendant, Defendant.id == ActionDefendant.defendant_id)
            .join(EnforcementAction, EnforcementAction.id == ActionDefendant.action_id)
            .outerjoin(MonetaryTerms, MonetaryTerms.action_id == EnforcementAction.id)
            .where(
                and_(
                    Defendant.canonical_name != "",
                    Defendant.canonical_name.isnot(None),
                    func.length(Defendant.canonical_name) > 3,
                    EnforcementAction.quality_score > MIN_QUALITY,
                    EnforcementAction.is_federal_litigation == False,
                )
            )
            .order_by(desc(EnforcementAction.date_announced))
        ).all()

        return pd.DataFrame(
            [{
                "company": r[0],
                "state": r[1],
                "date": r[2],
                "headline": r[3],
                "action_type": r[4],
                "source_url": r[5],
                "amount": float(r[6]) if r[6] else None,
            } for r in rows]
        ) if rows else pd.DataFrame()


# ── Page layout ───────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="State AG Enforcement Tracker",
        page_icon=":scales:",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # ── Custom CSS ─────────────────────────────────────────────────────
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .block-container { padding-top: 1.5rem; max-width: 1200px; }

    .main-title {
        font-family: 'Inter', sans-serif;
        font-size: 2.2rem;
        font-weight: 700;
        color: #1B2A4A;
        margin-bottom: 0;
        line-height: 1.2;
    }
    .main-subtitle {
        font-family: 'Inter', sans-serif;
        font-size: 1.05rem;
        color: #64748B;
        margin-top: 2px;
        margin-bottom: 1.2rem;
    }

    /* KPI cards */
    [data-testid="stMetric"] {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 16px 20px;
    }
    [data-testid="stMetricLabel"] {
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem !important;
        font-weight: 500;
        color: #64748B !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="stMetricValue"] {
        font-family: 'Inter', sans-serif;
        font-size: 1.8rem !important;
        font-weight: 700;
        color: #1B2A4A !important;
    }

    /* Section headers */
    .section-header {
        font-family: 'Inter', sans-serif;
        font-size: 1.25rem;
        font-weight: 600;
        color: #1B2A4A;
        margin-top: 0.8rem;
        margin-bottom: 0.4rem;
        padding-bottom: 0.3rem;
        border-bottom: 2px solid #E2E8F0;
    }

    /* Table styling */
    .dataframe { font-family: 'Inter', sans-serif !important; }

    /* Hide Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header[data-testid="stHeader"] { background: transparent; }

    /* Sidebar */
    [data-testid="stSidebar"] { background: #F8FAFC; }

    div[data-testid="stDataFrame"] div[class*="glideDataEditor"] {
        border: 1px solid #E2E8F0;
        border-radius: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── Title ──────────────────────────────────────────────────────────
    st.markdown(
        '<p class="main-title">State AG Enforcement Tracker</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="main-subtitle">5,700+ corporate enforcement actions across 8 states &mdash; structured, searchable, and ready for analysis</p>',
        unsafe_allow_html=True,
    )

    # ── Load data ──────────────────────────────────────────────────────
    actions_df = load_actions_df()
    monetary_df = load_monetary_df()
    categories_df = load_categories_df()
    defendants_df = load_defendants_df()
    multistate_df = load_multistate_df()
    search_data = load_company_search_data()

    if actions_df.empty:
        st.warning("No data loaded. Run the scrape and extract pipeline first.")
        return

    # ── KPI Row ────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)

    total_actions = len(actions_df)
    total_states = actions_df["state"].nunique()
    settlement_records = len(monetary_df[monetary_df["amount"] > 0]) if not monetary_df.empty else 0
    date_min = actions_df["date"].min()
    date_max = actions_df["date"].max()

    with k1:
        st.metric("Enforcement Actions", f"{total_actions:,}")
    with k2:
        st.metric("States Tracked", f"{total_states}")
    with k3:
        st.metric("Settlements Identified", f"{settlement_records:,}")
    with k4:
        st.metric("Date Range", f"{date_min.year}–{date_max.year}")

    st.markdown("")  # spacer

    # ── Multistate Enforcement Targets (the "wow" feature) ─────────────
    st.markdown(
        '<p class="section-header">Multistate Enforcement Targets</p>',
        unsafe_allow_html=True,
    )
    st.caption("Companies targeted by 3 or more state AG offices — the highest-risk enforcement targets in the dataset")

    if not multistate_df.empty:
        display_ms = multistate_df.copy()
        # Format settlement column
        display_ms["Settlements ($M)"] = display_ms["Settlements ($M)"].apply(
            lambda x: f"${x:,.1f}" if x and x > 0 else "—"
        )
        st.dataframe(
            display_ms[["Company", "States Targeted", "Total Actions", "States", "Settlements ($M)"]],
            use_container_width=True,
            hide_index=True,
            height=min(len(display_ms) * 36 + 40, 520),
            column_config={
                "Company": st.column_config.TextColumn("Company", width="medium"),
                "States Targeted": st.column_config.NumberColumn("States", width="small"),
                "Total Actions": st.column_config.NumberColumn("Actions", width="small"),
                "States": st.column_config.TextColumn("States Involved", width="medium"),
                "Settlements ($M)": st.column_config.TextColumn("Settlements ($M)", width="small"),
            },
        )

    st.markdown("")

    # ── Company Search ─────────────────────────────────────────────────
    st.markdown(
        '<p class="section-header">Company Search</p>',
        unsafe_allow_html=True,
    )

    search_query = st.text_input(
        "Search for a company",
        placeholder="e.g. Google, Meta, Purdue Pharma, JUUL, Amazon",
        label_visibility="collapsed",
    )

    if search_query and not search_data.empty:
        query_lower = search_query.strip().lower()
        matches = search_data[search_data["company"].str.lower().str.contains(query_lower, na=False)]

        if matches.empty:
            st.info(f"No enforcement actions found for \"{search_query}\".")
        else:
            company_names = matches["company"].unique()
            total_matches = len(matches)
            states_hit = matches["state"].nunique()
            total_amt = matches["amount"].sum()

            # Summary row
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                st.metric("Actions Found", f"{total_matches}")
            with sc2:
                st.metric("States Involved", f"{states_hit}")
            with sc3:
                if total_amt and total_amt > 0:
                    if total_amt >= 1e9:
                        st.metric("Total Settlements", f"${total_amt / 1e9:.1f}B")
                    elif total_amt >= 1e6:
                        st.metric("Total Settlements", f"${total_amt / 1e6:.1f}M")
                    else:
                        st.metric("Total Settlements", f"${total_amt:,.0f}")
                else:
                    st.metric("Total Settlements", "—")

            # Results table
            results = matches.copy()
            results["date"] = pd.to_datetime(results["date"]).dt.strftime("%Y-%m-%d")
            results["action_type"] = results["action_type"].map(
                lambda x: ACTION_TYPE_DISPLAY.get(x, x.replace("_", " ").title())
            )
            results["amount_display"] = results["amount"].apply(
                lambda x: f"${x / 1e6:.1f}M" if x and x >= 1e6 else (f"${x:,.0f}" if x and x > 0 else "—")
            )

            st.dataframe(
                results[["date", "state", "action_type", "headline", "amount_display"]].rename(
                    columns={
                        "date": "Date",
                        "state": "State",
                        "action_type": "Type",
                        "headline": "Headline",
                        "amount_display": "Amount",
                    }
                ),
                use_container_width=True,
                hide_index=True,
                height=min(total_matches * 36 + 40, 400),
            )

    st.markdown("")

    # ── Row: State Map + Category Breakdown ────────────────────────────
    map_col, cat_col = st.columns([3, 2])

    with map_col:
        st.markdown(
            '<p class="section-header">Enforcement Volume by State</p>',
            unsafe_allow_html=True,
        )
        state_counts = actions_df.groupby("state").size().reset_index(name="count")
        state_counts["state_name"] = state_counts["state"].map(STATE_NAMES)

        fig_map = px.choropleth(
            state_counts,
            locations="state",
            locationmode="USA-states",
            color="count",
            hover_name="state_name",
            hover_data={"count": ":,", "state": False},
            color_continuous_scale=[
                [0, "#E8EDF5"],
                [0.3, "#74B9FF"],
                [0.6, "#0984E3"],
                [1.0, "#1B2A4A"],
            ],
            scope="usa",
            labels={"count": "Actions"},
        )
        fig_map.update_layout(
            geo=dict(
                bgcolor="rgba(0,0,0,0)",
                lakecolor="rgba(0,0,0,0)",
                showlakes=False,
            ),
            margin=dict(l=0, r=0, t=0, b=0),
            height=400,
            coloraxis_colorbar=dict(
                title="Actions",
                thickness=12,
                len=0.6,
                tickfont=dict(size=11),
            ),
        )
        st.plotly_chart(fig_map, use_container_width=True)

    with cat_col:
        st.markdown(
            '<p class="section-header">Enforcement by Category</p>',
            unsafe_allow_html=True,
        )
        if not categories_df.empty:
            cat_counts = categories_df["category"].value_counts().reset_index()
            cat_counts.columns = ["category", "count"]
            cat_counts["label"] = cat_counts["category"].map(
                lambda x: CATEGORY_DISPLAY.get(x, x)
            )
            cat_counts = cat_counts.head(10)

            fig_cats = px.bar(
                cat_counts, x="count", y="label",
                orientation="h",
                color_discrete_sequence=[ACCENT_BLUE],
                labels={"count": "Actions", "label": ""},
            )
            fig_cats.update_layout(
                margin=dict(l=0, r=20, t=10, b=0),
                height=400,
                yaxis=dict(autorange="reversed"),
                xaxis=dict(title=""),
                plot_bgcolor="rgba(0,0,0,0)",
            )
            fig_cats.update_traces(
                marker=dict(cornerradius=4),
                texttemplate="%{x:,}",
                textposition="outside",
                textfont=dict(size=11),
            )
            st.plotly_chart(fig_cats, use_container_width=True)

    # ── Row: Category Trends + Action Type Breakdown ───────────────────
    trend_col, type_col = st.columns([3, 2])

    with trend_col:
        st.markdown(
            '<p class="section-header">Category Trends (2022–2025)</p>',
            unsafe_allow_html=True,
        )
        if not categories_df.empty:
            trend_df = categories_df[categories_df["year"].between(2022, 2025)]
            top_cats = trend_df["category"].value_counts().head(6).index.tolist()
            trend_filtered = trend_df[trend_df["category"].isin(top_cats)]

            trend_pivot = (
                trend_filtered
                .groupby(["year", "category"])
                .size()
                .reset_index(name="count")
            )
            trend_pivot["label"] = trend_pivot["category"].map(
                lambda x: CATEGORY_DISPLAY.get(x, x)
            )

            fig_trend = px.line(
                trend_pivot, x="year", y="count", color="label",
                markers=True,
                color_discrete_sequence=PALETTE,
                labels={"count": "Actions", "year": "", "label": ""},
            )
            fig_trend.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                height=380,
                legend=dict(
                    orientation="h", yanchor="top", y=-0.15,
                    font=dict(size=11),
                ),
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(dtick=1),
            )
            fig_trend.update_traces(line=dict(width=2.5))
            st.plotly_chart(fig_trend, use_container_width=True)

    with type_col:
        st.markdown(
            '<p class="section-header">Action Types</p>',
            unsafe_allow_html=True,
        )
        # Exclude "other" from the pie chart
        typed = actions_df[actions_df["action_type"] != "other"].copy()
        if not typed.empty:
            type_counts = typed["action_type"].value_counts().reset_index()
            type_counts.columns = ["type", "count"]
            type_counts["label"] = type_counts["type"].map(
                lambda x: ACTION_TYPE_DISPLAY.get(x, x.replace("_", " ").title())
            )

            fig_types = px.pie(
                type_counts, values="count", names="label",
                color_discrete_sequence=PALETTE,
                hole=0.45,
            )
            fig_types.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                height=380,
                showlegend=True,
                legend=dict(
                    orientation="h", yanchor="top", y=-0.1,
                    font=dict(size=11),
                ),
            )
            fig_types.update_traces(
                textposition="inside",
                textinfo="percent+label",
                textfont=dict(size=10),
            )
            st.plotly_chart(fig_types, use_container_width=True)

    # ── Top Defendants Bar Chart ───────────────────────────────────────
    st.markdown(
        '<p class="section-header">Most-Targeted Companies</p>',
        unsafe_allow_html=True,
    )

    if not defendants_df.empty:
        top15 = defendants_df.head(15).copy()
        top15 = top15.iloc[::-1]  # reverse for horizontal bar

        fig_defs = go.Figure()
        fig_defs.add_trace(go.Bar(
            x=top15["Actions"],
            y=top15["Company"],
            orientation="h",
            marker=dict(
                color=ACCENT_BLUE,
                cornerradius=4,
            ),
            text=top15["Actions"],
            textposition="outside",
            textfont=dict(size=11),
        ))
        fig_defs.update_layout(
            margin=dict(l=0, r=40, t=10, b=0),
            height=480,
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="", showgrid=False, showticklabels=False),
            yaxis=dict(title=""),
        )
        st.plotly_chart(fig_defs, use_container_width=True)

    # ── Footer ─────────────────────────────────────────────────────────
    st.markdown("")
    st.divider()
    st.markdown(
        "<div style='text-align: center; color: #94A3B8; font-size: 0.8rem; padding: 8px 0;'>"
        "State AG Enforcement Tracker &bull; Data sourced from official state Attorney General press releases &bull; "
        "Automated pipeline: scrape &rarr; extract &rarr; normalize &rarr; classify"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
