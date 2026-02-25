"""Streamlit dashboard for the AG Enforcement Tracker.

Polished, screenshot-ready dashboard for outreach and demos.

Run: streamlit run src/dashboard/app.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import select, func, desc, distinct, and_, cast, Integer

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


# ── Display-level data quality filters ────────────────────────────────────
# These catch garbage that slipped past extraction. Root cause fixes belong
# in extractors/patterns.py and config/defendant_blocklist.yaml; these are
# the safety net so the dashboard never shows embarrassing entries.

# Exact canonical names to suppress (case-insensitive comparison)
_DEFENDANT_DISPLAY_BLOCKLIST: set[str] = {
    # Generic terms that aren't company names
    "mortgage", "mortgage servicer", "e-cigarette", "cryptocurrency",
    "companies", "funding", "centers", "owners", "patients",
    "consumers is", "federal government", "telecommunications",
    "statement", "biotechnology", "national", "student", "software",
    "services", "territories", "construction", "marketing", "providers",
    "health", "firm", "oil", "opioid", "tips", "colleges",
    "cryptocurrency platform", "cryptocurrency trading",
    "cryptocurrency markets", "another cryptocurrency",
    # Headline fragments / descriptions mistaken for names
    "during the", "everett man", "seattle business",
    "huntington beach sinc", "paul means",
    # Criminal case individuals/businesses (not corporate enforcement)
    "blayne m", "blayne m. perez",
    "federal way discount guns",
    # AG names extracted as defendants
    "ag ferguson", "ag yost", "ag paxton", "ag bonta", "ag james",
    "ag healey", "ag campbell", "ag rayfield",
}

# Merge duplicate entity names for display. Maps variant -> canonical.
_DEFENDANT_MERGE_MAP: dict[str, str] = {
    "Juul Labs": "Juul",
    "ByteDance": "TikTok",
    "Tik Tok": "TikTok",
    "Sacklers": "Sackler Family",
    "Purdue": "Purdue Pharma",
    "Marriott International": "Marriott",
}

# Regex patterns — if any match (case-insensitive), suppress the name
_DEFENDANT_DISPLAY_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in [
        r"^cryptocurrency\b",        # "Cryptocurrency Companies Gemini...", etc.
        r"^crypto\s+firm\b",         # "Crypto Firm Genesis Global Capital"
        r"^former\s+ceo\s+of\b",     # "Former CEO of Celsius..."
        r"\bfor\s+deceiving\b",      # "TurboTax Owner Intuit for Deceiving Low-"
        r"\bman$",                    # "Everett man", "Tacoma man"
        r"\bbusiness$",              # "Seattle business"
        r"\bwoman$",                 # "Spokane woman"
        r"^(?:two|three|four|five|six|seven|eight|nine|ten)\s+",  # "Three Companies"
        r"^(?:a|an|the)\s+",         # "A Cryptocurrency Company"
        r"\bowner\b.*\bfor\b",       # "TurboTax Owner Intuit for..."
        r"^maker\s+of\b",           # "Maker of Opioid Addiction Treatment Drug"
        r"^\d+\s+corporate\b",      # "30 Corporate Defendants"
        r"^joint\s+statement\b",    # "Joint Statement on Lawsuit..."
        r"^multistate\b",           # "Multistate Coalition"
        r"^illegally\b",            # "Illegally Cutting Funds"
        r"^unlawfully\b",           # "Unlawfully Cutting Billions"
    ]
]

# Headlines in the settlements table that indicate non-enforcement items
_SETTLEMENT_HEADLINE_BLOCKLIST: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in [
        r"\bbill\s+creates\b",                   # Legislative items
        r"\blegislat",                            # Legislative items
        r"\bdismantling\s+cfpb\b",                # Policy advocacy
        r"\bfree\s+help\s+available\b",           # Consumer outreach
        r"\bcheck\s+the\s+mail\b",                # Consumer outreach
        r"\bplugs\s+free\s+help\b",               # Consumer outreach
        r"\bhomeowners?\s+(?:can|should|may)\b",   # Consumer tips
        r"\bconsumer\s+(?:tips?|alert)\b",         # Consumer alerts
        r"\bone\s+step\s+closer\b",               # Settlement status updates
        r"\bbenefits\s+are\s+one\s+step\b",        # Settlement status updates
        r"\bproviding\s+huge\s+benefits\b",        # Settlement follow-up press
        r"\bstruggling\b.*\bhomeowners?\b",        # Consumer outreach
        r"\bcourt\s+decision\s+allowing\s+states\s+to\s+participate\b",  # Procedural rulings
        r"^(?:june|july|august|september|october|november|december|january|february|march|april|may)\s+\d",  # Body text leaked as headline
        r"^olympia\s*[-–—]",                          # WA body text leaked as headline
        r"^the\s+money\s+is\b",                       # Body text leaked as headline
        r"^\w+\s*[-–—]\s*\w+\s+\d{1,2},\s*\d{4}",    # "City - Month DD, YYYY" body text leak
        r"\bconsumers?\s+will\s+benefit\b",            # Settlement benefit announcement, not action
    ]
]


def _is_clean_defendant(name: str) -> bool:
    """Return True if the defendant name is suitable for display."""
    if not name or len(name) <= 3:
        return False
    low = name.strip().lower()
    if low in _DEFENDANT_DISPLAY_BLOCKLIST:
        return False
    for pat in _DEFENDANT_DISPLAY_PATTERNS:
        if pat.search(name):
            return False
    return True


def _is_clean_settlement_headline(headline: str) -> bool:
    """Return True if the settlement headline looks like a real enforcement action."""
    if not headline:
        return False
    for pat in _SETTLEMENT_HEADLINE_BLOCKLIST:
        if pat.search(headline):
            return False
    return True


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
                "is_multistate": bool(a.is_multistate),
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
                EnforcementAction.is_multistate,
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
              "amount": float(r[4]), "is_estimated": r[5],
              "is_multistate": bool(r[6])} for r in rows]
        ) if rows else pd.DataFrame()


def _dedup_settlements(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Deduplicate multistate settlements by grouping on similar amounts and dates.

    Uses the is_multistate flag as a primary signal — multistate actions get
    wider matching tolerances since different states may announce the same
    settlement months apart with slightly different reported amounts.
    Returns one row per unique settlement with state count.
    """
    if df.empty:
        return df

    df = df.sort_values("amount", ascending=False).copy()
    df["date"] = pd.to_datetime(df["date"])

    groups: list[dict] = []
    used = set()

    for idx, row in df.iterrows():
        if idx in used:
            continue

        amt = row["amount"]
        dt = row["date"]
        is_ms = row.get("is_multistate", False)

        # Multistate actions get wider tolerances: states announce the same
        # settlement at different times and may round amounts differently
        if is_ms:
            amt_tol = 0.05   # 5% for multistate
            date_tol = pd.Timedelta(days=1460)  # 4 years — states announce same settlement years apart
        else:
            amt_tol = 0.01   # 1% for single-state
            date_tol = pd.Timedelta(days=365)

        mask = (
            (~df.index.isin(used)) &
            (df["amount"].between(amt * (1 - amt_tol), amt * (1 + amt_tol))) &
            ((df["date"] - dt).abs() <= date_tol)
        )
        cluster = df[mask]

        states = sorted(cluster["state"].unique())
        any_multistate = cluster["is_multistate"].any()
        used.update(cluster.index)

        groups.append({
            "headline": row["headline"],
            "amount": amt,
            "date": dt,
            "state": ", ".join(states) if len(states) > 1 else states[0],
            "state_count": len(states),
            "is_multistate": any_multistate or len(states) > 1,
        })

        if len(groups) >= top_n:
            break

    return pd.DataFrame(groups)


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

        df = pd.DataFrame(
            [{"Company": r[0], "Actions": r[1], "States": r[2],
              "State List": r[3]} for r in rows]
        ) if rows else pd.DataFrame()

        if not df.empty:
            df = df[df["Company"].apply(_is_clean_defendant)]
            # Merge duplicate entity names and re-aggregate
            df["Company"] = df["Company"].replace(_DEFENDANT_MERGE_MAP)
            df = (
                df.groupby("Company", as_index=False)
                .agg({"Actions": "sum", "States": "max", "State List": "first"})
                .sort_values("Actions", ascending=False)
            )
        return df


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

        # Filter out garbage names
        rows = [r for r in rows if _is_clean_defendant(r[0])]

        data = []
        for r in rows:
            data.append({
                "Company": r[0],
                "States Targeted": r[1],
                "Total Actions": r[2],
                "States": r[3],
            })

    df = pd.DataFrame(data) if data else pd.DataFrame()
    if not df.empty:
        # Merge duplicate entity names (e.g. "Juul" + "Juul Labs")
        df["Company"] = df["Company"].replace(_DEFENDANT_MERGE_MAP)
        df = (
            df.groupby("Company", as_index=False)
            .agg({
                "States Targeted": "max",
                "Total Actions": "sum",
                "States": "first",
            })
            .sort_values(["States Targeted", "Total Actions"], ascending=[False, False])
        )
    return df


@st.cache_data(ttl=120)
def load_coverage_df() -> pd.DataFrame:
    """Load per-state data coverage stats."""
    db = get_database()
    with db.get_session() as session:
        rows = session.execute(
            select(
                EnforcementAction.state,
                func.count().label("total_scraped"),
                func.sum(
                    cast(
                        EnforcementAction.quality_score > MIN_QUALITY,
                        Integer,
                    )
                ).label("active"),
                func.min(EnforcementAction.date_announced).label("earliest"),
                func.max(EnforcementAction.date_announced).label("latest"),
            )
            .group_by(EnforcementAction.state)
            .order_by(desc("active"))
        ).all()

        return pd.DataFrame(
            [{
                "State": STATE_NAMES.get(r[0], r[0]),
                "Code": r[0],
                "Total Scraped": r[1],
                "Active Records": r[2] or 0,
                "Earliest": r[3],
                "Latest": r[4],
            } for r in rows]
        ) if rows else pd.DataFrame()


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

        df = pd.DataFrame(
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

        if not df.empty:
            df = df[df["company"].apply(_is_clean_defendant)]
            df["company"] = df["company"].replace(_DEFENDANT_MERGE_MAP)
        return df


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
        color: #FFFFFF;
        margin-bottom: 0;
        line-height: 1.2;
    }
    .main-subtitle {
        font-family: 'Inter', sans-serif;
        font-size: 1.05rem;
        color: #94A3B8;
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
        color: #FFFFFF;
        margin-top: 0.8rem;
        margin-bottom: 0.4rem;
        padding-bottom: 0.3rem;
        border-bottom: 2px solid #E2E8F0;
    }

    /* Table styling */
    .dataframe { font-family: 'Inter', sans-serif !important; }

    /* Hide Streamlit branding, GitHub link, and hosted badges */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header[data-testid="stHeader"] { background: transparent; }
    [data-testid="stToolbar"] { display: none; }
    [data-testid="stAppDeployButton"] { display: none; }
    ._profileContainer_gzau3_53 { display: none !important; }
    ._container_gzau3_1 { display: none !important; }
    [data-testid="stStatusWidget"] { display: none; }
    /* Catch-all: hide fixed-position bottom-right badges */
    div[class*="profileContainer"] { display: none !important; }
    div[class*="hostContainer"] { display: none !important; }
    iframe[title="streamlit_badge"] { display: none !important; }
    #stStreamlitBadge { display: none !important; }

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
    # ── Load data ──────────────────────────────────────────────────────
    actions_df = load_actions_df()
    monetary_df = load_monetary_df()
    categories_df = load_categories_df()
    defendants_df = load_defendants_df()
    multistate_df = load_multistate_df()
    search_data = load_company_search_data()
    coverage_df = load_coverage_df()

    if actions_df.empty:
        st.warning("No data loaded. Run the scrape and extract pipeline first.")
        return

    total_actions = len(actions_df)
    total_states = actions_df["state"].nunique()
    total_defendants = len(defendants_df) if not defendants_df.empty else 0
    date_min = actions_df["date"].min()
    date_max = actions_df["date"].max()

    st.markdown(
        f'<p class="main-subtitle">{total_actions:,} enforcement actions across {total_states} states, '
        f'tracking {total_defendants:,} defendants &mdash; structured, searchable, and ready for analysis</p>',
        unsafe_allow_html=True,
    )

    # ── KPI Row ────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.metric("Enforcement Actions", f"{total_actions:,}")
    with k2:
        st.metric("States Tracked", f"{total_states}")
    with k3:
        st.metric("Defendants Tracked", f"{total_defendants:,}")
    with k4:
        st.metric("Date Range", f"{date_min.year}\u2013{date_max.year}")

    st.markdown("")  # spacer

    # ── Multistate Enforcement Targets (the "wow" feature) ─────────────
    st.markdown(
        '<p class="section-header">Multistate Enforcement Targets</p>',
        unsafe_allow_html=True,
    )
    st.caption("Companies targeted by 3 or more state AG offices — the highest-risk enforcement targets in the dataset")

    if not multistate_df.empty:
        display_ms = multistate_df.copy()
        st.dataframe(
            display_ms[["Company", "States Targeted", "Total Actions", "States"]],
            use_container_width=True,
            hide_index=True,
            height=min(len(display_ms) * 36 + 40, 520),
            column_config={
                "Company": st.column_config.TextColumn("Company", width="medium"),
                "States Targeted": st.column_config.NumberColumn("States", width="small"),
                "Total Actions": st.column_config.NumberColumn("Actions", width="small"),
                "States": st.column_config.TextColumn("States Involved", width="large"),
            },
        )

    # ── Multistate Statistics Row ──────────────────────────────────────
    ms_actions = actions_df[actions_df["is_multistate"] == True] if not actions_df.empty else pd.DataFrame()
    ms_count = len(ms_actions)
    ms_pct = (ms_count / total_actions * 100) if total_actions > 0 else 0

    # Average states per multistate action (from the multistate_df targets)
    avg_states = multistate_df["States Targeted"].mean() if not multistate_df.empty else 0

    ms1, ms2, ms3 = st.columns(3)
    with ms1:
        st.metric("Multistate Actions", f"{ms_count:,}")
    with ms2:
        st.metric("% of All Actions", f"{ms_pct:.1f}%")
    with ms3:
        st.metric("Avg States per Target", f"{avg_states:.1f}")

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

    # ── Largest Settlements (deduplicated) ─────────────────────────────
    st.markdown(
        '<p class="section-header">Largest Settlements</p>',
        unsafe_allow_html=True,
    )
    st.caption("Top 10 unique settlements — multistate actions shown once with participating state count")

    if not monetary_df.empty:
        clean_monetary = monetary_df[monetary_df["headline"].apply(_is_clean_settlement_headline)]
        # Request extra rows so we still have 10 after amount-level dedup
        top_settlements = _dedup_settlements(clean_monetary, top_n=20)
        if not top_settlements.empty:
            display_settle = top_settlements.copy()
            display_settle["Amount"] = display_settle["amount"].apply(
                lambda x: f"${x / 1e9:.1f}B" if x >= 1e9 else (f"${x / 1e6:.1f}M" if x >= 1e6 else f"${x:,.0f}")
            )
            # Drop duplicate display amounts (e.g. two "$7.4B" Purdue entries) — keep first (highest state count)
            display_settle = display_settle.drop_duplicates(subset="Amount", keep="first").head(10)
            display_settle["Date"] = pd.to_datetime(display_settle["date"]).dt.strftime("%Y-%m-%d")
            display_settle["Scope"] = display_settle.apply(
                lambda r: f"{r['state_count']}-state multistate" if r["state_count"] > 1
                else ("Multistate" if r["is_multistate"] else r["state"]),
                axis=1,
            )
            # Clean up headline display artifacts
            display_settle["Headline"] = (
                display_settle["headline"]
                .str.replace(r"^Press\s*Release\s*(?=AG\b)", "Press Release: ", regex=True)
                .str.replace(r"(?<=[a-z])(?=[A-Z])", " ", regex=True)  # "fromCVS" -> "from CVS"
                .str.replace(r"(?<=[A-Z]{2})(?=[a-z])", " ", regex=True)  # "CVSand" -> "CVS and"
                .str.replace(r"(\$\d+(?:\.\d+)?)(?=[A-Z])", r"\1 ", regex=True)  # "$21Billion" -> "$21 Billion"
                .str[:80]
            )

            st.dataframe(
                display_settle[["Date", "Scope", "Headline", "Amount"]],
                use_container_width=True,
                hide_index=True,
                height=min(len(display_settle) * 36 + 40, 420),
                column_config={
                    "Date": st.column_config.TextColumn("Date", width="small"),
                    "Scope": st.column_config.TextColumn("Scope", width="small"),
                    "Headline": st.column_config.TextColumn("Headline", width="large"),
                    "Amount": st.column_config.TextColumn("Amount", width="small"),
                },
            )

    st.markdown("")

    # ── Recent Actions Feed ──────────────────────────────────────────
    st.markdown(
        '<p class="section-header">Recent Enforcement Actions</p>',
        unsafe_allow_html=True,
    )
    st.caption("The 20 most recent enforcement actions in the dataset")

    # Filter recent actions: deduplicate by headline and remove non-enforcement items
    _recent_sorted = actions_df.sort_values("date", ascending=False).copy()
    # Exclude "other" action type — these are unclassified / likely non-enforcement
    _recent_sorted = _recent_sorted[_recent_sorted["action_type"] != "other"]
    # Drop duplicate headlines (e.g. same press release appearing in multiple rows)
    _recent_sorted = _recent_sorted.drop_duplicates(subset="headline", keep="first")
    # Remove headlines that look like policy statements / non-enforcement
    _RECENT_HEADLINE_REJECT = [
        re.compile(p, re.IGNORECASE) for p in [
            r"\bany\s+federal\s+legislation\b",
            r"\btransparency\s+and\s+accountability\b",
            r"\bmust\s+allow\s+states\b",
            r"\bfoundation\s+of\s+good\s+law\b",
            r"\bstatement\s+on\b",
            r"\bissues?\s+statement\b",
            r"\bconsumer\s+alert\b",
            r"\bconsumer\s+tips?\b",
            r"\burges?\b",
            r"\boppos(?:es?|ing)\s+(?:trump|biden|federal)\b",
            r"^press\s*release\s*(?:ag|attorney)\b",  # "Press ReleaseAG Campbell..."
            r"\bplead(?:ed|s)?\s+guilty\s+in\s+(?:mahoning|valley).*trafficking\b",  # criminal case, not civil
            r"\blegal\s+observation\s+project\b",  # AG monitoring, not enforcement
            r"\bto\s+monitor\s+federal\b",         # AG monitoring federal activity
        ]
    ]
    _recent_sorted = _recent_sorted[
        ~_recent_sorted["headline"].apply(
            lambda h: any(p.search(h) for p in _RECENT_HEADLINE_REJECT) if h else False
        )
    ]
    # Clean up "Press ReleaseAG" prefixes
    _recent_sorted["headline"] = _recent_sorted["headline"].str.replace(
        r"^Press\s*Release\s*(?=AG\b)", "Press Release: ", regex=True
    )
    recent_df = _recent_sorted.head(20).copy()
    if not recent_df.empty:
        # Join with monetary data for amounts
        if not monetary_df.empty:
            recent_with_amt = recent_df.merge(
                monetary_df[["id", "amount"]],
                on="id", how="left",
            )
        else:
            recent_with_amt = recent_df.copy()
            recent_with_amt["amount"] = None

        recent_with_amt["date_display"] = pd.to_datetime(recent_with_amt["date"]).dt.strftime("%Y-%m-%d")
        recent_with_amt["type_display"] = recent_with_amt.apply(
            lambda r: ("[Multistate] " if r.get("is_multistate") else "")
            + ACTION_TYPE_DISPLAY.get(r["action_type"], r["action_type"].replace("_", " ").title()),
            axis=1,
        )
        recent_with_amt["amount_display"] = recent_with_amt["amount"].apply(
            lambda x: f"${x / 1e6:.1f}M" if x and x >= 1e6 else (f"${x:,.0f}" if x and x > 0 else "\u2014")
        )

        st.dataframe(
            recent_with_amt[["date_display", "state", "type_display", "headline", "amount_display"]].rename(
                columns={
                    "date_display": "Date",
                    "state": "State",
                    "type_display": "Type",
                    "headline": "Headline",
                    "amount_display": "Amount",
                }
            ),
            use_container_width=True,
            hide_index=True,
            height=min(20 * 36 + 40, 520),
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
            '<p class="section-header">Category Trends (2022\u20132026)</p>',
            unsafe_allow_html=True,
        )
        if not categories_df.empty:
            trend_df = categories_df[categories_df["year"].between(2022, 2026)]
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

    # ── Year-over-Year Enforcement Volume ────────────────────────────
    st.markdown(
        '<p class="section-header">Enforcement Volume by Year</p>',
        unsafe_allow_html=True,
    )

    if not actions_df.empty:
        # Focus on years with meaningful data
        yoy_df = actions_df[actions_df["year"].between(2022, 2026)].copy()
        yoy_counts = yoy_df.groupby("year").size().reset_index(name="count")

        # Split multistate vs single-state for stacked bar
        yoy_ms = yoy_df[yoy_df["is_multistate"] == True].groupby("year").size().reset_index(name="multistate")
        yoy_single = yoy_df[yoy_df["is_multistate"] == False].groupby("year").size().reset_index(name="single_state")
        yoy_stacked = yoy_counts.merge(yoy_ms, on="year", how="left").merge(yoy_single, on="year", how="left").fillna(0)

        # Sort chronologically
        import datetime
        current_year = datetime.date.today().year
        yoy_stacked = yoy_stacked.sort_values("year").reset_index(drop=True)

        # Use numeric x-axis with custom tick labels for "(YTD)"
        tick_labels = [f"{int(y)} (YTD)" if int(y) == current_year else str(int(y))
                       for y in yoy_stacked["year"]]

        fig_yoy = go.Figure()
        fig_yoy.add_trace(go.Bar(
            x=yoy_stacked["year"],
            y=yoy_stacked["single_state"],
            name="Single-State",
            marker=dict(color=ACCENT_BLUE, cornerradius=4),
        ))
        fig_yoy.add_trace(go.Bar(
            x=yoy_stacked["year"],
            y=yoy_stacked["multistate"],
            name="Multistate",
            marker=dict(color="#F39C12", cornerradius=4),
        ))
        fig_yoy.update_layout(
            barmode="stack",
            margin=dict(l=0, r=0, t=30, b=0),
            height=350,
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                title="",
                tickmode="array",
                tickvals=yoy_stacked["year"].tolist(),
                ticktext=tick_labels,
                dtick=1,
            ),
            yaxis=dict(title="Enforcement Actions"),
            legend=dict(
                orientation="h", yanchor="top", y=1.08, x=0.5, xanchor="center",
                font=dict(size=12),
            ),
        )
        # Add total count annotation above each bar
        for _, row in yoy_stacked.iterrows():
            fig_yoy.add_annotation(
                x=row["year"], y=row["count"],
                text=f"{int(row['count']):,}",
                showarrow=False, yshift=12,
                font=dict(size=12, weight="bold"),
            )
        st.plotly_chart(fig_yoy, use_container_width=True)

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

    # ── Data Coverage ────────────────────────────────────────────────
    st.markdown(
        '<p class="section-header">Data Coverage</p>',
        unsafe_allow_html=True,
    )

    cov_col1, cov_col2 = st.columns([3, 2])

    with cov_col1:
        if not coverage_df.empty:
            display_cov = coverage_df.copy()
            # Only show states with active records
            display_cov = display_cov[display_cov["Active Records"] > 10].copy()
            display_cov["Date Range"] = display_cov.apply(
                lambda r: f"{r['Earliest'].strftime('%b %Y') if r['Earliest'] else '—'} – {r['Latest'].strftime('%b %Y') if r['Latest'] else '—'}",
                axis=1,
            )
            display_cov["Filter Rate"] = display_cov.apply(
                lambda r: f"{(1 - r['Active Records'] / r['Total Scraped']) * 100:.0f}%" if r["Total Scraped"] > 0 else "—",
                axis=1,
            )
            st.dataframe(
                display_cov[["State", "Code", "Active Records", "Total Scraped", "Date Range", "Filter Rate"]],
                use_container_width=True,
                hide_index=True,
                height=min(len(display_cov) * 36 + 40, 400),
                column_config={
                    "State": st.column_config.TextColumn("State", width="medium"),
                    "Code": st.column_config.TextColumn("Code", width="small"),
                    "Active Records": st.column_config.NumberColumn("Enforcement Actions", format="%d"),
                    "Total Scraped": st.column_config.NumberColumn("Total Scraped", format="%d"),
                    "Date Range": st.column_config.TextColumn("Date Range", width="medium"),
                    "Filter Rate": st.column_config.TextColumn("Filter Rate", width="small"),
                },
            )

    with cov_col2:
        st.markdown("""
**Coverage is actively expanding.** The tracker currently covers **{n_states} states** with
full scraping and extraction pipelines. Each state's AG website has a unique
structure requiring custom scraper configuration.

**What "Filter Rate" means:** Not all AG press releases are enforcement
actions — AGs also publish consumer alerts, policy statements, personnel
announcements, and legislative commentary. Our two-stage filter
(keyword screen + pattern validation) removes non-enforcement content.
A higher filter rate indicates more non-enforcement content on that state's website.
""".format(n_states=total_states))

    st.markdown("**Active coverage:** CA, NY, TX, WA, MA, OH, OR — representing the largest and most active AG offices in the country. Additional states are being onboarded.")

    # ── About / Methodology ──────────────────────────────────────────
    with st.expander("About This Data / Methodology", expanded=False):
        st.markdown("""
### How It Works

The AG Enforcement Tracker uses a fully automated pipeline to collect, extract, and
structure enforcement action data from state Attorney General press releases:

1. **Scrape** — Custom scrapers collect press releases from each state AG's website,
   respecting rate limits and `robots.txt`.
2. **Filter** — A two-stage classifier (keyword screen + pattern validation) separates
   genuine enforcement actions from consumer alerts, policy statements, and other content.
3. **Extract** — Rule-based regex extractors pull structured fields: settlement amounts,
   defendant names, action types, statute citations, and dates. **No LLM is used** for
   core extraction — deterministic rules ensure consistency across thousands of records.
4. **Normalize** — Company names are resolved to canonical forms (e.g., "Google LLC",
   "Google, Inc." → "Google"). Violations are classified into a standard taxonomy.
5. **Score** — Each record receives a quality score (0.0–1.0) based on extraction
   confidence. Records below 0.1 are excluded from the active dataset.

### Data Quality

- **Action type classification accuracy:** ~88% of records are classified into specific
  action types (settlement, lawsuit filed, judgment, injunction, consent decree)
- **Entity resolution:** Major companies are tracked across all states where they appear
- **Coverage period:** 2022–present for most states (WA and TX have deeper historical archives)
- **Update frequency:** Designed for daily automated scraping

### Limitations

- Coverage is currently limited to {n_states} states. Expanding to all 50 is planned.
- Settlement amounts reflect values stated in press releases, which may differ from
  final negotiated amounts.
- Some multistate actions appear multiple times (once per participating state);
  deduplication links them but totals should be interpreted carefully.
- ~12% of records remain classified as "other" action type when headlines/body text
  don't match specific enforcement patterns.
""".format(n_states=total_states))

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
