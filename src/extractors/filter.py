"""Non-enforcement press release filter.

Two-stage approach per CLAUDE.md:
1. Keyword screen: enforcement language vs. non-enforcement language
2. Pattern validation: must have defendant-like entity AND (dollar amount OR statute OR court)

This MUST filter out:
- Consumer alerts and advisories
- Policy statements and opinion letters
- Amicus briefs
- Personnel announcements, endorsements, grant announcements
- Legislative testimony

This MUST keep:
- Lawsuits filed, settlements, consent decrees
- Multistate coalition announcements with enforcement components
- Sentencing announcements
"""

import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Stage 1: Keyword Screen
# ---------------------------------------------------------------------------

# Strong enforcement indicators — if ANY of these appear, pass to stage 2
_ENFORCEMENT_KEYWORDS = [
    "settlement",
    "consent decree",
    "lawsuit",
    "complaint filed",
    "assurance of discontinuance",
    "civil penalty",
    "civil penalties",
    "injunctive relief",
    "injunction",
    "violated",
    "enforcement action",
    "sentenced",
    "sentencing",
    "convicted",
    "conviction",
    "indictment",
    "indicted",
    "pleaded guilty",
    "pled guilty",
    "plea agreement",
    "judgment",
    "restitution",
    "sues",
    "sued",
    "files suit",
    "filed suit",
    "files complaint",
    "filed complaint",
    "files action",
    "filed action",
    "files lawsuit",
    "filed lawsuit",
    "legal action",
    "cease and desist",
    "preliminary injunction",
    "permanent injunction",
    "consent order",
    "stipulated order",
    "held accountable",
]

# Non-enforcement indicators — if ONLY these appear (no enforcement keywords), reject
_NON_ENFORCEMENT_KEYWORDS = [
    "consumer alert",
    "consumer tips",
    "consumer advisory",
    "advisory",
    "awareness",
    "recognizes",
    "congratulates",
    "testimony",
    "legislative",
    "amicus brief",
    "friend of the court",
    "endorses",
    "endorsement",
    "supports",
    "supports rules",
    "supports legislation",
    "issues statement",
    "issues guidance",
    "regional convening",
    "reminds consumers",
    "warns consumers",
    "highlights",
    "releases report",
    "releases bulletin",
    "releases data",
    "annual report",
    "celebrating",
    "celebrates",
    "appointed",
    "appointment",
    "grant announcement",
    "volunteers",
    "sponsored bill",
    "signed into law",
    "opens investigation",
    "investigating officer",
    "investigating shooting",
    "awareness month",
    "heritage month",
    "comment letter",
    "testimony before",
    "urges",
    "calls on",
    "applauds",
    "commends",
    "welcomes",
]


# Headline-level override patterns — if the headline strongly indicates non-enforcement,
# override even if enforcement keywords appear in the body (e.g., "issues statement on
# legislation authorizing civil penalties" — the civil penalties are in legislation context)
_HEADLINE_NON_ENFORCEMENT_RE = [
    re.compile(r'issues?\s+(?:a\s+)?statement\s+(?:on|regarding|following|in\s+response)', re.IGNORECASE),
    re.compile(r'(?:sponsored|authored)\s+bill', re.IGNORECASE),
    re.compile(r'signed\s+into\s+law', re.IGNORECASE),
    re.compile(r'releases?\s+(?:updated\s+)?(?:guide|report|bulletin|data)', re.IGNORECASE),
    re.compile(r'warns?\s+consumers', re.IGNORECASE),
    re.compile(r'reminds?\s+(?:consumers|californians|immigrants)', re.IGNORECASE),
    re.compile(r'investigating\s+(?:officer|shooting)', re.IGNORECASE),
    re.compile(r'announces?\s+appointment', re.IGNORECASE),
    re.compile(r'(?:urges?|calls?\s+on|call\s+on)\s+', re.IGNORECASE),
    re.compile(r'(?:applauds?|commends?|welcomes?|praises?)\s+', re.IGNORECASE),
    re.compile(r'(?:comment|letter)\s+(?:to|on|regarding)\s+', re.IGNORECASE),
    re.compile(r'(?:heritage|awareness)\s+month', re.IGNORECASE),
]


def _keyword_screen(headline: str, body_first_500: str) -> str:
    """Stage 1: Keyword screen.

    Returns:
        "pass" — enforcement keywords found, proceed to stage 2
        "reject" — non-enforcement keywords only, filter out
        "ambiguous" — neither strong signal, proceed to stage 2 for safety
    """
    combined = (headline + " " + body_first_500).lower()

    # Check headline-level overrides first — these are strong non-enforcement signals
    # that override enforcement keywords appearing in body/context
    if any(p.search(headline) for p in _HEADLINE_NON_ENFORCEMENT_RE):
        # Even if enforcement keywords appear (e.g., "civil penalties" in legislation context),
        # the headline pattern indicates this is NOT an enforcement action
        return "reject"

    has_enforcement = any(kw in combined for kw in _ENFORCEMENT_KEYWORDS)
    has_non_enforcement = any(kw in combined for kw in _NON_ENFORCEMENT_KEYWORDS)

    if has_enforcement:
        return "pass"
    elif has_non_enforcement:
        return "reject"
    else:
        return "ambiguous"


# ---------------------------------------------------------------------------
# Stage 2: Pattern Validation
# ---------------------------------------------------------------------------

# Dollar amount pattern
_HAS_DOLLAR = re.compile(r'\$\s*[\d,]+(?:\.\d+)?\s*(?:million|billion|thousand)?', re.IGNORECASE)

# Statute/law citation pattern
_HAS_STATUTE = re.compile(
    r'(?:\b\w+\s+Code\s+(?:section|§)|U\.?S\.?C\.?\s*§|\b(?:Act|Law)\b)',
    re.IGNORECASE,
)

# Court name pattern
_HAS_COURT = re.compile(
    r'(?:(?:Superior|District|Circuit|Federal|Supreme)\s+Court|filed\s+in\s+(?:the\s+)?court)',
    re.IGNORECASE,
)

# Defendant-like entity patterns (company or person in accusatory context)
_HAS_DEFENDANT = re.compile(
    r'(?:'
    r'(?:against|with)\s+[\w\s,.&\'-]{3,60}(?:Inc|Corp|LLC|Ltd|Co\.|Company|Group)|'  # "against [Company]"
    r'(?:defendant|respondent)s?\s+[\w\s,.&\'-]{3,60}|'
    r'(?:sues?|sued)\s+[\w\s,.&\'-]{3,60}|'
    r'settlement\s+with\s+[\w\s,.&\'-]{3,60}'
    r')',
    re.IGNORECASE,
)


def _pattern_validation(headline: str, body_text: str) -> bool:
    """Stage 2: Pattern validation.

    Requires at least one defendant-like entity AND at least one of:
    - A dollar amount
    - A statute citation
    - A court name

    Returns True if the press release looks like an enforcement action.
    """
    combined = headline + " " + body_text

    has_defendant = bool(_HAS_DEFENDANT.search(combined))
    has_dollar = bool(_HAS_DOLLAR.search(combined))
    has_statute = bool(_HAS_STATUTE.search(combined))
    has_court = bool(_HAS_COURT.search(combined))

    return has_defendant and (has_dollar or has_statute or has_court)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass
class FilterResult:
    """Result of the non-enforcement filter."""
    is_enforcement: bool
    stage: str  # "keyword_pass", "keyword_reject", "pattern_pass", "pattern_reject"
    reason: str


def is_enforcement_action(headline: str, body_text: str) -> FilterResult:
    """Determine whether a press release is an enforcement action.

    Uses the two-stage approach from CLAUDE.md:
    1. Keyword screen
    2. Pattern validation (for keyword-pass and ambiguous cases)

    Args:
        headline: The press release headline/title.
        body_text: The full body text of the press release.

    Returns:
        FilterResult with is_enforcement flag, stage, and reason.
    """
    body_first_500 = body_text[:500]

    # Stage 1: Keyword screen
    screen = _keyword_screen(headline, body_first_500)

    if screen == "reject":
        return FilterResult(
            is_enforcement=False,
            stage="keyword_reject",
            reason="Only non-enforcement keywords found (consumer alert, policy statement, etc.)",
        )

    # Stage 2: Pattern validation (for "pass" and "ambiguous" results)
    if _pattern_validation(headline, body_text):
        return FilterResult(
            is_enforcement=True,
            stage="pattern_pass" if screen == "pass" else "pattern_pass_ambiguous",
            reason="Enforcement keywords and/or defendant + enforcement pattern found",
        )

    # Keyword screen passed but pattern validation failed
    if screen == "pass":
        # Enforcement keywords were present but no clear defendant/amount/statute pattern
        # Still include with lower confidence per CLAUDE.md: "false inclusions are less damaging"
        return FilterResult(
            is_enforcement=True,
            stage="keyword_pass_only",
            reason="Enforcement keywords found but no defendant/amount/statute pattern",
        )

    # Ambiguous with no patterns — reject
    return FilterResult(
        is_enforcement=False,
        stage="pattern_reject",
        reason="No enforcement keywords or patterns found",
    )
