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
    "encourages students",
    "high school students",
    "teen ambassador",
    "conceal carry",
    "concealed carry",
    "peace officer",
    "fallen officer",
    "gun buyback",
    "open carry",
    "legal explainer",
    "legal analysis",
    "provides update",
    "provides statement",
    "solicitor general",
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
    re.compile(r'(?:honors?|mourns?|remembers?|salutes?)\s+(?:fallen|slain)', re.IGNORECASE),
    re.compile(r'statement\s+on\s+(?:passing|death|fallen|shooting)', re.IGNORECASE),
    re.compile(r'(?:gun\s+buyback|guns?\s+turned\s+in)', re.IGNORECASE),
    re.compile(r'(?:issues?\s+(?:legal\s+)?(?:opinion|advisory))\b', re.IGNORECASE),
    re.compile(r'encourages?\s+(?:students|residents|high\s+school)', re.IGNORECASE),
    re.compile(r'announces?\s+(?:appointment|tour|town\s+hall)', re.IGNORECASE),
    re.compile(r'(?:kicks?\s+off|launches?)\s+(?:year|program|tour)', re.IGNORECASE),
    # "Warns Against X" / "Warns [State Residents]" — consumer alerts, not enforcement
    re.compile(r'warns?\s+(?:against|about|of\s+(?:potential|scam|fraud))\b', re.IGNORECASE),
    re.compile(r'warns?\s+(?:new\s+yorkers|texans|californians|ohioans|oregonians|residents)\b', re.IGNORECASE),
    # Statements and open letters (political/policy, not enforcement)
    re.compile(r'(?:statement\s+(?:from|on)\b)', re.IGNORECASE),
    re.compile(r'(?:issues?|releases?|publishes?)\s+(?:an?\s+)?open\s+letter\b', re.IGNORECASE),
    # Amicus briefs with "joins/signs/files"
    re.compile(r'(?:joins?|signs?|submits?|files?)\s+(?:an?\s+)?(?:amicus|friend.of.the.court)\s+brief', re.IGNORECASE),
    # Advocacy — "leads/joins effort/brief supporting/opposing"
    re.compile(r'(?:leads?|joins?)\s+(?:[\w\s-]+)?(?:effort|brief|letter)\s+(?:supporting|opposing|urging|calling)', re.IGNORECASE),
    # Political criticism / opinion
    re.compile(r'(?:condemns?|vows?\s+to|pledges?\s+to)\s+', re.IGNORECASE),
    # Administrative / informational
    re.compile(r'(?:holds?\s+(?:kickoff|meeting|convening|summit))\b', re.IGNORECASE),
    re.compile(r'(?:seeks?\s+(?:students|volunteers|applicants|high.school))\b', re.IGNORECASE),
    re.compile(r'(?:invites?\s+(?:students|ohio|high.school))\b', re.IGNORECASE),
    re.compile(r'(?:application\s+deadline|apply\s+for)\b', re.IGNORECASE),
    re.compile(r'(?:remembering|in\s+memory\s+of|legacy\s+of)\b', re.IGNORECASE),
    # Policy-focused AG statements about federal actions
    re.compile(r'(?:trump|biden)\s+administration.{0,20}s?\s+(?:illegal|unlawful|threatens?|attempt)', re.IGNORECASE),
    re.compile(r'(?:to\s+consumers?:)', re.IGNORECASE),
    re.compile(r'(?:issues?\s+(?:warning|legal\s+alert))\b', re.IGNORECASE),
    re.compile(r'(?:focuses?\s+on|questions?$|has\s+questions$)', re.IGNORECASE),
    # Reports, studies, data releases
    re.compile(r'(?:study\s+shows|did\s+not\s+drive|change\s+in\s+(?:concealed|carry|law))\b', re.IGNORECASE),
    re.compile(r'(?:releases?\s+(?:yellow\s+book|annual|20\d{2}))', re.IGNORECASE),
    # Missing persons / BCI / non-enforcement AG office functions
    re.compile(r'(?:age.progression|missing\s+(?:cleveland|man|woman|person|child)|identity\s+restored)', re.IGNORECASE),
    re.compile(r'(?:peace\s+officers?\s+(?:memorial|basic\s+training|ceremony))\b', re.IGNORECASE),
    re.compile(r'(?:concealed?\s+carry\s+report|conceal\s+carry\s+report)\b', re.IGNORECASE),
    # Broad: AG as political actor, not enforcer
    re.compile(r'(?:provides?\s+(?:legal\s+)?(?:analysis|update|explainer))\b', re.IGNORECASE),
    re.compile(r'(?:announces?\s+pick\s+for|new\s+(?:solicitor|deputy|chief))\b', re.IGNORECASE),
    re.compile(r'(?:preserves?\s+(?:its|california|ability))\b', re.IGNORECASE),
    # Consumer alerts in headline override body enforcement keywords
    re.compile(r'consumer\s+alert', re.IGNORECASE),
    # "Alerts/Reminds [group]" — advisories
    re.compile(r'(?:alerts?|reminds?)\s+(?:businesses|city\s+attorneys|consumers|residents|new\s+yorkers|texans|californians)', re.IGNORECASE),
    # Guidance issuances
    re.compile(r'(?:issues?|provides?|releases?)\s+(?:[\w\s]+)?(?:guidance|revised\s+(?:legal\s+)?guidance)', re.IGNORECASE),
    # Advocacy coalitions (support/oppose policy, not enforcement)
    re.compile(r'(?:leads?|joins?|co-?leads?)\s+(?:[\w\s-]+)?(?:coalition|effort|brief)\s+(?:[\w\s]+)?(?:support|oppos|urg|call|defend)', re.IGNORECASE),
    # Personnel actions
    re.compile(r'(?:names?\s+new|promotes?\s+|establishes?\s+)', re.IGNORECASE),
    # Know your rights / informational
    re.compile(r'(?:know\s+your\s+rights|remains?\s+in\s+effect|certif(?:y|ies)\s+(?:\d+\s+)?(?:initiative|petition))', re.IGNORECASE),
    # "Bills to..." — legislation
    re.compile(r'^bills?\s+to\b', re.IGNORECASE),
    # "To Congress / To U.S. Supreme Court" (advocacy briefs)
    re.compile(r'(?:to\s+(?:congress|u\.?s\.?\s+supreme\s+court):)', re.IGNORECASE),
    # "Stands with" — political solidarity
    re.compile(r'stands?\s+with\b', re.IGNORECASE),
    # Voting / election protection
    re.compile(r'(?:vote\s+early|voter\s+protection|election\s+integrity\s+law)', re.IGNORECASE),
    # "It Remains Illegal to" — informational
    re.compile(r'(?:remains?\s+illegal\s+to|it\s+remains?\s+illegal)', re.IGNORECASE),
    # "Responds to Court Decision" (commentary, not AG's own action)
    re.compile(r'responds?\s+to\s+(?:court|supreme|u\.?s\.?)', re.IGNORECASE),
    # Puts on notice / on notice for (warning, not enforcement action)
    re.compile(r'puts?\s+(?:[\w\s]+)?on\s+notice\b', re.IGNORECASE),
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
