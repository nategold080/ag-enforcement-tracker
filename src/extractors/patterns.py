"""Regex and rule-based extraction patterns for AG enforcement press releases.

This module contains deterministic extractors for structured fields:
- Dollar amounts (settlement values, penalties, restitution)
- Dates (filing dates, resolution dates)
- Statute citations
- Defendant names (rule-based extraction from headline and body patterns)
- Action type classification
"""

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import dateparser
import yaml


# ---------------------------------------------------------------------------
# Dollar Amount Extraction
# ---------------------------------------------------------------------------

# Pattern matches: $1.5 million, $3,500,000, $500,000.00, $1.2 billion, etc.
_DOLLAR_RE = re.compile(
    r'\$\s*'
    r'([\d,]+(?:\.\d+)?)'     # numeric part: 3,500,000 or 1.5
    r'\s*'
    r'(million|billion|thousand|[MBKmbk](?=\b|\s|[^a-zA-Z]))?',  # optional multiplier (incl. M/B/K abbreviations)
    re.IGNORECASE,
)

_MULTIPLIERS = {
    "thousand": Decimal("1000"),
    "million": Decimal("1000000"),
    "billion": Decimal("1000000000"),
    "k": Decimal("1000"),
    "m": Decimal("1000000"),
    "b": Decimal("1000000000"),
}

_APPROX_RE = re.compile(
    r'(?:approximately|about|roughly|nearly|up\s+to|more\s+than|over|at\s+least|exceed)',
    re.IGNORECASE,
)


@dataclass
class ExtractedAmount:
    raw_text: str
    amount: Decimal
    is_estimated: bool


def extract_dollar_amounts(text: str) -> list[ExtractedAmount]:
    """Extract all dollar amounts from text, returning structured results."""
    results = []
    for match in _DOLLAR_RE.finditer(text):
        raw = match.group(0)
        num_str = match.group(1).replace(",", "")
        multiplier_word = match.group(2)

        try:
            value = Decimal(num_str)
        except InvalidOperation:
            continue

        if multiplier_word:
            value *= _MULTIPLIERS.get(multiplier_word.lower(), Decimal("1"))

        # Check for approximation language in the 40 chars before the match
        start = max(0, match.start() - 40)
        preceding = text[start:match.start()]
        is_estimated = bool(_APPROX_RE.search(preceding))

        results.append(ExtractedAmount(raw_text=raw, amount=value, is_estimated=is_estimated))

    return results


def extract_largest_dollar_amount(text: str) -> Optional[ExtractedAmount]:
    """Return the largest dollar amount found in the text, or None."""
    amounts = extract_dollar_amounts(text)
    if not amounts:
        return None
    return max(amounts, key=lambda a: a.amount)


# Settlement-context dollar amount pattern — looks for "$X" near settlement/penalty keywords
_SETTLEMENT_CONTEXT_RE = re.compile(
    r'(?:settl(?:ement|ed|es|ing)|penalt(?:y|ies)|restitution|judgment|consent\s+decree|pay(?:ing)?)\s+'
    r'(?:of\s+|totaling\s+|worth\s+|valued?\s+at\s+|for\s+)?'
    r'(?:(?:approximately|about|nearly|over|more\s+than|at\s+least|up\s+to)\s+)?'
    r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|thousand)?',
    re.IGNORECASE,
)

_HEADLINE_DOLLAR_RE = re.compile(
    r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|thousand)?',
    re.IGNORECASE,
)


_NON_SETTLEMENT_CONTEXT_RE = re.compile(
    r'(?:grant|funding|appropriat|budget|federal\s+aid|allocat|'
    r'CDC|HHS|FEMA|federal\s+funds?|tax\s+(?:relief|cut|credit|revenue)|'
    r'government\s+(?:funding|spending)|legislative|'
    r'executive\s+order|stimulus|infrastructure\s+(?:funding|investment)|'
    r'seiz(?:ed?|ure|ing)|fentanyl|trafficking|narcotic|border|'
    r'gross\s+domestic|GDP|economy|economic\s+(?:impact|growth|loss)|'
    r'revenue|sales|market\s+(?:cap|value)|stock|share\s+price|'
    r'annual\s+(?:revenue|budget|sales)|industry|sector|'
    # Student loan / debt relief programs (policy, not settlements)
    r'student\s+loan|debt\s+(?:forgiveness|cancellation|discharge)|'
    # Hypothetical costs/harms in policy opinion pieces
    r'would\s+(?:cause|cost|harm|result|lose|destroy|eliminate)|irreparable|'
    # Agency dismantling / elimination (policy advocacy)
    r'dismantl|'
    # Federal agency impact amounts (not AG settlement amounts)
    r'CFPB|Consumer\s+Financial\s+Protection\s+Bureau|'
    # Proposed legislation / rulemaking cost estimates
    r'proposed\s+(?:rule|regulation|legislation|bill)|'
    # Wage amounts — "$1 per day" is not a settlement
    r'wage|per\s+(?:hour|day|week|month|year)|hourly\s+rate|minimum\s+wage|'
    # Ballot measures / initiatives — numbers parsed as dollars
    r'initiative|ballot\s+measure|proposition|I-\d+|'
    # Hypothetical exposure amounts — "could face up to $X"
    r'exposed?\s+to|risk\s+of|face(?:s|d)?\s+(?:up\s+to)|could\s+face|'
    r'maximum\s+(?:penalty|fine)\s+of|up\s+to\s+\$)',
    re.IGNORECASE,
)


def _is_non_settlement_context(text: str, match_start: int) -> bool:
    """Check if a dollar amount is in a non-settlement context (grants, funding, etc.)."""
    # Check 80 chars around the match
    start = max(0, match_start - 80)
    end = min(len(text), match_start + 80)
    context = text[start:end]
    return bool(_NON_SETTLEMENT_CONTEXT_RE.search(context))


def extract_settlement_amount(headline: str, body: str) -> Optional[ExtractedAmount]:
    """Extract the most relevant settlement/penalty dollar amount.

    Priority:
    1. Dollar amount in the headline (strongest signal — editors put the key number there)
    2. Dollar amount near settlement/penalty keywords in body text
    3. Fallback: largest dollar amount in body (with contextual guards)

    Filters out dollar amounts in non-settlement contexts (grants, funding, etc.).
    """
    # Universal sanity cap: no single AG enforcement action has exceeded $30B.
    # The largest was the $26B opioid distributor settlement.
    _MAX_SETTLEMENT = Decimal("30000000000")  # $30B

    # Fix missing spaces from soft-hyphen stripping (TX headlines: "$168Mfor" → "$168M for")
    headline = _fix_headline_spacing(headline)

    # Priority 1: Headline amount (but check for non-settlement context)
    headline_amounts = extract_dollar_amounts(headline)
    if headline_amounts:
        settlement_amounts = [
            a for a in headline_amounts
            if not _is_non_settlement_context(headline, headline.find(a.raw_text))
            and a.amount <= _MAX_SETTLEMENT
        ]
        if settlement_amounts:
            return max(settlement_amounts, key=lambda a: a.amount)

    # Priority 2: Amount in settlement context in body
    match = _SETTLEMENT_CONTEXT_RE.search(body)
    if match:
        # Verify it's not in a grant/funding context
        if not _is_non_settlement_context(body, match.start()):
            num_str = match.group(1).replace(",", "")
            try:
                value = Decimal(num_str)
                multiplier_word = match.group(2)
                if multiplier_word:
                    value *= _MULTIPLIERS.get(multiplier_word.lower(), Decimal("1"))
                if value <= _MAX_SETTLEMENT:
                    raw = match.group(0)
                    start = max(0, match.start() - 40)
                    preceding = body[start:match.start()]
                    is_estimated = bool(_APPROX_RE.search(preceding))
                    return ExtractedAmount(raw_text=raw, amount=value, is_estimated=is_estimated)
            except InvalidOperation:
                pass

    # Priority 3: Fallback to largest amount in first 1500 chars of body only
    body_head = _fix_headline_spacing(body[:1500])
    amounts = extract_dollar_amounts(body_head)
    if not amounts:
        return None
    filtered = [
        a for a in amounts
        if not _is_non_settlement_context(body_head, body_head.find(a.raw_text))
        and a.amount <= _MAX_SETTLEMENT
    ]
    if not filtered:
        return None
    return max(filtered, key=lambda a: a.amount)


# ---------------------------------------------------------------------------
# Penalty/Restitution Classification
# ---------------------------------------------------------------------------

_PENALTY_PATTERNS = [
    (re.compile(r'civil\s+penalt(?:y|ies)\s+(?:of\s+)?\$\s*([\d,.]+)\s*(million|billion|thousand)?', re.IGNORECASE), "civil_penalty"),
    (re.compile(r'(?:consumer\s+)?restitution\s+(?:of\s+)?\$\s*([\d,.]+)\s*(million|billion|thousand)?', re.IGNORECASE), "consumer_restitution"),
    (re.compile(r'(?:attorney.?s?\s+)?fees?\s+(?:and\s+costs?\s+)?(?:of\s+)?\$\s*([\d,.]+)\s*(million|billion|thousand)?', re.IGNORECASE), "fees_and_costs"),
]


def classify_monetary_components(text: str) -> dict[str, Decimal]:
    """Extract categorized monetary components from text."""
    components: dict[str, Decimal] = {}
    for pattern, label in _PENALTY_PATTERNS:
        match = pattern.search(text)
        if match:
            num_str = match.group(1).replace(",", "")
            try:
                value = Decimal(num_str)
                multiplier_word = match.group(2)
                if multiplier_word:
                    value *= _MULTIPLIERS.get(multiplier_word.lower(), Decimal("1"))
                components[label] = value
            except (InvalidOperation, IndexError):
                continue
    return components


# ---------------------------------------------------------------------------
# Date Extraction
# ---------------------------------------------------------------------------

_FILED_DATE_RE = re.compile(
    r'(?:filed|filing)\s+(?:on\s+)?(\w+\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE,
)

_RESOLVED_DATE_RE = re.compile(
    r'(?:settled|resolved|entered|approved|signed)\s+(?:on\s+)?(\w+\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE,
)


def extract_announced_date(text: str) -> Optional[date]:
    """Extract the publication/announcement date from press release text.

    Searches progressively deeper into the text: first 300 chars, then 1000,
    then 2000. Many press releases have navigation boilerplate at the top
    (especially Wayback Machine captures), so the actual date may be further in.
    """
    _date_re = re.compile(
        r'((?:January|February|March|April|May|June|July|August|September|'
        r'October|November|December)\s+\d{1,2},?\s+\d{4})'
    )

    # Search progressively deeper
    for limit in [300, 1000, 2000]:
        header = text[:limit]

        # Pattern: "Month DD, YYYY" — the most common format
        match = _date_re.search(header)
        if match:
            parsed = dateparser.parse(match.group(1))
            if parsed and 2018 <= parsed.year <= 2030:
                return parsed.date()

        # Pattern: "MM/DD/YYYY" or "MM-DD-YYYY"
        match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})', header)
        if match:
            parsed = dateparser.parse(match.group(1))
            if parsed and 2018 <= parsed.year <= 2030:
                return parsed.date()

    return None


def extract_filed_date(text: str) -> Optional[date]:
    """Extract a filing date from press release text."""
    match = _FILED_DATE_RE.search(text)
    if match:
        parsed = dateparser.parse(match.group(1))
        if parsed:
            return parsed.date()
    return None


def extract_resolved_date(text: str) -> Optional[date]:
    """Extract a resolution/settlement date from press release text."""
    match = _RESOLVED_DATE_RE.search(text)
    if match:
        parsed = dateparser.parse(match.group(1))
        if parsed:
            return parsed.date()
    return None


# ---------------------------------------------------------------------------
# Statute Citation Extraction
# ---------------------------------------------------------------------------

_STATUTE_PATTERNS = [
    # California statutes: "Business and Professions Code section 17200"
    re.compile(r'(?:California\s+)?(\w[\w\s]+?)\s+Code\s+(?:section|§§?)\s*([\d.]+(?:\s*(?:et\s+seq|through)\s*[\d.]*)?)', re.IGNORECASE),
    # Federal statutes by USC: "15 U.S.C. § 45"
    re.compile(r'(\d+)\s+U\.?S\.?C\.?\s*§?\s*([\d]+(?:\([a-z]\))?)', re.IGNORECASE),
    # Common law names (acronyms and full names)
    re.compile(r'\b(Sherman\s+Act|Clayton\s+Act|FTC\s+Act|False\s+Claims\s+Act|'
               r'CCPA|COPPA|TCPA|CAN-SPAM|HIPAA|FCRA|RICO|UDAP|ADA|FLSA|OSHA|ECOA|TILA|RESPA|FDCPA|'
               r'Unfair\s+Competition\s+Laws?|UCL|CLRA|'
               r'Telephone\s+Consumer\s+Protection\s+Act|Telemarketing\s+Sales\s+Rule|'
               r'Truth\s+in\s+Caller\s+ID\s+Act|Truth\s+in\s+Lending\s+Act|'
               r'Anti-?Kickback\s+Statute|Stark\s+Law|Unclaimed\s+Property\s+Law|'
               r'Clean\s+Air\s+Act|Clean\s+Water\s+Act|Fair\s+Housing\s+Act|'
               r'Fair\s+Credit\s+Reporting\s+Act|Fair\s+Debt\s+Collection\s+Practices\s+Act|'
               r'Dodd-?Frank\s+Act|Consumer\s+Legal\s+Remedies\s+Act|'
               r'Consumer\s+Fraud\s+Act|Elder\s+Abuse\s+Act|'
               r'Real\s+Estate\s+Settlement\s+Procedures\s+Act|'
               r'Equal\s+Credit\s+Opportunity\s+Act|'
               r'Racketeer\s+Influenced\s+and\s+Corrupt\s+Organizations\s+Act'
               r')\b', re.IGNORECASE),
    # "in violation of" / "violating" pattern (allows commas for lists of laws)
    re.compile(r'(?:in\s+violation\s+of|violat(?:ed|ing))\s+(?:the\s+|state\s+)?(?:[\w\s,]+?\s+)((?:Act|Laws?|Code|Statute|Rule|Regulation)s?)\b', re.IGNORECASE),
]


@dataclass
class ExtractedStatute:
    raw_citation: str
    is_state: bool
    is_federal: bool
    common_name: str


_FEDERAL_INDICATORS = {
    "u.s.c.", "usc", "federal", "ftc", "sherman", "clayton", "can-spam",
    "hipaa", "fcra", "coppa", "tcpa", "rico", "dodd-frank", "respa", "tila",
    "ecoa", "ada", "flsa", "osha", "stark", "anti-kickback", "clean air",
    "clean water", "fair housing act", "fdcpa", "telemarketing sales rule",
    "truth in caller", "truth in lending",
}
_STATE_INDICATORS = {
    "california", "business and professions", "civil code", "health and safety",
    "penal code", "ccpa", "ucl", "unfair competition", "clra", "consumer legal remedies",
    "unclaimed property", "udap", "consumer fraud act", "consumer protection act",
}


_STATUTE_COMMON_NAMES: list[tuple[re.Pattern, str]] = [
    # Federal statutes
    (re.compile(r'ccpa|consumer privacy act', re.IGNORECASE), "CCPA"),
    (re.compile(r'coppa|children.s online privacy', re.IGNORECASE), "COPPA"),
    (re.compile(r'tcpa|telephone consumer protection', re.IGNORECASE), "TCPA"),
    (re.compile(r'hipaa|health insurance portability', re.IGNORECASE), "HIPAA"),
    (re.compile(r'fcra|fair credit reporting', re.IGNORECASE), "FCRA"),
    (re.compile(r'can.spam', re.IGNORECASE), "CAN-SPAM"),
    (re.compile(r'clean air act', re.IGNORECASE), "Clean Air Act"),
    (re.compile(r'clean water act', re.IGNORECASE), "Clean Water Act"),
    (re.compile(r'\brico\b|racketeer influenced', re.IGNORECASE), "RICO"),
    (re.compile(r'dodd.frank', re.IGNORECASE), "Dodd-Frank Act"),
    (re.compile(r'respa|real estate settlement', re.IGNORECASE), "RESPA"),
    (re.compile(r'tila|truth in lending', re.IGNORECASE), "TILA"),
    (re.compile(r'ecoa|equal credit opportunity', re.IGNORECASE), "ECOA"),
    (re.compile(r'\bada\b|americans with disabilities', re.IGNORECASE), "ADA"),
    (re.compile(r'flsa|fair labor standards', re.IGNORECASE), "FLSA"),
    (re.compile(r'osha|occupational safety', re.IGNORECASE), "OSHA"),
    (re.compile(r'stark law', re.IGNORECASE), "Stark Law"),
    (re.compile(r'anti.kickback statute', re.IGNORECASE), "Anti-Kickback Statute"),
    (re.compile(r'ftc act|federal trade commission act', re.IGNORECASE), "FTC Act"),
    (re.compile(r'sherman act|sherman\s', re.IGNORECASE), "Sherman Act"),
    (re.compile(r'clayton act|clayton\s', re.IGNORECASE), "Clayton Act"),
    (re.compile(r'false claims act|false claims', re.IGNORECASE), "False Claims Act"),
    (re.compile(r'telemarketing sales rule', re.IGNORECASE), "Telemarketing Sales Rule"),
    (re.compile(r'truth in caller', re.IGNORECASE), "Truth in Caller ID Act"),
    (re.compile(r'fair housing act', re.IGNORECASE), "Fair Housing Act"),
    (re.compile(r'fair debt collection', re.IGNORECASE), "FDCPA"),
    (re.compile(r'elder abuse', re.IGNORECASE), "Elder Abuse Act"),
    (re.compile(r'unfair\s+(?:and\s+)?deceptive.*(?:act|practices)|udap', re.IGNORECASE), "UDAP"),
    (re.compile(r'consumer fraud act', re.IGNORECASE), "Consumer Fraud Act"),
    # California-specific statutes
    (re.compile(r'business and professions code\s+(?:section\s+)?(?:§§?\s*)?172', re.IGNORECASE), "UCL"),
    (re.compile(r'ucl|unfair competition law', re.IGNORECASE), "UCL"),
    (re.compile(r'civil code\s+(?:section\s+)?(?:§§?\s*)?175', re.IGNORECASE), "CLRA"),
    (re.compile(r'consumers?\s+legal\s+remedies', re.IGNORECASE), "CLRA"),
    (re.compile(r'unclaimed property', re.IGNORECASE), "Unclaimed Property Law"),
]


def _resolve_statute_common_name(raw_lower: str) -> str:
    """Map raw statute text to a common name."""
    for pattern, name in _STATUTE_COMMON_NAMES:
        if pattern.search(raw_lower):
            return name
    return ""


def extract_statutes(text: str) -> list[ExtractedStatute]:
    """Extract statute citations from press release text."""
    results = []
    seen_raw: set[str] = set()

    for pattern in _STATUTE_PATTERNS:
        for match in pattern.finditer(text):
            raw = match.group(0).strip()
            if raw in seen_raw:
                continue
            seen_raw.add(raw)

            raw_lower = raw.lower()
            is_federal = any(ind in raw_lower for ind in _FEDERAL_INDICATORS)
            is_state = any(ind in raw_lower for ind in _STATE_INDICATORS)

            # Determine common name
            common_name = _resolve_statute_common_name(raw_lower)

            results.append(ExtractedStatute(
                raw_citation=raw,
                is_state=is_state,
                is_federal=is_federal,
                common_name=common_name,
            ))

    return results


# ---------------------------------------------------------------------------
# Defendant Name Extraction
# ---------------------------------------------------------------------------

# Blocklist: phrases that regex patterns match but are NOT defendant names.
# Lowercased for comparison.
def _load_defendant_blocklist() -> tuple[set[str], list[re.Pattern]]:
    """Load the defendant blocklist from config/defendant_blocklist.yaml.

    Returns (exact_matches_set, compiled_patterns_list).
    """
    blocklist_path = Path(__file__).resolve().parent.parent.parent / "config" / "defendant_blocklist.yaml"
    if not blocklist_path.exists():
        return set(), []

    with open(blocklist_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    exact = {entry.lower().strip() for entry in data.get("exact_matches", [])}
    patterns = [re.compile(p, re.IGNORECASE) for p in data.get("patterns", [])]
    return exact, patterns


_DEFENDANT_BLOCKLIST, _DEFENDANT_BLOCKLIST_PATTERNS = _load_defendant_blocklist()


def _is_valid_defendant_name(name: str) -> bool:
    """Check if a candidate name looks like a real defendant (person or company)."""
    name_stripped = name.strip()
    name_lower = name_stripped.lower()

    # Too short or too long — real defendant names are at least 3 chars
    # and single-letter names are never real entities
    if len(name_stripped) < 3 or len(name_stripped) > 120:
        return False

    # Single word must be at least 3 chars (reject "U", "It"-type noise)
    # 3-char acronyms like CVS, IBM, 3M are valid company names
    words = name_stripped.split()
    if len(words) == 1 and len(name_stripped) < 3:
        return False

    # Check exact blocklist
    if name_lower in _DEFENDANT_BLOCKLIST:
        return False

    # Check blocklist patterns
    for pattern in _DEFENDANT_BLOCKLIST_PATTERNS:
        if pattern.search(name_stripped):
            return False

    # Must contain at least one letter
    if not re.search(r'[a-zA-Z]', name_stripped):
        return False

    # Reject names with "that Would/Could/Should" — always sentence fragments
    if re.search(r'\bthat\s+(?:would|could|should|will|may|might|did)\b', name_stripped, re.IGNORECASE):
        return False

    # Reject "Name, Descriptor" patterns where after-comma text is not a name
    # e.g., "Disney, Largest CCPA Settlement" — keep "Disney" but reject the whole string
    if ',' in name_stripped:
        after_comma = name_stripped.split(',', 1)[1].strip()
        if re.match(r'(?:largest|biggest|first|major|historic|record|significant)\b', after_comma, re.IGNORECASE):
            return False

    # Reject names containing "for [gerund/adjective]" — headline fragments like
    # "TurboTax Owner Intuit for Deceiving Low-" or "Company for Misleading"
    if re.search(r'\bfor\s+(?:deceiving|misleading|defrauding|violating|failing|scamming|'
                 r'harming|exploiting|overcharging|deceptive|illegal|unlawful|false|unfair)',
                 name_stripped, re.IGNORECASE):
        return False

    # Reject "[Place] man/woman/business/resident" patterns (WA headline style)
    if re.match(r'^[A-Z]\w+(?:\s+[A-Z]\w+)?\s+(?:man|woman|men|women|business|resident|couple|family)$',
                name_stripped, re.IGNORECASE):
        return False

    # Reject names that end with a trailing hyphen (truncated fragments)
    if name_stripped.endswith('-'):
        return False

    # Reject generic industry/crypto/e-cigarette terms as defendant names
    # Matches both standalone terms ("E-Cigarette") and prefixed names
    # ("Crypto Firm Genesis Global Capital", "Cryptocurrency Companies Gemini, Genesis")
    if re.match(r'^(?:e-?cigarette|cryptocurrency|crypto|vaping|mortgage|tobacco)\s*'
                r'(?:platform|companies|company|firm|firms|exchange|exchanges|lender|'
                r'lenders|broker|brokers|servicer|servicers)?s?(?:\s|,|$)',
                name_stripped, re.IGNORECASE):
        return False

    # Reject if it's mostly lowercase words (sentence fragments, not names)
    words = name_stripped.split()
    if len(words) > 3:
        lowercase_words = sum(1 for w in words if w[0].islower() and w not in ('and', 'of', 'the', 'de', 'von', 'van'))
        if lowercase_words > len(words) * 0.6:
            return False

    return True


# Headline patterns: "AG Sues [Company]", "Settlement with [Company]"
_HEADLINE_DEFENDANT_PATTERNS = [
    # 1. Core: "sues/action against [Company] for/over..."
    re.compile(r'(?:sues?|lawsuit\s+against|action\s+against)\s+([\w\s,.&\'-]+?)(?:\s+(?:for|over|in|regarding|alleging|with))', re.IGNORECASE),
    # 2. "settlement with [Company]" terminated by for/over/to or end-of-string/comma
    re.compile(r'settlement\s+with\s+([\w\s,.&\'-]+?)(?:\s+(?:for|over|in|regarding|resolving|to)|$)', re.IGNORECASE),
    # 3. "Charges [Company] with/over/for" — BEFORE generic "with" and "against"
    re.compile(r'\bcharges?\s+([\w\s,.&\'-]{2,50}?)\s+(?:with|over|for)\b', re.IGNORECASE),
    # 4. "from [Company]" pattern for settlements — "$X from Company for ..."
    re.compile(r'(?:from)\s+([\w\s,.&\'-]+?)(?:\s+(?:for|over|in|regarding|fueling|alleging))', re.IGNORECASE),
    # 5. "Against [Company]," or "Against [Company] for/over/..." or end-of-string (incl. "charges against")
    re.compile(r'(?:charges?\s+against|against)\s+([\w\s,.&\'-]+?)(?:\s*,\s*|\s+(?:for|over|in|regarding|benefiting|resulting)|$)', re.IGNORECASE),
    # 6. Broader "with [Company]" pattern
    re.compile(r'(?:with)\s+([\w\s,.&\'-]+?)(?:\s*,\s*|\s+(?:for|over|in|regarding|resolving|to)|$)', re.IGNORECASE),
    # 7. "v. [Company]" case name pattern (e.g., "Suit v. Apple" or "v. Google Inc.")
    re.compile(r'\bv\.?\s+([\w\s,.&\'-]+?)(?:\s*$|\s*[,;]|\s+(?:for|over|in|regarding|that))', re.IGNORECASE),
    # 8. "[Company] Must Pay / to Pay / Agrees to Pay"
    re.compile(r'(?:^|:\s*)([\w\s,.&\'-]+?)\s+(?:Must|Agrees?\s+to|to|Will|Ordered\s+to)\s+Pay\b', re.IGNORECASE),
    # 9. "Investigation Into [Company]"
    re.compile(r'(?:investigation\s+(?:into|of))\s+([\w\s,.&\'-]+?)(?:\s+(?:for|over|regarding|alleging|results|leads|reveals)|[,;]|$)', re.IGNORECASE),
    # 10. "Stops/Halts/Shuts Down [Company]"
    re.compile(r"(?:stops?|halts?|shuts?\s+down)\s+([\w\s,.&\'-]+?)(?:\s*(?:'s)?\s+(?:for|over|from|illegal|unlawful|deceptive))", re.IGNORECASE),
    # 11. "Secures/Recovers ... from [Company]"
    re.compile(r'(?:secures?|recovers?|obtains?)\s+[\$\d][\$\d\w\s,.]*?\s+from\s+([\w\s,.&\'-]+?)(?:\s+(?:for|over|in|to)|[,;]|$)', re.IGNORECASE),
    # 12. "[Company] Ordered/Required to"
    re.compile(r'(?:^|:\s*)([\w\s,.&\'-]+?)\s+(?:Ordered|Required|Directed)\s+to\b', re.IGNORECASE),
]

# Body patterns: "defendant [Company]", "[Company], a [state]-based"
_BODY_DEFENDANT_PATTERNS = [
    re.compile(r'(?:defendant|respondent)s?\s+([\w\s,.&\'-]{3,60}?)(?:\s*[,.])', re.IGNORECASE),
    re.compile(r'(?:filed\s+(?:a\s+)?(?:lawsuit|complaint|action)\s+against)\s+([\w\s,.&\'-]{3,60}?)(?:\s*[,.]|\s+(?:for|over|in|alleging))', re.IGNORECASE),
    re.compile(r'settlement\s+with\s+([\w\s,.&\'-]{3,80}?)(?:\s*[,.])', re.IGNORECASE),
    # "against [Company], a [place]-based" or "against [Company] for/over"
    re.compile(r'(?:against)\s+([\w\s,.&\'-]{3,60}?)(?:\s*,\s*(?:a|an|the|who|which)|\s+(?:for|over|in|regarding|alleging))', re.IGNORECASE),
    # Parenthetical company abbreviation: "U.S. Healthworks (USHW)"
    re.compile(r'(?:settlement\s+with|against|lawsuit\s+against)\s+([\w\s.&\'-]{3,60}?)\s*\(([A-Z]{2,10})\)', re.IGNORECASE),
    # "announced a lawsuit against [Company] for" in body text
    re.compile(r'(?:announced\s+(?:a\s+)?(?:lawsuit|complaint|action|suit)\s+against)\s+([\w\s,.&\'-]{3,60}?)(?:\s+(?:for|over|in|regarding|alleging))', re.IGNORECASE),
    # Sentencing context: "[Name] and [Name], were sentenced/convicted"
    re.compile(r'(?:ringleaders?|leaders?|organizers?)\s+(?:of\s+the\s+scheme\s*,?\s*)([\w\s,.&\'-]{5,80}?)(?:\s*,\s*(?:were|was)\s+(?:sentenced|convicted))', re.IGNORECASE),
    # "sued [Company]" in body
    re.compile(r'\bsued\s+([\w\s,.&\'-]{3,60}?)(?:\s*[,.]|\s+(?:for|over|in|alleging|under))', re.IGNORECASE),
    # "charges against [Company]" in body
    re.compile(r'charges?\s+against\s+([\w\s,.&\'-]{3,60}?)(?:\s*[,.]|\s+(?:for|over|in|alleging|relating))', re.IGNORECASE),
    # "lawsuit against [Company]" without "filed"
    re.compile(r'(?:a\s+)?(?:lawsuit|action|suit|complaint)\s+against\s+([\w\s,.&\'-]{3,60}?)(?:\s*[,.]|\s+(?:for|over|in|alleging))', re.IGNORECASE),
    # "[Company], Inc./LLC/Corp." — match company with legal suffix in first paragraphs
    re.compile(r'(?:against|with|sued|suing|charging)\s+([\w\s,.&\'-]{3,50}?(?:Inc\.?|Corp\.?|LLC|L\.?L\.?C\.?|Ltd\.?|L\.?P\.?|Company|Corporation))', re.IGNORECASE),
    # "investigation of/into [Company]" in body
    re.compile(r'investigation\s+(?:of|into)\s+([\w\s,.&\'-]{3,60}?)(?:\s*[,.]|\s+(?:for|over|in|regarding|related))', re.IGNORECASE),
]

# Legal suffixes to help identify company names
_LEGAL_SUFFIXES = re.compile(
    r'\b(?:Inc\.?|Corp\.?|LLC|L\.?L\.?C\.?|Ltd\.?|L\.?P\.?|Co\.?|Company|Corporation|Group|Holdings)\b',
    re.IGNORECASE,
)


def _fix_headline_spacing(headline: str) -> str:
    """Fix missing spaces in headlines (e.g., TX soft-hyphen stripping).

    Inserts a space before uppercase letters that follow lowercase letters
    without a space, handling patterns like 'fromCVSand' → 'from CVS and'.
    Also inserts a space after dollar multipliers (M/B/K) glued to the next
    word, e.g., '$168Mfor' → '$168M for', '$160MFraud' → '$160M Fraud'.
    """
    headline = re.sub(r'([a-z])([A-Z])', r'\1 \2', headline)
    headline = re.sub(r'(\d[MBKmbk])([A-Za-z])', r'\1 \2', headline)
    return headline


# Multi-word entity names that should NOT be split on "and"
_AND_PROTECTED_PHRASES = re.compile(
    r'\b(?:Food\s+and\s+Drug|Arms?\s+and\s+Ammunition|'
    r'Alcohol,?\s+Tobacco,?\s+(?:Firearms?\s+)?and\s+|'
    r'Johnson\s+and\s+Johnson|Procter\s+and\s+Gamble|'
    r'Ernst\s+and\s+Young|Standard\s+and\s+Poor|'
    r'Bath\s+and\s+Body|Harley.Davidson|'
    r'Bed\s+Bath\s+and\s+Beyond|Barnes\s+and\s+Noble|'
    r'Simon\s+and\s+Schuster|Merrill\s+Lynch.*and|'
    r'cease\s+and\s+desist|assault\s+and\s+battery)',
    re.IGNORECASE,
)


def _safe_and_split(name: str) -> list[str]:
    """Split on ' and ' only when it separates distinct defendants, not mid-entity."""
    if _AND_PROTECTED_PHRASES.search(name):
        return [name]
    return re.split(r'\s+and\s+(?:the\s+)?', name)


def extract_defendants_from_headline(headline: str) -> list[str]:
    """Extract defendant names from a press release headline."""
    # Fix missing spaces (common in TX headlines from soft-hyphen stripping)
    headline = _fix_headline_spacing(headline)

    results = []
    seen: set[str] = set()
    for pattern in _HEADLINE_DEFENDANT_PATTERNS:
        match = pattern.search(headline)
        if match:
            raw_name = match.group(1).strip().rstrip(",.")
            parts = _safe_and_split(raw_name)
            for part in parts:
                part = part.strip().rstrip(",.")
                if not _is_valid_defendant_name(part):
                    continue
                if part.lower() not in seen:
                    seen.add(part.lower())
                    results.append(part)
    return results


def extract_defendants_from_body(text: str, max_chars: int = 1000) -> list[str]:
    """Extract defendant names from press release body text.

    Scans the first `max_chars` characters (first 2-3 paragraphs).
    AG press releases almost always name defendants in the opening paragraphs.
    """
    # Restrict to opening paragraph(s) only
    search_text = text[:max_chars]

    results = []
    seen: set[str] = set()

    for pattern in _BODY_DEFENDANT_PATTERNS:
        for match in pattern.finditer(search_text):
            raw_name = match.group(1).strip().rstrip(",.")
            parts = _safe_and_split(raw_name)
            for name in parts:
                name = name.strip().rstrip(",.")
                if not _is_valid_defendant_name(name):
                    continue
                name_lower = name.lower()
                if name_lower not in seen:
                    seen.add(name_lower)
                    results.append(name)

    return results


# ---------------------------------------------------------------------------
# Action Type Classification
# ---------------------------------------------------------------------------

_ACTION_TYPE_PATTERNS = [
    # --- Consent decree (check before settlement since "consent" could partial-match) ---
    ("consent_decree", re.compile(r'\bconsent\s+(?:decree|order|agreement)\b', re.IGNORECASE)),

    # --- Assurance of discontinuance ---
    ("assurance_of_discontinuance", re.compile(r'\b(?:assurance\s+of\s+(?:discontinuance|voluntary\s+compliance)|AOD)\b', re.IGNORECASE)),

    # --- Settlement (broad — most common resolution type) ---
    ("settlement", re.compile(
        r'\b(?:settl(?:ed|ement|ements|es|ing)|agrees?\s+to\s+pay|agreed\s+to\s+pay|'
        r'reaches?\s+(?:an?\s+)?agreement|reached\s+(?:an?\s+)?agreement|'
        r'resolves?\s+(?:claims?|charges?|allegations?|dispute|investigation|case)|resolved|'
        r'pays?\s+fine|paid\s+fine|pays?\s+penalty|'
        r'consent\s+judgment|recover(?:s|ed|y|ies)|'
        r'secures?\s+(?:more\s+than\s+|over\s+|nearly\s+|approximately\s+)?\$|obtains?\s+\$|wins?\s+\$|'
        r'secures?\s+(?:[\w$.,]+\s+){0,6}(?:agreement|settlement|debt\s+relief)|'
        r'(?:ends?|ending)\s+(?:harmful|illegal|unlawful|deceptive)\s+\w+\s+practices?|'
        r'delivers?\s+\$|restitution\s+(?:in|on)\s+the|'
        r'(?:on\s+the\s+way|in\s+the\s+mail)\s+to|'
        r'agrees?\s+to\s+(?:remove|stop|halt|cease|reform|change|end|eliminate|address|provide|destroy|surrender)|'
        r'distributes?\s+(?:over\s+)?\$)\b', re.IGNORECASE)),

    # --- Judgment / criminal resolution ---
    ("judgment", re.compile(
        r'\b(?:judgments?|verdict|sentenced|sentencing|convicted|conviction|convictions?\s+of|'
        r'pleads?\s+guilty|pled\s+guilty|guilty\s+plea|guilty\s+verdict|'
        r'found\s+(?:guilty|liable)|court\s+orders?\b|jury\s+(?:verdict|finds?)|'
        r'(?:secures?|wins?)\s+(?:[\w]+\s+){0,3}(?:victory|ruling|win|review|decision)|'
        r'surrenders?\s+(?:\w+\s+)?license|'
        r'permanently\s+(?:shuts?|bars?|bans?|closes?)|'
        r'arrested|(?:\d+\s+)?arrests?\s+(?:for|in|of|made)|'
        r'court\s+(?:declares?|rules?|upholds?|affirms?|denies|strikes?\s+down)|'
        r'judge\s+(?:dismisses?|rules?|orders?|blocks?|upholds?|strikes?)|'
        r'(?:appellate|appeals?\s+court)\s+(?:decision|ruling|upholds?|affirms?)|'
        r'blocks?\s+(?:[\w]+\s*\'?s?\s+){0,3}(?:attempt|motion|request|bid))\b', re.IGNORECASE)),

    # --- Injunction ---
    ("injunction", re.compile(
        r'\b(?:injunction|restraining\s+order|TROs?\b|cease\s+and\s+desist|'
        r'banned?\s+from|bans?\s+(?:[\w]+\s+){1,5}from|temporarily\s+blocked|'
        r'preliminary\s+injunction|permanent\s+injunction|'
        r'shut\s+down|shuts?\s+down|'
        r'ordered?\s+to\s+(?:halt|stop|cease)|'
        r'orders?\s+\w+\s+to\s+(?:halt|stop|cease)|'
        r'demands?\s+(?:[\w]+\s+){0,4}(?:halt|stop|cease|immediate\s+halt))\b', re.IGNORECASE)),

    # --- Lawsuit filed (check after settlement — a "settlement" headline is more specific) ---
    ("lawsuit_filed", re.compile(
        r'\b(?:(?:files?|filed)\s+(?:[\w]+\s+){0,5}(?:lawsuit|complaint|action|suit|litigation|charges?|petition)|'
        r'announces?\s+(?:[\w]+\s+){0,3}(?:lawsuit|complaint|suit|litigation|charges?\s+against|indictment)|'
        r'brings?\s+(?:a\s+)?(?:action|charges?|suit|complaint)|'
        r'(?:takes?|took)\s+(?:legal\s+)?action\s+(?:against|to|in\s+)|'
        r'charged?\s+with|facing\s+(?:[\w]+\s+){0,3}charges|'
        r'charges?\s+(?:[\w]+\s+){0,5}(?:in\s+connection|defendant|suspect|individual)|'
        r'indicts?|indicted|indictment|'
        r'(?:co-?leads?|leads?|joins?)\s+(?:[\w-]+\s+){0,4}(?:lawsuit|litigation|suit|challenge)|'
        r'sues?|sued|suing|'
        r'(?:files?|launches?|announces?|commences?)\s+(?:[\w]+\s+){0,3}investigation|'
        r'investigates?\b|'
        r'(?:seeks?\s+to\s+(?:lead|co-?lead)\s+[\w\s-]*?(?:lawsuit|litigation|suit))|'
        r'appeals?\s+(?:ruling|decision|order)|'
        r'cracks?\s+down|crackdown|'
        r'(?:enforcement\s+action)\s+against|'
        r'pleads?\s+not\s+guilty|'
        r'(?:expands?|updates?)\s+(?:[\w]+\s+){0,3}(?:investigation|lawsuit|suit))\b', re.IGNORECASE)),
]


def classify_action_type(headline: str, body_text: str) -> str:
    """Classify the enforcement action type based on headline and body text.

    Checks headline first (stronger signal), then body text with increasing depth.
    Returns the ActionType value string.
    """
    # Check headline first — it's the strongest signal
    for action_type, pattern in _ACTION_TYPE_PATTERNS:
        if pattern.search(headline):
            return action_type

    # Fall back to body text — check first 2000 chars (first few paragraphs)
    for action_type, pattern in _ACTION_TYPE_PATTERNS:
        if pattern.search(body_text[:2000]):
            return action_type

    # Deeper body text search for weaker signals (first 5000 chars)
    for action_type, pattern in _ACTION_TYPE_PATTERNS:
        if pattern.search(body_text[:5000]):
            return action_type

    return "other"


# ---------------------------------------------------------------------------
# Multistate Detection
# ---------------------------------------------------------------------------

_MULTISTATE_PATTERNS = [
    re.compile(r'\bmultistate\b', re.IGNORECASE),
    re.compile(r'\bcoalition\s+of\s+(?:\d+\s+)?(?:state|attorney)', re.IGNORECASE),
    re.compile(r'\b(\d+)\s+state(?:s)?\s+(?:attorneys?\s+general|AGs?)\b', re.IGNORECASE),
    re.compile(r'\b(?:bipartisan|nationwide)\s+(?:coalition|group|states?)\b', re.IGNORECASE),
    re.compile(r'\bjoined?\s+(?:by\s+)?\d+\s+(?:other\s+)?state', re.IGNORECASE),
    # "attorneys general of [State], [State], ..." — listing 3+ states
    re.compile(r'attorneys?\s+general\s+of\s+(?:\w+(?:\s+\w+)?,?\s+){3,}', re.IGNORECASE),
    # "States Negotiating Committee" (used in multistate settlements like Purdue)
    re.compile(r'\bstates?\s+negotiating\s+committee\b', re.IGNORECASE),
    # Joining + listing of states
    re.compile(r'\bjoining\s+attorney\s+general\b.*\battorneys?\s+general\s+of\b', re.IGNORECASE | re.DOTALL),
]


def is_multistate_action(headline: str, body_text: str) -> bool:
    """Detect whether this is a multistate enforcement action."""
    combined = headline + " " + body_text[:2000]
    return any(p.search(combined) for p in _MULTISTATE_PATTERNS)
