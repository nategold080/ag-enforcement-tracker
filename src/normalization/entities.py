"""Entity resolution: company name normalization pipeline.

Pipeline:
1. Clean raw name (strip legal suffixes, normalize whitespace/case)
2. Check known aliases (config/entities.yaml)
3. Fuzzy match against existing canonical names (thefuzz, threshold 0.85)
4. If no match, create new canonical entity

No LLM usage. Deterministic matching where consistency > cleverness.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml
from thefuzz import fuzz

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "entities.yaml"
BLOCKLIST_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "defendant_blocklist.yaml"


def _load_defendant_blocklist() -> tuple[set[str], list[re.Pattern]]:
    """Load the defendant blocklist from config/defendant_blocklist.yaml.

    Returns (exact_matches_set, compiled_patterns_list).
    The exact_matches_set is lowercased for case-insensitive comparison.
    """
    if not BLOCKLIST_PATH.exists():
        return set(), []

    with open(BLOCKLIST_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    exact = {entry.lower().strip() for entry in data.get("exact_matches", [])}
    patterns = [re.compile(p, re.IGNORECASE) for p in data.get("patterns", [])]
    return exact, patterns


_BLOCKLIST_EXACT, _BLOCKLIST_PATTERNS = _load_defendant_blocklist()

# Legal suffixes to strip
_LEGAL_SUFFIXES = re.compile(
    r',?\s*\b('
    r'Inc\.?|Incorporated|Corp\.?|Corporation|LLC|L\.L\.C\.?|'
    r'Ltd\.?|Limited|L\.P\.?|LP|LLP|L\.L\.P\.?|'
    r'Co\.?|Company|PLC|P\.L\.C\.?|'
    r'NA|N\.A\.?|'
    r'et\s+al\.?|d/b/a\s+\S+'
    r')\s*$',
    re.IGNORECASE,
)

# Articles and filler to strip from the start
_LEADING_ARTICLES = re.compile(r'^(The|A|An)\s+', re.IGNORECASE)

# Trailing sentence fragments that get captured as part of defendant names
# during body-text extraction — truncate the name before these patterns
_TRAILING_FRAGMENTS = re.compile(
    r'\s*(?:'
    r'(?:claiming|alleging|asserting|accusing|contending)\s+(?:that|the)|'
    r'(?:regarding|over\s+the|for\s+(?:violating|illegally|deceptive|failing))|'
    r'(?:is\s+scheduled|has\s+now\s+been|today|to\s+hold\s+the)|'
    r'(?:secured\s+by|resulting|the\s+parent|to\s+(?:inc|move))|'
    r'(?:,?\s+with\s+a\s+u\.s\.)|'
    r'(?:\.\s+(?:Filed|The\s+states|The\s+money))'
    r').*$',
    re.IGNORECASE,
)

# Leading descriptive phrases to strip (e.g., "technology giant Google")
_LEADING_DESCRIPTORS = re.compile(
    r'^(?:technology\s+giant|internet\s+giant|pharmaceutical\s+(?:giant|manufacturer)|'
    r'opioid\s+manufacturer|oxycontin\s+maker|drug\s+manufacturers?|'
    r'e-?cigarette\s+(?:maker|company)|electronic\s+cigarette\s+maker,?\s*'
    r')\s*',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Canonical-name validation — reject garbage after cleaning
# ---------------------------------------------------------------------------

_ENTITY_STOPWORDS = frozenset({
    # Pronouns / determiners / articles
    "a", "an", "the", "his", "her", "its", "that", "this", "them", "they",
    "it", "he", "she", "we", "us", "me", "my", "our", "your", "their",
    "who", "what", "which", "where", "when", "how", "there", "here",
    # Common short words
    "all", "any", "new", "one", "two", "three", "four", "five", "six",
    "each", "than", "then", "also", "just", "like", "over", "only",
    "many", "some", "such", "more", "most", "very", "much", "well",
    "back", "into", "with", "from", "about", "will", "or", "if", "so",
    "no", "not", "but", "yet", "do", "can", "may", "shall",
    # Verbs / modals
    "be", "been", "being", "am", "is", "are", "was", "were",
    "have", "has", "had", "having", "get", "got", "did", "would",
    "could", "should", "must", "make", "made", "take", "took",
    "give", "gave", "keep", "kept", "let", "say", "said", "use",
    "run", "ran",
    # Generic descriptors that aren't entity names
    "business", "company", "nation", "office", "help",
    "complete", "election", "investigation", "discrimination",
    "travel", "pharmaceutical", "operating", "major", "other",
    "former", "another", "several", "act", "law", "court", "state",
    "federal", "child", "children", "people", "person",
    "real estate", "seller",
    # Additional single-word nouns that pass extraction but aren't entities
    "notice", "report", "order", "plan", "claim", "relief", "charge",
    "grant", "rule", "damage", "risk", "abuse", "penalty", "complaint",
    "violation", "statute", "settlement", "enforcement", "authority",
    "oversight", "guidance", "protection", "measure", "effort",
    "failure", "impact", "safety", "access", "matter", "issue",
    "harm", "threat", "concern", "policy",
    # Generic industry terms that pass extraction as single-word "names"
    "mortgage", "cryptocurrency", "e-cigarette", "companies",
    "funding", "centers", "patients", "owners",
})

# Patterns that indicate a "name" is actually a sentence fragment or not a defendant
_GARBAGE_NAME_PATTERNS = [
    # Starts with lowercase (except brand-convention names like eBay, iPhone)
    re.compile(r'^[a-z](?!bay|phone|pod|tunes)'),
    # "Death of X" — investigation subjects, not defendants
    re.compile(r'^death\s+of\b', re.IGNORECASE),
    # Government entities — not defendants in AG enforcement
    re.compile(r'(?:attorney.?s?\s+general|attorneys\s+general|district\s+attorney)',
               re.IGNORECASE),
    re.compile(r'(?:trump|biden|obama)\s+(?:administration|era)',
               re.IGNORECASE),
    re.compile(r'^(?:president\s+(?:trump|biden|donald))\b', re.IGNORECASE),
    re.compile(r'(?:department\s+of|secretary\s+of|u\.?s\.?\s+(?:supreme|district|department))',
               re.IGNORECASE),
    re.compile(r'^\d+\s+(?:state\s+)?attorneys?\s+general\b', re.IGNORECASE),
    # Government agency tail fragments (from "and"-split of multi-word names)
    re.compile(r'^(?:drug|highway|transit|veterans?)\s+administration\b', re.IGNORECASE),
    re.compile(r'^homeland\s+security\b', re.IGNORECASE),
    # Ends with government body suffix (standalone fragment)
    re.compile(r'\b(?:commission|bureau|department)\s*$', re.IGNORECASE),
    # Law name fragments — "Women Act", "Disabilities Act"
    re.compile(r'\b(?:women|disabilities|privacy|recovery|clean\s+(?:air|water))\s+act\b',
               re.IGNORECASE),
    # Generic industry role descriptors (including "N drug makers", "generic drug makers")
    re.compile(r'(?:^|\b)(?:drug|opioid|pharmaceutical|tobacco|e-?cigarette|generic)\s+'
               r'(?:maker|manufacturer|distributor|company|companies)s?\b', re.IGNORECASE),
    # Count-word at start — almost always a count phrase, not a company name.
    # Protects known companies: "Nine West", "Five Guys", "Three Rivers", "Six Flags"
    re.compile(r'^(?:two|three|four|five|six|seven|eight|ten)\s+'
               r'(?!west\b|guys\b|rivers?\b|flags?\b|star\b|point\b)',
               re.IGNORECASE),
    # "Food & Drug Administration" / "Food and Drug Administration" — the FDA
    re.compile(r'\bfood\s+(?:&|and)\s+drug\s+administration\b', re.IGNORECASE),
    # "Settlement With N" pattern
    re.compile(r'^settlement\s+with\b', re.IGNORECASE),
    # Police departments
    re.compile(r'\bpolice\s+department\b', re.IGNORECASE),
    # "Press Release AG..." headlines captured as names
    re.compile(r'^press\s+release\b', re.IGNORECASE),
    # "Acting [Title]" captured as a name
    re.compile(r'^acting\s+(?:tax|attorney|district|commissioner)', re.IGNORECASE),
    # "N things" — count phrases, not entity names
    re.compile(r'^\d+\s+(?:states?|individuals?|defendants?|companies|businesses|'
               r'officers?|suspects?|felonies|producers?|pharmacies|drug\s+makers|'
               r'other\s+sta)',
               re.IGNORECASE),
    # Fragment verbs in the name
    re.compile(r'\b(?:claiming|alleging|saying|stating|arguing|asserting|'
               r'accusing|contending|opposing|demanding|requesting|'
               r'attempting|resulting|announcing|filed|scheduled|'
               r'according|beginning|providing)\b', re.IGNORECASE),
    # "to [verb]" fragments (e.g., "to Protect Workers")
    re.compile(r'\bto\s+(?:protect|block|stop|prevent|hold|ensure|support|'
               r'help|combat|fight|halt|reform|end|strike|address|'
               r'require|prohibit|investigate|crack|avoid|enforce)\b',
               re.IGNORECASE),
    # Starts with pronoun + noun ("His companies", "Its subsidiaries")
    re.compile(r'^(?:his|her|its|their)\s+', re.IGNORECASE),
    # "Consumer Alert" captured as defendant
    re.compile(r'^consumer\s+alert\b', re.IGNORECASE),
    # Year + report title patterns
    re.compile(r'^20\d{2}\s+(?:capital|health|annual|cost|data)', re.IGNORECASE),
    # Comma followed by sentence fragment ("Google, claiming that")
    re.compile(r',\s+(?:the|a|an|with|for|that|which|who|in|on|to|its|his|her|'
               r'claiming|alleging|accusing|asserting|but|and\s+the)\s',
               re.IGNORECASE),
    # "Amicus Brief" / "Brief Joined By"
    re.compile(r'amicus\s+brief|brief\s+joined\s+by', re.IGNORECASE),
    # "Battleground States", "States on Behalf of"
    re.compile(r'(?:battleground|on\s+behalf\s+of)\s+', re.IGNORECASE),
    # "[Place]-Based Company That" — truncated fragment
    re.compile(r'-based\s+company\b', re.IGNORECASE),
    # "Largest/Biggest XXXX Settlement" — headline fragment
    re.compile(r'\blargest\s+\w+\s+settlement\b', re.IGNORECASE),
    # Fragments starting with adverbs
    re.compile(r'^(?:unlawfully|illegally|fraudulently|knowingly|willfully)\s+', re.IGNORECASE),
    # "Consumers Is/Are" — sentence fragment
    re.compile(r'^consumers?\s+(?:is|are|was|were|has|have)\b', re.IGNORECASE),
    # "Over [Policy]" — prepositional fragment
    re.compile(r'^over\s+(?:hud|the|a|an|its)\b', re.IGNORECASE),
    # Single-word generic role nouns
    re.compile(r'^(?:trafficker|lender|borrower|manufacturer|distributor|retailer|'
               r'contractor|employer|provider|operator|vendor|dealer|broker)s?$',
               re.IGNORECASE),
    # "[Place] man/woman/business/resident" — WA headline patterns
    re.compile(r'^[A-Z]\w+(?:\s+[A-Z]\w+)?\s+(?:man|woman|men|women|business|resident|couple|family)$',
               re.IGNORECASE),
    # Names containing "for [gerund/adjective]" — headline fragments
    re.compile(r'\bfor\s+(?:deceiving|misleading|defrauding|violating|failing|scamming|'
               r'harming|exploiting|overcharging|deceptive|illegal|unlawful|false|unfair)',
               re.IGNORECASE),
    # Names ending with trailing hyphen (truncated fragments)
    re.compile(r'-\s*$'),
    # Generic crypto/e-cigarette/industry descriptors as multi-word "names"
    # Matches both standalone terms and prefixed names like "Crypto Firm Genesis"
    re.compile(r'^(?:e-?cigarette|cryptocurrency|crypto|vaping|mortgage|tobacco)\s+'
               r'(?:platform|companies|company|firm|firms|exchange|exchanges|lender|'
               r'lenders|broker|brokers|servicer|servicers)s?(?:\s|,|$)',
               re.IGNORECASE),
    # "During The" and similar preposition-article fragments misidentified as names
    re.compile(r'^(?:during|before|after|between|within|toward|upon)\s+(?:the|a|an|this|that)$',
               re.IGNORECASE),
    # "Owner [Name] for" patterns — headline fragments with role prefix
    re.compile(r'^(?:owner|founder|ceo|president|chairman)\s+\w+\s+for\b', re.IGNORECASE),
]


def is_valid_canonical_name(name: str) -> bool:
    """Check whether a cleaned canonical name looks like a real entity.

    Returns False for stopwords, sentence fragments, government entities,
    investigation subjects, and other garbage that should not be a
    canonical defendant name.
    """
    if not name or len(name) < 2:
        return False

    # Single word under 3 chars (but allow "3M", "BP", "HP")
    if len(name) < 3 and not re.match(r'^[A-Z0-9]{2,3}$', name):
        return False

    # Exact stopword match
    if name.lower().strip() in _ENTITY_STOPWORDS:
        return False

    # Check against the defendant blocklist (case-insensitive exact match)
    if name.lower().strip() in _BLOCKLIST_EXACT:
        return False

    # Check against defendant blocklist patterns
    for pat in _BLOCKLIST_PATTERNS:
        if pat.search(name):
            return False

    # Regex patterns
    for pat in _GARBAGE_NAME_PATTERNS:
        if pat.search(name):
            return False

    # Pure numbers
    if re.match(r'^\d+$', name.strip()):
        return False

    return True


class EntityResolver:
    """Resolve raw company names to canonical forms.

    Loads alias mappings from config/entities.yaml and maintains
    a growing set of canonical names for fuzzy matching.
    """

    def __init__(self, config_path: Path | None = None):
        self._config_path = config_path or CONFIG_PATH
        self._aliases: dict[str, str] = {}       # lowered alias → canonical
        self._metadata: dict[str, dict] = {}      # canonical → metadata
        self._canonical_names: set[str] = set()   # all known canonical names
        self._review_queue: list[tuple[str, str, float]] = []  # (raw, candidate, score)
        self._load_config()

    def _load_config(self) -> None:
        """Load aliases and metadata from entities.yaml."""
        if not self._config_path.exists():
            logger.warning("Entity config not found at %s", self._config_path)
            return

        with open(self._config_path) as f:
            data = yaml.safe_load(f) or {}

        aliases = data.get("aliases", {})
        for alias, canonical in aliases.items():
            self._aliases[alias.lower().strip()] = canonical
            self._canonical_names.add(canonical)

        # Also add canonical names as their own alias
        for canonical in set(aliases.values()):
            self._aliases[canonical.lower().strip()] = canonical

        self._metadata = data.get("entities", {})
        logger.info(
            "Loaded %d aliases → %d canonical entities",
            len(aliases), len(self._canonical_names),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, raw_name: str) -> tuple[str, float]:
        """Resolve a raw name to its canonical form.

        Returns:
            (canonical_name, confidence) where confidence is:
            - 1.0 for exact alias match
            - 0.85-0.99 for fuzzy match above threshold
            - 0.5 for new entity (no match found)
        """
        cleaned = self.clean_name(raw_name)
        if not cleaned:
            return "", 0.0

        # Step 0: Validate the cleaned name is a real entity, not garbage
        if not is_valid_canonical_name(cleaned):
            logger.debug("Rejected garbage name: %r (cleaned: %r)", raw_name, cleaned)
            return "", 0.0

        # Step 1: Check known aliases (exact match on cleaned name)
        alias_key = cleaned.lower()
        if alias_key in self._aliases:
            return self._aliases[alias_key], 1.0

        # Step 2: Also check the original raw name lowered (before suffix stripping)
        raw_key = raw_name.strip().lower()
        if raw_key in self._aliases:
            return self._aliases[raw_key], 1.0

        # Step 3: Fuzzy match against existing canonical names
        best_match, best_score = self._fuzzy_match(cleaned)

        if best_score >= 85:
            # Auto-match: high confidence
            logger.debug("Fuzzy matched %r → %r (score=%d)", raw_name, best_match, best_score)
            return best_match, best_score / 100.0

        if best_score >= 70:
            # Flag for review
            self._review_queue.append((raw_name, best_match, best_score / 100.0))
            logger.debug("Review candidate: %r → %r (score=%d)", raw_name, best_match, best_score)

        # Step 4: No match — use cleaned name as new canonical entity
        self._canonical_names.add(cleaned)
        return cleaned, 0.5

    def resolve_batch(self, names: list[str]) -> list[tuple[str, str, float]]:
        """Resolve a batch of names. Returns [(raw, canonical, confidence), ...]."""
        return [(name, *self.resolve(name)) for name in names]

    def get_metadata(self, canonical_name: str) -> dict:
        """Get metadata for a canonical entity name (if available)."""
        return self._metadata.get(canonical_name, {})

    def get_review_queue(self) -> list[tuple[str, str, float]]:
        """Return names flagged for manual review (70-85% fuzzy match)."""
        return list(self._review_queue)

    # ------------------------------------------------------------------
    # Name cleaning
    # ------------------------------------------------------------------

    @staticmethod
    def clean_name(raw: str) -> str:
        """Clean a raw entity name: strip suffixes, normalize whitespace/case."""
        name = raw.strip()
        if not name:
            return ""

        # Strip trailing sentence fragments first (before other cleaning)
        name = _TRAILING_FRAGMENTS.sub("", name).strip().rstrip(",.")

        # Strip leading descriptive phrases ("technology giant Google" → "Google")
        name = _LEADING_DESCRIPTORS.sub("", name).strip()

        # Strip leading articles
        name = _LEADING_ARTICLES.sub("", name)

        # Strip legal suffixes (may need multiple passes)
        for _ in range(3):
            prev = name
            name = _LEGAL_SUFFIXES.sub("", name).strip().rstrip(",.")
            if name == prev:
                break

        # Normalize whitespace
        name = re.sub(r'\s+', ' ', name).strip()

        # Title case, but preserve known acronyms
        if name.isupper() or name.islower():
            name = name.title()

        return name

    # ------------------------------------------------------------------
    # Fuzzy matching
    # ------------------------------------------------------------------

    def _fuzzy_match(self, cleaned_name: str) -> tuple[str, int]:
        """Find the best fuzzy match among canonical names.

        Uses token_sort_ratio which handles word reordering well
        (e.g., "Blackstone Group" vs "Blackstone Inc" → high match).

        Requires both names to be at least 4 chars to avoid spurious
        short-string matches (e.g., "Chile" → "Children" at 77%).
        """
        if not self._canonical_names:
            return "", 0

        # Don't fuzzy match very short names — too many false positives
        if len(cleaned_name) < 4:
            return "", 0

        best_name = ""
        best_score = 0

        cleaned_lower = cleaned_name.lower()
        for canonical in self._canonical_names:
            # Skip short canonicals for fuzzy matching
            if len(canonical) < 4:
                continue
            # Reject if lengths differ dramatically (>3x ratio) — prevents
            # matching "Seller" to "Shell" or "Chile" to "Children"
            len_ratio = len(cleaned_name) / len(canonical) if len(canonical) > 0 else 0
            if len_ratio < 0.4 or len_ratio > 2.5:
                continue
            # Token sort ratio handles word order differences
            score = fuzz.token_sort_ratio(cleaned_lower, canonical.lower())
            if score > best_score:
                best_score = score
                best_name = canonical

        return best_name, best_score
