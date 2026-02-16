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
            return raw_name.strip(), 0.0

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
