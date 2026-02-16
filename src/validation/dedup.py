"""Duplicate detection for enforcement actions.

Handles two types of duplicates:
1. Multistate duplicates: same action reported by multiple states
2. Temporal duplicates: same action at different stages (lawsuit → settlement)

Uses composite key of (approximate date ± 30 days, defendant similarity,
dollar amount if present, state) for matching.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from thefuzz import fuzz

logger = logging.getLogger(__name__)

DATE_WINDOW_DAYS = 30
DEFENDANT_SIMILARITY_THRESHOLD = 80  # token_sort_ratio


@dataclass
class DedupCandidate:
    """Represents an action for dedup comparison."""
    action_id: str
    state: str
    date_announced: date
    defendants: list[str]  # canonical names
    total_amount: Decimal | None
    headline: str
    is_multistate: bool


@dataclass
class DedupMatch:
    """A detected duplicate pair."""
    action_id_a: str
    action_id_b: str
    match_type: str  # 'multistate' or 'temporal'
    confidence: float  # 0.0 - 1.0
    reason: str


def find_duplicates(candidates: list[DedupCandidate]) -> list[DedupMatch]:
    """Find potential duplicate pairs among a list of candidates.

    Compares all pairs (O(n²)) — fine for thousands of records.
    Returns matches sorted by confidence descending.
    """
    matches: list[DedupMatch] = []
    n = len(candidates)

    for i in range(n):
        for j in range(i + 1, n):
            a, b = candidates[i], candidates[j]
            match = _compare_pair(a, b)
            if match:
                matches.append(match)

    matches.sort(key=lambda m: m.confidence, reverse=True)
    return matches


def _compare_pair(a: DedupCandidate, b: DedupCandidate) -> DedupMatch | None:
    """Compare two candidates and return a DedupMatch if they look like duplicates."""

    # Must be within date window
    if abs((a.date_announced - b.date_announced).days) > DATE_WINDOW_DAYS:
        return None

    # Must have at least one defendant to compare
    if not a.defendants or not b.defendants:
        return None

    # Check defendant overlap
    defendant_score = _defendant_similarity(a.defendants, b.defendants)
    if defendant_score < DEFENDANT_SIMILARITY_THRESHOLD:
        return None

    # Calculate confidence
    confidence = 0.0
    reasons = []

    # Defendant match (base signal)
    confidence += 0.4 * (defendant_score / 100.0)
    reasons.append(f"defendants={defendant_score}%")

    # Date proximity
    date_diff = abs((a.date_announced - b.date_announced).days)
    date_score = 1.0 - (date_diff / DATE_WINDOW_DAYS)
    confidence += 0.2 * date_score
    reasons.append(f"date_diff={date_diff}d")

    # Amount match (strong signal if both present)
    if a.total_amount and b.total_amount:
        if a.total_amount == b.total_amount:
            confidence += 0.3
            reasons.append("exact_amount_match")
        elif _amounts_similar(a.total_amount, b.total_amount):
            confidence += 0.15
            reasons.append("similar_amount")

    # Headline similarity (supplementary)
    headline_score = fuzz.token_sort_ratio(a.headline.lower(), b.headline.lower())
    if headline_score > 70:
        confidence += 0.1 * (headline_score / 100.0)
        reasons.append(f"headline={headline_score}%")

    confidence = min(1.0, confidence)

    # Need minimum confidence to report
    if confidence < 0.5:
        return None

    # Determine match type
    if a.state != b.state:
        match_type = "multistate"
    else:
        match_type = "temporal"

    return DedupMatch(
        action_id_a=a.action_id,
        action_id_b=b.action_id,
        match_type=match_type,
        confidence=round(confidence, 3),
        reason="; ".join(reasons),
    )


def _defendant_similarity(defs_a: list[str], defs_b: list[str]) -> int:
    """Calculate best defendant name similarity between two lists.

    Returns 0-100 score. Matches if ANY defendant pair exceeds threshold.
    """
    best = 0
    for name_a in defs_a:
        for name_b in defs_b:
            score = fuzz.token_sort_ratio(name_a.lower(), name_b.lower())
            if score > best:
                best = score
    return best


def _amounts_similar(a: Decimal, b: Decimal) -> bool:
    """Check if two amounts are similar (within 10% of each other)."""
    if a == 0 or b == 0:
        return a == b
    ratio = float(min(a, b) / max(a, b))
    return ratio >= 0.9
