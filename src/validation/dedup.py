"""Duplicate detection for enforcement actions.

Handles two types of duplicates:
1. Multistate duplicates: same action reported by multiple states
2. Temporal duplicates: same action at different stages (lawsuit → settlement)

Uses composite key of (approximate date ± 30 days, defendant similarity,
dollar amount if present, state) for matching.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from thefuzz import fuzz

logger = logging.getLogger(__name__)

DATE_WINDOW_DAYS = 30
# Wider date window for multistate actions (same settlement announced weeks apart)
MULTISTATE_DATE_WINDOW_DAYS = 730  # ~2 years per P3 spec
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

    # Use wider date window when both are multistate
    date_window = DATE_WINDOW_DAYS
    if a.is_multistate and b.is_multistate and a.state != b.state:
        date_window = MULTISTATE_DATE_WINDOW_DAYS

    # Must be within date window
    if abs((a.date_announced - b.date_announced).days) > date_window:
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

    # Date proximity (use the same window that passed the initial filter)
    date_diff = abs((a.date_announced - b.date_announced).days)
    date_score = max(0.0, 1.0 - (date_diff / date_window))
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


# ---------------------------------------------------------------------------
# Multistate action clustering and DB linking
# ---------------------------------------------------------------------------

@dataclass
class MultistateCluster:
    """A cluster of enforcement actions that represent the same multistate action."""
    cluster_id: str
    action_ids: list[str]
    states: list[str]
    lead_state: str | None
    name: str
    total_settlement: Decimal | None


def cluster_multistate_matches(
    candidates: list[DedupCandidate],
    matches: list[DedupMatch],
) -> list[MultistateCluster]:
    """Group multistate duplicate matches into clusters using union-find.

    Takes pairwise matches and groups them into connected components,
    then builds MultistateCluster objects with metadata.
    """
    # Filter to multistate matches only
    ms_matches = [m for m in matches if m.match_type == "multistate"]
    if not ms_matches:
        return []

    # Build lookup for candidates
    cand_by_id: dict[str, DedupCandidate] = {c.action_id: c for c in candidates}

    # Union-Find to group connected actions into clusters
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for m in ms_matches:
        union(m.action_id_a, m.action_id_b)

    # Group by root
    groups: dict[str, list[str]] = defaultdict(list)
    all_ids = {m.action_id_a for m in ms_matches} | {m.action_id_b for m in ms_matches}
    for aid in all_ids:
        groups[find(aid)].append(aid)

    # Build clusters
    clusters: list[MultistateCluster] = []
    for group_ids in groups.values():
        if len(group_ids) < 2:
            continue

        cands = [cand_by_id[aid] for aid in group_ids if aid in cand_by_id]
        states = sorted(set(c.state for c in cands))

        # Pick the highest settlement amount as the canonical total
        amounts = [c.total_amount for c in cands if c.total_amount]
        total = max(amounts) if amounts else None

        # Use the earliest action's defendant list for the name
        earliest = min(cands, key=lambda c: c.date_announced)
        name_parts = earliest.defendants[:2] if earliest.defendants else []
        name = ", ".join(name_parts) if name_parts else earliest.headline[:80]

        # Lead state: the one that announced first
        lead_state = earliest.state

        clusters.append(MultistateCluster(
            cluster_id=str(uuid.uuid4()),
            action_ids=[c.action_id for c in cands],
            states=states,
            lead_state=lead_state,
            name=name,
            total_settlement=total,
        ))

    logger.info("Found %d multistate clusters from %d matches", len(clusters), len(ms_matches))
    return clusters


def link_multistate_actions(db) -> int:
    """Detect multistate duplicates and create multistate_actions records in the DB.

    Returns the number of multistate clusters created.
    """
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload
    from src.storage.models import (
        EnforcementAction,
        ActionDefendant,
        Defendant,
        MonetaryTerms,
        MultistateAction,
    )

    with db.get_session() as session:
        # Load all multistate-flagged actions with their defendants and amounts
        stmt = (
            select(EnforcementAction)
            .options(
                joinedload(EnforcementAction.action_defendants).joinedload(ActionDefendant.defendant),
                joinedload(EnforcementAction.monetary_terms),
            )
            .where(EnforcementAction.is_multistate == True)  # noqa: E712
        )
        actions = session.execute(stmt).unique().scalars().all()

        if not actions:
            logger.info("No multistate actions found to cluster.")
            return 0

        # Build DedupCandidates
        candidates = []
        for a in actions:
            defendants = [
                ad.defendant.canonical_name or ad.defendant.raw_name
                for ad in a.action_defendants
                if ad.defendant.canonical_name or ad.defendant.raw_name
            ]
            amount = a.monetary_terms.total_amount if a.monetary_terms else None
            candidates.append(DedupCandidate(
                action_id=a.id,
                state=a.state,
                date_announced=a.date_announced,
                defendants=defendants,
                total_amount=amount,
                headline=a.headline,
                is_multistate=True,
            ))

        logger.info("Comparing %d multistate candidates for dedup...", len(candidates))

        # Find pairwise matches
        matches = find_duplicates(candidates)

        # Cluster into groups
        clusters = cluster_multistate_matches(candidates, matches)
        if not clusters:
            logger.info("No multistate clusters detected.")
            return 0

        # Write to database
        for cluster in clusters:
            ms_action = MultistateAction(
                id=cluster.cluster_id,
                name=cluster.name,
                lead_state=cluster.lead_state,
                participating_states=cluster.states,
                total_settlement=cluster.total_settlement,
            )
            session.add(ms_action)

            # Link child actions
            for aid in cluster.action_ids:
                action = session.get(EnforcementAction, aid)
                if action:
                    action.multistate_action_id = cluster.cluster_id

        session.commit()
        logger.info("Created %d multistate_actions records.", len(clusters))
        return len(clusters)
