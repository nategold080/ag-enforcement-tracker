"""Tests for the deduplication module.

Covers:
- Pairwise comparison logic
- Multistate duplicate detection
- Union-find clustering
- Amount similarity
- Date window filtering
"""

from datetime import date
from decimal import Decimal

import pytest

from src.validation.dedup import (
    DedupCandidate,
    DedupMatch,
    MultistateCluster,
    _amounts_similar,
    _compare_pair,
    _defendant_similarity,
    cluster_multistate_matches,
    find_duplicates,
)


# ── Helpers ───────────────────────────────────────────────────────────────

def _make_candidate(
    action_id: str = "a1",
    state: str = "CA",
    date_announced: date = date(2024, 6, 1),
    defendants: list[str] | None = None,
    total_amount: Decimal | None = None,
    headline: str = "AG Settles with Test Corp",
    is_multistate: bool = False,
) -> DedupCandidate:
    return DedupCandidate(
        action_id=action_id,
        state=state,
        date_announced=date_announced,
        defendants=defendants if defendants is not None else ["Test Corp"],
        total_amount=total_amount,
        headline=headline,
        is_multistate=is_multistate,
    )


# ── Amount similarity ─────────────────────────────────────────────────────

class TestAmountSimilarity:
    def test_exact_match(self):
        assert _amounts_similar(Decimal("1000000"), Decimal("1000000"))

    def test_within_10_percent(self):
        assert _amounts_similar(Decimal("1000000"), Decimal("950000"))

    def test_outside_10_percent(self):
        assert not _amounts_similar(Decimal("1000000"), Decimal("800000"))

    def test_zero_amounts(self):
        assert _amounts_similar(Decimal("0"), Decimal("0"))
        assert not _amounts_similar(Decimal("0"), Decimal("1000"))


# ── Defendant similarity ──────────────────────────────────────────────────

class TestDefendantSimilarity:
    def test_exact_match(self):
        assert _defendant_similarity(["Test Corp"], ["Test Corp"]) == 100

    def test_fuzzy_match(self):
        score = _defendant_similarity(["Test Corporation"], ["Test Corp"])
        assert score >= 70

    def test_no_match(self):
        score = _defendant_similarity(["Alpha Inc"], ["Omega LLC"])
        assert score < 50

    def test_multi_defendant_best_match(self):
        score = _defendant_similarity(
            ["Alpha Inc", "Test Corp"],
            ["Test Corporation"],
        )
        assert score >= 70


# ── Pairwise comparison ──────────────────────────────────────────────────

class TestComparePair:
    def test_same_defendant_same_date_matches(self):
        a = _make_candidate("a1", "CA", date(2024, 6, 1), ["Test Corp"], Decimal("5000000"))
        b = _make_candidate("a2", "NY", date(2024, 6, 5), ["Test Corp"], Decimal("5000000"))
        match = _compare_pair(a, b)
        assert match is not None
        assert match.match_type == "multistate"
        assert match.confidence >= 0.5

    def test_different_defendants_no_match(self):
        a = _make_candidate("a1", "CA", defendants=["Alpha Corp"])
        b = _make_candidate("a2", "NY", defendants=["Omega Inc"])
        match = _compare_pair(a, b)
        assert match is None

    def test_date_outside_window_no_match(self):
        a = _make_candidate("a1", "CA", date(2024, 1, 1))
        b = _make_candidate("a2", "NY", date(2024, 6, 1))
        match = _compare_pair(a, b)
        assert match is None

    def test_no_defendants_no_match(self):
        a = _make_candidate("a1", defendants=[])
        b = _make_candidate("a2", defendants=["Test Corp"])
        match = _compare_pair(a, b)
        assert match is None

    def test_multistate_wider_date_window(self):
        """Multistate actions from different states get a 2-year date window."""
        a = _make_candidate("a1", "CA", date(2023, 1, 1), ["Opioid Corp"],
                           Decimal("26000000000"), is_multistate=True)
        b = _make_candidate("a2", "NY", date(2024, 6, 1), ["Opioid Corp"],
                           Decimal("26000000000"), is_multistate=True)
        match = _compare_pair(a, b)
        assert match is not None
        assert match.match_type == "multistate"

    def test_same_state_temporal_match(self):
        """Same state, same defendant, close dates = temporal duplicate."""
        a = _make_candidate("a1", "CA", date(2024, 6, 1), ["Test Corp"], Decimal("5000000"))
        b = _make_candidate("a2", "CA", date(2024, 6, 15), ["Test Corp"], Decimal("5000000"))
        match = _compare_pair(a, b)
        assert match is not None
        assert match.match_type == "temporal"


# ── find_duplicates ───────────────────────────────────────────────────────

class TestFindDuplicates:
    def test_finds_known_duplicate(self):
        candidates = [
            _make_candidate("a1", "CA", date(2024, 6, 1), ["Acme"], Decimal("1000000")),
            _make_candidate("a2", "NY", date(2024, 6, 5), ["Acme"], Decimal("1000000")),
            _make_candidate("a3", "TX", date(2024, 6, 10), ["Unrelated Inc"]),
        ]
        matches = find_duplicates(candidates)
        assert len(matches) >= 1
        ids = {(m.action_id_a, m.action_id_b) for m in matches}
        assert ("a1", "a2") in ids

    def test_no_duplicates_in_unique_set(self):
        candidates = [
            _make_candidate("a1", "CA", defendants=["Alpha"]),
            _make_candidate("a2", "NY", defendants=["Beta"]),
            _make_candidate("a3", "TX", defendants=["Gamma"]),
        ]
        matches = find_duplicates(candidates)
        assert len(matches) == 0

    def test_sorted_by_confidence(self):
        candidates = [
            _make_candidate("a1", "CA", date(2024, 6, 1), ["Acme"], Decimal("1000000")),
            _make_candidate("a2", "NY", date(2024, 6, 5), ["Acme"], Decimal("1000000")),
            _make_candidate("a3", "TX", date(2024, 6, 20), ["Acme"], Decimal("999000")),
        ]
        matches = find_duplicates(candidates)
        # Should be sorted by confidence descending
        for i in range(len(matches) - 1):
            assert matches[i].confidence >= matches[i + 1].confidence


# ── Clustering ────────────────────────────────────────────────────────────

class TestClusterMultistateMatches:
    def test_two_state_cluster(self):
        candidates = [
            _make_candidate("a1", "CA", date(2024, 6, 1), ["Acme"], Decimal("5000000"), is_multistate=True),
            _make_candidate("a2", "NY", date(2024, 6, 5), ["Acme"], Decimal("5000000"), is_multistate=True),
        ]
        matches = [
            DedupMatch("a1", "a2", "multistate", 0.9, "test"),
        ]
        clusters = cluster_multistate_matches(candidates, matches)
        assert len(clusters) == 1
        assert set(clusters[0].action_ids) == {"a1", "a2"}
        assert set(clusters[0].states) == {"CA", "NY"}

    def test_three_state_transitive_cluster(self):
        """A-B and B-C should form one cluster {A, B, C}."""
        candidates = [
            _make_candidate("a1", "CA", is_multistate=True),
            _make_candidate("a2", "NY", is_multistate=True),
            _make_candidate("a3", "TX", is_multistate=True),
        ]
        matches = [
            DedupMatch("a1", "a2", "multistate", 0.9, "test"),
            DedupMatch("a2", "a3", "multistate", 0.85, "test"),
        ]
        clusters = cluster_multistate_matches(candidates, matches)
        assert len(clusters) == 1
        assert len(clusters[0].action_ids) == 3

    def test_no_multistate_matches(self):
        candidates = [_make_candidate("a1"), _make_candidate("a2")]
        matches = [DedupMatch("a1", "a2", "temporal", 0.8, "test")]
        clusters = cluster_multistate_matches(candidates, matches)
        assert len(clusters) == 0

    def test_cluster_picks_earliest_lead_state(self):
        candidates = [
            _make_candidate("a1", "NY", date(2024, 6, 10), is_multistate=True),
            _make_candidate("a2", "CA", date(2024, 6, 1), is_multistate=True),
        ]
        matches = [DedupMatch("a1", "a2", "multistate", 0.9, "test")]
        clusters = cluster_multistate_matches(candidates, matches)
        assert clusters[0].lead_state == "CA"
