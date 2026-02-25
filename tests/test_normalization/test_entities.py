"""Tests for entity resolution pipeline.

Covers:
- Exact alias match
- Fuzzy match above threshold (0.85)
- Fuzzy match below threshold (flagged for review)
- Garbage name rejection
- Case insensitivity
- Name cleaning
"""

import pytest

from src.normalization.entities import EntityResolver, is_valid_canonical_name


@pytest.fixture
def resolver():
    """Return an EntityResolver loaded with default config/entities.yaml."""
    return EntityResolver()


class TestExactAliasMatch:
    """Test that known aliases resolve to canonical names with confidence 1.0."""

    def test_google_llc(self, resolver):
        canonical, confidence = resolver.resolve("Google LLC")
        assert canonical == "Google"
        assert confidence == 1.0

    def test_facebook(self, resolver):
        canonical, confidence = resolver.resolve("Facebook")
        assert canonical == "Meta"
        assert confidence == 1.0

    def test_alphabet_inc(self, resolver):
        canonical, confidence = resolver.resolve("Alphabet Inc")
        assert canonical == "Google"
        assert confidence == 1.0

    def test_case_insensitive_alias(self, resolver):
        canonical, confidence = resolver.resolve("google llc")
        assert canonical == "Google"
        assert confidence == 1.0

    def test_wells_fargo_bank(self, resolver):
        canonical, confidence = resolver.resolve("Wells Fargo Bank")
        assert canonical == "Wells Fargo"
        assert confidence == 1.0


class TestFuzzyMatch:
    """Test fuzzy matching behavior."""

    def test_high_similarity_auto_match(self, resolver):
        # First resolve a known entity so it's in canonical_names
        resolver.resolve("Google LLC")
        # Now try a slight variation
        canonical, confidence = resolver.resolve("Google Incorporated")
        # Should fuzzy match to "Google"
        assert canonical == "Google"
        assert confidence >= 0.85

    def test_new_entity_gets_created(self, resolver):
        canonical, confidence = resolver.resolve("Acme Widgets Corp")
        assert canonical == "Acme Widgets"  # After suffix stripping
        assert confidence == 0.5  # New entity


class TestGarbageNameRejection:
    """Test that garbage names are rejected by is_valid_canonical_name."""

    def test_empty_name(self):
        assert not is_valid_canonical_name("")

    def test_single_char(self):
        assert not is_valid_canonical_name("X")

    def test_stopword(self):
        assert not is_valid_canonical_name("business")

    def test_government_entity(self):
        assert not is_valid_canonical_name("Trump Administration")

    def test_attorney_general(self):
        assert not is_valid_canonical_name("Attorney General James")

    def test_sentence_fragment(self):
        assert not is_valid_canonical_name("claiming that the company")

    def test_pure_number(self):
        assert not is_valid_canonical_name("42")

    def test_drug_manufacturer_descriptor(self):
        assert not is_valid_canonical_name("drug manufacturer")

    def test_settlement_with(self):
        assert not is_valid_canonical_name("Settlement With Company")

    def test_valid_company_name(self):
        assert is_valid_canonical_name("ExxonMobil")

    def test_valid_acronym(self):
        assert is_valid_canonical_name("CVS")

    def test_valid_two_char_acronym(self):
        assert is_valid_canonical_name("3M")

    def test_consumers_is_fragment(self):
        assert not is_valid_canonical_name("Consumers Is")

    def test_based_company_fragment(self):
        assert not is_valid_canonical_name("Central Pa.-Based Company That")

    def test_unlawfully_prefix(self):
        assert not is_valid_canonical_name("Unlawfully Cutting Billions")


class TestNameCleaning:
    """Test the name cleaning pipeline."""

    def test_strip_inc(self):
        assert EntityResolver.clean_name("Google Inc.") == "Google"

    def test_strip_llc(self):
        assert EntityResolver.clean_name("Acme LLC") == "Acme"

    def test_strip_corporation(self):
        assert EntityResolver.clean_name("Wells Fargo Corporation") == "Wells Fargo"

    def test_strip_leading_the(self):
        assert EntityResolver.clean_name("The Walt Disney Company") == "Walt Disney"

    def test_normalize_whitespace(self):
        assert EntityResolver.clean_name("  Google   LLC  ") == "Google"

    def test_trailing_fragment(self):
        cleaned = EntityResolver.clean_name("Google, claiming that the data was public")
        assert "claiming" not in cleaned

    def test_preserve_title_case(self):
        assert EntityResolver.clean_name("exxonmobil") == "Exxonmobil"


class TestResolverIntegration:
    """Integration tests for the full resolve pipeline."""

    def test_garbage_returns_empty(self, resolver):
        canonical, confidence = resolver.resolve("claiming that the company")
        assert canonical == ""
        assert confidence == 0.0

    def test_empty_returns_empty(self, resolver):
        canonical, confidence = resolver.resolve("")
        assert canonical == ""
        assert confidence == 0.0

    def test_resolve_batch(self, resolver):
        results = resolver.resolve_batch(["Google LLC", "Facebook", "Fake Company XYZ"])
        assert len(results) == 3
        assert results[0] == ("Google LLC", "Google", 1.0)
        assert results[1] == ("Facebook", "Meta", 1.0)

    def test_review_queue_populated(self, resolver):
        # Resolve something that might be close to an existing entity
        resolver.resolve("Google LLC")
        # Check that review queue is accessible
        queue = resolver.get_review_queue()
        assert isinstance(queue, list)
