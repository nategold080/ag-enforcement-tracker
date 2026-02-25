"""Structured field extraction from press release text.

This module orchestrates the extraction pipeline:
1. Rule-based pattern extraction (amounts, dates, statutes, defendants)
2. Violation category classification
3. Action type classification
4. Quality scoring

LLM fallback is NOT used here — that's in llm_fallback.py (Phase 5).
"""

import logging
import uuid
from datetime import date as Date
from decimal import Decimal
from typing import Optional

from src.extractors.patterns import (
    ExtractedAmount,
    classify_action_type,
    classify_monetary_components,
    extract_defendants_from_body,
    extract_defendants_from_headline,
    extract_dollar_amounts,
    extract_filed_date,
    extract_largest_dollar_amount,
    extract_resolved_date,
    extract_settlement_amount,
    extract_statutes,
    is_multistate_action,
)
from src.validation.schemas import (
    ActionStatus,
    ActionType,
    DefendantSchema,
    EnforcementActionSchema,
    ExtractionMethod,
    MonetaryTermsSchema,
    PressRelease,
    StatuteCitedSchema,
    ViolationCategorySchema,
)

logger = logging.getLogger(__name__)


class PressReleaseExtractor:
    """Extract structured enforcement action data from a press release.

    Uses rules-first extraction: regex patterns and keyword matching.
    No LLM calls. Deterministic output for the same input.
    """

    def __init__(self, taxonomy: dict):
        """Initialize with the violation taxonomy from config/taxonomy.yaml."""
        self.taxonomy = taxonomy

    def extract(
        self,
        press_release: PressRelease,
        date_announced: Optional[Date] = None,
    ) -> EnforcementActionSchema:
        """Extract an EnforcementActionSchema from a PressRelease.

        Args:
            press_release: The fetched press release with body text.
            date_announced: Override date (from listing page), or use PR date.

        Returns:
            A fully populated EnforcementActionSchema with related objects.
        """
        headline = press_release.title
        body = press_release.body_text
        announced = date_announced or press_release.date

        # 1. Action type
        action_type_str = classify_action_type(headline, body)
        action_type = ActionType(action_type_str)

        # 2. Status — infer from action type
        status = self._infer_status(action_type)

        # 3. Dates
        date_filed = extract_filed_date(body)
        date_resolved = extract_resolved_date(body)

        # 4. Dollar amounts — use context-aware extraction (headline > settlement context > largest)
        largest_amount = extract_settlement_amount(headline, body)
        monetary_components = classify_monetary_components(body)

        # 5. Defendants
        defendants = self._extract_defendants(headline, body)

        # 6. Statutes
        statutes = self._extract_statutes(body)

        # 7. Violation categories
        categories = self._classify_violations(headline, body)

        # 8. Multistate detection
        multistate = is_multistate_action(headline, body)

        # 9. Summary (first 2-3 sentences)
        summary = self._generate_summary(body)

        # Build the action ID (needed for related objects)
        action_id = uuid.uuid4()

        # Build monetary terms — ONLY for resolved action types.
        # Lawsuits, injunctions, and "other" mention dollar figures ("$2.3B in
        # grants at stake") that are NOT settlement amounts.  Storing them as
        # settlement amounts is misleading.  We only keep extracted amounts when
        # the action type indicates an actual financial resolution.
        _AMOUNT_ELIGIBLE_TYPES = {ActionType.SETTLEMENT, ActionType.JUDGMENT,
                                  ActionType.CONSENT_DECREE,
                                  ActionType.ASSURANCE_OF_DISCONTINUANCE}

        monetary_terms = None
        if action_type in _AMOUNT_ELIGIBLE_TYPES and (largest_amount or monetary_components):
            total = largest_amount.amount if largest_amount else Decimal("0")
            is_estimated = largest_amount.is_estimated if largest_amount else False

            monetary_terms = MonetaryTermsSchema(
                action_id=action_id,
                total_amount=total,
                civil_penalty=monetary_components.get("civil_penalty"),
                consumer_restitution=monetary_components.get("consumer_restitution"),
                fees_and_costs=monetary_components.get("fees_and_costs"),
                amount_is_estimated=is_estimated,
            )

        # Build violation category schemas
        violation_schemas = [
            ViolationCategorySchema(
                action_id=action_id,
                category=cat,
                subcategory=subcat,
                confidence=conf,
            )
            for cat, subcat, conf in categories
        ]

        # Build statute schemas
        statute_schemas = [
            StatuteCitedSchema(
                action_id=action_id,
                statute_raw=s.raw_citation,
                statute_normalized="",
                statute_name=s.common_name,
                is_state_statute=s.is_state,
                is_federal_statute=s.is_federal,
            )
            for s in statutes
        ]

        # Build defendant schemas
        defendant_schemas = [
            DefendantSchema(raw_name=name)
            for name in defendants
        ]

        # Quality score — only count real categories (not fallback "other")
        has_real_category = any(c[0] != "other" for c in categories)
        quality_score = self._compute_quality_score(
            has_defendants=bool(defendants),
            has_amount=largest_amount is not None,
            has_category=has_real_category,
            has_statute=bool(statutes),
            has_date=announced is not None,
            action_type=action_type,
            body_length=len(body),
        )

        return EnforcementActionSchema(
            id=action_id,
            state=press_release.state,
            date_announced=announced or Date.today(),
            date_filed=date_filed,
            date_resolved=date_resolved,
            action_type=action_type,
            status=status,
            headline=headline,
            summary=summary,
            source_url=press_release.url,
            is_multistate=multistate,
            quality_score=quality_score,
            extraction_method=ExtractionMethod.RULES,
            raw_text=body,
            defendants=defendant_schemas,
            violation_categories=violation_schemas,
            monetary_terms=monetary_terms,
            statutes_cited=statute_schemas,
        )

    def _infer_status(self, action_type: ActionType) -> ActionStatus:
        """Infer the action status from the action type."""
        if action_type in (ActionType.SETTLEMENT, ActionType.CONSENT_DECREE, ActionType.JUDGMENT):
            return ActionStatus.SETTLED
        elif action_type == ActionType.LAWSUIT_FILED:
            return ActionStatus.PENDING
        elif action_type == ActionType.INJUNCTION:
            return ActionStatus.ONGOING
        return ActionStatus.ANNOUNCED

    def _extract_defendants(self, headline: str, body: str) -> list[str]:
        """Extract unique defendant names from headline and body."""
        names: list[str] = []
        seen: set[str] = set()

        # Headline first (higher confidence)
        for name in extract_defendants_from_headline(headline):
            key = name.lower().strip()
            if key not in seen:
                seen.add(key)
                names.append(name)

        # Then body
        for name in extract_defendants_from_body(body):
            key = name.lower().strip()
            if key not in seen:
                seen.add(key)
                names.append(name)

        return names

    def _extract_statutes(self, body: str) -> list:
        """Extract statute citations from body text."""
        return extract_statutes(body)

    def _classify_violations(
        self, headline: str, body: str,
    ) -> list[tuple[str, Optional[str], float]]:
        """Classify the press release into violation categories.

        Returns a list of (category_key, subcategory, confidence) tuples.
        Uses keyword matching against the taxonomy.
        """
        combined = (headline + " " + body).lower()
        results: list[tuple[str, Optional[str], float]] = []

        categories = self.taxonomy.get("categories", {})
        for cat_key, cat_data in categories.items():
            if cat_key == "other":
                continue

            keywords = cat_data.get("keywords", [])
            matched_keywords = [kw for kw in keywords if self._keyword_matches(kw.lower(), combined)]

            if matched_keywords:
                # Confidence based on number of keyword matches
                confidence = min(1.0, 0.5 + 0.1 * len(matched_keywords))

                # Try to identify subcategory
                subcategory = self._match_subcategory(combined, cat_data.get("subcategories", []))

                results.append((cat_key, subcategory, confidence))

        # If no categories matched, assign "other"
        if not results:
            results.append(("other", None, 0.3))

        return results

    @staticmethod
    def _keyword_matches(keyword: str, text: str) -> bool:
        """Check if a keyword matches in text using word boundaries.

        Always uses word boundary matching to prevent substring false positives
        like 'rent' matching 'current' or 'parent'.
        """
        import re
        return bool(re.search(r'\b' + re.escape(keyword) + r'\b', text))

    def _match_subcategory(self, text_lower: str, subcategories: list[str]) -> Optional[str]:
        """Try to match a subcategory by keyword presence."""
        for subcat in subcategories:
            # Use the subcategory name itself as a keyword pattern
            subcat_lower = subcat.lower()
            # Split on "/" and check individual parts
            parts = [p.strip() for p in subcat_lower.replace("(", "").replace(")", "").split("/")]
            for part in parts:
                words = part.split()
                if all(w in text_lower for w in words if len(w) > 2):
                    return subcat
        return None

    def _generate_summary(self, body: str) -> str:
        """Generate a summary from the first 2-3 sentences of the body text."""
        # Split on sentence-ending punctuation
        sentences = []
        current = []
        for char in body:
            current.append(char)
            if char in ".!?" and len(current) > 20:
                sentence = "".join(current).strip()
                if sentence:
                    sentences.append(sentence)
                current = []
                if len(sentences) >= 3:
                    break

        return " ".join(sentences).strip()

    def _compute_quality_score(
        self,
        has_defendants: bool,
        has_amount: bool,
        has_category: bool,
        has_statute: bool,
        has_date: bool,
        action_type: ActionType,
        body_length: int,
    ) -> float:
        """Compute a quality score (0.0-1.0) based on extraction completeness."""
        score = 0.0

        # Date is present
        if has_date:
            score += 0.15

        # Has at least one defendant
        if has_defendants:
            score += 0.25

        # Dollar amount found (expected for settlements, not for injunctions)
        if has_amount:
            score += 0.20
        elif action_type in (ActionType.INJUNCTION, ActionType.LAWSUIT_FILED):
            score += 0.10  # Partial credit — amount not expected

        # Violation category identified (not just "other")
        if has_category:
            score += 0.15

        # Statute citation found
        if has_statute:
            score += 0.10

        # Body text is substantial
        if body_length > 500:
            score += 0.10
        elif body_length > 200:
            score += 0.05

        # Action type is specific (not "other")
        if action_type != ActionType.OTHER:
            score += 0.05

        return min(1.0, round(score, 2))
