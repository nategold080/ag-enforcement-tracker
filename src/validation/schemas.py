"""Pydantic v2 schemas for all AG Enforcement Tracker data types.

These schemas are the canonical representation of enforcement action data.
They are used for validation at ingestion time and as the contract between
pipeline stages.
"""

import datetime as _dt
import uuid
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator

# Alias datetime types so field names like "date" don't shadow them
Date = _dt.date
DateTime = _dt.datetime


# ---------------------------------------------------------------------------
# Enums – match the ENUM columns defined in CLAUDE.md exactly
# ---------------------------------------------------------------------------

class ActionType(str, Enum):
    SETTLEMENT = "settlement"
    LAWSUIT_FILED = "lawsuit_filed"
    CONSENT_DECREE = "consent_decree"
    ASSURANCE_OF_DISCONTINUANCE = "assurance_of_discontinuance"
    JUDGMENT = "judgment"
    INJUNCTION = "injunction"
    OTHER = "other"


class ActionStatus(str, Enum):
    ANNOUNCED = "announced"
    PENDING = "pending"
    SETTLED = "settled"
    ONGOING = "ongoing"
    CLOSED = "closed"


class EntityType(str, Enum):
    CORPORATION = "corporation"
    INDIVIDUAL = "individual"
    ORGANIZATION = "organization"
    GOVERNMENT_ENTITY = "government_entity"


class DefendantRole(str, Enum):
    PRIMARY = "primary"
    CO_DEFENDANT = "co-defendant"
    RELATED_PARTY = "related_party"


class ExtractionMethod(str, Enum):
    RULES = "rules"
    LLM = "llm"
    HYBRID = "hybrid"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _new_uuid() -> uuid.UUID:
    return uuid.uuid4()


# ---------------------------------------------------------------------------
# Core Schemas
# ---------------------------------------------------------------------------

class DefendantSchema(BaseModel):
    """A defendant (company or individual) named in an enforcement action."""

    id: uuid.UUID = Field(default_factory=_new_uuid)
    raw_name: str = Field(..., min_length=1, description="Name as it appeared in the press release")
    canonical_name: str = Field(default="", description="Normalized name after entity resolution")
    entity_type: EntityType = Field(default=EntityType.CORPORATION)
    industry: Optional[str] = None
    parent_company: Optional[str] = None
    sec_cik: Optional[str] = None


class ActionDefendantSchema(BaseModel):
    """Junction record linking an enforcement action to a defendant."""

    action_id: uuid.UUID
    defendant_id: uuid.UUID
    role: DefendantRole = Field(default=DefendantRole.PRIMARY)


class ViolationCategorySchema(BaseModel):
    """A violation category assigned to an enforcement action."""

    action_id: uuid.UUID
    category: str = Field(..., description="Category key from taxonomy.yaml")
    subcategory: Optional[str] = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class MonetaryTermsSchema(BaseModel):
    """Dollar amounts associated with an enforcement action."""

    action_id: uuid.UUID
    total_amount: Decimal = Field(default=Decimal("0"))
    civil_penalty: Optional[Decimal] = None
    consumer_restitution: Optional[Decimal] = None
    fees_and_costs: Optional[Decimal] = None
    other_monetary: Optional[Decimal] = None
    amount_is_estimated: bool = False


class StatuteCitedSchema(BaseModel):
    """A statute or law cited in an enforcement action."""

    action_id: uuid.UUID
    statute_raw: str = Field(..., description="Citation as it appeared in the press release")
    statute_normalized: str = Field(default="")
    statute_name: str = Field(default="", description="Common name, e.g. 'CCPA', 'UDAP'")
    is_state_statute: bool = False
    is_federal_statute: bool = False


class MultistateActionSchema(BaseModel):
    """A multistate enforcement action linking actions across states."""

    id: uuid.UUID = Field(default_factory=_new_uuid)
    name: str
    lead_state: Optional[str] = Field(None, max_length=2)
    participating_states: list[str] = Field(default_factory=list)
    total_settlement: Optional[Decimal] = None


class EnforcementActionSchema(BaseModel):
    """The core record: a single enforcement action from a state AG."""

    id: uuid.UUID = Field(default_factory=_new_uuid)
    state: str = Field(..., min_length=2, max_length=2, description="Two-letter state code")
    date_announced: Date
    date_filed: Optional[Date] = None
    date_resolved: Optional[Date] = None
    action_type: ActionType = Field(default=ActionType.OTHER)
    status: ActionStatus = Field(default=ActionStatus.ANNOUNCED)
    headline: str = Field(..., min_length=1)
    summary: str = Field(default="")
    source_url: str = Field(..., min_length=1)
    settlement_doc_url: Optional[str] = None
    is_multistate: bool = False
    multistate_action_id: Optional[uuid.UUID] = None
    quality_score: float = Field(default=0.0, ge=0.0, le=1.0)
    extraction_method: ExtractionMethod = Field(default=ExtractionMethod.RULES)
    raw_text: str = Field(default="")
    created_at: DateTime = Field(default_factory=DateTime.utcnow)
    updated_at: DateTime = Field(default_factory=DateTime.utcnow)

    # Related objects (populated during extraction, not stored directly on this row)
    defendants: list[DefendantSchema] = Field(default_factory=list)
    violation_categories: list[ViolationCategorySchema] = Field(default_factory=list)
    monetary_terms: Optional[MonetaryTermsSchema] = None
    statutes_cited: list[StatuteCitedSchema] = Field(default_factory=list)

    @field_validator("state")
    @classmethod
    def uppercase_state(cls, v: str) -> str:
        return v.upper()


class ScrapeRunSchema(BaseModel):
    """Operational record tracking a single scrape run."""

    id: uuid.UUID = Field(default_factory=_new_uuid)
    state: str = Field(..., min_length=2, max_length=2)
    started_at: DateTime = Field(default_factory=DateTime.utcnow)
    completed_at: Optional[DateTime] = None
    press_releases_found: int = 0
    actions_extracted: int = 0
    errors: int = 0
    error_details: list[dict] = Field(default_factory=list)

    @field_validator("state")
    @classmethod
    def uppercase_state(cls, v: str) -> str:
        return v.upper()


# ---------------------------------------------------------------------------
# Lightweight schema for press release listing items (pre-extraction)
# ---------------------------------------------------------------------------

class PressReleaseListItem(BaseModel):
    """A single item from a press release listing page (before fetching body)."""

    title: str
    url: str
    date: Optional[Date] = None
    state: str = Field(..., min_length=2, max_length=2)

    @field_validator("state")
    @classmethod
    def uppercase_state(cls, v: str) -> str:
        return v.upper()


class PressRelease(BaseModel):
    """A fully fetched press release with body text."""

    title: str
    url: str
    date: Optional[Date] = None
    state: str = Field(..., min_length=2, max_length=2)
    body_html: str = ""
    body_text: str = ""

    @field_validator("state")
    @classmethod
    def uppercase_state(cls, v: str) -> str:
        return v.upper()
