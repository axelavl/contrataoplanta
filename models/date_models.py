from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class DateEvidence(BaseModel):
    label: str
    raw_text: str
    value: datetime | None = None
    confidence: float = 0.0
    source: str = "text"


class DateResolution(BaseModel):
    published_at: datetime | None = None
    application_start_at: datetime | None = None
    application_end_at: datetime | None = None
    is_expired: bool | None = None
    expiration_reason: str | None = None
    date_confidence: str = "low"
    evidence: list[DateEvidence] = Field(default_factory=list)
