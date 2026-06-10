from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ContentType = Literal[
    "job_posting",
    "public_competition",
    "recruitment_page",
    "news_article",
    "press_release",
    "event",
    "informational_page",
    "results_page",
    "historical_archive",
    "broken_page",
    "unknown",
]


class RuleTrace(BaseModel):
    rule_id: str
    weight: float
    matched_text: str | None = None
    reason: str


class ClassificationResult(BaseModel):
    is_job_posting: bool
    content_type: ContentType
    confidence: float
    positive_signals: list[str] = Field(default_factory=list)
    negative_signals: list[str] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    used_llm: bool = False
    llm_reasoning_summary: str | None = None
    score: float = 0.0
    rule_trace: list[RuleTrace] = Field(default_factory=list)
    needs_review: bool = False
