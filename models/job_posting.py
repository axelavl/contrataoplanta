from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class JobPosting(BaseModel):
    source_id: str
    source_name: str
    source_sigla: str | None = None
    platform: str | None = None
    job_title: str | None = None
    job_url: str
    external_job_id: str | None = None
    job_type: str | None = None
    department_or_unit: str | None = None
    location: str | None = None
    region: str | None = None
    vacancies_count: int | None = None
    salary_amount: float | None = None
    salary_currency: str | None = None
    salary_raw: str | None = None
    contract_type: str | None = None
    workday: str | None = None
    modality: str | None = None
    published_at: datetime | None = None
    application_start_at: datetime | None = None
    application_end_at: datetime | None = None
    is_expired: bool | None = None
    expiration_reason: str | None = None
    date_confidence: str = "low"
    description: str | None = None
    functions: list[str] = Field(default_factory=list)
    requirements: list[str] = Field(default_factory=list)
    desirable_requirements: list[str] = Field(default_factory=list)
    documents_required: list[str] = Field(default_factory=list)
    benefits: list[str] = Field(default_factory=list)
    selection_process: str | None = None
    attachments: list[str] = Field(default_factory=list)
    classification_confidence: float = 0.0
    field_completeness_score: float = 0.0
    overall_quality_score: float = 0.0
    needs_review: bool = False
    scraped_at: datetime
