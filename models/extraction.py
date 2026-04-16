from __future__ import annotations

from pydantic import BaseModel, Field


class ExtractionBundle(BaseModel):
    job_title: str | None = None
    functions: list[str] = Field(default_factory=list)
    requirements: list[str] = Field(default_factory=list)
    desirable_requirements: list[str] = Field(default_factory=list)
    salary_amount: float | None = None
    salary_currency: str | None = None
    salary_raw: str | None = None
    contract_type: str | None = None
    workday: str | None = None
    modality: str | None = None
    documents_required: list[str] = Field(default_factory=list)
    department_or_unit: str | None = None
    description: str | None = None
    selection_process: str | None = None
    attachments_used: list[str] = Field(default_factory=list)
    traces: list[str] = Field(default_factory=list)
