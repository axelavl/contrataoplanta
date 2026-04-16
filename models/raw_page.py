from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class RawPage(BaseModel):
    source_id: str
    source_name: str
    platform: str | None = None
    url: str
    final_url: str | None = None
    title: str | None = None
    meta_description: str | None = None
    breadcrumbs: list[str] = Field(default_factory=list)
    section_hint: str | None = None
    html_text: str
    tables_text: list[str] = Field(default_factory=list)
    attachment_urls: list[str] = Field(default_factory=list)
    attachment_texts: list[str] = Field(default_factory=list)
    found_dates: list[str] = Field(default_factory=list)
    discovered_at: datetime
    http_status: int | None = None
    headings: list[str] = Field(default_factory=list)
