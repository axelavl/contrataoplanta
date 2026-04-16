from __future__ import annotations

import re
from typing import Protocol

from pydantic import BaseModel, Field

from models.classification import ClassificationResult
from models.raw_page import RawPage


class LLMClassifierResponse(BaseModel):
    is_job_posting: bool
    content_type: str
    confidence: float
    reason: str
    likely_job_title: str | None = None
    evidence_for_job: list[str] = Field(default_factory=list)
    evidence_against_job: list[str] = Field(default_factory=list)


class LLMClient(Protocol):
    def classify_content(self, prompt: dict) -> dict: ...


class HeuristicLLMClient:
    """Mock determinístico para fallback cuando no hay proveedor real."""

    def classify_content(self, prompt: dict) -> dict:
        text = (prompt.get("content_preview") or "").lower()
        for_job = []
        against = []
        if re.search(r"cargo|postulación|requisitos|funciones", text):
            for_job.append("fragmentos laborales presentes")
        if re.search(r"noticia|comunicado|evento|resultados", text):
            against.append("fragmentos no laborales presentes")

        is_job = len(for_job) >= len(against)
        return {
            "is_job_posting": is_job,
            "content_type": "job_posting" if is_job else "news_article",
            "confidence": 0.62,
            "reason": "clasificación heurística de fallback",
            "likely_job_title": prompt.get("title"),
            "evidence_for_job": for_job,
            "evidence_against_job": against,
        }


def build_llm_summary(raw_page: RawPage, max_chars: int = 1600) -> dict:
    content = raw_page.html_text[:max_chars]
    joined = f"{raw_page.title or ''}\n{raw_page.html_text}"
    date_snippets = re.findall(r"[^.\n]{0,60}(?:\d{1,2}[^.\n]{0,20}(?:202\d|\d{1,2}:\d{2}))[^.\n]{0,60}", joined)

    def pick(pattern: str) -> list[str]:
        return re.findall(rf"[^.\n]{{0,80}}(?:{pattern})[^.\n]{{0,80}}", joined, flags=re.IGNORECASE)

    return {
        "title": raw_page.title,
        "url": raw_page.url,
        "breadcrumbs": raw_page.breadcrumbs,
        "headings": raw_page.headings,
        "content_preview": content,
        "date_fragments": date_snippets[:5],
        "job_fragments": pick(r"cargo|requisitos|renta|funciones|postulación")[:8],
        "non_job_fragments": pick(r"noticia|evento|resultados|nómina|comunicado")[:8],
        "pdf_names": [u.rsplit("/", 1)[-1] for u in raw_page.attachment_urls],
    }


def classify_with_llm_fallback(
    raw_page: RawPage,
    current: ClassificationResult,
    llm_client: LLMClient | None = None,
) -> ClassificationResult:
    client = llm_client or HeuristicLLMClient()
    payload = build_llm_summary(raw_page)
    parsed = LLMClassifierResponse.model_validate(client.classify_content(payload))

    strong_structural_negative = any(
        "URL de noticias" in n or "resultados/cierre" in n for n in current.negative_signals
    )
    contradiction = parsed.is_job_posting and strong_structural_negative

    return current.model_copy(
        update={
            "is_job_posting": parsed.is_job_posting,
            "content_type": parsed.content_type,
            "confidence": max(current.confidence, min(1.0, parsed.confidence)),
            "used_llm": True,
            "llm_reasoning_summary": parsed.reason,
            "positive_signals": current.positive_signals + parsed.evidence_for_job,
            "negative_signals": current.negative_signals + parsed.evidence_against_job,
            "needs_review": contradiction,
        }
    )
