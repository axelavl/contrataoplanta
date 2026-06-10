from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from classification.content_classifier import ContentClassifier
from extraction.date_extractor import (
    extract_dates_from_attachments,
    extract_dates_from_tables,
    extract_dates_from_text,
    resolve_best_dates,
)
from extraction.field_extractors import extract_structured_fields
from models.raw_page import RawPage
from normalization.job_normalizer import normalize_job_posting
from validation.expiry_validator import validate_expiry
from validation.job_validator import validate_minimum_completeness
from validation.quality_scoring import score_quality


class JobExtractionPipeline:
    def __init__(self) -> None:
        self.classifier = ContentClassifier()

    def run(self, raw_page: RawPage) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        logs: dict[str, Any] = {
            "url": raw_page.url,
            "source": raw_page.source_name,
            "classification": None,
            "rejection_reasons": [],
            "attachments_reviewed": raw_page.attachment_urls,
            "missing_fields": [],
        }

        classification = self.classifier.classify(raw_page)
        logs["classification"] = classification.model_dump()

        if not classification.is_job_posting:
            logs["rejection_reasons"] = classification.rejection_reasons or ["non_job_content"]
            return None, logs

        extraction = extract_structured_fields(raw_page)
        evidences = []
        evidences.extend(extract_dates_from_text(raw_page.html_text))
        evidences.extend(extract_dates_from_tables(raw_page.tables_text))
        evidences.extend(extract_dates_from_attachments(raw_page.attachment_texts))
        dates = resolve_best_dates(evidences)

        expired, reason = validate_expiry(classification, dates)
        if expired:
            classification.is_job_posting = False
            logs["rejection_reasons"] = [reason]
            return None, logs

        ok, missing = validate_minimum_completeness(extraction)
        logs["missing_fields"] = missing
        if not ok:
            logs["rejection_reasons"] = ["minimum_completeness_failed"]
            return None, logs

        quality = score_quality(classification, extraction, dates)
        posting = normalize_job_posting(raw_page, classification, extraction, dates, quality)
        return posting.model_dump(mode="json"), logs


def build_raw_page_from_generic(
    source_id: str,
    source_name: str,
    source_url: str,
    raw: dict[str, Any],
    platform: str = "generic_site",
) -> RawPage:
    return RawPage(
        source_id=source_id,
        source_name=source_name,
        platform=platform,
        url=raw.get("url") or source_url,
        final_url=raw.get("url") or source_url,
        title=raw.get("title"),
        meta_description=None,
        breadcrumbs=raw.get("breadcrumbs") or [],
        section_hint=raw.get("section_hint"),
        html_text=raw.get("content_text") or "",
        tables_text=raw.get("tables_text") or [],
        attachment_urls=raw.get("pdf_links") or [],
        attachment_texts=raw.get("attachment_texts") or [],
        found_dates=[str(raw.get("date") or ""), str(raw.get("fecha_cierre") or "")],
        discovered_at=datetime.now(timezone.utc),
        http_status=raw.get("http_status"),
        headings=raw.get("headings") or [],
    )
