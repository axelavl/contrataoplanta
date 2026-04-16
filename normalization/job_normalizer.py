from __future__ import annotations

from datetime import datetime, timezone

from models.classification import ClassificationResult
from models.date_models import DateResolution
from models.extraction import ExtractionBundle
from models.job_posting import JobPosting
from models.raw_page import RawPage


def normalize_job_posting(
    raw_page: RawPage,
    classification: ClassificationResult,
    extraction: ExtractionBundle,
    dates: DateResolution,
    quality_scores: dict,
) -> JobPosting:
    return JobPosting(
        source_id=raw_page.source_id,
        source_name=raw_page.source_name,
        platform=raw_page.platform,
        job_title=extraction.job_title,
        job_url=raw_page.final_url or raw_page.url,
        external_job_id=None,
        job_type=classification.content_type,
        department_or_unit=extraction.department_or_unit,
        location=None,
        region=None,
        vacancies_count=None,
        salary_amount=extraction.salary_amount,
        salary_currency=extraction.salary_currency,
        salary_raw=extraction.salary_raw,
        contract_type=extraction.contract_type,
        workday=extraction.workday,
        modality=extraction.modality,
        published_at=dates.published_at,
        application_start_at=dates.application_start_at,
        application_end_at=dates.application_end_at,
        is_expired=dates.is_expired,
        expiration_reason=dates.expiration_reason,
        date_confidence=dates.date_confidence,
        description=extraction.description,
        functions=extraction.functions,
        requirements=extraction.requirements,
        desirable_requirements=extraction.desirable_requirements,
        documents_required=extraction.documents_required,
        benefits=[],
        selection_process=extraction.selection_process,
        attachments=raw_page.attachment_urls,
        classification_confidence=quality_scores["classification_confidence"],
        field_completeness_score=quality_scores["field_completeness_score"],
        overall_quality_score=quality_scores["overall_quality_score"],
        needs_review=bool(quality_scores["needs_review"]),
        scraped_at=datetime.now(timezone.utc),
    )
