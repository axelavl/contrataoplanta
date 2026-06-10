from __future__ import annotations

from models.classification import ClassificationResult
from models.date_models import DateResolution
from models.extraction import ExtractionBundle


def score_quality(
    classification: ClassificationResult,
    extraction: ExtractionBundle,
    dates: DateResolution,
) -> dict[str, float | bool]:
    completeness = 0.0
    for flag in [
        bool(extraction.job_title),
        bool(extraction.functions),
        bool(extraction.requirements),
        bool(extraction.salary_amount),
        bool(extraction.contract_type),
        bool(dates.application_end_at),
    ]:
        completeness += 1.0 if flag else 0.0
    field_completeness = completeness / 6

    date_conf = {"low": 0.35, "medium": 0.65, "high": 0.95}.get(dates.date_confidence, 0.35)
    salary_conf = 0.85 if extraction.salary_amount else 0.25
    requirements_conf = 0.85 if extraction.requirements else 0.25

    overall = (
        classification.confidence * 0.30
        + field_completeness * 0.25
        + date_conf * 0.20
        + salary_conf * 0.10
        + requirements_conf * 0.15
    )
    overall -= 0.2 if classification.content_type in {"news_article", "results_page"} else 0.0
    overall = max(0.0, min(1.0, overall))
    needs_review = overall < 0.60 or classification.needs_review

    return {
        "classification_confidence": round(classification.confidence, 4),
        "field_completeness_score": round(field_completeness, 4),
        "date_confidence_score": round(date_conf, 4),
        "salary_confidence_score": round(salary_conf, 4),
        "requirements_confidence_score": round(requirements_conf, 4),
        "overall_quality_score": round(overall, 4),
        "needs_review": needs_review,
    }
