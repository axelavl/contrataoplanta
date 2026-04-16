from __future__ import annotations

from models.classification import ClassificationResult
from models.date_models import DateResolution


def validate_expiry(classification: ClassificationResult, dates: DateResolution) -> tuple[bool, str | None]:
    if classification.content_type in {"results_page", "historical_archive", "news_article", "event"}:
        return True, f"content_type={classification.content_type}"

    if dates.is_expired:
        return True, dates.expiration_reason or "expired"

    if classification.content_type == "informational_page" and not classification.is_job_posting:
        return True, "informational_non_posting"

    return False, None
