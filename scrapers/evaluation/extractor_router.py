from __future__ import annotations

from dataclasses import dataclass

from .models import (
    Availability,
    Decision,
    EvaluationResult,
    ExtractorKind,
    JobRelevance,
    PageType,
    SourceProfile,
    ValidityStatus,
)
from .reason_codes import ReasonCode, reason_detail


@dataclass(slots=True)
class ExtractorSelection:
    recommended_extractor: ExtractorKind | None
    decision: Decision
    reason_code: ReasonCode | None
    reason_detail: str | None
    extract_threshold_applied: float | None = None
    manual_threshold_applied: float | None = None


def select_extractor(profile: SourceProfile, *, availability: Availability, page_type: PageType, job_relevance: JobRelevance, validity_status: ValidityStatus, confidence: float) -> ExtractorSelection:
    extract_threshold = profile.extract_threshold or 0.75
    manual_threshold = profile.manual_threshold or 0.55

    if availability == Availability.JS_REQUIRED and profile.supports_playwright:
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_PLAYWRIGHT,
            decision=Decision.EXTRACT if confidence >= extract_threshold else Decision.MANUAL_REVIEW,
            reason_code=None if confidence >= extract_threshold else ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=None if confidence >= extract_threshold else reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
        )

    if availability != Availability.OK:
        return ExtractorSelection(
            recommended_extractor=None,
            decision=Decision.SOURCE_STATUS_ONLY,
            reason_code=ReasonCode(availability.value),
            reason_detail=reason_detail(ReasonCode(availability.value)),
        )

    if page_type == PageType.DOCUMENT_PAGE and profile.supports_pdf_enrichment:
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_PDF_JOBS,
            decision=Decision.EXTRACT,
            reason_code=None,
            reason_detail=None,
        )

    if page_type == PageType.ATS_EXTERNAL or profile.extractor_hint == ExtractorKind.SCRAPER_EXTERNAL_ATS:
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_EXTERNAL_ATS,
            decision=Decision.EXTRACT if confidence >= extract_threshold else Decision.MANUAL_REVIEW,
            reason_code=None if confidence >= extract_threshold else ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=None if confidence >= extract_threshold else reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
        )

    if profile.name == "empleos_publicos":
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS,
            decision=Decision.EXTRACT,
            reason_code=None,
            reason_detail=None,
        )

    if page_type in {PageType.WORDPRESS_POST, PageType.WORDPRESS_LISTING}:
        extractor = (
            ExtractorKind.SCRAPER_WORDPRESS_JOBS
            if job_relevance != JobRelevance.NON_JOB
            else ExtractorKind.SCRAPER_WORDPRESS_NEWS_FILTER
        )
        if job_relevance == JobRelevance.NON_JOB or validity_status == ValidityStatus.EXPIRED_BY_PUBLICATION_AGE:
            return ExtractorSelection(
                recommended_extractor=extractor,
                decision=Decision.SKIP,
                reason_code=ReasonCode.NOT_JOB_RELATED if job_relevance == JobRelevance.NON_JOB else ReasonCode.PUBLICATION_TOO_OLD,
                reason_detail=reason_detail(ReasonCode.NOT_JOB_RELATED if job_relevance == JobRelevance.NON_JOB else ReasonCode.PUBLICATION_TOO_OLD),
            )
        return ExtractorSelection(
            recommended_extractor=extractor,
            decision=Decision.EXTRACT if confidence >= extract_threshold else Decision.MANUAL_REVIEW,
            reason_code=None if confidence >= extract_threshold else ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=None if confidence >= extract_threshold else reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
        )

    if validity_status == ValidityStatus.EXPIRED_CONFIRMED:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.SKIP,
            reason_code=ReasonCode.ONLY_EXPIRED_CALLS,
            reason_detail=reason_detail(ReasonCode.ONLY_EXPIRED_CALLS),
        )

    if page_type == PageType.LISTING_PAGE and confidence < extract_threshold:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.MANUAL_REVIEW,
            reason_code=ReasonCode.LISTING_WITHOUT_OFFER_DETAIL,
            reason_detail=reason_detail(ReasonCode.LISTING_WITHOUT_OFFER_DETAIL),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
        )

    if confidence >= extract_threshold:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.EXTRACT,
            reason_code=None,
            reason_detail=None,
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
        )

    if confidence >= manual_threshold:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.MANUAL_REVIEW,
            reason_code=ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
        )

    return ExtractorSelection(
        recommended_extractor=profile.extractor_hint,
        decision=Decision.SKIP,
        reason_code=ReasonCode.NO_MATCHING_EXTRACTOR,
        reason_detail=reason_detail(ReasonCode.NO_MATCHING_EXTRACTOR),
        extract_threshold_applied=extract_threshold,
        manual_threshold_applied=manual_threshold,
    )
