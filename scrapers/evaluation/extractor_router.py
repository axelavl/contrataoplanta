from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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
    extract_threshold_applied: float
    manual_threshold_applied: float
    threshold_validation: dict[str, Any]

_VALIDATED_THRESHOLD_PROFILES = {
    "generic_site",
    "carabineros_pdf_first",
    "pdi_pdf_first",
    "policia_waf",
    "ffaa_waf",
}

_FAMILY_SAMPLE_FLOOR = {
    "generic": 12,
    "pdf_first_waf": 20,
    "waf_protected": 20,
    "wordpress": 16,
    "external_ats": 16,
    "trusted_portal": 10,
    "js_intensive": 20,
}

_FAMILY_DELTA_RULES = {
    "generic": {"good_precision": 0.8, "good_recall": 0.75, "poor_precision": 0.55, "poor_recall": 0.5, "delta": 0.05},
    "pdf_first_waf": {"good_precision": 0.85, "good_recall": 0.7, "poor_precision": 0.6, "poor_recall": 0.5, "delta": 0.05},
    "waf_protected": {"good_precision": 0.84, "good_recall": 0.68, "poor_precision": 0.62, "poor_recall": 0.48, "delta": 0.04},
    "wordpress": {"good_precision": 0.86, "good_recall": 0.74, "poor_precision": 0.58, "poor_recall": 0.5, "delta": 0.04},
    "external_ats": {"good_precision": 0.83, "good_recall": 0.72, "poor_precision": 0.57, "poor_recall": 0.52, "delta": 0.03},
    "trusted_portal": {"good_precision": 0.88, "good_recall": 0.78, "poor_precision": 0.6, "poor_recall": 0.52, "delta": 0.03},
}


def _resolve_thresholds(profile: SourceProfile, source_quality_metrics: dict[str, Any] | None) -> tuple[float, float, dict[str, Any]]:
    extract_threshold = profile.extract_threshold or 0.75
    manual_threshold = profile.manual_threshold or 0.55
    threshold_family = profile.threshold_family or "generic"
    validation: dict[str, Any] = {
        "profile_requires_historical_validation": profile.name in _VALIDATED_THRESHOLD_PROFILES
        or threshold_family in _FAMILY_DELTA_RULES,
        "threshold_family": threshold_family,
        "historical_validation_applied": False,
        "historical_sample_size": 0,
        "historical_quality_band": "unknown",
        "threshold_delta": 0.0,
    }
    if profile.name not in _VALIDATED_THRESHOLD_PROFILES and threshold_family not in _FAMILY_DELTA_RULES:
        return extract_threshold, manual_threshold, validation
    if not source_quality_metrics:
        return extract_threshold, manual_threshold, validation

    sample_size = int(source_quality_metrics.get("sample_size", 0) or 0)
    historical_precision = float(
        source_quality_metrics.get("historical_precision", source_quality_metrics.get("publish_ratio", 0.0)) or 0.0
    )
    historical_recall = float(
        source_quality_metrics.get("historical_recall", 1.0 - float(source_quality_metrics.get("flagged_ratio", 0.0) or 0.0))
        or 0.0
    )
    publish_ratio = float(source_quality_metrics.get("publish_ratio", historical_precision) or 0.0)
    flagged_ratio = float(source_quality_metrics.get("flagged_ratio", 1.0 - historical_recall) or 0.0)
    validation["historical_sample_size"] = sample_size
    validation["historical_precision"] = round(historical_precision, 4)
    validation["historical_recall"] = round(historical_recall, 4)

    family_sample_floor = _FAMILY_SAMPLE_FLOOR.get(threshold_family, 12)
    if sample_size < family_sample_floor:
        validation["historical_quality_band"] = "insufficient_sample"
        return extract_threshold, manual_threshold, validation

    validation["historical_validation_applied"] = True
    delta = 0.0
    quality_band = "stable"

    family_rules = _FAMILY_DELTA_RULES.get(threshold_family)
    if family_rules:
        max_delta = float(family_rules["delta"])
        good_precision = float(family_rules["good_precision"])
        good_recall = float(family_rules["good_recall"])
        poor_precision = float(family_rules["poor_precision"])
        poor_recall = float(family_rules["poor_recall"])
        if historical_precision >= good_precision and historical_recall >= good_recall:
            delta = -max_delta
            quality_band = "high_precision_recall"
        elif historical_precision <= poor_precision or historical_recall <= poor_recall:
            delta = max_delta
            quality_band = "low_precision_or_recall"
    else:
        if flagged_ratio >= 0.45:
            delta = 0.05
            quality_band = "high_noise"
        elif publish_ratio >= 0.8 and flagged_ratio <= 0.15:
            delta = -0.05
            quality_band = "high_precision"

    extract_threshold = min(0.95, max(0.55, round(extract_threshold + delta, 4)))
    manual_threshold = min(extract_threshold - 0.05, max(0.35, round(manual_threshold + delta, 4)))
    validation["historical_quality_band"] = quality_band
    validation["threshold_delta"] = delta
    validation["historical_publish_ratio"] = round(publish_ratio, 4)
    validation["historical_flagged_ratio"] = round(flagged_ratio, 4)
    return extract_threshold, manual_threshold, validation


def select_extractor(
    profile: SourceProfile,
    *,
    availability: Availability,
    page_type: PageType,
    job_relevance: JobRelevance,
    validity_status: ValidityStatus,
    confidence: float,
    source_quality_metrics: dict[str, Any] | None = None,
) -> ExtractorSelection:
    extract_threshold, manual_threshold, threshold_validation = _resolve_thresholds(profile, source_quality_metrics)

    if availability == Availability.JS_REQUIRED and profile.supports_playwright:
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_PLAYWRIGHT,
            decision=Decision.EXTRACT if confidence >= extract_threshold else Decision.MANUAL_REVIEW,
            reason_code=None if confidence >= extract_threshold else ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=None if confidence >= extract_threshold else reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if availability != Availability.OK:
        return ExtractorSelection(
            recommended_extractor=None,
            decision=Decision.SOURCE_STATUS_ONLY,
            reason_code=ReasonCode(availability.value),
            reason_detail=reason_detail(ReasonCode(availability.value)),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if page_type == PageType.DOCUMENT_PAGE and profile.supports_pdf_enrichment:
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_PDF_JOBS,
            decision=Decision.EXTRACT,
            reason_code=None,
            reason_detail=None,
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if page_type == PageType.ATS_EXTERNAL or profile.extractor_hint == ExtractorKind.SCRAPER_EXTERNAL_ATS:
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_EXTERNAL_ATS,
            decision=Decision.EXTRACT if confidence >= extract_threshold else Decision.MANUAL_REVIEW,
            reason_code=None if confidence >= extract_threshold else ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=None if confidence >= extract_threshold else reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if profile.name == "empleos_publicos":
        return ExtractorSelection(
            recommended_extractor=ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS,
            decision=Decision.EXTRACT,
            reason_code=None,
            reason_detail=None,
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
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
                extract_threshold_applied=extract_threshold,
                manual_threshold_applied=manual_threshold,
                threshold_validation=threshold_validation,
            )
        return ExtractorSelection(
            recommended_extractor=extractor,
            decision=Decision.EXTRACT if confidence >= extract_threshold else Decision.MANUAL_REVIEW,
            reason_code=None if confidence >= extract_threshold else ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=None if confidence >= extract_threshold else reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if validity_status == ValidityStatus.EXPIRED_CONFIRMED:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.SKIP,
            reason_code=ReasonCode.ONLY_EXPIRED_CALLS,
            reason_detail=reason_detail(ReasonCode.ONLY_EXPIRED_CALLS),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if page_type == PageType.LISTING_PAGE and confidence < extract_threshold:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.MANUAL_REVIEW,
            reason_code=ReasonCode.LISTING_WITHOUT_OFFER_DETAIL,
            reason_detail=reason_detail(ReasonCode.LISTING_WITHOUT_OFFER_DETAIL),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if confidence >= extract_threshold:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.EXTRACT,
            reason_code=None,
            reason_detail=None,
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    if confidence >= manual_threshold:
        return ExtractorSelection(
            recommended_extractor=profile.extractor_hint,
            decision=Decision.MANUAL_REVIEW,
            reason_code=ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            extract_threshold_applied=extract_threshold,
            manual_threshold_applied=manual_threshold,
            threshold_validation=threshold_validation,
        )

    return ExtractorSelection(
        recommended_extractor=profile.extractor_hint,
        decision=Decision.SKIP,
        reason_code=ReasonCode.NO_MATCHING_EXTRACTOR,
        reason_detail=reason_detail(ReasonCode.NO_MATCHING_EXTRACTOR),
        extract_threshold_applied=extract_threshold,
        manual_threshold_applied=manual_threshold,
        threshold_validation=threshold_validation,
    )
