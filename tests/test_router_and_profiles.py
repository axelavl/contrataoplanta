from __future__ import annotations

from scrapers.evaluation.extractor_router import select_extractor
from scrapers.evaluation.models import (
    Availability,
    Decision,
    ExtractorKind,
    JobRelevance,
    PageType,
    ValidityStatus,
)
from scrapers.evaluation.source_profiles import classify_source_profile, match_source_profile


def test_ats_profiles_route_trabajando_hiringroom_buk():
    trabajando = match_source_profile({"url_empleo": "https://portal.trabajando.cl/ofertas", "plataforma_empleo": "Trabajando.cl"})
    hiringroom = match_source_profile({"url_empleo": "https://empresa.hiringroom.com/jobs", "plataforma_empleo": "HiringRoom"})
    buk = match_source_profile({"url_empleo": "https://empresa.buk.cl/jobs", "plataforma_empleo": "Buk"})
    assert trabajando.name == "ats_trabajando"
    assert hiringroom.name == "ats_hiringroom"
    assert buk.name == "ats_buk"


def test_pdi_and_carabineros_use_pdf_first_profiles():
    pdi = match_source_profile({"id": 162, "url_empleo": "https://www.pdichile.cl/institucion/concursos-publicos/portada"})
    carabineros = match_source_profile({"id": 161, "url_empleo": "https://postulaciones.carabineros.cl/"})
    assert pdi.name == "pdi_pdf_first"
    assert pdi.supports_pdf_enrichment is True
    assert pdi.extractor_hint == ExtractorKind.SCRAPER_PDF_JOBS
    assert carabineros.name == "carabineros_pdf_first"
    assert carabineros.supports_pdf_enrichment is True
    assert carabineros.extractor_hint == ExtractorKind.SCRAPER_PDF_JOBS


def test_js_required_routes_to_playwright_or_manual_review():
    profile = match_source_profile({"id": 145, "url_empleo": "https://www.bcentral.cl/"})
    selection = select_extractor(
        profile,
        availability=Availability.JS_REQUIRED,
        page_type=PageType.GENERAL_PAGE,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.6,
    )
    assert selection.recommended_extractor == ExtractorKind.SCRAPER_PLAYWRIGHT
    assert selection.decision == Decision.MANUAL_REVIEW


def test_external_ats_selection_is_extractable():
    profile = match_source_profile({"url_empleo": "https://portal.trabajando.cl/ofertas", "plataforma_empleo": "Trabajando.cl"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.ATS_EXTERNAL,
        job_relevance=JobRelevance.JOB_LIKE,
        validity_status=ValidityStatus.OPEN_CONFIRMED,
        confidence=0.9,
    )
    assert selection.recommended_extractor == ExtractorKind.SCRAPER_EXTERNAL_ATS
    assert selection.decision == Decision.EXTRACT


def test_ats_profile_uses_permissive_threshold_for_layout_noise():
    profile = match_source_profile({"url_empleo": "https://empresa.hiringroom.com/jobs", "plataforma_empleo": "HiringRoom"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.ATS_EXTERNAL,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.68,
    )
    assert selection.decision == Decision.EXTRACT
    assert selection.extract_threshold_applied == 0.65
    assert selection.manual_threshold_applied == 0.45


def test_empleos_publicos_not_over_filtered_by_noise():
    profile = match_source_profile({"url_empleo": "https://www.empleospublicos.cl/"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.GENERAL_PAGE,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.5,
    )
    assert selection.recommended_extractor == ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS
    assert selection.decision == Decision.EXTRACT


def test_wordpress_profile_uses_stricter_thresholds():
    profile = match_source_profile({"url_empleo": "https://muni-ejemplo.cl/trabaja-con-nosotros", "plataforma_empleo": "WordPress"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.WORDPRESS_POST,
        job_relevance=JobRelevance.JOB_LIKE,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.75,
    )
    assert selection.decision == Decision.MANUAL_REVIEW
    assert selection.extract_threshold_applied == 0.8
    assert selection.manual_threshold_applied == 0.6


def test_generic_profile_keeps_global_default_thresholds():
    profile = match_source_profile({"url_empleo": "https://www.servicio-no-clasificado.gob.cl/empleos"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.GENERAL_PAGE,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.58,
    )
    assert profile.name == "generic_site"
    assert selection.decision == Decision.MANUAL_REVIEW
    assert selection.extract_threshold_applied == 0.78
    assert selection.manual_threshold_applied == 0.58
    assert selection.threshold_validation["profile_requires_historical_validation"] is True
    assert selection.threshold_validation["historical_validation_applied"] is False


def test_generic_profile_relaxes_thresholds_when_historical_precision_is_high():
    profile = match_source_profile({"url_empleo": "https://www.servicio-no-clasificado.gob.cl/empleos"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.GENERAL_PAGE,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.75,
        source_quality_metrics={"sample_size": 30, "historical_precision": 0.9, "historical_recall": 0.82},
    )
    assert selection.decision == Decision.EXTRACT
    assert selection.extract_threshold_applied == 0.73
    assert selection.manual_threshold_applied == 0.53
    assert selection.threshold_validation["historical_validation_applied"] is True
    assert selection.threshold_validation["historical_quality_band"] == "high_precision_recall"


def test_pdf_first_profile_uses_explicit_thresholds_with_history():
    profile = match_source_profile({"id": 161, "url_empleo": "https://postulaciones.carabineros.cl/"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.DETAIL_PAGE,
        job_relevance=JobRelevance.JOB_LIKE,
        validity_status=ValidityStatus.OPEN_CONFIRMED,
        confidence=0.67,
        source_quality_metrics={"sample_size": 40, "historical_precision": 0.4, "historical_recall": 0.45},
    )
    assert selection.decision == Decision.MANUAL_REVIEW
    assert selection.extract_threshold_applied == 0.75
    assert selection.manual_threshold_applied == 0.55
    assert selection.threshold_validation["historical_quality_band"] == "low_precision_or_recall"


def test_external_ats_family_uses_precision_recall_thresholds():
    profile = match_source_profile({"url_empleo": "https://empresa.hiringroom.com/jobs", "plataforma_empleo": "HiringRoom"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.ATS_EXTERNAL,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.63,
        source_quality_metrics={"sample_size": 20, "historical_precision": 0.88, "historical_recall": 0.8},
    )
    assert selection.decision == Decision.EXTRACT
    assert selection.extract_threshold_applied == 0.62
    assert selection.manual_threshold_applied == 0.42
    assert selection.threshold_validation["threshold_family"] == "external_ats"


def test_waf_profile_has_explicit_thresholds():
    profile = match_source_profile({"url_empleo": "https://ingreso.ejercito.cl/postulaciones"})
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.DETAIL_PAGE,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.7,
    )
    assert profile.name == "ffaa_waf"
    assert selection.decision == Decision.MANUAL_REVIEW
    assert selection.extract_threshold_applied == 0.72
    assert selection.manual_threshold_applied == 0.52
    assert selection.threshold_validation["profile_requires_historical_validation"] is True


def test_runtime_hints_match_ats_before_override():
    match = classify_source_profile(
        {"url_empleo": "https://example.gob.cl/empleos", "plataforma_empleo": "portal custom"},
        runtime_hints=("ats_hiringroom",),
    )
    assert match.profile.name == "ats_hiringroom"
    assert match.matched_by == "runtime"
    assert match.source_requires_override is False


def test_domain_match_has_priority_over_override():
    match = classify_source_profile(
        {"url_empleo": "https://portal.trabajando.cl/ofertas", "plataforma_empleo": "WordPress"},
        runtime_hints=(),
    )
    assert match.profile.name == "ats_trabajando"
    assert match.matched_by == "domain"
    assert match.source_requires_override is False


def test_override_only_after_auto_detection_fails_and_reports_severity():
    match = classify_source_profile(
        {"url_empleo": "https://example.gob.cl/empleos", "plataforma_empleo": "Trabajando.cl"},
        runtime_hints=(),
    )
    assert match.profile.name == "ats_trabajando"
    assert match.matched_by == "override"
    assert match.source_requires_override is True
    assert match.backlog_severity == "high"
