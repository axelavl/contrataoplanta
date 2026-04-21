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


def test_runtime_hints_match_ats_before_override():
    match = classify_source_profile(
        {"url_empleo": "https://example.gob.cl/empleos", "plataforma_empleo": "portal custom"},
        runtime_hints=("ats_hiringroom",),
    )
    assert match.profile.name == "ats_hiringroom"
    assert match.matched_by == "runtime"
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
