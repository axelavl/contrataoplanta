from __future__ import annotations

from scrapers.evaluation.extractor_router import select_extractor
from scrapers.evaluation.models import (
    Availability,
    Decision,
    ExtractorKind,
    JobRelevance,
    PageType,
    SourceProfile,
    ValidityStatus,
)
from scrapers.evaluation.source_profiles import match_source_profile


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


def test_profile_threshold_overrides_are_applied_in_selection():
    profile = SourceProfile(
        name="custom",
        extractor_hint=ExtractorKind.SCRAPER_GENERIC_FALLBACK,
        extract_threshold=0.85,
        manual_threshold=0.65,
    )
    selection = select_extractor(
        profile,
        availability=Availability.OK,
        page_type=PageType.GENERAL_PAGE,
        job_relevance=JobRelevance.UNCERTAIN,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        confidence=0.70,
    )
    assert selection.decision == Decision.MANUAL_REVIEW
    assert selection.extract_threshold_applied == 0.85
    assert selection.manual_threshold_applied == 0.65
