from __future__ import annotations

from types import SimpleNamespace

from scrapers.evaluation.models import Decision, ExtractorKind
from scrapers.run_all import RuntimeSource, _build_scrapers


def _runtime_source(*, inst_id: int, extractor: ExtractorKind, profile_name: str) -> RuntimeSource:
    return RuntimeSource(
        institucion={
            "id": inst_id,
            "nombre": f"Institucion {inst_id}",
            "url_empleo": f"https://example{inst_id}.cl/empleos",
            "sitio_web": f"https://example{inst_id}.cl",
            "sector": "Municipal",
            "region": "Metropolitana",
        },
        fuente_id=inst_id,
        evaluation=SimpleNamespace(
            decision=Decision.EXTRACT,
            recommended_extractor=extractor,
            profile_name=profile_name,
        ),
    )


def test_build_scrapers_maps_ats_extractors():
    runtime_sources = [
        _runtime_source(inst_id=10, extractor=ExtractorKind.SCRAPER_EXTERNAL_ATS, profile_name="ats_trabajando"),
        _runtime_source(inst_id=11, extractor=ExtractorKind.SCRAPER_EXTERNAL_ATS, profile_name="ats_hiringroom"),
        _runtime_source(inst_id=12, extractor=ExtractorKind.SCRAPER_EXTERNAL_ATS, profile_name="ats_buk"),
    ]
    assignments = _build_scrapers(runtime_sources)
    names = [type(assignment.scraper).__name__ for assignment in assignments]
    assert names == ["TrabajandoCLScraper", "HiringRoomScraper", "BukScraper"]


def test_build_scrapers_maps_pdf_first_and_custom_detail():
    runtime_sources = [
        _runtime_source(inst_id=161, extractor=ExtractorKind.SCRAPER_PDF_JOBS, profile_name="carabineros_pdf_first"),
        _runtime_source(inst_id=162, extractor=ExtractorKind.SCRAPER_PDF_JOBS, profile_name="pdi_pdf_first"),
        _runtime_source(inst_id=157, extractor=ExtractorKind.SCRAPER_CUSTOM_DETAIL, profile_name="ffaa_waf"),
    ]
    assignments = _build_scrapers(runtime_sources)
    names = [type(assignment.scraper).__name__ for assignment in assignments]
    assert names == ["CarabinerosScraper", "PdiScraper", "FfaaScraper"]
