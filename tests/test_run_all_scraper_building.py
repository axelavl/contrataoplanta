from __future__ import annotations

from types import SimpleNamespace

from scrapers.evaluation.models import Decision, ExtractorKind
from scrapers.evaluation.reason_codes import ReasonCode
from scrapers.run_all import (
    IDS_MUNICIPIOS,
    IDS_UNIVERSIDADES,
    IDS_UNIVERSIDADES_BASE,
    IDS_UNIVERSIDADES_PORTAL,
    IDS_UNIVERSIDADES_TABLA,
    IDS_UNIVERSIDADES_WP,
    RuntimeSource,
    _IDS_NUEVO_ESTANDAR,
    _bypass_evaluation,
    _build_scrapers,
    _enforce_playwright_capability,
)


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
    assignments, run_empleos_publicos = _build_scrapers(runtime_sources)
    names = [type(assignment.scraper).__name__ for assignment in assignments]
    # ats_trabajando y ats_buk fueron migrados a módulos ejecutar(): caen al
    # genérico en el dispatch de clases (en main() se filtran antes de
    # construirse). ats_hiringroom sigue siendo clase dedicada.
    assert names == ["GenericSiteScraper", "HiringRoomScraper", "GenericSiteScraper"]
    assert run_empleos_publicos is False


def test_build_scrapers_maps_pdf_first_and_custom_detail():
    runtime_sources = [
        _runtime_source(inst_id=161, extractor=ExtractorKind.SCRAPER_PDF_JOBS, profile_name="carabineros_pdf_first"),
        _runtime_source(inst_id=162, extractor=ExtractorKind.SCRAPER_PDF_JOBS, profile_name="pdi_pdf_first"),
        _runtime_source(inst_id=157, extractor=ExtractorKind.SCRAPER_CUSTOM_DETAIL, profile_name="ffaa_waf"),
    ]
    assignments, run_empleos_publicos = _build_scrapers(runtime_sources)
    names = [type(assignment.scraper).__name__ for assignment in assignments]
    # carabineros_pdf_first fue migrado a módulo ejecutar(): cae al genérico
    # aquí (en main() se filtra). pdi y ffaa siguen siendo clases dedicadas.
    # carabineros y pdi migrados a módulos ejecutar(): caen al genérico aquí.
    assert names == ["GenericSiteScraper", "GenericSiteScraper", "FfaaScraper"]
    assert run_empleos_publicos is False


def test_build_scrapers_flags_empleos_publicos_for_legacy_batch():
    runtime_sources = [
        _runtime_source(
            inst_id=1,
            extractor=ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS,
            profile_name="empleos_publicos_central",
        ),
        _runtime_source(
            inst_id=2,
            extractor=ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS,
            profile_name="empleos_publicos_central",
        ),
        _runtime_source(
            inst_id=3,
            extractor=ExtractorKind.SCRAPER_EXTERNAL_ATS,
            profile_name="ats_trabajando",
        ),
    ]
    assignments, run_empleos_publicos = _build_scrapers(runtime_sources)
    # Las dos fuentes empleos_publicos no van al despacho async, pero sí
    # deben gatillar la corrida del batch legacy en main().
    assert run_empleos_publicos is True
    names = [type(assignment.scraper).__name__ for assignment in assignments]
    # ats_trabajando migrado a módulo ejecutar(): cae al genérico en el dispatch.
    assert names == ["GenericSiteScraper"]


def test_bypass_dispatches_empleos_publicos_without_http_eval():
    """El portal central (empleospublicos.cl) se reescribió como SPA y geobloquea
    a los runners de GitHub, así que evaluarlo en vivo devuelve availability != OK
    y el gatekeeper NUNCA gatillaba su batch (la fuente #1 quedaba en 0 en silencio).
    Debe despacharse por bypass de kind, sin petición HTTP, con Decision.EXTRACT.
    """
    # Fuente típica del catálogo cuyo url_empleo apunta al portal central.
    source = {
        "id": 5,
        "nombre": "Ministerio de Hacienda",
        "url_empleo": "https://www.empleospublicos.cl",
        "sitio_web": "https://www.hacienda.cl",
        "plataforma_empleo": "empleospublicos.cl",
        "publica_en_empleospublicos": "Sí",
    }
    evaluation = _bypass_evaluation(source)
    assert evaluation is not None, "empleos_publicos debe entrar por bypass de kind"
    assert evaluation.decision == Decision.EXTRACT
    assert evaluation.recommended_extractor == ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS
    assert evaluation.profile_name == "empleos_publicos"

    # End-to-end: una fuente empleos_publicos bypassed debe gatillar el batch legacy.
    runtime_sources = [
        RuntimeSource(institucion=source, fuente_id=None, evaluation=evaluation),
    ]
    _, run_empleos_publicos = _build_scrapers(runtime_sources)
    assert run_empleos_publicos is True


def test_playwright_without_runtime_is_demoted_to_source_status_only(monkeypatch):
    runtime_sources = [
        _runtime_source(inst_id=99, extractor=ExtractorKind.SCRAPER_PLAYWRIGHT, profile_name="js_required_profile"),
    ]

    monkeypatch.setattr(
        "scrapers.run_all._playwright_runtime_available",
        lambda: (False, "missing chromium"),
    )
    _enforce_playwright_capability(runtime_sources)

    evaluation = runtime_sources[0].evaluation
    assert evaluation.decision == Decision.SOURCE_STATUS_ONLY
    assert evaluation.recommended_extractor is None
    assert evaluation.reason_code == ReasonCode.PLAYWRIGHT_RUNTIME_UNAVAILABLE
    assert evaluation.signals_json["playwright_runtime_available"] is False
    assert "missing chromium" in evaluation.signals_json["playwright_runtime_error"]
    assert _build_scrapers(runtime_sources) == ([], False)


def test_grouped_scraper_ids_are_excluded_from_generic_dispatch():
    """Los batches agrupados no deben correr también por GenericSiteScraper."""
    assert IDS_MUNICIPIOS <= _IDS_NUEVO_ESTANDAR
    assert IDS_UNIVERSIDADES <= _IDS_NUEVO_ESTANDAR


def test_universidades_union_matches_sub_batches():
    assert IDS_UNIVERSIDADES == (
        IDS_UNIVERSIDADES_BASE
        | IDS_UNIVERSIDADES_WP
        | IDS_UNIVERSIDADES_PORTAL
        | IDS_UNIVERSIDADES_TABLA
    )
