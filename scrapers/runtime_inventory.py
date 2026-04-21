from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from importlib import import_module
from typing import Any

from scrapers.evaluation.models import ExtractorKind


LEGACY_RETIREMENT_DATE = date(2026, 9, 30)
LEGACY_STATUS_DEPRECATED = "deprecated"
RUNTIME_STATUS_ACTIVE = "active"


@dataclass(frozen=True, slots=True)
class RuntimeModule:
    extractor: ExtractorKind
    profile_name: str
    module: str
    class_name: str
    status: str = RUNTIME_STATUS_ACTIVE


@dataclass(frozen=True, slots=True)
class LegacyModule:
    module: str
    replacement: str
    status: str = LEGACY_STATUS_DEPRECATED
    retirement_date: date = LEGACY_RETIREMENT_DATE


PRODUCTION_RUNTIME_MODULES: tuple[RuntimeModule, ...] = (
    RuntimeModule(ExtractorKind.SCRAPER_WORDPRESS_JOBS, "*", "scrapers.plataformas.wordpress", "WordPressScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_WORDPRESS_NEWS_FILTER, "*", "scrapers.plataformas.wordpress", "WordPressScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_EXTERNAL_ATS, "ats_trabajando", "scrapers.plataformas.trabajando_cl", "TrabajandoCLScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_EXTERNAL_ATS, "ats_hiringroom", "scrapers.plataformas.hiringroom", "HiringRoomScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_EXTERNAL_ATS, "ats_buk", "scrapers.plataformas.buk", "BukScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_EXTERNAL_ATS, "*", "scrapers.plataformas.generic_site", "GenericSiteScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_PDF_JOBS, "carabineros_pdf_first", "scrapers.plataformas.carabineros", "CarabinerosScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_PDF_JOBS, "pdi_pdf_first", "scrapers.plataformas.pdi", "PdiScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_PDF_JOBS, "*", "scrapers.plataformas.generic_site", "GenericSiteScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_CUSTOM_DETAIL, "ffaa_waf", "scrapers.plataformas.ffaa", "FfaaScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_CUSTOM_DETAIL, "*", "scrapers.plataformas.generic_site", "GenericSiteScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_PLAYWRIGHT, "*", "scrapers.plataformas.playwright_scraper", "PlaywrightScraper"),
    RuntimeModule(ExtractorKind.SCRAPER_GENERIC_FALLBACK, "*", "scrapers.plataformas.generic_site", "GenericSiteScraper"),
)


LEGACY_MODULES: tuple[LegacyModule, ...] = (
    LegacyModule("scrapers/banco_central.py", "scrapers.plataformas.generic_site.GenericSiteScraper"),
    LegacyModule("scrapers/codelco.py", "scrapers.plataformas.generic_site.GenericSiteScraper"),
    LegacyModule("scrapers/externouchile.py", "scrapers.plataformas.generic_site.GenericSiteScraper"),
    LegacyModule("scrapers/gobiernos_regionales.py", "scrapers.plataformas.generic_site.GenericSiteScraper"),
    LegacyModule("scrapers/muni_la_florida.py", "scrapers.plataformas.wordpress.WordPressScraper"),
    LegacyModule("scrapers/muni_puente_alto.py", "scrapers.plataformas.wordpress.WordPressScraper"),
    LegacyModule("scrapers/muni_san_bernardo.py", "scrapers.plataformas.wordpress.WordPressScraper"),
    LegacyModule("scrapers/muni_temuco.py", "scrapers.plataformas.wordpress.WordPressScraper"),
    LegacyModule("scrapers/poder_judicial.py", "scrapers.plataformas.generic_site.GenericSiteScraper"),
    LegacyModule("scrapers/trabajando.py", "scrapers.plataformas.trabajando_cl.TrabajandoCLScraper"),
    LegacyModule("scrapers/tvn.py", "scrapers.plataformas.generic_site.GenericSiteScraper"),
)

LEGACY_MODULE_PATHS: frozenset[str] = frozenset(module.module for module in LEGACY_MODULES)


def _find_runtime_module(
    extractor: ExtractorKind | None,
    profile_name: str,
) -> RuntimeModule | None:
    if extractor is None:
        return None

    exact_match: RuntimeModule | None = None
    wildcard_match: RuntimeModule | None = None

    for module in PRODUCTION_RUNTIME_MODULES:
        if module.status != RUNTIME_STATUS_ACTIVE:
            continue
        if module.extractor != extractor:
            continue
        if module.profile_name == profile_name:
            exact_match = module
            break
        if module.profile_name == "*" and wildcard_match is None:
            wildcard_match = module

    return exact_match or wildcard_match


def _load_runtime_class(module: RuntimeModule) -> type[Any]:
    imported = import_module(module.module)
    cls = getattr(imported, module.class_name, None)
    if cls is None:
        raise RuntimeError(f"No se encontró {module.class_name} en {module.module}")
    return cls


def iter_runtime_rows() -> tuple[dict[str, str], ...]:
    """Retorna el inventario runtime en un formato serializable/loggable."""
    rows: list[dict[str, str]] = []
    for module in PRODUCTION_RUNTIME_MODULES:
        rows.append(
            {
                "extractor": module.extractor.value,
                "profile_name": module.profile_name,
                "module": module.module,
                "class_name": module.class_name,
                "status": module.status,
            }
        )
    return tuple(rows)


def iter_legacy_rows() -> tuple[dict[str, str], ...]:
    """Retorna legacy deprecados con fecha de retiro explícita."""
    rows: list[dict[str, str]] = []
    for module in LEGACY_MODULES:
        rows.append(
            {
                "module": module.module,
                "replacement": module.replacement,
                "status": module.status,
                "retirement_date": module.retirement_date.isoformat(),
            }
        )
    return tuple(rows)


def is_legacy_module(module_path: str) -> bool:
    """Indica si un módulo pertenece al set deprecado del runtime."""
    return module_path in LEGACY_MODULE_PATHS


def build_runtime_scraper(item: Any) -> Any | None:
    evaluation = item.evaluation
    extractor = evaluation.recommended_extractor
    profile_name = evaluation.profile_name or ""
    runtime_module = _find_runtime_module(extractor, profile_name)
    if runtime_module is None:
        return None

    scraper_cls = _load_runtime_class(runtime_module)
    inst_id = item.institucion.get("id")
    if runtime_module.class_name == "WordPressScraper":
        url_base = (
            str(item.institucion.get("url_empleo") or "").strip()
            or str(item.institucion.get("sitio_web") or "").strip()
        )
        return scraper_cls(
            fuente_id=item.fuente_id,
            nombre_fuente=str(item.institucion.get("nombre") or item.institucion.get("sigla") or f"wp-{inst_id}"),
            url_base=url_base,
            sector=item.institucion.get("sector"),
            region=item.institucion.get("region"),
        )
    return scraper_cls(fuente_id=item.fuente_id, institucion=item.institucion)
