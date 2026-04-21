from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from scrapers.evaluation.models import ExtractorKind
from scrapers.plataformas.buk import BukScraper
from scrapers.plataformas.carabineros import CarabinerosScraper
from scrapers.plataformas.ffaa import FfaaScraper
from scrapers.plataformas.generic_site import GenericSiteScraper
from scrapers.plataformas.hiringroom import HiringRoomScraper
from scrapers.plataformas.pdi import PdiScraper
from scrapers.plataformas.playwright_scraper import PlaywrightScraper
from scrapers.plataformas.trabajando_cl import TrabajandoCLScraper
from scrapers.plataformas.wordpress import WordPressScraper


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
    inst_id = item.institucion.get("id")

    if extractor in {ExtractorKind.SCRAPER_WORDPRESS_JOBS, ExtractorKind.SCRAPER_WORDPRESS_NEWS_FILTER}:
        url_base = (
            str(item.institucion.get("url_empleo") or "").strip()
            or str(item.institucion.get("sitio_web") or "").strip()
        )
        return WordPressScraper(
            fuente_id=item.fuente_id,
            nombre_fuente=str(item.institucion.get("nombre") or item.institucion.get("sigla") or f"wp-{inst_id}"),
            url_base=url_base,
            sector=item.institucion.get("sector"),
            region=item.institucion.get("region"),
        )

    if extractor == ExtractorKind.SCRAPER_EXTERNAL_ATS:
        if profile_name == "ats_trabajando":
            return TrabajandoCLScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        if profile_name == "ats_hiringroom":
            return HiringRoomScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        if profile_name == "ats_buk":
            return BukScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        return GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion)

    if extractor == ExtractorKind.SCRAPER_PDF_JOBS:
        if inst_id == 161:
            return CarabinerosScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        if inst_id == 162:
            return PdiScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        return GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion)

    if extractor == ExtractorKind.SCRAPER_CUSTOM_DETAIL:
        if profile_name == "ffaa_waf" or inst_id in {157, 158}:
            return FfaaScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        if inst_id == 161:
            return CarabinerosScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        if inst_id == 162:
            return PdiScraper(fuente_id=item.fuente_id, institucion=item.institucion)
        return GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion)

    if extractor == ExtractorKind.SCRAPER_PLAYWRIGHT:
        return PlaywrightScraper(fuente_id=item.fuente_id, institucion=item.institucion)

    if extractor == ExtractorKind.SCRAPER_GENERIC_FALLBACK:
        return GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion)

    return None
