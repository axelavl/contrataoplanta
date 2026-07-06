# -*- coding: utf-8 -*-
"""
Orquestador principal del scraping guiado por gatekeeper.

Flujo:
    Discovery -> Evaluation/Gatekeeper -> Extractor Router -> Extraction
    -> Post-Extraction Quality -> Persistence -> Audit

Este modulo reemplaza el comportamiento anterior basado en ejecutar
directamente "todo lo conocido". Ahora primero evaluamos cada fuente y
dejamos una decision trazable incluso cuando no se extrae nada.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# Permitir `python scrapers/run_all.py` ademas de `python -m scrapers.run_all`:
# al invocar el script directamente, Python agrega la carpeta del script a
# sys.path en lugar de la raiz del proyecto, por lo que `from scrapers.*`
# falla con ModuleNotFoundError. Anteponemos la raiz para ambos casos.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import psycopg2

from scrapers.base import (
    BaseScraper,
    HttpClient,
    PrecisionReport,
    cerrar_pool,
    conexion,
    generar_reporte,
    get_pool,
    limpiar_vencidas,
    setup_logging,
)
from scrapers.frequency_policy import should_evaluate_now
from scrapers.empleos_publicos import EmpleosPublicosScraper
from scrapers import carabineros as carabineros_scraper
from scrapers import trabajando as trabajando_scraper
from scrapers import buk as buk_scraper
from scrapers import pdi as pdi_scraper
from scrapers import bcentral as bcentral_scraper
from scrapers import armada as armada_scraper
from scrapers import divsal as divsal_scraper
from scrapers import municipios as municipios_scraper
from scrapers import famae as famae_scraper
from scrapers import laborum as laborum_scraper
from scrapers import codelco as codelco_scraper
from scrapers import tribunal_constitucional as tribunal_constitucional_scraper
from scrapers import senado as senado_scraper
from scrapers import poder_judicial as poder_judicial_scraper
from scrapers import fiscalia as fiscalia_scraper
from scrapers import puertos_empresas as puertos_empresas_scraper
from scrapers import linkedin_penalolen as linkedin_penalolen_scraper
from scrapers import aira_integra as aira_integra_scraper
from scrapers import adp as adp_scraper
from scrapers import universidades as universidades_scraper
from scrapers import universidades_wp as universidades_wp_scraper
from scrapers import universidades_portal as universidades_portal_scraper
from scrapers import universidades_tabla as universidades_tabla_scraper
from scrapers.runtime_inventory import (
    build_runtime_scraper,
    iter_legacy_rows,
    iter_runtime_rows,
)
from scrapers.evaluation.audit_store import AuditStore
from scrapers.evaluation.catalog_loader import CatalogLoader
from scrapers.evaluation.models import (
    Availability, Decision, ExtractorKind,
    JobRelevance, OpenCallsStatus, PageType, EvaluationResult,
    RetryPolicy, ValidityStatus,
)
from scrapers.evaluation.reason_codes import ReasonCode, reason_detail
from scrapers.evaluation.source_evaluator import SourceEvaluator
from scrapers.source_status import ScraperKind, classify_source


log = setup_logging("run_all")
MAX_EVALUATIONS_CONCURRENT = 8
MAX_SCRAPERS_CONCURRENT = 6

SUPPORTED_RUNTIME_EXTRACTORS = {
    ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS,
    ExtractorKind.SCRAPER_WORDPRESS_JOBS,
    ExtractorKind.SCRAPER_WORDPRESS_NEWS_FILTER,
    ExtractorKind.SCRAPER_EXTERNAL_ATS,
    ExtractorKind.SCRAPER_PDF_JOBS,
    ExtractorKind.SCRAPER_CUSTOM_DETAIL,
    ExtractorKind.SCRAPER_GENERIC_FALLBACK,
    ExtractorKind.SCRAPER_PLAYWRIGHT,
}

# Mapa de ScraperKind (override) → (ExtractorKind, profile_name) para bypass del gatekeeper.
# Cuando classify_source (source_overrides.json o clasificación automática) declara uno de
# estos kinds, se omite la evaluación HTTP (que puede fallar por WAF, JS-render, geobloqueo
# por IP, etc.) y se despacha directamente con Decision.EXTRACT.
#
# empleos_publicos va acá por una razón crítica: el portal se reescribió (jun-2026) como una
# SPA que responde un shell JS sin markup de ofertas, y además geobloquea por IP a los runners
# de GitHub (datacenter EE.UU.). El gatekeeper, al evaluar https://www.empleospublicos.cl en
# vivo, obtenía availability != OK → Decision.SOURCE_STATUS_ONLY → el batch legacy del portal
# central (la fuente #1 del sitio) NUNCA se despachaba y quedaba en 0 ofertas en silencio. El
# scraper ya no lee el HTML del portal: consume la API JSON apiConvocatorias.ashx (ver
# empleos_publicos.py), así que su despacho no debe depender de que la home SPA sea evaluable.
_KIND_BYPASS: dict[str, tuple[ExtractorKind, str]] = {
    ScraperKind.CUSTOM_TRABAJANDO.value: (ExtractorKind.SCRAPER_EXTERNAL_ATS, "ats_trabajando"),
    ScraperKind.CUSTOM_HIRINGROOM.value:  (ExtractorKind.SCRAPER_EXTERNAL_ATS,  "ats_hiringroom"),
    ScraperKind.CUSTOM_BUK.value:         (ExtractorKind.SCRAPER_EXTERNAL_ATS,  "ats_buk"),
    ScraperKind.CUSTOM_FFAA.value:        (ExtractorKind.SCRAPER_CUSTOM_DETAIL, "ffaa_waf"),
    ScraperKind.EMPLEOS_PUBLICOS.value:   (ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS, "empleos_publicos"),
}
# custom_policia depende del ID: 161=Carabineros, 162=PDI — se resuelve en _bypass_evaluation.
_POLICIA_PROFILES = {
    161: (ExtractorKind.SCRAPER_PDF_JOBS, "carabineros_pdf_first"),
    162: (ExtractorKind.SCRAPER_PDF_JOBS, "pdi_pdf_first"),
}

# Perfiles migrados al "nuevo estándar": módulos auto-contenidos con ejecutar()
# en scrapers/carabineros.py y scrapers/trabajando.py. Las fuentes con estos
# perfiles se excluyen del dispatch de CLASES en main() para que NO corran dos
# veces; las atienden los batches sync _run_modulo_ejecutar_sync.
_PERFILES_NUEVO_ESTANDAR: frozenset[str] = frozenset({"carabineros_pdf_first", "ats_trabajando", "ats_buk", "pdi_pdf_first"})
# Fuentes migradas que NO tienen un perfil propio (el gatekeeper las trata como
# genéricas): se excluyen del dispatch de clases por id de institución para que
# generic_site no las scrapee en paralelo. Banco Central (145) = bcentral.py;
# Armada (158) = armada.py (excluida por id, NO por perfil, porque comparte el
# perfil ffaa_waf con Ejercito (157), que sigue usando el FfaaScraper generico).
# DIVSAL (706) = divsal.py: institución FF.AA. nueva, scraper dedicado vía API REST;
# excluida por id para que no la tome el FfaaScraper genérico ni se superponga.
# + Fiscalía (146, fiscalia.py, sale de trabajando) y puertos/empresas
# (166 ENAER, 285 EPI, 290 Talcahuano, 291 Pto Montt, 292 Chacabuco,
#  293 Austral, 708 FOJI, 709 PRODEMU) por puertos_empresas.py.
_IDS_NUEVO_ESTANDAR: frozenset[int] = frozenset({
    145, 158, 706, 165, 707, 279, 280, 275,
    146, 166, 285, 290, 291, 292, 293, 708, 709,
    132,  # Fundación Integra (aira_integra.py + trabajando.py _EXTRA)
    135, 138,  # Senado (senado.py) y Poder Judicial (poder_judicial.py)
    # municipios.py (scraper agrupado, un modo por sitio): municipalidades y
    # corporaciones con extracción funcional. Las fuentes spa/bloqueada quedan
    # al genérico (no scrapean nada por ahora). Ver scrapers/municipios.py.
    345, 363, 382, 384, 385, 387, 388, 401, 407, 409, 416, 419, 456, 527, 537, 580, 647, 670, 676,
    # universidades.py + universidades_wp.py + universidades_portal.py +
    # universidades_tabla.py (scrapers agrupados de universidades)
    # 254 (UTEM) → trabajando.py; 258/259 (UOH/UAysén) → universidades_wp.py
    # 251 (UNAP) / 246 (UFRO) → universidades_portal.py
    # 243 (USACH) / 244 (UV) / 252 (UTA) → universidades_tabla.py
    # 249 (UMAG) / 254 (UTEM) / 257 (UTalca) → trabajando.py (auto-descubiertos)
    243, 244, 245, 246, 248, 249, 251, 252, 253, 254, 255, 257, 258, 259,
})

#: Fuentes atendidas por el scraper agrupado municipios.py. A nivel de módulo
#: para que el panel (api/routers/admin.py, mode="municipios") pueda armar el
#: `--ids` sin duplicar la lista. El gate del batch (más abajo) usa este set.
IDS_MUNICIPIOS: frozenset[int] = frozenset({
    345, 363, 382, 384, 385, 388, 398, 401, 407, 409, 416, 419,
    456, 527, 537, 580, 647, 670, 676,
})

#: Universidades (unión de los 4 batches: universidades.py, _wp, _portal,
#: _tabla). El panel usa la unión (mode="universidades"); los gates de cada
#: sub-batch conservan su subconjunto propio más abajo.
IDS_UNIVERSIDADES: frozenset[int] = frozenset({
    243, 244, 245, 246, 248, 251, 252, 253, 255, 258, 259,
})

#: Puertos y empresas del Estado (puertos_empresas.py): ENAER, EPI, puertos
#: de Talcahuano/Pto. Montt/Chacabuco/Austral, FOJI, PRODEMU.
IDS_PUERTOS_EMPRESAS: frozenset[int] = frozenset({
    166, 285, 290, 291, 292, 293, 708, 709,
})


@lru_cache(maxsize=1)
def _playwright_runtime_available() -> tuple[bool, str | None]:
    """Verifica si Playwright puede lanzar Chromium realmente en este runtime."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return False, f"Playwright import failed: {exc.__class__.__name__}: {exc}"

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            browser.close()
        return True, None
    except Exception as exc:
        return False, f"Playwright launch failed: {exc.__class__.__name__}: {exc}"


def _enforce_playwright_capability(runtime_sources: list[RuntimeSource]) -> None:
    available, error_detail = _playwright_runtime_available()
    if available:
        return

    for item in runtime_sources:
        evaluation = item.evaluation
        if evaluation.recommended_extractor != ExtractorKind.SCRAPER_PLAYWRIGHT:
            continue
        evaluation.decision = Decision.SOURCE_STATUS_ONLY
        evaluation.recommended_extractor = None
        evaluation.reason_code = ReasonCode.PLAYWRIGHT_RUNTIME_UNAVAILABLE
        evaluation.reason_detail = reason_detail(
            ReasonCode.PLAYWRIGHT_RUNTIME_UNAVAILABLE,
            fallback="Playwright runtime no disponible para rendering JS.",
        )
        evaluation.retry_policy = RetryPolicy.HIGH
        with suppress(AttributeError):
            signals = dict(getattr(evaluation, "signals_json", None) or {})
            if error_detail:
                signals["playwright_runtime_error"] = error_detail[:400]
            signals["playwright_runtime_available"] = False
            evaluation.signals_json = signals
        log.warning(
            "Fuente %s (id=%s) requiere SCRAPER_PLAYWRIGHT, pero runtime no disponible. Se marca SOURCE_STATUS_ONLY.",
            item.institucion.get("nombre", "?"),
            item.institucion.get("id"),
        )


def _bypass_evaluation(source: dict[str, Any]) -> EvaluationResult | None:
    """
    Si source_overrides.json declara un kind personalizado conocido, devuelve un
    EvaluationResult forzado con Decision.EXTRACT sin realizar petición HTTP.
    Retorna None si no aplica bypass (el gatekeeper debe evaluarse normalmente).
    """
    try:
        decision = classify_source(source)
    except Exception:
        return None

    kind_val = decision.kind.value if decision.kind else ""
    source_url = str(source.get("url_empleo") or source.get("sitio_web") or "")

    # Sólo aplicar bypass si la fuente está explícitamente en source_overrides.json
    # (status=active) y tiene un kind de bypass conocido.
    if decision.status.value not in ("active",):
        return None

    # Resolver extractor y profile según el kind
    if kind_val == ScraperKind.CUSTOM_POLICIA.value:
        inst_id = source.get("id")
        pair = _POLICIA_PROFILES.get(inst_id)
        if pair is None:
            return None
        extractor, profile_name = pair
    elif kind_val in _KIND_BYPASS:
        extractor, profile_name = _KIND_BYPASS[kind_val]
    else:
        return None

    log.info(
        "Bypass gatekeeper para %s (id=%s, kind=%s) → %s / %s",
        source.get("nombre", "?"), source.get("id"), kind_val, extractor.value, profile_name,
    )
    return EvaluationResult(
        source_url=source_url,
        availability=Availability.OK,
        http_status=200,
        page_type=PageType.LISTING_PAGE,
        job_relevance=JobRelevance.JOB_LIKE,
        open_calls_status=OpenCallsStatus.UNKNOWN_STATUS,
        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
        recommended_extractor=extractor,
        decision=Decision.EXTRACT,
        reason_code=None,
        reason_detail=f"Bypass por kind={kind_val} en source_overrides.json",
        confidence=1.0,
        retry_policy=RetryPolicy.HIGH,
        signals_json={"bypass": True, "kind": kind_val},
        evaluated_at=datetime.now(tz=timezone.utc),
        profile_name=profile_name,
    )


@dataclass(slots=True)
class RuntimeSource:
    institucion: dict[str, Any]
    fuente_id: int | None
    evaluation: Any


@dataclass(slots=True)
class CooldownDecision:
    source: dict[str, Any]
    reevaluate: bool
    reason: str


@dataclass(slots=True)
class RuntimeScraperAssignment:
    institucion_id: int | None
    scraper: BaseScraper


@dataclass(slots=True)
class LastEvaluationState:
    retry_policy: str | None
    evaluated_at: datetime | None
    consecutive_zero_extractions: int
    zero_cooldown_bypass_used: bool


def _host(url: str | None) -> str:
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = parsed.netloc.lower()
    return host[4:] if host.startswith("www.") else host


def _load_fuentes_index() -> list[dict[str, Any]]:
    try:
        with conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, nombre, url_base, tipo_plataforma
                    FROM fuentes
                    WHERE activa = TRUE
                    ORDER BY id
                    """
                )
                rows = cur.fetchall()
    except Exception as exc:
        log.warning("No se pudo cargar la tabla fuentes: %s", exc)
        return []
    return [
        {
            "id": row[0],
            "nombre": row[1],
            "url_base": row[2],
            "tipo_plataforma": row[3],
        }
        for row in rows
    ]


def _resolve_fuente_id(institucion: dict[str, Any], evaluation: Any, fuentes_index: list[dict[str, Any]]) -> int | None:
    if evaluation.recommended_extractor == ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS:
        for item in fuentes_index:
            if (item.get("tipo_plataforma") or "").lower() == "empleospublicos":
                return item["id"]
        return None  # fuentes table not yet populated

    target_hosts = {
        _host(institucion.get("url_empleo")),
        _host(institucion.get("sitio_web")),
    }
    target_hosts.discard("")
    for item in fuentes_index:
        if _host(item.get("url_base")) in target_hosts:
            return item["id"]

    nombre = str(institucion.get("nombre") or "").strip().lower()
    for item in fuentes_index:
        if str(item.get("nombre") or "").strip().lower() == nombre:
            return item["id"]
    return None


def _build_discovery_catalog(
    loader: CatalogLoader,
    *,
    limit: int | None = None,
    skip_empleos_publicos: bool = False,
    only_kind: str | None = None,
) -> list[dict[str, Any]]:
    bundle = loader.load(prefer_json=True)
    items = [
        inst
        for inst in bundle.instituciones
        if inst.get("url_empleo") or inst.get("sitio_web")
    ]
    if skip_empleos_publicos:
        items = [
            inst for inst in items
            if "empleospublicos.cl" not in str(inst.get("url_empleo", ""))
        ]
    if only_kind:
        # Filtrar por ScraperKind clasificado ANTES de aplicar --limit, para que
        # `--only-kind wordpress --limit 10` devuelva 10 wordpress y no 10 del
        # catálogo completo de los que casi ninguno sea wordpress.
        def _kind_de(inst: dict[str, Any]) -> str:
            try:
                return classify_source(inst).kind.value
            except Exception:
                return ""
        items = [inst for inst in items if _kind_de(inst) == only_kind]
    return items[:limit] if limit else items


def _load_last_evaluations() -> dict[int, LastEvaluationState]:
    """Carga el último estado incremental por institucion_id desde source_evaluations.

    Returns:
        Dict ``{institucion_id: LastEvaluationState}``.
    """
    result: dict[int, LastEvaluationState] = {}
    try:
        with conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (institucion_id)
                        institucion_id,
                        retry_policy,
                        evaluated_at,
                        COALESCE((signals_json->>'consecutive_zero_extractions')::int, 0) AS consecutive_zero_extractions,
                        COALESCE((signals_json->>'zero_cooldown_bypass_used')::boolean, FALSE) AS zero_cooldown_bypass_used
                    FROM source_evaluations
                    WHERE institucion_id IS NOT NULL
                    ORDER BY institucion_id, evaluated_at DESC
                    """
                )
                for inst_id, retry_policy, evaluated_at, zero_count, bypass_used in cur.fetchall():
                    result[inst_id] = LastEvaluationState(
                        retry_policy=retry_policy,
                        evaluated_at=evaluated_at,
                        consecutive_zero_extractions=int(zero_count or 0),
                        zero_cooldown_bypass_used=bool(bypass_used),
                    )
    except Exception as exc:
        log.warning("No se pudo cargar source_evaluations para cooldown: %s", exc)
    return result


def _partition_by_cooldown(
    sources: list[dict[str, Any]],
    last_evaluations: dict[int, LastEvaluationState],
    *,
    now: datetime | None = None,
) -> tuple[list[dict[str, Any]], list[CooldownDecision]]:
    """Separa fuentes en (due, in_cooldown).

    ``due`` son las que deben evaluarse ahora.
    ``in_cooldown`` son las que aún no han cumplido su cooldown desde la
    última evaluación y se saltan en esta corrida.
    """
    COOLDOWN_BYPASS_ZERO_THRESHOLD = 3
    due: list[dict[str, Any]] = []
    decisions: list[CooldownDecision] = []
    _now = now or datetime.now(tz=timezone.utc)
    for source in sources:
        inst_id = source.get("id")
        if inst_id is None:
            due.append(source)
            decisions.append(CooldownDecision(source=source, reevaluate=True, reason="sin_institucion_id"))
            continue
        entry = last_evaluations.get(int(inst_id))
        if entry is None:
            due.append(source)
            decisions.append(CooldownDecision(source=source, reevaluate=True, reason="sin_historial"))
            continue
        if should_evaluate_now(retry_policy=entry.retry_policy, last_evaluated_at=entry.evaluated_at, now=_now):
            due.append(source)
            decisions.append(CooldownDecision(source=source, reevaluate=True, reason="cooldown_vencido"))
        else:
            if entry.consecutive_zero_extractions > COOLDOWN_BYPASS_ZERO_THRESHOLD and not entry.zero_cooldown_bypass_used:
                due.append(source)
                decisions.append(CooldownDecision(source=source, reevaluate=True, reason="cooldown_bypass_one_time_zero_threshold"))
            elif (entry.retry_policy or "").lower() in {"critical", "high"}:
                due.append(source)
                decisions.append(CooldownDecision(source=source, reevaluate=True, reason="cooldown_light_recheck_high_priority"))
            elif entry.consecutive_zero_extractions > 0:
                due.append(source)
                decisions.append(CooldownDecision(source=source, reevaluate=True, reason="cooldown_light_recheck_zero_streak"))
            else:
                decisions.append(CooldownDecision(source=source, reevaluate=False, reason="cooldown"))
    return due, decisions


async def _evaluate_sources(
    sources: list[dict[str, Any]],
    *,
    audit_store: AuditStore,
    fuentes_index: list[dict[str, Any]],
) -> list[RuntimeSource]:
    runtime_sources: list[RuntimeSource] = []
    sem = asyncio.Semaphore(MAX_EVALUATIONS_CONCURRENT)
    async with HttpClient() as http:
        evaluator = SourceEvaluator(http)

        async def _evaluate_one(source: dict[str, Any]) -> RuntimeSource:
            async with sem:
                source_id = _resolve_fuente_id(source, type("EmptyEval", (), {"recommended_extractor": None})(), fuentes_index)
                # Bypass del gatekeeper para kinds con override explícito en source_overrides.json
                bypass = _bypass_evaluation(source)
                if bypass is not None:
                    fuente_id = _resolve_fuente_id(source, bypass, fuentes_index)
                    return RuntimeSource(institucion=source, fuente_id=fuente_id, evaluation=bypass)
                historical_noise_ratio = 0.0
                historical_source_metrics: dict[str, float | int] | None = None
                try:
                    with conexion() as conn:
                        historical_noise_ratio = audit_store.get_institution_noise_ratio(conn, source.get("id"))
                        historical_source_metrics = audit_store.get_source_quality_metrics(conn, source_id)
                except Exception:
                    historical_noise_ratio = 0.0
                    historical_source_metrics = None
                evaluation = await evaluator.evaluate(
                    source,
                    historical_noise_ratio=historical_noise_ratio,
                    historical_source_metrics=historical_source_metrics,
                )
                fuente_id = _resolve_fuente_id(source, evaluation, fuentes_index)
                return RuntimeSource(institucion=source, fuente_id=fuente_id, evaluation=evaluation)

        async def _evaluate_one_safe(source: dict[str, Any]) -> RuntimeSource:
            try:
                return await asyncio.wait_for(_evaluate_one(source), timeout=45)
            except (asyncio.TimeoutError, Exception) as e:
                log.debug("Evaluación abortada para %s: %s", source.get("nombre", "?"), e)
                from scrapers.evaluation.models import (
                    Availability, PageType, JobRelevance, OpenCallsStatus,
                    ValidityStatus, Decision, RetryPolicy,
                )
                from scrapers.evaluation.source_evaluator import SourceEvaluator as _SE
                fallback = _SE._make_result(  # type: ignore[attr-defined]
                    source_url=source.get("url_empleo", ""),
                    availability=Availability.UNREACHABLE,
                    http_status=0,
                    page_type=PageType.UNKNOWN,
                    job_relevance=JobRelevance.UNKNOWN,
                    open_calls_status=OpenCallsStatus.UNKNOWN,
                    validity_status=ValidityStatus.UNKNOWN,
                    decision=Decision.SKIP,
                    confidence=0.0,
                    retry_policy=RetryPolicy.PAUSE,
                ) if False else None
                if fallback is None:
                    from scrapers.evaluation.models import EvaluationResult
                    from datetime import timezone
                    fallback = EvaluationResult(
                        source_url=source.get("url_empleo", ""),
                        availability=Availability.TIMEOUT,
                        http_status=0,
                        page_type=PageType.UNKNOWN_PAGE_TYPE,
                        job_relevance=JobRelevance.UNCERTAIN,
                        open_calls_status=OpenCallsStatus.UNKNOWN_STATUS,
                        validity_status=ValidityStatus.UNKNOWN_VALIDITY,
                        recommended_extractor=None,
                        decision=Decision.SKIP,
                        reason_code=None,
                        reason_detail=f"Timeout o error: {str(e)[:120]}",
                        confidence=0.0,
                        retry_policy=RetryPolicy.EVENTUAL,
                        signals_json={},
                        evaluated_at=datetime.now(tz=timezone.utc),
                    )
                return RuntimeSource(institucion=source, fuente_id=None, evaluation=fallback)

        runtime_sources = await asyncio.gather(*[_evaluate_one_safe(source) for source in sources])

    return list(runtime_sources)


def _build_scrapers(
    runtime_sources: list[RuntimeSource],
) -> tuple[list[RuntimeScraperAssignment], bool]:
    """Construye assignments runtime y reporta si hay que correr el batch
    legacy de empleospublicos.cl.

    EmpleosPublicosScraper no implementa el contrato async ``descubrir_ofertas``
    de BaseScraper — usa la API síncrona ``LegacyBaseScraper.run()`` y barre
    el portal central completo en una sola corrida. Por eso se omite del
    despacho concurrente y se ejecuta aparte (ver ``_run_empleos_publicos_sync``).
    """
    assignments: list[RuntimeScraperAssignment] = []
    empleos_publicos_pendiente = False

    for item in runtime_sources:
        evaluation = item.evaluation
        if evaluation.decision != Decision.EXTRACT:
            continue
        if evaluation.recommended_extractor not in SUPPORTED_RUNTIME_EXTRACTORS:
            continue
        # fuente_id puede ser None si la tabla fuentes no está poblada —
        # los scrapers aceptan None y la columna ofertas.fuente_id es nullable.

        if evaluation.recommended_extractor == ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS:
            if not empleos_publicos_pendiente:
                log.info(
                    "SCRAPER_EMPLEOS_PUBLICOS detectado — se ejecutará en batch legacy aparte del despacho async."
                )
                empleos_publicos_pendiente = True
            continue

        runtime_scraper = build_runtime_scraper(item)
        if runtime_scraper is not None:
            assignments.append(
                RuntimeScraperAssignment(
                    institucion_id=item.institucion.get("id"),
                    scraper=runtime_scraper,
                )
            )
    return assignments, empleos_publicos_pendiente


def _alertar_portal_central_caido(found: int) -> None:
    """Si el portal central (empleospublicos.cl) devuelve 0 ofertas, avisa por
    email al administrador. Aplica cooldown (env ``ALERT_COOLDOWN_H``, default 6h)
    para no spamear cada 2h mientras dura la caída. Falla en silencio: una alerta
    nunca debe romper la corrida del scraper."""
    if found and found > 0:
        return
    try:
        import os

        cooldown_h = float(os.getenv("ALERT_COOLDOWN_H", "6"))
        log_dir = Path(os.getenv("LOG_DIR", "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        marker = log_dir / ".alerta_empleospublicos_cero"
        if marker.exists() and (time.time() - marker.stat().st_mtime) < cooldown_h * 3600:
            log.warning(
                "Portal central con 0 ofertas; alerta en cooldown (<%sh), no se reenvía.",
                cooldown_h,
            )
            return

        from api.services.email_alerts import enviar_alerta_operacional

        mensaje = (
            "EmpleosPublicosScraper (empleospublicos.cl) devolvió 0 ofertas en esta "
            "corrida. Es la fuente principal del sitio: 0 ofertas suele indicar que "
            "la API o el markup del portal cambió, o que la fuente está caída.\n\n"
            "Revisar el endpoint apiConvocatorias.ashx y el log de la corrida en Railway."
        )
        if enviar_alerta_operacional("Portal central sin ofertas (0)", mensaje).get("ok"):
            marker.touch()
    except Exception as exc:  # nunca romper el run por una alerta
        log.warning("No se pudo enviar alerta de portal central caído: %s", exc)


def _run_empleos_publicos_sync(
    instituciones: list[dict[str, Any]],
) -> PrecisionReport:
    """Corre el batch legacy del portal central empleospublicos.cl y traduce
    sus stats a un PrecisionReport para que se integre al reporte global.
    """
    log.info("Ejecutando EmpleosPublicosScraper (batch legacy del portal central)...")
    scraper = EmpleosPublicosScraper(instituciones=instituciones, dry_run=False)
    report = PrecisionReport(institucion="empleospublicos.cl")
    try:
        stats = scraper.run() or {}
    except Exception as exc:
        log.exception("EmpleosPublicosScraper falló: %s", exc)
        report.errores += 1
        return report
    report.total_encontradas = int(stats.get("found") or 0)
    report.guardadas = int(stats.get("nuevas") or 0)
    report.ya_existian = int(stats.get("actualizadas") or 0)
    report.errores = int(stats.get("errores") or 0)
    log.info(
        "EmpleosPublicosScraper terminó: found=%s nuevas=%s actualizadas=%s cerradas=%s errores=%s duracion=%ss",
        stats.get("found"),
        stats.get("nuevas"),
        stats.get("actualizadas"),
        stats.get("cerradas"),
        stats.get("errores"),
        stats.get("duracion_seg"),
    )
    _alertar_portal_central_caido(report.total_encontradas)
    return report


def _run_modulo_ejecutar_sync(nombre: str, ejecutar_fn: Any, **extra_kwargs: Any) -> PrecisionReport:
    """Corre un scraper del 'nuevo estándar' (módulo auto-contenido con
    ``ejecutar()`` que descubre sus propias fuentes y persiste vía db.database)
    y traduce sus stats a un PrecisionReport para integrarlo al reporte global.

    ``extra_kwargs`` se reenvían a ``ejecutar()`` (p. ej. ``allow_ocr=True``
    para Carabineros); los módulos que no los acepten no deben recibirlos.
    """
    log.info("Ejecutando %s (módulo ejecutar() nuevo estándar)...", nombre)
    report = PrecisionReport(institucion=nombre)
    try:
        stats = ejecutar_fn(dry_run=False, **extra_kwargs) or {}
    except Exception as exc:
        log.exception("Módulo %s falló: %s", nombre, exc)
        report.errores += 1
        return report
    report.total_encontradas = int(stats.get("encontradas") or 0)
    report.guardadas = int(stats.get("nuevas") or 0)
    report.ya_existian = int(stats.get("actualizadas") or 0)
    report.errores = int(stats.get("errores") or 0)
    log.info(
        "%s terminó: encontradas=%s nuevas=%s actualizadas=%s cerradas=%s errores=%s",
        nombre,
        stats.get("encontradas"),
        stats.get("nuevas"),
        stats.get("actualizadas"),
        stats.get("cerradas"),
        stats.get("errores"),
    )
    return report


async def _run_scraper(assignment: RuntimeScraperAssignment, sem: asyncio.Semaphore) -> PrecisionReport:
    async with sem:
        scraper = assignment.scraper
        try:
            async with scraper:
                return await scraper.run()
        except Exception as exc:
            log.exception(
                "Scraper %s fallo para institucion_id=%s: %s",
                scraper.nombre_fuente,
                assignment.institucion_id,
                exc,
            )
            report = scraper.report
            report.errores += 1
            return report


async def _run_scrapers(assignments: list[RuntimeScraperAssignment]) -> list[tuple[int | None, PrecisionReport]]:
    if not assignments:
        return []
    sem = asyncio.Semaphore(MAX_SCRAPERS_CONCURRENT)
    tasks = [_run_scraper(assignment, sem) for assignment in assignments]
    reports = await asyncio.gather(*tasks)
    return [(assignment.institucion_id, report) for assignment, report in zip(assignments, reports)]


def _compute_consecutive_zero_extractions(
    *,
    runtime_sources: list[RuntimeSource],
    report_rows: list[tuple[int | None, PrecisionReport]],
    previous_states: dict[int, LastEvaluationState],
) -> dict[int, int]:
    report_by_inst: dict[int, PrecisionReport] = {
        int(inst_id): report
        for inst_id, report in report_rows
        if inst_id is not None
    }
    updated: dict[int, int] = {}
    for item in runtime_sources:
        inst_id = item.institucion.get("id")
        if inst_id is None:
            continue
        inst_id = int(inst_id)
        prev = previous_states.get(inst_id)
        prev_count = prev.consecutive_zero_extractions if prev else 0
        if item.evaluation.decision != Decision.EXTRACT:
            updated[inst_id] = prev_count
            continue
        report = report_by_inst.get(inst_id)
        if report is None:
            updated[inst_id] = prev_count
            continue
        updated[inst_id] = prev_count + 1 if report.total_encontradas == 0 else 0
    return updated


def _persist_source_evaluations(
    *,
    runtime_sources: list[RuntimeSource],
    audit_store: AuditStore,
    consecutive_zero_by_inst: dict[int, int],
    cooldown_reason_by_inst: dict[int, str],
    previous_states: dict[int, LastEvaluationState],
) -> None:
    import os
    saved = 0
    errors = 0
    try:
        db_url = os.environ.get("DATABASE_URL") or (
            f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
            f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', 5432)}/{os.environ['DB_NAME']}"
        )
        conn_direct = psycopg2.connect(db_url)
        try:
            for item in runtime_sources:
                try:
                    inst_id_raw = item.institucion.get("id")
                    inst_id = int(inst_id_raw) if inst_id_raw is not None else None
                    prev_state = previous_states.get(inst_id) if inst_id is not None else None
                    zero_count = consecutive_zero_by_inst.get(
                        inst_id,
                        prev_state.consecutive_zero_extractions if prev_state else 0,
                    ) if inst_id is not None else 0
                    bypass_used = prev_state.zero_cooldown_bypass_used if prev_state else False
                    if cooldown_reason_by_inst.get(inst_id) == "cooldown_bypass_one_time_zero_threshold":
                        bypass_used = True
                    if zero_count == 0:
                        bypass_used = False
                    item.evaluation.signals_json = {
                        **(item.evaluation.signals_json or {}),
                        "consecutive_zero_extractions": zero_count,
                        "zero_cooldown_bypass_used": bypass_used,
                    }
                    if cooldown_reason_by_inst.get(inst_id):
                        item.evaluation.signals_json["incremental_cooldown_reason"] = cooldown_reason_by_inst[inst_id]

                    audit_store.save_source_evaluation(
                        conn_direct,
                        source_id=item.fuente_id,
                        institucion_id=inst_id,
                        evaluation=item.evaluation,
                    )
                    if item.fuente_id is None and item.evaluation.decision == Decision.EXTRACT:
                        audit_store.save_catalog_event(
                            conn_direct,
                            institucion_id=inst_id,
                            event_type="missing_runtime_source",
                            detail="La fuente fue evaluada como extraible, pero no existe mapeo fuente_id en el runtime actual.",
                            payload=item.evaluation.to_record(),
                        )
                    if item.evaluation.signals_json.get("source_requires_override"):
                        severity = item.evaluation.signals_json.get("override_backlog_severity") or "medium"
                        audit_store.save_catalog_event(
                            conn_direct,
                            institucion_id=inst_id,
                            event_type="source_requires_override",
                            detail=f"La fuente requiere override para clasificarse en runtime (severity={severity}).",
                            payload=item.evaluation.to_record(),
                        )
                    saved += 1
                except Exception as e:
                    errors += 1
                    log.debug("Error guardando evaluación de %s: %s", item.institucion.get("nombre", "?"), e)
            conn_direct.commit()
        finally:
            conn_direct.close()
    except Exception as e:
        log.warning("Error persistiendo evaluaciones al batch: %s", e)
    log.info("Evaluaciones persistidas: %d OK, %d errores", saved, errors)


def persistir_corrida(
    *,
    evaluations: list[RuntimeSource],
    reports: list[PrecisionReport],
    duration_seconds: float,
    vencidas_cerradas: int,
) -> None:
    total_encontradas = sum(report.total_encontradas for report in reports)
    total_nuevas = sum(report.guardadas for report in reports)
    total_actualizadas = sum(report.ya_existian for report in reports)
    total_descartadas = sum(
        report.descartadas_negativas + report.descartadas_sin_keywords + report.descartadas_vencidas
        for report in reports
    )
    total_errores = sum(report.errores for report in reports)
    reason_codes_counter: Counter[str] = Counter()
    for report in reports:
        reason_codes_counter.update(report.descartes_por_reason_code)
    distribucion_descartes_reason_code = dict(
        sorted(reason_codes_counter.items(), key=lambda item: (-item[1], item[0]))
    )
    top_reason_codes_por_fuente = {
        report.institucion: [
            {"reason_code": reason_code, "count": count}
            for reason_code, count in report.top_reason_codes()
        ]
        for report in reports
        if report.descartes_por_reason_code
    }
    detail = {
        "reports": {report.institucion: report.to_dict() for report in reports},
        "distribucion_descartes_reason_code": distribucion_descartes_reason_code,
        "top_reason_codes_por_fuente": top_reason_codes_por_fuente,
        "evaluations": {
            "total": len(evaluations),
            "extract": sum(1 for item in evaluations if item.evaluation.decision == Decision.EXTRACT),
            "skip": sum(1 for item in evaluations if item.evaluation.decision == Decision.SKIP),
            "manual_review": sum(1 for item in evaluations if item.evaluation.decision == Decision.MANUAL_REVIEW),
            "source_status_only": sum(1 for item in evaluations if item.evaluation.decision == Decision.SOURCE_STATUS_ONLY),
        },
    }
    tasa = (
        (total_nuevas + total_actualizadas) / total_encontradas * 100.0
        if total_encontradas else 0.0
    )

    try:
        with conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scraper_runs (
                        duracion_segundos,
                        total_instituciones,
                        total_encontradas,
                        total_nuevas,
                        total_actualizadas,
                        total_vencidas,
                        total_descartadas,
                        total_errores,
                        tasa_precision,
                        detalle
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        int(duration_seconds),
                        len(reports),
                        total_encontradas,
                        total_nuevas,
                        total_actualizadas,
                        vencidas_cerradas,
                        total_descartadas,
                        total_errores,
                        round(tasa, 2),
                        json.dumps(detail, ensure_ascii=False),
                    ),
                )
            conn.commit()
    except Exception as exc:
        log.warning("No se pudo registrar scraper_runs: %s", exc)

    # Parte 1.10 — resumen legible de la corrida (además del registro en DB).
    # "ok" = normalizadas y persistidas (nuevas + actualizadas). Los reason
    # codes top dan visibilidad de descartes (incluye renta/contenido no
    # parseable). El detalle por-campo se acumula en scrapers.ingest_summary
    # cuando se instrumente el bucle de upsert.
    top_descartes = ", ".join(
        f"{code}={n}" for code, n in list(distribucion_descartes_reason_code.items())[:5]
    )
    log.info(
        "ingesta_resumen instituciones=%d encontradas=%d ok=%d (nuevas=%d actualizadas=%d) "
        "descartadas=%d vencidas=%d errores=%d tasa=%.1f%%%s",
        len(reports),
        total_encontradas,
        total_nuevas + total_actualizadas,
        total_nuevas,
        total_actualizadas,
        total_descartadas,
        vencidas_cerradas,
        total_errores,
        tasa,
        (f" | top_descartes[{top_descartes}]" if top_descartes else ""),
    )


def _print_evaluation_summary(runtime_sources: list[RuntimeSource]) -> None:
    by_decision: dict[str, int] = {}
    for item in runtime_sources:
        key = item.evaluation.decision.value
        by_decision[key] = by_decision.get(key, 0) + 1
    log.info("Resumen de gatekeeper: %s", by_decision)


def _log_runtime_inventory() -> None:
    runtime_rows = iter_runtime_rows()
    legacy_rows = iter_legacy_rows()
    log.info(
        "Runtime productivo explícito (%d módulos activos): %s",
        len(runtime_rows),
        [
            f"{row['extractor']}:{row['profile_name']}->{row['class_name']}"
            for row in runtime_rows
        ],
    )
    log.info(
        "Legacy deprecados (%d módulos): %s",
        len(legacy_rows),
        [
            f"{row['module']}({row['status']}, retiro={row['retirement_date']})"
            for row in legacy_rows
        ],
    )


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run all scraping pipeline with gatekeeper.")
    parser.add_argument("--catalog-json", help="Ruta alternativa al catalogo JSON.")
    parser.add_argument("--catalog-xlsx", help="Ruta alternativa al catalogo XLSX.")
    parser.add_argument(
        "--mode",
        choices=["production", "development", "staging"],
        default="development",
        help="Modo de ejecución (no cambia el comportamiento, sirve para trazabilidad en logs).",
    )
    parser.add_argument("--limit", type=int, help="Limitar fuentes para una corrida parcial.")
    parser.add_argument(
        "--ids",
        type=str,
        help="Lista de IDs de instituciones separados por coma para correr solo esas (ej: 315,387,562).",
    )
    parser.add_argument(
        "--id",
        type=int,
        help="Atajo para correr una sola institución por ID (equivale a --ids con un valor).",
    )
    parser.add_argument(
        "--only-kind",
        type=str,
        help=(
            "Correr solo las fuentes cuyo ScraperKind (clasificación de "
            "source_status) coincida (ej: wordpress, empleos_publicos, "
            "custom_trabajando). Se aplica antes de --limit."
        ),
    )
    parser.add_argument(
        "--skip-empleos-publicos",
        action="store_true",
        help="Excluir instituciones cuya url_empleo apunta a empleospublicos.cl (útil para probar scrapers de portales propios).",
    )
    parser.add_argument("--evaluate-only", action="store_true", help="Solo ejecutar discovery+evaluation.")
    parser.add_argument(
        "--force-evaluate",
        action="store_true",
        help=(
            "Ignorar cooldowns de retry_policy y re-evaluar todas las fuentes. "
            "Por defecto las fuentes con evaluación reciente se saltan según su "
            "retry_policy (critical=3h, high=6h, … eventual=168h)."
        ),
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help=(
            "Habilitar OCR de PDFs escaneados en los scrapers que lo soporten "
            "(hoy Carabineros, para leer las 'Bases' del concurso escaneadas). "
            "Lento; requiere el binario tesseract-ocr + tesseract-ocr-spa "
            "instalado en el sistema. Apagado por defecto; el servicio de "
            "producción lo activa."
        ),
    )
    args = parser.parse_args(argv)

    log.info("Inicio run_all gatekeeper %s modo=%s", datetime.now().isoformat(timespec="seconds"), args.mode)
    _log_runtime_inventory()
    t0 = time.monotonic()
    db_enabled = True
    try:
        get_pool()
    except Exception as exc:
        db_enabled = False
        log.warning("BD no disponible para esta corrida: %s", exc)
        if not args.evaluate_only:
            log.warning("Se fuerza modo --evaluate-only porque no hay acceso a BD.")
            args.evaluate_only = True

    loader = CatalogLoader(json_path=args.catalog_json, xlsx_path=args.catalog_xlsx)
    fuentes_index = _load_fuentes_index()
    audit_store = AuditStore()
    # Validar --only-kind contra los ScraperKind conocidos (evita un filtro
    # silencioso que no calza con nada y "corre 0 fuentes" sin avisar).
    only_kind = getattr(args, "only_kind", None)
    if only_kind:
        _kinds_validos = {k.value for k in ScraperKind}
        if only_kind not in _kinds_validos:
            parser.error(
                f"--only-kind inválido: {only_kind!r}. Válidos: {sorted(_kinds_validos)}"
            )

    catalog_sources = _build_discovery_catalog(
        loader,
        limit=args.limit,
        skip_empleos_publicos=getattr(args, "skip_empleos_publicos", False),
        only_kind=only_kind,
    )
    if only_kind:
        log.info("--only-kind=%s activo: %d fuentes tras filtrar por kind.", only_kind, len(catalog_sources))

    # Filtrar por IDs específicos si se indica --ids / --id
    target_ids: set[int] = set()
    if getattr(args, "ids", None):
        target_ids |= {int(x.strip()) for x in args.ids.split(",") if x.strip()}
    if getattr(args, "id", None):
        target_ids.add(int(args.id))
    if target_ids:
        catalog_sources = [s for s in catalog_sources if s.get("id") in target_ids]
        log.info("--ids/--id activo: filtrando a %d instituciones: %s", len(catalog_sources), sorted(target_ids))

    # ── Filtrado por cooldown de retry_policy ──────────────────────────────
    cooldown_reason_by_inst: dict[int, str] = {}
    if args.force_evaluate:
        log.info("--force-evaluate activo: se evaluarán las %d fuentes sin respetar cooldown.", len(catalog_sources))
        sources_to_evaluate = catalog_sources
        for src in sources_to_evaluate:
            if src.get("id") is not None:
                cooldown_reason_by_inst[int(src["id"])] = "force_evaluate"
    else:
        last_evaluations = _load_last_evaluations()
        sources_to_evaluate, cooldown_decisions = _partition_by_cooldown(catalog_sources, last_evaluations)
        in_cooldown = [d.source for d in cooldown_decisions if not d.reevaluate]
        reevaluated_in_cooldown = [d for d in cooldown_decisions if d.reevaluate and d.reason.startswith("cooldown_") and d.reason != "cooldown_vencido"]
        for d in cooldown_decisions:
            inst_id = d.source.get("id")
            if inst_id is not None and d.reevaluate:
                cooldown_reason_by_inst[int(inst_id)] = d.reason
        if in_cooldown:
            log.info(
                "Retry-policy cooldown: %d fuentes en cooldown (se saltan), %d fuentes a evaluar ahora.",
                len(in_cooldown),
                len(sources_to_evaluate),
            )
            # Resumen de por qué están en cooldown
            cooldown_by_policy: dict[str, int] = {}
            for src in in_cooldown:
                inst_id = src.get("id")
                policy = last_evaluations.get(int(inst_id)).retry_policy if inst_id and last_evaluations.get(int(inst_id)) else "unknown"
                cooldown_by_policy[policy] = cooldown_by_policy.get(policy, 0) + 1
            log.info("Distribución cooldown por retry_policy: %s", cooldown_by_policy)
            # Detalle por-fuente solo en DEBUG: en producción son cientos de
            # líneas que saturan el límite de logs de Railway (500/seg). El
            # resumen por retry_policy de arriba ya da la visibilidad necesaria.
            for src in in_cooldown:
                log.debug("Fuente omitida por cooldown: id=%s nombre=%s", src.get("id"), src.get("nombre", "?"))
        else:
            log.info("Todas las %d fuentes están listas para evaluación.", len(sources_to_evaluate))
        if reevaluated_in_cooldown:
            summary: dict[str, int] = {}
            for decision in reevaluated_in_cooldown:
                summary[decision.reason] = summary.get(decision.reason, 0) + 1
            log.info("Reevaluaciones incrementales en cooldown: %s", summary)
    if args.force_evaluate:
        last_evaluations = _load_last_evaluations()

    if not sources_to_evaluate:
        log.info("No hay fuentes a evaluar en esta corrida (todas en cooldown). Finalizando.")
        if db_enabled:
            cerrar_pool()
        return 0

    runtime_sources = await _evaluate_sources(sources_to_evaluate, audit_store=audit_store, fuentes_index=fuentes_index)
    _enforce_playwright_capability(runtime_sources)
    for item in runtime_sources:
        if item.evaluation.decision != Decision.EXTRACT:
            # DEBUG: el resumen agregado lo da _print_evaluation_summary abajo.
            log.debug(
                "Fuente omitida por evaluador: id=%s nombre=%s decision=%s reason=%s",
                item.institucion.get("id"),
                item.institucion.get("nombre", "?"),
                item.evaluation.decision.value,
                item.evaluation.reason_code.value if item.evaluation.reason_code else "none",
            )
    _print_evaluation_summary(runtime_sources)

    reports: list[PrecisionReport] = []
    report_rows: list[tuple[int | None, PrecisionReport]] = []
    if not args.evaluate_only:
        # Excluir del dispatch de CLASES las fuentes migradas al nuevo estándar
        # (las atienden los batches ejecutar() más abajo). Evita doble corrida.
        sources_para_clases = [
            it for it in runtime_sources
            if (getattr(it.evaluation, "profile_name", None) or "") not in _PERFILES_NUEVO_ESTANDAR
            and it.institucion.get("id") not in _IDS_NUEVO_ESTANDAR
        ]
        assignments, run_empleos_publicos = _build_scrapers(sources_para_clases)
        log.info("Scrapers ejecutables en este runtime: %s", len(assignments))
        report_rows = await _run_scrapers(assignments)
        reports = [report for _, report in report_rows]

        if run_empleos_publicos and not getattr(args, "skip_empleos_publicos", False):
            ep_report = await asyncio.to_thread(_run_empleos_publicos_sync, catalog_sources)
            reports.append(ep_report)

        # Nuevo estándar: módulos ejecutar() auto-contenidos. Se corren solo si
        # sus fuentes están en el catálogo de esta corrida (respeta --ids/--limit).
        # Carabineros = fuente 161; Trabajando = cualquier *.trabajando.cl.
        hay_carabineros = any(s.get("id") == 161 for s in catalog_sources)
        hay_trabajando = any(
            "trabajando.cl" in str(s.get("url_empleo") or "").lower() for s in catalog_sources
        )
        if hay_carabineros:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "carabineros (161)",
                    carabineros_scraper.ejecutar, allow_ocr=args.ocr,
                )
            )
        if hay_trabajando:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "trabajando.cl", trabajando_scraper.ejecutar
                )
            )
        hay_buk = any(
            "buk.cl" in str(s.get("url_empleo") or "").lower() for s in catalog_sources
        )
        if hay_buk:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "buk.cl", buk_scraper.ejecutar
                )
            )
        hay_pdi = any(s.get("id") == 162 for s in catalog_sources)
        if hay_pdi:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "pdi (162)", pdi_scraper.ejecutar
                )
            )
        hay_bcentral = any(s.get("id") == 145 for s in catalog_sources)
        if hay_bcentral:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "banco central (145)", bcentral_scraper.ejecutar
                )
            )
        hay_armada = any(s.get("id") == 158 for s in catalog_sources)
        if hay_armada:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "armada (158)", armada_scraper.ejecutar
                )
            )
        hay_divsal = any(s.get("id") == 706 for s in catalog_sources)
        if hay_divsal:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "divsal (706)", divsal_scraper.ejecutar
                )
            )
        hay_famae = any(s.get("id") == 165 for s in catalog_sources)
        if hay_famae:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "famae (165)", famae_scraper.ejecutar
                )
            )
        # Laborum es plataforma: corre si alguna fuente del catálogo es un perfil
        # registrado en laborum.PERFILES (Hospital Militar 707, y futuras).
        hay_laborum = any(
            s.get("id") in laborum_scraper.PERFILES for s in catalog_sources
        )
        if hay_laborum:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "laborum", laborum_scraper.ejecutar
                )
            )
        hay_codelco = any(s.get("id") == 275 for s in catalog_sources)
        if hay_codelco:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "codelco (275)", codelco_scraper.ejecutar
                )
            )
        hay_tc = any(s.get("id") == 139 for s in catalog_sources)
        if hay_tc:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "tribunal_constitucional (139)",
                    tribunal_constitucional_scraper.ejecutar
                )
            )
        hay_senado = any(s.get("id") == 135 for s in catalog_sources)
        if hay_senado:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "senado (135)", senado_scraper.ejecutar
                )
            )
        hay_pj = any(s.get("id") == 138 for s in catalog_sources)
        if hay_pj:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "poder_judicial (138)",
                    poder_judicial_scraper.ejecutar
                )
            )
        hay_fiscalia = any(s.get("id") == 146 for s in catalog_sources)
        if hay_fiscalia:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "fiscalia (146)", fiscalia_scraper.ejecutar
                )
            )
        hay_puertos = any(s.get("id") in IDS_PUERTOS_EMPRESAS for s in catalog_sources)
        if hay_puertos:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "puertos/empresas", puertos_empresas_scraper.ejecutar
                )
            )
        # municipios.py: scraper agrupado de municipalidades/corporaciones con
        # modos de extracción por sitio (pdf_links, secciones, enlaces_detalle,
        # headings, lista_o_vacio). Corre si alguna de sus fuentes funcionales
        # está en el catálogo activo. Excluidas del genérico vía _IDS_NUEVO_ESTANDAR.
        hay_municipios = any(s.get("id") in IDS_MUNICIPIOS for s in catalog_sources)
        if hay_municipios:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "municipios", municipios_scraper.ejecutar
                )
            )
        # LinkedIn Peñalolén (401): herramienta asistida; corre solo si existe el
        # archivo curado de URLs junto al scraper (si no, no haría nada útil).
        _linkedin_urls = Path(__file__).resolve().parent / "linkedin_urls.txt"
        if _linkedin_urls.exists() and any(s.get("id") == 401 for s in catalog_sources):
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "linkedin_penalolen (401)", linkedin_penalolen_scraper.ejecutar
                )
            )
        # Fundación Integra portal Aira (132). Su otra plataforma
        # (integra.trabajando.cl) la cubre trabajando.py; mismo institucion_id
        # pero dominios distintos, así que no se cierran ofertas entre sí.
        hay_aira_integra = any(s.get("id") == 132 for s in catalog_sources)
        if hay_aira_integra:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "aira_integra (132)", aira_integra_scraper.ejecutar
                )
            )
        # Alta Dirección Pública (Servicio Civil, id 130): fuente ADICIONAL a
        # su publicación en empleospublicos.cl. Dominio adp.serviciocivil.cl,
        # así que el cierre por dominio no toca las ofertas de empleospublicos.
        hay_adp = any(s.get("id") == 130 for s in catalog_sources)
        if hay_adp:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "adp (130)", adp_scraper.ejecutar
                )
            )
        _IDS_UNIVERSIDADES = {245, 248, 253, 255}  # 254→trabajando, 258/259→uni_wp, 246/251→uni_portal, 243/244/252→uni_tabla
        hay_universidades = any(s.get("id") in _IDS_UNIVERSIDADES for s in catalog_sources)
        if hay_universidades:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "universidades", universidades_scraper.ejecutar
                )
            )
        _IDS_UNIVERSIDADES_WP = {258, 259}
        hay_uni_wp = any(s.get("id") in _IDS_UNIVERSIDADES_WP for s in catalog_sources)
        if hay_uni_wp:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "universidades_wp", universidades_wp_scraper.ejecutar
                )
            )
        _IDS_UNIVERSIDADES_PORTAL = {251, 246}
        hay_uni_portal = any(s.get("id") in _IDS_UNIVERSIDADES_PORTAL for s in catalog_sources)
        if hay_uni_portal:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "universidades_portal", universidades_portal_scraper.ejecutar
                )
            )
        _IDS_UNIVERSIDADES_TABLA = {243, 244, 252}
        hay_uni_tabla = any(s.get("id") in _IDS_UNIVERSIDADES_TABLA for s in catalog_sources)
        if hay_uni_tabla:
            reports.append(
                await asyncio.to_thread(
                    _run_modulo_ejecutar_sync, "universidades_tabla", universidades_tabla_scraper.ejecutar
                )
            )

    consecutive_zero_by_inst = _compute_consecutive_zero_extractions(
        runtime_sources=runtime_sources,
        report_rows=report_rows,
        previous_states=last_evaluations,
    )
    _persist_source_evaluations(
        runtime_sources=runtime_sources,
        audit_store=audit_store,
        consecutive_zero_by_inst=consecutive_zero_by_inst,
        cooldown_reason_by_inst=cooldown_reason_by_inst,
        previous_states=last_evaluations,
    )

    vencidas_cerradas = 0
    try:
        with conexion() as conn:
            vencidas_cerradas = limpiar_vencidas(conn)
    except Exception as exc:
        log.warning("No se pudo ejecutar limpiar_vencidas: %s", exc)

    duration_seconds = time.monotonic() - t0
    if reports:
        print("\n" + generar_reporte(reports))
        top_reason_codes_por_fuente = {
            report.institucion: report.top_reason_codes()
            for report in reports
            if report.descartes_por_reason_code
        }
        if top_reason_codes_por_fuente:
            log.info("Top reason_code de descarte por fuente: %s", top_reason_codes_por_fuente)
    persistir_corrida(
        evaluations=runtime_sources,
        reports=reports,
        duration_seconds=duration_seconds,
        vencidas_cerradas=vencidas_cerradas,
    )

    # Validación de URLs de ofertas (poblar url_oferta_valida / url_bases_valida,
    # que el frontend usa para gatear "Ver bases"/"Postular"). Antes lo hacía
    # run_scrapers.py. max_edad_h=24 → solo revalida lo nuevo o vencido, no todo
    # cada corrida. Corre en thread (usa su propio asyncio.run) y nunca rompe.
    if not args.evaluate_only and db_enabled:
        try:
            import validate_offer_urls
            stats_urls = await asyncio.to_thread(validate_offer_urls.main, None, 20, 24)
            log.info("Validación de URLs de ofertas: %s", stats_urls)
        except Exception as exc:
            log.warning("No se pudo validar URLs de ofertas: %s", exc)

    if db_enabled:
        cerrar_pool()

    if not runtime_sources:
        return 1
    if args.evaluate_only:
        return 0
    if reports and all(report.errores > 0 and (report.guardadas + report.ya_existian) == 0 for report in reports):
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        log.warning("Interrumpido por el usuario")
        raise SystemExit(130)
