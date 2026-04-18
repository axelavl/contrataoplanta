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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

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
from scrapers.evaluation.audit_store import AuditStore
from scrapers.evaluation.catalog_loader import CatalogLoader
from scrapers.evaluation.models import Decision, ExtractorKind
from scrapers.evaluation.source_evaluator import SourceEvaluator
from scrapers.plataformas.buk import BukScraper
from scrapers.plataformas.carabineros import CarabinerosScraper
from scrapers.plataformas.ffaa import FfaaScraper
from scrapers.plataformas.generic_site import GenericSiteScraper
from scrapers.plataformas.hiringroom import HiringRoomScraper
from scrapers.plataformas.pdi import PdiScraper
from scrapers.plataformas.playwright_scraper import PlaywrightScraper
from scrapers.plataformas.trabajando_cl import TrabajandoCLScraper
from scrapers.plataformas.wordpress import WordPressScraper


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


@dataclass(slots=True)
class RuntimeSource:
    institucion: dict[str, Any]
    fuente_id: int | None
    evaluation: Any


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
    return items[:limit] if limit else items


def _load_last_evaluations() -> dict[int, tuple[str | None, datetime | None]]:
    """Carga la última retry_policy y evaluated_at por institucion_id desde source_evaluations.

    Returns:
        Dict ``{institucion_id: (retry_policy_str, evaluated_at)}``.
        Las claves son ``int``; el valor puede tener ``None`` en cualquier campo.
    """
    result: dict[int, tuple[str | None, datetime | None]] = {}
    try:
        with conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (institucion_id)
                        institucion_id,
                        retry_policy,
                        evaluated_at
                    FROM source_evaluations
                    WHERE institucion_id IS NOT NULL
                    ORDER BY institucion_id, evaluated_at DESC
                    """
                )
                for inst_id, retry_policy, evaluated_at in cur.fetchall():
                    result[inst_id] = (retry_policy, evaluated_at)
    except Exception as exc:
        log.warning("No se pudo cargar source_evaluations para cooldown: %s", exc)
    return result


def _partition_by_cooldown(
    sources: list[dict[str, Any]],
    last_evaluations: dict[int, tuple[str | None, datetime | None]],
    *,
    now: datetime | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Separa fuentes en (due, in_cooldown).

    ``due`` son las que deben evaluarse ahora.
    ``in_cooldown`` son las que aún no han cumplido su cooldown desde la
    última evaluación y se saltan en esta corrida.
    """
    due: list[dict[str, Any]] = []
    in_cooldown: list[dict[str, Any]] = []
    _now = now or datetime.now(tz=timezone.utc)
    for source in sources:
        inst_id = source.get("id")
        if inst_id is None:
            due.append(source)
            continue
        entry = last_evaluations.get(int(inst_id))
        if entry is None:
            due.append(source)
            continue
        retry_policy, last_evaluated_at = entry
        if should_evaluate_now(retry_policy=retry_policy, last_evaluated_at=last_evaluated_at, now=_now):
            due.append(source)
        else:
            in_cooldown.append(source)
    return due, in_cooldown


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
                historical_noise_ratio = 0.0
                try:
                    with conexion() as conn:
                        historical_noise_ratio = audit_store.get_institution_noise_ratio(conn, source.get("id"))
                except Exception:
                    historical_noise_ratio = 0.0
                evaluation = await evaluator.evaluate(source, historical_noise_ratio=historical_noise_ratio)
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

    # Persistir evaluaciones con conexión directa limpia (evita estado sucio del pool)
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
                    audit_store.save_source_evaluation(
                        conn_direct,
                        source_id=item.fuente_id,
                        institucion_id=item.institucion.get("id"),
                        evaluation=item.evaluation,
                    )
                    if item.fuente_id is None and item.evaluation.decision == Decision.EXTRACT:
                        audit_store.save_catalog_event(
                            conn_direct,
                            institucion_id=item.institucion.get("id"),
                            event_type="missing_runtime_source",
                            detail="La fuente fue evaluada como extraible, pero no existe mapeo fuente_id en el runtime actual.",
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

    return list(runtime_sources)


def _build_scrapers(runtime_sources: list[RuntimeSource]) -> list[BaseScraper]:
    scrapers: list[BaseScraper] = []
    empleos_publicos_agregado = False

    for item in runtime_sources:
        evaluation = item.evaluation
        if evaluation.decision != Decision.EXTRACT:
            continue
        if evaluation.recommended_extractor not in SUPPORTED_RUNTIME_EXTRACTORS:
            continue
        # fuente_id puede ser None si la tabla fuentes no está poblada —
        # los scrapers aceptan None y la columna ofertas.fuente_id es nullable.

        if evaluation.recommended_extractor == ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS:
            # EmpleosPublicosScraper usa arquitectura legacy (ejecutar() propio,
            # no implementa descubrir_ofertas). Se ejecuta aparte si es necesario.
            if not empleos_publicos_agregado:
                log.info("SCRAPER_EMPLEOS_PUBLICOS detectado — se omite del despacho BaseScraper (correr por separado)")
                empleos_publicos_agregado = True
            continue

        if evaluation.recommended_extractor in {
            ExtractorKind.SCRAPER_WORDPRESS_JOBS,
            ExtractorKind.SCRAPER_WORDPRESS_NEWS_FILTER,
        }:
            url_base = (
                str(item.institucion.get("url_empleo") or "").strip()
                or str(item.institucion.get("sitio_web") or "").strip()
            )
            scrapers.append(
                WordPressScraper(
                    fuente_id=item.fuente_id,
                    nombre_fuente=str(item.institucion.get("nombre") or item.institucion.get("sigla") or f"wp-{item.institucion.get('id')}"),
                    url_base=url_base,
                    sector=item.institucion.get("sector"),
                    region=item.institucion.get("region"),
                )
            )
            continue

        if evaluation.recommended_extractor == ExtractorKind.SCRAPER_EXTERNAL_ATS:
            profile_name = item.evaluation.profile_name or ""
            if profile_name == "ats_trabajando":
                scrapers.append(TrabajandoCLScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            elif profile_name == "ats_hiringroom":
                scrapers.append(HiringRoomScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            elif profile_name == "ats_buk":
                scrapers.append(BukScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            else:
                scrapers.append(GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            continue

        if evaluation.recommended_extractor == ExtractorKind.SCRAPER_PDF_JOBS:
            inst_id = item.institucion.get("id")
            if inst_id == 161:
                scrapers.append(CarabinerosScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            elif inst_id == 162:
                scrapers.append(PdiScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            else:
                scrapers.append(GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            continue

        if evaluation.recommended_extractor == ExtractorKind.SCRAPER_CUSTOM_DETAIL:
            profile_name = item.evaluation.profile_name or ""
            if profile_name == "ffaa_waf" or item.institucion.get("id") in {157, 158}:
                scrapers.append(FfaaScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            elif item.institucion.get("id") == 161:
                scrapers.append(CarabinerosScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            elif item.institucion.get("id") == 162:
                scrapers.append(PdiScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            else:
                scrapers.append(GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            continue

        if evaluation.recommended_extractor == ExtractorKind.SCRAPER_PLAYWRIGHT:
            scrapers.append(PlaywrightScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            continue

        if evaluation.recommended_extractor == ExtractorKind.SCRAPER_GENERIC_FALLBACK:
            scrapers.append(GenericSiteScraper(fuente_id=item.fuente_id, institucion=item.institucion))
            continue
    return scrapers


async def _run_scraper(scraper: BaseScraper, sem: asyncio.Semaphore) -> PrecisionReport:
    async with sem:
        try:
            async with scraper:
                return await scraper.run()
        except Exception as exc:
            log.exception("Scraper %s fallo: %s", scraper.nombre_fuente, exc)
            report = scraper.report
            report.errores += 1
            return report


async def _run_scrapers(scrapers: list[BaseScraper]) -> list[PrecisionReport]:
    if not scrapers:
        return []
    sem = asyncio.Semaphore(MAX_SCRAPERS_CONCURRENT)
    return list(await asyncio.gather(*[_run_scraper(scraper, sem) for scraper in scrapers]))


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
    detail = {
        "reports": {report.institucion: report.to_dict() for report in reports},
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


def _print_evaluation_summary(runtime_sources: list[RuntimeSource]) -> None:
    by_decision: dict[str, int] = {}
    for item in runtime_sources:
        key = item.evaluation.decision.value
        by_decision[key] = by_decision.get(key, 0) + 1
    log.info("Resumen de gatekeeper: %s", by_decision)


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
    args = parser.parse_args(argv)

    log.info("Inicio run_all gatekeeper %s modo=%s", datetime.now().isoformat(timespec="seconds"), args.mode)
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
    catalog_sources = _build_discovery_catalog(
        loader,
        limit=args.limit,
        skip_empleos_publicos=getattr(args, "skip_empleos_publicos", False),
    )
    # Filtrar por IDs específicos si se indica --ids
    if getattr(args, "ids", None):
        target_ids = {int(x.strip()) for x in args.ids.split(",") if x.strip()}
        catalog_sources = [s for s in catalog_sources if s.get("id") in target_ids]
        log.info("--ids activo: filtrando a %d instituciones: %s", len(catalog_sources), sorted(target_ids))

    # ── Filtrado por cooldown de retry_policy ──────────────────────────────
    if args.force_evaluate:
        log.info("--force-evaluate activo: se evaluarán las %d fuentes sin respetar cooldown.", len(catalog_sources))
        sources_to_evaluate = catalog_sources
    else:
        last_evaluations = _load_last_evaluations()
        sources_to_evaluate, in_cooldown = _partition_by_cooldown(catalog_sources, last_evaluations)
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
                policy = (last_evaluations.get(int(inst_id), (None, None))[0] or "unknown") if inst_id else "unknown"
                cooldown_by_policy[policy] = cooldown_by_policy.get(policy, 0) + 1
            log.info("Distribución cooldown por retry_policy: %s", cooldown_by_policy)
        else:
            log.info("Todas las %d fuentes están listas para evaluación.", len(sources_to_evaluate))

    if not sources_to_evaluate:
        log.info("No hay fuentes a evaluar en esta corrida (todas en cooldown). Finalizando.")
        if db_enabled:
            cerrar_pool()
        return 0

    runtime_sources = await _evaluate_sources(sources_to_evaluate, audit_store=audit_store, fuentes_index=fuentes_index)
    _print_evaluation_summary(runtime_sources)

    reports: list[PrecisionReport] = []
    if not args.evaluate_only:
        scrapers = _build_scrapers(runtime_sources)
        log.info("Scrapers ejecutables en este runtime: %s", len(scrapers))
        reports = await _run_scrapers(scrapers)

    vencidas_cerradas = 0
    try:
        with conexion() as conn:
            vencidas_cerradas = limpiar_vencidas(conn)
    except Exception as exc:
        log.warning("No se pudo ejecutar limpiar_vencidas: %s", exc)

    duration_seconds = time.monotonic() - t0
    if reports:
        print("\n" + generar_reporte(reports))
    persistir_corrida(
        evaluations=runtime_sources,
        reports=reports,
        duration_seconds=duration_seconds,
        vencidas_cerradas=vencidas_cerradas,
    )
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
