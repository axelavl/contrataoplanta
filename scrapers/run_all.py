"""
Orquestador principal de scrapers.

Filtra las fuentes del catálogo según su clasificación operativa
(``source_status.classify_source``) y ejecuta el scraper que le corresponde
a cada una. Por defecto corre sólo fuentes ``active``: todo lo demás queda
fuera salvo que se pase un flag explícito.

Ejemplos:

    # Corrida normal: sólo fuentes activas, modo production (rápido).
    python scrapers/run_all.py

    # Incluir experimentales (WordPress sin verificar, portales de terceros).
    python scrapers/run_all.py --include-experimental

    # Corrida exploratoria sobre las que están en revisión manual.
    python scrapers/run_all.py --include-manual-review --mode exploration

    # Sólo WordPress, máx 10 ofertas por fuente.
    python scrapers/run_all.py --only-kind wordpress --max 10

    # Probar rápido sin escribir DB ni correr el batch de empleospublicos.
    python scrapers/run_all.py --dry-run --max 3 --skip-empleos-publicos
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.base import LOG_DIR, build_file_handler, clean_text, normalize_key
from scrapers.empleos_publicos import EmpleosPublicosScraper
from scrapers.plataformas.generic_site import GenericSiteScraper
from scrapers.plataformas.wordpress import WordPressScraper
from scrapers.source_status import (
    DEFAULT_RUN_STATUSES,
    ScraperKind,
    SourceDecision,
    SourceStatus,
    classify_source,
    enrich_with_status,
    filter_runnable,
    kind_breakdown,
    status_breakdown,
)


LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scrapers.run_all")
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = build_file_handler(
        LOG_DIR / f"scraper_{time.strftime('%Y%m%d')}.log"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
logger.propagate = False


# Mapeo ScraperKind -> módulo importable con función ``ejecutar``.
PLATFORM_MODULES: dict[ScraperKind, str] = {
    ScraperKind.CUSTOM_TRABAJANDO: "scrapers.plataformas.trabajando_cl",
    ScraperKind.CUSTOM_HIRINGROOM: "scrapers.plataformas.hiringroom",
    ScraperKind.CUSTOM_BUK: "scrapers.plataformas.buk",
    ScraperKind.CUSTOM_PLAYWRIGHT: "scrapers.plataformas.playwright_scraper",
    ScraperKind.CUSTOM_POLICIA: "scrapers.plataformas.policia",
    ScraperKind.CUSTOM_FFAA: "scrapers.plataformas.ffaa",
}


@dataclass(slots=True)
class ResultadoEjecucion:
    nombre: str
    status: str
    found: int = 0
    nuevas: int = 0
    actualizadas: int = 0
    cerradas: int = 0
    errores: int = 0
    detalle: str = ""
    duracion: float = 0.0
    kind: str = ""


# ────────────────────────────── Carga catálogo ────────────────────────

def load_repository(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    instituciones = payload.get("instituciones") if isinstance(payload, dict) else payload
    if not isinstance(instituciones, list):
        raise ValueError("El JSON maestro no contiene una lista valida de instituciones")
    return instituciones


# ────────────────────────────── main ──────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Orquestador de scrapers publicos")
    parser.add_argument(
        "--json",
        default=str(
            Path(__file__).resolve().parents[1]
            / "repositorio_instituciones_publicas_chile.json"
        ),
        help="Ruta al repositorio maestro de instituciones",
    )
    parser.add_argument("--sector", default=None, help="Filtrar por sector")
    parser.add_argument("--id", type=int, default=None, help="Ejecutar solo una institucion")
    parser.add_argument("--dry-run", action="store_true", help="No guarda en PostgreSQL")
    parser.add_argument("--max", type=int, default=None, help="Limite de ofertas por scraper")
    parser.add_argument(
        "--max-fuentes",
        type=int,
        default=None,
        help="Máximo de fuentes a ejecutar (después de filtros). Útil para pruebas.",
    )
    parser.add_argument(
        "--mode",
        choices=["production", "exploration"],
        default="production",
        help="Perfil del scraper genérico: production (rápido) o exploration (amplio)",
    )
    parser.add_argument(
        "--include-experimental",
        action="store_true",
        help="Incluir fuentes con status=experimental",
    )
    parser.add_argument(
        "--include-manual-review",
        action="store_true",
        help="Incluir fuentes con status=manual_review",
    )
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Incluir fuentes disabled/broken/no_data/blocked/js_required",
    )
    parser.add_argument(
        "--only-status",
        default=None,
        help="Restringir a un único status (p.ej. active)",
    )
    parser.add_argument(
        "--only-kind",
        default=None,
        help="Restringir a un único kind (wordpress|generic|empleos_publicos|custom_trabajando|...)",
    )
    parser.add_argument(
        "--only-platform",
        default=None,
        help="Alias de --only-kind, por compatibilidad",
    )
    parser.add_argument(
        "--skip-empleos-publicos",
        action="store_true",
        help="No correr el batch de empleospublicos.cl",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Sólo imprime la clasificación y sale sin scrapear",
    )
    args = parser.parse_args()

    instituciones = load_repository(args.json)

    # ── filtros del catálogo base ──
    if args.sector:
        sector_key = normalize_key(args.sector)
        instituciones = [
            item for item in instituciones if normalize_key(item.get("sector")) == sector_key
        ]
    if args.id is not None:
        instituciones = [item for item in instituciones if item.get("id") == args.id]

    if not instituciones:
        print("No hay instituciones en el catálogo para los filtros dados.")
        return

    # ── clasificación ──
    enriched = enrich_with_status(instituciones)
    status_counts = status_breakdown(enriched)
    kind_counts = kind_breakdown(enriched)

    # ── status permitidos ──
    allowed: set[SourceStatus] = set(DEFAULT_RUN_STATUSES)
    if args.include_experimental:
        allowed.add(SourceStatus.EXPERIMENTAL)
    if args.include_manual_review:
        allowed.add(SourceStatus.MANUAL_REVIEW)
    if args.include_disabled:
        allowed |= {
            SourceStatus.DISABLED,
            SourceStatus.BROKEN,
            SourceStatus.NO_DATA,
            SourceStatus.BLOCKED,
            SourceStatus.JS_REQUIRED,
        }
    if args.only_status:
        try:
            allowed = {SourceStatus(args.only_status)}
        except ValueError:
            raise SystemExit(
                f"--only-status inválido: {args.only_status}. "
                f"Válidos: {', '.join(s.value for s in SourceStatus)}"
            )

    # ── kind filter ──
    only_kind: ScraperKind | None = None
    only_kind_raw = args.only_kind or args.only_platform
    if only_kind_raw:
        try:
            only_kind = ScraperKind(only_kind_raw)
        except ValueError:
            raise SystemExit(
                f"--only-kind inválido: {only_kind_raw}. "
                f"Válidos: {', '.join(k.value for k in ScraperKind)}"
            )

    # ── fuentes que efectivamente entran a la corrida por-sitio ──
    runnable_all = filter_runnable(enriched, allowed, only_kind=only_kind)

    # Excluimos EMPLEOS_PUBLICOS de la corrida por-sitio: lo maneja el batch.
    runnable_per_site = [
        (inst, dec) for inst, dec in runnable_all if dec.kind != ScraperKind.EMPLEOS_PUBLICOS
    ]

    if args.max_fuentes is not None:
        runnable_per_site = runnable_per_site[: args.max_fuentes]

    # ── resumen previo ──
    print_pre_run_summary(
        total_catalogo=len(instituciones),
        status_counts=status_counts,
        kind_counts=kind_counts,
        runnable_count=len(runnable_per_site),
        allowed=allowed,
        mode=args.mode,
        only_kind=only_kind_raw,
    )
    logger.info(
        "evento=run_all_inicio total=%s runnable=%s mode=%s dry_run=%s max=%s",
        len(instituciones),
        len(runnable_per_site),
        args.mode,
        args.dry_run,
        args.max,
    )

    if args.list_only:
        print_classification_detail(enriched, allowed)
        return

    # ── ejecución ──
    start = time.time()
    resultados: list[ResultadoEjecucion] = []

    # 1) batch empleospublicos.cl — a menos que el usuario lo corte
    if (
        not args.skip_empleos_publicos
        and (only_kind is None or only_kind == ScraperKind.EMPLEOS_PUBLICOS)
    ):
        instituciones_ep = [
            inst for inst, dec in enriched if dec.covered_by_central
        ]
        if instituciones_ep:
            resultados.append(
                run_empleos_publicos(
                    instituciones=instituciones_ep,
                    dry_run=args.dry_run,
                    max_results=args.max,
                )
            )

    # 2) corrida por-sitio (WordPress / Generic / Custom)
    for institucion, decision in runnable_per_site:
        resultados.append(
            run_single_source(
                institucion=institucion,
                decision=decision,
                instituciones_catalogo=instituciones,
                dry_run=args.dry_run,
                max_results=args.max,
                mode=args.mode,
            )
        )

    elapsed = round(time.time() - start, 2)
    print_summary(resultados, elapsed)
    logger.info(
        "evento=run_all_fin total_resultados=%s duracion_seg=%s",
        len(resultados),
        elapsed,
    )


# ────────────────────────── Routing por kind ──────────────────────────

def run_single_source(
    institucion: dict[str, Any],
    decision: SourceDecision,
    instituciones_catalogo: list[dict[str, Any]],
    dry_run: bool,
    max_results: int | None,
    mode: str,
) -> ResultadoEjecucion:
    kind = decision.kind
    if kind == ScraperKind.WORDPRESS:
        result = run_wordpress(
            institucion=institucion,
            instituciones_catalogo=instituciones_catalogo,
            dry_run=dry_run,
            max_results=max_results,
        )
    elif kind == ScraperKind.GENERIC:
        result = run_generic_site(
            institucion=institucion,
            instituciones_catalogo=instituciones_catalogo,
            dry_run=dry_run,
            max_results=max_results,
            mode=mode,
            detalle=decision.reason,
        )
    elif kind in PLATFORM_MODULES:
        result = run_platform_module(
            institucion=institucion,
            instituciones_catalogo=instituciones_catalogo,
            platform_module=PLATFORM_MODULES[kind],
            dry_run=dry_run,
            max_results=max_results,
        )
    else:
        result = ResultadoEjecucion(
            nombre=display_name(institucion),
            status="SKIP",
            detalle=f"kind no soportado: {kind.value}",
        )
    result.kind = kind.value
    return result


def run_empleos_publicos(
    instituciones: list[dict[str, Any]],
    dry_run: bool,
    max_results: int | None,
) -> ResultadoEjecucion:
    try:
        scraper = EmpleosPublicosScraper(
            instituciones=instituciones,
            dry_run=dry_run,
            max_results=max_results,
            strict_institution_match=True,
        )
        stats = scraper.run()
        result = result_from_stats("empleospublicos.cl", stats)
        result.kind = ScraperKind.EMPLEOS_PUBLICOS.value
        return result
    except Exception as exc:
        logger.exception("evento=run_ep_error error=%s", exc)
        return ResultadoEjecucion(
            nombre="empleospublicos.cl",
            status="ERR",
            errores=1,
            detalle=str(exc),
            kind=ScraperKind.EMPLEOS_PUBLICOS.value,
        )


def run_wordpress(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]],
    dry_run: bool,
    max_results: int | None,
) -> ResultadoEjecucion:
    try:
        scraper = WordPressScraper(
            institucion=institucion,
            instituciones_catalogo=instituciones_catalogo,
            dry_run=dry_run,
            max_results=max_results,
        )
        stats = scraper.run()
        return result_from_stats(display_name(institucion), stats)
    except Exception as exc:
        logger.exception(
            "evento=run_wordpress_error institucion=%s error=%s",
            institucion.get("nombre"),
            exc,
        )
        return ResultadoEjecucion(
            nombre=display_name(institucion),
            status="ERR",
            errores=1,
            detalle=str(exc),
        )


def run_generic_site(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]],
    dry_run: bool,
    max_results: int | None,
    mode: str = "production",
    detalle: str = "",
) -> ResultadoEjecucion:
    try:
        scraper = GenericSiteScraper(
            institucion=institucion,
            instituciones_catalogo=instituciones_catalogo,
            dry_run=dry_run,
            max_results=max_results,
            mode=mode,
        )
        stats = scraper.run()
        result = result_from_stats(display_name(institucion), stats)
        if detalle and not result.detalle:
            result.detalle = detalle
        return result
    except Exception as exc:
        logger.exception(
            "evento=run_generic_error institucion=%s error=%s",
            institucion.get("nombre"),
            exc,
        )
        return ResultadoEjecucion(
            nombre=display_name(institucion),
            status="ERR",
            errores=1,
            detalle=str(exc),
        )


def run_platform_module(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]],
    platform_module: str,
    dry_run: bool,
    max_results: int | None,
) -> ResultadoEjecucion:
    try:
        module = importlib.import_module(platform_module)
    except ModuleNotFoundError:
        return ResultadoEjecucion(
            nombre=display_name(institucion),
            status="SKIP",
            detalle=f"modulo no disponible: {platform_module}",
        )

    if not hasattr(module, "ejecutar"):
        return ResultadoEjecucion(
            nombre=display_name(institucion),
            status="SKIP",
            detalle=f"modulo sin ejecutar(): {platform_module}",
        )

    try:
        stats = module.ejecutar(
            institucion=institucion,
            instituciones_catalogo=instituciones_catalogo,
            dry_run=dry_run,
            max_results=max_results,
        )
        return result_from_stats(display_name(institucion), stats)
    except Exception as exc:
        logger.exception(
            "evento=run_platform_error institucion=%s modulo=%s error=%s",
            institucion.get("nombre"),
            platform_module,
            exc,
        )
        return ResultadoEjecucion(
            nombre=display_name(institucion),
            status="ERR",
            errores=1,
            detalle=str(exc),
        )


# ────────────────────────── Helpers varios ────────────────────────────

def result_from_stats(nombre: str, stats: dict[str, Any]) -> ResultadoEjecucion:
    status = stats.get("status") or "OK"
    if status == "ERROR":
        status = "ERR"
    return ResultadoEjecucion(
        nombre=nombre,
        status=status,
        found=stats.get("found", 0),
        nuevas=stats.get("nuevas", 0),
        actualizadas=stats.get("actualizadas", 0),
        cerradas=stats.get("cerradas", 0),
        errores=stats.get("errores", 0),
        detalle=stats.get("detalle", ""),
        duracion=float(stats.get("duracion_seg") or 0.0),
    )


def display_name(institucion: dict[str, Any]) -> str:
    return clean_text(institucion.get("nombre")) or f"institucion_{institucion.get('id')}"


# ────────────────────────── Resumen pre-run ───────────────────────────

def print_pre_run_summary(
    total_catalogo: int,
    status_counts: dict[str, int],
    kind_counts: dict[str, int],
    runnable_count: int,
    allowed: set[SourceStatus],
    mode: str,
    only_kind: str | None,
) -> None:
    print("=" * 70)
    print(f"CATÁLOGO: {total_catalogo} instituciones")
    print("-" * 70)
    print("Clasificación por status:")
    for status in SourceStatus:
        count = status_counts.get(status.value, 0)
        mark = "→" if status in allowed else " "
        print(f"  {mark} {status.value:<16} {count:>4}")
    print("-" * 70)
    print("Clasificación por kind:")
    for kind, count in sorted(kind_counts.items(), key=lambda kv: -kv[1]):
        if count:
            print(f"    {kind:<20} {count:>4}")
    print("-" * 70)
    print(
        f"Se ejecutarán {runnable_count} fuentes por-sitio "
        f"(mode={mode}, only_kind={only_kind or '-'})"
    )
    print("=" * 70)


def print_classification_detail(
    enriched: list[tuple[dict[str, Any], SourceDecision]],
    allowed: set[SourceStatus],
) -> None:
    for inst, dec in enriched:
        mark = "RUN " if dec.status in allowed else "SKIP"
        nombre = clean_text(inst.get("nombre"))[:45]
        print(
            f"{mark} [{dec.status.value:<14}] [{dec.kind.value:<18}] "
            f"id={inst.get('id')} {nombre} — {dec.reason}"
        )


# ────────────────────────── Resumen final ─────────────────────────────

def print_summary(resultados: list[ResultadoEjecucion], elapsed: float) -> None:
    total_nuevas = sum(item.nuevas for item in resultados)
    total_actualizadas = sum(item.actualizadas for item in resultados)
    total_cerradas = sum(item.cerradas for item in resultados)
    total_found = sum(item.found for item in resultados)

    by_status: dict[str, int] = {}
    con_datos = 0
    sin_datos = 0
    con_error = 0

    for item in resultados:
        by_status[item.status] = by_status.get(item.status, 0) + 1
        if item.status == "ERR":
            con_error += 1
        elif item.found > 0:
            con_datos += 1
        else:
            sin_datos += 1

    status_symbol = {"OK": "OK  ", "PARCIAL": "WARN", "ERR": "ERR ", "SKIP": "SKIP"}
    mostrados = [item for item in resultados if item.status != "SKIP"]
    skips = [item for item in resultados if item.status == "SKIP"]

    for item in mostrados:
        detalle = f" | {item.detalle}" if item.detalle else ""
        symbol = status_symbol.get(item.status, item.status)
        duracion = f" {item.duracion:>5.1f}s" if item.duracion else "       "
        print(
            f"{symbol} {item.nombre[:32]:<32} -> {item.found:>4} ofertas "
            f"({item.errores} err){duracion}{detalle[:50]}"
        )

    if skips:
        resumen_skips: dict[str, int] = {}
        for item in skips:
            clave = item.detalle or "skip"
            resumen_skips[clave] = resumen_skips.get(clave, 0) + 1
        for detalle, cantidad in sorted(
            resumen_skips.items(), key=lambda entry: (-entry[1], entry[0])
        ):
            print(f"SKIP  {cantidad:>3} instituciones | {detalle}")

    print("-" * 70)
    print(
        f"Resumen: {con_datos} con datos | {sin_datos} sin datos | "
        f"{con_error} con error | {len(skips)} skip"
    )
    print(
        f"Totales:  {total_found} ofertas encontradas | {total_nuevas} nuevas | "
        f"{total_actualizadas} actualizadas | {total_cerradas} cerradas"
    )
    print(f"Duración: {elapsed:.2f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
