"""Endpoints de administración del panel de gestión (bajo `ADMIN_PATH`).

Todos requieren un JWT válido obtenido en `/api/{ADMIN_PATH}/auth/login`
(router en `api/routers/auth.py`). El prefijo secreto `ADMIN_PATH` es
defense-in-depth — la barrera principal es el JWT.

Incluye:

- Dashboard: `/stats`, `/diagnostico`, `/evaluaciones`, `/revision`.
- Ofertas: listado/edit/toggle/bulk-desactivar/export/marcar-revisada.
- Fuentes/Instituciones: CRUD + scraper/catalog + scraper/run.
- Config del sitio: `/config` GET/PUT.
- URLs: `/urls/revalidar`.
- Suscripciones a alertas: listado/delete/export/enviar/test-email.
- Meilisearch: `/meilisearch/reindexar`.

### Notas de consolidación

El refactor detectó un **endpoint duplicado** en el `main.py`
pre-refactor: `POST /api/{ADMIN_PATH}/alertas/enviar` estaba registrado
dos veces (líneas ~440 y ~1612). FastAPI atendía sólo al primero (sin
payload, dispara broadcast automático); el segundo (con payload JSON)
era **código muerto** desde el PR #149 cuando se añadió la ruta
pública al prefix admin. En este PR se consolida: mantiene el
comportamiento actual (primer registro) y elimina el segundo.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import subprocess
import sys
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel

from api.deps import (
    ADMIN_PATH,
    SITE_URL,
    _PROJECT_ROOT,
    verify_admin_jwt as _verify_admin_jwt,
)
from api.services.db import (
    DB_CONFIG,
    execute_fetch_all,
    execute_fetch_one,
    get_cursor,
    _coalesce_present,
    _table_columns,
)
from api.services.formatters import (
    _slugify,
    _truncate_text,
    validate_email,
)
from api.services.sql import (
    ACTIVE_OFFER_SQL,
    OFFER_STATUS_SQL,
    STATUS_LEGACY_MAP,
    build_ofertas_filters,
    ofertas_base_sql,
    ofertas_select_sql,
)
from api.services.seo import serialize_offer
from api.services.regiones import get_comunas, get_regiones
from api.services.email_alerts import enviar_alerta_ofertas, enviar_verificacion
from api.services.meilisearch_svc import (
    autocompletar as meili_autocompletar,
    buscar as meili_buscar,
    configurar_indice as meili_configurar,
    indexar_ofertas as meili_indexar,
)

# `scrapers.source_status` opcional (tests / entornos minimal)
try:
    from scrapers.source_status import (  # type: ignore[import]
        SourceStatus,
        classify_source,
        enrich_with_status,
        kind_breakdown,
        status_breakdown,
    )
    _SOURCE_STATUS_AVAILABLE = True
except Exception:  # pragma: no cover
    _SOURCE_STATUS_AVAILABLE = False

logger = logging.getLogger("api.routers.admin")

router = APIRouter(tags=["admin"])


@router.post(f"/api/{ADMIN_PATH}/meilisearch/reindexar", tags=["admin"])
def api_reindexar_meili(_user: str = Depends(_verify_admin_jwt)) -> dict[str, Any]:
    """Re-indexa todas las ofertas activas en Meilisearch.

    Movido bajo el prefijo admin: antes era público y permitía gatillar
    una re-indexación de 10 000 ofertas sin autenticación (abuso de
    recursos y cuota de Meilisearch).
    """
    ofertas = execute_fetch_all(
        f"""
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        WHERE {ACTIVE_OFFER_SQL}
        ORDER BY o.id
        LIMIT 10000
        """
    )
    if not ofertas:
        return {"ok": False, "mensaje": "No hay ofertas para indexar"}

    meili_configurar()
    ok = meili_indexar(ofertas)
    return {
        "ok": ok,
        "indexadas": len(ofertas) if ok else 0,
        "mensaje": f"{len(ofertas)} ofertas indexadas" if ok else "Error al indexar",
    }


# ──────────────────── Alertas mejoradas con Resend ──────────────────────────

@router.post(f"/api/{ADMIN_PATH}/alertas/enviar", tags=["admin"])
def api_enviar_alertas_pendientes(
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Procesa y envía alertas pendientes a los suscriptores.
    Busca ofertas nuevas (últimas 24h) que coincidan con los filtros
    de cada suscriptor y les envía un email via Resend.

    Movido bajo el prefijo admin: antes era público y permitía gatillar
    envío masivo de emails vía Resend sin autenticación (abuso de
    cuota + spam a toda la lista de suscriptores).
    """
    suscripciones = execute_fetch_all(
        "SELECT * FROM alertas_suscripciones WHERE activa = TRUE"
    )
    if not suscripciones:
        return {"ok": True, "enviados": 0, "mensaje": "Sin suscripciones activas"}

    enviados = 0
    errores = 0

    for sub in suscripciones:
        where_parts = [ACTIVE_OFFER_SQL]
        params: list[Any] = []

        # Only offers from last 24h
        where_parts.append(
            "COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) >= NOW() - INTERVAL '24 hours'"
        )

        if sub.get("region"):
            where_parts.append("COALESCE(o.region, i.region, '') ILIKE %s")
            params.append(f"%{sub['region']}%")
        if sub.get("termino"):
            where_parts.append("(o.cargo ILIKE %s OR COALESCE(o.descripcion, '') ILIKE %s)")
            params.extend([f"%{sub['termino']}%", f"%{sub['termino']}%"])
        if sub.get("tipo_contrato"):
            where_parts.append("COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) ILIKE %s")
            params.append(f"%{sub['tipo_contrato']}%")
        if sub.get("sector"):
            where_parts.append("COALESCE(i.sector, o.sector, i.tipo, '') ILIKE %s")
            params.append(f"%{sub['sector']}%")

        where_sql = " AND ".join(where_parts)
        ofertas = execute_fetch_all(
            f"""
            {ofertas_select_sql()}
            {ofertas_base_sql()}
            WHERE {where_sql}
            ORDER BY fecha_scraped DESC NULLS LAST
            LIMIT 20
            """,
            params,
        )

        if ofertas:
            result = enviar_alerta_ofertas(
                email=sub["email"],
                ofertas=ofertas,
                filtros={
                    "region": sub.get("region"),
                    "termino": sub.get("termino"),
                    "tipo_contrato": sub.get("tipo_contrato"),
                    "sector": sub.get("sector"),
                },
            )
            if result.get("ok"):
                enviados += 1
            else:
                errores += 1

    return {
        "ok": True,
        "total_suscripciones": len(suscripciones),
        "enviados": enviados,
        "errores": errores,
    }




@router.get(f"/api/{ADMIN_PATH}/stats", tags=["admin"])
def admin_stats(_user: str = Depends(_verify_admin_jwt)) -> dict[str, Any]:
    """Métricas completas para el dashboard de administración."""
    totales = execute_fetch_one("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE activa = TRUE)  AS activas,
            COUNT(*) FILTER (WHERE activa = FALSE) AS inactivas,
            COUNT(*) FILTER (WHERE url_oferta_valida = FALSE) AS urls_rotas,
            COUNT(*) FILTER (WHERE url_oferta_valida IS NULL)  AS urls_sin_validar,
            COUNT(*) FILTER (
                WHERE COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en)
                      >= NOW() - INTERVAL '24 hours'
            ) AS nuevas_24h,
            COUNT(*) FILTER (
                WHERE fecha_cierre IS NOT NULL AND fecha_cierre < CURRENT_DATE AND activa = TRUE
            ) AS activas_vencidas
        FROM ofertas
    """) or {}

    por_sector = execute_fetch_all(f"""
        SELECT COALESCE(i.sector, 'Sin sector') AS sector, COUNT(*) AS total
        {ofertas_base_sql()}
        WHERE {ACTIVE_OFFER_SQL}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)

    # La tabla scraper_runs varió de schema: versiones antiguas tenían
    # `ejecutado_en` / `total_encontradas`; las recientes los renombraron a
    # `started_at` / `total_nuevas`. Postgres no permite referenciar columnas
    # inexistentes ni dentro de COALESCE, así que construimos la query
    # solo con las columnas presentes en el schema actual.
    runs_cols = _table_columns("scraper_runs")
    started_expr = _coalesce_present(runs_cols, ("started_at", "ejecutado_en"))
    nuevas_expr = _coalesce_present(runs_cols, ("total_nuevas", "total_encontradas"), default="0")
    scraper_runs = execute_fetch_all(f"""
        SELECT id,
               {started_expr} AS started_at,
               COALESCE(status, CASE WHEN duracion_segundos IS NOT NULL THEN 'completado' ELSE NULL END) AS status,
               COALESCE(total_instituciones, 0) AS total_instituciones,
               COALESCE(total_evaluadas, 0) AS total_evaluadas,
               COALESCE(total_extract, 0) AS total_extract,
               {nuevas_expr} AS total_nuevas,
               COALESCE(total_actualizadas, 0) AS total_actualizadas,
               COALESCE(total_errores, 0) AS total_errores,
               tasa_precision, duracion_segundos
        FROM scraper_runs
        ORDER BY {started_expr} DESC NULLS LAST
        LIMIT 10
    """)

    eval_resumen = execute_fetch_one("""
        SELECT
            COUNT(DISTINCT institucion_id) AS instituciones_evaluadas,
            COUNT(*) FILTER (WHERE decision = 'extract')       AS extract,
            COUNT(*) FILTER (WHERE decision = 'skip')          AS skip,
            COUNT(*) FILTER (WHERE decision = 'manual_review') AS manual_review,
            MAX(evaluated_at) AS ultima_evaluacion
        FROM source_evaluations
    """) or {}

    url_validez = execute_fetch_one("""
        SELECT
            COUNT(*) FILTER (WHERE url_oferta_valida = TRUE)  AS validas,
            COUNT(*) FILTER (WHERE url_oferta_valida = FALSE) AS rotas,
            COUNT(*) FILTER (WHERE url_oferta_valida IS NULL) AS sin_validar,
            MAX(url_valida_chequeada_en) AS ultimo_chequeo
        FROM ofertas WHERE activa = TRUE
    """) or {}

    return {
        "totales": totales,
        "por_sector": por_sector,
        "scraper_runs": scraper_runs,
        "evaluaciones": eval_resumen,
        "url_validez": url_validez,
    }


@router.get(f"/api/{ADMIN_PATH}/ofertas", tags=["admin"])
def admin_ofertas(
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(50, ge=1, le=200),
    activa: str | None = Query(None, description="true/false/all"),
    url_rota: bool | None = Query(None),
    sector: str | None = Query(None),
    region: str | None = Query(None),
    q: str | None = Query(None),
    orden: str = Query("reciente", description="reciente|cierre|cargo"),
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Lista paginada de ofertas con filtros para revisión."""
    conditions = []
    params: list[Any] = []

    if activa == "true":
        conditions.append("o.activa = TRUE")
    elif activa == "false":
        conditions.append("o.activa = FALSE")

    if url_rota is True:
        conditions.append("o.url_oferta_valida = FALSE")
    elif url_rota is False:
        conditions.append("(o.url_oferta_valida = TRUE OR o.url_oferta_valida IS NULL)")

    if sector:
        conditions.append("i.sector = %s")
        params.append(sector)

    if region:
        conditions.append("o.region ILIKE %s")
        params.append(f"%{region}%")

    if q:
        conditions.append(
            "(o.cargo ILIKE %s OR o.institucion_nombre ILIKE %s OR i.nombre ILIKE %s)"
        )
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    order_sql = {
        "reciente": "COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en) DESC NULLS LAST",
        "cierre": "o.fecha_cierre ASC NULLS LAST",
        "cargo": "o.cargo ASC",
    }.get(orden, "COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en) DESC NULLS LAST")

    offset = (pagina - 1) * por_pagina

    sql = f"""
        SELECT
            o.id, o.cargo,
            o.institucion_nombre,
            COALESCE(NULLIF(TRIM(o.institucion_nombre),''), i.nombre, 'Sin institución') AS institucion_display,
            COALESCE(i.sigla, i.nombre_corto) AS inst_sigla,
            o.region, o.sector,
            COALESCE(i.sector, o.sector, 'Sin sector') AS sector_real,
            o.tipo_contrato, o.fecha_cierre, o.fecha_publicacion,
            o.activa, o.estado, o.url_oferta, o.url_oferta_valida,
            o.url_bases, o.url_bases_valida,
            o.renta_bruta_min, o.renta_bruta_max,
            o.fecha_scraped, o.detectada_en,
            o.institucion_id, o.descripcion,
            o.overall_quality_score, o.needs_review,
            i.sector AS inst_sector, i.nombre AS inst_nombre_catalogo,
            i.url_empleo AS inst_url_empleo
        {ofertas_base_sql()}
        {where_clause}
        ORDER BY {order_sql}
        LIMIT %s OFFSET %s
    """
    count_sql = f"""
        SELECT COUNT(*) AS total
        {ofertas_base_sql()}
        {where_clause}
    """

    rows = execute_fetch_all(sql, params + [por_pagina, offset])
    total_row = execute_fetch_one(count_sql, params) or {}
    total = int(total_row.get("total") or 0)

    return {
        "total": total,
        "pagina": pagina,
        "por_pagina": por_pagina,
        "paginas": math.ceil(total / por_pagina) if total else 0,
        "ofertas": rows,
    }


@router.post(f"/api/{ADMIN_PATH}/ofertas/{{oferta_id}}/toggle-activa", tags=["admin"])
def admin_toggle_activa(
    oferta_id: int,
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Activa o desactiva una oferta."""
    with get_cursor() as (conn, cur):
        cur.execute("SELECT activa FROM ofertas WHERE id = %s", [oferta_id])
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Oferta no encontrada")
        nuevo_estado = not (row["activa"] if isinstance(row, dict) else row[0])
        cur.execute(
            "UPDATE ofertas SET activa = %s, actualizada_en = NOW() WHERE id = %s",
            [nuevo_estado, oferta_id],
        )
        conn.commit()
    return {"id": oferta_id, "activa": nuevo_estado}


@router.put(f"/api/{ADMIN_PATH}/ofertas/{{oferta_id}}", tags=["admin"])
def admin_editar_oferta(
    oferta_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Edita campos básicos de una oferta (cargo, descripcion, fecha_cierre, activa)."""
    CAMPOS_PERMITIDOS = {"cargo", "descripcion", "fecha_cierre", "activa", "estado", "region", "tipo_contrato"}
    updates = {k: v for k, v in payload.items() if k in CAMPOS_PERMITIDOS}
    if not updates:
        raise HTTPException(400, "Sin campos válidos para actualizar")

    set_clause = ", ".join(f"{col} = %s" for col in updates)
    vals = list(updates.values()) + [oferta_id]

    with get_cursor() as (conn, cur):
        cur.execute(
            f"UPDATE ofertas SET {set_clause}, actualizada_en = NOW() WHERE id = %s",
            vals,
        )
        if cur.rowcount == 0:
            raise HTTPException(404, "Oferta no encontrada")
        conn.commit()

    return {"id": oferta_id, "updated": list(updates.keys())}


@router.get(f"/api/{ADMIN_PATH}/scraper-runs", tags=["admin"])
def admin_scraper_runs(
    limit: int = Query(20, ge=1, le=100),
    con_detalle: bool = Query(False, description="Incluir resumen por institución"),
    _user: str = Depends(_verify_admin_jwt),
) -> list[dict[str, Any]]:
    """Historial de corridas del scraper con detalle."""
    runs_cols = _table_columns("scraper_runs")
    started_expr = _coalesce_present(runs_cols, ("started_at", "ejecutado_en"))
    nuevas_expr = _coalesce_present(runs_cols, ("total_nuevas", "total_encontradas"), default="0")
    vencidas_expr = _coalesce_present(runs_cols, ("total_vencidas", "total_cerradas"), default="0")
    rows = execute_fetch_all(f"""
        SELECT id,
               {started_expr} AS started_at,
               finished_at,
               COALESCE(status, CASE WHEN duracion_segundos IS NOT NULL THEN 'completado' ELSE NULL END) AS status,
               COALESCE(run_mode, 'batch') AS run_mode,
               COALESCE(total_instituciones, 0) AS total_instituciones,
               COALESCE(total_evaluadas, 0) AS total_evaluadas,
               COALESCE(total_extract, 0) AS total_extract,
               COALESCE(total_skip, 0) AS total_skip,
               {nuevas_expr} AS total_nuevas,
               COALESCE(total_actualizadas, 0) AS total_actualizadas,
               {vencidas_expr} AS total_vencidas,
               COALESCE(total_descartadas, 0) AS total_descartadas,
               COALESCE(total_errores, 0) AS total_errores,
               tasa_precision, duracion_segundos, notas, detalle
        FROM scraper_runs
        ORDER BY {started_expr} DESC NULLS LAST
        LIMIT %s
    """, [limit])

    # Extraer resumen por institución del JSONB si se pide
    for row in rows:
        detalle = row.pop("detalle", None) or {}
        if con_detalle and detalle and isinstance(detalle, dict):
            reports = detalle.get("reports") or {}
            instituciones_resumen = []
            for nombre, rep in reports.items():
                if isinstance(rep, dict):
                    instituciones_resumen.append({
                        "nombre": nombre,
                        "encontradas": rep.get("total_encontradas", 0),
                        "nuevas": rep.get("guardadas", 0),
                        "existian": rep.get("ya_existian", 0),
                        "errores": rep.get("errores", 0),
                    })
            # Ordenar: primero las que tienen nuevas, luego por nombre
            instituciones_resumen.sort(key=lambda x: (-x["nuevas"], x["nombre"]))
            row["instituciones"] = instituciones_resumen
        else:
            row["instituciones"] = None

    return rows


@router.get(f"/api/{ADMIN_PATH}/scraper-runs/{{run_id}}", tags=["admin"])
def admin_scraper_run_detalle(
    run_id: int,
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Detalle completo de una corrida del scraper, incluyendo reporte por institución."""
    runs_cols = _table_columns("scraper_runs")
    started_expr = _coalesce_present(runs_cols, ("started_at", "ejecutado_en"))
    row = execute_fetch_one(f"""
        SELECT id,
               {started_expr} AS started_at,
               finished_at, status, run_mode,
               total_instituciones, total_evaluadas, total_extract, total_skip,
               total_nuevas, total_actualizadas, total_vencidas, total_descartadas,
               total_errores, tasa_precision, duracion_segundos, notas, detalle
        FROM scraper_runs WHERE id = %s
    """, [run_id])

    if not row:
        raise HTTPException(404, "Corrida no encontrada")

    detalle = row.pop("detalle", None) or {}
    reports = (detalle.get("reports") or {}) if isinstance(detalle, dict) else {}
    instituciones = []
    for nombre, rep in reports.items():
        if isinstance(rep, dict):
            instituciones.append({
                "nombre": nombre,
                "encontradas": rep.get("total_encontradas", 0),
                "nuevas": rep.get("guardadas", 0),
                "existian": rep.get("ya_existian", 0),
                "errores": rep.get("errores", 0),
            })
    instituciones.sort(key=lambda x: (-x["nuevas"], x["nombre"]))
    row["instituciones"] = instituciones
    row["total_con_detalle"] = len(instituciones)
    return row


@router.get(f"/api/{ADMIN_PATH}/evaluaciones", tags=["admin"])
def admin_evaluaciones(
    decision: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    _user: str = Depends(_verify_admin_jwt),
) -> list[dict[str, Any]]:
    """Última evaluación por institución (gatekeeper)."""
    where = "WHERE e.decision = %s" if decision else ""
    params = [decision] if decision else []
    params.append(limit)
    return execute_fetch_all(f"""
        SELECT DISTINCT ON (e.institucion_id)
            e.institucion_id,
            COALESCE(i.nombre, e.source_url) AS nombre,
            i.sector,
            e.source_url,
            e.decision,
            e.recommended_extractor,
            e.open_calls_status,
            e.retry_policy,
            e.confidence,
            e.reason_detail,
            e.availability,
            e.http_status,
            e.evaluated_at
        FROM source_evaluations e
        LEFT JOIN instituciones i ON i.id = e.institucion_id
        {where}
        ORDER BY e.institucion_id, e.evaluated_at DESC
        LIMIT %s
    """, params)


@router.get(f"/api/{ADMIN_PATH}/fuentes", tags=["admin"])
def admin_fuentes(
    con_ofertas: bool | None = Query(None),
    sector: str | None = Query(None),
    _user: str = Depends(_verify_admin_jwt),
) -> list[dict[str, Any]]:
    """Instituciones con su última evaluación y conteo de ofertas activas."""
    conditions = []
    params: list[Any] = []
    if con_ofertas is True:
        conditions.append("oferta_count > 0")
    elif con_ofertas is False:
        conditions.append("oferta_count = 0")
    if sector:
        conditions.append("i.sector = %s")
        params.append(sector)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    return execute_fetch_all(f"""
        SELECT
            i.id, i.nombre, i.sigla, i.sector, i.region,
            i.url_empleo, i.plataforma_empleo,
            COALESCE(ev.decision, 'sin_evaluar') AS ultima_decision,
            ev.recommended_extractor,
            ev.retry_policy,
            ev.confidence,
            ev.evaluated_at AS ultima_evaluacion,
            ev.availability,
            ev.http_status,
            oferta_count
        FROM instituciones i
        LEFT JOIN LATERAL (
            SELECT decision, recommended_extractor, retry_policy, confidence,
                   evaluated_at, availability, http_status
            FROM source_evaluations
            WHERE institucion_id = i.id
            ORDER BY evaluated_at DESC
            LIMIT 1
        ) ev ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS oferta_count
            FROM ofertas o
            WHERE o.institucion_id = i.id AND o.activa = TRUE
        ) oc ON TRUE
        {where}
        ORDER BY oferta_count DESC NULLS LAST, i.nombre ASC
    """, params)


# ── Admin: ejecución manual de scrapers ──────────────────────────────────────

@router.get(f"/api/{ADMIN_PATH}/scraper/catalog", tags=["admin"])
def admin_scraper_catalog(
    _user: str = Depends(_verify_admin_jwt),
) -> list[dict[str, Any]]:
    """Lista instituciones del catálogo con su clasificación (source_status)."""
    try:
        import json as _json
        catalog_path = _PROJECT_ROOT / "repositorio_instituciones_publicas_chile.json"
        if not catalog_path.exists():
            return []
        with open(catalog_path, encoding="utf-8") as f:
            instituciones = _json.load(f)
        if not _SOURCE_STATUS_AVAILABLE:
            return [
                {"id": i.get("id"), "nombre": i.get("nombre"), "sector": i.get("sector"), "url_empleo": i.get("url_empleo")}
                for i in instituciones
            ]
        enriched = []
        for item in instituciones:  # catálogo completo (~640 instituciones)
            try:
                info = enrich_with_status(item)
                enriched.append({
                    "id": info.get("id"),
                    "nombre": info.get("nombre"),
                    "sector": info.get("sector"),
                    "url_empleo": info.get("url_empleo"),
                    "status": str(info.get("status", "")),
                    "kind": str(info.get("kind", "")),
                    "fuente_id": info.get("fuente_id"),
                })
            except Exception:
                enriched.append({
                    "id": item.get("id"), "nombre": item.get("nombre"),
                    "sector": item.get("sector"), "url_empleo": item.get("url_empleo"),
                    "status": "unknown", "kind": "unknown",
                })
        return enriched
    except Exception as exc:
        raise HTTPException(500, f"Error leyendo catálogo: {exc}") from exc


@router.post(f"/api/{ADMIN_PATH}/scraper/run", tags=["admin"])
async def admin_scraper_run(
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Dispara un scraper en background.

    Body (JSON):
      - mode: "empleos_publicos" | "institucion" | "kind"
      - institucion_id: int   (para mode=institucion)
      - kind: str             (para mode=kind, ej. "wordpress")
      - dry_run: bool         (default false)
      - max: int              (máx ofertas, default 50)
    """
    import subprocess, sys as _sys, shlex

    mode       = payload.get("mode", "empleos_publicos")
    dry_run    = bool(payload.get("dry_run", False))
    max_offers = int(payload.get("max", 50))

    # Construir comando
    python = _sys.executable
    run_all = str(_PROJECT_ROOT / "scrapers" / "run_all.py")

    cmd = [python, run_all, "--mode", "production", "--max", str(max_offers)]
    if dry_run:
        cmd.append("--dry-run")

    if mode == "empleos_publicos":
        cmd += ["--only-kind", "empleos_publicos"]
    elif mode == "institucion":
        inst_id = payload.get("institucion_id")
        if not inst_id:
            raise HTTPException(400, "institucion_id es requerido para mode=institucion")
        cmd += ["--id", str(inst_id), "--skip-empleos-publicos"]
    elif mode == "kind":
        kind = payload.get("kind", "wordpress")
        cmd += ["--only-kind", kind, "--skip-empleos-publicos"]
    else:
        raise HTTPException(400, f"mode inválido: {mode}")

    logger.info(f"[admin] scraper run: {shlex.join(cmd)}")

    # Ejecutar en background (no bloquea la respuesta)
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(_PROJECT_ROOT),
            text=True,
        )
    except Exception as exc:
        raise HTTPException(500, f"No se pudo lanzar el proceso: {exc}") from exc

    # Registrar inicio en scraper_runs
    run_id: int | None = None
    try:
        with get_cursor() as (conn, cur):
            cur.execute(
                """INSERT INTO scraper_runs (started_at, status, run_mode, notas)
                   VALUES (NOW(), 'en_curso', %s, %s)
                   RETURNING id""",
                [f"manual-{mode}", f"pid={proc.pid} dry={dry_run}"],
            )
            row = cur.fetchone()
            run_id = (row["id"] if isinstance(row, dict) else row[0]) if row else None
            conn.commit()
    except Exception:
        pass  # tabla puede no existir

    return {
        "ok": True,
        "pid": proc.pid,
        "run_id": run_id,
        "cmd": cmd[2:],  # omitir python path
        "dry_run": dry_run,
        "mode": mode,
    }


# ── Admin: configuración del sitio ───────────────────────────────────────────

# Campos del sitio editables en caliente (guardados en tabla site_config si existe,
# o en memoria como fallback para esta instancia del proceso).
_SITE_CONFIG_MEMORY: dict[str, str] = {}


def _get_site_config_db() -> dict[str, str]:
    try:
        rows = execute_fetch_all(
            "SELECT clave, valor FROM site_config ORDER BY clave", []
        )
        return {r["clave"]: r["valor"] for r in rows}
    except Exception:
        return {}


def _set_site_config_db(clave: str, valor: str) -> None:
    with get_cursor() as (conn, cur):
        cur.execute(
            """INSERT INTO site_config (clave, valor, actualizado_en)
               VALUES (%s, %s, NOW())
               ON CONFLICT (clave) DO UPDATE
               SET valor = EXCLUDED.valor, actualizado_en = NOW()""",
            [clave, valor],
        )
        conn.commit()


@router.get(f"/api/{ADMIN_PATH}/config", tags=["admin"])
def admin_get_config(
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Lee la configuración editable del sitio."""
    db_conf = _get_site_config_db()
    conf = {**_SITE_CONFIG_MEMORY, **db_conf}
    # Añadir valores de env vars relevantes (no secretos)
    env_info = {
        "SITE_URL": SITE_URL,
        "ADMIN_PATH_set": bool(os.getenv("ADMIN_PATH")),
        "ADMIN_PASSWORD_set": bool(os.getenv("ADMIN_PASSWORD")),
        "MEILISEARCH_URL_set": bool(os.getenv("MEILISEARCH_URL")),
        "RESEND_API_KEY_set": bool(os.getenv("RESEND_API_KEY")),
    }
    return {"config": conf, "env": env_info}


@router.put(f"/api/{ADMIN_PATH}/config", tags=["admin"])
def admin_set_config(
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Actualiza configuración editable del sitio.

    Claves soportadas: banner_mensaje, banner_activo, mantenimiento,
    max_resultados_pagina, alertas_activas, footer_extra.
    """
    CLAVES_PERMITIDAS = {
        "banner_mensaje", "banner_activo", "mantenimiento",
        "max_resultados_pagina", "alertas_activas", "footer_extra",
    }
    updated: list[str] = []
    for clave, valor in payload.items():
        if clave not in CLAVES_PERMITIDAS:
            continue
        val_str = str(valor)
        _SITE_CONFIG_MEMORY[clave] = val_str
        try:
            _set_site_config_db(clave, val_str)
        except Exception:
            pass  # si no existe la tabla, solo en memoria
        updated.append(clave)
    if not updated:
        raise HTTPException(400, f"Sin claves válidas. Permitidas: {sorted(CLAVES_PERMITIDAS)}")
    return {"updated": updated}


# ── Admin: acciones sobre ofertas (bulk) ─────────────────────────────────────

@router.post(f"/api/{ADMIN_PATH}/ofertas/bulk-desactivar", tags=["admin"])
def admin_bulk_desactivar(
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Desactiva en bloque ofertas según criterios.

    Body (JSON):
      - ids: list[int]          — lista explícita de IDs
      - url_rota: true          — todas las activas con URL rota
      - fecha_cierre_vencida: true — activas con fecha_cierre < hoy
    """
    ids: list[int] = []

    if "ids" in payload:
        ids = [int(i) for i in payload["ids"] if str(i).isdigit()]
    elif payload.get("url_rota"):
        rows = execute_fetch_all(
            "SELECT id FROM ofertas WHERE activa=TRUE AND url_oferta_valida=FALSE", []
        )
        ids = [r["id"] for r in rows]
    elif payload.get("fecha_cierre_vencida"):
        rows = execute_fetch_all(
            "SELECT id FROM ofertas WHERE activa=TRUE AND fecha_cierre < CURRENT_DATE", []
        )
        ids = [r["id"] for r in rows]

    if not ids:
        return {"desactivadas": 0, "ids": []}

    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE ofertas SET activa=FALSE, estado='cerrada', actualizada_en=NOW() WHERE id = ANY(%s)",
            [ids],
        )
        count = cur.rowcount
        conn.commit()

    logger.info(f"[admin] bulk-desactivar: {count} ofertas por {_user}")
    return {"desactivadas": count, "ids": ids[:50]}


@router.post(f"/api/{ADMIN_PATH}/urls/revalidar", tags=["admin"])
async def admin_revalidar_urls(
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Dispara revalidación de URLs en background (llama a validate_offer_urls.py).

    Body: { workers: int (default 20), max_edad_h: int (default 0 = todas) }
    """
    import subprocess, sys as _sys

    workers   = int(payload.get("workers", 20))
    max_edad  = int(payload.get("max_edad_h", 0))
    limit     = int(payload.get("limit", 2000))

    validate_script = _PROJECT_ROOT / "validate_offer_urls.py"
    if not validate_script.exists():
        raise HTTPException(404, "validate_offer_urls.py no encontrado")

    cmd = [
        _sys.executable, str(validate_script),
        "--workers", str(workers),
        "--max-edad-h", str(max_edad),
        "--limit", str(limit),
    ]
    logger.info(f"[admin] revalidar URLs: {cmd}")
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        cwd=str(_PROJECT_ROOT), text=True,
    )
    return {"ok": True, "pid": proc.pid, "workers": workers, "limit": limit}


# ── Admin: gestión de fuentes (instituciones) ────────────────────────────────

@router.get(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}", tags=["admin"])
def admin_get_fuente(
    fuente_id: int,
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Detalle de una institución con su última evaluación y últimas 10 ofertas."""
    inst = execute_fetch_one(
        "SELECT * FROM instituciones WHERE id = %s", [fuente_id]
    )
    if not inst:
        raise HTTPException(404, "Institución no encontrada")
    eval_row = execute_fetch_one(
        """SELECT decision, recommended_extractor, retry_policy, confidence,
                  reason_detail, availability, http_status, open_calls_status, evaluated_at
           FROM source_evaluations WHERE institucion_id = %s
           ORDER BY evaluated_at DESC LIMIT 1""",
        [fuente_id],
    )
    ofertas = execute_fetch_all(
        """SELECT id, cargo, activa, estado, fecha_cierre, url_oferta_valida, fecha_scraped
           FROM ofertas WHERE institucion_id = %s
           ORDER BY COALESCE(fecha_scraped, detectada_en) DESC NULLS LAST LIMIT 10""",
        [fuente_id],
    )
    return {"institucion": inst, "evaluacion": eval_row, "ultimas_ofertas": ofertas}


@router.put(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}", tags=["admin"])
def admin_editar_fuente(
    fuente_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Edita campos de una institución.

    Campos permitidos: nombre, sigla, sector, region, url_empleo, plataforma_empleo,
    activa, notas_admin.
    """
    CAMPOS = {"nombre", "sigla", "sector", "region", "url_empleo",
              "plataforma_empleo", "activa", "notas_admin"}
    updates = {k: v for k, v in payload.items() if k in CAMPOS}
    if not updates:
        raise HTTPException(400, "Sin campos válidos")
    set_clause = ", ".join(f"{c} = %s" for c in updates)
    vals = list(updates.values()) + [fuente_id]
    with get_cursor() as (conn, cur):
        cur.execute(
            f"UPDATE instituciones SET {set_clause} WHERE id = %s",
            vals,
        )
        if cur.rowcount == 0:
            raise HTTPException(404, "Institución no encontrada")
        conn.commit()
    return {"id": fuente_id, "updated": list(updates.keys())}


@router.post(f"/api/{ADMIN_PATH}/fuentes", tags=["admin"])
def admin_crear_fuente(
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Crea una nueva institución en el catálogo interno.

    Campos: nombre* (requerido), sigla, sector, region, url_empleo, plataforma_empleo.
    También crea una entrada en source_overrides.json con status=experimental.
    """
    nombre = (payload.get("nombre") or "").strip()
    if not nombre:
        raise HTTPException(400, "nombre es requerido")

    with get_cursor() as (conn, cur):
        cur.execute(
            """INSERT INTO instituciones
               (nombre, sigla, sector, region, url_empleo, plataforma_empleo)
               VALUES (%s, %s, %s, %s, %s, %s)
               RETURNING id""",
            [
                nombre,
                payload.get("sigla") or None,
                payload.get("sector") or None,
                payload.get("region") or None,
                payload.get("url_empleo") or None,
                payload.get("plataforma_empleo") or None,
            ],
        )
        row = cur.fetchone()
        new_id = (row["id"] if isinstance(row, dict) else row[0]) if row else None
        conn.commit()

    logger.info(f"[admin] nueva institución creada: {new_id} — {nombre}")
    return {"id": new_id, "nombre": nombre}


@router.delete(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}", tags=["admin"])
def admin_desactivar_fuente(
    fuente_id: int,
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Desactiva una fuente: marca todas sus ofertas activas como cerradas
    y añade la institución a source_overrides.json con status=disabled.
    """
    # Contar ofertas activas
    row = execute_fetch_one(
        "SELECT COUNT(*) AS n FROM ofertas WHERE institucion_id=%s AND activa=TRUE",
        [fuente_id],
    )
    n = int(row.get("n") or 0) if row else 0

    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE ofertas SET activa=FALSE, estado='cerrada', actualizada_en=NOW() WHERE institucion_id=%s AND activa=TRUE",
            [fuente_id],
        )
        conn.commit()

    # Añadir override en source_overrides.json
    try:
        import json as _json
        overrides_path = _PROJECT_ROOT / "scrapers" / "source_overrides.json"
        if overrides_path.exists():
            with open(overrides_path, encoding="utf-8") as f:
                overrides = _json.load(f)
        else:
            overrides = {}
        overrides[str(fuente_id)] = {"status": "disabled", "reason": f"desactivado via admin por {_user}"}
        with open(overrides_path, "w", encoding="utf-8") as f:
            _json.dump(overrides, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.warning(f"[admin] no se pudo actualizar source_overrides.json: {exc}")

    return {"id": fuente_id, "ofertas_cerradas": n}


# ── Admin: bandeja de revisión manual ────────────────────────────────────────

@router.get(f"/api/{ADMIN_PATH}/revision", tags=["admin"])
def admin_revision_queue(
    limit: int = Query(50, ge=1, le=200),
    tipo: str | None = Query(None),  # url_rota | sin_sector | calidad_baja | sin_fecha | texto_corto
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Bandeja de ofertas que requieren revisión manual, clasificadas por tipo de problema.

    Tipos: url_rota, sin_sector, calidad_baja, sin_fecha, texto_corto, duplicado_posible
    """
    categorias: dict[str, Any] = {}

    # 1. URLs rotas (activas con url_oferta_valida=FALSE)
    if not tipo or tipo == "url_rota":
        rows = execute_fetch_all(
            """SELECT id, cargo, institucion_nombre, url_oferta, url_bases,
                      url_oferta_valida, url_bases_valida, fecha_cierre,
                      url_valida_chequeada_en
               FROM ofertas
               WHERE activa=TRUE AND (url_oferta_valida=FALSE OR url_bases_valida=FALSE)
               ORDER BY url_valida_chequeada_en DESC NULLS LAST
               LIMIT %s""",
            [limit],
        )
        categorias["url_rota"] = {"total": len(rows), "items": rows}

    # 2. Sin sector asignado
    if not tipo or tipo == "sin_sector":
        rows = execute_fetch_all(
            """SELECT o.id, o.cargo, o.institucion_nombre, o.sector, o.region,
                      o.fecha_cierre, o.activa
               FROM ofertas o
               LEFT JOIN instituciones i ON i.id=o.institucion_id
               WHERE o.activa=TRUE
                 AND COALESCE(o.sector, i.sector, '') = ''
               ORDER BY COALESCE(o.fecha_scraped, o.detectada_en) DESC NULLS LAST
               LIMIT %s""",
            [limit],
        )
        categorias["sin_sector"] = {"total": len(rows), "items": rows}

    # 3. Calidad baja (overall_quality_score < 0.4 o needs_review=TRUE)
    if not tipo or tipo == "calidad_baja":
        rows = execute_fetch_all(
            """SELECT id, cargo, institucion_nombre, overall_quality_score,
                      needs_review, fecha_cierre, activa
               FROM ofertas
               WHERE activa=TRUE
                 AND (overall_quality_score < 0.4 OR needs_review=TRUE)
               ORDER BY overall_quality_score ASC NULLS FIRST
               LIMIT %s""",
            [limit],
        )
        categorias["calidad_baja"] = {"total": len(rows), "items": rows}

    # 4. Sin fecha de cierre (puede estar publicada sin límite claro)
    if not tipo or tipo == "sin_fecha":
        rows = execute_fetch_all(
            """SELECT id, cargo, institucion_nombre, sector, fecha_cierre,
                      COALESCE(fecha_scraped, detectada_en) AS detectada
               FROM ofertas
               WHERE activa=TRUE AND fecha_cierre IS NULL
               ORDER BY COALESCE(fecha_scraped, detectada_en) DESC NULLS LAST
               LIMIT %s""",
            [limit],
        )
        categorias["sin_fecha"] = {"total": len(rows), "items": rows}

    # 5. Descripción muy corta (< 80 chars) — probable extracción fallida
    if not tipo or tipo == "texto_corto":
        rows = execute_fetch_all(
            """SELECT id, cargo, institucion_nombre,
                      CHAR_LENGTH(COALESCE(descripcion,'')) AS desc_len,
                      descripcion, fecha_cierre, activa
               FROM ofertas
               WHERE activa=TRUE
                 AND CHAR_LENGTH(COALESCE(descripcion,'')) < 80
               ORDER BY CHAR_LENGTH(COALESCE(descripcion,'')) ASC
               LIMIT %s""",
            [limit],
        )
        categorias["texto_corto"] = {"total": len(rows), "items": rows}

    # Resumen de totales por categoría
    resumen = {k: v["total"] for k, v in categorias.items()}
    return {"resumen": resumen, "categorias": categorias}


@router.post(f"/api/{ADMIN_PATH}/revision/{{oferta_id}}/marcar-revisada", tags=["admin"])
def admin_marcar_revisada(
    oferta_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Marca una oferta como revisada (needs_review=FALSE) y guarda una nota opcional."""
    nota = (payload.get("nota") or "").strip()
    with get_cursor() as (conn, cur):
        cur.execute(
            """UPDATE ofertas
               SET needs_review=FALSE, actualizada_en=NOW()
               WHERE id=%s""",
            [oferta_id],
        )
        if cur.rowcount == 0:
            raise HTTPException(404, "Oferta no encontrada")
        conn.commit()
    logger.info(f"[admin] oferta {oferta_id} marcada revisada por {_user}. Nota: {nota}")
    return {"id": oferta_id, "revisada": True}


# ── Admin: diagnóstico y alertas ─────────────────────────────────────────────

@router.get(f"/api/{ADMIN_PATH}/diagnostico", tags=["admin"])
def admin_diagnostico(
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Diagnóstico completo del estado de la plataforma.
    Devuelve contadores de alertas agrupados por categoría.
    """
    resultado: dict[str, Any] = {}

    # Ofertas con problemas
    problemas = execute_fetch_one(
        """SELECT
            COUNT(*) FILTER (WHERE activa=TRUE AND url_oferta_valida=FALSE) AS url_oferta_rota,
            COUNT(*) FILTER (WHERE activa=TRUE AND url_bases_valida=FALSE) AS url_bases_rota,
            COUNT(*) FILTER (WHERE activa=TRUE AND needs_review=TRUE) AS needs_review,
            COUNT(*) FILTER (WHERE activa=TRUE AND fecha_cierre < CURRENT_DATE) AS vencidas_activas,
            COUNT(*) FILTER (WHERE activa=TRUE AND CHAR_LENGTH(COALESCE(descripcion,''))<80) AS descripcion_corta,
            COUNT(*) FILTER (WHERE activa=TRUE AND sector IS NULL AND institucion_id IS NULL) AS sin_sector,
            COUNT(*) FILTER (WHERE activa=TRUE AND fecha_cierre IS NULL) AS sin_fecha_cierre,
            COUNT(*) FILTER (
                WHERE activa=TRUE
                  AND url_valida_chequeada_en IS NOT NULL
                  AND url_valida_chequeada_en < NOW() - INTERVAL '48 hours'
            ) AS url_no_chequeada_48h
           FROM ofertas""",
        [],
    ) or {}
    resultado["ofertas"] = problemas

    # Scrapers: última corrida y estado
    ultima_corrida = execute_fetch_one(
        """SELECT started_at, status, total_nuevas, total_errores, duracion_segundos
           FROM scraper_runs ORDER BY started_at DESC NULLS LAST LIMIT 1""",
        [],
    )
    resultado["ultima_corrida"] = ultima_corrida

    # Horas desde última corrida
    if ultima_corrida and ultima_corrida.get("started_at"):
        horas_row = execute_fetch_one(
            "SELECT EXTRACT(EPOCH FROM (NOW() - %s))/3600 AS horas",
            [ultima_corrida["started_at"]],
        )
        resultado["horas_desde_ultima_corrida"] = round(float(horas_row["horas"]), 1) if horas_row else None

    # Fuentes sin evaluar o con decisión ERROR
    fuentes_problema = execute_fetch_one(
        """SELECT
            COUNT(*) FILTER (WHERE ev.decision='ERROR') AS fuentes_error,
            COUNT(*) FILTER (WHERE ev.decision IS NULL) AS fuentes_sin_evaluar,
            COUNT(*) FILTER (
                WHERE ev.evaluated_at IS NOT NULL
                  AND ev.evaluated_at < NOW() - INTERVAL '7 days'
            ) AS fuentes_eval_antigua
           FROM instituciones i
           LEFT JOIN LATERAL (
               SELECT decision, evaluated_at FROM source_evaluations
               WHERE institucion_id=i.id ORDER BY evaluated_at DESC LIMIT 1
           ) ev ON TRUE""",
        [],
    ) or {}
    resultado["fuentes"] = fuentes_problema

    # Nivel de alerta global
    alertas: list[dict[str, Any]] = []
    o = problemas
    if int(o.get("vencidas_activas") or 0) > 0:
        alertas.append({"nivel":"warning","mensaje": f"{o['vencidas_activas']} ofertas activas con fecha ya vencida","accion":"bulk-desactivar"})
    if int(o.get("url_oferta_rota") or 0) > 50:
        alertas.append({"nivel":"warning","mensaje": f"{o['url_oferta_rota']} ofertas con URL de oferta rota","accion":"revalidar-urls"})
    if int(o.get("needs_review") or 0) > 0:
        alertas.append({"nivel":"info","mensaje": f"{o['needs_review']} ofertas pendientes de revisión manual","accion":"revision"})
    if resultado.get("horas_desde_ultima_corrida") and resultado["horas_desde_ultima_corrida"] > 26:
        alertas.append({"nivel":"error","mensaje": f"Última corrida hace {resultado['horas_desde_ultima_corrida']:.0f}h — posible falla del timer","accion":"scraper-runs"})
    if int(fuentes_problema.get("fuentes_error") or 0) > 10:
        alertas.append({"nivel":"warning","mensaje": f"{fuentes_problema['fuentes_error']} fuentes con decisión ERROR","accion":"evaluaciones"})

    resultado["alertas"] = alertas
    resultado["nivel_global"] = (
        "error"   if any(a["nivel"]=="error"   for a in alertas) else
        "warning" if any(a["nivel"]=="warning" for a in alertas) else
        "ok"
    )
    return resultado


# ── Admin: gestión de suscripciones y alertas por email ──────────────────────

@router.get(f"/api/{ADMIN_PATH}/suscripciones", tags=["admin"])
def admin_suscripciones(
    activa: bool | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Lista de suscriptores de alertas con estadísticas."""
    where = ""
    params: list[Any] = []
    if activa is not None:
        where = "WHERE activa = %s"
        params = [activa]

    subs = execute_fetch_all(
        f"""SELECT id, email, region, termino, tipo_contrato, sector,
                   frecuencia, activa, creada_en, actualizada_en
            FROM alertas_suscripciones
            {where}
            ORDER BY creada_en DESC
            LIMIT %s""",
        params + [limit],
    )
    resumen = execute_fetch_one(
        """SELECT
            COUNT(*)                     AS total,
            COUNT(*) FILTER(WHERE activa)                     AS activas,
            COUNT(*) FILTER(WHERE NOT activa)                 AS inactivas,
            COUNT(DISTINCT LOWER(email))                      AS emails_unicos,
            COUNT(*) FILTER(WHERE region IS NOT NULL)         AS con_region,
            COUNT(*) FILTER(WHERE termino IS NOT NULL)        AS con_termino,
            COUNT(*) FILTER(WHERE sector IS NOT NULL)         AS con_sector
           FROM alertas_suscripciones""",
        [],
    ) or {}
    return {"resumen": resumen, "suscripciones": subs}


@router.delete(f"/api/{ADMIN_PATH}/suscripciones/{{sub_id}}", tags=["admin"])
def admin_eliminar_suscripcion(
    sub_id: int,
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Elimina (o desactiva) una suscripción."""
    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE alertas_suscripciones SET activa=FALSE, actualizada_en=NOW() WHERE id=%s",
            [sub_id],
        )
        if cur.rowcount == 0:
            raise HTTPException(404, "Suscripción no encontrada")
        conn.commit()
    return {"id": sub_id, "desactivada": True}


@router.post(f"/api/{ADMIN_PATH}/alertas/test-email", tags=["admin"])
def admin_test_email(
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Envía un email de prueba con las últimas 3 ofertas activas.

    Body: { email: str (requerido) }
    """
    from api.services.email_alerts import enviar_alerta_ofertas as _enviar

    email = (payload.get("email") or "").strip()
    if not email:
        raise HTTPException(400, "email requerido")

    ofertas = execute_fetch_all(
        f"""{ofertas_select_sql()}
            {ofertas_base_sql()}
            WHERE {ACTIVE_OFFER_SQL}
            ORDER BY COALESCE(o.fecha_scraped, o.detectada_en) DESC NULLS LAST
            LIMIT 3""",
        [],
    )
    if not ofertas:
        raise HTTPException(404, "No hay ofertas activas para el email de prueba")

    result = _enviar(email=email, ofertas=ofertas, filtros={"_test": True})
    return {"ok": result.get("ok"), "email": email, "ofertas": len(ofertas), "resend_id": result.get("id"), "error": result.get("error")}


@router.get(f"/api/{ADMIN_PATH}/suscripciones/export", tags=["admin"])
def admin_export_suscripciones(
    _user: str = Depends(_verify_admin_jwt),
) -> Response:
    """Exporta suscripciones activas como CSV."""
    import csv, io
    subs = execute_fetch_all(
        "SELECT id, email, region, termino, tipo_contrato, sector, frecuencia, creada_en FROM alertas_suscripciones WHERE activa=TRUE ORDER BY creada_en",
        [],
    )
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id","email","region","termino","tipo_contrato","sector","frecuencia","creada_en"])
    writer.writeheader()
    for row in subs:
        writer.writerow({k: (str(v) if v is not None else "") for k,v in row.items() if k in writer.fieldnames})
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=suscripciones.csv"},
    )


@router.get(f"/api/{ADMIN_PATH}/ofertas/export", tags=["admin"])
def admin_export_ofertas(
    activa: str | None = Query(None),
    sector: str | None = Query(None),
    q: str | None = Query(None),
    _user: str = Depends(_verify_admin_jwt),
) -> Response:
    """Exporta ofertas filtradas como CSV (máx. 5000 filas)."""
    import csv, io
    conditions = []
    params: list[Any] = []
    if activa == "true":
        conditions.append("o.activa = TRUE")
    elif activa == "false":
        conditions.append("o.activa = FALSE")
    if sector:
        conditions.append("i.sector = %s")
        params.append(sector)
    if q:
        conditions.append("(o.cargo ILIKE %s OR o.institucion_nombre ILIKE %s OR i.nombre ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    rows = execute_fetch_all(
        f"""SELECT o.id,
               COALESCE(NULLIF(TRIM(o.institucion_nombre),''), i.nombre, '') AS institucion,
               o.cargo, o.region,
               COALESCE(i.sector, o.sector, '') AS sector,
               o.tipo_contrato, o.fecha_publicacion, o.fecha_cierre,
               o.activa, o.estado, o.url_oferta, o.url_oferta_valida,
               o.renta_bruta_min, o.renta_bruta_max
            {ofertas_base_sql()}
            {where}
            ORDER BY COALESCE(o.fecha_scraped,o.detectada_en) DESC NULLS LAST
            LIMIT 5000""",
        params,
    )
    buf = io.StringIO()
    campos = ["id","institucion","cargo","region","sector","tipo_contrato","fecha_publicacion","fecha_cierre","activa","estado","url_oferta","url_oferta_valida","renta_bruta_min","renta_bruta_max"]
    writer = csv.DictWriter(buf, fieldnames=campos, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: (str(v) if v is not None else "") for k,v in row.items()})
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ofertas.csv"},
    )


# ── Fin endpoints admin ───────────────────────────────────────────────────────


