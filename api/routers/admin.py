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
import math
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
    ROLES_VALIDOS,
    SITE_URL,
    _PROJECT_ROOT,
    hash_password,
    require_admin as _require_admin,
    require_editor as _require_editor,
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
from api.services.analitica import (
    resumen_interno as analitica_resumen_interno,
    resumen_umami as analitica_resumen_umami,
    umami_configurado as analitica_umami_configurado,
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


# ── Auditoría de acciones admin ──────────────────────────────────────────────

def _auditar(
    usuario: str,
    accion: str,
    entidad: str | None = None,
    entidad_id: Any = None,
    detalle: dict[str, Any] | None = None,
) -> None:
    """Registra una acción admin en `admin_audit_log` (best-effort).

    Nunca interrumpe el endpoint: si la tabla no existe (migración
    `20260612_0003_admin_audit` sin aplicar) sólo deja un warning en logs.
    """
    try:
        with get_cursor() as (conn, cur):
            cur.execute(
                """INSERT INTO admin_audit_log (usuario, accion, entidad, entidad_id, detalle)
                   VALUES (%s, %s, %s, %s, %s)""",
                [
                    usuario,
                    accion,
                    entidad,
                    str(entidad_id) if entidad_id is not None else None,
                    json.dumps(detalle, ensure_ascii=False, default=str) if detalle else None,
                ],
            )
            conn.commit()
    except Exception as exc:
        logger.warning(f"[audit] no se pudo registrar '{accion}': {exc}")


@router.get(f"/api/{ADMIN_PATH}/audit", tags=["admin"])
def admin_audit(
    limit: int = Query(100, ge=1, le=500),
    accion: str | None = Query(None),
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Registro de acciones realizadas desde el panel, más reciente primero."""
    where = "WHERE accion = %s" if accion else ""
    params: list[Any] = [accion] if accion else []
    try:
        rows = execute_fetch_all(
            f"""SELECT id, ts, usuario, accion, entidad, entidad_id, detalle
                FROM admin_audit_log
                {where}
                ORDER BY ts DESC
                LIMIT %s""",
            params + [limit],
        )
        return {"items": rows}
    except Exception as exc:
        logger.warning(f"[audit] no se pudo leer admin_audit_log: {exc}")
        return {
            "items": [],
            "warning": "Tabla admin_audit_log no disponible — aplicar `alembic upgrade head`.",
        }


@router.post(f"/api/{ADMIN_PATH}/meilisearch/reindexar", tags=["admin"])
def api_reindexar_meili(_user: str = Depends(_require_editor)) -> dict[str, Any]:
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
    payload: dict[str, Any] | None = None,
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """
    Procesa y envía alertas pendientes a los suscriptores.
    Busca ofertas nuevas (últimas `horas`) que coincidan con los filtros
    de cada suscriptor y les envía un email via Resend.

    Body (JSON, opcional):
      - email: str    — sólo a este suscriptor (match exacto, case-insensitive)
      - horas: int    — ventana de ofertas nuevas (default 24, máx 168)
      - dry_run: bool — simula: calcula coincidencias SIN enviar emails

    Movido bajo el prefijo admin: antes era público y permitía gatillar
    envío masivo de emails vía Resend sin autenticación (abuso de
    cuota + spam a toda la lista de suscriptores).
    """
    payload = payload or {}
    email_filtro = (str(payload.get("email") or "")).strip().lower()
    dry_run = bool(payload.get("dry_run", False))
    try:
        horas = max(1, min(int(payload.get("horas") or 24), 168))
    except (TypeError, ValueError):
        horas = 24

    # Sólo suscripciones VERIFICADAS (doble opt-in): nunca enviar a un correo
    # que no confirmó su alta. Las filas heredadas quedaron verificada=TRUE en
    # la migración, así que no se pierden envíos existentes.
    if email_filtro:
        suscripciones = execute_fetch_all(
            "SELECT * FROM alertas_suscripciones "
            "WHERE activa = TRUE AND verificada = TRUE AND LOWER(email) = %s",
            [email_filtro],
        )
    else:
        suscripciones = execute_fetch_all(
            "SELECT * FROM alertas_suscripciones WHERE activa = TRUE AND verificada = TRUE"
        )
    if not suscripciones:
        return {
            "ok": True, "enviados": 0, "errores": 0, "sin_coincidencias": 0,
            "total_suscripciones": 0, "dry_run": dry_run, "detalles": [],
            "mensaje": "Sin suscripciones activas que coincidan",
        }

    enviados = 0
    errores = 0
    sin_coincidencias = 0
    detalles: list[dict[str, Any]] = []

    for sub in suscripciones:
        where_parts = [ACTIVE_OFFER_SQL]
        params: list[Any] = []

        # Sólo ofertas de la ventana solicitada
        where_parts.append(
            "COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) >= NOW() - make_interval(hours => %s)"
        )
        params.append(horas)

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

        if not ofertas:
            sin_coincidencias += 1
            detalles.append({"email": sub["email"], "resultado": "sin_coincidencias", "coincidencias": 0})
            continue

        if dry_run:
            enviados += 1  # "se enviaría"
            detalles.append({"email": sub["email"], "resultado": "simulado", "coincidencias": len(ofertas)})
            continue

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
            detalles.append({"email": sub["email"], "resultado": "enviado", "coincidencias": len(ofertas)})
        else:
            errores += 1
            detalles.append({"email": sub["email"], "resultado": "error", "coincidencias": len(ofertas)})

    if not dry_run:
        _auditar(_user, "enviar_alertas", "suscripciones", None, {
            "total_suscripciones": len(suscripciones),
            "enviados": enviados, "errores": errores, "horas": horas,
            "email_filtro": email_filtro or None,
        })
    return {
        "ok": True,
        "total_suscripciones": len(suscripciones),
        "enviados": enviados,
        "errores": errores,
        "sin_coincidencias": sin_coincidencias,
        "dry_run": dry_run,
        "horas": horas,
        "detalles": detalles,
    }




@router.get(f"/api/{ADMIN_PATH}/stats", tags=["admin"])
def admin_stats(_user: str = Depends(_verify_admin_jwt)) -> dict[str, Any]:
    """Métricas completas para el dashboard de administración."""
    totales = execute_fetch_one("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE activa = TRUE)  AS activas,
            COUNT(*) FILTER (WHERE activa = FALSE) AS inactivas,
            COUNT(*) FILTER (WHERE url_oferta_valida = TRUE AND activa = TRUE)  AS urls_validas,
            COUNT(*) FILTER (WHERE url_oferta_valida = FALSE) AS urls_rotas,
            COUNT(*) FILTER (WHERE url_oferta_valida IS NULL)  AS urls_sin_validar,
            COUNT(*) FILTER (WHERE needs_review = TRUE AND activa = TRUE) AS needs_review,
            COUNT(*) FILTER (
                WHERE COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en)
                      >= NOW() - INTERVAL '24 hours'
            ) AS nuevas_24h,
            COUNT(*) FILTER (
                WHERE fecha_cierre IS NOT NULL AND fecha_cierre < (NOW() AT TIME ZONE 'America/Santiago')::date AND activa = TRUE
            ) AS activas_vencidas
        FROM ofertas
    """) or {}

    inst_row = execute_fetch_one(
        "SELECT COUNT(*) AS n FROM instituciones", []
    ) or {}
    totales["instituciones"] = int(inst_row.get("n") or 0)

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

    # Claves alineadas con renderUrlStats() del panel (gestion.js).
    url_validez = execute_fetch_one("""
        SELECT
            COUNT(*) FILTER (WHERE url_oferta_valida = TRUE)  AS url_oferta_validas,
            COUNT(*) FILTER (WHERE url_oferta_valida = FALSE) AS url_oferta_rotas,
            COUNT(*) FILTER (WHERE url_bases_valida = TRUE)   AS url_bases_validas,
            COUNT(*) FILTER (WHERE url_bases_valida = FALSE)  AS url_bases_rotas,
            COUNT(*) FILTER (
                WHERE url_valida_chequeada_en IS NULL
                   OR url_valida_chequeada_en < NOW() - INTERVAL '24 hours'
            ) AS sin_chequear_hoy,
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


@router.get(f"/api/{ADMIN_PATH}/analitica", tags=["admin"])
def admin_analitica(
    dias: int = Query(30, ge=1, le=365),
    incluir_umami: bool = Query(True),
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Estadísticas de tráfico del sitio: analítica propia + Umami (si está).

    La analítica propia sale de `web_eventos` (beacon de `web/analytics.js`).
    Umami se consulta solo si está configurado vía env vars; si no, la
    sección viene con `configurado=false` y el panel la oculta.
    """
    interno = analitica_resumen_interno(dias)
    umami: dict[str, Any] = {"configurado": False}
    if incluir_umami and analitica_umami_configurado():
        umami = analitica_resumen_umami(dias)
    return {"dias": dias, "interno": interno, "umami": umami}


@router.get(f"/api/{ADMIN_PATH}/analitica/export", tags=["admin"])
def admin_analitica_export(
    dias: int = Query(30, ge=1, le=365),
    _user: str = Depends(_verify_admin_jwt),
) -> Response:
    """Exporta la analítica del sitio como CSV (serie diaria + páginas top).

    Pensado para abrir en una planilla: una sección con visitas por día y
    otra con las páginas más vistas del período.
    """
    interno = analitica_resumen_interno(dias)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([f"Estadísticas del sitio — últimos {dias} días"])
    writer.writerow([])
    writer.writerow(["Fecha", "Páginas vistas", "Visitantes"])
    for fila in (interno.get("serie") or []):
        writer.writerow([fila.get("dia", ""), fila.get("vistas", 0), fila.get("visitantes", 0)])
    writer.writerow([])
    writer.writerow(["Página", "Vistas"])
    for fila in (interno.get("top_paginas") or []):
        writer.writerow([fila.get("path", ""), fila.get("vistas", 0)])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=estadisticas_{dias}d.csv"},
    )


# Portales/scrapers de origen reconocibles por el dominio de la URL de la oferta.
# clave (para el filtro `origen`) → lista de dominios. "propio" = sitio
# institucional (URL que no cae en ningún portal). Los dominios son constantes,
# no entra input del usuario, así que interpolarlos en el ILIKE es seguro
# (igual se pasan como parámetros).
_ORIGEN_DOMINIOS: dict[str, list[str]] = {
    "empleospublicos":    ["empleospublicos.cl"],
    "trabajando":         ["trabajando.com", "trabajando.cl"],
    "hiringroom":         ["hiringroom.com"],
    "buk":                ["buk.cl"],
    "chileatiende":       ["chileatiende.cl"],
    "empleos_gob":        ["empleos.gob.cl"],
    "postulaciones":      ["postulaciones.cl"],
    "sistemadeconcursos": ["sistemadeconcursos.cl"],
    "mitrabajodigno":     ["mitrabajodigno.cl"],
    "ucampus":            ["ucampus.net"],
}
_ORIGEN_TODOS_PORTALES = [d for doms in _ORIGEN_DOMINIOS.values() for d in doms]


@router.get(f"/api/{ADMIN_PATH}/ofertas", tags=["admin"])
def admin_ofertas(
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(50, ge=1, le=200),
    activa: str | None = Query(None, description="true/false/all"),
    url_rota: bool | None = Query(None),
    sector: str | None = Query(None),
    region: str | None = Query(None),
    q: str | None = Query(None),
    oferta_id: int | None = Query(None, description="ID exacto"),
    institucion_id: int | None = Query(None),
    estado: str | None = Query(None, description="activa|cerrada|vencida|suspendida"),
    cierre_desde: date | None = Query(None),
    cierre_hasta: date | None = Query(None),
    needs_review: bool | None = Query(None),
    sin_renta: bool | None = Query(None, description="true: sin renta_bruta_min ni max"),
    destacada: str | None = Query(None, description="true/false: filtrar por destacada"),
    origen: str | None = Query(None, description="Portal/scraper de origen (dominio de la URL) o 'propio'"),
    orden: str = Query("reciente", description="reciente|cierre|cargo|renta"),
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Lista paginada de ofertas con filtros para revisión."""
    conditions = []
    params: list[Any] = []

    if activa == "true":
        conditions.append("o.activa = TRUE")
    elif activa == "false":
        conditions.append("o.activa = FALSE")

    # Filtro por origen (portal/scraper). Los dominios salen de un allowlist
    # fijo; el valor del cliente sólo selecciona una clave conocida.
    if origen:
        if origen == "propio":
            # Sitio institucional propio: la URL no está en ningún portal.
            clauses = []
            for dom in _ORIGEN_TODOS_PORTALES:
                clauses.append("COALESCE(o.url_oferta, '') NOT ILIKE %s")
                clauses.append("COALESCE(o.url_original, '') NOT ILIKE %s")
                params.extend([f"%{dom}%", f"%{dom}%"])
            conditions.append("(" + " AND ".join(clauses) + ")")
        elif origen in _ORIGEN_DOMINIOS:
            clauses = []
            for dom in _ORIGEN_DOMINIOS[origen]:
                clauses.append("COALESCE(o.url_oferta, '') ILIKE %s")
                clauses.append("COALESCE(o.url_original, '') ILIKE %s")
                params.extend([f"%{dom}%", f"%{dom}%"])
            conditions.append("(" + " OR ".join(clauses) + ")")
        # origen desconocido → se ignora

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
        # Si el término es un número, también permitir match directo por ID.
        if q.strip().isdigit():
            conditions.append(
                "(o.id = %s OR o.cargo ILIKE %s OR o.institucion_nombre ILIKE %s OR i.nombre ILIKE %s)"
            )
            params.extend([int(q.strip()), f"%{q}%", f"%{q}%", f"%{q}%"])
        else:
            conditions.append(
                "(o.cargo ILIKE %s OR o.institucion_nombre ILIKE %s OR i.nombre ILIKE %s)"
            )
            params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

    if oferta_id is not None:
        conditions.append("o.id = %s")
        params.append(oferta_id)

    if institucion_id is not None:
        conditions.append("o.institucion_id = %s")
        params.append(institucion_id)

    if estado:
        conditions.append("COALESCE(NULLIF(o.estado, ''), CASE WHEN o.activa THEN 'activa' ELSE 'cerrada' END) = %s")
        params.append(estado)

    if cierre_desde is not None:
        conditions.append("o.fecha_cierre >= %s")
        params.append(cierre_desde)

    if cierre_hasta is not None:
        conditions.append("o.fecha_cierre <= %s")
        params.append(cierre_hasta)

    if needs_review is True:
        conditions.append("o.needs_review = TRUE")
    elif needs_review is False:
        conditions.append("COALESCE(o.needs_review, FALSE) = FALSE")

    if sin_renta is True:
        conditions.append("o.renta_bruta_min IS NULL AND o.renta_bruta_max IS NULL")

    if destacada == "true":
        conditions.append("COALESCE(o.destacada, FALSE) = TRUE")
    elif destacada == "false":
        conditions.append("COALESCE(o.destacada, FALSE) = FALSE")

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    order_sql = {
        "reciente": "COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en) DESC NULLS LAST",
        "cierre": "o.fecha_cierre ASC NULLS LAST",
        "cargo": "o.cargo ASC",
        "renta": "GREATEST(COALESCE(o.renta_bruta_max, 0), COALESCE(o.renta_bruta_min, 0)) DESC",
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
            o.activa, COALESCE(o.destacada, FALSE) AS destacada,
            o.estado, o.url_oferta, o.url_oferta_valida,
            o.url_bases, o.url_bases_valida,
            o.renta_bruta_min, o.renta_bruta_max,
            o.fecha_scraped, o.detectada_en,
            o.institucion_id, o.descripcion, o.requisitos, o.requisitos_texto,
            o.ciudad, o.lugar_desempenio, o.area_profesional, o.calidad_juridica,
            o.estamento, o.jornada, o.numero_vacantes, o.renta_tipo, o.grado_eus,
            o.fecha_publicacion, o.email_postulacion, o.email_consultas,
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
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Activa o desactiva una oferta."""
    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE ofertas SET activa = NOT COALESCE(activa, TRUE), actualizada_en = NOW() "
            "WHERE id = %s RETURNING activa",
            [oferta_id],
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Oferta no encontrada")
        nuevo_estado = row["activa"] if isinstance(row, dict) else row[0]
        conn.commit()
    _auditar(_user, "toggle_activa", "oferta", oferta_id, {"activa": nuevo_estado})
    return {"id": oferta_id, "activa": nuevo_estado}


@router.post(f"/api/{ADMIN_PATH}/ofertas/{{oferta_id}}/toggle-destacada", tags=["admin"])
def admin_toggle_destacada(
    oferta_id: int,
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Marca/desmarca una oferta como destacada (sección de redes sociales).

    Las ofertas destacadas alimentan la pestaña pública "Destacadas":
    las que el equipo publica activamente en Instagram / TikTok / LinkedIn.
    """
    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE ofertas SET destacada = NOT COALESCE(destacada, FALSE), actualizada_en = NOW() "
            "WHERE id = %s RETURNING destacada",
            [oferta_id],
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Oferta no encontrada")
        nuevo_estado = row["destacada"] if isinstance(row, dict) else row[0]
        conn.commit()
    _auditar(_user, "toggle_destacada", "oferta", oferta_id, {"destacada": nuevo_estado})
    return {"id": oferta_id, "destacada": nuevo_estado}


# ── Cursos (directorio gestionable desde el panel) ───────────────────────────

_CURSO_CAMPOS = (
    "curso_id", "titulo", "proveedor", "categoria", "modalidad", "duracion",
    "nivel", "tipo", "url", "logo", "descripcion", "gratuito", "demo", "orden", "activo",
)


@router.get(f"/api/{ADMIN_PATH}/cursos", tags=["admin"])
def admin_listar_cursos(_user: str = Depends(_verify_admin_jwt)) -> dict[str, Any]:
    """Lista TODOS los cursos (activos e inactivos) para el panel."""
    rows = execute_fetch_all(
        """
        SELECT id, curso_id, titulo, proveedor, categoria, modalidad, duracion,
               nivel, tipo, url, logo, descripcion, gratuito, demo, orden, activo
        FROM cursos ORDER BY orden ASC, id ASC
        """,
        [],
    )
    return {"cursos": rows}


@router.post(f"/api/{ADMIN_PATH}/cursos", tags=["admin"])
def admin_crear_curso(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Crea un curso. Requerido: titulo. `curso_id` se deriva del título si falta."""
    titulo = (payload.get("titulo") or "").strip()
    if not titulo:
        raise HTTPException(400, "El título es obligatorio")
    datos = {
        "curso_id": ((payload.get("curso_id") or _slugify(titulo)) or "")[:80] or None,
        "titulo": titulo[:300],
        "proveedor": (payload.get("proveedor") or "")[:200] or None,
        "categoria": (payload.get("categoria") or "")[:40] or None,
        "modalidad": (payload.get("modalidad") or "")[:80] or None,
        "duracion": (payload.get("duracion") or "")[:60] or None,
        "nivel": "destacado" if payload.get("nivel") == "destacado" else "estandar",
        "tipo": (payload.get("tipo") or "curso")[:30],
        "url": (str(payload.get("url") or "").strip()) or None,
        "logo": (str(payload.get("logo") or "").strip()) or None,
        "descripcion": (payload.get("descripcion") or "") or None,
        "gratuito": bool(payload.get("gratuito", True)),
        "demo": bool(payload.get("demo", False)),
        "orden": int(payload.get("orden") or 100),
        "activo": bool(payload.get("activo", True)),
    }
    with get_cursor() as (conn, cur):
        cur.execute(
            """
            INSERT INTO cursos (curso_id, titulo, proveedor, categoria, modalidad,
                duracion, nivel, tipo, url, logo, descripcion, gratuito, demo, orden, activo)
            VALUES (%(curso_id)s, %(titulo)s, %(proveedor)s, %(categoria)s, %(modalidad)s,
                %(duracion)s, %(nivel)s, %(tipo)s, %(url)s, %(logo)s, %(descripcion)s, %(gratuito)s,
                %(demo)s, %(orden)s, %(activo)s)
            ON CONFLICT (curso_id) DO NOTHING
            RETURNING id
            """,
            datos,
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(409, "Ya existe un curso con ese identificador")
        conn.commit()
        nuevo_id = row["id"] if isinstance(row, dict) else row[0]
    _auditar(_user, "crear_curso", "curso", nuevo_id, {"titulo": titulo})
    return {"id": nuevo_id}


@router.put(f"/api/{ADMIN_PATH}/cursos/{{curso_pk}}", tags=["admin"])
def admin_editar_curso(
    curso_pk: int,
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Edita un curso. Acepta cualquier subconjunto de los campos del curso."""
    updates = {k: v for k, v in payload.items() if k in set(_CURSO_CAMPOS)}
    if not updates:
        raise HTTPException(400, "Sin campos válidos para actualizar")
    if "nivel" in updates:
        updates["nivel"] = "destacado" if updates["nivel"] == "destacado" else "estandar"
    for b in ("gratuito", "demo", "activo"):
        if b in updates:
            updates[b] = bool(updates[b])
    if "orden" in updates:
        try:
            updates["orden"] = int(updates["orden"] or 100)
        except (TypeError, ValueError):
            raise HTTPException(400, "orden debe ser numérico") from None
    from psycopg2 import sql as _sql
    set_parts = [_sql.SQL("{} = %({})s").format(_sql.Identifier(c), _sql.Identifier(c)) for c in updates]
    query = _sql.SQL("UPDATE cursos SET {}, actualizada_en = NOW() WHERE id = %(_pk)s").format(
        _sql.SQL(", ").join(set_parts),
    )
    params = dict(updates)
    params["_pk"] = curso_pk
    with get_cursor() as (conn, cur):
        cur.execute(query, params)
        if cur.rowcount == 0:
            raise HTTPException(404, "Curso no encontrado")
        conn.commit()
    _auditar(_user, "editar_curso", "curso", curso_pk, {"campos": sorted(updates)})
    return {"id": curso_pk, "updated": list(updates.keys())}


@router.delete(f"/api/{ADMIN_PATH}/cursos/{{curso_pk}}", tags=["admin"])
def admin_borrar_curso(
    curso_pk: int,
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Elimina un curso del directorio."""
    with get_cursor() as (conn, cur):
        cur.execute("DELETE FROM cursos WHERE id = %s", [curso_pk])
        if cur.rowcount == 0:
            raise HTTPException(404, "Curso no encontrado")
        conn.commit()
    _auditar(_user, "borrar_curso", "curso", curso_pk)
    return {"id": curso_pk, "deleted": True}


# ── Cursos: categorías (gestionables) ────────────────────────────────────────

@router.get(f"/api/{ADMIN_PATH}/cursos/categorias", tags=["admin"])
def admin_listar_categorias(_user: str = Depends(_verify_admin_jwt)) -> dict[str, Any]:
    rows = execute_fetch_all(
        "SELECT id, slug, etiqueta, orden, activo FROM cursos_categorias "
        "ORDER BY orden ASC, etiqueta ASC",
        [],
    )
    return {"categorias": rows}


@router.post(f"/api/{ADMIN_PATH}/cursos/categorias", tags=["admin"])
def admin_crear_categoria(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    etiqueta = (payload.get("etiqueta") or "").strip()
    if not etiqueta:
        raise HTTPException(400, "La etiqueta es obligatoria")
    slug = ((payload.get("slug") or _slugify(etiqueta)) or "")[:40] or None
    if not slug:
        raise HTTPException(400, "Slug inválido")
    orden = int(payload.get("orden") or 100)
    with get_cursor() as (conn, cur):
        cur.execute(
            "INSERT INTO cursos_categorias (slug, etiqueta, orden, activo) "
            "VALUES (%s, %s, %s, TRUE) ON CONFLICT (slug) DO NOTHING RETURNING id",
            [slug, etiqueta[:120], orden],
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(409, "Ya existe una categoría con ese slug")
        conn.commit()
        nuevo_id = row["id"] if isinstance(row, dict) else row[0]
    _auditar(_user, "crear_categoria_curso", "cursos_categoria", nuevo_id, {"slug": slug})
    return {"id": nuevo_id, "slug": slug}


@router.put(f"/api/{ADMIN_PATH}/cursos/categorias/{{cat_id}}", tags=["admin"])
def admin_editar_categoria(
    cat_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Renombra (etiqueta) o reordena/activa una categoría. El slug no se cambia
    para no romper los cursos ya asignados."""
    updates: dict[str, Any] = {}
    if "etiqueta" in payload:
        et = (payload.get("etiqueta") or "").strip()
        if not et:
            raise HTTPException(400, "La etiqueta no puede quedar vacía")
        updates["etiqueta"] = et[:120]
    if "orden" in payload:
        try:
            updates["orden"] = int(payload.get("orden") or 100)
        except (TypeError, ValueError):
            raise HTTPException(400, "orden debe ser numérico") from None
    if "activo" in payload:
        updates["activo"] = bool(payload.get("activo"))
    if not updates:
        raise HTTPException(400, "Sin campos para actualizar")
    from psycopg2 import sql as _sql
    set_parts = [_sql.SQL("{} = %({})s").format(_sql.Identifier(c), _sql.Identifier(c)) for c in updates]
    query = _sql.SQL("UPDATE cursos_categorias SET {} WHERE id = %(_id)s").format(
        _sql.SQL(", ").join(set_parts),
    )
    params = dict(updates)
    params["_id"] = cat_id
    with get_cursor() as (conn, cur):
        cur.execute(query, params)
        if cur.rowcount == 0:
            raise HTTPException(404, "Categoría no encontrada")
        conn.commit()
    _auditar(_user, "editar_categoria_curso", "cursos_categoria", cat_id, {"campos": sorted(updates)})
    return {"id": cat_id, "updated": list(updates.keys())}


@router.delete(f"/api/{ADMIN_PATH}/cursos/categorias/{{cat_id}}", tags=["admin"])
def admin_borrar_categoria(
    cat_id: int,
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Borra una categoría. Los cursos que la usaban pasan a 'Otros' para no
    quedar huérfanos. 'Otros' no se puede borrar (es el destino de reasignación)."""
    with get_cursor() as (conn, cur):
        cur.execute("SELECT slug FROM cursos_categorias WHERE id = %s", [cat_id])
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Categoría no encontrada")
        slug = row["slug"] if isinstance(row, dict) else row[0]
        if slug == "otros":
            raise HTTPException(400, "No se puede borrar 'Otros' (es el destino de reasignación)")
        cur.execute("UPDATE cursos SET categoria = 'otros' WHERE categoria = %s", [slug])
        cur.execute("DELETE FROM cursos_categorias WHERE id = %s", [cat_id])
        conn.commit()
    _auditar(_user, "borrar_categoria_curso", "cursos_categoria", cat_id, {"slug": slug})
    return {"id": cat_id, "deleted": True}


@router.put(f"/api/{ADMIN_PATH}/ofertas/{{oferta_id}}", tags=["admin"])
def admin_editar_oferta(
    oferta_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Edita campos de una oferta.

    Permitidos: cargo, descripcion, fecha_cierre, activa, estado, region,
    tipo_contrato, renta_bruta_min, renta_bruta_max, url_oferta, url_bases.
    Si cambia una URL, su flag de validez vuelve a NULL (sin validar) para
    que el próximo pase de `validate_offer_urls.py` la chequee.
    """
    CAMPOS_PERMITIDOS = {
        "cargo", "descripcion", "requisitos", "fecha_cierre", "fecha_publicacion",
        "activa", "estado", "region", "ciudad", "lugar_desempenio", "sector",
        "area_profesional", "tipo_contrato", "calidad_juridica", "estamento",
        "jornada", "numero_vacantes", "renta_bruta_min", "renta_bruta_max",
        "renta_tipo", "grado_eus", "email_postulacion", "email_consultas",
        "url_oferta", "url_bases",
    }
    updates = {k: v for k, v in payload.items() if k in CAMPOS_PERMITIDOS}
    if not updates:
        raise HTTPException(400, "Sin campos válidos para actualizar")

    for campo in ("renta_bruta_min", "renta_bruta_max", "numero_vacantes"):
        if campo in updates and updates[campo] not in (None, ""):
            try:
                updates[campo] = int(updates[campo])
            except (TypeError, ValueError):
                raise HTTPException(400, f"{campo} debe ser numérico") from None

    for campo in ("email_postulacion", "email_consultas"):
        if campo in updates and updates[campo]:
            correo = str(updates[campo]).strip()
            if correo and "@" not in correo:
                raise HTTPException(400, f"{campo} debe ser un correo válido")
            updates[campo] = correo or None

    for campo in ("url_oferta", "url_bases"):
        if campo in updates:
            valor = (str(updates[campo] or "")).strip()
            if valor and not valor.startswith(("http://", "https://")):
                raise HTTPException(400, f"{campo} debe ser una URL http(s)")
            updates[campo] = valor or None

    from psycopg2 import sql as _sql
    set_parts = [_sql.SQL("{} = %s").format(_sql.Identifier(col)) for col in updates]
    if "url_oferta" in updates:
        set_parts.append(_sql.SQL("url_oferta_valida = NULL"))
    if "url_bases" in updates:
        set_parts.append(_sql.SQL("url_bases_valida = NULL"))
    query = _sql.SQL("UPDATE ofertas SET {}, actualizada_en = NOW() WHERE id = %s").format(
        _sql.SQL(", ").join(set_parts),
    )
    vals = list(updates.values()) + [oferta_id]

    with get_cursor() as (conn, cur):
        cur.execute(query, vals)
        if cur.rowcount == 0:
            raise HTTPException(404, "Oferta no encontrada")
        conn.commit()

    _auditar(_user, "editar_oferta", "oferta", oferta_id, {"campos": sorted(updates)})
    return {"id": oferta_id, "updated": list(updates.keys())}


@router.post(f"/api/{ADMIN_PATH}/ofertas", tags=["admin"])
def admin_crear_oferta(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Crea una oferta manual desde el panel.

    Requeridos: cargo, institucion_nombre. Opcionales: descripcion,
    fecha_cierre, fecha_publicacion, region, sector, tipo_contrato,
    renta_bruta_min/max, url_oferta, url_bases, institucion_id.
    """
    cargo = (payload.get("cargo") or "").strip()
    institucion = (payload.get("institucion_nombre") or "").strip()
    if not cargo or not institucion:
        raise HTTPException(400, "cargo e institucion_nombre son requeridos")

    def _int_o_none(v: Any) -> int | None:
        if v in (None, ""):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    url_oferta = (str(payload.get("url_oferta") or "")).strip() or None
    url_bases = (str(payload.get("url_bases") or "")).strip() or None
    for nombre_url, valor_url in (("url_oferta", url_oferta), ("url_bases", url_bases)):
        if valor_url and not valor_url.startswith(("http://", "https://")):
            raise HTTPException(400, f"{nombre_url} debe ser una URL http(s)")

    cols = _table_columns("ofertas")
    fila = {
        "cargo": cargo[:500],
        "institucion_nombre": institucion[:300],
        "institucion_id": _int_o_none(payload.get("institucion_id")),
        "descripcion": payload.get("descripcion") or None,
        "requisitos": payload.get("requisitos") or None,
        "fecha_cierre": payload.get("fecha_cierre") or None,
        "fecha_publicacion": payload.get("fecha_publicacion") or date.today(),
        "region": (payload.get("region") or None),
        "ciudad": (payload.get("ciudad") or None),
        "lugar_desempenio": (payload.get("lugar_desempenio") or None),
        "sector": (payload.get("sector") or None),
        "area_profesional": (payload.get("area_profesional") or None),
        "tipo_contrato": (payload.get("tipo_contrato") or None),
        "calidad_juridica": (payload.get("calidad_juridica") or None),
        "estamento": (payload.get("estamento") or None),
        "jornada": (payload.get("jornada") or None),
        "numero_vacantes": _int_o_none(payload.get("numero_vacantes")),
        "renta_bruta_min": _int_o_none(payload.get("renta_bruta_min")),
        "renta_bruta_max": _int_o_none(payload.get("renta_bruta_max")),
        "renta_tipo": (payload.get("renta_tipo") or None),
        "grado_eus": (payload.get("grado_eus") or None),
        "email_postulacion": (payload.get("email_postulacion") or None),
        "email_consultas": (payload.get("email_consultas") or None),
        "url_oferta": url_oferta,
        "url_bases": url_bases,
        "activa": True,
        "estado": "activa",
    }
    # Sólo columnas que existan en el schema actual (estado/tipo_contrato
    # vienen de migraciones; en DBs viejas pueden faltar).
    fila = {k: v for k, v in fila.items() if k in cols}

    from psycopg2 import sql as _sql
    query = _sql.SQL("INSERT INTO ofertas ({}) VALUES ({}) RETURNING id").format(
        _sql.SQL(", ").join(_sql.Identifier(c) for c in fila),
        _sql.SQL(", ").join(_sql.Placeholder() for _ in fila),
    )
    with get_cursor() as (conn, cur):
        cur.execute(query, list(fila.values()))
        row = cur.fetchone()
        nuevo_id = (row["id"] if isinstance(row, dict) else row[0]) if row else None
        conn.commit()

    _auditar(_user, "crear_oferta", "oferta", nuevo_id, {"cargo": cargo[:80], "institucion": institucion[:80]})
    return {"id": nuevo_id, "cargo": cargo}


@router.post(f"/api/{ADMIN_PATH}/ofertas/bulk-marcar-revisadas", tags=["admin"])
def admin_bulk_marcar_revisadas(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Marca en bloque ofertas como revisadas (needs_review=FALSE).

    Body: { ids: list[int] }
    """
    ids = [int(i) for i in (payload.get("ids") or []) if str(i).isdigit()]
    if not ids:
        raise HTTPException(400, "ids es requerido")
    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE ofertas SET needs_review=FALSE, actualizada_en=NOW() WHERE id = ANY(%s)",
            [ids],
        )
        count = cur.rowcount
        conn.commit()
    _auditar(_user, "bulk_marcar_revisadas", "ofertas", None, {"marcadas": count})
    return {"marcadas": count}


@router.post(f"/api/{ADMIN_PATH}/ofertas/bulk-destacar", tags=["admin"])
def admin_bulk_destacar(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Marca/desmarca en bloque ofertas como destacadas.

    Body: { ids: list[int], destacada: bool }
    """
    ids = [int(i) for i in (payload.get("ids") or []) if str(i).isdigit()]
    if not ids:
        raise HTTPException(400, "ids es requerido")
    nuevo = bool(payload.get("destacada", True))
    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE ofertas SET destacada=%s, actualizada_en=NOW() WHERE id = ANY(%s)",
            [nuevo, ids],
        )
        count = cur.rowcount
        conn.commit()
    _auditar(_user, "bulk_destacar", "ofertas", None,
             {"destacada": nuevo, "afectadas": count})
    return {"afectadas": count, "destacada": nuevo}


@router.get(f"/api/{ADMIN_PATH}/destacadas/stats", tags=["admin"])
def admin_destacadas_stats(
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Estadísticas de ofertas destacadas y criterio auto."""
    from api.services.sql import DESTACADAS_AUTO_SQL
    # El estado del criterio automático lo gobierna el toggle `destacadas_auto`
    # de site_config (panel), no la constante del módulo.
    conf = _get_site_config_db()
    auto_activo = str(conf.get("destacadas_auto", "")).strip().lower() in ("1", "true", "yes")
    manual = execute_fetch_one(
        "SELECT COUNT(*) AS n FROM ofertas WHERE activa AND COALESCE(destacada, FALSE) = TRUE", []
    )
    auto = execute_fetch_one(
        f"SELECT COUNT(*) AS n FROM ofertas WHERE activa AND {DESTACADAS_AUTO_SQL}", []
    ) if auto_activo else {"n": 0}
    total_activas = execute_fetch_one(
        "SELECT COUNT(*) AS n FROM ofertas WHERE activa", []
    )
    return {
        "manual": int((manual or {}).get("n", 0)),
        "auto": int((auto or {}).get("n", 0)),
        "total_activas": int((total_activas or {}).get("n", 0)),
        "auto_activo": auto_activo,
        "auto_criterio": "Ofertas con renta bruta publicada" if auto_activo else None,
    }


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


# ── Admin: procesos en background (scrapers / revalidación) ─────────────────
#
# Cada proceso lanzado desde el panel escribe su salida a un archivo en
# `logs/admin_runs/`. La metadata (pid, tipo, comando) va en la primera
# línea del log con el prefijo `### ADMIN-RUN`, de modo que cualquier
# worker de uvicorn pueda listar y consultar procesos aunque no los haya
# lanzado él (el dict en memoria sólo agrega el returncode exacto).

_ADMIN_RUNS_LOG_DIR = _PROJECT_ROOT / "logs" / "admin_runs"
_LOG_FILENAME_RE = re.compile(r"^[\w.-]+\.log$")


@dataclass
class _ProcesoLanzado:
    pid: int
    tipo: str
    cmd: list[str]
    log_path: Path
    started_at: datetime
    proc: subprocess.Popen


_PROCESOS: dict[int, _ProcesoLanzado] = {}  # pid → info (sólo este worker)


def _lanzar_proceso(cmd: list[str], tipo: str) -> _ProcesoLanzado:
    """Lanza un proceso en background con su stdout/stderr a un log propio.

    Reemplaza el patrón anterior `stdout=subprocess.PIPE` que nadie leía:
    si el hijo escribía más que el buffer del pipe, quedaba bloqueado.
    """
    _ADMIN_RUNS_LOG_DIR.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(tz=timezone.utc)
    log_path = _ADMIN_RUNS_LOG_DIR / f"{tipo}_{ahora.strftime('%Y%m%d_%H%M%S')}.log"
    log_f = open(log_path, "a", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(_PROJECT_ROOT),
        text=True,
    )
    # Header con metadata para que otros workers puedan reconstruir estado.
    cmd_vista = " ".join(cmd[1:])  # sin el path de python
    log_f.write(
        f"### ADMIN-RUN pid={proc.pid} tipo={tipo} "
        f"started={ahora.isoformat()} cmd={cmd_vista}\n"
    )
    log_f.flush()
    log_f.close()
    info = _ProcesoLanzado(
        pid=proc.pid, tipo=tipo, cmd=cmd, log_path=log_path,
        started_at=ahora, proc=proc,
    )
    _PROCESOS[proc.pid] = info
    return info


def _pid_vivo(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError, OSError):
        return False


def _parse_log_header(log_path: Path) -> dict[str, Any] | None:
    """Lee la primera línea `### ADMIN-RUN ...` de un log y la parsea."""
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            primera = f.readline().strip()
    except OSError:
        return None
    if not primera.startswith("### ADMIN-RUN "):
        return None
    m = re.match(
        r"### ADMIN-RUN pid=(\d+) tipo=(\S+) started=(\S+) cmd=(.*)$", primera
    )
    if not m:
        return None
    return {
        "pid": int(m.group(1)),
        "tipo": m.group(2),
        "started_at": m.group(3),
        "cmd": m.group(4),
    }


def _estado_proceso(pid: int) -> tuple[str, int | None]:
    """(estado, returncode). returncode sólo si este worker lanzó el proceso."""
    info = _PROCESOS.get(pid)
    if info is not None:
        rc = info.proc.poll()
        if rc is None:
            return "en_curso", None
        return ("completado" if rc == 0 else "error"), rc
    return ("en_curso", None) if _pid_vivo(pid) else ("finalizado", None)


@router.get(f"/api/{ADMIN_PATH}/procesos", tags=["admin"])
def admin_procesos(
    limit: int = Query(20, ge=1, le=100),
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Lista los procesos lanzados desde el panel (scrapers, revalidación).

    Se reconstruye desde `logs/admin_runs/` para funcionar con múltiples
    workers de uvicorn; el estado en vivo sale de `os.kill(pid, 0)`.
    """
    if not _ADMIN_RUNS_LOG_DIR.exists():
        return {"procesos": []}
    logs = sorted(
        _ADMIN_RUNS_LOG_DIR.glob("*.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:limit]
    procesos = []
    for log_path in logs:
        header = _parse_log_header(log_path)
        if not header:
            continue
        estado, rc = _estado_proceso(header["pid"])
        procesos.append({
            **header,
            "estado": estado,
            "returncode": rc,
            "log": log_path.name,
            "log_bytes": log_path.stat().st_size,
        })
    return {"procesos": procesos}


@router.get(f"/api/{ADMIN_PATH}/procesos/log", tags=["admin"])
def admin_proceso_log(
    archivo: str = Query(..., description="Nombre del archivo en logs/admin_runs/"),
    tail: int = Query(200, ge=1, le=2000),
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """Devuelve las últimas `tail` líneas del log de un proceso lanzado."""
    if not _LOG_FILENAME_RE.match(archivo):
        raise HTTPException(400, "Nombre de archivo inválido")
    log_path = (_ADMIN_RUNS_LOG_DIR / archivo).resolve()
    if log_path.parent != _ADMIN_RUNS_LOG_DIR.resolve() or not log_path.exists():
        raise HTTPException(404, "Log no encontrado")
    try:
        lineas = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        raise HTTPException(500, f"No se pudo leer el log: {exc}") from exc
    header = _parse_log_header(log_path) or {}
    estado, rc = _estado_proceso(int(header.get("pid") or 0))
    return {
        "archivo": archivo,
        "pid": header.get("pid"),
        "tipo": header.get("tipo"),
        "estado": estado,
        "returncode": rc,
        "total_lineas": len(lineas),
        "lineas": lineas[-tail:],
    }


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
                decision = classify_source(item)
                enriched.append({
                    "id": item.get("id"),
                    "nombre": item.get("nombre"),
                    "sector": item.get("sector"),
                    "url_empleo": item.get("url_empleo"),
                    "status": str(decision.status.value) if decision.status else "",
                    "kind": str(decision.kind.value) if decision.kind else "",
                    "fuente_id": item.get("fuente_id"),
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
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """
    Dispara un scraper en background.

    Body (JSON):
      - mode: "all" | "empleos_publicos" | "institucion" | "kind"
      - institucion_id: int    (para mode=institucion)
      - kind: str              (para mode=kind, ej. "wordpress")
      - dry_run: bool          (default false → corre como --evaluate-only)
      - limite_fuentes: int    (opcional; máx. instituciones, → --limit)
    """
    import subprocess, sys as _sys, shlex

    mode       = payload.get("mode", "empleos_publicos")
    dry_run    = bool(payload.get("dry_run", False))
    # `limite_fuentes` (opcional) acota la cantidad de instituciones (--limit).
    # Compat: versiones viejas del panel mandaban `max` (que run_all no soporta).
    limite     = payload.get("limite_fuentes", payload.get("limite"))
    try:
        limite = int(limite) if limite not in (None, "") else 0
    except (TypeError, ValueError):
        limite = 0

    # Construir comando con flags REALES de run_all.py: --mode, --limit, --ids,
    # --id, --only-kind, --skip-empleos-publicos, --evaluate-only. (No existen
    # --max, --dry-run ni --include-experimental.)
    python = _sys.executable
    run_all = str(_PROJECT_ROOT / "scrapers" / "run_all.py")

    cmd = [python, run_all, "--mode", "production"]
    if dry_run:
        # run_all no tiene --dry-run; --evaluate-only hace discovery+evaluación
        # SIN extraer ni persistir ofertas → equivale a una simulación.
        cmd.append("--evaluate-only")
    if limite > 0:
        cmd += ["--limit", str(limite)]

    if mode == "all":
        # Corrida completa: run_all decide qué corre según el gatekeeper
        # (status=active); el conjunto experimental lo gobierna la evaluación.
        pass
    elif mode == "empleos_publicos":
        cmd += ["--only-kind", "empleos_publicos"]
    elif mode == "institucion":
        inst_id = payload.get("institucion_id")
        if not inst_id:
            raise HTTPException(400, "institucion_id es requerido para mode=institucion")
        cmd += ["--id", str(int(inst_id)), "--skip-empleos-publicos"]
    elif mode == "kind":
        kind = str(payload.get("kind", "wordpress"))
        # `kind` se pasa como `--only-kind <kind>`. Sin validar, un valor como
        # "--alguna-flag" se colaría como flag arbitraria (argument injection).
        # Lo restringimos a los valores conocidos del enum ScraperKind.
        try:
            from scrapers.source_status import ScraperKind
            _kinds_validos = {k.value for k in ScraperKind}
        except Exception:
            _kinds_validos = {
                "empleos_publicos", "wordpress", "generic",
                "custom_trabajando", "custom_hiringroom", "custom_buk",
                "custom_playwright", "custom_policia", "custom_ffaa",
            }
        if kind not in _kinds_validos:
            raise HTTPException(
                400, f"kind inválido: {kind!r}. Válidos: {sorted(_kinds_validos)}"
            )
        cmd += ["--only-kind", kind, "--skip-empleos-publicos"]
    else:
        raise HTTPException(400, f"mode inválido: {mode}")

    logger.info(f"[admin] scraper run: {shlex.join(cmd)}")

    # Ejecutar en background (no bloquea la respuesta); output a logs/admin_runs/
    try:
        proceso = _lanzar_proceso(cmd, tipo=f"scraper-{mode}")
        proc = proceso.proc
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

    _auditar(_user, "scraper_run", "proceso", proc.pid, {
        "mode": mode, "dry_run": dry_run, "limite_fuentes": limite or None,
        "run_id": run_id, "log": proceso.log_path.name,
    })
    return {
        "ok": True,
        "pid": proc.pid,
        "run_id": run_id,
        "cmd": cmd[2:],  # omitir python path
        "dry_run": dry_run,
        "mode": mode,
        "log": proceso.log_path.name,
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
    # Catálogo de criterios de Destacadas (para que el panel arme el editor).
    try:
        from api.services.sql import catalogo_criterios_destacadas
        criterios_catalogo = catalogo_criterios_destacadas()
    except Exception:
        criterios_catalogo = []
    return {"config": conf, "env": env_info, "criterios_catalogo": criterios_catalogo}


@router.put(f"/api/{ADMIN_PATH}/config", tags=["admin"])
def admin_set_config(
    payload: dict[str, Any],
    _user: str = Depends(_require_admin),
) -> dict[str, Any]:
    """
    Actualiza configuración editable del sitio.

    Claves soportadas: banner_mensaje, banner_activo, mantenimiento,
    max_resultados_pagina, alertas_activas, footer_extra.
    """
    CLAVES_PERMITIDAS = {
        "banner_mensaje", "banner_activo", "mantenimiento",
        "max_resultados_pagina", "alertas_activas", "footer_extra",
        "ads_enabled", "ads_client",
        "ads_slot_resultados", "ads_slot_sidebar", "ads_slot_contenido",
        # Destacadas: toggle del criterio automático + lista de criterios (JSON)
        # y modo de combinación (any/all). Ver catálogo en api/services/sql.py.
        "destacadas_auto", "destacadas_criterios", "destacadas_criterios_modo",
        # Recuadro "Anúnciate" (oferta + valores para publicar) en cursos.html.
        "cursos_anunciate_activo",
    }
    # `destacadas_criterios` debe ser JSON de una lista [{tipo,valor}]; si llega
    # algo que no parsea a lista, se descarta para no guardar basura.
    if "destacadas_criterios" in payload:
        import json as _json
        try:
            _parsed = _json.loads(payload["destacadas_criterios"] or "[]")
            if not isinstance(_parsed, list):
                raise ValueError
        except Exception:
            raise HTTPException(400, "destacadas_criterios debe ser una lista JSON")
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
    _auditar(_user, "editar_config", "site_config", None, {"claves": updated})
    return {"updated": updated}


# ── Admin: acciones sobre ofertas (bulk) ─────────────────────────────────────

@router.post(f"/api/{ADMIN_PATH}/ofertas/bulk-desactivar", tags=["admin"])
def admin_bulk_desactivar(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
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
            "SELECT id FROM ofertas WHERE activa=TRUE AND fecha_cierre < (NOW() AT TIME ZONE 'America/Santiago')::date", []
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
    _auditar(_user, "bulk_desactivar", "ofertas", None, {
        "desactivadas": count,
        "criterio": (
            "ids" if "ids" in payload
            else "url_rota" if payload.get("url_rota")
            else "fecha_cierre_vencida"
        ),
    })
    return {"desactivadas": count, "ids": ids[:50]}


@router.post(f"/api/{ADMIN_PATH}/urls/revalidar", tags=["admin"])
async def admin_revalidar_urls(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
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
    try:
        proceso = _lanzar_proceso(cmd, tipo="revalidar-urls")
    except Exception as exc:
        raise HTTPException(500, f"No se pudo lanzar el proceso: {exc}") from exc
    _auditar(_user, "revalidar_urls", "proceso", proceso.pid, {
        "workers": workers, "max_edad_h": max_edad, "limit": limit,
        "log": proceso.log_path.name,
    })
    return {
        "ok": True, "pid": proceso.pid, "workers": workers,
        "limit": limit, "log": proceso.log_path.name,
    }


@router.post(f"/api/{ADMIN_PATH}/ofertas/limpiar-no-laborales", tags=["admin"])
async def admin_limpiar_no_laborales(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """
    Lanza scripts/limpiar_ofertas_no_laborales.py en background: re-pasa las
    ofertas activas por el intake actual y desactiva las que hoy serían
    rechazadas (noticias municipales, actas, decretos, licitaciones, etc.).

    Body: { apply: bool (default true; false = dry-run), limit: int (opcional) }
    """
    import sys as _sys

    script = _PROJECT_ROOT / "scripts" / "limpiar_ofertas_no_laborales.py"
    if not script.exists():
        raise HTTPException(404, "limpiar_ofertas_no_laborales.py no encontrado")

    aplicar = bool(payload.get("apply", True))
    limite = payload.get("limit")
    cmd = [_sys.executable, str(script)]
    if aplicar:
        cmd.append("--apply")
    if limite:
        cmd += ["--limit", str(int(limite))]

    logger.info(f"[admin] limpiar no laborales: {cmd}")
    try:
        proceso = _lanzar_proceso(cmd, tipo="limpiar-no-laborales")
    except Exception as exc:
        raise HTTPException(500, f"No se pudo lanzar el proceso: {exc}") from exc
    _auditar(_user, "limpiar_no_laborales", "proceso", proceso.pid, {
        "apply": aplicar, "limit": limite, "log": proceso.log_path.name,
    })
    return {
        "ok": True, "pid": proceso.pid, "apply": aplicar,
        "log": proceso.log_path.name,
    }


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
    _user: str = Depends(_require_editor),
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
    from psycopg2 import sql as _sql
    set_parts = [_sql.SQL("{} = %s").format(_sql.Identifier(c)) for c in updates]
    query = _sql.SQL("UPDATE instituciones SET {} WHERE id = %s").format(
        _sql.SQL(", ").join(set_parts),
    )
    vals = list(updates.values()) + [fuente_id]
    with get_cursor() as (conn, cur):
        cur.execute(query, vals)
        if cur.rowcount == 0:
            raise HTTPException(404, "Institución no encontrada")
        conn.commit()
    _auditar(_user, "editar_fuente", "institucion", fuente_id, {"campos": sorted(updates)})
    return {"id": fuente_id, "updated": list(updates.keys())}


@router.post(f"/api/{ADMIN_PATH}/fuentes", tags=["admin"])
def admin_crear_fuente(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
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
    _auditar(_user, "crear_fuente", "institucion", new_id, {"nombre": nombre})
    return {"id": new_id, "nombre": nombre}


@router.delete(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}", tags=["admin"])
def admin_desactivar_fuente(
    fuente_id: int,
    _user: str = Depends(_require_editor),
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

    # Override persistente en DB (sobrevive redeploys; el JSON del repo
    # queda sólo como fallback versionado).
    _set_override_db(fuente_id, status="disabled",
                     reason=f"desactivado via admin por {_user}", usuario=_user)

    _auditar(_user, "desactivar_fuente", "institucion", fuente_id, {"ofertas_cerradas": n})
    return {"id": fuente_id, "ofertas_cerradas": n}


def _set_override_db(
    institucion_id: int,
    status: str | None = None,
    kind: str | None = None,
    reason: str | None = None,
    usuario: str = "ops",
) -> None:
    """Upsert en `source_overrides` + invalidación del cache en este proceso."""
    with get_cursor() as (conn, cur):
        cur.execute(
            """INSERT INTO source_overrides
               (institucion_id, status, kind, reason, actualizado_en, actualizado_por)
               VALUES (%s, %s, %s, %s, NOW(), %s)
               ON CONFLICT (institucion_id) DO UPDATE
               SET status = EXCLUDED.status, kind = EXCLUDED.kind,
                   reason = EXCLUDED.reason, actualizado_en = NOW(),
                   actualizado_por = EXCLUDED.actualizado_por""",
            [institucion_id, status, kind, reason, usuario],
        )
        conn.commit()
    if _SOURCE_STATUS_AVAILABLE:
        with suppress(Exception):
            from scrapers.source_status import reset_overrides_cache
            reset_overrides_cache()


_OVERRIDE_STATUSES = {"active", "experimental", "manual_review", "js_required",
                      "blocked", "broken", "no_data", "disabled"}


@router.put(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}/override", tags=["admin"])
def admin_set_override(
    fuente_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Fija un override de clasificación para una fuente (persistente en DB).

    Body: { status?: str, kind?: str, reason?: str }. `status` debe ser
    uno de los SourceStatus válidos; `kind` un ScraperKind válido.
    """
    status = (payload.get("status") or "").strip() or None
    kind = (payload.get("kind") or "").strip() or None
    reason = (payload.get("reason") or "").strip() or None
    if not status and not kind:
        raise HTTPException(400, "Se requiere status o kind")
    if status and status not in _OVERRIDE_STATUSES:
        raise HTTPException(400, f"status inválido. Válidos: {sorted(_OVERRIDE_STATUSES)}")
    if kind:
        # `kind` gobierna qué módulo scraper se despacha para la fuente; un
        # valor fuera del enum quedaría persistido y rompería el dispatch.
        # Se valida contra ScraperKind, igual que en `admin_scraper_run`.
        try:
            from scrapers.source_status import ScraperKind
            _kinds_validos = {k.value for k in ScraperKind}
        except Exception:
            _kinds_validos = {
                "empleos_publicos", "wordpress", "generic",
                "custom_trabajando", "custom_hiringroom", "custom_buk",
                "custom_playwright", "custom_policia", "custom_ffaa", "skip",
            }
        if kind not in _kinds_validos:
            raise HTTPException(400, f"kind inválido. Válidos: {sorted(_kinds_validos)}")
    try:
        _set_override_db(fuente_id, status=status, kind=kind, reason=reason, usuario=_user)
    except Exception as exc:
        raise HTTPException(500, f"No se pudo guardar el override (¿migración 0004 aplicada?): {exc}") from exc
    _auditar(_user, "set_override", "institucion", fuente_id, {"status": status, "kind": kind})
    return {"id": fuente_id, "status": status, "kind": kind}


@router.delete(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}/override", tags=["admin"])
def admin_quitar_override(
    fuente_id: int,
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Elimina el override de una fuente — vuelve a la clasificación automática."""
    with get_cursor() as (conn, cur):
        cur.execute("DELETE FROM source_overrides WHERE institucion_id = %s", [fuente_id])
        count = cur.rowcount
        conn.commit()
    if _SOURCE_STATUS_AVAILABLE:
        with suppress(Exception):
            from scrapers.source_status import reset_overrides_cache
            reset_overrides_cache()
    _auditar(_user, "quitar_override", "institucion", fuente_id)
    return {"id": fuente_id, "eliminado": count > 0}


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

    # 6. Duplicados posibles: mismo cargo (normalizado) + misma institución,
    #    ambas activas. `dup_grupo` = id mínimo del grupo para agruparlas en UI.
    if not tipo or tipo == "duplicado_posible":
        rows = execute_fetch_all(
            """WITH grupos AS (
                   SELECT LOWER(TRIM(cargo)) AS cargo_n,
                          COALESCE(institucion_id::text, LOWER(TRIM(COALESCE(institucion_nombre,'')))) AS inst_n,
                          COUNT(*) AS copias,
                          MIN(id)  AS dup_grupo
                   FROM ofertas
                   WHERE activa = TRUE AND COALESCE(TRIM(cargo), '') <> ''
                   GROUP BY 1, 2
                   HAVING COUNT(*) > 1
               )
               SELECT o.id, o.cargo, o.institucion_nombre, o.fecha_cierre,
                      o.url_oferta, o.activa, g.dup_grupo, g.copias
               FROM ofertas o
               JOIN grupos g
                 ON LOWER(TRIM(o.cargo)) = g.cargo_n
                AND COALESCE(o.institucion_id::text, LOWER(TRIM(COALESCE(o.institucion_nombre,'')))) = g.inst_n
               WHERE o.activa = TRUE
               ORDER BY g.dup_grupo, o.id
               LIMIT %s""",
            [limit],
        )
        categorias["duplicado_posible"] = {"total": len(rows), "items": rows}

    # Resumen de totales por categoría
    resumen = {k: v["total"] for k, v in categorias.items()}
    return {"resumen": resumen, "categorias": categorias}


@router.post(f"/api/{ADMIN_PATH}/revision/{{oferta_id}}/marcar-revisada", tags=["admin"])
def admin_marcar_revisada(
    oferta_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
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
    _auditar(_user, "marcar_revisada", "oferta", oferta_id, {"nota": nota} if nota else None)
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
            COUNT(*) FILTER (WHERE activa=TRUE AND fecha_cierre < (NOW() AT TIME ZONE 'America/Santiago')::date) AS vencidas_activas,
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
    # PII (correos de suscriptores): el rol `lector` (solo-lectura del
    # dashboard) no debe poder listar/exportar el padrón. Se exige `editor`,
    # alineado con el trato que se le da a `/usuarios`.
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Lista de suscriptores de alertas con estadísticas."""
    where = ""
    params: list[Any] = []
    if activa is not None:
        where = "WHERE activa = %s"
        params = [activa]

    subs = execute_fetch_all(
        f"""SELECT id, email, region, termino, tipo_contrato, sector,
                   frecuencia, activa, verificada, creada_en, actualizada_en
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
            COUNT(*) FILTER(WHERE verificada)                 AS verificadas,
            COUNT(*) FILTER(WHERE NOT verificada)             AS sin_verificar,
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
    _user: str = Depends(_require_editor),
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
    _auditar(_user, "desactivar_suscripcion", "suscripcion", sub_id)
    return {"id": sub_id, "desactivada": True}


@router.post(f"/api/{ADMIN_PATH}/alertas/test-email", tags=["admin"])
def admin_test_email(
    payload: dict[str, Any],
    _user: str = Depends(_require_editor),
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
    _auditar(_user, "test_email", "email", email, {"ok": result.get("ok")})
    return {"ok": result.get("ok"), "email": email, "ofertas": len(ofertas), "resend_id": result.get("id"), "error": result.get("error")}


@router.get(f"/api/{ADMIN_PATH}/alertas/eventos", tags=["admin"])
def admin_email_eventos(
    limit: int = Query(50, ge=1, le=500),
    email: str | None = Query(None),
    # Expone correos de destinatarios (PII): mínimo rol `editor`.
    _user: str = Depends(_require_editor),
) -> dict[str, Any]:
    """Métricas de entrega de emails (webhooks de Resend) + últimos eventos.

    Requiere la migración 0004 (tabla email_eventos) y el webhook
    configurado en Resend apuntando a `POST /api/webhooks/resend` con
    `RESEND_WEBHOOK_SECRET` seteado.
    """
    try:
        resumen = execute_fetch_one(
            """SELECT
                COUNT(*) FILTER (WHERE evento LIKE '%%sent%%')      AS enviados,
                COUNT(*) FILTER (WHERE evento LIKE '%%delivered%%') AS entregados,
                COUNT(*) FILTER (WHERE evento LIKE '%%bounce%%')    AS rebotes,
                COUNT(*) FILTER (WHERE evento LIKE '%%open%%')      AS aperturas,
                COUNT(*) FILTER (WHERE evento LIKE '%%click%%')     AS clics,
                COUNT(*) FILTER (WHERE evento LIKE '%%complain%%')  AS quejas,
                COUNT(*) AS total,
                MIN(ts) AS desde,
                MAX(ts) AS ultimo
               FROM email_eventos
               WHERE ts >= NOW() - INTERVAL '30 days'""",
            [],
        ) or {}
        where = "WHERE email ILIKE %s" if email else ""
        params: list[Any] = [f"%{email}%"] if email else []
        eventos = execute_fetch_all(
            f"""SELECT id, ts, evento, email, resend_id, asunto
                FROM email_eventos
                {where}
                ORDER BY ts DESC
                LIMIT %s""",
            params + [limit],
        )
        webhook_listo = bool(os.getenv("RESEND_WEBHOOK_SECRET"))
        return {"resumen": resumen, "eventos": eventos, "webhook_configurado": webhook_listo}
    except Exception as exc:
        logger.warning(f"[admin] email_eventos no disponible: {exc}")
        return {
            "resumen": {}, "eventos": [],
            "webhook_configurado": bool(os.getenv("RESEND_WEBHOOK_SECRET")),
            "warning": "Tabla email_eventos no disponible — aplicar `alembic upgrade head`.",
        }


@router.get(f"/api/{ADMIN_PATH}/suscripciones/export", tags=["admin"])
def admin_export_suscripciones(
    # Exporta el padrón completo de correos (PII): mínimo rol `editor`.
    _user: str = Depends(_require_editor),
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


# ── Admin: gestión de usuarios del panel (solo rol admin) ────────────────────

@router.get(f"/api/{ADMIN_PATH}/usuarios", tags=["admin"])
def admin_listar_usuarios(_user: str = Depends(_require_admin)) -> dict[str, Any]:
    """Lista las cuentas del panel (sin exponer el hash de contraseña)."""
    try:
        rows = execute_fetch_all(
            """SELECT id, usuario, nombre, rol, activo, creado_en, ultimo_login
               FROM admin_usuarios ORDER BY usuario ASC""",
            [],
        )
        return {"usuarios": rows}
    except Exception as exc:
        logger.warning(f"[usuarios] tabla no disponible: {exc}")
        return {
            "usuarios": [],
            "warning": "Tabla admin_usuarios no disponible — aplicar `alembic upgrade head`.",
        }


@router.post(f"/api/{ADMIN_PATH}/usuarios", tags=["admin"])
def admin_crear_usuario(
    payload: dict[str, Any],
    _user: str = Depends(_require_admin),
) -> dict[str, Any]:
    """Crea una cuenta del panel. Requeridos: usuario, password. rol opcional."""
    usuario = (payload.get("usuario") or "").strip()
    password = str(payload.get("password") or "")
    rol = (payload.get("rol") or "editor").strip()
    nombre = (payload.get("nombre") or "").strip() or None
    if not usuario or len(usuario) < 3:
        raise HTTPException(400, "El usuario debe tener al menos 3 caracteres")
    if len(password) < 8:
        raise HTTPException(400, "La contraseña debe tener al menos 8 caracteres")
    if rol not in ROLES_VALIDOS:
        raise HTTPException(400, f"rol inválido. Válidos: {list(ROLES_VALIDOS)}")
    if usuario.lower() == "ops":
        raise HTTPException(400, "'ops' está reservado para la contraseña maestra")
    try:
        with get_cursor() as (conn, cur):
            cur.execute(
                """INSERT INTO admin_usuarios (usuario, nombre, password_hash, rol, activo)
                   VALUES (%s, %s, %s, %s, TRUE)
                   ON CONFLICT (LOWER(usuario)) DO NOTHING RETURNING id""",
                [usuario, nombre, hash_password(password), rol],
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(409, "Ya existe un usuario con ese nombre")
            conn.commit()
            nuevo_id = row["id"] if isinstance(row, dict) else row[0]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"No se pudo crear el usuario (¿migración 0003 aplicada?): {exc}") from exc
    _auditar(_user, "crear_usuario", "usuario", nuevo_id, {"usuario": usuario, "rol": rol})
    return {"id": nuevo_id, "usuario": usuario, "rol": rol}


def _es_ultimo_admin_activo(cur, usuario_id: int) -> bool:
    """True si `usuario_id` es un admin activo y el ÚNICO que queda.

    Evita que una edición/baja deje el panel sin administradores nominales. La
    contraseña maestra `ops` sigue siendo un respaldo, pero perder a todos los
    admins con cuenta propia obliga a operar con la maestra, que es lo que se
    quiere evitar.
    """
    cur.execute(
        "SELECT COUNT(*) AS n FROM admin_usuarios WHERE rol = 'admin' AND activo = TRUE"
    )
    row = cur.fetchone()
    total = int((row["n"] if isinstance(row, dict) else row[0]) or 0)
    if total > 1:
        return False
    cur.execute(
        "SELECT 1 FROM admin_usuarios WHERE id = %s AND rol = 'admin' AND activo = TRUE",
        [usuario_id],
    )
    return cur.fetchone() is not None


@router.put(f"/api/{ADMIN_PATH}/usuarios/{{usuario_id}}", tags=["admin"])
def admin_editar_usuario(
    usuario_id: int,
    payload: dict[str, Any],
    _user: str = Depends(_require_admin),
) -> dict[str, Any]:
    """Edita una cuenta: nombre, rol, activo y, opcionalmente, su contraseña."""
    updates: dict[str, Any] = {}
    if "nombre" in payload:
        updates["nombre"] = (payload.get("nombre") or "").strip() or None
    if "rol" in payload:
        rol = (payload.get("rol") or "").strip()
        if rol not in ROLES_VALIDOS:
            raise HTTPException(400, f"rol inválido. Válidos: {list(ROLES_VALIDOS)}")
        updates["rol"] = rol
    if "activo" in payload:
        updates["activo"] = bool(payload.get("activo"))
    if payload.get("password"):
        if len(str(payload["password"])) < 8:
            raise HTTPException(400, "La contraseña debe tener al menos 8 caracteres")
        updates["password_hash"] = hash_password(str(payload["password"]))
    if not updates:
        raise HTTPException(400, "Sin campos para actualizar")
    # No dejar el panel sin administradores: si esta edición quita el rol admin
    # (degradación) o desactiva la cuenta, y es el último admin activo, se rechaza.
    quita_admin = (updates.get("rol") not in (None, "admin")) or (updates.get("activo") is False)
    if quita_admin:
        with get_cursor() as (_c, _cur):
            if _es_ultimo_admin_activo(_cur, usuario_id):
                raise HTTPException(
                    409,
                    "No puedes quitar el rol admin ni desactivar la única cuenta de "
                    "administrador activa. Crea o promueve otro admin primero.",
                )
    from psycopg2 import sql as _sql
    set_parts = [_sql.SQL("{} = %s").format(_sql.Identifier(c)) for c in updates]
    query = _sql.SQL("UPDATE admin_usuarios SET {} WHERE id = %s").format(
        _sql.SQL(", ").join(set_parts),
    )
    with get_cursor() as (conn, cur):
        cur.execute(query, list(updates.values()) + [usuario_id])
        if cur.rowcount == 0:
            raise HTTPException(404, "Usuario no encontrado")
        conn.commit()
    _auditar(_user, "editar_usuario", "usuario", usuario_id,
             {"campos": sorted(k for k in updates if k != "password_hash")})
    return {"id": usuario_id, "updated": [k for k in updates if k != "password_hash"]}


@router.delete(f"/api/{ADMIN_PATH}/usuarios/{{usuario_id}}", tags=["admin"])
def admin_borrar_usuario(
    usuario_id: int,
    _user: str = Depends(_require_admin),
) -> dict[str, Any]:
    """Elimina una cuenta del panel."""
    with get_cursor() as (conn, cur):
        # No dejar el panel sin administradores nominales.
        if _es_ultimo_admin_activo(cur, usuario_id):
            raise HTTPException(
                409,
                "No puedes eliminar la única cuenta de administrador activa. "
                "Crea o promueve otro admin primero.",
            )
        cur.execute("DELETE FROM admin_usuarios WHERE id = %s", [usuario_id])
        if cur.rowcount == 0:
            raise HTTPException(404, "Usuario no encontrado")
        conn.commit()
    _auditar(_user, "borrar_usuario", "usuario", usuario_id)
    return {"id": usuario_id, "deleted": True}


# ── Admin: programador de recolecciones (config solo rol admin) ──────────────

@router.get(f"/api/{ADMIN_PATH}/scheduler", tags=["admin"])
def admin_get_scheduler(_user: str = Depends(_verify_admin_jwt)) -> dict[str, Any]:
    """Estado actual del programador automático de recolecciones."""
    from api.services.scheduler import get_estado
    estado = get_estado()
    if estado is None:
        return {
            "disponible": False,
            "warning": "Tabla scheduler_state no disponible — aplicar `alembic upgrade head`.",
        }
    return {"disponible": True, "estado": estado}


@router.put(f"/api/{ADMIN_PATH}/scheduler", tags=["admin"])
def admin_set_scheduler(
    payload: dict[str, Any],
    _user: str = Depends(_require_admin),
) -> dict[str, Any]:
    """Configura el programador: activo, intervalo_horas, modo, limite_fuentes."""
    from api.services.scheduler import set_estado, _UNSET
    try:
        estado = set_estado(
            activo=payload.get("activo") if "activo" in payload else None,
            intervalo_horas=payload.get("intervalo_horas") if "intervalo_horas" in payload else None,
            modo=payload.get("modo") if "modo" in payload else None,
            limite_fuentes=payload.get("limite_fuentes") if "limite_fuentes" in payload else _UNSET,
            usuario=_user,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    except Exception as exc:
        raise HTTPException(500, f"No se pudo guardar (¿migración 0003 aplicada?): {exc}") from exc
    _auditar(_user, "config_scheduler", "scheduler", 1, {
        k: payload.get(k) for k in ("activo", "intervalo_horas", "modo", "limite_fuentes") if k in payload
    })
    return {"ok": True, "estado": estado}


# ── Fin endpoints admin ────────────────────────────────────────────────────


