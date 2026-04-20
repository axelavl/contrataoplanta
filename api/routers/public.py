"""Endpoints públicos de la API (sin `ADMIN_PATH`).

Incluye: listado y detalle de ofertas, estadísticas, instituciones,
historial, sugerencias, suscripción a alertas (POST), scraper status
público, regiones/comunas, lookup de leyes BCN, validación de email,
búsqueda full-text, autocompletar y el index `/api` con el map de
endpoints.

Los endpoints de administración (`/api/{ADMIN_PATH}/...`) siguen por
ahora en `api/main.py`; se moverán a `api/routers/admin.py` en un PR
siguiente.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import io
import json
import math
import os
import re
import secrets
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus, urlencode, urlparse

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, EmailStr

from api.deps import (
    ADMIN_PATH,
    DEFAULT_OG_IMAGE,
    SITE_URL,
    WEB_INDEX_PATH,
    _PROJECT_ROOT,
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
    EMAIL_RE,
    _format_fecha_larga,
    _format_renta_bruta,
    _slugify,
    _truncate_text,
    dias_restantes,
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
from api.services.seo import (
    build_offer_meta,
    fetch_offer_for_meta,
    serialize_offer,
)
from api.services.regiones import get_comunas, get_regiones
from api.services.leyes import buscar_ley_bcn, get_ley_institucion
from api.services.mailcheck import validar_email as mailcheck_validar
from api.services.email_alerts import enviar_alerta_ofertas, enviar_verificacion
from api.services.meilisearch_svc import (
    autocompletar as meili_autocompletar,
    buscar as meili_buscar,
    configurar_indice as meili_configurar,
    indexar_ofertas as meili_indexar,
)

# Clasificación de fuentes de scraping — import opcional: si el
# paquete `scrapers` no está disponible (tests, entornos minimal), el
# endpoint /api/scraper/resumen devuelve un payload "disponible=False".
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

# Import lazy de og_image: sólo cuando se invoca /api/og/{id}.png.
# Se hace dentro del endpoint para no cargar Pillow al startup si no
# hace falta.

router = APIRouter(tags=["public"])


# ═══════════════════════════════════════════════════════════════════════════
#  Pydantic / dataclass models usadas por los endpoints públicos
# ═══════════════════════════════════════════════════════════════════════════

class AlertaPayload(BaseModel):
    email: str
    region: str | None = None
    termino: str | None = None
    tipo_contrato: str | None = None
    sector: str | None = None
    frecuencia: str | None = "diaria"


@dataclass(slots=True)
class Paginacion:
    pagina: int
    por_pagina: int

    @property
    def offset(self) -> int:
        return (self.pagina - 1) * self.por_pagina

@router.get("/api/ofertas")
def get_ofertas(
    q: str | None = Query(None),
    region: str | None = Query(None),
    sector: str | None = Query(None),
    tipo: str | None = Query(None),
    institucion: int | None = Query(None, description="ID de institución específica"),
    area_profesional: str | None = Query(None),
    renta_min: int | None = Query(None, ge=0),
    ciudad: str | None = Query(None),
    comunas: str | None = Query(None, description="Lista de comunas separadas por coma"),
    cierra_pronto: bool = Query(False),
    nuevas: bool = Query(False),
    vista: str = Query("vigentes", pattern="^(vigentes|cerradas|todas)$"),
    orden: str = Query("recientes"),
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    pag = Paginacion(pagina=pagina, por_pagina=por_pagina)
    only_active = vista == "vigentes"
    only_closed = vista == "cerradas"
    where_sql, params = build_ofertas_filters(
        q=q,
        region=region,
        sector=sector,
        tipo=tipo,
        institucion_id=institucion,
        area_profesional=area_profesional,
        renta_min=renta_min,
        ciudad=ciudad,
        comunas=comunas,
        cierra_pronto=cierra_pronto,
        nuevas=nuevas,
        solo_activas=only_active,
        closed_only=only_closed,
    )

    # Ofertas sin fecha_cierre van al final;
    # dentro de cada grupo se ordena normalmente.
    sin_fechas = "CASE WHEN fecha_cierre IS NULL THEN 1 ELSE 0 END ASC"
    order_sql = {
        "recientes":  f"{sin_fechas}, fecha_scraped DESC NULLS LAST, id DESC",
        "cierre":     f"{sin_fechas}, fecha_cierre ASC NULLS LAST, id DESC",
        "renta_desc": f"{sin_fechas}, renta_bruta_max DESC NULLS LAST, renta_bruta_min DESC NULLS LAST, id DESC",
        "renta":      f"{sin_fechas}, renta_bruta_max DESC NULLS LAST, renta_bruta_min DESC NULLS LAST, id DESC",
        "renta_asc":  f"{sin_fechas}, LEAST(COALESCE(renta_bruta_min, renta_bruta_max), COALESCE(renta_bruta_max, renta_bruta_min)) ASC NULLS LAST, id DESC",
        "az":         f"{sin_fechas}, cargo ASC NULLS LAST, id ASC",
    }.get(orden, f"{sin_fechas}, fecha_scraped DESC NULLS LAST, id DESC")

    select_sql = f"""
    WITH base AS (
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        {where_sql}
    )
    SELECT * FROM base
    ORDER BY {order_sql}
    LIMIT %s OFFSET %s
    """
    count_sql = f"""
    SELECT COUNT(*) AS total
    {ofertas_base_sql()}
    {where_sql}
    """
    rows = execute_fetch_all(select_sql, [*params, pag.por_pagina, pag.offset])
    total_row = execute_fetch_one(count_sql, params)
    total = int(total_row["total"]) if total_row else 0
    paginas = math.ceil(total / pag.por_pagina) if total else 0

    return {
        "total": total,
        "pagina": pag.pagina,
        "por_pagina": pag.por_pagina,
        "paginas": paginas,
        "ofertas": [serialize_offer(row) for row in rows],
    }


@router.get("/api/ofertas/{oferta_id}")
def get_oferta(oferta_id: int) -> dict[str, Any]:
    sql = f"""
    WITH base AS (
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        WHERE o.id = %s
    )
    SELECT * FROM base
    """
    row = execute_fetch_one(sql, [oferta_id])
    if not row:
        raise HTTPException(status_code=404, detail="Oferta no encontrada")
    return serialize_offer(row)


# Versión del renderer. Se incluye en el ETag para invalidar cachés de CDN
# cuando cambiamos el layout de la tarjeta.
_OG_RENDERER_VERSION = "v2"


@router.get("/api/og/{oferta_id}.png")
def og_image_oferta(
    oferta_id: int,
    request: Request,
    format: str = Query("horizontal", pattern="^(horizontal|square)$"),
) -> Response:
    """Imagen OG/Twitter/RRSS dinámica para una oferta concreta.

    - ``format=horizontal`` (default) → 1200x630 Open Graph / Twitter card,
      apto para WhatsApp, LinkedIn, Facebook y X.
    - ``format=square`` → 1080x1080, pensado para Instagram (stories/feed)
      y mensajería cuadrada. Se puede descargar como activo desde el modal
      "Compartir en Instagram" del frontend.

    Responde con `Cache-Control` agresivo y un ETag compuesto por la versión
    del renderer, el formato y la última actualización de la oferta — así el
    CDN (Cloudflare Pages / Railway) puede revalidar con 304 cuando la oferta
    no ha cambiado.
    """
    sql = f"""
    WITH base AS (
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        WHERE o.id = %s
    )
    SELECT * FROM base
    """
    row = execute_fetch_one(sql, [oferta_id])
    if not row:
        raise HTTPException(status_code=404, detail="Oferta no encontrada")
    oferta = serialize_offer(row)

    # ETag derivado del estado mutable de la oferta: cuando la institución
    # actualiza la oferta o cambia el estado (active → closing_today →
    # closed) el cliente debe revalidar. `dias_restantes` cambia día a día
    # pero el Cache-Control de max-age=86400 cubre ese ciclo.
    actualizado = oferta.get("fecha_actualizado") or oferta.get("fecha_scraped")
    etag_seed = f"{_OG_RENDERER_VERSION}:{format}:{oferta_id}:{actualizado}:{oferta.get('estado')}"
    etag = '"' + hashlib.md5(etag_seed.encode("utf-8")).hexdigest() + '"'

    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag})

    try:
        from api.services.og_image import render_offer_card
        png = render_offer_card(oferta, fmt=format)  # type: ignore[arg-type]
    except ImportError as exc:  # Pillow no instalado
        raise HTTPException(status_code=503, detail=f"Generador OG no disponible: {exc}") from exc

    filename = f"oferta-{oferta_id}-{format}.png"
    return Response(
        content=png,
        media_type="image/png",
        headers={
            "Cache-Control": "public, max-age=86400, stale-while-revalidate=604800",
            "ETag": etag,
            # Permite que el modal "Descargar imagen" del frontend guarde
            # directamente con un nombre descriptivo.
            "Content-Disposition": f'inline; filename="{filename}"',
        },
    )


@router.get("/api/estadisticas")
def get_estadisticas() -> dict[str, Any]:
    ultima_actualizacion_row = execute_fetch_one(
        """
        SELECT MAX(COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en)) AS ultima_actualizacion
        FROM ofertas
        """
    ) or {}

    conteos = execute_fetch_one(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE {ACTIVE_OFFER_SQL.replace('o.', '')}) AS activas_hoy,
            COUNT(*) FILTER (
                WHERE COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en) >= NOW() - INTERVAL '48 hours'
                  AND {ACTIVE_OFFER_SQL.replace('o.', '')}
            ) AS nuevas_48h,
            COUNT(*) FILTER (
                WHERE fecha_cierre = CURRENT_DATE
                  AND {ACTIVE_OFFER_SQL.replace('o.', '')}
            ) AS cierran_hoy,
            COUNT(DISTINCT institucion_id) FILTER (WHERE {ACTIVE_OFFER_SQL.replace('o.', '')}) AS instituciones_activas
        FROM ofertas o
        """
    ) or {}

    por_sector = execute_fetch_all(
        f"""
        SELECT
            COALESCE(i.sector, o.sector, i.tipo, 'Sin sector') AS sector,
            COUNT(*) AS total
        {ofertas_base_sql()}
        WHERE {ACTIVE_OFFER_SQL}
        GROUP BY 1
        ORDER BY total DESC, sector ASC
        LIMIT 8
        """
    )

    historico_mensual = execute_fetch_all(
        """
        SELECT
            TO_CHAR(DATE_TRUNC('month', COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en)), 'YYYY-MM') AS mes,
            COUNT(*) AS total
        FROM ofertas
        WHERE COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en) >= NOW() - INTERVAL '12 months'
        GROUP BY 1
        ORDER BY mes ASC
        """
    )

    mas_activas = execute_fetch_all(
        f"""
        SELECT
            i.id,
            COALESCE(i.nombre, 'Sin institucion') AS nombre,
            COUNT(*) AS activas,
            COUNT(*) FILTER (
                WHERE COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) >= NOW() - INTERVAL '7 days'
            ) AS nuevas_semana
        {ofertas_base_sql()}
        WHERE {ACTIVE_OFFER_SQL}
        GROUP BY i.id, i.nombre
        ORDER BY activas DESC, nuevas_semana DESC, nombre ASC
        LIMIT 5
        """
    )

    return {
        "activas_hoy": int(conteos.get("activas_hoy") or 0),
        "nuevas_48h": int(conteos.get("nuevas_48h") or 0),
        "cierran_hoy": int(conteos.get("cierran_hoy") or 0),
        "instituciones_activas": int(conteos.get("instituciones_activas") or 0),
        "ultima_actualizacion": ultima_actualizacion_row.get("ultima_actualizacion"),
        "por_sector": por_sector,
        "historico_mensual": historico_mensual,
        "mas_activas": mas_activas,
    }


@router.get("/api/instituciones")
def get_instituciones(
    q: str | None = Query(None),
    sector: str | None = Query(None),
    region: str | None = Query(None),
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    pag = Paginacion(pagina=pagina, por_pagina=por_pagina)
    where = ["1=1"]
    params: list[Any] = []

    if q:
        where.append("(COALESCE(i.nombre, '') ILIKE %s OR COALESCE(i.sigla, i.nombre_corto, '') ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])
    if sector:
        where.append("COALESCE(i.sector, i.tipo, '') ILIKE %s")
        params.append(f"%{sector}%")
    if region:
        where.append("COALESCE(i.region, '') ILIKE %s")
        params.append(f"%{region}%")

    where_sql = " AND ".join(where)
    sql = f"""
    SELECT
        i.id,
        i.nombre,
        COALESCE(i.sigla, i.nombre_corto) AS sigla,
        COALESCE(i.sector, i.tipo) AS sector,
        i.region,
        i.url_empleo,
        i.plataforma_empleo,
        COUNT(o.id) FILTER (WHERE {ACTIVE_OFFER_SQL}) AS activas
    FROM instituciones i
    LEFT JOIN ofertas o ON o.institucion_id = i.id
    WHERE {where_sql}
    GROUP BY i.id, i.nombre, i.sigla, i.nombre_corto, i.sector, i.tipo, i.region, i.url_empleo, i.plataforma_empleo
    ORDER BY activas DESC, i.nombre ASC
    LIMIT %s OFFSET %s
    """
    count_sql = f"SELECT COUNT(*) AS total FROM instituciones i WHERE {where_sql}"

    rows = execute_fetch_all(sql, [*params, pag.por_pagina, pag.offset])
    total_row = execute_fetch_one(count_sql, params)
    total = int(total_row["total"]) if total_row else 0
    paginas = math.ceil(total / pag.por_pagina) if total else 0

    return {
        "total": total,
        "pagina": pag.pagina,
        "por_pagina": pag.por_pagina,
        "paginas": paginas,
        "instituciones": rows,
    }


@router.get("/api/instituciones/{institucion_id}/ofertas")
def get_institucion_ofertas(
    institucion_id: int,
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    pag = Paginacion(pagina=pagina, por_pagina=por_pagina)
    where_sql, params = build_ofertas_filters(institucion_id=institucion_id, solo_activas=True)
    sql = f"""
    WITH base AS (
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        {where_sql}
    )
    SELECT * FROM base
    ORDER BY CASE WHEN fecha_cierre IS NULL THEN 1 ELSE 0 END ASC, fecha_cierre ASC NULLS LAST, fecha_scraped DESC NULLS LAST
    LIMIT %s OFFSET %s
    """
    count_sql = f"SELECT COUNT(*) AS total {ofertas_base_sql()} {where_sql}"
    rows = execute_fetch_all(sql, [*params, pag.por_pagina, pag.offset])
    total_row = execute_fetch_one(count_sql, params)
    total = int(total_row["total"]) if total_row else 0
    paginas = math.ceil(total / pag.por_pagina) if total else 0

    return {
        "total": total,
        "pagina": pag.pagina,
        "por_pagina": pag.por_pagina,
        "paginas": paginas,
        "ofertas": [serialize_offer(row) for row in rows],
    }


@router.get("/api/instituciones/{institucion_id}/estadisticas")
def get_institucion_estadisticas(institucion_id: int) -> dict[str, Any]:
    total_historico = execute_fetch_one(
        "SELECT COUNT(*) AS total FROM ofertas WHERE institucion_id = %s",
        [institucion_id],
    )
    promedio_por_mes = execute_fetch_one(
        """
        SELECT ROUND(AVG(total_mes), 2) AS promedio
        FROM (
            SELECT DATE_TRUNC('month', COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en)) AS mes, COUNT(*) AS total_mes
            FROM ofertas
            WHERE institucion_id = %s
            GROUP BY 1
        ) sub
        """,
        [institucion_id],
    )
    tipos = execute_fetch_all(
        """
        SELECT
            COALESCE(NULLIF(tipo_contrato, ''), NULLIF(tipo_cargo, ''), 'sin_dato') AS tipo,
            COUNT(*) AS total
        FROM ofertas
        WHERE institucion_id = %s
        GROUP BY 1
        ORDER BY total DESC, tipo ASC
        LIMIT 5
        """,
        [institucion_id],
    )
    cargos = execute_fetch_all(
        """
        SELECT cargo, COUNT(*) AS total
        FROM ofertas
        WHERE institucion_id = %s
        GROUP BY cargo
        ORDER BY total DESC, cargo ASC
        LIMIT 10
        """,
        [institucion_id],
    )
    return {
        "institucion_id": institucion_id,
        "total_historico": int(total_historico["total"]) if total_historico else 0,
        "promedio_por_mes": float(promedio_por_mes["promedio"]) if promedio_por_mes and promedio_por_mes["promedio"] is not None else 0,
        "tipos_contrato_frecuentes": tipos,
        "cargos_frecuentes": cargos,
    }


@router.get("/api/historial")
def get_historial(
    institucion_id: int | None = Query(None),
    sector: str | None = Query(None),
    region: str | None = Query(None),
    tipo: str | None = Query(None),
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    pag = Paginacion(pagina=pagina, por_pagina=por_pagina)
    where_sql, params = build_ofertas_filters(
        region=region,
        sector=sector,
        tipo=tipo,
        institucion_id=institucion_id,
        solo_activas=False,
        closed_only=True,
    )
    sql = f"""
    WITH base AS (
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        {where_sql}
    )
    SELECT * FROM base
    ORDER BY fecha_scraped DESC NULLS LAST
    LIMIT %s OFFSET %s
    """
    count_sql = f"SELECT COUNT(*) AS total {ofertas_base_sql()} {where_sql}"
    rows = execute_fetch_all(sql, [*params, pag.por_pagina, pag.offset])
    total_row = execute_fetch_one(count_sql, params)
    total = int(total_row["total"]) if total_row else 0
    paginas = math.ceil(total / pag.por_pagina) if total else 0

    return {
        "total": total,
        "pagina": pag.pagina,
        "por_pagina": pag.por_pagina,
        "paginas": paginas,
        "historial": [serialize_offer(row) for row in rows],
    }


@router.get("/api/sugerencias")
def get_sugerencias(q: str = Query(..., min_length=1, max_length=100)) -> list[str]:
    rows = execute_fetch_all(
        """
        SELECT cargo
        FROM ofertas
        WHERE cargo ILIKE %s
        GROUP BY cargo
        ORDER BY COUNT(*) DESC, cargo ASC
        LIMIT 10
        """,
        [f"{q}%"],
    )
    return [row["cargo"] for row in rows]


@router.post("/api/alertas")
def crear_alerta(payload: AlertaPayload) -> dict[str, Any]:
    email         = validate_email(payload.email)
    region        = payload.region.strip()        if payload.region        else None
    termino       = payload.termino.strip()       if payload.termino       else None
    tipo_contrato = payload.tipo_contrato.strip() if payload.tipo_contrato else None
    sector        = payload.sector.strip()        if payload.sector        else None
    frecuencia    = (payload.frecuencia or "diaria").strip().lower()
    if frecuencia not in ("diaria", "semanal"):
        frecuencia = "diaria"

    # Mailcheck: validate email quality
    check = mailcheck_validar(email)
    if not check["valido"]:
        raise HTTPException(status_code=422, detail=check["motivo"])

    with get_cursor() as (connection, cursor):
        cursor.execute(
            """
            UPDATE alertas_suscripciones
            SET activa = TRUE, frecuencia = %s, actualizada_en = NOW()
            WHERE LOWER(email) = LOWER(%s)
              AND COALESCE(region, '')        = COALESCE(%s, '')
              AND COALESCE(termino, '')       = COALESCE(%s, '')
              AND COALESCE(tipo_contrato, '') = COALESCE(%s, '')
              AND COALESCE(sector, '')        = COALESCE(%s, '')
            """,
            [frecuencia, email, region, termino, tipo_contrato, sector],
        )
        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO alertas_suscripciones (
                    email, region, termino, tipo_contrato, sector, frecuencia,
                    activa, creada_en, actualizada_en
                ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
                """,
                [email, region, termino, tipo_contrato, sector, frecuencia],
            )
        connection.commit()

    response: dict[str, Any] = {"ok": True, "mensaje": "Alerta registrada correctamente"}
    if check.get("sugerencia"):
        response["sugerencia_email"] = check["sugerencia"]
    return response


# ──────────────────── Scraper sources (catálogo + clasificación) ───────────

_CATALOG_PATH = _PROJECT_ROOT / "repositorio_instituciones_publicas_chile.json"
_catalog_cache: dict[str, Any] = {"mtime": 0.0, "enriched": None}


def _load_catalog_enriched() -> list[tuple[dict[str, Any], Any]] | None:
    """Carga el catálogo maestro y lo clasifica. Cacheado por mtime del archivo."""
    if not _SOURCE_STATUS_AVAILABLE or not _CATALOG_PATH.exists():
        return None
    try:
        mtime = _CATALOG_PATH.stat().st_mtime
    except OSError:
        return None
    if _catalog_cache["enriched"] is not None and _catalog_cache["mtime"] == mtime:
        return _catalog_cache["enriched"]
    try:
        payload = json.loads(_CATALOG_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        return None
    insts = payload.get("instituciones") if isinstance(payload, dict) else payload
    if not isinstance(insts, list):
        return None
    enriched = enrich_with_status(insts)
    _catalog_cache["mtime"] = mtime
    _catalog_cache["enriched"] = enriched
    return enriched


@router.get("/api/scraper/resumen")
def get_scraper_resumen() -> dict[str, Any]:
    """
    Resumen operacional de fuentes del catálogo maestro:
    cuántas están active / experimental / manual_review / etc.
    Sirve para que el frontend muestre honestamente el estado de cobertura.
    """
    enriched = _load_catalog_enriched()
    if enriched is None:
        return {
            "disponible": False,
            "total": 0,
            "por_status": {},
            "por_kind": {},
            "cobertura_activa_pct": 0.0,
        }

    total = len(enriched)
    status_counts = status_breakdown(enriched)
    kind_counts = kind_breakdown(enriched)

    activas = (
        status_counts.get(SourceStatus.ACTIVE.value, 0)
        if _SOURCE_STATUS_AVAILABLE
        else 0
    )
    cobertura = round((activas / total) * 100, 1) if total else 0.0

    return {
        "disponible": True,
        "total": total,
        "activas": activas,
        "por_status": status_counts,
        "por_kind": {k: v for k, v in kind_counts.items() if v},
        "cobertura_activa_pct": cobertura,
    }


@router.get("/api/scraper/fuentes")
def get_scraper_fuentes(
    status: str | None = Query(None, description="Filtrar por status"),
    kind: str | None = Query(None, description="Filtrar por kind"),
    limit: int = Query(200, ge=1, le=1000),
) -> dict[str, Any]:
    """
    Lista detallada de fuentes del catálogo con su clasificación operativa.
    Pensado para la vista de administración/transparencia del scraper.
    """
    enriched = _load_catalog_enriched()
    if enriched is None:
        return {"disponible": False, "total": 0, "fuentes": []}

    rows: list[dict[str, Any]] = []
    for inst, decision in enriched:
        if status and decision.status.value != status:
            continue
        if kind and decision.kind.value != kind:
            continue
        rows.append(
            {
                "id": inst.get("id"),
                "nombre": inst.get("nombre"),
                "sigla": inst.get("sigla"),
                "sector": inst.get("sector"),
                "region": inst.get("region"),
                "plataforma_declarada": inst.get("plataforma_empleo"),
                "url_empleo": inst.get("url_empleo"),
                "sitio_web": inst.get("sitio_web"),
                **decision.as_dict(),
            }
        )
        if len(rows) >= limit:
            break

    return {
        "disponible": True,
        "total": len(rows),
        "fuentes": rows,
    }


# ──────────────────── Regiones y Comunas (DPA API) ──────────────────────────

@router.get("/api/regiones")
async def api_regiones() -> list[dict[str, Any]]:
    """Regiones de Chile con nombres oficiales (API DPA del Estado)."""
    return await get_regiones()


@router.get("/api/regiones/{codigo_region}/comunas")
async def api_comunas(codigo_region: str) -> list[dict[str, Any]]:
    """Comunas de una región específica (API DPA del Estado)."""
    return await get_comunas(codigo_region)


# ──────────────────── Leyes por institución (BCN Ley Chile) ─────────────────

@router.get("/api/instituciones/{institucion_id}/ley")
def api_institucion_ley(institucion_id: int) -> dict[str, Any]:
    """Ley orgánica que rige a una institución, con enlace a BCN LeyChile."""
    inst = execute_fetch_one(
        """
        SELECT i.nombre, COALESCE(i.sigla, i.nombre_corto) AS sigla,
               COALESCE(i.sector, i.tipo) AS sector
        FROM instituciones i WHERE i.id = %s
        """,
        [institucion_id],
    )
    if not inst:
        raise HTTPException(status_code=404, detail="Institución no encontrada")

    ley = get_ley_institucion(
        nombre=inst["nombre"],
        sigla=inst.get("sigla"),
        sector=inst.get("sector"),
    )
    return {
        "institucion_id": institucion_id,
        "institucion": inst["nombre"],
        **ley,
    }


@router.get("/api/leyes/buscar")
async def api_buscar_ley(q: str = Query(..., min_length=2, max_length=200)) -> list[dict[str, Any]]:
    """Buscar normativa en BCN LeyChile."""
    return await buscar_ley_bcn(q)


# ──────────────────── Validación de email (Mailcheck) ───────────────────────

@router.get("/api/validar-email")
def api_validar_email(email: str = Query(..., min_length=3, max_length=200)) -> dict[str, Any]:
    """
    Valida un email: detecta dominios temporales/desechables y sugiere
    correcciones de typos comunes (gmial→gmail, hotnail→hotmail).
    """
    return mailcheck_validar(email)


# ──────────────────── Búsqueda rápida (Meilisearch) ─────────────────────────

@router.get("/api/buscar")
def api_buscar_meili(
    q: str = Query(..., min_length=1, max_length=200),
    region: str | None = Query(None),
    sector: str | None = Query(None),
    tipo: str | None = Query(None),
    limite: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """
    Búsqueda rápida con Meilisearch (~10ms).
    Soporta sinónimos ("RRHH" → "Recursos Humanos"), tolerancia a typos,
    y resultados con highlights.
    """
    filtros = {}
    if region:
        filtros["region"] = region
    if sector:
        filtros["sector"] = sector
    if tipo:
        filtros["tipo_contrato"] = tipo
    filtros["activo"] = "true"

    return meili_buscar(q, filtros=filtros, limite=limite, offset=offset)


@router.get("/api/autocompletar")
def api_autocompletar(
    q: str = Query(..., min_length=1, max_length=100),
    limite: int = Query(8, ge=1, le=20),
) -> list[dict[str, str]]:
    """
    Autocompletado instantáneo de cargos con Meilisearch.
    Retorna sugerencias con highlights y contexto.
    """
    return meili_autocompletar(q, limite=limite)




@router.get("/api", include_in_schema=False)
def root() -> dict[str, Any]:
    return {
        "nombre": "contrata o planta .cl - API",
        "version": "3.0.0",
        "docs": "/docs",
        "db_host": DB_CONFIG["host"],
        "endpoints": [
            "GET /api/ofertas",
            "GET /api/ofertas/{id}",
            "GET /api/estadisticas",
            "GET /api/instituciones",
            "GET /api/instituciones/{id}/ofertas",
            "GET /api/instituciones/{id}/estadisticas",
            "GET /api/instituciones/{id}/ley",
            "GET /api/historial",
            "GET /api/sugerencias",
            "GET /api/regiones",
            "GET /api/regiones/{codigo}/comunas",
            "GET /api/leyes/buscar",
            "GET /api/validar-email",
            "GET /api/buscar",
            "GET /api/autocompletar",
            "GET /api/scraper/resumen",
            "GET /api/scraper/fuentes",
            "POST /api/alertas",
            "POST /api/alertas/enviar",
            "POST /api/meilisearch/reindexar",
            "GET /health",
        ],
    }
