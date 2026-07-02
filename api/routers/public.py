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
from pydantic import BaseModel, EmailStr, Field

import time as _time
from collections import defaultdict

from api.deps import (
    ADMIN_PATH,
    DEFAULT_OG_IMAGE,
    SITE_URL,
    WEB_INDEX_PATH,
    _PROJECT_ROOT,
    check_public_rate_limit,
    client_ip,
)
from api.services.db import (
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
    DESTACADAS_AUTO,
    OFFER_STATUS_SQL,
    STATUS_LEGACY_MAP,
    build_cargo_relevance,
    build_ofertas_filters,
    ofertas_base_sql,
    ofertas_select_sql,
)


def _destacadas_config() -> tuple[bool, list[dict[str, Any]] | None, str]:
    """Lee la config de Destacadas de `site_config` (panel admin).

    Devuelve ``(auto, criterios, modo)``:
    - ``auto``: si la pestaña pública incluye los criterios automáticos además de
      las marcadas a mano. Si la clave falta / DB caída → constante `DESTACADAS_AUTO`.
    - ``criterios``: lista ``[{tipo, valor}]`` parseada del JSON `destacadas_criterios`
      (o None si no hay / es inválido → se usa el criterio por defecto).
    - ``modo``: 'any' (OR) | 'all' (AND).
    """
    auto = DESTACADAS_AUTO
    criterios: list[dict[str, Any]] | None = None
    modo = "any"
    try:
        rows = execute_fetch_all(
            "SELECT clave, valor FROM site_config "
            "WHERE clave IN ('destacadas_auto','destacadas_criterios','destacadas_criterios_modo')",
            [],
        )
        conf = {r["clave"]: r["valor"] for r in rows}
        if conf.get("destacadas_auto") is not None:
            auto = str(conf["destacadas_auto"]).strip().lower() in ("1", "true", "yes")
        if str(conf.get("destacadas_criterios_modo", "")).strip().lower() == "all":
            modo = "all"
        raw = conf.get("destacadas_criterios")
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                criterios = [c for c in parsed if isinstance(c, dict)]
    except Exception:
        pass
    return auto, criterios, modo


def _destacadas_filtros() -> dict[str, Any]:
    """kwargs de Destacadas para `build_ofertas_filters` (solo en esa pestaña)."""
    auto, criterios, modo = _destacadas_config()
    return {
        "destacadas_auto": auto,
        "destacadas_criterios": criterios,
        "destacadas_modo": modo,
    }
from api.services.seo import (
    build_offer_meta,
    fetch_offer_for_meta,
    serialize_offer,
    serialize_offer_batch,
)
from api.services.regiones import get_comunas, get_regiones
from api.services.leyes import buscar_ley_bcn, get_ley_institucion
from api.services.mailcheck import validar_email as mailcheck_validar
from api.services.analitica import registrar_evento
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


_PUBLIC_RATE_WINDOW = 600
_PUBLIC_RATE_MAX = 20


def _check_public_rate(
    request: Request, max_hits: int = _PUBLIC_RATE_MAX, *, bucket: str = "publico"
) -> None:
    """Rate limit por IP para endpoints públicos sensibles.

    Estado compartido entre workers en Postgres (tabla `public_rate_hits`), con
    fallback en memoria si la DB no responde. `bucket` separa el presupuesto por
    familia de endpoint para que el abuso de uno no consuma la cuota de otro.
    """
    check_public_rate_limit(
        client_ip(request), bucket,
        window_seg=_PUBLIC_RATE_WINDOW, max_hits=max_hits,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Pydantic / dataclass models usadas por los endpoints públicos
# ═══════════════════════════════════════════════════════════════════════════

class AlertaPayload(BaseModel):
    # Límites de longitud explícitos: sin ellos, el body POST aceptaba
    # cadenas ilimitadas que (a) alimentaban el regex de validación de email
    # con entradas enormes y (b) reventaban con 500 al superar el límite
    # VARCHAR de la columna en el INSERT. Un email válido cabe en 254 chars
    # (RFC 5321); el resto son términos cortos de filtro.
    email: str = Field(..., min_length=3, max_length=254)
    region: str | None = Field(default=None, max_length=100)
    termino: str | None = Field(default=None, max_length=200)
    tipo_contrato: str | None = Field(default=None, max_length=50)
    sector: str | None = Field(default=None, max_length=100)
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
    institucion: str | None = Query(
        None,
        description="ID(s) de institución. Acepta uno ('12') o varios separados por coma ('12,34').",
    ),
    area_profesional: str | None = Query(None),
    profesion: str | None = Query(None, description="Familia profesional (salud, juridico, etc.) — expande a lexemas del cargo."),
    nivel: str | None = Query(None, description="Nivel/estamento. Acepta varios separados por coma."),
    renta_min: int | None = Query(None, ge=0),
    renta_max: int | None = Query(None, ge=0),
    ciudad: str | None = Query(None),
    comunas: str | None = Query(None, description="Lista de comunas separadas por coma"),
    cierra_pronto: bool = Query(False),
    nuevas: bool = Query(False),
    solo_con_correo: bool = Query(False, description="Solo ofertas con correo de postulación/contacto."),
    destacadas: bool = Query(False, description="Solo ofertas destacadas (las que se publican en redes sociales)."),
    sin_experiencia: bool = Query(False, description="Solo ofertas que no exigen experiencia previa (best-effort por texto)."),
    vista: str = Query("vigentes", pattern="^(vigentes|cerradas|todas)$"),
    orden: str = Query("recientes"),
    pagina: int = Query(1, ge=1, le=10000),
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
        profesion=profesion,
        nivel=nivel,
        renta_min=renta_min,
        renta_max=renta_max,
        ciudad=ciudad,
        comunas=comunas,
        cierra_pronto=cierra_pronto,
        nuevas=nuevas,
        solo_con_correo=solo_con_correo,
        solo_destacadas=destacadas,
        sin_experiencia=sin_experiencia,
        solo_activas=only_active,
        closed_only=only_closed,
        # Solo se consulta la config de Destacadas cuando es esa pestaña.
        **(_destacadas_filtros() if destacadas else {}),
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

    # Cuando hay búsqueda, anteponemos relevancia por cargo: los avisos cuyo
    # TÍTULO contiene la frase aparecen primero; los que solo la mencionan en la
    # descripción quedan después, sin perder el orden elegido dentro de cada grupo.
    rel_sql = ""
    rel_params: list[Any] = []
    if q:
        rel_sql, rel_params = build_cargo_relevance(q)

    # En la pestaña "Destacadas" las marcadas a mano (las que se publican en
    # redes sociales) van SIEMPRE primero; debajo, las incluidas por el
    # criterio automático (DESTACADAS_AUTO). `destacada` viene de la CTE base.
    destacadas_prefix = "destacada DESC, " if destacadas else ""

    select_sql = f"""
    WITH base AS (
        {ofertas_select_sql(truncate_text=True)}
        {ofertas_base_sql()}
        {where_sql}
    )
    SELECT * FROM base
    ORDER BY {destacadas_prefix}{rel_sql}{order_sql}
    LIMIT %s OFFSET %s
    """
    count_sql = f"""
    SELECT COUNT(*) AS total
    {ofertas_base_sql()}
    {where_sql}
    """
    rows = execute_fetch_all(select_sql, [*params, *rel_params, pag.por_pagina, pag.offset])
    total_row = execute_fetch_one(count_sql, params)
    total = int(total_row["total"]) if total_row else 0
    paginas = math.ceil(total / pag.por_pagina) if total else 0

    return {
        "total": total,
        "pagina": pag.pagina,
        "por_pagina": pag.por_pagina,
        "paginas": paginas,
        "ofertas": serialize_offer_batch(rows, truncate=True),
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


@router.get("/api/cursos")
def get_cursos() -> dict[str, Any]:
    """Directorio de cursos ACTIVOS para `web/cursos.js`, ordenado por `orden`.
    Si la tabla aún no existe (migración pendiente), el frontend cae al archivo
    estático `cursos-data.js`, así que devolvemos lista vacía en vez de 500."""
    try:
        rows = execute_fetch_all(
            """
            SELECT curso_id AS id, titulo, proveedor, categoria, modalidad,
                   duracion, nivel, tipo, url, descripcion, gratuito, demo, logo
            FROM cursos
            WHERE activo = TRUE
            ORDER BY orden ASC, id ASC
            """,
            [],
        )
    except Exception:
        rows = []
    return {"cursos": rows}


@router.get("/api/cursos/categorias")
def get_cursos_categorias() -> dict[str, Any]:
    """Categorías ACTIVAS de cursos (chips de /cursos.html). Fallback a [] si la
    tabla aún no existe (el frontend usa las del archivo estático)."""
    try:
        rows = execute_fetch_all(
            "SELECT slug, etiqueta FROM cursos_categorias "
            "WHERE activo = TRUE ORDER BY orden ASC, etiqueta ASC",
            [],
        )
    except Exception:
        rows = []
    return {"categorias": rows}


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
        # El detalle de la excepción (rutas/versiones internas) queda en logs;
        # al cliente se le devuelve un mensaje genérico.
        import logging
        logging.getLogger("api.routers.public").warning("Generador OG no disponible: %s", exc)
        raise HTTPException(status_code=503, detail="Generador de imágenes no disponible") from exc

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


_estadisticas_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_ESTADISTICAS_TTL = 90

_scraper_resumen_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_SCRAPER_RESUMEN_TTL = 300


@router.get("/api/estadisticas")
def get_estadisticas() -> dict[str, Any]:
    now = _time.monotonic()
    if _estadisticas_cache["data"] is not None and (now - _estadisticas_cache["ts"]) < _ESTADISTICAS_TTL:
        return _estadisticas_cache["data"]

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
                WHERE {OFFER_STATUS_SQL.replace('o.', '')} = 'closing_today'
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

    result = {
        "activas_hoy": int(conteos.get("activas_hoy") or 0),
        "nuevas_48h": int(conteos.get("nuevas_48h") or 0),
        "cierran_hoy": int(conteos.get("cierran_hoy") or 0),
        "instituciones_activas": int(conteos.get("instituciones_activas") or 0),
        "ultima_actualizacion": ultima_actualizacion_row.get("ultima_actualizacion"),
        "por_sector": por_sector,
        "historico_mensual": historico_mensual,
        "mas_activas": mas_activas,
    }
    _estadisticas_cache["data"] = result
    _estadisticas_cache["ts"] = _time.monotonic()
    return result


@router.get("/api/mercado/agregados")
def get_mercado_agregados() -> dict[str, Any]:
    """Agregaciones del mercado laboral público para el panel B2B.

    Calcula sobre el universo COMPLETO de ofertas activas (no una muestra):
    demanda por región, por área profesional (normalizada), por tipo de
    vínculo, y cobertura de renta estructurada. Alimenta `panel-mercado.html`.
    """
    activo = ACTIVE_OFFER_SQL.replace("o.", "")

    por_region = execute_fetch_all(
        f"""
        SELECT COALESCE(NULLIF(TRIM(region), ''), 'Sin región') AS region,
               COUNT(*) AS total
        FROM ofertas
        WHERE {activo}
        GROUP BY 1
        ORDER BY total DESC, region ASC
        LIMIT 16
        """
    )

    # INITCAP(LOWER(...)) normaliza casing inconsistente (ej. "Salud" vs "salud").
    por_area = execute_fetch_all(
        f"""
        SELECT COALESCE(NULLIF(INITCAP(LOWER(TRIM(area_profesional))), ''), 'Sin clasificar') AS area,
               COUNT(*) AS total
        FROM ofertas
        WHERE {activo}
        GROUP BY 1
        ORDER BY total DESC, area ASC
        LIMIT 12
        """
    )

    por_tipo = execute_fetch_all(
        f"""
        SELECT INITCAP(LOWER(COALESCE(
                   NULLIF(TRIM(tipo_contrato), ''),
                   NULLIF(TRIM(calidad_juridica), ''),
                   'Sin especificar'))) AS tipo,
               COUNT(*) AS total
        FROM ofertas
        WHERE {activo}
        GROUP BY 1
        ORDER BY total DESC, tipo ASC
        LIMIT 8
        """
    )

    renta = execute_fetch_one(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (
                WHERE renta_bruta_min IS NOT NULL OR renta_bruta_max IS NOT NULL
            ) AS con_renta
        FROM ofertas
        WHERE {activo}
        """
    ) or {}

    total = int(renta.get("total") or 0)
    con_renta = int(renta.get("con_renta") or 0)
    pct_renta = round(con_renta / total * 100) if total else 0

    return {
        "total_activas": total,
        "por_region": por_region,
        "por_area": por_area,
        "por_tipo": por_tipo,
        "renta": {
            "total": total,
            "con_renta": con_renta,
            "pct_con_renta": pct_renta,
        },
    }


@router.get("/api/instituciones")
def get_instituciones(
    q: str | None = Query(None),
    sector: str | None = Query(None),
    region: str | None = Query(None),
    pagina: int = Query(1, ge=1, le=10000),
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
    pagina: int = Query(1, ge=1, le=10000),
    por_pagina: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    pag = Paginacion(pagina=pagina, por_pagina=por_pagina)
    where_sql, params = build_ofertas_filters(institucion_id=institucion_id, solo_activas=True)
    sql = f"""
    WITH base AS (
        {ofertas_select_sql(truncate_text=True)}
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
        "ofertas": serialize_offer_batch(rows),
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
    q: str | None = Query(None),
    institucion_id: int | None = Query(None),
    institucion: str | None = Query(None, description="Alias de institucion_id; acepta uno o varios separados por coma."),
    sector: str | None = Query(None),
    region: str | None = Query(None),
    tipo: str | None = Query(None),
    nivel: str | None = Query(None),
    profesion: str | None = Query(None),
    area_profesional: str | None = Query(None),
    renta_min: int | None = Query(None, ge=0),
    renta_max: int | None = Query(None, ge=0),
    ciudad: str | None = Query(None),
    comunas: str | None = Query(None, description="Lista de comunas separadas por coma"),
    solo_con_correo: bool = Query(False),
    sin_experiencia: bool = Query(False),
    pagina: int = Query(1, ge=1, le=10000),
    por_pagina: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    pag = Paginacion(pagina=pagina, por_pagina=por_pagina)
    where_sql, params = build_ofertas_filters(
        q=q,
        region=region,
        sector=sector,
        tipo=tipo,
        institucion_id=institucion if institucion is not None else institucion_id,
        nivel=nivel,
        profesion=profesion,
        area_profesional=area_profesional,
        renta_min=renta_min,
        renta_max=renta_max,
        ciudad=ciudad,
        comunas=comunas,
        solo_con_correo=solo_con_correo,
        sin_experiencia=sin_experiencia,
        solo_activas=False,
        closed_only=True,
    )
    sql = f"""
    WITH base AS (
        {ofertas_select_sql(truncate_text=True)}
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
        "historial": serialize_offer_batch(rows, truncate=True),
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
def crear_alerta(request: Request, payload: AlertaPayload) -> dict[str, Any]:
    _check_public_rate(request, bucket="alertas")
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

    # Doble opt-in: la suscripción NO se activa para envíos hasta que se
    # confirma el correo. Así se evita que alguien suscriba a un tercero y le
    # llegue spam (y se protege la reputación del dominio remitente). Si Resend
    # NO está configurado (dev/staging sin RESEND_API_KEY), no habría forma de
    # confirmar, así que se degrada al comportamiento anterior: se marca como
    # verificada de inmediato.
    email_configurado = bool(os.getenv("RESEND_API_KEY") or os.getenv("EMAIL_API_KEY"))
    token = secrets.token_urlsafe(32) if email_configurado else None
    verificada_inicial = not email_configurado

    with get_cursor() as (connection, cursor):
        cursor.execute(
            """
            INSERT INTO alertas_suscripciones (
                email, region, termino, tipo_contrato, sector, frecuencia,
                activa, verificada, token_verificacion, creada_en, actualizada_en
            ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, NOW(), NOW())
            ON CONFLICT (
                LOWER(email),
                COALESCE(region, ''),
                COALESCE(termino, ''),
                COALESCE(tipo_contrato, ''),
                COALESCE(sector, '')
            ) DO UPDATE SET
                activa = TRUE,
                frecuencia = EXCLUDED.frecuencia,
                actualizada_en = NOW(),
                -- Sólo (re)generar token si la suscripción NO estaba verificada;
                -- una re-suscripción de un correo ya confirmado no exige
                -- reconfirmar (queda activa y verificada).
                token_verificacion = CASE
                    WHEN alertas_suscripciones.verificada THEN alertas_suscripciones.token_verificacion
                    ELSE EXCLUDED.token_verificacion END
            RETURNING verificada, token_verificacion
            """,
            [email, region, termino, tipo_contrato, sector, frecuencia,
             verificada_inicial, token],
        )
        row = cursor.fetchone()
        connection.commit()

    row = dict(row) if row else {}
    ya_verificada = bool(row.get("verificada"))
    token_envio = row.get("token_verificacion")

    response: dict[str, Any] = {"ok": True}
    if ya_verificada:
        response["mensaje"] = "Alerta registrada correctamente"
        response["verificada"] = True
    else:
        # Pendiente de confirmación: enviar (o reenviar) el correo de verificación.
        if token_envio:
            try:
                enviar_verificacion(email, token_envio)
            except Exception as exc:  # noqa: BLE001 — no romper el alta por un fallo de envío
                import logging
                logging.getLogger("api.routers.public").warning(
                    "No se pudo enviar verificación de alerta a %s: %s", email, exc
                )
        response["mensaje"] = "Te enviamos un correo para confirmar tu suscripción. Revisa tu bandeja (y spam)."
        response["verificada"] = False

    if check.get("sugerencia"):
        response["sugerencia_email"] = check["sugerencia"]
    return response


@router.get("/api/alertas/confirmar")
def confirmar_alerta(request: Request, token: str = Query(..., min_length=16, max_length=64)) -> dict[str, Any]:
    """Confirma una suscripción a alertas a partir del token del correo.

    Marca la fila como `verificada = TRUE`, la deja activa y consume el token
    (un solo uso). Idempotente en la práctica: si el token ya se usó no existe
    ninguna fila con él y se responde 404, pero una fila ya verificada no se ve
    afectada.
    """
    _check_public_rate(request, max_hits=30, bucket="alertas_confirmar")
    with get_cursor() as (connection, cursor):
        cursor.execute(
            """
            UPDATE alertas_suscripciones
               SET verificada = TRUE,
                   activa = TRUE,
                   verificada_en = NOW(),
                   token_verificacion = NULL,
                   actualizada_en = NOW()
             WHERE token_verificacion = %s
            RETURNING email
            """,
            [token],
        )
        row = cursor.fetchone()
        connection.commit()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Enlace de confirmación inválido o ya utilizado.",
        )
    return {"ok": True, "mensaje": "Suscripción confirmada. Ya recibirás tus alertas."}


# ──────────────────── Analítica interna (beacon de tráfico) ─────────────────

#: Límite holgado y propio para el beacon: una sesión normal genera muchas
#: vistas, así que no comparte presupuesto con `/api/alertas`.
#:
#: A diferencia de `_check_public_rate`, este límite se mantiene EN MEMORIA a
#: propósito: el beacon es alto volumen (hasta 120/min por IP) y su valor de
#: seguridad es bajo (protege sólo contra spam de analítica, no toca DB de
#: negocio ni envía correos). Un INSERT por hit en Postgres sería un costo de
#: escritura desproporcionado. La fragmentación por worker es aceptable acá.
_track_rate: dict[str, list[float]] = defaultdict(list)
_TRACK_RATE_WINDOW = 60
_TRACK_RATE_MAX = 120


def _check_track_rate(request: Request) -> bool:
    ip = client_ip(request)
    ahora = _time.time()
    corte = ahora - _TRACK_RATE_WINDOW
    hits = _track_rate[ip] = [t for t in _track_rate[ip] if t > corte]
    if len(hits) >= _TRACK_RATE_MAX:
        return False
    _track_rate[ip].append(ahora)
    return True


@router.post("/api/track")
async def track_evento(request: Request) -> Response:
    """Registra una vista de página o un evento del sitio (analítica propia).

    Lo llama `web/analytics.js` vía `navigator.sendBeacon`. El cuerpo se
    parsea a mano (no se declara un modelo de FastAPI) porque el beacon se
    envía como `text/plain` para evitar el preflight CORS que `sendBeacon`
    no puede hacer cross-origin.

    No guarda IP ni datos personales; el «visitante único» se aproxima con
    un hash anónimo que rota a diario. Siempre responde 204 (incluso si se
    descarta o la tabla aún no existe) para no entorpecer la navegación.
    """
    if _check_track_rate(request):
        try:
            raw = await request.body()
            data = json.loads(raw or b"{}")
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        registrar_evento(
            tipo=str(data.get("tipo") or "pageview"),
            path=data.get("path"),
            evento=data.get("evento"),
            oferta_id=data.get("oferta_id"),
            referrer=data.get("ref") or request.headers.get("referer"),
            user_agent=request.headers.get("user-agent", ""),
            ip=client_ip(request),
        )
    return Response(status_code=204)


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
    now = _time.monotonic()
    if _scraper_resumen_cache["data"] is not None and (now - _scraper_resumen_cache["ts"]) < _SCRAPER_RESUMEN_TTL:
        return _scraper_resumen_cache["data"]

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

    result = {
        "disponible": True,
        "total": total,
        "activas": activas,
        "por_status": status_counts,
        "por_kind": {k: v for k, v in kind_counts.items() if v},
        "cobertura_activa_pct": cobertura,
    }
    _scraper_resumen_cache["data"] = result
    _scraper_resumen_cache["ts"] = _time.monotonic()
    return result


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
def api_validar_email(request: Request, email: str = Query(..., min_length=3, max_length=200)) -> dict[str, Any]:
    """
    Valida un email: detecta dominios temporales/desechables y sugiere
    correcciones de typos comunes (gmial→gmail, hotnail→hotmail).
    """
    _check_public_rate(request, max_hits=30, bucket="validar_email")
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
            "GET /api/site-config",
            "GET /health",
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Configuración pública del sitio (editable desde el panel admin)
# ═══════════════════════════════════════════════════════════════════════════

#: Claves de `site_config` expuestas al frontend público. Todo lo demás
#: (si algún día se guardan claves internas) queda fuera.
_SITE_CONFIG_PUBLICA = {
    "banner_mensaje", "banner_activo", "mantenimiento",
    "max_resultados_pagina", "alertas_activas", "footer_extra",
    # AdSense gestionable desde el admin (el `ca-pub` y los slot id son
    # identificadores públicos, van en el HTML; no son secretos).
    "ads_enabled", "ads_client",
    "ads_slot_resultados", "ads_slot_sidebar", "ads_slot_contenido",
    # Recuadro "Anúnciate" (oferta + valores para publicar) en cursos.html.
    "cursos_anunciate_activo",
}


@router.get("/api/site-config")
def get_site_config() -> dict[str, Any]:
    """Config editable del sitio (banner, mantenimiento, footer extra).

    La edita el panel admin (`PUT /api/{ADMIN_PATH}/config`); el
    frontend público la consulta al cargar para renderizar banner de
    aviso, modo mantenimiento y footer extra. Si la tabla no existe,
    devuelve config vacía (el sitio funciona igual).
    """
    try:
        rows = execute_fetch_all(
            "SELECT clave, valor FROM site_config ORDER BY clave", []
        )
        conf = {r["clave"]: r["valor"] for r in rows if r["clave"] in _SITE_CONFIG_PUBLICA}
    except Exception:
        conf = {}
    return {"config": conf}


# ═══════════════════════════════════════════════════════════════════════════
#  Webhook de Resend (eventos de entrega de email)
# ═══════════════════════════════════════════════════════════════════════════

def _verificar_firma_svix(secret: str, svix_id: str, svix_timestamp: str,
                          svix_signature: str, body: bytes) -> bool:
    """Verifica la firma svix que usa Resend en sus webhooks.

    Esquema documentado: HMAC-SHA256 de ``{id}.{timestamp}.{body}`` con
    el secreto base64 (tras el prefijo ``whsec_``). El header
    ``svix-signature`` trae una lista separada por espacios de
    ``v1,<firma_base64>``.
    """
    try:
        secret_b = base64.b64decode(secret.removeprefix("whsec_"))
        signed = f"{svix_id}.{svix_timestamp}.".encode() + body
        esperado = base64.b64encode(
            hmac.new(secret_b, signed, hashlib.sha256).digest()
        ).decode()
    except Exception:
        return False
    for parte in svix_signature.split(" "):
        version, _, firma = parte.partition(",")
        if version == "v1" and firma and hmac.compare_digest(firma, esperado):
            return True
    return False


@router.post("/api/webhooks/resend")
async def webhook_resend(request: Request) -> dict[str, Any]:
    """Recibe eventos de Resend (delivered, bounced, opened, clicked, …).

    Requiere `RESEND_WEBHOOK_SECRET` (el "Signing Secret" del webhook en
    el dashboard de Resend). Sin esa env var el endpoint responde 503 y
    no procesa nada. La firma se valida SIEMPRE — un webhook público sin
    verificación permitiría inyectar eventos falsos.
    """
    secret = os.getenv("RESEND_WEBHOOK_SECRET", "").strip()
    if not secret:
        raise HTTPException(503, "Webhook no configurado (falta RESEND_WEBHOOK_SECRET)")

    svix_id = request.headers.get("svix-id", "")
    svix_ts = request.headers.get("svix-timestamp", "")
    svix_sig = request.headers.get("svix-signature", "")
    body = await request.body()
    if not (svix_id and svix_ts and svix_sig):
        raise HTTPException(401, "Headers de firma faltantes")

    # Tolerancia de timestamp (5 min) contra replay.
    try:
        ts = int(svix_ts)
        if abs(datetime.now(tz=timezone.utc).timestamp() - ts) > 300:
            raise HTTPException(401, "Timestamp fuera de tolerancia")
    except ValueError:
        raise HTTPException(401, "Timestamp inválido") from None

    if not _verificar_firma_svix(secret, svix_id, svix_ts, svix_sig, body):
        raise HTTPException(401, "Firma inválida")

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(400, "Body no es JSON") from None

    evento = str(payload.get("type") or "desconocido")[:40]
    data = payload.get("data") or {}
    destinatarios = data.get("to") or []
    if isinstance(destinatarios, str):
        destinatarios = [destinatarios]
    email = (destinatarios[0] if destinatarios else None)
    resend_id = (data.get("email_id") or data.get("id") or None)
    asunto = (data.get("subject") or None)

    try:
        with get_cursor() as (conn, cur):
            cur.execute(
                """INSERT INTO email_eventos (evento, email, resend_id, asunto, payload)
                   VALUES (%s, %s, %s, %s, %s)""",
                [evento, email, resend_id, asunto, json.dumps(payload, ensure_ascii=False)],
            )
            conn.commit()
    except Exception as exc:
        # Tabla ausente (migración 0004 sin aplicar): 200 igual para que
        # Resend no reintente eternamente, pero queda en logs.
        import logging
        logging.getLogger("api.routers.public").warning(
            f"[webhook resend] no se pudo guardar evento: {exc}"
        )

    return {"ok": True}

