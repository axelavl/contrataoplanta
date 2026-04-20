from __future__ import annotations

import hashlib
import html
import json
import logging
import math
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

try:
    import psycopg2
    import psycopg2.extras
    _PG_DRIVER = "psycopg2"
except ImportError:
    import pg8000.dbapi as _pg8000  # type: ignore[import]
    _PG_DRIVER = "pg8000"

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, RedirectResponse
from pydantic import BaseModel

# Para poder importar scrapers.source_status desde la API, agregamos la raíz del
# proyecto al sys.path (api/ está bajo la raíz).
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from scrapers.source_status import (
        SourceStatus,
        classify_source,
        enrich_with_status,
        kind_breakdown,
        status_breakdown,
    )
    _SOURCE_STATUS_AVAILABLE = True
except Exception:  # pragma: no cover
    _SOURCE_STATUS_AVAILABLE = False

# ── Service integrations ──
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


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("api.contrataoplanta")

# DB config centralizada: `db/config.py` es la única fuente de verdad.
# Lee `DATABASE_URL` (Railway) o los split `DB_HOST` / `DB_PORT` / ...
# y aborta loud si no hay password. Ver docstring de ese módulo.
from db.config import DB_CONFIG  # noqa: E402  (sys.path seteado arriba)
from db import pool as db_pool  # noqa: E402  (pool de conexiones psycopg2)

# Helpers extraídos a api/services/* (PR refactor #2). Los símbolos se
# re-importan al namespace de este módulo con sus nombres privados
# originales para que los ~60 endpoints inline sigan funcionando sin
# cambios en el cuerpo. PRs futuras moverán los endpoints a routers
# propios que importen directamente de api.services.* sin pasar por
# main.py.
from api.deps import (  # noqa: E402
    SITE_URL, WEB_INDEX_PATH, DEFAULT_OG_IMAGE,
)
from api.services.db import (  # noqa: E402
    _DictCursorWrapper,
    get_connection, _release_connection, get_cursor,
    execute_fetch_all, execute_fetch_one,
    _table_columns, _coalesce_present,
)
from api.services.formatters import (  # noqa: E402
    EMAIL_RE,
    _PORTAL_DOMAINS_LOWER, _sitio_web_cache, _CATALOG_PATH,
    _fold_institution_name, _slugify,
    _extract_root_domain, _load_sitio_web_map, resolve_institucion_sitio_web,
    validate_email,
    _truncate_text, _format_fecha_larga, _format_renta_bruta, _escape_attr,
    _descripcion_a_parrafos_html,
    dias_restantes,
)
from api.services.sql import (  # noqa: E402
    OFFER_STATUS_SQL, ACTIVE_OFFER_SQL, STATUS_LEGACY_MAP,
    ofertas_base_sql, ofertas_select_sql, build_ofertas_filters,
)
from api.services.seo import (  # noqa: E402
    _STATIC_SITEMAP_URLS,
    _LANDING_REGIONES, _LANDING_SECTORES,
    _REGION_BY_SLUG, _SECTOR_BY_SLUG, _find_landing,
    _OFFER_PATH_RE, _INSTITUCION_PATH_RE,
    serialize_offer,
    _set_title, _set_meta, _set_canonical, _inject_offer_path_bootstrap,
    fetch_offer_for_meta,
    build_job_posting_jsonld,
    build_offer_ssr_html, build_offer_meta,
    fetch_landing_ofertas, fetch_landing_total,
    build_landing_meta, build_landing_ssr_html, build_landing_itemlist_jsonld,
    fetch_institucion_para_landing, fetch_institucion_ofertas, fetch_institucion_total,
    build_institucion_meta, build_institucion_ssr_html,
    render_index_with_meta,
)

DEFAULT_ALLOW_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://estadoemplea.pages.dev",
]


def _load_allow_origins() -> list[str]:
    raw = (os.getenv("CORS_ALLOW_ORIGINS", "") or "").strip()
    if not raw:
        return DEFAULT_ALLOW_ORIGINS
    parsed = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return parsed or DEFAULT_ALLOW_ORIGINS


ALLOW_ORIGINS = _load_allow_origins()

# Dominio público del frontend. Los dominios de marca históricos
# (contrataoplanta.cl / estadoemplea.cl / empleoestado.cl) ya no resuelven
# en DNS — si se filtran a un og:image o og:url, el crawler recibe NXDOMAIN
# y el unfurl no se renderiza. Apuntamos a Cloudflare Pages por defecto.
# Constantes y helpers de auth + rate limit centralizados en
# api/deps.py. Los re-exportamos como módulo-level bindings para que
# los 30+ endpoints admin que siguen viviendo en este archivo puedan
# usarlos via `Depends(_verify_admin_jwt)`, `_check_rate_limit(...)`,
# etc., sin cambiar sus firmas (mientras se migran a routers propios
# en PRs siguientes).
from api.deps import (  # noqa: E402
    ADMIN_JWT_ALG,
    ADMIN_JWT_SECRET,
    ADMIN_JWT_TTL_SEG,
    ADMIN_JWT_USER,
    ADMIN_PASSWORD,
    ADMIN_PATH,
    check_rate_limit as _check_rate_limit,
    client_ip as _client_ip,
    create_admin_token as _create_admin_token,
    record_failure as _record_failure,
    revoke_jti as _revoke_jti,
    verify_admin_jwt as _verify_admin_jwt,
)


def ensure_api_schema() -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS instituciones (
            id INTEGER PRIMARY KEY,
            nombre VARCHAR(300) NOT NULL,
            sigla VARCHAR(50),
            sector VARCHAR(100),
            region VARCHAR(100),
            url_empleo TEXT,
            plataforma_empleo VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ofertas (
            id SERIAL PRIMARY KEY,
            institucion_id INTEGER,
            cargo VARCHAR(500) NOT NULL,
            descripcion TEXT,
            requisitos TEXT,
            tipo_contrato VARCHAR(50),
            region VARCHAR(100),
            ciudad VARCHAR(150),
            renta_bruta_min BIGINT,
            renta_bruta_max BIGINT,
            grado_eus VARCHAR(20),
            jornada VARCHAR(100),
            area_profesional VARCHAR(200),
            fecha_publicacion DATE,
            fecha_cierre DATE,
            url_oferta TEXT UNIQUE,
            url_bases TEXT,
            estado VARCHAR(20) DEFAULT 'activo',
            fecha_scraped TIMESTAMP DEFAULT NOW(),
            fecha_actualizado TIMESTAMP DEFAULT NOW()
        )
        """,
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS sigla VARCHAR(50)",
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS nombre_corto VARCHAR(80)",
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS sector VARCHAR(100)",
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS tipo VARCHAR(80)",
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS region VARCHAR(100)",
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS url_empleo TEXT",
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS plataforma_empleo VARCHAR(100)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS institucion_id INTEGER",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS institucion_nombre VARCHAR(300)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS descripcion TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS requisitos TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS requisitos_texto TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS tipo_contrato VARCHAR(50)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS tipo_cargo VARCHAR(50)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS region VARCHAR(100)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS ciudad VARCHAR(150)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS sector VARCHAR(100)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_bruta_min BIGINT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_bruta_max BIGINT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_texto VARCHAR(200)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS grado_eus VARCHAR(20)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS jornada VARCHAR(100)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS area_profesional VARCHAR(200)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_publicacion DATE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_inicio DATE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_cierre DATE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_oferta TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_original TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS estado VARCHAR(20)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_inicio DATE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS activa BOOLEAN DEFAULT TRUE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_scraped TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_actualizado TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_oferta_valida BOOLEAN",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases_valida BOOLEAN",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_valida_chequeada_en TIMESTAMP",
        # Columnas de la pipeline nueva usadas por admin_ofertas/admin_diagnostico.
        # En prod pueden faltar si la DB no pasó por las migraciones del pipeline.
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS needs_review BOOLEAN DEFAULT FALSE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS overall_quality_score NUMERIC",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS detectada_en TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS actualizada_en TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS creada_en TIMESTAMPTZ DEFAULT NOW()",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_ofertas_url_oferta ON ofertas (url_oferta)",
        """
        CREATE TABLE IF NOT EXISTS alertas_suscripciones (
            id SERIAL PRIMARY KEY,
            email VARCHAR(200) NOT NULL,
            region VARCHAR(100),
            termino VARCHAR(200),
            tipo_contrato VARCHAR(50),
            activa BOOLEAN DEFAULT TRUE,
            creada_en TIMESTAMP DEFAULT NOW(),
            actualizada_en TIMESTAMP DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_alertas_email ON alertas_suscripciones (LOWER(email))",
        "ALTER TABLE alertas_suscripciones ADD COLUMN IF NOT EXISTS sector VARCHAR(100)",
        "ALTER TABLE alertas_suscripciones ADD COLUMN IF NOT EXISTS frecuencia VARCHAR(20) DEFAULT 'diaria'",
        # Columnas extendidas para scraper_runs (compatibilidad con admin panel)
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ",
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ",
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS status VARCHAR(20)",
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS run_mode VARCHAR(50)",
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS total_evaluadas INTEGER DEFAULT 0",
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS total_extract INTEGER DEFAULT 0",
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS total_skip INTEGER DEFAULT 0",
        "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS notas TEXT",
        # Rellenar started_at desde ejecutado_en para filas antiguas (solo si
        # la columna heredada aún existe — en prod puede haber sido eliminada
        # tras el renombre).
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'scraper_runs'
                  AND column_name = 'ejecutado_en'
            ) THEN
                EXECUTE 'UPDATE scraper_runs SET started_at = ejecutado_en '
                     || 'WHERE started_at IS NULL AND ejecutado_en IS NOT NULL';
            END IF;
        END $$
        """,
        "UPDATE scraper_runs SET status = 'completado' WHERE status IS NULL AND duracion_segundos IS NOT NULL",
        # Tabla de configuración editable del sitio
        """
        CREATE TABLE IF NOT EXISTS site_config (
            clave VARCHAR(100) PRIMARY KEY,
            valor TEXT,
            actualizado_en TIMESTAMP DEFAULT NOW()
        )
        """,
    ]
    with get_cursor() as (connection, cursor):
        for statement in statements:
            cursor.execute(statement)
        connection.commit()


# ───── Resolución de sitio web real por institución ──────────────────────────
# El catálogo JSON (repositorio_instituciones_publicas_chile.json) contiene
# `sitio_web` — el dominio oficial de la institución — incluso cuando su
# `url_empleo` apunta al portal intermediario (empleospublicos.cl, etc.).
# Esa información NO vive en la tabla `instituciones`, así que la cargamos
# en memoria a partir del JSON y la cacheamos por mtime del archivo.


app = FastAPI(
    title="contrata o planta .cl - API",
    version="2.1.0",
    description="API publica del agregador de empleo publico chileno",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    # El regex cubre branch previews de Cloudflare Pages del proyecto activo
    # (`<branch>.estadoemplea.pages.dev`). Los dominios de marca muertos
    # (contrataoplanta.*, estadoemplea.cl, *.netlify.app) se eliminaron para
    # evitar permitir orígenes que ya no corresponden a este deploy.
    allow_origin_regex=(
        r"https?://("
        r"(localhost|127\.0\.0\.1)(:\d+)?"
        r"|([a-z0-9-]+\.)?estadoemplea\.pages\.dev"
        r")$"
    ),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    # allow_headers explícito — evita la combinación peligrosa
    # `allow_headers=["*"] + allow_credentials=True`, que expande la
    # superficie de CSRF desde subdominios permitidos.
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
)


# ── Security headers middleware ───────────────────────────────────────────
# Añade los mismos headers que sirve Cloudflare Pages (`web/_headers`) en
# todas las respuestas del backend: HTML SSR (`/`, `/oferta/{id}`,
# `/sitemap.xml`) y JSON (`/api/...`). Defense-in-depth para navegadores
# que lleguen directo a Railway sin pasar por Pages.
#
# CSP va en modo Report-Only por la misma razón que en `_headers`: el
# frontend tiene JS/CSS inline.
_SECURITY_HEADERS = {
    "X-Frame-Options": "SAMEORIGIN",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains; preload",
    "Permissions-Policy": (
        "camera=(), microphone=(), geolocation=(), payment=(), "
        "usb=(), interest-cohort=()"
    ),
    "Content-Security-Policy-Report-Only": (
        "default-src 'self'; "
        "script-src 'self'; "
        "style-src 'self' 'unsafe-inline'; "
        "font-src 'self' data:; "
        "img-src 'self' data: https:; "
        "connect-src 'self' https://estadoemplea.pages.dev; "
        "frame-ancestors 'self'; "
        "base-uri 'self'; "
        "form-action 'self' https://estadoemplea.pages.dev; "
        "object-src 'none'; "
        "upgrade-insecure-requests"
    ),
}


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    for name, value in _SECURITY_HEADERS.items():
        # setdefault para no sobrescribir si un endpoint ya los setea
        # (ej: un iframe embebible podría querer X-Frame-Options distinto).
        response.headers.setdefault(name, value)
    return response


@app.on_event("startup")
def on_startup() -> None:
    # No bloquear el arranque si Postgres aún no responde: la API queda viva
    # respondiendo 503 por request hasta que la DB vuelva. Si abortamos aquí,
    # uvicorn cae y nginx devuelve 502/connection refused al frontend.
    try:
        db_pool.init_pool()
    except Exception as exc:
        logger.error("Pool de DB no inicializado al arranque: %s", exc)
    # Antes corríamos `ensure_api_schema()` (60+ DDL `IF NOT EXISTS`) en
    # cada arranque. La auditoría marcó ese patrón como antipatrón: hace
    # lento el arranque, con múltiples workers compite con sí mismo, y
    # enmascara el drift real entre código y DB. Ahora el schema se
    # aplica con Alembic como paso explícito del deploy:
    #     alembic upgrade head
    # La función `ensure_api_schema()` sigue disponible por si alguien
    # necesita correrla one-shot contra una DB heredada, pero NO se
    # invoca automáticamente. Ver `docs/MIGRATIONS.md` para el runbook.
    logger.info("API iniciada (schema gestionado por Alembic)")


@app.on_event("shutdown")
def on_shutdown() -> None:
    # Cierra limpiamente las conexiones del pool. Importante al redeploy:
    # sin esto, Railway puede matar el proceso antes de que Postgres libere
    # las conexiones y el contador de max_connections crece sin volver a
    # bajar hasta que Postgres las expira por idle_timeout.
    db_pool.close_pool()


@app.post(f"/api/{ADMIN_PATH}/meilisearch/reindexar", tags=["admin"])
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

@app.post(f"/api/{ADMIN_PATH}/alertas/enviar", tags=["admin"])
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


# ──────────────────── Health & Root ─────────────────────────────────────────

@app.get("/web/index.html", response_class=HTMLResponse, include_in_schema=False)
def web_index(oferta: int | None = Query(None, ge=1)) -> HTMLResponse:
    canonical = f"{SITE_URL}/web/index.html"
    if oferta:
        canonical = f"{SITE_URL}/oferta/{oferta}"
    oferta_data = fetch_offer_for_meta(oferta) if oferta else None
    meta = build_offer_meta(oferta_data, canonical_url=canonical)
    html_doc = render_index_with_meta(meta, oferta_id_for_bootstrap=oferta)
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=60, stale-while-revalidate=600"},
    )


@app.get("/oferta/{path:path}", response_class=HTMLResponse, include_in_schema=False)
def web_offer(path: str) -> Response:
    """Sirve la SPA con meta tags + JSON-LD inyectados para una oferta.

    URLs aceptadas:
        /oferta/42                           → 301 a /oferta/42-slug-canonico
        /oferta/42-slug-obsoleto             → 301 a /oferta/42-slug-canonico
        /oferta/42-slug-canonico             → 200 con HTML pre-renderizado

    El slug se deriva del ``cargo`` y es sólo cosmético para SEO y para
    humanos que lean el URL. El ``id`` es la identidad real.
    """
    match = _OFFER_PATH_RE.match(path.strip())
    if not match:
        raise HTTPException(status_code=404, detail="Oferta no encontrada")
    oferta_id = int(match.group("id"))
    slug_actual = match.group("slug") or ""

    oferta_data = fetch_offer_for_meta(oferta_id)
    if not oferta_data:
        raise HTTPException(status_code=404, detail="Oferta no encontrada")

    slug_canonico = _slugify(oferta_data.get("cargo") or "")
    path_canonico = f"/oferta/{oferta_id}"
    if slug_canonico:
        path_canonico += f"-{slug_canonico}"

    if slug_actual != slug_canonico:
        # 301 permanente: el canónico para Google es la versión con slug.
        return RedirectResponse(url=path_canonico, status_code=301)

    canonical = f"{SITE_URL}{path_canonico}"
    meta = build_offer_meta(oferta_data, canonical_url=canonical)
    html_doc = render_index_with_meta(
        meta,
        oferta_id_for_bootstrap=oferta_id,
        oferta=oferta_data,
    )
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=120, stale-while-revalidate=900"},
    )


@app.get("/share/oferta/{oferta_id}", include_in_schema=False)
def web_offer_share(oferta_id: int) -> RedirectResponse:
    # Delegamos al canonical builder: primero va a /oferta/{id} (sin slug),
    # que a su vez responde 301 al slug canónico. Así mantenemos una sola
    # fuente de verdad.
    return RedirectResponse(url=f"/oferta/{oferta_id}", status_code=308)


def _render_landing(tipo: str, slug: str) -> Response:
    """Endpoint compartido para /empleos/region/{slug} y /empleos/sector/{slug}."""
    landing = _find_landing(tipo, slug)
    if not landing:
        raise HTTPException(status_code=404, detail=f"{tipo.capitalize()} no encontrada")

    aliases = tuple(landing["aliases"])
    try:
        total = fetch_landing_total(tipo, aliases)
        ofertas = fetch_landing_ofertas(tipo, aliases, limite=30) if total else []
    except Exception:
        logger.exception("Error armando landing %s/%s", tipo, slug)
        total, ofertas = 0, []

    canonical = f"{SITE_URL}/empleos/{tipo}/{slug}"
    meta = build_landing_meta(tipo, landing["nombre"], total, canonical)
    landing_html = build_landing_ssr_html(
        tipo, landing["nombre"], slug, total, ofertas
    )
    landing_jsonld = build_landing_itemlist_jsonld(ofertas, canonical)

    html_doc = render_index_with_meta(
        meta,
        landing_html=landing_html,
        landing_jsonld=landing_jsonld,
    )
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=300, stale-while-revalidate=1800"},
    )


@app.get("/empleos/region/{slug}", response_class=HTMLResponse, include_in_schema=False)
def web_landing_region(slug: str) -> Response:
    return _render_landing("region", slug)


@app.get("/empleos/sector/{slug}", response_class=HTMLResponse, include_in_schema=False)
def web_landing_sector(slug: str) -> Response:
    return _render_landing("sector", slug)


# ── Landing por institución: /empleos/institucion/{id}-{slug} ─────────────
# Mismo patrón que /oferta/{id}-{slug}: el id es canonical, el slug es
# cosmético para SEO y legibilidad. Si el slug no matchea el canónico
# derivado del nombre, responde 301. Si la institución no existe o no
# tiene ofertas (activa en `instituciones` o id ausente), 404.

@app.get(
    "/empleos/institucion/{path:path}",
    response_class=HTMLResponse,
    include_in_schema=False,
)
def web_landing_institucion(path: str) -> Response:
    """Landing SEO por institución. URL canónica `/empleos/institucion/{id}-{slug}`.

    `/empleos/institucion/42` (sin slug) → 301 al canónico.
    `/empleos/institucion/42-slug-viejo` → 301 si el slug no matchea el
    derivado del nombre.
    """
    match = _INSTITUCION_PATH_RE.match(path.strip())
    if not match:
        raise HTTPException(status_code=404, detail="Institución no encontrada")
    inst_id = int(match.group("id"))
    slug_actual = match.group("slug") or ""

    inst = fetch_institucion_para_landing(inst_id)
    if not inst:
        raise HTTPException(status_code=404, detail="Institución no encontrada")

    slug_canonico = _slugify(inst.get("nombre") or "")
    path_canonico = f"/empleos/institucion/{inst_id}"
    if slug_canonico:
        path_canonico += f"-{slug_canonico}"
    if slug_actual != slug_canonico:
        return RedirectResponse(url=path_canonico, status_code=301)

    try:
        total = fetch_institucion_total(inst_id)
        ofertas = fetch_institucion_ofertas(inst_id, limite=30) if total else []
    except Exception:
        logger.exception("Error armando landing institucion/%s", inst_id)
        total, ofertas = 0, []

    canonical = f"{SITE_URL}{path_canonico}"
    meta = build_institucion_meta(inst, total, canonical)
    landing_html = build_institucion_ssr_html(inst, total, ofertas)
    landing_jsonld = build_landing_itemlist_jsonld(ofertas, canonical)

    html_doc = render_index_with_meta(
        meta,
        landing_html=landing_html,
        landing_jsonld=landing_jsonld,
    )
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=300, stale-while-revalidate=1800"},
    )


@app.get("/index.html", include_in_schema=False)
def legacy_index_redirect(request: Request) -> RedirectResponse:
    query = f"?{urlencode(list(request.query_params.multi_items()))}" if request.query_params else ""
    return RedirectResponse(url=f"/web/index.html{query}", status_code=308)


@app.get("/health", response_model=None)
def health() -> dict[str, Any] | JSONResponse:
    try:
        row = execute_fetch_one("SELECT NOW() AS ts")
        return {"status": "ok", "db": str(row["ts"]) if row else None}
    except Exception as exc:  # pragma: no cover
        logger.warning("Healthcheck sin DB: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "detail": "database_unavailable"},
        )


@app.get("/sitemap.xml", include_in_schema=False)
def sitemap_xml() -> Response:
    """Sitemap dinámico con URLs estáticas + una entrada por oferta activa.

    Las URLs siempre apuntan a ``SITE_URL`` (frontend en Cloudflare Pages),
    aunque el sitemap se sirva desde el backend en Railway. Google/Bing
    aceptan sitemaps cross-host siempre que ambos dominios estén
    verificados en Search Console.

    Tope de 45 000 URLs (dentro del límite oficial de 50 000). Si algún
    día hay más ofertas activas, se parte en un sitemap-index paginado.
    """
    try:
        rows = execute_fetch_all(
            f"""
            SELECT
                o.id,
                o.cargo,
                COALESCE(o.actualizada_en, o.fecha_scraped, o.detectada_en, o.creada_en) AS lastmod
            FROM ofertas o
            WHERE {ACTIVE_OFFER_SQL}
            ORDER BY o.id DESC
            LIMIT 45000
            """
        )
    except Exception:
        logger.exception("No se pudo leer ofertas para sitemap; devolviendo solo estáticas")
        rows = []

    hoy = date.today().isoformat()
    partes: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path, priority, changefreq in _STATIC_SITEMAP_URLS:
        partes.append(
            f"  <url><loc>{html.escape(SITE_URL + path)}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>{changefreq}</changefreq>"
            f"<priority>{priority}</priority></url>"
        )
    # Landings SEO (16 regiones + 8 sectores).
    for reg in _LANDING_REGIONES:
        partes.append(
            f"  <url><loc>{html.escape(SITE_URL)}/empleos/region/{reg['slug']}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.8</priority></url>"
        )
    for sec in _LANDING_SECTORES:
        partes.append(
            f"  <url><loc>{html.escape(SITE_URL)}/empleos/sector/{sec['slug']}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.8</priority></url>"
        )
    # Landings por institución: sólo las que tienen ≥1 oferta activa hoy.
    # Evita indexar landings vacías que Google podría marcar como thin.
    try:
        inst_rows = execute_fetch_all(
            f"""
            SELECT i.id, i.nombre
            FROM instituciones i
            WHERE EXISTS (
                SELECT 1 FROM ofertas o
                WHERE o.institucion_id = i.id
                  AND {ACTIVE_OFFER_SQL}
            )
            ORDER BY i.nombre
            """
        )
    except Exception:
        logger.exception("No se pudo leer instituciones para sitemap; saltando.")
        inst_rows = []
    for inst in inst_rows:
        slug = _slugify(inst.get("nombre") or "")
        loc = f"{SITE_URL}/empleos/institucion/{inst['id']}"
        if slug:
            loc += f"-{slug}"
        partes.append(
            f"  <url><loc>{html.escape(loc)}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.6</priority></url>"
        )
    for row in rows:
        slug = _slugify(row.get("cargo"))
        loc = f"{SITE_URL}/oferta/{row['id']}" + (f"-{slug}" if slug else "")
        raw_lastmod = row.get("lastmod")
        if raw_lastmod is None:
            lastmod_str = hoy
        elif hasattr(raw_lastmod, "date"):
            lastmod_str = raw_lastmod.date().isoformat()
        else:
            lastmod_str = raw_lastmod.isoformat()
        partes.append(
            f"  <url><loc>{html.escape(loc)}</loc>"
            f"<lastmod>{lastmod_str}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.7</priority></url>"
        )
    partes.append("</urlset>")
    return Response(
        content="\n".join(partes),
        media_type="application/xml",
        headers={"Cache-Control": "public, max-age=3600, stale-while-revalidate=86400"},
    )


@app.get("/")
def web_root(request: Request, oferta: int | None = Query(None, ge=1)) -> Response:
    accept_types = [
        item.split(";", 1)[0].strip().lower()
        for item in request.headers.get("accept", "").split(",")
        if item.strip()
    ]
    accepts_html = any(item in {"text/html", "application/xhtml+xml"} for item in accept_types)
    accepts_json = any(item == "application/json" or item.endswith("+json") for item in accept_types)
    accepts_any = "*/*" in accept_types
    if accepts_json or (accepts_any and not accepts_html):
        return JSONResponse(
            {
                "nombre": "contrata o planta .cl - API",
                "version": "3.0.0",
                "docs": "/docs",
                "db_host": DB_CONFIG["host"],
            }
        )
    if oferta:
        return RedirectResponse(url=f"/oferta/{oferta}", status_code=308)
    meta = build_offer_meta(None, canonical_url=f"{SITE_URL}/")
    html_doc = render_index_with_meta(meta)
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=60, stale-while-revalidate=600"},
    )


# ════════════════════════════════════════════════════════════════════════════
#  ADMIN API — login/logout/me vive en `api/routers/auth.py`. El resto de
#  endpoints admin sigue más abajo en este archivo (pendiente de extraerlos
#  a un router propio en PRs siguientes).
# ════════════════════════════════════════════════════════════════════════════
from api.routers.auth import router as _auth_router  # noqa: E402

app.include_router(_auth_router)

from api.routers.public import router as _public_router  # noqa: E402
app.include_router(_public_router)


@app.get(f"/api/{ADMIN_PATH}/stats", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/ofertas", tags=["admin"])
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


@app.post(f"/api/{ADMIN_PATH}/ofertas/{{oferta_id}}/toggle-activa", tags=["admin"])
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


@app.put(f"/api/{ADMIN_PATH}/ofertas/{{oferta_id}}", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/scraper-runs", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/scraper-runs/{{run_id}}", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/evaluaciones", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/fuentes", tags=["admin"])
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

@app.get(f"/api/{ADMIN_PATH}/scraper/catalog", tags=["admin"])
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


@app.post(f"/api/{ADMIN_PATH}/scraper/run", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/config", tags=["admin"])
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


@app.put(f"/api/{ADMIN_PATH}/config", tags=["admin"])
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

@app.post(f"/api/{ADMIN_PATH}/ofertas/bulk-desactivar", tags=["admin"])
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


@app.post(f"/api/{ADMIN_PATH}/urls/revalidar", tags=["admin"])
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

@app.get(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}", tags=["admin"])
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


@app.put(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}", tags=["admin"])
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


@app.post(f"/api/{ADMIN_PATH}/fuentes", tags=["admin"])
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


@app.delete(f"/api/{ADMIN_PATH}/fuentes/{{fuente_id}}", tags=["admin"])
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

@app.get(f"/api/{ADMIN_PATH}/revision", tags=["admin"])
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


@app.post(f"/api/{ADMIN_PATH}/revision/{{oferta_id}}/marcar-revisada", tags=["admin"])
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

@app.get(f"/api/{ADMIN_PATH}/diagnostico", tags=["admin"])
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

@app.get(f"/api/{ADMIN_PATH}/suscripciones", tags=["admin"])
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


@app.delete(f"/api/{ADMIN_PATH}/suscripciones/{{sub_id}}", tags=["admin"])
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


@app.post(f"/api/{ADMIN_PATH}/alertas/enviar", tags=["admin"])
def admin_enviar_alertas(
    payload: dict[str, Any],
    _user: str = Depends(_verify_admin_jwt),
) -> dict[str, Any]:
    """
    Dispara el envío de alertas manualmente.

    Body (opcional):
      - email: str   — solo para ese suscriptor
      - horas: int   — ventana de ofertas nuevas (default 24)
      - dry_run: bool — no envía, solo calcula coincidencias
    """
    from api.services.email_alerts import enviar_alerta_ofertas as _enviar

    email_filtro = payload.get("email")
    horas = int(payload.get("horas", 24))
    dry_run = bool(payload.get("dry_run", False))

    where_sub = "WHERE activa = TRUE"
    params_sub: list[Any] = []
    if email_filtro:
        where_sub += " AND LOWER(email) = LOWER(%s)"
        params_sub.append(email_filtro)

    suscripciones = execute_fetch_all(
        f"SELECT * FROM alertas_suscripciones {where_sub} ORDER BY creada_en",
        params_sub,
    )
    if not suscripciones:
        return {"ok": True, "enviados": 0, "msg": "Sin suscripciones activas"}

    enviados, errores, sin_ofertas = 0, 0, 0
    detalles: list[dict[str, Any]] = []

    for sub in suscripciones:
        where_parts = [ACTIVE_OFFER_SQL]
        params_q: list[Any] = []
        where_parts.append(
            f"COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en) >= NOW() - INTERVAL '{horas} hours'"
        )
        if sub.get("region"):
            where_parts.append("COALESCE(o.region, i.region, '') ILIKE %s")
            params_q.append(f"%{sub['region']}%")
        if sub.get("termino"):
            where_parts.append("(o.cargo ILIKE %s OR COALESCE(o.descripcion,'') ILIKE %s)")
            params_q.extend([f"%{sub['termino']}%", f"%{sub['termino']}%"])
        if sub.get("tipo_contrato"):
            where_parts.append("COALESCE(NULLIF(o.tipo_contrato,''),NULLIF(o.tipo_cargo,'')) ILIKE %s")
            params_q.append(f"%{sub['tipo_contrato']}%")
        if sub.get("sector"):
            where_parts.append("COALESCE(i.sector, o.sector,'') ILIKE %s")
            params_q.append(f"%{sub['sector']}%")

        ofertas = execute_fetch_all(
            f"""{ofertas_select_sql()}
                {ofertas_base_sql()}
                WHERE {" AND ".join(where_parts)}
                ORDER BY fecha_scraped DESC NULLS LAST
                LIMIT 20""",
            params_q,
        )

        det: dict[str, Any] = {"email": sub["email"], "coincidencias": len(ofertas)}
        if not ofertas:
            sin_ofertas += 1
            det["resultado"] = "sin_coincidencias"
        elif dry_run:
            det["resultado"] = "dry_run"
        else:
            r = _enviar(
                email=sub["email"],
                ofertas=ofertas,
                filtros={k: sub.get(k) for k in ("region","termino","tipo_contrato","sector")},
            )
            if r.get("ok"):
                enviados += 1
                det["resultado"] = "enviado"
                det["resend_id"] = r.get("id")
            else:
                errores += 1
                det["resultado"] = "error"
                det["error"] = r.get("error")
        detalles.append(det)

    return {
        "ok": True,
        "total_suscripciones": len(suscripciones),
        "enviados": enviados,
        "errores": errores,
        "sin_coincidencias": sin_ofertas,
        "dry_run": dry_run,
        "detalles": detalles,
    }


@app.post(f"/api/{ADMIN_PATH}/alertas/test-email", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/suscripciones/export", tags=["admin"])
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


@app.get(f"/api/{ADMIN_PATH}/ofertas/export", tags=["admin"])
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


