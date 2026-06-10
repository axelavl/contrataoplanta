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
# CSP en modo **enforce** tras #164 + #165 + ciclo de observación sin
# violations. `style-src 'unsafe-inline'` se mantiene por los `style=`
# del HTML de oferta — moverlos a clases va en otro PR.
_SECURITY_HEADERS = {
    "X-Frame-Options": "SAMEORIGIN",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains; preload",
    "Permissions-Policy": (
        "camera=(), microphone=(), geolocation=(), payment=(), "
        "usb=(), interest-cohort=()"
    ),
    "Content-Security-Policy": (
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


# ──────────────────── Health & Root ─────────────────────────────────────────


# ════════════════════════════════════════════════════════════════════════════
#  ADMIN API — login/logout/me vive en `api/routers/auth.py`. El resto de
#  endpoints admin sigue más abajo en este archivo (pendiente de extraerlos
#  a un router propio en PRs siguientes).
# ════════════════════════════════════════════════════════════════════════════
from api.routers.auth import router as _auth_router  # noqa: E402

app.include_router(_auth_router)

from api.routers.public import router as _public_router  # noqa: E402
app.include_router(_public_router)

from api.routers.web import router as _web_router  # noqa: E402
app.include_router(_web_router)

from api.routers.admin import router as _admin_router  # noqa: E402
app.include_router(_admin_router)


