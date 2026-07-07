from __future__ import annotations

import asyncio
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
from urllib.parse import urlencode, urlparse

try:
    import psycopg2
    import psycopg2.extras
    _PG_DRIVER = "psycopg2"
except ImportError:
    import pg8000.dbapi as _pg8000  # type: ignore[import]
    _PG_DRIVER = "pg8000"

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
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
    "https://contrataoplanta.cl",
    # Variante con www: aunque el sitio redirige www→apex a nivel de Cloudflare,
    # si un usuario queda en www (redirect cacheado/lento) el fetch a la API se
    # hace con Origin https://www.contrataoplanta.cl. Sin este entry, el CORS lo
    # rechaza y el navegador reporta "Failed to fetch" en TODAS las llamadas.
    "https://www.contrataoplanta.cl",
    "https://estadoemplea.pages.dev",
]


def _load_allow_origins() -> list[str]:
    raw = (os.getenv("CORS_ALLOW_ORIGINS", "") or "").strip()
    if not raw:
        return DEFAULT_ALLOW_ORIGINS
    parsed = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return parsed or DEFAULT_ALLOW_ORIGINS


ALLOW_ORIGINS = _load_allow_origins()

# Dominio público del frontend. El dominio de marca es contrataoplanta.cl;
# Cloudflare Pages además sirve el sitio en estadoemplea.pages.dev, que se
# mantiene en la allowlist durante la transición para no romper el deploy
# vigente. Los dominios de marca previos (estadoemplea.cl / empleoestado.cl)
# ya no resuelven en DNS — si se filtran a un og:image o og:url, el crawler
# recibe NXDOMAIN y el unfurl no se renderiza.
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
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_tipo VARCHAR(20)",
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
        # Doble opt-in (verificación de email). Ver migración
        # 20260701_0002_alertas_doble_optin. Las filas heredadas se marcan
        # verificadas para no cortar envíos existentes.
        "ALTER TABLE alertas_suscripciones ADD COLUMN IF NOT EXISTS verificada BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE alertas_suscripciones ADD COLUMN IF NOT EXISTS token_verificacion VARCHAR(64)",
        "ALTER TABLE alertas_suscripciones ADD COLUMN IF NOT EXISTS verificada_en TIMESTAMPTZ",
        "UPDATE alertas_suscripciones SET verificada = TRUE WHERE verificada = FALSE AND verificada_en IS NULL",
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


_DOCS_ENABLED = os.getenv("DOCS_ENABLED", "").lower() in ("1", "true", "yes")

app = FastAPI(
    title="contrata o planta .cl - API",
    version="2.1.0",
    description="API publica del agregador de empleo publico chileno",
    docs_url="/docs" if _DOCS_ENABLED else None,
    redoc_url="/redoc" if _DOCS_ENABLED else None,
    openapi_url="/openapi.json" if _DOCS_ENABLED else None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    # El regex cubre branch previews de Cloudflare Pages del proyecto activo
    # (`<branch>.estadoemplea.pages.dev`). El dominio de marca contrataoplanta.cl
    # va en la allowlist estática (DEFAULT_ALLOW_ORIGINS). Los dominios muertos
    # (estadoemplea.cl, *.netlify.app) se mantienen fuera para evitar permitir
    # orígenes que ya no corresponden a este deploy.
    allow_origin_regex=(
        r"https://([a-z0-9-]+\.)?estadoemplea\.pages\.dev$"
    ),
    allow_credentials=True,
    # Incluye PUT y DELETE: sin ellos, el preflight CORS bloqueaba editar config
    # (banner/mantenimiento), editar ofertas y editar/borrar cursos desde el
    # panel admin → el navegador reportaba "Failed to fetch".
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    # allow_headers explícito — evita la combinación peligrosa
    # `allow_headers=["*"] + allow_credentials=True`, que expande la
    # superficie de CSRF desde subdominios permitidos.
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
)

app.add_middleware(GZipMiddleware, minimum_size=500)


# ── Security headers middleware ───────────────────────────────────────────
# Añade los mismos headers que sirve Cloudflare Pages (`web/_headers`) en
# todas las respuestas del backend: HTML SSR (`/`, `/oferta/{id}`,
# `/sitemap.xml`) y JSON (`/api/...`). Defense-in-depth para navegadores
# que lleguen directo a Railway sin pasar por Pages.
#
# CSP en modo **enforce** tras #164 + #165 + ciclo de observación sin
# violations. `style-src 'unsafe-inline'` se mantiene de forma consciente:
# hay ~263 atributos `style=` repartidos entre el HTML estático y los
# templates que el JS inyecta vía innerHTML (la mayoría estáticos y
# repetitivos, ~18 dinámicos con valores calculados como anchos/`display`).
# Migrarlos todos a clases es un refactor grande y frágil (una omisión rompe
# estilos en prod en silencio al endurecer la directiva). El beneficio es
# bajo: el vector que `'unsafe-inline'` en *style-src* habilita es CSS
# injection, que requiere una inyección de HTML previa — ya mitigada porque
# el contenido no confiable se escapa y `script-src` NO lleva 'unsafe-inline'
# (el vector grave, XSS de scripts, está cerrado). Por eso se prioriza dejarlo
# documentado antes que migrar. Ver auditoría para el detalle del trade-off.
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
        "connect-src 'self' https://contrataoplanta.cl https://estadoemplea.pages.dev https://contrataoplanta-production.up.railway.app; "
        "frame-ancestors 'self'; "
        "base-uri 'self'; "
        "form-action 'self' https://contrataoplanta.cl https://estadoemplea.pages.dev; "
        "object-src 'none'; "
        "upgrade-insecure-requests"
    ),
}


# Regex de orígenes permitidos (branch previews de Pages), en paralelo al que
# consume CORSMiddleware. Se usa para poder re-adjuntar los headers CORS cuando
# una excepción NO controlada escapa del handler: en ese caso Starlette genera
# el 500 en su capa más externa (ServerErrorMiddleware), POR FUERA del
# CORSMiddleware, así que la respuesta llegaría sin `Access-Control-Allow-Origin`
# y el navegador reportaría un opaco "Failed to fetch" en vez del error real.
_ALLOW_ORIGIN_REGEX = re.compile(r"https://([a-z0-9-]+\.)?estadoemplea\.pages\.dev$")

# Hosts donde el contenido SÍ debe indexarse. Derivado de SITE_URL para que un
# cambio de dominio de marca no deje este set desactualizado.
_HOST_MARCA = urlparse(SITE_URL).hostname or "contrataoplanta.cl"
_HOSTS_MARCA = {_HOST_MARCA, f"www.{_HOST_MARCA}"}


def _cors_headers_para(origin: str | None) -> dict[str, str]:
    """Headers CORS a echar de vuelta si el Origin está permitido."""
    if not origin:
        return {}
    if origin in ALLOW_ORIGINS or _ALLOW_ORIGIN_REGEX.match(origin):
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Vary": "Origin",
        }
    return {}


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    try:
        response = await call_next(request)
    except Exception:
        # Red de seguridad: una excepción no controlada aquí (p.ej. un error de
        # DB que no se convirtió en HTTPException) escaparía al 500 de Starlette
        # SIN headers CORS. La atrapamos en la capa más externa de la app y
        # devolvemos un 500 con CORS + headers de seguridad, para que el panel
        # admin muestre "Error interno" en vez de "Failed to fetch".
        logger.exception("Excepción no controlada en %s %s", request.method, request.url.path)
        response = JSONResponse(status_code=500, content={"detail": "Error interno del servidor"})
        for name, value in _cors_headers_para(request.headers.get("origin")).items():
            response.headers[name] = value
    for name, value in _SECURITY_HEADERS.items():
        # setdefault para no sobrescribir si un endpoint ya los setea
        # (ej: un iframe embebible podría querer X-Frame-Options distinto).
        response.headers.setdefault(name, value)
    # ── noindex fuera del dominio de marca ─────────────────────────────────
    # El backend responde igual en su host técnico (Railway) que detrás del
    # proxy de Pages, así que Google indexaba contrataoplanta-production.up.
    # railway.app como un sitio duplicado (reparte señales SEO entre dos
    # dominios y confunde a quien aterriza ahí). Las Pages Functions marcan
    # sus requests con X-Canonical-Proxy (ver functions/_railway-proxy.js);
    # todo lo demás que no llegue por el host de marca recibe noindex. A
    # propósito NO se usa robots.txt con Disallow: Google necesita poder
    # rastrear las URLs para VER el noindex y desindexar las ya conocidas.
    if "x-canonical-proxy" not in request.headers:
        host = (request.headers.get("host") or "").split(":")[0].lower()
        if host not in _HOSTS_MARCA:
            response.headers["X-Robots-Tag"] = "noindex, nofollow"
    # Datos vivos (ofertas, estadísticas) toleran un stale breve: max-age=60
    # sirve la respuesta cacheada durante 1 minuto; stale-while-revalidate=300
    # permite servir la copia vieja hasta 5 min más mientras revalida en
    # background. Así la vuelta al sitio es instantánea sin mostrar datos de
    # horas atrás. Endpoints con su propio Cache-Control (OG images) no se pisan.
    path = request.url.path
    if path == "/api/estadisticas":
        response.headers.setdefault(
            "Cache-Control", "public, max-age=120, stale-while-revalidate=600"
        )
    elif path.startswith("/api/ofertas/"):
        # Detalle de una oferta. El editor del panel admin actualiza estos datos
        # a mano, así que la caché NO puede ser agresiva o los cambios tardan en
        # verse (antes: max-age=300 + stale-while-revalidate=3600 → hasta ~1 h
        # sirviendo copia vieja). Con esto un cambio se refleja en ~1 min.
        response.headers.setdefault(
            "Cache-Control", "public, max-age=60, stale-while-revalidate=120"
        )
    elif path == "/api/ofertas":
        # Listado. Igual: ventana corta para que las ediciones del panel aparezcan
        # pronto, sin pegarle a la DB en cada scroll (antes SWR=300).
        response.headers.setdefault(
            "Cache-Control", "public, max-age=60, stale-while-revalidate=120"
        )
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


@app.on_event("startup")
async def on_startup_scheduler() -> None:
    # Programador propio de recolecciones (in-app). Vive en cada worker; el
    # claim atómico en `scheduler_state` evita disparos duplicados. Está
    # apagado por defecto en la DB (activo=FALSE) y se puede deshabilitar del
    # todo con SCHEDULER_DISABLED=1 (p. ej. en procesos one-shot / cron).
    if os.getenv("SCHEDULER_DISABLED", "").strip().lower() in ("1", "true", "yes"):
        logger.info("Programador in-app deshabilitado por SCHEDULER_DISABLED")
        return
    try:
        from api.services.scheduler import scheduler_loop, _desactivar_vencidas
        n = await asyncio.to_thread(_desactivar_vencidas)
        if n:
            logger.info("Startup: %d ofertas vencidas desactivadas", n)
        app.state.scheduler_task = asyncio.create_task(scheduler_loop())
        logger.info("Programador in-app activo (loop en background)")
    except Exception as exc:
        logger.error("No se pudo iniciar el programador in-app: %s", exc)


@app.on_event("shutdown")
def on_shutdown() -> None:
    # Cierra limpiamente las conexiones del pool. Importante al redeploy:
    # sin esto, Railway puede matar el proceso antes de que Postgres libere
    # las conexiones y el contador de max_connections crece sin volver a
    # bajar hasta que Postgres las expira por idle_timeout.
    task = getattr(app.state, "scheduler_task", None)
    if task is not None:
        task.cancel()
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


