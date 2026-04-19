from __future__ import annotations

import asyncio
import base64
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

import secrets

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

def _requerido_env(nombre: str) -> str:
    """Lee una variable de entorno obligatoria o aborta el arranque.

    Sin fallback con credenciales hardcodeadas: si la variable falta en
    Railway/entorno, el proceso debe fallar ruidoso al importar este módulo.
    """
    valor = os.getenv(nombre)
    if not valor:
        raise RuntimeError(
            f"Variable de entorno {nombre!r} no definida. "
            f"Configúrala en Railway/entorno (ver .env.example)."
        )
    return valor


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "empleospublicos"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": _requerido_env("DB_PASSWORD"),
}

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

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
OFFER_STATUS_SQL = (
    "CASE "
    "WHEN COALESCE(o.activa, TRUE) = FALSE THEN 'closed' "
    "WHEN LOWER(COALESCE(NULLIF(o.estado, ''), '')) IN "
    "('cerrada', 'cerrado', 'cerrada_manual', 'vencido', 'finalizada', 'closed', 'expired') THEN 'closed' "
    "WHEN COALESCE(o.fecha_inicio, o.fecha_publicacion) IS NOT NULL "
    "  AND COALESCE(o.fecha_inicio, o.fecha_publicacion) > CURRENT_DATE THEN 'upcoming' "
    "WHEN o.fecha_cierre IS NOT NULL AND o.fecha_cierre < CURRENT_DATE THEN 'closed' "
    "WHEN o.fecha_cierre = CURRENT_DATE THEN 'closing_today' "
    "WHEN o.fecha_cierre IS NULL OR o.fecha_cierre > CURRENT_DATE THEN 'active' "
    "ELSE 'unknown' "
    "END"
)
ACTIVE_OFFER_SQL = f"{OFFER_STATUS_SQL} IN ('active', 'closing_today')"
# Dominio público del frontend. Los dominios de marca históricos
# (contrataoplanta.cl / estadoemplea.cl / empleoestado.cl) ya no resuelven
# en DNS — si se filtran a un og:image o og:url, el crawler recibe NXDOMAIN
# y el unfurl no se renderiza. Apuntamos a Cloudflare Pages por defecto.
SITE_URL = (
    os.getenv("SITE_URL", "https://estadoemplea.pages.dev")
    or "https://estadoemplea.pages.dev"
).rstrip("/")
ADMIN_PASSWORD = _requerido_env("ADMIN_PASSWORD")
# ADMIN_PATH: prefijo secreto de las rutas de administración.
# Configura esta variable en Railway para ocultar el punto de entrada.
# Ejemplo: ADMIN_PATH=f8a3d2e7  → rutas en /api/f8a3d2e7/stats, etc.
ADMIN_PATH = os.getenv("ADMIN_PATH", "_gestion_ops").strip("/")

# ── Rate limiting en memoria (simple, por IP) ─────────────────
import time as _time
from collections import defaultdict as _defaultdict

_auth_failures: dict[str, list[float]] = _defaultdict(list)
_RATE_WINDOW_SEG = 600   # 10 minutos
_RATE_MAX_INTENTOS = 5   # máx. intentos fallidos por ventana


def _check_rate_limit(ip: str) -> None:
    ahora = _time.monotonic()
    corte = ahora - _RATE_WINDOW_SEG
    _auth_failures[ip] = [t for t in _auth_failures[ip] if t > corte]
    if len(_auth_failures[ip]) >= _RATE_MAX_INTENTOS:
        raise HTTPException(
            status_code=429,
            detail="Demasiados intentos fallidos. Espere 10 minutos.",
            headers={"Retry-After": str(_RATE_WINDOW_SEG)},
        )


def _record_failure(ip: str) -> None:
    _auth_failures[ip].append(_time.monotonic())


def _verify_admin(request: Request) -> str:
    """Verifica Authorization + rate limiting para endpoints de administración.

    Acepta esquemas ``Basic <b64>`` o ``Bearer <b64>`` con payload
    ``ops:<password>`` codificado en base64. Usamos ``Bearer`` desde el
    frontend para evitar que Firefox/Safari intercepten el 401 con su diálogo
    nativo de autenticación (que aborta la promesa ``fetch()`` con
    "Failed to fetch" antes de que el JS pueda leer el status). Aceptamos
    ``Basic`` también por compatibilidad con clientes CLI (curl).
    """
    ip = (request.client.host if request.client else "unknown") or "unknown"
    _check_rate_limit(ip)

    auth_header = request.headers.get("authorization")
    if not auth_header:
        # Sin credenciales: emitimos desafío con un scheme no estándar
        # ("Token") para que el navegador no active su diálogo de Basic Auth.
        raise HTTPException(
            status_code=401,
            detail="Autenticación requerida",
            headers={"WWW-Authenticate": 'Token realm="Gestion"'},
        )

    scheme, _, token = auth_header.partition(" ")
    credentials_ok = False
    if scheme.lower() in ("basic", "bearer") and token:
        try:
            decoded = base64.b64decode(token.strip(), validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            decoded = ""
        user, sep, password = decoded.partition(":")
        if sep and user == "ops" and password:
            credentials_ok = secrets.compare_digest(
                password.encode("utf-8"),
                ADMIN_PASSWORD.encode("utf-8"),
            )

    if not credentials_ok:
        _record_failure(ip)
        # El cliente ya envió credenciales; no incluimos WWW-Authenticate para
        # evitar el re-desafío nativo del navegador.
        raise HTTPException(
            status_code=401,
            detail="Credenciales incorrectas",
        )
    return "ops"
WEB_INDEX_PATH = _PROJECT_ROOT / "web" / "index.html"
DEFAULT_OG_IMAGE = f"{SITE_URL}/og-default.jpg"
STATUS_LEGACY_MAP = {
    "active": "activo",
    "closing_today": "activo",
    "upcoming": "proximo",
    "closed": "cerrado",
    "unknown": "desconocido",
}


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


class _DictCursorWrapper:
    """Thin wrapper that makes pg8000 cursors behave like psycopg2 RealDictCursor."""
    def __init__(self, cursor: Any) -> None:
        self._cur = cursor

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount

    def execute(self, sql: str, params: Any = None) -> None:
        self._cur.execute(sql, params or [])

    def _cols(self) -> list[str]:
        return [d[0] for d in (self._cur.description or [])]

    def fetchall(self) -> list[dict[str, Any]]:
        cols = self._cols()
        return [dict(zip(cols, row)) for row in (self._cur.fetchall() or [])]

    def fetchone(self) -> dict[str, Any] | None:
        row = self._cur.fetchone()
        if row is None:
            return None
        return dict(zip(self._cols(), row))

    def __enter__(self) -> "_DictCursorWrapper":
        return self

    def __exit__(self, *_: Any) -> None:
        self._cur.close()


def get_connection() -> Any:
    try:
        if _PG_DRIVER == "psycopg2":
            return psycopg2.connect(**DB_CONFIG)
        else:
            return _pg8000.connect(
                host=DB_CONFIG["host"],
                port=int(DB_CONFIG["port"]),
                database=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
            )
    except Exception as exc:  # pragma: no cover
        logger.exception("No se pudo abrir la conexion a PostgreSQL: %s", exc)
        raise HTTPException(status_code=503, detail="Base de datos no disponible") from exc


@contextmanager
def get_cursor():
    connection = get_connection()
    try:
        if _PG_DRIVER == "psycopg2":
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                yield connection, cursor
        else:
            cursor = _DictCursorWrapper(connection.cursor())
            yield connection, cursor
    finally:
        connection.close()


def execute_fetch_all(sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with get_cursor() as (_, cursor):
        cursor.execute(sql, params or [])
        return [dict(row) for row in cursor.fetchall()]


def execute_fetch_one(sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> dict[str, Any] | None:
    with get_cursor() as (_, cursor):
        cursor.execute(sql, params or [])
        row = cursor.fetchone()
        return dict(row) if row else None


_table_columns_cache: dict[str, set[str]] = {}


def _table_columns(table: str) -> set[str]:
    """Devuelve el set de columnas de una tabla (cacheado por proceso).

    Se usa para construir queries resilientes cuando el schema de prod no
    coincide exactamente con el del repo (renombres, columnas opcionales).
    """
    if table not in _table_columns_cache:
        rows = execute_fetch_all(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s",
            [table],
        )
        _table_columns_cache[table] = {r["column_name"] for r in rows}
    return _table_columns_cache[table]


def _coalesce_present(cols: set[str], candidates: tuple[str, ...], default: str | None = None) -> str:
    """Genera una expresión SQL con las columnas candidatas presentes en ``cols``.

    Si ninguna existe, retorna ``default`` (ej. ``"NULL"`` o ``"0"``).
    """
    present = [c for c in candidates if c in cols]
    if not present:
        return default if default is not None else "NULL"
    if len(present) == 1 and default is None:
        return present[0]
    parts = present + ([default] if default is not None else [])
    return f"COALESCE({', '.join(parts)})"


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


def ofertas_base_sql() -> str:
    return """
    FROM ofertas o
    LEFT JOIN instituciones i ON i.id = o.institucion_id
    """


def ofertas_select_sql() -> str:
    return f"""
    SELECT
        o.id,
        o.institucion_id,
        -- Prioridad: nombre tal como aparece en la oferta oficial (o.institucion_nombre)
        -- sobre el match del catálogo (i.nombre). El match por institucion_id se
        -- hace por heurística sobre nombres y puede asignar el portal madre o el
        -- ministerio superior (ej. "Superintendencia de Salud") cuando la vacante
        -- real pertenece a un hospital/servicio específico (ej. "Hospital Base
        -- San José de Osorno"). El texto extraído por el scraper desde la oferta
        -- es más fiel a lo que el usuario debe ver.
        COALESCE(NULLIF(TRIM(o.institucion_nombre), ''), i.nombre, 'Sin institución') AS institucion,
        COALESCE(i.sigla, i.nombre_corto) AS sigla,
        COALESCE(o.cargo, 'Sin cargo') AS cargo,
        COALESCE(o.descripcion, '') AS descripcion,
        COALESCE(o.requisitos, o.requisitos_texto, '') AS requisitos,
        COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) AS tipo_contrato,
        COALESCE(o.region, i.region) AS region,
        o.ciudad,
        COALESCE(i.sector, o.sector, i.tipo) AS sector,
        o.renta_bruta_min,
        o.renta_bruta_max,
        o.grado_eus,
        COALESCE(
            o.jornada,
            CASE
                WHEN o.horas_semanales IS NOT NULL THEN o.horas_semanales::text || ' hrs / semana'
                ELSE NULL
            END
        ) AS jornada,
        o.area_profesional,
        o.fecha_publicacion,
        o.fecha_inicio,
        o.fecha_cierre,
        COALESCE(o.url_oferta, o.url_original) AS url_oferta,
        COALESCE(o.url_bases, o.url_original, o.url_oferta) AS url_bases,
        o.url_oferta_valida,
        o.url_bases_valida,
        o.url_valida_chequeada_en,
        {OFFER_STATUS_SQL} AS estado,
        COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) AS fecha_scraped,
        COALESCE(o.fecha_actualizado, o.actualizada_en, o.creada_en) AS fecha_actualizado,
        i.plataforma_empleo AS plataforma,
        i.url_empleo AS institucion_url_empleo
    """


def build_ofertas_filters(
    q: str | None = None,
    region: str | None = None,
    sector: str | None = None,
    tipo: str | None = None,
    institucion_id: int | None = None,
    area_profesional: str | None = None,
    renta_min: int | None = None,
    ciudad: str | None = None,
    comunas: str | None = None,
    cierra_pronto: bool = False,
    nuevas: bool = False,
    solo_activas: bool = True,
    closed_only: bool = False,
) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []

    if solo_activas:
        where.append(ACTIVE_OFFER_SQL)
    if closed_only:
        where.append(f"{OFFER_STATUS_SQL} = 'closed'")

    if q:
        where.append(
            "("
            "to_tsvector('spanish', coalesce(o.cargo, '') || ' ' || coalesce(i.nombre, '') || ' ' || coalesce(o.descripcion, '')) @@ plainto_tsquery('spanish', %s) "
            "OR o.cargo ILIKE %s "
            "OR COALESCE(i.nombre, o.institucion_nombre, '') ILIKE %s "
            "OR COALESCE(o.descripcion, '') ILIKE %s"
            ")"
        )
        like = f"%{q}%"
        params.extend([q, like, like, like])

    if region:
        where.append("COALESCE(o.region, i.region, '') ILIKE %s")
        params.append(f"%{region}%")

    if sector:
        where.append("COALESCE(i.sector, o.sector, i.tipo, '') ILIKE %s")
        params.append(f"%{sector}%")

    if tipo:
        tipos = [item.strip() for item in tipo.split(",") if item.strip()]
        if len(tipos) == 1:
            where.append("COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) ILIKE %s")
            params.append(f"%{tipos[0]}%")
        elif tipos:
            clauses = []
            for item in tipos:
                clauses.append("COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) ILIKE %s")
                params.append(f"%{item}%")
            where.append("(" + " OR ".join(clauses) + ")")

    if institucion_id is not None:
        where.append("o.institucion_id = %s")
        params.append(institucion_id)

    if area_profesional:
        where.append("o.area_profesional ILIKE %s")
        params.append(f"%{area_profesional}%")

    if renta_min is not None:
        where.append("(o.renta_bruta_min >= %s OR o.renta_bruta_max >= %s)")
        params.extend([renta_min, renta_min])

    if comunas:
        lista_comunas = [item.strip() for item in comunas.split(",") if item.strip()]
        if lista_comunas:
            clauses = []
            for item in lista_comunas:
                clauses.append("o.ciudad ILIKE %s")
                params.append(f"%{item}%")
            where.append("(" + " OR ".join(clauses) + ")")
    elif ciudad:
        where.append("o.ciudad ILIKE %s")
        params.append(f"%{ciudad}%")

    if cierra_pronto:
        where.append("o.fecha_cierre BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '5 days'")

    if nuevas:
        where.append("COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) >= NOW() - INTERVAL '48 hours'")

    return (" WHERE " + " AND ".join(where)) if where else "", params


def dias_restantes(value: date | None) -> int | None:
    if value is None:
        return None
    return (value - date.today()).days


# ───── Resolución de sitio web real por institución ──────────────────────────
# El catálogo JSON (repositorio_instituciones_publicas_chile.json) contiene
# `sitio_web` — el dominio oficial de la institución — incluso cuando su
# `url_empleo` apunta al portal intermediario (empleospublicos.cl, etc.).
# Esa información NO vive en la tabla `instituciones`, así que la cargamos
# en memoria a partir del JSON y la cacheamos por mtime del archivo.

_PORTAL_DOMAINS_LOWER = {
    "empleospublicos.cl", "www.empleospublicos.cl",
    "trabajando.com", "www.trabajando.com",
    "trabajando.cl", "www.trabajando.cl",
    "hiringroom.com", "www.hiringroom.com",
    "buk.cl", "www.buk.cl",
    "chileatiende.cl", "www.chileatiende.cl",
    "empleos.gob.cl", "www.empleos.gob.cl",
    "postulaciones.cl", "www.postulaciones.cl",
    "sistemadeconcursos.cl", "www.sistemadeconcursos.cl",
    "mitrabajodigno.cl", "www.mitrabajodigno.cl",
    "ucampus.net", "www.ucampus.net",
}

_sitio_web_cache: dict[str, Any] = {"mtime": 0.0, "by_name": {}, "by_id": {}}


def _fold_institution_name(value: str | None) -> str:
    if not value:
        return ""
    import unicodedata
    folded = unicodedata.normalize("NFD", value)
    folded = "".join(ch for ch in folded if unicodedata.category(ch) != "Mn")
    folded = re.sub(r"[^a-zA-Z0-9\s]", " ", folded.lower())
    folded = re.sub(r"\s+", " ", folded).strip()
    return folded


def _extract_root_domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        host = urlparse(url if "://" in url else f"https://{url}").hostname or ""
    except Exception:
        return None
    host = host.strip().lower().lstrip(".")
    if not host:
        return None
    if host in _PORTAL_DOMAINS_LOWER:
        return None
    # Remueve www. para que logo.clearbit.com tenga mejor hit rate.
    return host[4:] if host.startswith("www.") else host


def _load_sitio_web_map() -> dict[str, Any]:
    """Mapea nombre normalizado de institución → dominio oficial (sitio_web)."""
    if not _CATALOG_PATH.exists():
        return _sitio_web_cache
    try:
        mtime = _CATALOG_PATH.stat().st_mtime
    except OSError:
        return _sitio_web_cache
    if _sitio_web_cache["mtime"] == mtime and _sitio_web_cache["by_name"]:
        return _sitio_web_cache
    try:
        payload = json.loads(_CATALOG_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        return _sitio_web_cache
    insts = payload.get("instituciones") if isinstance(payload, dict) else payload
    if not isinstance(insts, list):
        return _sitio_web_cache
    by_name: dict[str, str] = {}
    by_id: dict[int, str] = {}
    for inst in insts:
        domain = _extract_root_domain(inst.get("sitio_web"))
        if not domain:
            continue
        nombre = inst.get("nombre")
        if nombre:
            key = _fold_institution_name(nombre)
            if key:
                by_name.setdefault(key, domain)
        sigla = inst.get("sigla")
        if sigla:
            key_sigla = _fold_institution_name(sigla)
            if key_sigla:
                by_name.setdefault(key_sigla, domain)
        inst_id = inst.get("id")
        if isinstance(inst_id, int):
            by_id.setdefault(inst_id, domain)
    _sitio_web_cache["mtime"] = mtime
    _sitio_web_cache["by_name"] = by_name
    _sitio_web_cache["by_id"] = by_id
    return _sitio_web_cache


def resolve_institucion_sitio_web(
    institucion: str | None, institucion_id: int | None = None
) -> str | None:
    """Devuelve el dominio oficial (sin esquema) de la institución o None.

    Estrategia:
      1. Match por `institucion_id` en el catálogo (más preciso).
      2. Match por nombre normalizado.
      3. Match por contención parcial (p. ej. "Municipalidad de X" contiene
         una entrada "Municipalidad de X" del catálogo).
    """
    cache = _load_sitio_web_map()
    by_id = cache.get("by_id") or {}
    by_name = cache.get("by_name") or {}
    if isinstance(institucion_id, int) and institucion_id in by_id:
        return by_id[institucion_id]
    key = _fold_institution_name(institucion)
    if not key:
        return None
    if key in by_name:
        return by_name[key]
    # Match por contención: escoger la entrada del catálogo con la clave más
    # larga contenida en el nombre consultado (evita falsos positivos cortos).
    best: tuple[int, str] | None = None
    for catalog_key, domain in by_name.items():
        if len(catalog_key) < 10:
            continue
        if catalog_key in key:
            if best is None or len(catalog_key) > best[0]:
                best = (len(catalog_key), domain)
    return best[1] if best else None


def serialize_offer(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["dias_restantes"] = dias_restantes(data.get("fecha_cierre"))
    estado = str(data.get("estado") or "unknown").strip().lower()
    data["estado_normalizado"] = estado
    data["estado_legacy"] = STATUS_LEGACY_MAP.get(estado, "desconocido")
    # Expone el sitio web real de la institución (desde el catálogo JSON), para
    # que el frontend pueda resolver el logo correcto aunque la oferta venga
    # intermediada por Empleos Públicos u otros portales.
    data["institucion_sitio_web"] = resolve_institucion_sitio_web(
        data.get("institucion"), data.get("institucion_id")
    )
    return data


def validate_email(email: str) -> str:
    value = email.strip().lower()
    if not EMAIL_RE.match(value):
        raise HTTPException(status_code=422, detail="Email invalido")
    return value


def _truncate_text(value: str, max_len: int) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip(" ,.-") + "…"


def _format_fecha_larga(value: date | None) -> str | None:
    if value is None:
        return None
    meses = (
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    )
    return f"{value.day} de {meses[value.month - 1]} de {value.year}"


def _format_renta_bruta(oferta: dict[str, Any]) -> str | None:
    rmin = oferta.get("renta_bruta_min")
    rmax = oferta.get("renta_bruta_max")
    if isinstance(rmin, int) and isinstance(rmax, int) and rmin > 0 and rmax > 0:
        if rmin == rmax:
            return f"${rmin:,.0f}".replace(",", ".")
        return f"${rmin:,.0f}".replace(",", ".") + " a " + f"${rmax:,.0f}".replace(",", ".")
    if isinstance(rmax, int) and rmax > 0:
        return f"Hasta ${rmax:,.0f}".replace(",", ".")
    if isinstance(rmin, int) and rmin > 0:
        return f"Desde ${rmin:,.0f}".replace(",", ".")
    return None


def _escape_attr(value: str) -> str:
    return html.escape(value, quote=True)


def _set_title(html_doc: str, title: str) -> str:
    safe = html.escape(title)
    if re.search(r"<title>.*?</title>", html_doc, flags=re.IGNORECASE | re.DOTALL):
        return re.sub(r"<title>.*?</title>", f"<title>{safe}</title>", html_doc, count=1, flags=re.IGNORECASE | re.DOTALL)
    return html_doc.replace("</head>", f"<title>{safe}</title>\n</head>", 1)


def _set_meta(html_doc: str, key: str, content: str, *, attr: str = "name") -> str:
    pattern = re.compile(
        rf'<meta\s+[^>]*{attr}\s*=\s*["\']{re.escape(key)}["\'][^>]*>',
        flags=re.IGNORECASE,
    )
    tag = f'<meta {attr}="{_escape_attr(key)}" content="{_escape_attr(content)}">'
    if pattern.search(html_doc):
        return pattern.sub(tag, html_doc, count=1)
    return html_doc.replace("</head>", f"{tag}\n</head>", 1)


def _set_canonical(html_doc: str, href: str) -> str:
    pattern = re.compile(r'<link\s+[^>]*rel\s*=\s*["\']canonical["\'][^>]*>', flags=re.IGNORECASE)
    tag = f'<link rel="canonical" href="{_escape_attr(href)}">'
    if pattern.search(html_doc):
        return pattern.sub(tag, html_doc, count=1)
    return html_doc.replace("</head>", f"{tag}\n</head>", 1)


def _inject_offer_path_bootstrap(html_doc: str, oferta_id: int | None) -> str:
    if not oferta_id:
        return html_doc
    marker = "window.__OFERTA_PATH_ID__"
    if marker in html_doc:
        return html_doc
    script = (
        "<script>"
        f"{marker}={oferta_id};"
        "try{const u=new URL(window.location.href);"
        "if(!u.searchParams.get('oferta')){u.searchParams.set('oferta',String(window.__OFERTA_PATH_ID__));"
        "history.replaceState(null,'',u.pathname+u.search+u.hash);}}catch(e){}"
        "</script>"
    )
    return html_doc.replace("</head>", f"{script}\n</head>", 1)


def fetch_offer_for_meta(oferta_id: int) -> dict[str, Any] | None:
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
        return None
    return serialize_offer(row)


def build_offer_meta(oferta: dict[str, Any] | None, canonical_url: str) -> dict[str, str]:
    if not oferta:
        return {
            "title": "estadoemplea.cl — Empleos públicos vigentes en Chile",
            "description": "Encuentra empleos públicos en Chile, filtra por institución y revisa oportunidades del sector público.",
            "og_image": DEFAULT_OG_IMAGE,
            "canonical": canonical_url,
        }

    cargo = (oferta.get("cargo") or "Oferta laboral").strip()
    institucion = (oferta.get("institucion") or "Institución pública").strip()
    ciudad = (oferta.get("ciudad") or "").strip()
    region = (oferta.get("region") or "").strip()
    tipo = (oferta.get("tipo_contrato") or "").strip()
    cierre = _format_fecha_larga(oferta.get("fecha_cierre"))
    estado = (oferta.get("estado") or "").strip()
    renta = _format_renta_bruta(oferta)

    title = _truncate_text(f"{cargo} – {institucion}", 90)
    desc_parts = []
    if region:
        desc_parts.append(region)
    if ciudad and ciudad.lower() not in region.lower():
        desc_parts.append(ciudad)
    if tipo:
        desc_parts.append(tipo.capitalize())
    if renta:
        desc_parts.append(renta)
    if cierre:
        desc_parts.append(f"Cierre: {cierre}")
    elif estado:
        desc_parts.append(f"Estado: {estado}")
    description = _truncate_text(" · ".join(desc_parts) or "Revisa requisitos, renta y plazos de postulación.", 200)
    oferta_id = oferta.get("id")
    image_url = f"{SITE_URL}/api/og/{oferta_id}.png" if oferta_id else DEFAULT_OG_IMAGE

    return {
        "title": title,
        "description": description,
        "og_image": image_url,
        "canonical": canonical_url,
    }


def render_index_with_meta(meta: dict[str, str], *, oferta_id_for_bootstrap: int | None = None) -> str:
    html_doc = WEB_INDEX_PATH.read_text(encoding="utf-8")
    html_doc = _set_title(html_doc, meta["title"])
    html_doc = _set_meta(html_doc, "description", meta["description"], attr="name")
    html_doc = _set_meta(html_doc, "og:title", meta["title"], attr="property")
    html_doc = _set_meta(html_doc, "og:description", meta["description"], attr="property")
    html_doc = _set_meta(html_doc, "og:url", meta["canonical"], attr="property")
    html_doc = _set_meta(html_doc, "og:image", meta["og_image"], attr="property")
    # Hints explícitos: algunos crawlers (WhatsApp, Slack) fallan a summary
    # pequeño si no encuentran dimensiones declaradas.
    html_doc = _set_meta(html_doc, "og:image:width", "1200", attr="property")
    html_doc = _set_meta(html_doc, "og:image:height", "630", attr="property")
    html_doc = _set_meta(html_doc, "og:image:alt", meta["title"], attr="property")
    html_doc = _set_meta(html_doc, "og:type", "website", attr="property")
    html_doc = _set_meta(html_doc, "twitter:card", "summary_large_image", attr="name")
    html_doc = _set_meta(html_doc, "twitter:title", meta["title"], attr="name")
    html_doc = _set_meta(html_doc, "twitter:description", meta["description"], attr="name")
    html_doc = _set_meta(html_doc, "twitter:image", meta["og_image"], attr="name")
    html_doc = _set_meta(html_doc, "twitter:image:alt", meta["title"], attr="name")
    html_doc = _set_canonical(html_doc, meta["canonical"])
    html_doc = _inject_offer_path_bootstrap(html_doc, oferta_id_for_bootstrap)
    return html_doc


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


@app.on_event("startup")
def on_startup() -> None:
    # No bloquear el arranque si Postgres aún no responde: la API queda viva
    # respondiendo 503 por request hasta que la DB vuelva. Si abortamos aquí,
    # uvicorn cae y nginx devuelve 502/connection refused al frontend.
    try:
        ensure_api_schema()
        logger.info("API iniciada y esquema verificado")
    except Exception as exc:
        logger.error(
            "API iniciada sin validar esquema (DB no disponible aún): %s", exc
        )


@app.get("/api/ofertas")
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


@app.get("/api/ofertas/{oferta_id}")
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


@app.get("/api/og/{oferta_id}.png")
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


@app.get("/api/estadisticas")
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


@app.get("/api/instituciones")
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


@app.get("/api/instituciones/{institucion_id}/ofertas")
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


@app.get("/api/instituciones/{institucion_id}/estadisticas")
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


@app.get("/api/historial")
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


@app.get("/api/sugerencias")
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


@app.post("/api/alertas")
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


@app.get("/api/scraper/resumen")
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


@app.get("/api/scraper/fuentes")
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

@app.get("/api/regiones")
async def api_regiones() -> list[dict[str, Any]]:
    """Regiones de Chile con nombres oficiales (API DPA del Estado)."""
    return await get_regiones()


@app.get("/api/regiones/{codigo_region}/comunas")
async def api_comunas(codigo_region: str) -> list[dict[str, Any]]:
    """Comunas de una región específica (API DPA del Estado)."""
    return await get_comunas(codigo_region)


# ──────────────────── Leyes por institución (BCN Ley Chile) ─────────────────

@app.get("/api/instituciones/{institucion_id}/ley")
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


@app.get("/api/leyes/buscar")
async def api_buscar_ley(q: str = Query(..., min_length=2, max_length=200)) -> list[dict[str, Any]]:
    """Buscar normativa en BCN LeyChile."""
    return await buscar_ley_bcn(q)


# ──────────────────── Validación de email (Mailcheck) ───────────────────────

@app.get("/api/validar-email")
def api_validar_email(email: str = Query(..., min_length=3, max_length=200)) -> dict[str, Any]:
    """
    Valida un email: detecta dominios temporales/desechables y sugiere
    correcciones de typos comunes (gmial→gmail, hotnail→hotmail).
    """
    return mailcheck_validar(email)


# ──────────────────── Búsqueda rápida (Meilisearch) ─────────────────────────

@app.get("/api/buscar")
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


@app.get("/api/autocompletar")
def api_autocompletar(
    q: str = Query(..., min_length=1, max_length=100),
    limite: int = Query(8, ge=1, le=20),
) -> list[dict[str, str]]:
    """
    Autocompletado instantáneo de cargos con Meilisearch.
    Retorna sugerencias con highlights y contexto.
    """
    return meili_autocompletar(q, limite=limite)


@app.post(f"/api/{ADMIN_PATH}/meilisearch/reindexar", tags=["admin"])
def api_reindexar_meili(_user: str = Depends(_verify_admin)) -> dict[str, Any]:
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
    _user: str = Depends(_verify_admin),
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


@app.get("/oferta/{oferta_id}", response_class=HTMLResponse, include_in_schema=False)
def web_offer(oferta_id: int) -> HTMLResponse:
    oferta_data = fetch_offer_for_meta(oferta_id)
    canonical = f"{SITE_URL}/oferta/{oferta_id}"
    meta = build_offer_meta(oferta_data, canonical_url=canonical)
    html_doc = render_index_with_meta(meta, oferta_id_for_bootstrap=oferta_id)
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=120, stale-while-revalidate=900"},
    )


@app.get("/share/oferta/{oferta_id}", include_in_schema=False)
def web_offer_share(oferta_id: int) -> RedirectResponse:
    return RedirectResponse(url=f"/oferta/{oferta_id}", status_code=308)


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
#  ADMIN API — protegida con HTTP Basic Auth (ADMIN_PASSWORD env var)
# ════════════════════════════════════════════════════════════════════════════

@app.get(f"/api/{ADMIN_PATH}/stats", tags=["admin"])
def admin_stats(_user: str = Depends(_verify_admin)) -> dict[str, Any]:
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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
    _user: str = Depends(_verify_admin),
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


@app.get("/api", include_in_schema=False)
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
