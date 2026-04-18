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
from urllib.parse import urlencode

try:
    import psycopg2
    import psycopg2.extras
    _PG_DRIVER = "psycopg2"
except ImportError:
    import pg8000.dbapi as _pg8000  # type: ignore[import]
    _PG_DRIVER = "pg8000"

from fastapi import FastAPI, HTTPException, Query, Request
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

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "empleospublicos"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "axel1234"),
}

DEFAULT_ALLOW_ORIGINS = [
    "null",
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
SITE_URL = (os.getenv("SITE_URL", "https://contrataoplanta.cl") or "https://contrataoplanta.cl").rstrip("/")
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
    allow_headers=["*"],
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


@app.post("/api/meilisearch/reindexar")
def api_reindexar_meili() -> dict[str, Any]:
    """Re-indexa todas las ofertas activas en Meilisearch."""
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

@app.post("/api/alertas/enviar")
def api_enviar_alertas_pendientes() -> dict[str, Any]:
    """
    Procesa y envía alertas pendientes a los suscriptores.
    Busca ofertas nuevas (últimas 24h) que coincidan con los filtros
    de cada suscriptor y les envía un email via Resend.
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
