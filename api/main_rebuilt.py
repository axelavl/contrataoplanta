from __future__ import annotations

import logging
import math
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("api.contrataoplanta")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "empleospublicos"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "axel1234"),
}

ALLOW_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://contrataoplanta.cl",
    "https://www.contrataoplanta.cl",
]

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ESTADO_SQL = (
    "COALESCE(NULLIF(o.estado, ''), "
    "CASE "
    "WHEN COALESCE(o.activa, TRUE) = FALSE THEN 'cerrado' "
    "WHEN o.fecha_cierre IS NOT NULL AND o.fecha_cierre < CURRENT_DATE THEN 'vencido' "
    "ELSE 'activo' END)"
)


class AlertaPayload(BaseModel):
    email: str
    region: str | None = None
    termino: str | None = None
    tipo_contrato: str | None = None


@dataclass(slots=True)
class Paginacion:
    pagina: int
    por_pagina: int

    @property
    def offset(self) -> int:
        return (self.pagina - 1) * self.por_pagina


def get_connection() -> psycopg2.extensions.connection:
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as exc:  # pragma: no cover
        logger.exception("No se pudo abrir la conexion a PostgreSQL: %s", exc)
        raise HTTPException(status_code=503, detail="Base de datos no disponible") from exc


@contextmanager
def get_cursor():
    connection = get_connection()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
        "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS sector VARCHAR(100)",
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
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_cierre DATE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_oferta TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_original TEXT",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS estado VARCHAR(20)",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS activa BOOLEAN DEFAULT TRUE",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_scraped TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_actualizado TIMESTAMP DEFAULT NOW()",
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
        COALESCE(i.nombre, o.institucion_nombre, 'Sin institucion') AS institucion,
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
        o.fecha_cierre,
        o.fecha_inicio,
        COALESCE(o.url_oferta, o.url_original) AS url_oferta,
        COALESCE(o.url_bases, o.url_original, o.url_oferta) AS url_bases,
        {ESTADO_SQL} AS estado,
        COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) AS fecha_scraped,
        COALESCE(o.fecha_actualizado, o.actualizada_en, o.creada_en) AS fecha_actualizado,
        i.plataforma_empleo AS plataforma
    """


def build_ofertas_filters(
    q: str | None = None,
    region: str | None = None,
    sector: str | None = None,
    tipo: str | None = None,
    institucion_id: int | None = None,
    cierra_pronto: bool = False,
    nuevas: bool = False,
    solo_activas: bool = True,
    closed_only: bool = False,
) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []

    if solo_activas:
        where.append(f"{ESTADO_SQL} = 'activo'")
    if closed_only:
        where.append(f"{ESTADO_SQL} IN ('cerrado', 'vencido')")

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
        where.append("COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) ILIKE %s")
        params.append(f"%{tipo}%")

    if institucion_id is not None:
        where.append("o.institucion_id = %s")
        params.append(institucion_id)

    if cierra_pronto:
        where.append("o.fecha_cierre BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '5 days'")

    if nuevas:
        where.append("COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) >= NOW() - INTERVAL '48 hours'")

    return (" WHERE " + " AND ".join(where)) if where else "", params


def dias_restantes(value: date | None) -> int | None:
    if value is None:
        return None
    return (value - date.today()).days


def serialize_offer(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["dias_restantes"] = dias_restantes(data.get("fecha_cierre"))
    return data


def validate_email(email: str) -> str:
    value = email.strip().lower()
    if not EMAIL_RE.match(value):
        raise HTTPException(status_code=422, detail="Email invalido")
    return value


app = FastAPI(
    title="contrata o planta .cl - API",
    version="2.1.0",
    description="API publica del agregador de empleo publico chileno",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    ensure_api_schema()
    logger.info("API iniciada y esquema verificado")


@app.get("/api/ofertas")
def get_ofertas(
    q: str | None = Query(None),
    region: str | None = Query(None),
    sector: str | None = Query(None),
    tipo: str | None = Query(None),
    cierra_pronto: bool = Query(False),
    nuevas: bool = Query(False),
    orden: str = Query("recientes"),
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    pag = Paginacion(pagina=pagina, por_pagina=por_pagina)
    where_sql, params = build_ofertas_filters(
        q=q,
        region=region,
        sector=sector,
        tipo=tipo,
        cierra_pronto=cierra_pronto,
        nuevas=nuevas,
        solo_activas=True,
    )
    sin_fechas = "CASE WHEN fecha_cierre IS NULL AND fecha_inicio IS NULL THEN 1 ELSE 0 END ASC"
    order_sql = {
        "recientes": f"{sin_fechas}, fecha_scraped DESC NULLS LAST, id DESC",
        "cierre": f"{sin_fechas}, fecha_cierre ASC NULLS LAST, id DESC",
        "renta": f"{sin_fechas}, renta_bruta_max DESC NULLS LAST, renta_bruta_min DESC NULLS LAST, id DESC",
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


@app.get("/api/estadisticas")
def get_estadisticas() -> dict[str, Any]:
    conteos = execute_fetch_one(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE {ESTADO_SQL.replace('o.', '')} = 'activo') AS activas_hoy,
            COUNT(*) FILTER (
                WHERE COALESCE(fecha_scraped, detectada_en, actualizada_en, creada_en) >= NOW() - INTERVAL '48 hours'
                  AND {ESTADO_SQL.replace('o.', '')} = 'activo'
            ) AS nuevas_48h,
            COUNT(*) FILTER (
                WHERE fecha_cierre = CURRENT_DATE
                  AND {ESTADO_SQL.replace('o.', '')} = 'activo'
            ) AS cierran_hoy,
            COUNT(DISTINCT institucion_id) FILTER (WHERE {ESTADO_SQL.replace('o.', '')} = 'activo') AS instituciones_activas
        FROM ofertas o
        """
    ) or {}

    por_sector = execute_fetch_all(
        f"""
        SELECT
            COALESCE(i.sector, o.sector, i.tipo, 'Sin sector') AS sector,
            COUNT(*) AS total
        {ofertas_base_sql()}
        WHERE {ESTADO_SQL} = 'activo'
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
        WHERE {ESTADO_SQL} = 'activo'
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
        COUNT(o.id) FILTER (WHERE {ESTADO_SQL} = 'activo') AS activas
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
    ORDER BY CASE WHEN fecha_cierre IS NULL AND fecha_inicio IS NULL THEN 1 ELSE 0 END ASC, fecha_cierre ASC NULLS LAST, fecha_scraped DESC NULLS LAST
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
    email = validate_email(payload.email)
    region = payload.region.strip() if payload.region else None
    termino = payload.termino.strip() if payload.termino else None
    tipo_contrato = payload.tipo_contrato.strip() if payload.tipo_contrato else None

    with get_cursor() as (connection, cursor):
        cursor.execute(
            """
            UPDATE alertas_suscripciones
            SET activa = TRUE, actualizada_en = NOW()
            WHERE LOWER(email) = LOWER(%s)
              AND COALESCE(region, '') = COALESCE(%s, '')
              AND COALESCE(termino, '') = COALESCE(%s, '')
              AND COALESCE(tipo_contrato, '') = COALESCE(%s, '')
            """,
            [email, region, termino, tipo_contrato],
        )
        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO alertas_suscripciones (
                    email, region, termino, tipo_contrato, activa, creada_en, actualizada_en
                ) VALUES (%s, %s, %s, %s, TRUE, NOW(), NOW())
                """,
                [email, region, termino, tipo_contrato],
            )
        connection.commit()

    return {"ok": True, "mensaje": "Alerta registrada correctamente"}


@app.get("/health")
def health() -> dict[str, Any] | JSONResponse:
    try:
        row = execute_fetch_one("SELECT NOW() AS ts")
        return {"status": "ok", "db": str(row["ts"]) if row else None}
    except Exception as exc:  # pragma: no cover
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(exc)})


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "nombre": "contrata o planta .cl - API",
        "version": "2.1.0",
        "docs": "/docs",
        "db_host": DB_CONFIG["host"],
        "endpoints": [
            "GET /api/ofertas",
            "GET /api/ofertas/{id}",
            "GET /api/estadisticas",
            "GET /api/instituciones",
            "GET /api/instituciones/{id}/ofertas",
            "GET /api/instituciones/{id}/estadisticas",
            "GET /api/historial",
            "GET /api/sugerencias",
            "POST /api/alertas",
            "GET /health",
        ],
    }
