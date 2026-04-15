"""
contrata o planta .cl — API principal
FastAPI + PostgreSQL (psycopg2)

Endpoints:
  GET /api/ofertas          → lista con filtros y paginación
  GET /api/ofertas/{id}     → detalle de una oferta
  GET /api/estadisticas     → conteos para el sidebar
  GET /api/instituciones    → lista de instituciones

Instalación:
  pip install fastapi uvicorn psycopg2-binary python-dotenv

Ejecutar en desarrollo:
  uvicorn api.main:app --reload --port 8000

Documentación automática:
  http://localhost:8000/docs
"""

import os
import logging
from datetime import date, datetime
from typing import Optional, List
from contextlib import asynccontextmanager

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Configuración de la BD — edita estos valores o usa variables de entorno
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "empleospublicos"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin123"),  # ← cambia esto
}

# ─── CONEXIÓN A LA BD ─────────────────────────────────────────────────────────

def get_conn():
    """Abre una conexión a PostgreSQL y la retorna."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        log.error(f"Error conectando a BD: {e}")
        raise HTTPException(status_code=503, detail="Base de datos no disponible")

def query(sql: str, params=None, one=False):
    """Ejecuta una consulta y retorna filas como lista de dicts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if one:
                return cur.fetchone()
            return cur.fetchall()
    finally:
        conn.close()

# ─── MODELOS PYDANTIC (respuestas de la API) ──────────────────────────────────

class OfertaResumen(BaseModel):
    """Lo que aparece en la tarjeta del listado."""
    id: int
    institucion: str
    cargo: str
    tipo_contrato: Optional[str]       # planta | contrata | honorarios
    region: Optional[str]
    sector: Optional[str]
    ciudad: Optional[str]
    renta_bruta_min: Optional[int]
    renta_bruta_max: Optional[int]
    grado_eus: Optional[str]
    fecha_cierre: Optional[date]
    fecha_scraped: Optional[datetime]
    dias_restantes: Optional[int]
    url_oferta: Optional[str]
    estado: str                        # activo | cerrado | vencido

class OfertaDetalle(OfertaResumen):
    """Detalle completo para el modal."""
    descripcion: Optional[str]
    requisitos: Optional[str]
    jornada: Optional[str]
    area_profesional: Optional[str]
    experiencia_anos: Optional[int]
    url_bases: Optional[str]
    plataforma: Optional[str]
    institucion_id: Optional[int]

class Estadisticas(BaseModel):
    activas_hoy: int
    nuevas_48h: int
    cierran_hoy: int
    instituciones_activas: int
    por_sector: List[dict]
    historico_mensual: List[dict]

class InstitucionResumen(BaseModel):
    id: int
    nombre: str
    sigla: Optional[str]
    sector: Optional[str]
    region: Optional[str]
    url_empleo: Optional[str]
    activas: int

# ─── APP FASTAPI ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Iniciando contrata o planta .cl API...")
    conn = get_conn()
    conn.close()
    log.info("Conexión a BD verificada OK")
    yield
    log.info("API detenida")

app = FastAPI(
    title="contrata o planta .cl — API",
    description="La plataforma laboral del sector público chileno",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — permite que el frontend (en otro dominio) consuma la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "https://contrataoplanta.cl",
        "https://www.contrataoplanta.cl",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─── ENDPOINT 1: LISTADO DE OFERTAS ──────────────────────────────────────────

@app.get(
    "/api/ofertas",
    response_model=dict,
    summary="Listado de ofertas con filtros y paginación",
    tags=["Ofertas"],
)
def get_ofertas(
    # Filtros de búsqueda
    q:            Optional[str] = Query(None, description="Búsqueda en cargo, institución o descripción"),
    region:       Optional[str] = Query(None, description="Región de Chile"),
    sector:       Optional[str] = Query(None, description="Sector público (Municipal, Salud, etc.)"),
    tipo:         Optional[str] = Query(None, description="planta | contrata | honorarios"),
    institucion:  Optional[int] = Query(None, description="ID de institución específica"),
    cierra_pronto:bool          = Query(False, description="Solo ofertas que cierran en los próximos 5 días"),
    nuevas:       bool          = Query(False, description="Solo ofertas publicadas en las últimas 48 horas"),
    # Ordenamiento
    orden:        str           = Query("recientes", description="recientes | cierre | renta"),
    # Paginación
    pagina:       int           = Query(1, ge=1),
    por_pagina:   int           = Query(20, ge=1, le=100),
):
    # Construir la consulta dinámicamente
    where = ["o.estado = 'activo'"]
    params = []

    if q:
        where.append("(o.cargo ILIKE %s OR i.nombre ILIKE %s OR o.descripcion ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]

    if region:
        where.append("o.region ILIKE %s")
        params.append(f"%{region}%")

    if sector:
        where.append("i.sector ILIKE %s")
        params.append(f"%{sector}%")

    if tipo:
        where.append("o.tipo_contrato ILIKE %s")
        params.append(f"%{tipo}%")

    if institucion:
        where.append("o.institucion_id = %s")
        params.append(institucion)

    if cierra_pronto:
        where.append("o.fecha_cierre BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '5 days'")

    if nuevas:
        where.append("o.fecha_scraped >= NOW() - INTERVAL '48 hours'")

    # Ordenamiento
    orden_sql = {
        "recientes": "o.fecha_scraped DESC",
        "cierre":    "o.fecha_cierre ASC NULLS LAST",
        "renta":     "o.renta_bruta_max DESC NULLS LAST",
    }.get(orden, "o.fecha_scraped DESC")

    where_sql = " AND ".join(where)
    offset = (pagina - 1) * por_pagina

    # Consulta principal
    sql = f"""
        SELECT
            o.id,
            i.nombre                                    AS institucion,
            o.cargo,
            o.tipo_contrato,
            o.region,
            i.sector,
            o.ciudad,
            o.renta_bruta_min,
            o.renta_bruta_max,
            o.grado_eus,
            o.fecha_cierre,
            o.fecha_scraped,
            o.url_oferta,
            o.estado,
            CASE
                WHEN o.fecha_cierre IS NOT NULL
                THEN (o.fecha_cierre - CURRENT_DATE)
                ELSE NULL
            END                                         AS dias_restantes
        FROM ofertas o
        JOIN instituciones i ON o.institucion_id = i.id
        WHERE {where_sql}
        ORDER BY {orden_sql}
        LIMIT %s OFFSET %s
    """

    # Contar total para la paginación
    sql_count = f"""
        SELECT COUNT(*) AS total
        FROM ofertas o
        JOIN instituciones i ON o.institucion_id = i.id
        WHERE {where_sql}
    """

    rows     = query(sql,       params + [por_pagina, offset])
    total_r  = query(sql_count, params, one=True)
    total    = total_r["total"] if total_r else 0

    return {
        "total":      total,
        "pagina":     pagina,
        "por_pagina": por_pagina,
        "paginas":    (total + por_pagina - 1) // por_pagina,
        "ofertas":    [dict(r) for r in rows],
    }

# ─── ENDPOINT 2: DETALLE DE UNA OFERTA ───────────────────────────────────────

@app.get(
    "/api/ofertas/{oferta_id}",
    response_model=dict,
    summary="Detalle completo de una oferta",
    tags=["Ofertas"],
)
def get_oferta(oferta_id: int):
    sql = """
        SELECT
            o.*,
            i.nombre        AS institucion,
            i.sigla         AS institucion_sigla,
            i.sector,
            i.region        AS institucion_region,
            i.url_empleo,
            i.plataforma_empleo AS plataforma,
            (o.fecha_cierre - CURRENT_DATE) AS dias_restantes
        FROM ofertas o
        JOIN instituciones i ON o.institucion_id = i.id
        WHERE o.id = %s
    """
    row = query(sql, (oferta_id,), one=True)
    if not row:
        raise HTTPException(status_code=404, detail="Oferta no encontrada")

    return dict(row)

# ─── ENDPOINT 3: ESTADÍSTICAS PARA EL SIDEBAR ────────────────────────────────

@app.get(
    "/api/estadisticas",
    response_model=dict,
    summary="Estadísticas en tiempo real para el sidebar",
    tags=["Estadísticas"],
)
def get_estadisticas():

    # Conteos principales
    conteos = query("""
        SELECT
            COUNT(*)                                                    AS activas_hoy,
            COUNT(*) FILTER (WHERE fecha_scraped >= NOW() - INTERVAL '48 hours')
                                                                        AS nuevas_48h,
            COUNT(*) FILTER (WHERE fecha_cierre = CURRENT_DATE)        AS cierran_hoy,
            COUNT(DISTINCT institucion_id)                              AS instituciones_activas
        FROM ofertas
        WHERE estado = 'activo'
    """, one=True)

    # Por sector
    por_sector = query("""
        SELECT
            i.sector,
            COUNT(o.id) AS total
        FROM ofertas o
        JOIN instituciones i ON o.institucion_id = i.id
        WHERE o.estado = 'activo'
          AND i.sector IS NOT NULL
        GROUP BY i.sector
        ORDER BY total DESC
        LIMIT 8
    """)

    # Histórico mensual — últimos 7 meses
    historico = query("""
        SELECT
            TO_CHAR(DATE_TRUNC('month', fecha_scraped), 'Mon') AS mes,
            DATE_TRUNC('month', fecha_scraped)                  AS fecha,
            COUNT(*) AS total
        FROM ofertas
        WHERE fecha_scraped >= NOW() - INTERVAL '7 months'
        GROUP BY DATE_TRUNC('month', fecha_scraped)
        ORDER BY fecha ASC
    """)

    # Instituciones más activas esta semana
    mas_activas = query("""
        SELECT
            i.id,
            i.nombre,
            i.sigla,
            COUNT(o.id)                                                      AS activas,
            COUNT(o.id) FILTER (WHERE o.fecha_scraped >= NOW() - INTERVAL '7 days') AS nuevas_semana
        FROM ofertas o
        JOIN instituciones i ON o.institucion_id = i.id
        WHERE o.estado = 'activo'
        GROUP BY i.id, i.nombre, i.sigla
        ORDER BY nuevas_semana DESC, activas DESC
        LIMIT 5
    """)

    return {
        "activas_hoy":          conteos["activas_hoy"]          if conteos else 0,
        "nuevas_48h":           conteos["nuevas_48h"]           if conteos else 0,
        "cierran_hoy":          conteos["cierran_hoy"]          if conteos else 0,
        "instituciones_activas":conteos["instituciones_activas"] if conteos else 0,
        "por_sector":           [dict(r) for r in por_sector],
        "historico_mensual":    [dict(r) for r in historico],
        "mas_activas":          [dict(r) for r in mas_activas],
    }

# ─── ENDPOINT 4: INSTITUCIONES ────────────────────────────────────────────────

@app.get(
    "/api/instituciones",
    response_model=dict,
    summary="Lista de instituciones con conteo de ofertas activas",
    tags=["Instituciones"],
)
def get_instituciones(
    q:       Optional[str] = Query(None, description="Búsqueda por nombre o sigla"),
    sector:  Optional[str] = Query(None),
    region:  Optional[str] = Query(None),
    con_activas: bool      = Query(False, description="Solo instituciones con ofertas activas ahora"),
    pagina:  int           = Query(1, ge=1),
    por_pagina: int        = Query(50, ge=1, le=200),
):
    where = ["1=1"]
    params = []

    if q:
        where.append("(i.nombre ILIKE %s OR i.sigla ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]

    if sector:
        where.append("i.sector ILIKE %s")
        params.append(f"%{sector}%")

    if region:
        where.append("i.region ILIKE %s")
        params.append(f"%{region}%")

    if con_activas:
        where.append("activas > 0")

    where_sql = " AND ".join(where)
    offset = (pagina - 1) * por_pagina

    sql = f"""
        SELECT
            i.id,
            i.nombre,
            i.sigla,
            i.sector,
            i.region,
            i.url_empleo,
            i.plataforma_empleo,
            COUNT(o.id) FILTER (WHERE o.estado = 'activo') AS activas
        FROM instituciones i
        LEFT JOIN ofertas o ON o.institucion_id = i.id
        GROUP BY i.id, i.nombre, i.sigla, i.sector, i.region, i.url_empleo, i.plataforma_empleo
        HAVING {where_sql}
        ORDER BY activas DESC, i.nombre ASC
        LIMIT %s OFFSET %s
    """

    sql_count = f"""
        SELECT COUNT(*) AS total FROM (
            SELECT i.id,
                COUNT(o.id) FILTER (WHERE o.estado = 'activo') AS activas
            FROM instituciones i
            LEFT JOIN ofertas o ON o.institucion_id = i.id
            GROUP BY i.id
            HAVING {where_sql}
        ) sub
    """

    rows    = query(sql,       params + [por_pagina, offset])
    total_r = query(sql_count, params, one=True)
    total   = total_r["total"] if total_r else 0

    return {
        "total":          total,
        "pagina":         pagina,
        "por_pagina":     por_pagina,
        "instituciones":  [dict(r) for r in rows],
    }

# ─── ENDPOINT EXTRA: HISTORIAL (OFERTAS CERRADAS) ─────────────────────────────

@app.get(
    "/api/historial",
    response_model=dict,
    summary="Ofertas cerradas para estadísticas e histórico",
    tags=["Estadísticas"],
)
def get_historial(
    institucion_id: Optional[int] = Query(None),
    sector:         Optional[str] = Query(None),
    region:         Optional[str] = Query(None),
    tipo:           Optional[str] = Query(None),
    desde:          Optional[date] = Query(None),
    hasta:          Optional[date] = Query(None),
    pagina:         int = Query(1, ge=1),
    por_pagina:     int = Query(50, ge=1, le=200),
):
    """
    Retorna ofertas ya cerradas. Esto es el histórico acumulado desde que
    el scraper comenzó a operar. Permite construir estadísticas como:
    - ¿Qué cargos publica más el SII?
    - ¿En qué meses contrata más la Municipalidad de Santiago?
    - ¿Cuál es la renta promedio de un psicólogo en el sector público?
    """
    where = ["o.estado IN ('cerrado', 'vencido')"]
    params = []

    if institucion_id:
        where.append("o.institucion_id = %s")
        params.append(institucion_id)

    if sector:
        where.append("i.sector ILIKE %s")
        params.append(f"%{sector}%")

    if region:
        where.append("o.region ILIKE %s")
        params.append(f"%{region}%")

    if tipo:
        where.append("o.tipo_contrato ILIKE %s")
        params.append(f"%{tipo}%")

    if desde:
        where.append("o.fecha_scraped >= %s")
        params.append(desde)

    if hasta:
        where.append("o.fecha_scraped <= %s")
        params.append(hasta)

    where_sql = " AND ".join(where)
    offset = (pagina - 1) * por_pagina

    sql = f"""
        SELECT
            o.id,
            i.nombre        AS institucion,
            i.sector,
            o.cargo,
            o.tipo_contrato,
            o.region,
            o.renta_bruta_min,
            o.renta_bruta_max,
            o.grado_eus,
            o.fecha_scraped,
            o.fecha_cierre,
            o.estado
        FROM ofertas o
        JOIN instituciones i ON o.institucion_id = i.id
        WHERE {where_sql}
        ORDER BY o.fecha_scraped DESC
        LIMIT %s OFFSET %s
    """

    sql_count = f"""
        SELECT COUNT(*) AS total
        FROM ofertas o
        JOIN instituciones i ON o.institucion_id = i.id
        WHERE {where_sql}
    """

    rows    = query(sql,       params + [por_pagina, offset])
    total_r = query(sql_count, params, one=True)
    total   = total_r["total"] if total_r else 0

    return {
        "total":     total,
        "pagina":    pagina,
        "por_pagina":por_pagina,
        "historial": [dict(r) for r in rows],
    }

# ─── HEALTH CHECK ─────────────────────────────────────────────────────────────

@app.get("/health", include_in_schema=False)
def health():
    """Railway y otros servicios usan este endpoint para verificar que la API está viva."""
    try:
        result = query("SELECT NOW() AS ts", one=True)
        return {"status": "ok", "db": str(result["ts"])}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(e)})

@app.get("/", include_in_schema=False)
def root():
    return {
        "nombre": "contrata o planta .cl — API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "GET /api/ofertas",
            "GET /api/ofertas/{id}",
            "GET /api/estadisticas",
            "GET /api/instituciones",
            "GET /api/historial",
        ]
    }
