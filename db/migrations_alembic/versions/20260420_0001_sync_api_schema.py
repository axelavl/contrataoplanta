"""sync schema de la API (reemplaza ensure_api_schema de runtime)

Antes `api/main.py::ensure_api_schema()` corría ~60 sentencias DDL en
cada arranque del proceso (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE
ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, ...). Es una
forma de "auto-migración en runtime" que la auditoría marcó como
antipatrón: enmascara drift de schema, hace lento el arranque, y en
uvicorn con workers múltiples compite con sí misma.

Esta migración aplica el mismo set de DDL una sola vez y de forma
explícita. Todas las sentencias siguen siendo idempotentes (`IF NOT
EXISTS`), así que correr `alembic upgrade head` sobre una DB ya
actualizada es no-op. En DBs legadas con columnas faltantes, esta
migración cierra el gap sin perder datos.

`downgrade()` es intencionalmente un no-op: bajar este schema
perdería datos y no existe un caso de uso donde queramos hacerlo.

Revision ID: 20260420_0001_sync_api_schema
Revises: 20260420_0000_baseline
Create Date: 2026-04-20
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260420_0001_sync_api_schema"
down_revision: Union[str, None] = "20260420_0000_baseline"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_STATEMENTS: tuple[str, ...] = (
    # ── Tablas base (idempotentes) ─────────────────────────────────
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

    # ── Columnas incrementales sobre `instituciones` ───────────────
    "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS sigla VARCHAR(50)",
    "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS nombre_corto VARCHAR(80)",
    "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS sector VARCHAR(100)",
    "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS tipo VARCHAR(80)",
    "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS region VARCHAR(100)",
    "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS url_empleo TEXT",
    "ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS plataforma_empleo VARCHAR(100)",

    # ── Columnas incrementales sobre `ofertas` ─────────────────────
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
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS activa BOOLEAN DEFAULT TRUE",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_scraped TIMESTAMP DEFAULT NOW()",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_actualizado TIMESTAMP DEFAULT NOW()",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_oferta_valida BOOLEAN",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases_valida BOOLEAN",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_valida_chequeada_en TIMESTAMP",
    # Columnas usadas por el pipeline y por admin_diagnostico.
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS needs_review BOOLEAN DEFAULT FALSE",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS overall_quality_score NUMERIC",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS detectada_en TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS actualizada_en TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS creada_en TIMESTAMPTZ DEFAULT NOW()",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_ofertas_url_oferta ON ofertas (url_oferta)",

    # ── alertas_suscripciones ──────────────────────────────────────
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

    # ── scraper_runs (compat admin panel) ──────────────────────────
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ",
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ",
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS status VARCHAR(20)",
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS run_mode VARCHAR(50)",
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS total_evaluadas INTEGER DEFAULT 0",
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS total_extract INTEGER DEFAULT 0",
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS total_skip INTEGER DEFAULT 0",
    "ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS notas TEXT",
    # Rellenar started_at desde ejecutado_en solo si la col heredada existe.
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
    "UPDATE scraper_runs SET status = 'completado' "
    "WHERE status IS NULL AND duracion_segundos IS NOT NULL",

    # ── site_config editable desde admin ───────────────────────────
    """
    CREATE TABLE IF NOT EXISTS site_config (
        clave VARCHAR(100) PRIMARY KEY,
        valor TEXT,
        actualizado_en TIMESTAMP DEFAULT NOW()
    )
    """,
)


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    # Drop destructivo: no desarmamos nada acá.
    pass
