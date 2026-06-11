"""logs_scraping sin FK a fuentes (recupera el registro de corridas)

El registro de cada corrida (`db.database.registrar_log`) inserta en
`logs_scraping`. En producción esa tabla no existía y, además, su columna
`fuente_id` tenía una FK a `fuentes(id)`. Los scrapers de nuevo estándar
(famae=165, laborum=707/279, bcentral=145, pdi=162, …) pasan el
`institucion_id` (o 0), que no tiene fila en `fuentes` (tabla vacía), por lo
que el INSERT violaba la FK. Resultado: `registrar_log` fallaba en cada
corrida y ensuciaba el log con tracebacks (relation does not exist / FK
violation), aunque el scraping y el upsert de ofertas sí ocurrían.

Esta migración:
  1. Crea `logs_scraping` si falta, con `fuente_id` como INTEGER simple
     (sin FK): es sólo un identificador de la fuente/institución que corrió,
     no una relación que debamos integrar referencialmente.
  2. Si la tabla ya existía con la FK (instalaciones que aplicaron
     db/schema.sql antiguo), elimina cualquier FK sobre la tabla.

Idempotente: correr `alembic upgrade head` sobre una DB ya migrada es no-op.
`downgrade()` es no-op a propósito (no queremos perder el historial de logs).

Revision ID: 20260611_0002_logs_sin_fk
Revises: 20260420_0001_sync_api_schema
Create Date: 2026-06-11

Nota: el ID de revisión se mantiene corto (<=32 chars) porque
`alembic_version.version_num` es VARCHAR(32).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260611_0002_logs_sin_fk"
down_revision: Union[str, None] = "20260420_0001_sync_api_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_STATEMENTS: tuple[str, ...] = (
    # 1) Crear la tabla si falta — fuente_id INTEGER simple, sin FK.
    """
    CREATE TABLE IF NOT EXISTS logs_scraping (
        id                   SERIAL PRIMARY KEY,
        fuente_id            INTEGER,
        iniciado_en          TIMESTAMPTZ DEFAULT NOW(),
        finalizado_en        TIMESTAMPTZ,
        duracion_seg         NUMERIC(8,2),
        estado               VARCHAR(20),
        ofertas_nuevas       INTEGER DEFAULT 0,
        ofertas_actualizadas INTEGER DEFAULT 0,
        ofertas_cerradas     INTEGER DEFAULT 0,
        paginas_visitadas    INTEGER DEFAULT 0,
        error_mensaje        TEXT,
        detalle              JSONB
    )
    """,
    # 2) Si la tabla ya existía con FK (schema.sql antiguo), eliminarla.
    """
    DO $$
    DECLARE c text;
    BEGIN
        FOR c IN
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'logs_scraping'::regclass
              AND contype  = 'f'
        LOOP
            EXECUTE 'ALTER TABLE logs_scraping DROP CONSTRAINT ' || quote_ident(c);
        END LOOP;
    END $$;
    """,
    # 3) Índice útil para consultar el historial por fuente/fecha.
    """
    CREATE INDEX IF NOT EXISTS idx_logs_scraping_fuente_fecha
        ON logs_scraping (fuente_id, finalizado_en DESC)
    """,
)


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    # No desarmamos: conservar el historial de corridas.
    pass
