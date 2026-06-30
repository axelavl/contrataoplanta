"""Persistir `modalidad` y `horas_semanales` en `ofertas` (+ `url_bases` idempotente)

Los scrapers (Carabineros y, a futuro, otros vía `upsert_oferta`) ahora recogen
la modalidad contractual, la jornada en horas y el enlace a las bases del
concurso. Las columnas ya estaban declaradas en `db/schema.sql` (CREATE TABLE)
pero faltaba garantizarlas bajo control de Alembic para despliegues que avanzan
con `alembic upgrade head`.

Todo aditivo, nullable e idempotente (`ADD COLUMN IF NOT EXISTS`). El check de
rango de `horas_semanales` replica el de `schema.sql` y se crea solo si no
existe.

Revision ID: 20260626_0001_oferta_modalidad_horas
Revises: 20260625_0002_ofertas_destacada
Create Date: 2026-06-26
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260626_0001_oferta_modalidad_horas"
down_revision: Union[str, None] = "20260625_0002_ofertas_destacada"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # `url_bases` es anterior (sync_api_schema), se reafirma por idempotencia.
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases TEXT")
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS modalidad VARCHAR(50)")
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS horas_semanales INTEGER")
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'chk_ofertas_horas_semanales_rango'
            ) THEN
                ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_horas_semanales_rango
                    CHECK (horas_semanales IS NULL OR horas_semanales BETWEEN 1 AND 88);
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # Solo revertimos modalidad/horas: `url_bases` es anterior a esta migración
    # (existía vía sync_api_schema) y la usa el frontend.
    op.execute("ALTER TABLE ofertas DROP CONSTRAINT IF EXISTS chk_ofertas_horas_semanales_rango")
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS horas_semanales")
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS modalidad")
