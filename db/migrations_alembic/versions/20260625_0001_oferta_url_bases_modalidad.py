"""Persistir `url_bases`, `modalidad` y `horas_semanales` en `ofertas`

El scraper de Carabineros (y a futuro otros) ahora recoge el enlace a las bases
del concurso, la modalidad contractual y la jornada en horas. Las columnas ya
estaban declaradas en `db/schema.sql` (CREATE TABLE) pero faltaba garantizarlas
en despliegues antiguos y dejarlas bajo control de Alembic.

Todo aditivo, nullable e idempotente (`ADD COLUMN IF NOT EXISTS`).

Revision ID: 20260625_0001_oferta_url_bases_modalidad
Revises: 20260624_0002_snapshot_renta_tipo
Create Date: 2026-06-25
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260625_0001_oferta_url_bases_modalidad"
down_revision: Union[str, None] = "20260624_0002_snapshot_renta_tipo"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases TEXT")
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS modalidad VARCHAR(50)")
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS horas_semanales INTEGER")


def downgrade() -> None:
    # Solo revertimos modalidad/horas: `url_bases` es anterior a esta migración
    # (existía vía schema.sql/ensure_api_schema) y la usa el frontend.
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS horas_semanales")
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS modalidad")
