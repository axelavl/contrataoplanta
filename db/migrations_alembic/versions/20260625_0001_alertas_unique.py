"""Unique index en alertas_suscripciones para evitar duplicados por race condition

La combinación (email, region, termino, tipo_contrato, sector) debe ser única.
El endpoint POST /api/alertas usaba UPDATE-then-INSERT sin protección contra
requests concurrentes; dos requests simultáneas podían ambas ver rowcount=0 e
insertar filas duplicadas. Este índice único permite usar INSERT ... ON CONFLICT.

Antes de crear el índice, se eliminan duplicados existentes conservando la fila
más reciente de cada grupo.

Revision ID: 20260625_0001_alertas_uniq
Revises: 20260624_0002_snap_renta_tipo
Create Date: 2026-06-25
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260625_0001_alertas_uniq"
down_revision: Union[str, None] = "20260624_0002_snap_renta_tipo"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        DELETE FROM alertas_suscripciones a
        USING alertas_suscripciones b
        WHERE a.id < b.id
          AND LOWER(a.email) = LOWER(b.email)
          AND COALESCE(a.region, '')        = COALESCE(b.region, '')
          AND COALESCE(a.termino, '')       = COALESCE(b.termino, '')
          AND COALESCE(a.tipo_contrato, '') = COALESCE(b.tipo_contrato, '')
          AND COALESCE(a.sector, '')        = COALESCE(b.sector, '')
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_alertas_suscripcion
        ON alertas_suscripciones (
            LOWER(email),
            COALESCE(region, ''),
            COALESCE(termino, ''),
            COALESCE(tipo_contrato, ''),
            COALESCE(sector, '')
        )
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_alertas_suscripcion")
