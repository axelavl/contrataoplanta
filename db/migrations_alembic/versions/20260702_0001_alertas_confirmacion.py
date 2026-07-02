"""Doble opt-in en alertas_suscripciones (confirmación por email)

El endpoint público POST /api/alertas creaba suscripciones con activa=TRUE
sin verificar que el correo pertenezca a quien lo envía — cualquiera podía
suscribir direcciones ajenas (spam/abuso). Ahora las suscripciones nuevas
nacen con activa=FALSE + token de verificación y sólo se activan cuando el
dueño del correo abre el enlace (GET /api/alertas/confirmar).

Backfill: las filas existentes se marcan confirmada=TRUE — se crearon bajo
el régimen anterior y cortarles las alertas retroactivamente sería peor que
el riesgo residual. El opt-in aplica sólo hacia adelante.

Revision ID: 20260702_0001_alertas_confirmacion
Revises: 20260701_0001_composite_indexes_performance
Create Date: 2026-07-02
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260702_0001_alertas_confirmacion"
down_revision: Union[str, None] = "20260701_0001_composite_indexes_performance"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE alertas_suscripciones "
        "ADD COLUMN IF NOT EXISTS confirmada BOOLEAN NOT NULL DEFAULT FALSE"
    )
    op.execute(
        "ALTER TABLE alertas_suscripciones "
        "ADD COLUMN IF NOT EXISTS token_verificacion VARCHAR(64)"
    )
    op.execute(
        "ALTER TABLE alertas_suscripciones "
        "ADD COLUMN IF NOT EXISTS token_creado_en TIMESTAMPTZ"
    )
    # Suscriptores pre-existentes: confirmados de oficio (ver docstring).
    op.execute("UPDATE alertas_suscripciones SET confirmada = TRUE")
    # Lookup del enlace de confirmación. Parcial: sólo filas pendientes.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_alertas_token_verificacion "
        "ON alertas_suscripciones (token_verificacion) "
        "WHERE token_verificacion IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_alertas_token_verificacion")
    op.execute("ALTER TABLE alertas_suscripciones DROP COLUMN IF EXISTS token_creado_en")
    op.execute("ALTER TABLE alertas_suscripciones DROP COLUMN IF EXISTS token_verificacion")
    op.execute("ALTER TABLE alertas_suscripciones DROP COLUMN IF EXISTS confirmada")
