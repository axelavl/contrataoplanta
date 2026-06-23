"""Dedup difuso entre portales (`dedup_hash`) y renta multi-región (`renta_regional`)

Plan Parte 1.8 y 1.5. Dos columnas aditivas y nullable en `ofertas`:

- `dedup_hash VARCHAR(40)`: clave difusa estable (institución + cargo + fecha de
  cierre + región, plegados) que permite detectar la MISMA oferta llegada desde
  portales distintos. No reemplaza a `url_hash` (match exacto por URL): es una
  segunda llave de fusión. La calcula `db.database.clave_dedup_difusa`.

- `renta_regional JSONB`: tabla de renta reconstruida por región
  ([{region, sin_bono, con_bono, notas}, ...]) cuando el texto trae una tabla
  aplanada. La produce `extraction.renta_regional.parse_renta_regional`; queda
  NULL cuando no es parseable con confianza (se conserva `renta_texto`).

Ambas son aditivas: aplicar sobre producción no afecta filas ni código previo.
Idempotente vía `ADD COLUMN IF NOT EXISTS`.

Revision ID: 20260622_0001_dedup_renta
Revises: 20260618_0001_oferta_contacto
Create Date: 2026-06-22
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260622_0001_dedup_renta"
down_revision: Union[str, None] = "20260618_0001_oferta_contacto"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS dedup_hash VARCHAR(40)")
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_regional JSONB")
    # Índice parcial para la búsqueda de gemelas entre portales.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_ofertas_dedup_hash "
        "ON ofertas (dedup_hash) WHERE dedup_hash IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_ofertas_dedup_hash")
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS renta_regional")
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS dedup_hash")
