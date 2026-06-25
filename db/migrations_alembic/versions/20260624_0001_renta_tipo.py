"""Tipo de renta (`renta_tipo`): bruta / líquida / indeterminada

Estándar del proyecto: la renta informada debe ser la **bruta**. Cuando la
fuente solo publica la **líquida**, hay que dejarlo señalado. Esta columna
guarda esa clasificación de forma estructurada (además de la marca textual que
el pipeline anexa a ``renta_texto``).

- ``renta_tipo VARCHAR(20)``: 'bruta' | 'liquida' | 'indeterminada' | NULL.
  La calcula ``extraction.renta_tipo.clasificar_tipo_renta`` en la capa de
  persistencia (``db.database.upsert_oferta`` y ``scrapers.base``).

Aditiva y nullable: aplicar sobre producción no afecta filas ni código previo.
Idempotente vía ``ADD COLUMN IF NOT EXISTS``.

Revision ID: 20260624_0001_renta_tipo
Revises: 20260623_0002_cursos_salud_logo
Create Date: 2026-06-24
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260624_0001_renta_tipo"
down_revision: Union[str, None] = "20260623_0002_cursos_salud_logo"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_tipo VARCHAR(20)")


def downgrade() -> None:
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS renta_tipo")
