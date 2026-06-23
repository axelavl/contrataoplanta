"""Agrega columna `tipo` a cursos (curso / diplomado / certificación / charla)

Para poder filtrar el directorio por tipo de formación. Default 'curso'; se
marcan las certificaciones de ChileCompra y el diplomado de SUBDERE.

Revision ID: 20260622_0004_cursos_tipo
Revises: 20260622_0003_cursos_refr
Create Date: 2026-06-22
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260622_0004_cursos_tipo"
down_revision: Union[str, None] = "20260622_0003_cursos_refr"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE cursos ADD COLUMN IF NOT EXISTS tipo VARCHAR(30) DEFAULT 'curso'")
    op.execute(
        "UPDATE cursos SET tipo = 'certificación' "
        "WHERE curso_id IN ('chilecompra-cert-basico','chilecompra-cert-intermedio','chilecompra-cert-avanzado')"
    )
    op.execute("UPDATE cursos SET tipo = 'diplomado' WHERE curso_id = 'subdere-municipal-regional'")


def downgrade() -> None:
    op.execute("ALTER TABLE cursos DROP COLUMN IF EXISTS tipo")
