"""Reemplaza UNIQUE(url_oferta) por UNIQUE(url_oferta, cargo)

Un mismo PDF de bases de concurso puede contener varios cargos distintos,
todos con la misma url_oferta. La constraint original impedía crear estas
ofertas legítimas. La nueva constraint compuesta permite misma URL con
distinto cargo, pero sigue bloqueando duplicados exactos (misma URL + mismo
cargo).

Revision ID: 20260710_0001_url_oferta_cargo_unique
Revises: 20260701_0003_public_rate_limit
Create Date: 2026-07-10
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260710_0001_url_oferta_cargo_unique"
down_revision: Union[str, None] = "20260701_0003_public_rate_limit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ofertas DROP CONSTRAINT IF EXISTS ofertas_url_oferta_key")
    op.execute("DROP INDEX IF EXISTS uq_ofertas_url_oferta")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_ofertas_url_oferta_cargo "
        "ON ofertas (url_oferta, cargo) WHERE url_oferta IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_ofertas_url_oferta_cargo")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_ofertas_url_oferta "
        "ON ofertas (url_oferta)"
    )
