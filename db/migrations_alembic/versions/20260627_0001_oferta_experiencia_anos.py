"""Garantizar `experiencia_anos` en `ofertas` (filtro "sin experiencia")

La columna `experiencia_anos` está declarada en `db/schema.sql` (CREATE TABLE)
pero nunca se aseguró bajo Alembic: a diferencia de `requisitos_texto` y el
resto, quedó fuera de `sync_api_schema`. En despliegues que avanzan con
`alembic upgrade head` (producción), la columna NO existe.

El filtro público "sin experiencia" (`api/services/sql.py`, `build_ofertas_filters`)
agrega la cláusula `o.experiencia_anos = 0`. Al faltar la columna, `/api/ofertas?...&sin_experiencia=true`
levantaba `UndefinedColumn` → 500; como la respuesta de error no lleva headers
CORS, el navegador lo reporta como "Failed to fetch" y la lista no carga. El
resto del sitio funciona porque ninguna otra consulta referencia la columna.

Aditivo, nullable e idempotente (`ADD COLUMN IF NOT EXISTS`).

Revision ID: 20260627_0001_oferta_experiencia_anos
Revises: 20260626_0003_usuarios_scheduler
Create Date: 2026-06-27
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260627_0001_oferta_experiencia_anos"
down_revision: Union[str, None] = "20260626_0003_usuarios_scheduler"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS experiencia_anos INTEGER")


def downgrade() -> None:
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS experiencia_anos")
