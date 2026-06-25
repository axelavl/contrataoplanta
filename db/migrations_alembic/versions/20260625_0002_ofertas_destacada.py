"""Ofertas destacadas para la sección "Destacadas" (redes sociales).

Agrega la columna ``ofertas.destacada`` (BOOLEAN, default FALSE). Marca de
curaduría manual: las ofertas que el equipo destaca y publica activamente en
redes sociales (Instagram, TikTok, LinkedIn). La nueva pestaña pública
"Destacadas" del frontend muestra exactamente esas ofertas, para que quien
las vio en RRSS las reencuentre fácil en el sitio.

- ``destacada BOOLEAN DEFAULT FALSE``: la enciende/apaga el panel admin
  (``POST /api/{ADMIN_PATH}/ofertas/{id}/toggle-destacada``). El endpoint
  público ``GET /api/ofertas?destacadas=true`` filtra por esta columna.

Aditiva y nullable-friendly (default FALSE): aplicar sobre producción no
afecta filas ni código previo. Idempotente vía ``ADD COLUMN IF NOT EXISTS``.

Además, esta revisión **fusiona los dos heads** que existían en la cadena
(``20260625_0001_alertas_uniq`` y ``20260625_0001_cursos_cea_enlaces``, ambos
colgando de ``20260624_0002_snap_renta_tipo``). Sin la fusión, ``alembic
upgrade head`` —el paso de deploy— fallaría por "multiple head revisions".

Revision ID: 20260625_0002_ofertas_destacada
Revises: 20260625_0001_alertas_uniq, 20260625_0001_cursos_cea_enlaces
Create Date: 2026-06-25
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260625_0002_ofertas_destacada"
down_revision: Union[str, Sequence[str], None] = (
    "20260625_0001_alertas_uniq",
    "20260625_0001_cursos_cea_enlaces",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS destacada BOOLEAN DEFAULT FALSE")
    # Índice parcial: la pestaña "Destacadas" sólo consulta el puñado de
    # ofertas marcadas, no la tabla completa.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ofertas_destacada "
        "ON ofertas(destacada) WHERE destacada = TRUE"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_ofertas_destacada")
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS destacada")
