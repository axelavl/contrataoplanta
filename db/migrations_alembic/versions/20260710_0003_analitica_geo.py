"""Columnas de geolocalización en web_eventos

Almacena país (código ISO 2 letras) y ciudad del visitante, extraídos
de los headers de Cloudflare (CF-IPCountry, CF-IPCity) o Accept-Language.
Permite ver distribución geográfica de visitas en el panel de estadísticas.

Revision ID: 20260710_0003_analitica_geo
Revises: 20260710_0002_analitica_ip_exclusion
Create Date: 2026-07-10
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260710_0003_analitica_geo"
down_revision: Union[str, None] = "20260710_0002_analitica_ip_exclusion"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE web_eventos "
        "ADD COLUMN IF NOT EXISTS pais VARCHAR(2)"
    )
    op.execute(
        "ALTER TABLE web_eventos "
        "ADD COLUMN IF NOT EXISTS ciudad VARCHAR(100)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_web_eventos_pais "
        "ON web_eventos (pais) WHERE pais IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_web_eventos_pais")
    op.execute("ALTER TABLE web_eventos DROP COLUMN IF EXISTS ciudad")
    op.execute("ALTER TABLE web_eventos DROP COLUMN IF EXISTS pais")
