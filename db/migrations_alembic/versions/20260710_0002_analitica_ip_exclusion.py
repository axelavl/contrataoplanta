"""Columna excluida en web_eventos para filtrar IPs propias de las estadísticas

Permite marcar eventos de IPs del administrador para excluirlos del conteo
de visitas reales. Las IPs excluidas se configuran en site_config
(clave 'ips_excluidas', CSV). El beacon marca excluida=TRUE al registrar.

Revision ID: 20260710_0002_analitica_ip_exclusion
Revises: 20260710_0001_url_oferta_cargo_unique
Create Date: 2026-07-10
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260710_0002_analitica_ip_exclusion"
down_revision: Union[str, None] = "20260710_0001_url_oferta_cargo_unique"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE web_eventos "
        "ADD COLUMN IF NOT EXISTS excluida BOOLEAN NOT NULL DEFAULT FALSE"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_web_eventos_excluida "
        "ON web_eventos (excluida) WHERE excluida"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_web_eventos_excluida")
    op.execute("ALTER TABLE web_eventos DROP COLUMN IF EXISTS excluida")
