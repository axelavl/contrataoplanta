"""rate limit de endpoints públicos compartido entre workers

El límite de `/api/alertas`, `/api/alertas/confirmar` y `/api/validar-email`
vivía en un dict en memoria de cada proceso (`_check_public_rate` en
`api/routers/public.py`). Con la API en >1 worker de uvicorn (`--workers 2`) el
límite era *por worker* (≈N× el nominal) y se reiniciaba en cada deploy; además
las claves de IP nunca se purgaban (crecimiento de memoria no acotado).

Esta tabla es la fuente de verdad compartida, análoga a `admin_auth_failures`.
Cada request cuenta como una fila `(ip, bucket, ts)`; `bucket` separa el
presupuesto por familia de endpoint. Se purga perezosamente desde el código
(DELETE de filas vencidas en cada chequeo), sin job de limpieza aparte.

`downgrade()` elimina la tabla → el código cae a su fallback en memoria.

Revision ID: 20260701_0003_public_rate_limit
Revises: 20260701_0002_alertas_doble_optin
Create Date: 2026-07-01
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260701_0003_public_rate_limit"
down_revision: Union[str, None] = "20260701_0002_alertas_doble_optin"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_UPGRADE: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS public_rate_hits (
        ip      TEXT        NOT NULL,
        bucket  TEXT        NOT NULL,
        ts      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # COUNT filtrado por (bucket, ip) dentro de la ventana → índice compuesto.
    "CREATE INDEX IF NOT EXISTS idx_public_rate_hits_bucket_ip_ts "
    "ON public_rate_hits (bucket, ip, ts)",
    # Purga de vencidos por ts.
    "CREATE INDEX IF NOT EXISTS idx_public_rate_hits_ts "
    "ON public_rate_hits (ts)",
)

_DOWNGRADE: tuple[str, ...] = (
    "DROP TABLE IF EXISTS public_rate_hits",
)


def upgrade() -> None:
    for stmt in _UPGRADE:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in _DOWNGRADE:
        op.execute(stmt)
