"""admin_audit_log: registro de acciones del panel de gestión

Los endpoints admin mutadores (editar/toggle ofertas, bulk-desactivar,
CRUD de fuentes, envío de alertas, cambios de config, lanzamiento de
scrapers, etc.) registran cada acción vía `api.routers.admin._auditar`.
Antes el único rastro quedaba en logs de proceso, que en Railway se
pierden con cada redeploy.

El insert es best-effort: si esta migración no se aplicó, el endpoint
sigue funcionando y sólo queda un warning en el log.

Idempotente: correr `alembic upgrade head` sobre una DB ya migrada es
no-op. `downgrade()` elimina la tabla (se pierde el historial).

Revision ID: 20260612_0003_admin_audit
Revises: 20260611_0002_logs_sin_fk
Create Date: 2026-06-12

Nota: el ID de revisión se mantiene corto (<=32 chars) porque
`alembic_version.version_num` es VARCHAR(32).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260612_0003_admin_audit"
down_revision: Union[str, None] = "20260611_0002_logs_sin_fk"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_audit_log (
            id         BIGSERIAL PRIMARY KEY,
            ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            usuario    VARCHAR(60) NOT NULL,
            accion     VARCHAR(60) NOT NULL,
            entidad    VARCHAR(40),
            entidad_id VARCHAR(120),
            detalle    JSONB
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_audit_ts ON admin_audit_log (ts DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_audit_accion ON admin_audit_log (accion)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS admin_audit_log")
