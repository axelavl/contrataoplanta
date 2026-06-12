"""source_overrides en DB + email_eventos (webhooks Resend)

1. `source_overrides`: reemplaza a `scrapers/source_overrides.json` como
   fuente de verdad de los overrides de clasificación. El archivo JSON
   vivía en el filesystem de Railway y se perdía en cada redeploy, por
   lo que "desactivar fuente" desde el panel admin no persistía. Además
   el panel escribía un formato (dict por id) que `_load_overrides()`
   ni siquiera leía (espera lista) — bug silencioso desde el PR del
   panel. `scrapers/source_status.py` ahora lee primero la tabla y cae
   al JSON sólo si la DB no está disponible.

2. `email_eventos`: almacena los webhooks de Resend (delivered, bounced,
   opened, clicked, complained) para mostrar métricas de entrega en el
   tab Alertas del panel. El endpoint público `/api/webhooks/resend`
   verifica la firma svix con `RESEND_WEBHOOK_SECRET` antes de insertar.

Idempotente: correr `alembic upgrade head` sobre una DB ya migrada es
no-op.

Revision ID: 20260612_0004_overrides_email
Revises: 20260612_0003_admin_audit
Create Date: 2026-06-12

Nota: el ID de revisión se mantiene corto (<=32 chars) porque
`alembic_version.version_num` es VARCHAR(32).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260612_0004_overrides_email"
down_revision: Union[str, None] = "20260612_0003_admin_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS source_overrides (
            institucion_id INTEGER PRIMARY KEY,
            status         VARCHAR(30),
            kind           VARCHAR(40),
            reason         TEXT,
            notes          TEXT,
            frequency_tier VARCHAR(20),
            actualizado_en TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            actualizado_por VARCHAR(60)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS email_eventos (
            id        BIGSERIAL PRIMARY KEY,
            ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            evento    VARCHAR(40) NOT NULL,
            email     VARCHAR(320),
            resend_id VARCHAR(120),
            asunto    TEXT,
            payload   JSONB
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_email_eventos_ts ON email_eventos (ts DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_email_eventos_email ON email_eventos (email)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_email_eventos_evento ON email_eventos (evento)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS email_eventos")
    op.execute("DROP TABLE IF EXISTS source_overrides")
