"""web_eventos: analítica interna de tráfico del sitio (sin PII)

Tabla liviana que alimenta la pestaña «Estadísticas del sitio» del panel
de gestión. El frontend (`web/analytics.js`) envía un beacon a
`POST /api/track` en cada vista de página y en eventos clave (clic en
«Postular», «Ver bases», suscripción a alerta, etc.). El backend agrega
esos eventos para mostrar visitas, páginas más vistas, referidos y
dispositivos — todo sin almacenar IP ni datos personales.

`sesion` es un hash anónimo que rota a diario (no identifica a la persona;
solo permite contar «visitantes únicos» aproximados por día). `referrer_host`
guarda solo el dominio de origen, nunca la URL completa.

Aditivo e idempotente (`CREATE TABLE IF NOT EXISTS`). El insert desde la
API es best-effort: si esta migración no se aplicó, el endpoint de tracking
sigue respondiendo 200 y solo deja un warning en el log.

Revision ID: 20260626_0002_web_analytics
Revises: 20260626_0001_oferta_modalidad_horas
Create Date: 2026-06-26
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260626_0002_web_analytics"
down_revision: Union[str, None] = "20260626_0001_oferta_modalidad_horas"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS web_eventos (
            id            BIGSERIAL PRIMARY KEY,
            ts            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            tipo          VARCHAR(20) NOT NULL DEFAULT 'pageview',
            path          VARCHAR(300),
            evento        VARCHAR(80),
            oferta_id     INTEGER,
            referrer_host VARCHAR(200),
            dispositivo   VARCHAR(20),
            sesion        VARCHAR(64)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_web_eventos_ts ON web_eventos (ts DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_web_eventos_tipo ON web_eventos (tipo)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_web_eventos_path ON web_eventos (path)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_web_eventos_evento ON web_eventos (evento) "
        "WHERE evento IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS web_eventos")
