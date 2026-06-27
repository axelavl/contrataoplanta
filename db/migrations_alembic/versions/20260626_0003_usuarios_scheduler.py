"""admin_usuarios (roles) + scheduler_state (programación de recolecciones)

Dos funciones nuevas del panel de gestión:

1. **Usuarios admin con roles** (`admin_usuarios`). Hasta ahora el panel
   tenía una sola contraseña (`ADMIN_PASSWORD`) y todas las acciones se
   registraban como el usuario lógico «ops». Esta tabla permite cuentas
   nominales con rol (`admin` / `editor` / `lector`), de modo que el
   «Historial de cambios del equipo» muestre quién hizo qué y se pueda
   limitar quién edita. La contraseña maestra sigue funcionando como
   respaldo (entra como «ops» con rol admin).

2. **Programador de recolecciones** (`scheduler_state`). Tabla singleton
   (fila id=1) que guarda si la recolección automática propia está activa,
   cada cuántas horas corre, en qué modo y cuándo toca la próxima. Un loop
   en segundo plano (api/services/scheduler.py) la consulta y dispara la
   corrida con un claim atómico para que, con varios workers de uvicorn,
   solo uno la ejecute.

Aditivo e idempotente. El código degrada con gracia si estas tablas no
existen (los endpoints responden con un aviso, igual que admin_audit).

Revision ID: 20260626_0003_usuarios_scheduler
Revises: 20260626_0002_web_analytics
Create Date: 2026-06-26
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260626_0003_usuarios_scheduler"
down_revision: Union[str, None] = "20260626_0002_web_analytics"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_usuarios (
            id            BIGSERIAL PRIMARY KEY,
            usuario       VARCHAR(60) NOT NULL,
            nombre        VARCHAR(120),
            password_hash VARCHAR(255) NOT NULL,
            rol           VARCHAR(20) NOT NULL DEFAULT 'editor',
            activo        BOOLEAN NOT NULL DEFAULT TRUE,
            creado_en     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ultimo_login  TIMESTAMPTZ
        )
        """
    )
    # Unicidad case-insensitive: el login busca con LOWER(usuario), así que la
    # restricción debe igualar esa semántica (si no, 'Ana' y 'ana' coexistirían
    # y el login podría autenticar la fila equivocada).
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_admin_usuarios_usuario_lower "
        "ON admin_usuarios (LOWER(usuario))"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduler_state (
            id                 INTEGER PRIMARY KEY DEFAULT 1,
            activo             BOOLEAN NOT NULL DEFAULT FALSE,
            intervalo_horas    INTEGER NOT NULL DEFAULT 24,
            modo               VARCHAR(40) NOT NULL DEFAULT 'completa',
            limite_fuentes     INTEGER,
            proxima_ejecucion  TIMESTAMPTZ,
            ultima_ejecucion   TIMESTAMPTZ,
            actualizado_en     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            actualizado_por    VARCHAR(60),
            CONSTRAINT chk_scheduler_singleton CHECK (id = 1)
        )
        """
    )
    # Fila singleton inicial (desactivada por defecto — no dispara nada).
    op.execute(
        "INSERT INTO scheduler_state (id, activo) VALUES (1, FALSE) "
        "ON CONFLICT (id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS scheduler_state")
    op.execute("DROP TABLE IF EXISTS admin_usuarios")
