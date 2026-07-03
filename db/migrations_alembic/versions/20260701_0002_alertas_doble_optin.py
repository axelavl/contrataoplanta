"""Doble opt-in en alertas_suscripciones (verificación de email)

POST /api/alertas activaba la suscripción sin comprobar que quien la crea sea
dueño del correo → cualquiera podía suscribir a un tercero y hacerle llegar
alertas no solicitadas (spam/acoso y daño a la reputación del dominio remitente).

Este cambio añade el estado de verificación:

- `verificada`         — sólo se envían alertas a suscripciones verificadas.
- `token_verificacion` — token del enlace de confirmación (un solo uso).
- `verificada_en`      — timestamp de la confirmación.

Las filas PREEXISTENTES se marcan como verificadas (`verificada = TRUE`): fueron
altas bajo el modelo anterior y no deben dejar de recibir correos por este cambio.
Sólo las altas nuevas nacen sin verificar hasta que se confirme el enlace.

Revision ID: 20260701_0002_alertas_doble_optin
Revises: 20260701_0001_composite_indexes_performance
Create Date: 2026-07-01
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260701_0002_alertas_doble_optin"
down_revision: Union[str, None] = "20260701_0001_composite_indexes_performance"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE alertas_suscripciones "
        "ADD COLUMN IF NOT EXISTS verificada BOOLEAN NOT NULL DEFAULT FALSE"
    )
    op.execute(
        "ALTER TABLE alertas_suscripciones "
        "ADD COLUMN IF NOT EXISTS token_verificacion VARCHAR(64)"
    )
    op.execute(
        "ALTER TABLE alertas_suscripciones "
        "ADD COLUMN IF NOT EXISTS verificada_en TIMESTAMPTZ"
    )
    # Filas preexistentes: se consideran verificadas para no cortar sus envíos.
    # `verificada_en IS NULL` distingue estas altas heredadas de las que se
    # confirmen tras el despliegue. Sólo aplica al backfill inicial: las nuevas
    # nacen con verificada = FALSE (el default) hasta confirmar el enlace.
    op.execute(
        "UPDATE alertas_suscripciones SET verificada = TRUE "
        "WHERE verificada = FALSE AND verificada_en IS NULL"
    )
    # Índice para resolver el enlace de confirmación por token en O(log n).
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_alertas_token "
        "ON alertas_suscripciones (token_verificacion) "
        "WHERE token_verificacion IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_alertas_token")
    op.execute("ALTER TABLE alertas_suscripciones DROP COLUMN IF EXISTS verificada_en")
    op.execute("ALTER TABLE alertas_suscripciones DROP COLUMN IF EXISTS token_verificacion")
    op.execute("ALTER TABLE alertas_suscripciones DROP COLUMN IF EXISTS verificada")
