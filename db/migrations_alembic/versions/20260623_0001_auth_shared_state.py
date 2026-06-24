"""estado de auth compartido entre workers (rate limit + denylist de jti)

El rate limit de /auth/login y la denylist de JWT revocados vivían en
dicts en memoria de cada proceso (`api/deps.py`). Con la API corriendo
en >1 worker de uvicorn (el systemd usa `--workers 2`) eso fragmentaba
el estado:

  - El límite de 5 intentos fallidos por ventana era *por worker*, así
    que en la práctica eran ~N×5 para fuerza bruta del ADMIN_PASSWORD.
  - Un jti revocado en /logout seguía siendo válido en los otros
    workers hasta que el token expirara (hasta 12h).

Esta migración crea las dos tablas que `api/deps.py` usa ahora como
fuente de verdad compartida. Ambas se purgan perezosamente desde el
código (DELETE de filas vencidas en cada chequeo/escritura), así que no
requieren un job de limpieza aparte.

`downgrade()` elimina las tablas: bajar esta migración devuelve el
comportamiento en memoria (el código tiene fallback), sólo se pierde el
estado compartido, que es efímero por diseño.

Revision ID: 20260623_0001_auth_shared_state
Revises: 20260622_0005_cursos_cat
Create Date: 2026-06-23
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260623_0001_auth_shared_state"
down_revision: Union[str, None] = "20260622_0005_cursos_cat"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_UPGRADE: tuple[str, ...] = (
    # Intentos de login fallidos por IP (ventana deslizante de 10 min).
    # Sin PK artificial: cada fallo es una fila; se cuentan dentro de la
    # ventana y se borran los vencidos. Índice por (ip, ts) para que el
    # COUNT filtrado sea rápido.
    """
    CREATE TABLE IF NOT EXISTS admin_auth_failures (
        ip  TEXT        NOT NULL,
        ts  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_admin_auth_failures_ip_ts "
    "ON admin_auth_failures (ip, ts)",
    # Denylist de JWT revocados. `exp_ts` es el `exp` del token: pasado ese
    # instante PyJWT ya lo rechaza por su cuenta, así que las filas vencidas
    # son borrables sin riesgo.
    """
    CREATE TABLE IF NOT EXISTS admin_jwt_denylist (
        jti     TEXT        PRIMARY KEY,
        exp_ts  TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_admin_jwt_denylist_exp "
    "ON admin_jwt_denylist (exp_ts)",
)

_DOWNGRADE: tuple[str, ...] = (
    "DROP TABLE IF EXISTS admin_jwt_denylist",
    "DROP TABLE IF EXISTS admin_auth_failures",
)


def upgrade() -> None:
    for stmt in _UPGRADE:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in _DOWNGRADE:
        op.execute(stmt)
