"""baseline: schema inicial + migraciones SQL manuales 001..005

El schema real (tablas, índices, triggers, constraints) vive en:

    db/schema.sql                          # estado completo
    db/migrations/001..005_*.sql           # migraciones previas a Alembic

Esta migración Alembic es **intencionalmente vacía**. Representa el
punto donde el repo empieza a usar Alembic para cambios nuevos.

Flujo para una DB nueva:

    psql "$DATABASE_URL" -f db/schema.sql
    for f in db/migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
    alembic stamp head

Flujo para una DB existente (ya con schema aplicado):

    alembic stamp head

En ambos casos, `alembic current` debería devolver este revision.
A partir de aquí, cualquier cambio de schema se hace con
`alembic revision -m "descripcion"` + `alembic upgrade head`.

Revision ID: 20260420_0000_baseline
Revises:
Create Date: 2026-04-20
"""
from __future__ import annotations

from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "20260420_0000_baseline"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # No-op: el schema viene de db/schema.sql + db/migrations/*.sql.
    # Ver docstring arriba.
    pass


def downgrade() -> None:
    # No-op: no desarmamos el schema — para eso drop/recrear la DB.
    pass
