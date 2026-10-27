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

import re
from pathlib import Path
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260420_0000_baseline"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DB_DIR = Path(__file__).resolve().parents[2]  # .../db

# `BEGIN;` / `COMMIT;` sueltos (control de transacción de psql). Se quitan
# porque Alembic ya corre todo dentro de una transacción. Los `BEGIN` de
# bloques plpgsql (sin `;` en la misma línea) no calzan y se conservan.
_RE_TX = re.compile(r"^\s*(BEGIN|COMMIT)\s*;\s*$", re.IGNORECASE | re.MULTILINE)


def _db_vacia() -> bool:
    cur = op.get_bind().connection.dbapi_connection.cursor()
    cur.execute("SELECT to_regclass('public.ofertas') IS NULL")
    return bool(cur.fetchone()[0])


def _ejecutar_sql(ruta: Path) -> None:
    sql = _RE_TX.sub("", ruta.read_text(encoding="utf-8"))
    # Cursor DBAPI directo y sin parámetros: evita que SQLAlchemy interprete
    # `:nombre` o psycopg2 interprete `%` dentro del SQL. Comparte la misma
    # conexión/transacción de Alembic.
    cur = op.get_bind().connection.dbapi_connection.cursor()
    cur.execute(sql)


def upgrade() -> None:
    # DB existente (ya tiene schema): no-op, igual que antes.
    # DB nueva y vacía: aplica db/schema.sql + db/migrations/*.sql para que
    # `alembic upgrade head` funcione desde cero. Sin esto, la migración
    # siguiente (sync_api_schema) falla con "relation scraper_runs does not
    # exist" porque nada había creado las tablas base.
    if not _db_vacia():
        return
    _ejecutar_sql(_DB_DIR / "schema.sql")
    for ruta in sorted((_DB_DIR / "migrations").glob("*.sql")):
        _ejecutar_sql(ruta)


def downgrade() -> None:
    # No-op: no desarmamos el schema — para eso drop/recrear la DB.
    pass
