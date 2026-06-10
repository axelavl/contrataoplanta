"""
Entry-point de Alembic para contrataoplanta.

Reutiliza `db.config.get_database_config()` (la única fuente de verdad
de cómo conectarse a Postgres) para que las migraciones corran contra
la misma DB que la API y los scrapers. Sin `sqlalchemy.url` hardcoded
en `alembic.ini`.

`target_metadata` apunta al `Base.metadata` de `db/database.py` — esto
deja las puertas abiertas para `alembic revision --autogenerate` en el
futuro. Por ahora la migración baseline está vacía (el schema real
viene de `db/schema.sql` + `db/migrations/001..005.sql`) y las nuevas
se escriben a mano con `op.execute(...)`.
"""
from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

# Aseguramos que el root del proyecto esté en sys.path para que
# `import db.config` funcione cuando Alembic corre como CLI.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from alembic import context
from sqlalchemy import engine_from_config, pool

from db.config import get_database_config

config = context.config

# Setea la URL dinámicamente desde las env vars.
config.set_main_option(
    "sqlalchemy.url", get_database_config().to_sqlalchemy_url()
)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# `target_metadata` opcional pero útil si se activa autogenerate.
try:
    from db.database import Base  # type: ignore

    target_metadata = Base.metadata
except Exception:
    target_metadata = None


def run_migrations_offline() -> None:
    """Genera SQL sin conectarse a la DB (útil para revisar diffs)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Conecta y ejecuta migraciones (el caso normal)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
