"""
Pytest session setup.

`db.database` creates a SQLAlchemy engine at import time using
`config.DATABASE_URL`. The engine is lazy (no network connection until a query
runs), but the URL must be a valid Postgres DSN or `create_engine` rejects the
pool options. Setting a fake DSN here lets us import pure helpers without a
running database.
"""

import os

os.environ.setdefault(
    "DATABASE_URL", "postgresql://test:test@localhost:5432/test_empleospublicos"
)
