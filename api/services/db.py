"""Capa fina sobre psycopg2 + pool de conexiones.

Se extrajo de `api/main.py` para que los routers en `api/routers/*` y
otros helpers (`services/seo.py` entre otros) puedan consumir la DB
sin crear una dependencia circular con el entry point.

Comportamiento preservado tal cual estaba en `main.py`:

- `get_connection()` prefiere sacar del pool (`db.pool`); si no hay
  pool o falla, cae al `psycopg2.connect()` directo o al fallback
  `pg8000`. Levanta `HTTPException(503)` en error.
- `get_cursor()` context manager que devuelve `(conn, cursor)` con
  `RealDictCursor` de psycopg2 o el wrapper equivalente de pg8000.
- `execute_fetch_all` / `execute_fetch_one` envuelven el patrón común.
- `_table_columns` cacheado por proceso para queries resilientes al
  drift de schema en producción.
- `ensure_api_schema()` se conserva aunque ya no se invoca en startup
  (ver `docs/MIGRATIONS.md`) — sigue siendo útil one-shot.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any

from fastapi import HTTPException

from db.config import DB_CONFIG
from db import pool as db_pool

try:
    import psycopg2
    import psycopg2.extras

    _PG_DRIVER = "psycopg2"
except ImportError:  # pragma: no cover
    import pg8000.dbapi as _pg8000  # type: ignore[import]

    _PG_DRIVER = "pg8000"
    psycopg2 = None  # type: ignore[assignment]

logger = logging.getLogger("api.services.db")


class _DictCursorWrapper:
    """Thin wrapper que hace que los cursors de pg8000 se comporten
    como `psycopg2.extras.RealDictCursor`."""

    def __init__(self, cursor: Any) -> None:
        self._cur = cursor

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount

    def execute(self, sql: str, params: Any = None) -> None:
        self._cur.execute(sql, params or [])

    def _cols(self) -> list[str]:
        return [d[0] for d in (self._cur.description or [])]

    def fetchall(self) -> list[dict[str, Any]]:
        cols = self._cols()
        return [dict(zip(cols, row)) for row in (self._cur.fetchall() or [])]

    def fetchone(self) -> dict[str, Any] | None:
        row = self._cur.fetchone()
        if row is None:
            return None
        return dict(zip(self._cols(), row))

    def __enter__(self) -> "_DictCursorWrapper":
        return self

    def __exit__(self, *_: Any) -> None:
        self._cur.close()


def get_connection() -> Any:
    """Abre una conexión para una request.

    Preferencia: sacarla del pool (`db.pool`, psycopg2 threaded).
    Fallback: `psycopg2.connect()` directo si el pool aún no está
    inicializado (import temprano) o `pg8000.connect()` si psycopg2
    no está disponible en el entorno.
    """
    try:
        if _PG_DRIVER == "psycopg2":
            pool = db_pool.get_pool()
            if pool is not None:
                return pool.getconn()
            return psycopg2.connect(**DB_CONFIG)
        else:
            return _pg8000.connect(
                host=DB_CONFIG["host"],
                port=int(DB_CONFIG["port"]),
                database=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
            )
    except Exception as exc:  # pragma: no cover
        logger.exception("No se pudo abrir la conexion a PostgreSQL: %s", exc)
        raise HTTPException(status_code=503, detail="Base de datos no disponible") from exc


def _release_connection(connection: Any) -> None:
    """Devuelve la conexión al pool o la cierra si vino del fallback."""
    if connection is None:
        return
    if _PG_DRIVER == "psycopg2" and db_pool.get_pool() is not None:
        db_pool.return_connection(connection)
        return
    try:
        connection.close()
    except Exception:
        pass


@contextmanager
def get_cursor():
    connection = get_connection()
    try:
        if _PG_DRIVER == "psycopg2":
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                yield connection, cursor
        else:
            cursor = _DictCursorWrapper(connection.cursor())
            yield connection, cursor
    finally:
        _release_connection(connection)


def execute_fetch_all(
    sql: str, params: list[Any] | tuple[Any, ...] | None = None
) -> list[dict[str, Any]]:
    with get_cursor() as (_, cursor):
        cursor.execute(sql, params or [])
        return [dict(row) for row in cursor.fetchall()]


def execute_fetch_one(
    sql: str, params: list[Any] | tuple[Any, ...] | None = None
) -> dict[str, Any] | None:
    with get_cursor() as (_, cursor):
        cursor.execute(sql, params or [])
        row = cursor.fetchone()
        return dict(row) if row else None


# ── Schema drift helpers ──────────────────────────────────────────────────

_table_columns_cache: dict[str, set[str]] = {}


def _table_columns(table: str) -> set[str]:
    """Devuelve el set de columnas de una tabla (cacheado por proceso).

    Se usa para construir queries resilientes cuando el schema de prod
    no coincide exactamente con el del repo (renombres, columnas
    opcionales).
    """
    if table not in _table_columns_cache:
        rows = execute_fetch_all(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s",
            [table],
        )
        _table_columns_cache[table] = {r["column_name"] for r in rows}
    return _table_columns_cache[table]


def _coalesce_present(
    cols: set[str], candidates: tuple[str, ...], default: str | None = None
) -> str:
    """Genera una expresión SQL con las columnas candidatas presentes.

    Si ninguna existe, retorna ``default`` (ej. ``"NULL"`` o ``"0"``).
    """
    present = [c for c in candidates if c in cols]
    if not present:
        return default if default is not None else "NULL"
    if len(present) == 1 and default is None:
        return present[0]
    parts = present + ([default] if default is not None else [])
    return f"COALESCE({', '.join(parts)})"
