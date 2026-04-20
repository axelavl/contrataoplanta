"""
Pool de conexiones Postgres para la API (proceso de larga vida).

Antes `api/main.py:get_connection` hacía `psycopg2.connect(**DB_CONFIG)`
por cada request — un 3-way handshake TCP + TLS + autenticación por
cada llamada. Bajo carga (>10 req/s) se nota tanto en latencia como en
presión sobre Postgres (cada conexión ~10MB RAM server-side, contador
de `max_connections` saturable).

Este módulo expone un `ThreadedConnectionPool` de psycopg2 que mantiene
entre `POOL_MIN` y `POOL_MAX` conexiones abiertas y reutilizables. El
pool es thread-safe — FastAPI ejecuta handlers sync en un threadpool,
así que cada thread llama `getconn()` al entrar a `get_cursor()` y
`putconn()` al salir.

No lo usan los scrapers (son procesos cortos y ya tienen su propio pool
en `scrapers/base.py`). Tampoco `db/database.py` (SQLAlchemy maneja su
propio pool con `create_engine(pool_size=5)`).

Configuración por env:

    API_DB_POOL_MIN    por defecto 2
    API_DB_POOL_MAX    por defecto 10

Sizing: POOL_MAX debería ser >= al número esperado de requests
concurrentes. FastAPI corre handlers sync en un threadpool de 40
(default). Si se exceden POOL_MAX, psycopg2 lanza `PoolError` y la
request termina con 503 (no queda colgada). Subir `POOL_MAX` sólo si
el plan de Postgres soporta más conexiones concurrentes.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from db.config import get_database_config

logger = logging.getLogger(__name__)

POOL_MIN = int(os.getenv("API_DB_POOL_MIN", "2"))
POOL_MAX = int(os.getenv("API_DB_POOL_MAX", "10"))

_pool: Any = None


def init_pool() -> Any:
    """Crea el pool psycopg2 si aún no existe. Idempotente.

    Devuelve `None` (sin romper) si psycopg2 no está disponible en el
    entorno — el caller puede caer a `psycopg2.connect()` directo o al
    fallback pg8000 de la API. Tras el primer `init_pool()` exitoso, las
    llamadas siguientes devuelven el mismo pool sin efectos.
    """
    global _pool
    if _pool is not None:
        return _pool
    try:
        from psycopg2 import pool as psycopg2_pool
    except ImportError:
        logger.warning("psycopg2 no disponible — la API correrá sin pool de DB")
        return None

    kwargs = get_database_config().to_psycopg2_kwargs()
    # psycopg2 acepta port como string o int; el dict de db.config lo
    # entrega como string para compat con pg8000. Aquí no importa.
    _pool = psycopg2_pool.ThreadedConnectionPool(POOL_MIN, POOL_MAX, **kwargs)
    logger.info(
        "DB pool inicializado: min=%d max=%d host=%s",
        POOL_MIN, POOL_MAX, kwargs.get("host"),
    )
    return _pool


def get_pool() -> Any:
    """Devuelve el pool existente o `None` si no se ha inicializado."""
    return _pool


def close_pool() -> None:
    """Cierra todas las conexiones del pool. Llamar en el shutdown."""
    global _pool
    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            logger.exception("Error cerrando el pool de DB")
        finally:
            _pool = None
            logger.info("DB pool cerrado")


def return_connection(conn: Any) -> None:
    """Devuelve una conexión al pool, o la cierra si no hay pool.

    Hace rollback antes de devolver — si la request abortó con
    excepción, el thread queda con la transacción en estado `aborted`
    y la próxima request que la reciba del pool fallaría. El rollback
    es idempotente y barato si la tx ya está limpia.
    """
    if conn is None:
        return
    p = _pool
    if p is None:
        try:
            conn.close()
        except Exception:
            pass
        return
    try:
        if hasattr(conn, "closed") and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        p.putconn(conn)
    except Exception:
        # Si putconn falla (ej: conn fue cerrada externamente), al menos
        # asegura el close para que el contador server-side baje.
        logger.exception("Error devolviendo conexión al pool; cerrando")
        try:
            conn.close()
        except Exception:
            pass
