"""
Configuración centralizada de acceso a PostgreSQL.

Única fuente de verdad de cómo se conecta a la DB cualquier capa del
proyecto. Reemplaza las tres convenciones que coexistían:

- `api/main.py` definía un `DB_CONFIG` dict leyendo `DB_HOST`, `DB_PORT`,
  `DB_NAME`, `DB_USER`, `DB_PASSWORD` (consumido por psycopg2/pg8000).
- `scrapers/base.py` tenía un `DB_CONFIG` análogo pero con `port` como
  int en vez de string.
- `config.py` leía `DATABASE_URL` (DSN completo) para SQLAlchemy y
  tenía la contraseña hardcodeada como fallback.

Ahora todas las capas llaman a `get_database_config()` y piden la
representación que necesitan: `.to_psycopg2_kwargs()` (dict para
`psycopg2.connect(**kwargs)`) o `.to_sqlalchemy_url()` (DSN para
`create_engine`).

La precedencia de env vars es:

1. `DATABASE_URL` completo (lo que provee Railway/Heroku al añadir un
   plugin de Postgres). La contraseña es obligatoria dentro del DSN.
2. Variables split `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` /
   `DB_PASSWORD` (dev local con `.env`). `DB_PASSWORD` es obligatorio;
   el resto tiene defaults para desarrollo.

Sin password en ninguno de los dos caminos, el proceso aborta al
importar este módulo — comportamiento por diseño tras la auditoría.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from urllib.parse import quote_plus, urlparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # pragma: no cover
    pass


def _requerido(nombre: str) -> str:
    """Lee una env var obligatoria o levanta `RuntimeError`.

    No usa `sys.exit` — preferimos la excepción para que quien importa
    pueda manejarla si tiene sentido (tests, CLI con flags, etc.). El
    default en producción es que uvicorn/el scraper deje caer la
    excepción y aborte el arranque con traceback claro.
    """
    valor = os.getenv(nombre)
    if not valor:
        raise RuntimeError(
            f"Variable de entorno {nombre!r} no definida. "
            f"Configúrala en Railway/entorno (ver .env.example)."
        )
    return valor


@dataclass(frozen=True)
class DatabaseConfig:
    """Estado de conexión a Postgres, inmutable por proceso."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    def to_psycopg2_kwargs(self) -> dict[str, Any]:
        """Dict consumible por `psycopg2.connect(**kwargs)`.

        `port` se devuelve como string porque es la forma que aceptan
        tanto psycopg2 como pg8000 (el fallback en `api/main.py` si
        psycopg2 no está disponible).
        """
        return {
            "host": self.host,
            "port": str(self.port),
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }

    def to_asyncpg_kwargs(self) -> dict[str, Any]:
        """Dict consumible por `asyncpg.connect(**kwargs)`.

        Por si alguien migra el API a async en el futuro. `port` aquí sí
        va como int y el nombre del parámetro es `database`, no `dbname`.
        """
        return {
            "host": self.host,
            "port": self.port,
            "database": self.dbname,
            "user": self.user,
            "password": self.password,
        }

    def to_sqlalchemy_url(self) -> str:
        """URL DSN para `sqlalchemy.create_engine(...)`.

        La contraseña se URL-encodea para que caracteres como `@` o `:`
        no rompan el parser del DSN.
        """
        pw = quote_plus(self.password)
        return (
            f"postgresql+psycopg2://{self.user}:{pw}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )


@lru_cache(maxsize=1)
def get_database_config() -> DatabaseConfig:
    """Devuelve la config de DB, construida y cacheada por proceso."""
    dsn = (os.getenv("DATABASE_URL") or "").strip()
    if dsn:
        parsed = urlparse(dsn)
        if not parsed.password:
            raise RuntimeError(
                "DATABASE_URL está definido pero no incluye contraseña. "
                "Usa el formato postgresql://user:pass@host:port/dbname."
            )
        return DatabaseConfig(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            dbname=(parsed.path or "/").lstrip("/") or "empleospublicos",
            user=parsed.username or "postgres",
            password=parsed.password,
        )

    return DatabaseConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "empleospublicos"),
        user=os.getenv("DB_USER", "postgres"),
        password=_requerido("DB_PASSWORD"),
    )


#: Dict psycopg2-ready, pre-computado al importar. Capas existentes que
#: hacen `from db.config import DB_CONFIG` lo consumen directamente sin
#: tener que instanciar la dataclass.
DB_CONFIG: dict[str, Any] = get_database_config().to_psycopg2_kwargs()
