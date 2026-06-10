"""
EmpleoEstado.cl — Configuración central (no-DB)

La configuración de base de datos vive en `db/config.py` como única
fuente de verdad. Este módulo expone el resto de la config del
proyecto (scraping, email, Meilisearch, Umami, logging) y re-exporta
el DSN SQLAlchemy derivado para que `db/database.py` y
`run_scrapers.py` sigan funcionando sin cambios.
"""

import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv opcional


@dataclass
class Config:
    # ── Base de datos ──
    # Se deriva de `db.config.get_database_config()` para reutilizar la
    # misma precedencia (DATABASE_URL > split DB_*) y no duplicar la
    # validación de credenciales. Lazy: `field(default_factory=...)`
    # evita ejecutar la llamada al importar este módulo en contextos
    # que no necesitan la DB (ej: tests de helpers puros).
    DB_URL: str = field(default_factory=lambda: _sqlalchemy_url_from_env())

    # ── Scraping ──
    DELAY_ENTRE_REQUESTS: float = float(os.getenv("DELAY_REQUESTS", "1.5"))
    TIMEOUT_REQUEST: int = int(os.getenv("TIMEOUT_REQUEST", "20"))
    MAX_REINTENTOS: int = int(os.getenv("MAX_REINTENTOS", "3"))
    MAX_PAGINAS: int = int(os.getenv("MAX_PAGINAS", "999"))

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    ]

    # ── Email (alertas vía Resend) ──
    EMAIL_PROVIDER: str = os.getenv("EMAIL_PROVIDER", "resend")
    EMAIL_API_KEY: str  = os.getenv("EMAIL_API_KEY", "")
    RESEND_API_KEY: str = os.getenv("RESEND_API_KEY", os.getenv("EMAIL_API_KEY", ""))
    EMAIL_FROM: str     = os.getenv("EMAIL_FROM", "alertas@estadoemplea.pages.dev")

    # ── Meilisearch ──
    MEILISEARCH_URL: str     = os.getenv("MEILISEARCH_URL", "http://localhost:7700")
    MEILISEARCH_API_KEY: str = os.getenv("MEILISEARCH_API_KEY", "")

    # ── Umami Analytics ──
    UMAMI_SCRIPT_URL: str = os.getenv("UMAMI_SCRIPT_URL", "")
    UMAMI_WEBSITE_ID: str = os.getenv("UMAMI_WEBSITE_ID", "")

    # ── Logging ──
    LOG_DIR: str  = os.getenv("LOG_DIR", "logs")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


def _sqlalchemy_url_from_env() -> str:
    """Import tardío de `db.config` para evitar que un import cíclico
    durante `from config import ...` rompa cuando `db/` todavía no es
    resolvible en `sys.path`."""
    from db.config import get_database_config
    return get_database_config().to_sqlalchemy_url()


config = Config()
