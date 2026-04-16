"""
EmpleoEstado.cl — Configuración central
Edita las variables de entorno en un archivo .env en la raíz del proyecto.
"""

import os
from dataclasses import dataclass

# ── Intenta cargar .env si existe ──
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv opcional


@dataclass
class Config:
    # ── Base de datos ──
    DB_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:axel1234@localhost:5432/empleospublicos"
    )

    # ── Scraping ──
    DELAY_ENTRE_REQUESTS: float = float(os.getenv("DELAY_REQUESTS", "1.5"))  # segundos
    TIMEOUT_REQUEST: int = int(os.getenv("TIMEOUT_REQUEST", "20"))
    MAX_REINTENTOS: int = int(os.getenv("MAX_REINTENTOS", "3"))
    MAX_PAGINAS: int = int(os.getenv("MAX_PAGINAS", "999"))  # límite de seguridad

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
    EMAIL_FROM: str     = os.getenv("EMAIL_FROM", "alertas@contrataoplanta.cl")

    # ── Meilisearch ──
    MEILISEARCH_URL: str     = os.getenv("MEILISEARCH_URL", "http://localhost:7700")
    MEILISEARCH_API_KEY: str = os.getenv("MEILISEARCH_API_KEY", "")

    # ── Umami Analytics ──
    UMAMI_SCRIPT_URL: str = os.getenv("UMAMI_SCRIPT_URL", "")
    UMAMI_WEBSITE_ID: str = os.getenv("UMAMI_WEBSITE_ID", "")

    # ── Logging ──
    LOG_DIR: str  = os.getenv("LOG_DIR", "logs")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
