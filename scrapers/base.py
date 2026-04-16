from __future__ import annotations

import abc
import difflib
import itertools
import json
import logging
import logging.handlers
import os
import random
import re
import tempfile
import time
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests
from requests import exceptions as requests_exceptions

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "empleospublicos"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "axel1234"),
}

USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
]

# Headers estándar que un navegador real envía en cada request.
# Su ausencia es señal fuerte para WAFs/anti-bot (Cloudflare, Sucuri, etc.).
DEFAULT_BROWSER_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

TIPO_MAP = {
    "planta": "planta",
    "contrata": "contrata",
    "honorario": "honorarios",
    "honorarios": "honorarios",
    "codigo del trabajo": "codigo_trabajo",
    "codigo trabajo": "codigo_trabajo",
    "reemplazo": "reemplazo",
}

REGION_MAP = {
    "arica": "Arica y Parinacota",
    "tarapaca": "Tarapacá",
    "antofagasta": "Antofagasta",
    "atacama": "Atacama",
    "coquimbo": "Coquimbo",
    "valparaiso": "Valparaíso",
    "metropolitana": "Metropolitana de Santiago",
    "region metropolitana": "Metropolitana de Santiago",
    "rm": "Metropolitana de Santiago",
    "ohiggins": "O'Higgins",
    "libertador general bernardo ohiggins": "O'Higgins",
    "maule": "Maule",
    "nuble": "Ñuble",
    "biobio": "Biobío",
    "bio bio": "Biobío",
    "araucania": "La Araucanía",
    "la araucania": "La Araucanía",
    "los rios": "Los Ríos",
    "los lagos": "Los Lagos",
    "aysen": "Aysén",
    "magallanes": "Magallanes",
}

DATE_PATTERNS = (
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%Y-%m-%d",
    "%d/%m/%y",
    "%d-%m-%y",
)

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)


def strip_accents(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    return normalized.encode("ascii", "ignore").decode("ascii")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def normalize_key(value: str | None) -> str:
    return re.sub(r"\s+", " ", strip_accents(clean_text(value)).lower()).strip(" :")


def truncate(value: str | None, max_len: int) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    return text[:max_len]


def parse_date(value: str | date | datetime | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = clean_text(value)
    for pattern in DATE_PATTERNS:
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue

    # Con año explícito: "20 de junio de 2026"
    match = re.search(
        r"\b(\d{1,2})\s+de\s+([a-zA-Záéíóú]+)\s+(?:de\s+)?(\d{4})\b",
        strip_accents(text).lower(),
    )
    if match:
        day = int(match.group(1))
        month = SPANISH_MONTHS.get(match.group(2))
        year = int(match.group(3))
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass

    # Sin año: "20 de junio" — inferir año actual o siguiente
    match = re.search(
        r"\b(\d{1,2})\s+de\s+([a-zA-Záéíóú]+)\b",
        strip_accents(text).lower(),
    )
    if match:
        day = int(match.group(1))
        month = SPANISH_MONTHS.get(match.group(2))
        if month:
            today = date.today()
            year = today.year
            try:
                candidate = date(year, month, day)
                # Si la fecha ya pasó este año, probablemente es el año siguiente
                if candidate < today:
                    candidate = date(year + 1, month, day)
                return candidate
            except ValueError:
                pass

    return None


def normalize_tipo_contrato(value: str | None) -> str | None:
    key = normalize_key(value)
    if not key:
        return None
    for raw, normalized in TIPO_MAP.items():
        if raw in key:
            return normalized
    return None


def normalize_region(value: str | None) -> str | None:
    text = clean_text(value)
    if not text:
        return None

    key = normalize_key(text)
    for candidate, normalized in REGION_MAP.items():
        if candidate in key:
            return normalized
    return text


def parse_renta(text: str | None) -> tuple[int | None, int | None, str | None]:
    content = clean_text(text)
    if not content:
        return None, None, None

    grado_match = re.search(
        r"\bgrado\s+([0-9]{1,2}(?:\s*[a-z])?)\s*(?:eus|e\.u\.s\.|escala unica)?\b",
        normalize_key(content),
        re.IGNORECASE,
    )
    grado_eus = grado_match.group(1).upper().replace(" ", "") if grado_match else None

    amounts: list[int] = []
    for raw in re.findall(r"\$?\s*(\d{1,3}(?:[.\s]\d{3})+|\d{6,9})", content):
        digits = re.sub(r"[^\d]", "", raw)
        if not digits:
            continue
        number = int(digits)
        if 100_000 <= number <= 99_999_999:
            amounts.append(number)

    if not amounts:
        return None, None, grado_eus
    if len(amounts) == 1:
        return amounts[0], amounts[0], grado_eus
    return min(amounts), max(amounts), grado_eus


def extract_host_like_pattern(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if not parsed.netloc:
        return None
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}%"


def build_file_handler(base_path: Path) -> logging.Handler:
    """Crea un FileHandler tolerando bloqueos de archivo en Windows."""
    try:
        return logging.FileHandler(base_path, encoding="utf-8")
    except PermissionError:
        fallback_path = base_path.with_name(
            f"{base_path.stem}_{os.getpid()}{base_path.suffix}"
        )
        try:
            return logging.FileHandler(fallback_path, encoding="utf-8")
        except PermissionError:
            temp_dir = Path(tempfile.gettempdir()) / "empleoestado_logs"
            temp_dir.mkdir(parents=True, exist_ok=True)
            temp_path = temp_dir / fallback_path.name
            try:
                return logging.FileHandler(temp_path, encoding="utf-8")
            except PermissionError:
                return logging.NullHandler()


class BaseScraper(abc.ABC):
    """Clase base para todos los scrapers del proyecto."""

    #: estados HTTP que justifican reintento (transitorios)
    RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
    #: estados HTTP terminales (no se reintenta)
    TERMINAL_STATUS_CODES: frozenset[int] = frozenset({400, 401, 403, 404, 405, 410, 451})

    def __init__(
        self,
        nombre: str,
        instituciones: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        delay: float = 2.0,
        timeout: int = 10,
        max_results: int | None = None,
        max_retries: int = 2,
    ) -> None:
        self.nombre = nombre
        self.dry_run = dry_run
        self.delay = delay
        self.timeout = timeout
        self.max_results = max_results
        # max_retries=2 significa "1 intento + 1 reintento". Antes eran 3 attempts
        # con backoff 2^n, que llevaba ~30s perdidos por URL muerta.
        self.max_retries = max(1, int(max_retries))
        self.session = requests.Session()
        self.session.trust_env = False
        self.user_agents = itertools.cycle(random.sample(USER_AGENTS, len(USER_AGENTS)))
        self._last_request_at = 0.0
        self._instituciones = instituciones or []
        self._institucion_lookup = self._build_institucion_lookup(self._instituciones)
        self.logger = self._build_logger()
        self.scope_url_patterns: list[str] = []
        self.scope_institucion_ids: list[int] = []
        self.stats = {
            "status": "OK",
            "found": 0,
            "parsed": 0,
            "nuevas": 0,
            "actualizadas": 0,
            "cerradas": 0,
            "errores": 0,
            "duracion_seg": 0.0,
        }

        if not self.dry_run:
            self.ensure_schema()
            if self._instituciones:
                self.sync_instituciones_catalogo(self._instituciones)

    @abc.abstractmethod
    def fetch_ofertas(self) -> list[dict[str, Any]]:
        """Obtiene las ofertas crudas desde la fuente."""

    @abc.abstractmethod
    def parse_oferta(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        """Normaliza una oferta al esquema interno/DB."""

    def run(self) -> dict[str, Any]:
        started_at = time.time()
        self.logger.info("evento=inicio scraper=%s dry_run=%s", self.nombre, self.dry_run)
        try:
            raw_ofertas = self.fetch_ofertas()
            if self.max_results:
                raw_ofertas = raw_ofertas[: self.max_results]

            self.stats["found"] = len(raw_ofertas)
            self.logger.info(
                "evento=ofertas_obtenidas scraper=%s cantidad=%s",
                self.nombre,
                len(raw_ofertas),
            )

            parsed: list[dict[str, Any]] = []
            for raw in raw_ofertas:
                try:
                    normalized = self.parse_oferta(raw)
                    if normalized:
                        parsed.append(normalized)
                except Exception as exc:  # pragma: no cover - defensa runtime
                    self.stats["errores"] += 1
                    self.logger.exception(
                        "evento=parse_error scraper=%s error=%s raw=%s",
                        self.nombre,
                        exc,
                        truncate(json.dumps(raw, ensure_ascii=False), 300),
                    )

            self.stats["parsed"] = len(parsed)
            if not self.dry_run:
                db_stats = self.save_to_db(parsed)
                for key, value in db_stats.items():
                    if key == "errores":
                        self.stats[key] += value
                    else:
                        self.stats[key] = value
        except Exception as exc:  # pragma: no cover - defensa runtime
            self.stats["status"] = "ERROR"
            self.stats["errores"] += 1
            self.logger.exception("evento=run_error scraper=%s error=%s", self.nombre, exc)
            raise
        finally:
            self.stats["duracion_seg"] = round(time.time() - started_at, 2)
            if self.stats["status"] == "OK" and self.stats["errores"] > 0:
                self.stats["status"] = "PARCIAL"
            self.logger.info(
                "evento=fin scraper=%s status=%s found=%s parsed=%s nuevas=%s actualizadas=%s cerradas=%s errores=%s duracion=%s",
                self.nombre,
                self.stats["status"],
                self.stats["found"],
                self.stats["parsed"],
                self.stats["nuevas"],
                self.stats["actualizadas"],
                self.stats["cerradas"],
                self.stats["errores"],
                self.stats["duracion_seg"],
            )
        return dict(self.stats)

    def save_to_db(self, ofertas: list[dict[str, Any]]) -> dict[str, int]:
        stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
        if not ofertas:
            self.logger.warning("evento=sin_ofertas scraper=%s", self.nombre)
            return stats

        seen_urls = sorted({offer["url_oferta"] for offer in ofertas if offer.get("url_oferta")})
        connection = self.get_connection()
        connection.autocommit = False

        try:
            with connection.cursor() as cursor:
                for offer in ofertas:
                    cursor.execute("SAVEPOINT oferta_sp")
                    try:
                        normalized = self.normalize_offer(offer)
                        exists = self._oferta_exists(cursor, normalized["url_oferta"])
                        self._upsert_institucion_si_corresponde(cursor, normalized)
                        cursor.execute(self._offer_upsert_sql(), self._offer_params(normalized))
                        cursor.execute("RELEASE SAVEPOINT oferta_sp")
                        if exists:
                            stats["actualizadas"] += 1
                        else:
                            stats["nuevas"] += 1
                    except Exception as exc:  # pragma: no cover - defensa runtime
                        cursor.execute("ROLLBACK TO SAVEPOINT oferta_sp")
                        stats["errores"] += 1
                        self.logger.exception(
                            "evento=db_offer_error scraper=%s url=%s error=%s",
                            self.nombre,
                            offer.get("url_oferta"),
                            exc,
                        )

                stats["cerradas"] = self._mark_missing_offers_closed(cursor, seen_urls)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return stats

    def normalize_offer(self, offer: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(offer)
        institucion_nombre = clean_text(
            normalized.get("institucion_nombre")
            or normalized.get("institucion")
            or normalized.get("nombre_institucion")
        )
        institucion_id = normalized.get("institucion_id") or self.match_institucion_id(
            institucion_nombre
        )

        renta_min = normalized.get("renta_bruta_min")
        renta_max = normalized.get("renta_bruta_max")
        grado_eus = normalized.get("grado_eus")
        if renta_min is None and renta_max is None:
            renta_min, renta_max, grado_from_text = parse_renta(
                normalized.get("renta_texto") or normalized.get("descripcion")
            )
            grado_eus = grado_eus or grado_from_text

        fecha_cierre = parse_date(normalized.get("fecha_cierre"))
        estado = normalized.get("estado") or self._resolve_estado(fecha_cierre)

        normalized = {
            "institucion_id": institucion_id,
            "institucion_nombre": truncate(institucion_nombre, 300),
            "cargo": truncate(normalized.get("cargo"), 500),
            "descripcion": clean_text(normalized.get("descripcion")) or None,
            "requisitos": clean_text(normalized.get("requisitos")) or None,
            "tipo_contrato": normalize_tipo_contrato(normalized.get("tipo_contrato")),
            "region": truncate(
                normalize_region(normalized.get("region") or self._default_region(normalized)),
                100,
            ),
            "ciudad": truncate(normalized.get("ciudad"), 150),
            "renta_bruta_min": self._to_int(renta_min),
            "renta_bruta_max": self._to_int(renta_max),
            "grado_eus": truncate(grado_eus, 20),
            "jornada": truncate(normalized.get("jornada"), 100),
            "area_profesional": truncate(normalized.get("area_profesional"), 200),
            "fecha_publicacion": parse_date(normalized.get("fecha_publicacion")),
            "fecha_cierre": fecha_cierre,
            "url_oferta": clean_text(normalized.get("url_oferta") or normalized.get("url_original")),
            "url_bases": clean_text(normalized.get("url_bases")) or None,
            "estado": estado,
        }

        if not normalized["cargo"]:
            raise ValueError("La oferta no tiene cargo valido")
        if not normalized["url_oferta"]:
            raise ValueError("La oferta no tiene url_oferta valida")
        return normalized

    def get_connection(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(**DB_CONFIG)

    def ensure_schema(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS instituciones (
            id INTEGER PRIMARY KEY,
            nombre VARCHAR(300) NOT NULL,
            sigla VARCHAR(50),
            sector VARCHAR(100),
            region VARCHAR(100),
            url_empleo TEXT,
            plataforma_empleo VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS ofertas (
            id SERIAL PRIMARY KEY,
            institucion_id INTEGER REFERENCES instituciones(id),
            cargo VARCHAR(500) NOT NULL,
            descripcion TEXT,
            requisitos TEXT,
            tipo_contrato VARCHAR(50),
            region VARCHAR(100),
            ciudad VARCHAR(150),
            renta_bruta_min INTEGER,
            renta_bruta_max INTEGER,
            grado_eus VARCHAR(20),
            jornada VARCHAR(100),
            area_profesional VARCHAR(200),
            fecha_publicacion DATE,
            fecha_cierre DATE,
            url_oferta TEXT UNIQUE,
            url_bases TEXT,
            estado VARCHAR(20) DEFAULT 'activo',
            fecha_scraped TIMESTAMP DEFAULT NOW(),
            fecha_actualizado TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_ofertas_estado ON ofertas (estado);
        CREATE INDEX IF NOT EXISTS idx_ofertas_institucion_id ON ofertas (institucion_id);
        CREATE INDEX IF NOT EXISTS idx_ofertas_fecha_cierre ON ofertas (fecha_cierre);

        ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS sigla VARCHAR(50);
        ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS sector VARCHAR(100);
        ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS region VARCHAR(100);
        ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS url_empleo TEXT;
        ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS plataforma_empleo VARCHAR(100);

        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS institucion_id INTEGER;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS cargo VARCHAR(500);
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS descripcion TEXT;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS requisitos TEXT;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS tipo_contrato VARCHAR(50);
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS region VARCHAR(100);
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS ciudad VARCHAR(150);
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_bruta_min INTEGER;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS renta_bruta_max INTEGER;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS grado_eus VARCHAR(20);
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS jornada VARCHAR(100);
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS area_profesional VARCHAR(200);
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_publicacion DATE;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_cierre DATE;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_oferta TEXT;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases TEXT;
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS estado VARCHAR(20) DEFAULT 'activo';
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_scraped TIMESTAMP DEFAULT NOW();
        ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_actualizado TIMESTAMP DEFAULT NOW();

        CREATE UNIQUE INDEX IF NOT EXISTS uq_ofertas_url_oferta ON ofertas (url_oferta);
        """
        connection = self.get_connection()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(ddl)
        finally:
            connection.close()

    def sync_instituciones_catalogo(self, instituciones: list[dict[str, Any]]) -> None:
        if not instituciones:
            return

        sql = """
        INSERT INTO instituciones (
            id,
            nombre,
            sigla,
            sector,
            region,
            url_empleo,
            plataforma_empleo
        ) VALUES (
            %(id)s,
            %(nombre)s,
            %(sigla)s,
            %(sector)s,
            %(region)s,
            %(url_empleo)s,
            %(plataforma_empleo)s
        )
        ON CONFLICT (id) DO UPDATE
        SET nombre = EXCLUDED.nombre,
            sigla = EXCLUDED.sigla,
            sector = EXCLUDED.sector,
            region = EXCLUDED.region,
            url_empleo = EXCLUDED.url_empleo,
            plataforma_empleo = EXCLUDED.plataforma_empleo
        """

        rows = []
        for item in instituciones:
            if item.get("id") is None:
                continue
            rows.append(
                {
                    "id": item["id"],
                    "nombre": truncate(item.get("nombre"), 300),
                    "sigla": truncate(item.get("sigla"), 50),
                    "sector": truncate(item.get("sector"), 100),
                    "region": truncate(normalize_region(item.get("region")), 100),
                    "url_empleo": clean_text(
                        item.get("url_empleo") or item.get("url_portal_empleos")
                    )
                    or None,
                    "plataforma_empleo": truncate(item.get("plataforma_empleo"), 100),
                }
            )

        if not rows:
            return

        connection = self.get_connection()
        try:
            with connection:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_batch(cursor, sql, rows, page_size=200)
        finally:
            connection.close()

    def match_institucion_id(self, institution_name: str | None) -> int | None:
        name = clean_text(institution_name)
        if not name:
            return None

        key = normalize_key(name)
        exact = self._institucion_lookup["exact"].get(key)
        if exact is not None:
            return exact

        for candidate_key, candidate_id in self._institucion_lookup["exact"].items():
            if key in candidate_key or candidate_key in key:
                return candidate_id

        choices = list(self._institucion_lookup["names"])
        matches = difflib.get_close_matches(key, choices, n=1, cutoff=0.80)
        if not matches:
            self.logger.warning(
                "evento=institucion_no_match scraper=%s institucion=%s",
                self.nombre,
                name,
            )
            return None
        return self._institucion_lookup["exact"].get(matches[0])

    def request(
        self,
        url: str,
        method: str = "GET",
        **kwargs: Any,
    ) -> requests.Response:
        """
        Hace una petición HTTP con política de retry selectiva:

        - 4xx terminales (404, 403, 401, 400, 405, 410, 451) -> NO se reintenta.
        - 5xx / 408 / 425 / 429 -> se reintenta con backoff corto.
        - Timeout / ConnectionError -> se reintenta con backoff corto.
        - SSLError -> un reintento único con verify=False.
        - Otros errores de DNS / conexión rechazada -> NO se reintenta.

        max_attempts = self.max_retries (1 intento + retries).
        """
        base_kwargs = dict(kwargs)
        verify = base_kwargs.pop("verify", True)
        insecure_retry_done = False

        max_attempts = self.max_retries + 1
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                self._respect_rate_limit()
                request_kwargs = dict(base_kwargs)
                headers = dict(request_kwargs.pop("headers", {}) or {})
                for key, value in DEFAULT_BROWSER_HEADERS.items():
                    headers.setdefault(key, value)
                headers.setdefault("User-Agent", next(self.user_agents))
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    timeout=request_kwargs.pop("timeout", self.timeout),
                    headers=headers,
                    verify=verify,
                    **request_kwargs,
                )
                status = response.status_code
                if status in self.TERMINAL_STATUS_CODES:
                    # No hay nada que rescatar: abortamos de inmediato.
                    response.raise_for_status()
                if status in self.RETRYABLE_STATUS_CODES or status >= 500:
                    response.raise_for_status()
                return response

            except requests_exceptions.SSLError as exc:
                last_error = exc
                if insecure_retry_done:
                    self.logger.warning(
                        "evento=request_ssl_fail scraper=%s url=%s",
                        self.nombre,
                        url,
                    )
                    break
                insecure_retry_done = True
                verify = False
                self.logger.warning(
                    "evento=request_ssl_insecure_retry scraper=%s url=%s",
                    self.nombre,
                    url,
                )
                continue

            except requests_exceptions.HTTPError as exc:
                last_error = exc
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                # Terminales: cortamos sin reintentar.
                if status_code in self.TERMINAL_STATUS_CODES:
                    self.logger.info(
                        "evento=request_terminal scraper=%s url=%s status=%s",
                        self.nombre,
                        url,
                        status_code,
                    )
                    break
                # Retryables: backoff corto.
                if attempt >= max_attempts:
                    break
                backoff = min(2.0, 0.5 * attempt)
                self.logger.warning(
                    "evento=request_retry scraper=%s url=%s intento=%s status=%s espera=%.1f",
                    self.nombre,
                    url,
                    attempt,
                    status_code,
                    backoff,
                )
                time.sleep(backoff)

            except (requests_exceptions.Timeout, requests_exceptions.ConnectionError) as exc:
                last_error = exc
                # DNS errors / NameResolution suelen aparecer como ConnectionError:
                # no tiene sentido reintentarlos más de 1 vez.
                message = str(exc).lower()
                is_dns = (
                    "name or service not known" in message
                    or "nodename nor servname" in message
                    or "getaddrinfo failed" in message
                    or "failed to resolve" in message
                )
                if is_dns:
                    self.logger.info(
                        "evento=request_dns_fail scraper=%s url=%s",
                        self.nombre,
                        url,
                    )
                    break
                if attempt >= max_attempts:
                    break
                backoff = min(2.0, 0.5 * attempt)
                self.logger.warning(
                    "evento=request_retry scraper=%s url=%s intento=%s espera=%.1f error=%s",
                    self.nombre,
                    url,
                    attempt,
                    backoff,
                    type(exc).__name__,
                )
                time.sleep(backoff)

            except requests.RequestException as exc:
                last_error = exc
                self.logger.info(
                    "evento=request_abort scraper=%s url=%s error=%s",
                    self.nombre,
                    url,
                    type(exc).__name__,
                )
                break

        assert last_error is not None
        raise last_error

    def request_text(self, url: str, method: str = "GET", **kwargs: Any) -> str:
        response = self.request(url=url, method=method, **kwargs)
        response.encoding = response.encoding or "utf-8"
        return response.text

    def request_json(self, url: str, method: str = "GET", **kwargs: Any) -> Any:
        response = self.request(url=url, method=method, **kwargs)
        return response.json()

    def _default_region(self, normalized: dict[str, Any]) -> str | None:
        if normalized.get("institucion_id") is None:
            return None
        for institution in self._instituciones:
            if institution.get("id") == normalized["institucion_id"]:
                return institution.get("region")
        return None

    def _oferta_exists(self, cursor: psycopg2.extensions.cursor, url_oferta: str) -> bool:
        cursor.execute("SELECT 1 FROM ofertas WHERE url_oferta = %s", (url_oferta,))
        return cursor.fetchone() is not None

    def _upsert_institucion_si_corresponde(
        self,
        cursor: psycopg2.extensions.cursor,
        offer: dict[str, Any],
    ) -> None:
        institucion_id = offer.get("institucion_id")
        if not institucion_id:
            return
        institution = self._find_institution_by_id(institucion_id)
        if not institution:
            return
        cursor.execute(
            """
            INSERT INTO instituciones (
                id, nombre, sigla, sector, region, url_empleo, plataforma_empleo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET nombre = EXCLUDED.nombre,
                sigla = EXCLUDED.sigla,
                sector = EXCLUDED.sector,
                region = EXCLUDED.region,
                url_empleo = EXCLUDED.url_empleo,
                plataforma_empleo = EXCLUDED.plataforma_empleo
            """,
            (
                institution.get("id"),
                truncate(institution.get("nombre"), 300),
                truncate(institution.get("sigla"), 50),
                truncate(institution.get("sector"), 100),
                truncate(normalize_region(institution.get("region")), 100),
                clean_text(institution.get("url_empleo") or institution.get("url_portal_empleos"))
                or None,
                truncate(institution.get("plataforma_empleo"), 100),
            ),
        )

    def _offer_upsert_sql(self) -> str:
        return """
        INSERT INTO ofertas (
            institucion_id,
            cargo,
            descripcion,
            requisitos,
            tipo_contrato,
            region,
            ciudad,
            renta_bruta_min,
            renta_bruta_max,
            grado_eus,
            jornada,
            area_profesional,
            fecha_publicacion,
            fecha_cierre,
            url_oferta,
            url_bases,
            estado,
            fecha_scraped,
            fecha_actualizado
        ) VALUES (
            %(institucion_id)s,
            %(cargo)s,
            %(descripcion)s,
            %(requisitos)s,
            %(tipo_contrato)s,
            %(region)s,
            %(ciudad)s,
            %(renta_bruta_min)s,
            %(renta_bruta_max)s,
            %(grado_eus)s,
            %(jornada)s,
            %(area_profesional)s,
            %(fecha_publicacion)s,
            %(fecha_cierre)s,
            %(url_oferta)s,
            %(url_bases)s,
            %(estado)s,
            NOW(),
            NOW()
        )
        ON CONFLICT (url_oferta) DO UPDATE
        SET institucion_id = COALESCE(EXCLUDED.institucion_id, ofertas.institucion_id),
            cargo = EXCLUDED.cargo,
            descripcion = COALESCE(EXCLUDED.descripcion, ofertas.descripcion),
            requisitos = COALESCE(EXCLUDED.requisitos, ofertas.requisitos),
            tipo_contrato = COALESCE(EXCLUDED.tipo_contrato, ofertas.tipo_contrato),
            region = COALESCE(EXCLUDED.region, ofertas.region),
            ciudad = COALESCE(EXCLUDED.ciudad, ofertas.ciudad),
            renta_bruta_min = COALESCE(EXCLUDED.renta_bruta_min, ofertas.renta_bruta_min),
            renta_bruta_max = COALESCE(EXCLUDED.renta_bruta_max, ofertas.renta_bruta_max),
            grado_eus = COALESCE(EXCLUDED.grado_eus, ofertas.grado_eus),
            jornada = COALESCE(EXCLUDED.jornada, ofertas.jornada),
            area_profesional = COALESCE(EXCLUDED.area_profesional, ofertas.area_profesional),
            fecha_publicacion = COALESCE(EXCLUDED.fecha_publicacion, ofertas.fecha_publicacion),
            fecha_cierre = COALESCE(EXCLUDED.fecha_cierre, ofertas.fecha_cierre),
            url_bases = COALESCE(EXCLUDED.url_bases, ofertas.url_bases),
            estado = EXCLUDED.estado,
            fecha_scraped = NOW(),
            fecha_actualizado = NOW()
        """

    def _offer_params(self, offer: dict[str, Any]) -> dict[str, Any]:
        return offer

    def _mark_missing_offers_closed(
        self,
        cursor: psycopg2.extensions.cursor,
        seen_urls: list[str],
    ) -> int:
        if not seen_urls:
            self.logger.warning(
                "evento=skip_close_missing scraper=%s motivo=sin_urls_vistas",
                self.nombre,
            )
            return 0

        scope_sql, params = self._build_scope_where_sql()
        if not scope_sql:
            self.logger.warning(
                "evento=skip_close_missing scraper=%s motivo=sin_scope",
                self.nombre,
            )
            return 0

        sql = f"""
        UPDATE ofertas
        SET estado = 'cerrado',
            fecha_actualizado = NOW()
        WHERE estado = 'activo'
          AND ({scope_sql})
          AND NOT (url_oferta = ANY(%s))
        """
        cursor.execute(sql, (*params, seen_urls))
        return cursor.rowcount

    def _build_scope_where_sql(self) -> tuple[str, list[Any]]:
        conditions: list[str] = []
        params: list[Any] = []

        if self.scope_institucion_ids:
            conditions.append("institucion_id = ANY(%s)")
            params.append(self.scope_institucion_ids)

        if self.scope_url_patterns:
            pattern_sql = " OR ".join(["url_oferta LIKE %s"] * len(self.scope_url_patterns))
            conditions.append(f"({pattern_sql})")
            params.extend(self.scope_url_patterns)

        return " AND ".join(conditions), params

    def _resolve_estado(self, fecha_cierre: date | None) -> str:
        if fecha_cierre and fecha_cierre < date.today():
            return "vencido"
        return "activo"

    def _respect_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_at = time.monotonic()

    def _build_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.nombre)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
            )
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            file_handler = build_file_handler(
                LOG_DIR / f"scraper_{date.today().strftime('%Y%m%d')}.log"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            logger.addHandler(file_handler)
        logger.propagate = False
        return logger

    def _build_institucion_lookup(
        self,
        instituciones: list[dict[str, Any]],
    ) -> dict[str, dict[str, int] | list[str]]:
        exact: dict[str, int] = {}
        for institution in instituciones:
            if institution.get("id") is None:
                continue
            candidates = {
                institution.get("nombre"),
                institution.get("sigla"),
            }
            for candidate in candidates:
                key = normalize_key(candidate)
                if key:
                    exact[key] = institution["id"]
        return {"exact": exact, "names": list(exact.keys())}

    def _find_institution_by_id(self, institution_id: int) -> dict[str, Any] | None:
        for institution in self._instituciones:
            if institution.get("id") == institution_id:
                return institution
        return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, int):
            return value
        digits = re.sub(r"[^\d]", "", str(value))
        if not digits:
            return None
        return int(digits)
