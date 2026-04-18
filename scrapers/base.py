# -*- coding: utf-8 -*-
"""
scrapers/base.py — Núcleo compartido por todos los scrapers
contrataoplanta.cl  |  post-audit 2026-04-15

Contenido:
    - Configuración y pool de PostgreSQL
    - Cliente HTTP async con retries, timeout, rate limit por dominio
    - Filtros de precisión (keywords positivas/negativas)
    - Validación de vigencia
    - Extracción robusta de fechas y renta
    - Fuzzy matching de instituciones
    - Normalización canónica de URLs + hash
    - PrecisionReport
    - BaseScraper (ABC) que todos los scrapers deben extender
    - Helpers de limpieza (limpiar_vencidas)
    - setup_logging con rotación

Python 3.10+, UTF-8 explícito en todos los open() y handlers.
Todo comentario en español.
"""

from __future__ import annotations

import abc
import asyncio
import difflib
import hashlib
import logging
import logging.handlers
import os
import random
import re
import sys
import time
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp
import itertools  # usado por LegacyBaseScraper

try:
    import requests  # usado por LegacyBaseScraper (scrapers síncronos legacy)
    from requests import exceptions as requests_exceptions
except ImportError:
    requests = None  # type: ignore[assignment]
    requests_exceptions = None  # type: ignore[assignment]

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import pool as pg_pool
except ImportError:  # pragma: no cover - depende del entorno
    psycopg2 = None  # type: ignore[assignment]
    pg_pool = None  # type: ignore[assignment]

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ═══════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════

def _requerido(nombre: str) -> str:
    """Lee una variable de entorno obligatoria o aborta.

    Post-audit 1.1: se elimina cualquier fallback con credenciales
    hardcodeadas. Si falta la variable, el proceso debe fallar ruidoso.
    """
    valor = os.getenv(nombre)
    if not valor:
        sys.stderr.write(
            f"[FATAL] Variable de entorno {nombre!r} no definida. "
            f"Crea un archivo .env a partir de .env.example.\n"
        )
        sys.exit(2)
    return valor


DB_CONFIG: dict[str, Any] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "empleospublicos"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": _requerido("DB_PASSWORD"),
}

# Timeout global de HTTP (post-audit 1.10)
HTTP_TIMEOUT_TOTAL = int(os.getenv("HTTP_TIMEOUT_TOTAL", "30"))
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "10"))

# Concurrencia máxima global
MAX_CONCURRENCIA_GLOBAL = int(os.getenv("MAX_CONCURRENCIA_GLOBAL", "10"))

# Concurrencia máxima por dominio (para no saturar municipios pequeños)
MAX_CONCURRENCIA_POR_DOMINIO = int(os.getenv("MAX_CONCURRENCIA_POR_DOMINIO", "2"))

# User-Agents rotativos (post-audit 1.11)
USER_AGENTS: list[str] = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
     "Gecko/20100101 Firefox/124.0"),
]


# ═══════════════════════════════════════════════════════════════════
#  LOGGING CON ROTACIÓN (post-audit 1.16, 4.6)
# ═══════════════════════════════════════════════════════════════════

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)-25s — %(message)s"


def setup_logging(nombre: str, nivel: int = logging.INFO) -> logging.Logger:
    """Crea un logger con consola + archivo rotativo (10MB × 5).

    UTF-8 explícito para funcionar en Windows sin romperse con tildes.
    """
    logger = logging.getLogger(nombre)
    if logger.handlers:
        # Ya configurado: evita duplicar handlers en tests / imports múltiples
        return logger

    logger.setLevel(nivel)
    fmt = logging.Formatter(_LOG_FORMAT)

    consola = logging.StreamHandler(sys.stdout)
    consola.setFormatter(fmt)
    logger.addHandler(consola)

    archivo = logging.handlers.RotatingFileHandler(
        LOG_DIR / f"{nombre}.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    archivo.setFormatter(fmt)
    logger.addHandler(archivo)

    logger.propagate = False
    return logger


log = setup_logging("scraper.base")


# ═══════════════════════════════════════════════════════════════════
#  POOL DE CONEXIONES POSTGRESQL (post-audit 1.13)
# ═══════════════════════════════════════════════════════════════════

_DB_POOL: pg_pool.ThreadedConnectionPool | None = None


def get_pool() -> pg_pool.ThreadedConnectionPool:
    """Retorna el pool global, creándolo perezosamente la primera vez."""
    global _DB_POOL
    if pg_pool is None:
        raise RuntimeError("psycopg2 no esta instalado; no se puede abrir el pool PostgreSQL.")
    if _DB_POOL is None:
        _DB_POOL = pg_pool.ThreadedConnectionPool(
            minconn=int(os.getenv("DB_POOL_MIN", "1")),
            maxconn=int(os.getenv("DB_POOL_MAX", "5")),
            **DB_CONFIG,
        )
        log.info("Pool de PostgreSQL inicializado (min=%s, max=%s)",
                 os.getenv("DB_POOL_MIN", "1"), os.getenv("DB_POOL_MAX", "5"))
    return _DB_POOL


def cerrar_pool() -> None:
    """Cierra el pool al finalizar el proceso."""
    global _DB_POOL
    if _DB_POOL is not None:
        _DB_POOL.closeall()
        _DB_POOL = None
        log.info("Pool de PostgreSQL cerrado")


@contextmanager
def conexion():
    """Context manager que entrega una conexión del pool y la devuelve.

    Uso:
        with conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ═══════════════════════════════════════════════════════════════════
#  NORMALIZACIÓN BÁSICA DE TEXTO
# ═══════════════════════════════════════════════════════════════════

def strip_accents(value: str | None) -> str:
    """Quita tildes y diacríticos; devuelve string vacío si None."""
    if not value:
        return ""
    normalizado = unicodedata.normalize("NFKD", value)
    return "".join(c for c in normalizado if not unicodedata.combining(c))


def normalizar_texto(value: str | None) -> str:
    """Lowercase + sin tildes + collapso de espacios. Para comparaciones."""
    if not value:
        return ""
    limpio = strip_accents(value).lower()
    limpio = re.sub(r"\s+", " ", limpio).strip()
    return limpio


def clean_text(value: str | None) -> str:
    """Alias compat para scrapers mas nuevos."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_key(value: str | None) -> str:
    """Alias compat: texto normalizado para llaves y comparaciones."""
    return normalizar_texto(value)


def extract_host_like_pattern(url: str | None) -> str:
    """Normaliza un host para reglas de scope y matching de extractores."""
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = parsed.netloc.lower()
    return host[4:] if host.startswith("www.") else host


# ═══════════════════════════════════════════════════════════════════
#  FILTROS DE PRECISIÓN — KEYWORDS (post-audit 1.3, spec 1B pasos 1-2)
# ═══════════════════════════════════════════════════════════════════

KEYWORDS_OFERTA: tuple[str, ...] = (
    # Tipos de cargo y llamados formales
    "concurso publico",
    "llamado a concurso",
    "provision de cargo",
    "proveer cargo",
    "vacante",
    "planta municipal",
    "cargo de planta",
    "cargo a contrata",
    "cargo contrata",
    "honorarios",
    "seleccion de personal",
    "proceso de seleccion",
    "reclutamiento",
    "postulacion",
    "postulantes",
    "trabaje con nosotros",
    "trabaja con nosotros",
    "oferta laboral",
    "oferta de empleo",
    "llamado a postulacion",
    "bases concurso",
    "bases del concurso",
    "reemplazo",
    "suplencia",
    "banco de reemplazos",
    "banco curricular",
    # Escalafones y estatutos
    "estatuto administrativo",
    "ley 18.883",
    "ley 18883",
    "ley 19.378",
    "ley 19378",
    "escalafon",
    "grado eus",
    "grado ems",
    "escala municipal",
    # Palabras de proceso laboral
    "curriculum",
    "cv",
    "antecedentes",
    "postular",
    "perfil del cargo",
    "descriptor de cargo",
    "requisitos del cargo",
)


KEYWORDS_EXCLUIR: tuple[str, ...] = (
    # Concursos que no son de empleo
    "concurso de fotografia",
    "concurso literario",
    "concurso artistico",
    "concurso cultural",
    "concurso de dibujo",
    "concurso de pintura",
    "concurso de cuento",
    "concurso de poesia",
    "concurso musical",
    "concurso de canto",
    "concurso de video",
    "concurso fondeve",
    "fondo concursable",
    "concurso vecinal",
    "concurso comunitario",
    "concurso estudiantil",
    "concurso escolar",
    "concurso juvenil",
    "reina",
    "embajadora",
    "carnaval",
    "festival",
    # Licitaciones y compras
    "licitacion",
    "propuesta publica",
    "llamado a propuesta",
    "bases de licitacion",
    "contratacion de servicios",
    "adquisicion",
    "cotizacion",
    "presupuesto participativo",
    # Noticias y comunicados
    "inauguracion",
    "ceremonia",
    "capacitacion vecinal",
    "cuenta publica",
    "rendicion",
    "memoria anual",
    "plan regulador",
    "ordenanza",
    # Beneficios municipales
    "subsidio",
    "beneficio social",
    "postulacion vivienda",
    "beca",
    "fondo solidario",
    "fonasa",
    # Permisos y patentes
    "permiso de circulacion",
    "patente comercial",
    "permiso de edificacion",
)


# Keywords que habilitan descartes por título EXACTO incluso si contiene
# alguna keyword positiva (p.ej. "concurso" aparece en ambas listas)
KEYWORDS_EXCLUIR_DURAS: tuple[str, ...] = (
    "licitacion",
    "fondeve",
    "beca",
    "subsidio",
    "permiso de circulacion",
    "patente comercial",
    "festival",
)


# Para WordPress
CATEGORIAS_EMPLEO_WP: tuple[str, ...] = (
    "concursos", "concurso-publico", "empleo", "personal",
    "planta", "contrata", "honorarios", "recursos-humanos",
    "trabaja-con-nosotros", "oferta-laboral",
)

CATEGORIAS_EXCLUIR_WP: tuple[str, ...] = (
    "noticias", "eventos", "cultural", "licitaciones",
    "transparencia", "fondeve", "comunitario", "deportes",
)


def es_oferta_laboral(titulo: str, contenido: str = "") -> tuple[bool, str, str]:
    """Determina si una publicación es una oferta laboral real.

    Retorna una tupla (es_oferta, motivo, keyword_detectada).
    `motivo` es uno de:
        - "ok" si pasa el filtro
        - "keyword_negativa"
        - "sin_keywords"
        - "contenido_vacio"
    """
    if not titulo or not titulo.strip():
        return False, "contenido_vacio", ""

    titulo_norm = normalizar_texto(titulo)
    contenido_norm = normalizar_texto(contenido or "")
    texto_completo = f"{titulo_norm} {contenido_norm}"

    # Descartes duros por título (licitaciones, fondeve, etc.)
    for kw in KEYWORDS_EXCLUIR_DURAS:
        if kw in titulo_norm:
            return False, "keyword_negativa", kw

    # Descartes por keyword negativa en el título
    for kw in KEYWORDS_EXCLUIR:
        if kw in titulo_norm:
            return False, "keyword_negativa", kw

    # Filtro positivo: al menos una keyword laboral en título o contenido
    for kw in KEYWORDS_OFERTA:
        if kw in texto_completo:
            return True, "ok", kw

    return False, "sin_keywords", ""


# ═══════════════════════════════════════════════════════════════════
#  VALIDACIÓN DE VIGENCIA (spec 1B paso 3)
# ═══════════════════════════════════════════════════════════════════

TOLERANCIA_DIAS_CIERRE = 0     # gatekeeper nuevo: no publicar si fecha_cierre < hoy
VIDA_UTIL_SIN_CIERRE_DIAS = 60 # criterio subsidiario legacy


def es_vigente(fecha_cierre: date | None, fecha_publicacion: date | None) -> bool:
    """Determina si una oferta está dentro de un rango temporal razonable."""
    hoy = date.today()

    if fecha_cierre is not None:
        if fecha_cierre >= hoy:
            return True
        if (hoy - fecha_cierre).days <= TOLERANCIA_DIAS_CIERRE:
            return True
        return False

    if fecha_publicacion is not None:
        return (hoy - fecha_publicacion).days <= VIDA_UTIL_SIN_CIERRE_DIAS

    # Sin ninguna fecha: se captura pero queda marcada para revisión
    return True


# ═══════════════════════════════════════════════════════════════════
#  EXTRACCIÓN DE FECHAS (post-audit 1.8, spec 1B paso 4)
# ═══════════════════════════════════════════════════════════════════

MESES_ES: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


# Patrones de fecha. El orden importa: ISO primero para que DD-MM no lo capture.
PATRONES_FECHA: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b"), "iso"),
    (re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"), "dmy_slash"),
    (re.compile(r"\b(\d{1,2})-(\d{1,2})-(\d{4})\b"), "dmy_dash"),
    (re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b"), "dmy_dot"),
    (re.compile(
        r"\b(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(\d{4})\b",
        re.IGNORECASE,
    ), "spanish"),
]

FRASES_CIERRE: tuple[str, ...] = (
    "fecha de cierre",
    "plazo de postulacion",
    "hasta el",
    "cierra el",
    "fecha limite",
    "recepcion hasta",
    "postulaciones hasta",
    "plazo hasta",
    "vence el",
    "termino",
)


def _parse_match(match: re.Match[str], tipo: str) -> date | None:
    try:
        if tipo == "iso":
            y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return date(y, m, d)
        if tipo in ("dmy_slash", "dmy_dash", "dmy_dot"):
            d, m, y = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return date(y, m, d)
        if tipo == "spanish":
            d = int(match.group(1))
            mes_nombre = strip_accents(match.group(2)).lower()
            m = MESES_ES.get(mes_nombre)
            y = int(match.group(3))
            if m is None:
                return None
            return date(y, m, d)
    except (ValueError, IndexError):
        return None
    return None


def _buscar_primera_fecha(texto: str) -> date | None:
    """Primera fecha válida encontrada en `texto`."""
    for patron, tipo in PATRONES_FECHA:
        m = patron.search(texto)
        if m:
            fecha = _parse_match(m, tipo)
            if fecha is not None:
                return fecha
    return None


def _buscar_todas_fechas(texto: str) -> list[date]:
    fechas: list[date] = []
    for patron, tipo in PATRONES_FECHA:
        for m in patron.finditer(texto):
            fecha = _parse_match(m, tipo)
            if fecha is not None:
                fechas.append(fecha)
    return fechas


def extraer_fecha(texto: str) -> date | None:
    """Extrae la primera fecha del texto. Para fechas de publicación, etc."""
    if not texto:
        return None
    return _buscar_primera_fecha(texto)


def extraer_fecha_cierre(texto: str) -> date | None:
    """Extrae la fecha de cierre priorizando frases clave.

    Estrategia:
        1. Buscar una frase de cierre ("plazo hasta", "cierra el", ...)
           y extraer la primera fecha en los 100 caracteres siguientes.
        2. Si no hay frase, devolver la primera fecha futura encontrada.
        3. Si no hay ninguna fecha futura, devolver la fecha más futura
           dentro de los 30 días pasados (tolerancia por mal parseo).
    """
    if not texto:
        return None

    texto_norm = normalizar_texto(texto)

    # Paso 1: cerca de frases clave
    for frase in FRASES_CIERRE:
        idx = texto_norm.find(frase)
        if idx == -1:
            continue
        fragmento = texto[idx:idx + 150]
        fecha = _buscar_primera_fecha(fragmento)
        if fecha is not None:
            return fecha

    # Paso 2: todas las fechas del texto, preferencia a la futura más cercana
    fechas = _buscar_todas_fechas(texto)
    if not fechas:
        return None

    hoy = date.today()
    limite_inferior = hoy - timedelta(days=30)
    fechas_relevantes = [f for f in fechas if f >= limite_inferior]
    if not fechas_relevantes:
        return max(fechas)

    futuras = sorted(f for f in fechas_relevantes if f >= hoy)
    if futuras:
        return futuras[0]
    return max(fechas_relevantes)


# ═══════════════════════════════════════════════════════════════════
#  EXTRACCIÓN DE RENTA (post-audit 1.9)
# ═══════════════════════════════════════════════════════════════════

# $1.250.000 / $ 1.250.000 / 1250000 (al menos 6 dígitos)
_RE_RENTA_PESOS = re.compile(
    r"\$?\s*((?:\d{1,3}(?:[.,]\d{3})+|\d{6,9}))",
    re.IGNORECASE,
)
_RE_RENTA_RANGO = re.compile(
    r"entre\s*\$?\s*((?:\d{1,3}(?:[.,]\d{3})+|\d{6,9}))\s*y\s*\$?\s*"
    r"((?:\d{1,3}(?:[.,]\d{3})+|\d{6,9}))",
    re.IGNORECASE,
)
_RE_GRADO_EUS = re.compile(
    r"grado\s*(\d{1,2})[\s°]*(?:e\.?u\.?s\.?|e\.?m\.?s\.?)?",
    re.IGNORECASE,
)


def _limpiar_numero_pesos(raw: str) -> int | None:
    """'1.250.000' → 1250000; '1,250,000' → 1250000."""
    solo_digitos = re.sub(r"[^\d]", "", raw)
    if not solo_digitos:
        return None
    try:
        valor = int(solo_digitos)
    except ValueError:
        return None
    # Filtro de cordura: entre 300.000 y 30 millones
    if valor < 300_000 or valor > 30_000_000:
        return None
    return valor


@dataclass
class RentaExtraida:
    minimo: int | None = None
    maximo: int | None = None
    texto_libre: str | None = None
    grado_eus: str | None = None


def extraer_renta(texto: str) -> RentaExtraida:
    """Intenta extraer mínimo, máximo y/o grado EUS desde un texto libre."""
    if not texto:
        return RentaExtraida()

    resultado = RentaExtraida()
    texto_norm = normalizar_texto(texto)

    # Rango "entre X y Y"
    m = _RE_RENTA_RANGO.search(texto_norm)
    if m:
        mn = _limpiar_numero_pesos(m.group(1))
        mx = _limpiar_numero_pesos(m.group(2))
        if mn and mx:
            resultado.minimo = min(mn, mx)
            resultado.maximo = max(mn, mx)
            resultado.texto_libre = m.group(0)
            return resultado

    # Monto único en pesos
    m = _RE_RENTA_PESOS.search(texto)
    if m:
        valor = _limpiar_numero_pesos(m.group(1))
        if valor:
            resultado.minimo = valor
            resultado.texto_libre = m.group(0)

    # Grado EUS
    m = _RE_GRADO_EUS.search(texto_norm)
    if m:
        grado = int(m.group(1))
        if 1 <= grado <= 25:
            resultado.grado_eus = f"EUS-{grado}"
            resultado.texto_libre = resultado.texto_libre or f"Grado {grado} EUS"

    return resultado


# ═══════════════════════════════════════════════════════════════════
#  NORMALIZACIÓN DE TIPO DE CARGO Y REGIÓN
# ═══════════════════════════════════════════════════════════════════

TIPO_MAP: dict[str, str] = {
    "planta": "planta",
    "contrata": "contrata",
    "honorario": "honorarios",
    "honorarios": "honorarios",
    "honorarios asimilados": "honorarios",
    "codigo del trabajo": "codigo_trabajo",
    "codigo trabajo": "codigo_trabajo",
    "cod. del trabajo": "codigo_trabajo",
    "reemplazo": "reemplazo",
    "suplencia": "reemplazo",
}


def normalizar_tipo_cargo(raw: str | None) -> str | None:
    """Normaliza texto libre a uno de: planta|contrata|honorarios|reemplazo|codigo_trabajo."""
    if not raw:
        return None
    limpio = normalizar_texto(raw)
    # Orden más específicos primero
    for clave in sorted(TIPO_MAP.keys(), key=len, reverse=True):
        if clave in limpio:
            return TIPO_MAP[clave]
    return None


REGION_MAP: dict[str, str] = {
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


def normalizar_region(raw: str | None) -> str | None:
    if not raw:
        return None
    limpio = normalizar_texto(raw)
    for clave in sorted(REGION_MAP.keys(), key=len, reverse=True):
        if clave in limpio:
            return REGION_MAP[clave]
    return None


# ═══════════════════════════════════════════════════════════════════
#  MATCH DE INSTITUCIONES FUZZY (post-audit 1.7)
# ═══════════════════════════════════════════════════════════════════

# Prefijos y palabras que se remueven para comparar nombres de municipios
_PREFIJOS_MUNI = (
    "i. municipalidad de ",
    "ilustre municipalidad de ",
    "ilustre municipalidad ",
    "i municipalidad de ",
    "municipalidad de ",
    "municipalidad ",
    "muni ",
    "muni. ",
)


def normalizar_nombre_institucion(nombre: str) -> str:
    """Versión canónica del nombre para fuzzy match."""
    norm = normalizar_texto(nombre)
    for prefijo in _PREFIJOS_MUNI:
        if norm.startswith(prefijo):
            norm = norm[len(prefijo):]
            break
    norm = re.sub(r"[^\w\s]", " ", norm)
    norm = re.sub(r"\s+", " ", norm).strip()
    return norm


def match_institucion(
    conn,
    nombre_raw: str,
    sector: str | None = None,
    url_empleo: str | None = None,
    umbral: float = 0.88,
) -> int | None:
    """Busca una institución por nombre usando fuzzy match.

    Si no encuentra ninguna con similitud >= umbral, INSERTA una nueva
    y retorna su id. Si `nombre_raw` es vacío, retorna None.
    """
    if not nombre_raw:
        return None

    nombre_norm = normalizar_nombre_institucion(nombre_raw)
    if not nombre_norm:
        return None

    with conn.cursor() as cur:
        # nombre_norm no existe como columna; normalizamos el nombre al vuelo.
        cur.execute("SELECT id, nombre FROM instituciones")
        candidatos = cur.fetchall()

    mejor_id: int | None = None
    mejor_ratio = 0.0
    for cid, cnombre in candidatos:
        cnorm = normalizar_nombre_institucion(cnombre or "")
        if not cnorm:
            continue
        ratio = difflib.SequenceMatcher(None, nombre_norm, cnorm).ratio()
        if ratio > mejor_ratio:
            mejor_ratio = ratio
            mejor_id = cid

    if mejor_id is not None and mejor_ratio >= umbral:
        return mejor_id

    # Insertar nueva (nombre_corto guarda la versión normalizada para futuras consultas)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instituciones (nombre, nombre_corto, sector, url_empleo)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (nombre_raw.strip(), nombre_norm, sector, url_empleo),
        )
        nuevo_id = cur.fetchone()[0]
    conn.commit()
    log.info("Institución nueva creada: %s (id=%s)", nombre_raw, nuevo_id)
    return nuevo_id


# ═══════════════════════════════════════════════════════════════════
#  URL CANÓNICA + HASH (post-audit 1.5)
# ═══════════════════════════════════════════════════════════════════

_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "_hsenc", "_hsmi",
}


def canonicalizar_url(url: str) -> str:
    """Devuelve una forma canónica estable para deduplicar."""
    if not url:
        return ""
    u = urlparse(url.strip())
    netloc = u.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = u.path.rstrip("/")
    if not path:
        path = "/"
    # Filtrar tracking params y ordenar los demás
    params = [
        (k, v) for k, v in parse_qsl(u.query, keep_blank_values=False)
        if k.lower() not in _TRACKING_PARAMS
    ]
    params.sort()
    query = urlencode(params)
    return urlunparse((u.scheme.lower() or "https", netloc, path, "", query, ""))


def url_hash(url: str) -> str:
    """SHA256 de la URL canónica; index único en ofertas.url_hash."""
    return hashlib.sha256(canonicalizar_url(url).encode("utf-8")).hexdigest()


def contenido_hash(*campos: Any) -> str:
    """Hash estable de los campos principales, para detectar cambios."""
    payload = "|".join(str(c or "") for c in campos)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════════════
#  PRECISION REPORT (spec 1B paso 6)
# ═══════════════════════════════════════════════════════════════════

@dataclass
class PrecisionReport:
    institucion: str
    total_encontradas: int = 0
    descartadas_negativas: int = 0
    descartadas_sin_keywords: int = 0
    descartadas_vencidas: int = 0
    descartadas_duplicadas: int = 0
    guardadas: int = 0
    ya_existian: int = 0
    errores: int = 0

    @property
    def tasa_precision(self) -> float:
        if self.total_encontradas == 0:
            return 0.0
        return (self.guardadas + self.ya_existian) / self.total_encontradas * 100.0

    def resumen(self) -> str:
        return (
            f"{self.institucion:40.40} | {self.total_encontradas:4d} encontradas → "
            f"{self.guardadas:3d} nuevas, {self.ya_existian:3d} ya existían | "
            f"precisión: {self.tasa_precision:5.1f}%"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "institucion": self.institucion,
            "total_encontradas": self.total_encontradas,
            "descartadas_negativas": self.descartadas_negativas,
            "descartadas_sin_keywords": self.descartadas_sin_keywords,
            "descartadas_vencidas": self.descartadas_vencidas,
            "descartadas_duplicadas": self.descartadas_duplicadas,
            "guardadas": self.guardadas,
            "ya_existian": self.ya_existian,
            "errores": self.errores,
            "tasa_precision": round(self.tasa_precision, 2),
        }


# ═══════════════════════════════════════════════════════════════════
#  CLIENTE HTTP CON RATE LIMIT POR DOMINIO (post-audit 1.10-1.12)
# ═══════════════════════════════════════════════════════════════════


@dataclass(slots=True)
class HttpFetchResult:
    url: str
    final_url: str
    status: int | None
    body: str | None
    headers: dict[str, str] = field(default_factory=dict)
    json_data: Any | None = None
    error_type: str | None = None
    error_detail: str | None = None

class HttpClient:
    """Wrapper sobre aiohttp con retries, rate limit por dominio y UA rotativo."""

    def __init__(
        self,
        max_por_dominio: int = MAX_CONCURRENCIA_POR_DOMINIO,
        intentos: int = 3,
        delay_base: float = 1.0,
    ) -> None:
        self._dominio_semaforos: dict[str, asyncio.Semaphore] = {}
        self._max_por_dominio = max_por_dominio
        self._intentos = intentos
        self._delay_base = delay_base
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "HttpClient":
        timeout = aiohttp.ClientTimeout(
            total=HTTP_TIMEOUT_TOTAL,
            connect=HTTP_TIMEOUT_CONNECT,
        )
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCIA_GLOBAL, ssl=False)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"Accept-Language": "es-CL,es;q=0.9"},
        )
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        if self._session is not None:
            await self._session.close()

    def _semaforo(self, url: str) -> asyncio.Semaphore:
        dominio = urlparse(url).netloc.lower()
        if dominio not in self._dominio_semaforos:
            self._dominio_semaforos[dominio] = asyncio.Semaphore(self._max_por_dominio)
        return self._dominio_semaforos[dominio]

    async def get(
        self,
        url: str,
        *,
        as_json: bool = False,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> str | dict[str, Any] | None:
        """GET con retries. Retorna str, dict o None si falla definitivamente."""
        result = await self.fetch(
            url,
            as_json=as_json,
            params=params,
            extra_headers=extra_headers,
        )
        if as_json:
            return result.json_data
        return result.body

    async def fetch(
        self,
        url: str,
        *,
        as_json: bool = False,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> HttpFetchResult:
        """GET con trazabilidad de status/headers/errores para el gatekeeper."""
        if self._session is None:
            raise RuntimeError("HttpClient debe usarse dentro de un async with")

        headers = {"User-Agent": random.choice(USER_AGENTS)}
        if extra_headers:
            headers.update(extra_headers)

        sem = self._semaforo(url)
        intento = 0
        while intento < self._intentos:
            intento += 1
            try:
                async with sem:
                    async with self._session.get(url, params=params, headers=headers) as resp:
                        body_text: str | None = None
                        json_data: Any | None = None
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", "5"))
                            log.warning("429 en %s, durmiendo %ss", url, retry_after)
                            await asyncio.sleep(retry_after)
                            continue
                        if resp.status in (403, 503):
                            log.warning("HTTP %s en %s (intento %s)", resp.status, url, intento)
                            body_text = await resp.text(errors="replace")
                            if intento < self._intentos:
                                await self._backoff(intento)
                                continue
                            return HttpFetchResult(
                                url=url,
                                final_url=str(resp.url),
                                status=resp.status,
                                body=body_text,
                                headers=dict(resp.headers),
                            )
                        if resp.status >= 400:
                            log.warning("HTTP %s en %s — no reintentable", resp.status, url)
                            body_text = await resp.text(errors="replace")
                            return HttpFetchResult(
                                url=url,
                                final_url=str(resp.url),
                                status=resp.status,
                                body=body_text,
                                headers=dict(resp.headers),
                            )
                        if as_json:
                            json_data = await resp.json(content_type=None)
                            return HttpFetchResult(
                                url=url,
                                final_url=str(resp.url),
                                status=resp.status,
                                body=None,
                                headers=dict(resp.headers),
                                json_data=json_data,
                            )
                        body_text = await resp.text(errors="replace")
                        return HttpFetchResult(
                            url=url,
                            final_url=str(resp.url),
                            status=resp.status,
                            body=body_text,
                            headers=dict(resp.headers),
                        )
            except asyncio.TimeoutError:
                log.warning("Timeout en %s (intento %s)", url, intento)
                await self._backoff(intento)
                if intento >= self._intentos:
                    return HttpFetchResult(
                        url=url,
                        final_url=url,
                        status=None,
                        body=None,
                        error_type="timeout",
                        error_detail="asyncio.TimeoutError",
                    )
            except aiohttp.TooManyRedirects as e:
                return HttpFetchResult(
                    url=url,
                    final_url=url,
                    status=None,
                    body=None,
                    error_type="redirect_loop",
                    error_detail=str(e),
                )
            except aiohttp.ClientError as e:
                log.warning("ClientError %s en %s (intento %s)", e, url, intento)
                await self._backoff(intento)
                if intento >= self._intentos:
                    error_type = "client_error"
                    name = type(e).__name__.lower()
                    if "dns" in name:
                        error_type = "dns_error"
                    elif "ssl" in name:
                        error_type = "ssl_error"
                    return HttpFetchResult(
                        url=url,
                        final_url=url,
                        status=None,
                        body=None,
                        error_type=error_type,
                        error_detail=str(e),
                    )

        log.error("FALLO definitivo tras %s intentos: %s", self._intentos, url)
        return HttpFetchResult(
            url=url,
            final_url=url,
            status=None,
            body=None,
            error_type="unknown_error",
            error_detail="fallo definitivo",
        )

    async def get_bytes(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> bytes | None:
        """GET binario simple para PDFs/adjuntos."""
        if self._session is None:
            raise RuntimeError("HttpClient debe usarse dentro de un async with")

        headers = {"User-Agent": random.choice(USER_AGENTS)}
        if extra_headers:
            headers.update(extra_headers)

        sem = self._semaforo(url)
        intento = 0
        while intento < self._intentos:
            intento += 1
            try:
                async with sem:
                    async with self._session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", "5"))
                            await asyncio.sleep(retry_after)
                            continue
                        if resp.status in (403, 503):
                            if intento < self._intentos:
                                await self._backoff(intento)
                                continue
                            return None
                        if resp.status >= 400:
                            return None
                        return await resp.read()
            except asyncio.TimeoutError:
                await self._backoff(intento)
            except aiohttp.ClientError:
                await self._backoff(intento)
        return None

    async def _backoff(self, intento: int) -> None:
        delay = self._delay_base * (3 ** (intento - 1)) + random.uniform(0, 1)
        await asyncio.sleep(delay)


# ═══════════════════════════════════════════════════════════════════
#  BASE SCRAPER (abstract)
# ═══════════════════════════════════════════════════════════════════

@dataclass
class OfertaRaw:
    """Payload mínimo que un scraper debe producir por cada oferta encontrada."""
    url: str
    cargo: str
    institucion_nombre: str
    descripcion: str = ""
    sector: str | None = None
    tipo_cargo: str | None = None
    region: str | None = None
    ciudad: str | None = None
    renta_texto: str | None = None
    renta_min: int | None = None
    renta_max: int | None = None
    grado_eus: str | None = None
    fecha_publicacion: date | None = None
    fecha_cierre: date | None = None
    area_profesional: str | None = None
    nivel: str | None = None
    id_externo: str | None = None
    url_bases: str | None = None


class BaseScraper(abc.ABC):
    """Clase base que todos los scrapers deben extender.

    Protocolo:
        async with ScraperConcreto(fuente_id, nombre_fuente) as scraper:
            resultado = await scraper.run()

    Cada scraper concreto implementa `descubrir_ofertas()` como async iterator
    que produce `OfertaRaw`. El método `run()` del base se encarga de:
        - aplicar filtros de precisión,
        - validar vigencia,
        - deduplicar por url_hash,
        - upsertar en postgres,
        - calcular reporte de precisión,
        - cerrar ofertas que dejaron de aparecer en el scraping.
    """

    def __init__(self, fuente_id: int, nombre_fuente: str) -> None:
        self.fuente_id = fuente_id
        self.nombre_fuente = nombre_fuente
        self.report = PrecisionReport(institucion=nombre_fuente)
        self.log = setup_logging(f"scraper.{nombre_fuente.replace(' ', '_').lower()}")
        self.http: HttpClient | None = None
        self._hashes_vistos: set[str] = set()
        # Marcador: si hubo errores fatales, NO se cierran ofertas que "no aparecieron"
        self.hubo_error_fatal = False
        self._confia_en_la_fuente = False

    async def __aenter__(self) -> "BaseScraper":
        self.http = HttpClient()
        await self.http.__aenter__()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self.http is not None:
            await self.http.__aexit__(*exc)

    # ── Métodos que cada scraper concreto debe implementar ─────────

    @abc.abstractmethod
    async def descubrir_ofertas(self) -> Iterable[OfertaRaw]:
        """Devuelve una lista de OfertaRaw.

        Implementado como método async que retorna una lista (no async iterator
        para simplicidad). El scraper concreto es responsable de paginar y
        extraer campos.
        """
        raise NotImplementedError

    # ── Motor común ────────────────────────────────────────────────

    async def run(self) -> PrecisionReport:
        self.log.info("=== Inicio scraper: %s ===", self.nombre_fuente)
        inicio = time.monotonic()

        try:
            crudas = list(await self.descubrir_ofertas())
        except Exception as e:
            self.log.exception("Error fatal descubriendo ofertas: %s", e)
            self.hubo_error_fatal = True
            self.report.errores += 1
            return self.report

        self.report.total_encontradas = len(crudas)
        self.log.info("Descubiertas %s publicaciones crudas", len(crudas))

        with conexion() as conn:
            for raw in crudas:
                try:
                    self._procesar_una(conn, raw)
                except Exception as e:
                    self.log.exception("Error procesando %s: %s", raw.url, e)
                    self.report.errores += 1
                    # Recuperar el estado de la transacción para que el
                    # siguiente raw no falle con "transaction is aborted".
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            # Cerrar ofertas que NO vimos en esta corrida (post-audit 1.6)
            if not self.hubo_error_fatal:
                self._cerrar_desaparecidas(conn)

            conn.commit()

        duracion = time.monotonic() - inicio
        self.log.info(
            "=== Fin scraper: %s — %.1fs — %s ===",
            self.nombre_fuente, duracion, self.report.resumen()
        )
        return self.report

    # ── Lógica por oferta ──────────────────────────────────────────

    def _procesar_una(self, conn, raw: OfertaRaw) -> None:
        # Filtro 1: keywords
        if not self._confia_en_la_fuente:
            es_oferta, motivo, kw = es_oferta_laboral(raw.cargo, raw.descripcion)
            if not es_oferta:
                if motivo == "keyword_negativa":
                    self.report.descartadas_negativas += 1
                elif motivo == "sin_keywords":
                    self.report.descartadas_sin_keywords += 1
                self._registrar_descarte(conn, raw, motivo, kw)
                return

        # Filtro 2: vigencia
        if not es_vigente(raw.fecha_cierre, raw.fecha_publicacion):
            self.report.descartadas_vencidas += 1
            self._registrar_descarte(conn, raw, "vencida", "")
            return

        # Filtro 3: URL válida
        if not raw.url:
            return
        uhash = url_hash(raw.url)
        if uhash in self._hashes_vistos:
            self.report.descartadas_duplicadas += 1
            return
        self._hashes_vistos.add(uhash)

        # Normalizaciones
        tipo_norm = normalizar_tipo_cargo(raw.tipo_cargo)
        region_norm = normalizar_region(raw.region)

        # Renta: si el scraper no trajo valores, extraer del texto
        renta_min = raw.renta_min
        renta_max = raw.renta_max
        grado = raw.grado_eus
        if renta_min is None and renta_max is None and grado is None:
            extraida = extraer_renta(raw.renta_texto or raw.descripcion or "")
            renta_min = extraida.minimo
            renta_max = extraida.maximo
            grado = extraida.grado_eus

        # Match de institución
        inst_id = match_institucion(
            conn,
            nombre_raw=raw.institucion_nombre,
            sector=raw.sector,
            url_empleo=raw.url,
        )

        oferta_validable = {
            "institucion_id": inst_id,
            "institucion_nombre": raw.institucion_nombre,
            "cargo": raw.cargo,
            "descripcion": raw.descripcion,
            "fecha_publicacion": raw.fecha_publicacion,
            "fecha_cierre": raw.fecha_cierre,
            "url_bases": raw.url_bases,
            "renta_bruta_min": renta_min,
            "renta_bruta_max": renta_max,
            "estado": "activo",
            "activa": True,
        }
        from scrapers.evaluation.audit_store import AuditStore
        from scrapers.evaluation.models import QualityDecision
        from scrapers.evaluation.quality_validator import QualityValidator

        audit_store = AuditStore()
        validator = QualityValidator(valid_institution_ids={inst_id} if inst_id is not None else set())
        validation = validator.validate(oferta_validable)
        if inst_id is None and validation.primary_reason_code is None:
            from scrapers.evaluation.reason_codes import ReasonCode

            validation.reason_codes.append(ReasonCode.INVALID_INSTITUTION_REFERENCE)
            validation.reason_detail = (validation.reason_detail or "") + " institucion_id no pudo reconciliarse con el catalogo."
            validation.decision = QualityDecision.REJECT

        if validation.decision != QualityDecision.PUBLISH:
            self._registrar_descarte(
                conn,
                raw,
                validation.primary_reason_code.value if validation.primary_reason_code else "fecha_invalida",
                "",
            )
            audit_store.save_quality_event(
                conn,
                oferta_id=None,
                fuente_id=self.fuente_id,
                institucion_id=inst_id,
                url_oferta=raw.url,
                validation=validation,
            )
            return

        # Hash de contenido
        c_hash = contenido_hash(
            raw.cargo, raw.descripcion, raw.fecha_cierre,
            renta_min, renta_max, region_norm,
        )

        canonical = canonicalizar_url(raw.url)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ofertas (
                    fuente_id, id_externo, url_oferta, url_original, url_hash, contenido_hash,
                    cargo, descripcion, institucion_id, institucion_nombre, sector,
                    area_profesional, tipo_cargo, nivel, region, ciudad,
                    renta_bruta_min, renta_bruta_max, renta_texto, grado_eus, url_bases,
                    fecha_publicacion, fecha_cierre,
                    activa, es_nueva, detectada_en, ultima_vista_en
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    TRUE, TRUE, NOW(), NOW()
                )
                ON CONFLICT (url_hash) DO UPDATE SET
                    cargo               = EXCLUDED.cargo,
                    descripcion         = EXCLUDED.descripcion,
                    institucion_nombre  = EXCLUDED.institucion_nombre,
                    sector              = EXCLUDED.sector,
                    area_profesional    = EXCLUDED.area_profesional,
                    tipo_cargo          = EXCLUDED.tipo_cargo,
                    nivel               = EXCLUDED.nivel,
                    region              = EXCLUDED.region,
                    ciudad              = EXCLUDED.ciudad,
                    renta_bruta_min     = EXCLUDED.renta_bruta_min,
                    renta_bruta_max     = EXCLUDED.renta_bruta_max,
                    renta_texto         = EXCLUDED.renta_texto,
                    grado_eus           = EXCLUDED.grado_eus,
                    url_bases           = EXCLUDED.url_bases,
                    fecha_cierre        = EXCLUDED.fecha_cierre,
                    contenido_hash      = EXCLUDED.contenido_hash,
                    activa              = TRUE,
                    actualizada_en      = NOW(),
                    ultima_vista_en     = NOW(),
                    es_nueva            = FALSE
                RETURNING id, (xmax = 0) AS es_insert
                """,
                (
                    self.fuente_id, raw.id_externo, canonical, raw.url, uhash, c_hash,
                    raw.cargo.strip()[:500], raw.descripcion, inst_id,
                    raw.institucion_nombre.strip()[:300], raw.sector,
                    raw.area_profesional, tipo_norm, raw.nivel,
                    region_norm, raw.ciudad,
                    renta_min, renta_max, raw.renta_texto, grado, raw.url_bases,
                    raw.fecha_publicacion, raw.fecha_cierre,
                ),
            )
            oferta_id, es_insert = cur.fetchone()

        if es_insert:
            self.report.guardadas += 1
        else:
            self.report.ya_existian += 1

        audit_store.save_quality_event(
            conn,
            oferta_id=oferta_id,
            fuente_id=self.fuente_id,
            institucion_id=inst_id,
            url_oferta=canonical,
            validation=validation,
        )

    def _registrar_descarte(
        self,
        conn,
        raw: OfertaRaw,
        motivo: str,
        keyword: str,
    ) -> None:
        motivo_legacy = motivo
        if motivo_legacy not in {
            "keyword_negativa",
            "sin_keywords",
            "vencida",
            "duplicada",
            "fecha_invalida",
            "contenido_vacio",
        }:
            if "salary" in motivo_legacy or "institution" in motivo_legacy or "catalog" in motivo_legacy:
                motivo_legacy = "fecha_invalida"
            else:
                motivo_legacy = "keyword_negativa"
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scraper_descartes (
                        institucion_nombre, titulo, url, motivo, keyword_detectada
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (raw.institucion_nombre[:300], raw.cargo[:500], raw.url[:1000],
                     motivo_legacy, keyword[:100]),
                )
        except Exception:  # nunca dejes que esto rompa la corrida
            # Rollback para no dejar la transacción en estado abortado,
            # lo que envenenaría las llamadas siguientes en el mismo conn.
            try:
                conn.rollback()
            except Exception:
                pass

    def _cerrar_desaparecidas(self, conn) -> None:
        """Cierra ofertas que venían activas de esta fuente y que no vimos hoy."""
        if not self._hashes_vistos:
            return
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ofertas
                SET activa = FALSE,
                    actualizada_en = NOW(),
                    fecha_cierre_detectada = NOW()
                WHERE fuente_id = %s
                  AND activa = TRUE
                  AND url_hash IS NOT NULL
                  AND url_hash <> ALL(%s)
                """,
                (self.fuente_id, list(self._hashes_vistos)),
            )
            cerradas = cur.rowcount
        if cerradas > 0:
            self.log.info("Cerradas %s ofertas desaparecidas", cerradas)


# ═══════════════════════════════════════════════════════════════════
#  MANTENIMIENTO: limpieza de vencidas
# ═══════════════════════════════════════════════════════════════════

def limpiar_vencidas(conn) -> int:
    """Marca como inactivas las ofertas cuya fecha_cierre ya pasó.

    Post-audit 1.15 / 4.4. Devuelve el número de filas afectadas.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ofertas
            SET activa = FALSE,
                actualizada_en = NOW(),
                fecha_cierre_detectada = COALESCE(fecha_cierre_detectada, NOW())
            WHERE activa = TRUE
              AND fecha_cierre IS NOT NULL
              AND fecha_cierre < CURRENT_DATE
            """
        )
        cerradas = cur.rowcount
    conn.commit()
    log.info("limpiar_vencidas: %s ofertas marcadas como vencidas", cerradas)
    return cerradas


def generar_reporte(reports: list[PrecisionReport]) -> str:
    """Genera el cuadro textual de resultados para imprimir en consola/log."""
    if not reports:
        return "Sin instituciones procesadas."

    lineas: list[str] = []
    lineas.append("─" * 95)
    lineas.append(f"{'INSTITUCIÓN':40.40} | {'TOT':>4} | {'NUE':>4} | {'EXI':>4} | {'PREC':>7}")
    lineas.append("─" * 95)

    totales = {
        "total": 0, "nuevas": 0, "existian": 0, "descartadas": 0, "errores": 0,
    }
    revisar: list[str] = []

    for r in reports:
        lineas.append(
            f"{r.institucion:40.40} | "
            f"{r.total_encontradas:4d} | "
            f"{r.guardadas:4d} | "
            f"{r.ya_existian:4d} | "
            f"{r.tasa_precision:6.1f}%"
        )
        totales["total"] += r.total_encontradas
        totales["nuevas"] += r.guardadas
        totales["existian"] += r.ya_existian
        totales["descartadas"] += (
            r.descartadas_negativas + r.descartadas_sin_keywords + r.descartadas_vencidas
        )
        totales["errores"] += r.errores
        if r.total_encontradas >= 5 and r.tasa_precision < 70:
            revisar.append(r.institucion)

    lineas.append("─" * 95)
    lineas.append(
        f"TOTAL: {totales['total']} encontradas → {totales['nuevas']} nuevas, "
        f"{totales['existian']} existían, {totales['descartadas']} descartadas, "
        f"{totales['errores']} errores"
    )
    if revisar:
        lineas.append(f"⚠ Instituciones con precisión < 70% (revisar): {', '.join(revisar)}")
    lineas.append("─" * 95)
    return "\n".join(lineas)


# Aliases de compatibilidad para scrapers heredados / perfiles nuevos.
normalize_region = normalizar_region
normalize_tipo_contrato = normalizar_tipo_cargo


def parse_date(value: str | date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return extraer_fecha(str(value))


def parse_renta(texto: str | None) -> tuple[int | None, int | None, str | None]:
    extraida = extraer_renta(texto or "")
    return extraida.minimo, extraida.maximo, extraida.grado_eus


def truncate(value: str | None, max_len: int) -> str:
    value = value or ""
    return value if len(value) <= max_len else value[: max_len - 3] + "..."


DEFAULT_BROWSER_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
}


# ── Compatibilidad para scrapers legacy (síncronos) ───────────────────────────

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


def extract_host_like_pattern(url: str | None) -> str | None:
    """Extrae el host de una URL para matching por dominio."""
    if not url:
        return None
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return None


import tempfile  # noqa: E402  (ya importado arriba en algunos entornos)


class LegacyBaseScraper(abc.ABC):
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
            "descartadas": 0,
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
                "evento=fin scraper=%s status=%s found=%s parsed=%s nuevas=%s actualizadas=%s cerradas=%s descartadas=%s errores=%s duracion=%s",
                self.nombre,
                self.stats["status"],
                self.stats["found"],
                self.stats["parsed"],
                self.stats["nuevas"],
                self.stats["actualizadas"],
                self.stats["cerradas"],
                self.stats["descartadas"],
                self.stats["errores"],
                self.stats["duracion_seg"],
            )
        return dict(self.stats)

    def save_to_db(self, ofertas: list[dict[str, Any]]) -> dict[str, int]:
        stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "descartadas": 0}
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
                    except IntakeRejected as exc:
                        # No es un error: el intake transversal decidió descartarla.
                        cursor.execute("ROLLBACK TO SAVEPOINT oferta_sp")
                        stats["descartadas"] += 1
                        self.logger.info(
                            "evento=oferta_descartada scraper=%s url=%s motivo=%s",
                            self.nombre,
                            offer.get("url_oferta"),
                            exc.motivo,
                        )
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

        # Capa de intake transversal: descarta basura/vencidos/montos absurdos
        # y marca needs_review cuando corresponda. Cualquier scraper que use
        # BaseScraper.save_to_db hereda esta validación.
        from scrapers.intake import intake_validate_offer

        decision = intake_validate_offer(normalized)
        if decision.discard:
            self.logger.info(
                "evento=intake_descarte scraper=%s url=%s motivo=%s cargo=%s",
                self.nombre,
                normalized.get("url_oferta"),
                decision.motivo_descarte,
                truncate(normalized.get("cargo"), 80),
            )
            raise IntakeRejected(decision.motivo_descarte or "intake_rejected")
        if decision.needs_review:
            self.logger.info(
                "evento=intake_revision scraper=%s url=%s razones=%s",
                self.nombre,
                normalized.get("url_oferta"),
                ",".join(decision.review_reasons),
            )
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
            for alias in institution.get("aliases", []):
                candidates.add(alias)
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
