"""Helpers puros de formateo, sin DB ni app state.

Se movieron desde `api/main.py` para que los routers en
`api/routers/*` los consuman sin crear dependencias circulares con el
entry point.

Secciones:

- **Texto**: `_slugify`, `_truncate_text`, `_escape_attr`,
  `_descripcion_a_parrafos_html`, `_fold_institution_name`.
- **Fechas y montos**: `dias_restantes`, `_format_fecha_larga`,
  `_format_renta_bruta`.
- **Email**: `EMAIL_RE`, `validate_email`.
- **Catálogo de instituciones**: `_extract_root_domain`,
  `_load_sitio_web_map`, `resolve_institucion_sitio_web`. Usan una
  caché por mtime del catálogo JSON.
"""
from __future__ import annotations

import html
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import HTTPException


# ── Regex y constantes ────────────────────────────────────────────────────

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CATALOG_PATH = _PROJECT_ROOT / "repositorio_instituciones_publicas_chile.json"

#: Dominios que son *portales intermediarios* (empleospublicos.cl, etc.)
#: y por eso no representan el sitio oficial de la institución. Se
#: excluyen de la resolución del dominio oficial para que
#: `logo.clearbit.com` no termine buscando el logo de un portal.
_PORTAL_DOMAINS_LOWER = {
    "empleospublicos.cl", "www.empleospublicos.cl",
    "trabajando.com", "www.trabajando.com",
    "trabajando.cl", "www.trabajando.cl",
    "hiringroom.com", "www.hiringroom.com",
    "buk.cl", "www.buk.cl",
    "chileatiende.cl", "www.chileatiende.cl",
    "empleos.gob.cl", "www.empleos.gob.cl",
    "postulaciones.cl", "www.postulaciones.cl",
    "sistemadeconcursos.cl", "www.sistemadeconcursos.cl",
    "mitrabajodigno.cl", "www.mitrabajodigno.cl",
    "ucampus.net", "www.ucampus.net",
}

#: Caché en memoria del catálogo JSON (sitio web oficial por institución).
_sitio_web_cache: dict[str, Any] = {"mtime": 0.0, "by_name": {}, "by_id": {}}


# ── Texto ─────────────────────────────────────────────────────────────────

def _fold_institution_name(value: str | None) -> str:
    if not value:
        return ""
    import unicodedata
    folded = unicodedata.normalize("NFD", value)
    folded = "".join(ch for ch in folded if unicodedata.category(ch) != "Mn")
    folded = re.sub(r"[^a-zA-Z0-9\s]", " ", folded.lower())
    folded = re.sub(r"\s+", " ", folded).strip()
    return folded


def _slugify(value: str | None, max_len: int = 80) -> str:
    """Genera un slug URL-safe a partir de texto libre.

    Se usa para construir URLs de ofertas en el sitemap:
    ``/oferta/{id}-{slug}``. El ``id`` mantiene la unicidad; el slug
    solo añade valor semántico para SEO y para humanos que lean el URL.
    """
    if not value:
        return ""
    import unicodedata
    folded = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    folded = re.sub(r"[^a-zA-Z0-9]+", "-", folded).strip("-").lower()
    return folded[:max_len].rstrip("-")


def _truncate_text(value: str, max_len: int) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip(" ,.-") + "…"


def _escape_attr(value: str) -> str:
    return html.escape(value, quote=True)


def _descripcion_a_parrafos_html(texto: str, max_chars: int = 2000) -> str:
    """Convierte un texto plano de descripción en ``<p>`` escapados.

    No asumimos HTML en entrada — el scraper guarda descripción como
    texto plano y si llegara con tags los escapamos literalmente (más
    seguro que renderizarlos). Corta a ``max_chars`` y agrupa oraciones
    en párrafos de hasta ~400 caracteres para legibilidad.
    """
    if not texto:
        return ""
    texto = re.sub(r"\s+", " ", texto).strip()
    if len(texto) > max_chars:
        texto = texto[: max_chars - 1].rstrip(" ,.-") + "…"
    parrafos: list[str] = []
    buffer = ""
    for oracion in re.split(r"(?<=[.!?])\s+", texto):
        if not oracion:
            continue
        if buffer and len(buffer) + len(oracion) > 400:
            parrafos.append(buffer.strip())
            buffer = oracion
        else:
            buffer = (buffer + " " + oracion).strip() if buffer else oracion
    if buffer:
        parrafos.append(buffer.strip())
    return "".join(f"<p>{html.escape(p)}</p>" for p in parrafos)


# ── Fechas y rentas ───────────────────────────────────────────────────────

def dias_restantes(value: date | None) -> int | None:
    if value is None:
        return None
    return (value - date.today()).days


def _format_fecha_larga(value: date | None) -> str | None:
    if value is None:
        return None
    meses = (
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    )
    return f"{value.day} de {meses[value.month - 1]} de {value.year}"


def _format_renta_bruta(oferta: dict[str, Any]) -> str | None:
    rmin = oferta.get("renta_bruta_min")
    rmax = oferta.get("renta_bruta_max")
    if isinstance(rmin, int) and isinstance(rmax, int) and rmin > 0 and rmax > 0:
        if rmin == rmax:
            return f"${rmin:,.0f}".replace(",", ".")
        return f"${rmin:,.0f}".replace(",", ".") + " a " + f"${rmax:,.0f}".replace(",", ".")
    if isinstance(rmax, int) and rmax > 0:
        return f"Hasta ${rmax:,.0f}".replace(",", ".")
    if isinstance(rmin, int) and rmin > 0:
        return f"Desde ${rmin:,.0f}".replace(",", ".")
    return None


# ── Email ─────────────────────────────────────────────────────────────────

def validate_email(email: str) -> str:
    value = email.strip().lower()
    if not EMAIL_RE.match(value):
        raise HTTPException(status_code=422, detail="Email invalido")
    return value


# ── Resolución de sitio web oficial por institución ───────────────────────
# El catálogo JSON contiene `sitio_web` incluso cuando `url_empleo`
# apunta al portal intermediario. Esa info no vive en la tabla
# `instituciones`, así que la cargamos en memoria y la cacheamos por
# mtime.

def _extract_root_domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        host = urlparse(url if "://" in url else f"https://{url}").hostname or ""
    except Exception:
        return None
    host = host.strip().lower().lstrip(".")
    if not host:
        return None
    if host in _PORTAL_DOMAINS_LOWER:
        return None
    # Remueve www. para que logo.clearbit.com tenga mejor hit rate.
    return host[4:] if host.startswith("www.") else host


def _load_sitio_web_map() -> dict[str, Any]:
    """Mapea nombre normalizado de institución → dominio oficial (sitio_web)."""
    if not _CATALOG_PATH.exists():
        return _sitio_web_cache
    try:
        mtime = _CATALOG_PATH.stat().st_mtime
    except OSError:
        return _sitio_web_cache
    if _sitio_web_cache["mtime"] == mtime and _sitio_web_cache["by_name"]:
        return _sitio_web_cache
    try:
        payload = json.loads(_CATALOG_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        return _sitio_web_cache
    insts = payload.get("instituciones") if isinstance(payload, dict) else payload
    if not isinstance(insts, list):
        return _sitio_web_cache
    by_name: dict[str, str] = {}
    by_id: dict[int, str] = {}
    for inst in insts:
        domain = _extract_root_domain(inst.get("sitio_web"))
        if not domain:
            continue
        nombre = inst.get("nombre")
        if nombre:
            key = _fold_institution_name(nombre)
            if key:
                by_name.setdefault(key, domain)
        sigla = inst.get("sigla")
        if sigla:
            key_sigla = _fold_institution_name(sigla)
            if key_sigla:
                by_name.setdefault(key_sigla, domain)
        inst_id = inst.get("id")
        if isinstance(inst_id, int):
            by_id.setdefault(inst_id, domain)
    _sitio_web_cache["mtime"] = mtime
    _sitio_web_cache["by_name"] = by_name
    _sitio_web_cache["by_id"] = by_id
    return _sitio_web_cache


def resolve_institucion_sitio_web(
    institucion: str | None, institucion_id: int | None = None
) -> str | None:
    """Devuelve el dominio oficial (sin esquema) de la institución o None.

    Estrategia:
      1. Match por ``institucion_id`` en el catálogo (más preciso).
      2. Match por nombre normalizado.
      3. Match por contención parcial (ej. "Municipalidad de X" contiene
         una entrada del catálogo).
    """
    cache = _load_sitio_web_map()
    by_id = cache.get("by_id") or {}
    by_name = cache.get("by_name") or {}
    if isinstance(institucion_id, int) and institucion_id in by_id:
        return by_id[institucion_id]
    key = _fold_institution_name(institucion)
    if not key:
        return None
    if key in by_name:
        return by_name[key]
    # Match por contención: escoger la entrada del catálogo con la clave
    # más larga contenida en el nombre consultado (evita falsos
    # positivos cortos).
    best: tuple[int, str] | None = None
    for catalog_key, domain in by_name.items():
        if len(catalog_key) < 10:
            continue
        if catalog_key in key:
            if best is None or len(catalog_key) > best[0]:
                best = (len(catalog_key), domain)
    return best[1] if best else None
