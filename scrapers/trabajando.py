"""
EmpleoEstado.cl — Scraper genérico para portales Trabajando.cl (v2, API-first)

Cubre cualquier institución cuyo `url_empleo` sea un subdominio de
`trabajando.cl` (FACH, UMAG, UTalca, ENAP, BancoEstado, Metro, Correos,
ZOFRI, etc.) más las fuentes extra registradas manualmente (UCH externo).

Estrategia (en orden):
    1. API interna del portal (todos los portales *.trabajando.cl comparten
       la misma plataforma Nuxt):
         a. Autodetectar `idDominio` decodificando el payload SSR
            (__NUXT_DATA__, formato devalue) de /trabajo-empleo/ o /.
         b. GET /api/searchjob (paginado) -> listado completo de ofertas.
         c. GET /api/ofertas/{idOferta}  -> detalle (descripción, requisitos,
            sueldo, fecha de cierre, vacantes). Omitible con --sin-detalle.
       Datos estructurados: sin heurísticas, con paginación real.
    2. Fallback heurístico HTML (+ Playwright si está instalado) solo si la
       API no responde: parseo de enlaces /ficha|oferta|empleo/, tablas y
       cards. Es el comportamiento del scraper v1, conservado por si algún
       portal corre una versión legacy de la plataforma.

Uso:
    python scrapers/trabajando.py                      # todas las fuentes
    python scrapers/trabajando.py --dry-run --verbose
    python scrapers/trabajando.py --id 242             # solo una fuente (id)
    python scrapers/trabajando.py --host externouchile # solo una fuente (host)
    python scrapers/trabajando.py --sin-detalle        # solo listado (rápido)
    python scrapers/trabajando.py --export salida      # además CSV+JSON
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
import sys
import time
from datetime import date, datetime
from html import unescape
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import requests

# ── Integración con el proyecto (con fallback standalone) ───────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from config import config
    from db.database import (
        SessionLocal,
        generar_id_estable,
        limpiar_texto,
        marcar_ofertas_cerradas,
        normalizar_area,
        normalizar_region,
        registrar_log,
        upsert_oferta,
    )
    STANDALONE = False
except ImportError:  # permite ejecutar el archivo suelto (solo dry-run/export)
    STANDALONE = True

    class _Cfg:
        LOG_DIR = "logs"
        LOG_LEVEL = "INFO"
        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        ]

    config = _Cfg()  # type: ignore[assignment]
    SessionLocal = None  # type: ignore[assignment]

    def generar_id_estable(*partes: Any) -> str:  # type: ignore[misc]
        import hashlib
        return hashlib.sha1("||".join(str(p) for p in partes).encode()).hexdigest()[:24]

    def limpiar_texto(t: str | None) -> str:  # type: ignore[misc]
        return re.sub(r"\s+", " ", t or "").strip()

    def normalizar_area(cargo: str) -> str | None:  # type: ignore[misc]
        return None

    def normalizar_region(r: str) -> str:  # type: ignore[misc]
        return r.title()

    def marcar_ofertas_cerradas(*a: Any, **k: Any) -> int:  # type: ignore[misc]
        return 0

    def registrar_log(*a: Any, **k: Any) -> None:  # type: ignore[misc]
        return None

    def upsert_oferta(*a: Any, **k: Any) -> tuple[bool, bool]:  # type: ignore[misc]
        return False, False

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
except ImportError:
    sync_playwright = None  # type: ignore[assignment,misc]
    PwTimeout = Exception  # type: ignore[assignment,misc]

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.trabajando")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "trabajando.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

REPO_PATH = Path(__file__).resolve().parents[1] / "repositorio_instituciones_publicas_chile.json"

HTTP_TIMEOUT = 25
PAGE_LOAD_TIMEOUT = 30_000
API_MAX_RETRIES = 3
API_DELAY = 0.5  # segundos entre requests a la API (sobreescribible con --delay)

_PATH_CANDIDATES = (
    "", "/trabajo-empleo", "/empleos", "/empleos/buscar-empleos",
    "/buscar-empleos", "/ofertas-laborales",
)

_KEYWORDS_OFERTA = (
    "cargo", "vacante", "oferta", "empleo", "postul",
    "profesional", "analista", "ingenier", "coordinador", "coordinadora",
    "asistente", "tecnico", "técnico", "academico", "académico", "docente",
    "administrativo", "administrativa", "secretario", "secretaria",
    "auxiliar", "encargado", "encargada", "jefe", "jefa",
    "investigador", "investigadora", "asesor", "asesora",
    "operario", "operaria", "operador", "operadora",
    "ejecutivo", "ejecutiva", "supervisor", "supervisora",
)

_DETAIL_HREF_RE = re.compile(r"(ficha|oferta|empleo|job[-_ ]?offer|postul)", re.I)

_EXTRA_SOURCES: list[dict[str, Any]] = [
    {
        "id": 242,
        "nombre": "Universidad de Chile (Portal Externo)",
        "sigla": "UCH",
        "sector": "Universidad/Educación",
        "region": "Nacional",
        "url_empleo": "https://externouchile.trabajando.cl/",
    },
]


# ── Fuentes ──────────────────────────────────────────────────────────────────
def _load_all_instituciones(path: Path = REPO_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload.get("instituciones") if isinstance(payload, dict) else payload


def _filter_trabajando(
    instituciones: list[dict[str, Any]],
    solo_id: int | None = None,
    solo_host: str | None = None,
) -> list[dict[str, Any]]:
    candidatos: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for fuente in list(instituciones) + _EXTRA_SOURCES:
        url = (fuente.get("url_empleo") or "").lower()
        if "trabajando.cl" not in url:
            continue
        fid = fuente.get("id")
        if fid in seen_ids:
            continue
        seen_ids.add(fid)
        candidatos.append(fuente)
    if solo_id is not None:
        candidatos = [c for c in candidatos if c.get("id") == solo_id]
    if solo_host:
        h = solo_host.lower().replace("https://", "").replace("http://", "").rstrip("/")
        candidatos = [c for c in candidatos if h in (c.get("url_empleo") or "").lower()]
    return candidatos


def _base_url(fuente: dict[str, Any]) -> str:
    url = (fuente.get("url_empleo") or "").strip()
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return url.rstrip("/")


# ═════════════════════════════════════════════════════════════════════════════
# ESTRATEGIA 1: API interna de la plataforma Trabajando.cl
# ═════════════════════════════════════════════════════════════════════════════
def _api_headers(base: str) -> dict[str, str]:
    return {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
        "Referer": base + "/trabajo-empleo/",
    }


def _api_get(session: requests.Session, url: str, **kwargs: Any) -> requests.Response:
    """GET con reintentos y backoff exponencial."""
    for intento in range(1, API_MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if intento == API_MAX_RETRIES:
                raise
            espera = 2 ** intento
            logger.info("  API reintento %d/%d (%s) en %ds",
                        intento, API_MAX_RETRIES, type(exc).__name__, espera)
            time.sleep(espera)
    raise RuntimeError("unreachable")


def _decodificar_devalue(data: list, i: int, profundidad: int = 0) -> Any:
    """Decodifica el formato devalue del payload __NUXT_DATA__ de Nuxt."""
    if profundidad > 30:
        return None
    v = data[i]
    if isinstance(v, list):
        if v and isinstance(v[0], str) and v[0] in (
            "Reactive", "Ref", "ShallowRef", "EmptyRef", "ShallowReactive"
        ):
            return _decodificar_devalue(data, v[1], profundidad + 1)
        return [_decodificar_devalue(data, x, profundidad + 1) for x in v]
    if isinstance(v, dict):
        return {k: _decodificar_devalue(data, ix, profundidad + 1) for k, ix in v.items()}
    return v


def _detectar_id_dominio(session: requests.Session, base: str) -> int | None:
    """Extrae el idDominio del portal desde el payload SSR (devalue)."""
    for path in ("/trabajo-empleo/", "/"):
        try:
            r = _api_get(session, base + path)
        except requests.RequestException:
            continue
        m = re.search(r'id="__NUXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
        if not m:
            continue
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        # Preferir el objeto parametrosBusqueda (contiene palabraClave)
        for i, v in enumerate(data):
            if isinstance(v, dict) and "idDominio" in v and "palabraClave" in v:
                params = _decodificar_devalue(data, i)
                if isinstance(params.get("idDominio"), int) and params["idDominio"] > 0:
                    return params["idDominio"]
        for i, v in enumerate(data):
            if isinstance(v, dict) and "idDominio" in v:
                obj = _decodificar_devalue(data, i)
                if isinstance(obj.get("idDominio"), int) and obj["idDominio"] > 0:
                    return obj["idDominio"]
    return None


def _api_listado(
    session: requests.Session, base: str, id_dominio: int, delay: float,
    max_results: int | None,
) -> list[dict]:
    """Recorre todas las páginas de /api/searchjob."""
    ofertas: list[dict] = []
    pagina, total_paginas = 1, 1
    while pagina <= total_paginas:
        params = {
            "idDominio": id_dominio,
            "pagina": pagina,
            "orden": "FECHA_PUBLICACION",
            "tipoOrden": "DESC",
            "ofertaConfidencial": "false",
        }
        d = _api_get(session, base + "/api/searchjob", params=params).json()
        total_paginas = d.get("cantidadPaginas", 1) or 1
        lote = d.get("ofertas", []) or []
        ofertas.extend(lote)
        logger.info("  API página %d/%d: %d ofertas", pagina, total_paginas, len(lote))
        if max_results and len(ofertas) >= max_results:
            return ofertas[:max_results]
        pagina += 1
        time.sleep(delay)
    return ofertas


def _api_detalle(
    session: requests.Session, base: str, id_oferta: int, id_dominio: int,
) -> dict | None:
    try:
        r = _api_get(
            session, f"{base}/api/ofertas/{id_oferta}",
            headers={"iddominio": str(id_dominio)},
        )
        d = r.json()
        return d if isinstance(d, dict) and d.get("idOferta") else None
    except (requests.RequestException, json.JSONDecodeError):
        return None


def _html_a_texto(texto: str | None) -> str:
    """HTML de la API -> texto plano legible."""
    if not texto:
        return ""
    texto = re.sub(r"</(p|li|ul|ol|div|br|h[1-6])>", "\n", texto, flags=re.I)
    texto = re.sub(r"<li[^>]*>", "- ", texto, flags=re.I)
    texto = re.sub(r"<[^>]+>", "", texto)
    texto = unescape(texto)
    return re.sub(r"\n{3,}", "\n\n", texto).strip()


def _parse_fecha(valor: str | None) -> date | None:
    if not valor:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(valor.strip()[:16], fmt).date()
        except ValueError:
            continue
    return None


def _parse_ubicacion(ubicacion: str | None) -> tuple[str | None, str | None]:
    """'Providencia, Metropolitana de Santiago' -> (ciudad, region)."""
    if not ubicacion:
        return None, None
    partes = [p.strip() for p in ubicacion.split(",") if p.strip()]
    if len(partes) >= 2:
        return partes[0], normalizar_region(partes[-1])
    if len(partes) == 1:
        return None, normalizar_region(partes[0])
    return None, None


_RENTA_BRUTA_RE = re.compile(
    r"(?:renta|remuneraci[oó]n|sueldo)[^$\n]{0,40}brut[ao][^$\n]{0,40}\$\s*([\d.]{5,12})", re.I)
_RENTA_LIQUIDA_RE = re.compile(
    r"(?:renta|remuneraci[oó]n|sueldo)[^$\n]{0,40}l[ií]quid[ao][^$\n]{0,40}\$\s*([\d.]{5,12})", re.I)
# "Renta: $1.200.000" / "sueldo de $950.000" sin calificativo: se asume bruta,
# que es la convención de los avisos (ventana corta para evitar montos ajenos).
_RENTA_SIMPLE_RE = re.compile(
    r"(?:renta|remuneraci[oó]n|sueldo)(?:\s+(?:mensual|ofrecid[ao]|imponible))?"
    r"\s*(?:de|:)?\s*\$\s*([\d.]{5,12})", re.I)


def _monto_pesos(raw: Any) -> int | None:
    """Limpia y valida un monto en CLP (cordura: 300 mil – 30 millones)."""
    try:
        valor = int(str(raw).replace(".", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if 300_000 <= valor <= 30_000_000:
        return valor
    return None


def _extraer_renta_bruta(texto: str) -> int | None:
    m = _RENTA_BRUTA_RE.search(texto or "")
    if m:
        return _monto_pesos(m.group(1))
    return None


def _extraer_renta(texto: str) -> tuple[int | None, str | None]:
    """(renta_bruta, renta_texto) desde la descripción del aviso.

    - Renta bruta explícita → renta_bruta.
    - Renta líquida → solo renta_texto (no se reporta como bruta).
    - Renta sin calificativo → renta_bruta (convención de avisos).
    """
    t = texto or ""
    bruta = _extraer_renta_bruta(t)
    if bruta:
        return bruta, None
    m = _RENTA_LIQUIDA_RE.search(t)
    if m:
        valor = _monto_pesos(m.group(1))
        if valor:
            texto_renta = f"Renta líquida ${valor:,.0f}".replace(",", ".")
            return None, texto_renta
    m = _RENTA_SIMPLE_RE.search(t)
    if m:
        valor = _monto_pesos(m.group(1))
        if valor:
            return valor, None
    return None, None


_NIVEL_API = {
    "directivo": "Directivo", "jefatura": "Directivo", "gerencia": "Directivo",
    "profesional": "Profesional", "técnico": "Técnico", "tecnico": "Técnico",
    "administrativo": "Técnico", "operario": "Técnico", "auxiliar": "Técnico",
}


def _construir_oferta_api(
    item: dict, detalle: dict | None, base: str, fuente: dict[str, Any],
) -> dict:
    """Mapea oferta de la API al esquema de BD (mismas columnas que v1)."""
    fuente_id = int(fuente["id"])
    nombre = fuente.get("nombre") or ""
    id_oferta = item.get("idOferta")
    cargo = limpiar_texto(item.get("nombreCargo") or "")[:500]

    slug = re.sub(r"[^a-z0-9]+", "-", cargo.lower()).strip("-")
    url = f"{base}/trabajo/{id_oferta}-{slug}"

    descripcion = ""
    requisitos = None
    fecha_cierre = None
    renta_texto = None
    renta_bruta = None
    nivel_api = None
    if detalle:
        descripcion = _html_a_texto(detalle.get("descripcionOferta"))
        requisitos = _html_a_texto(detalle.get("requisitosMinimos")) or None
        fecha_cierre = _parse_fecha(detalle.get("fechaExpiracion"))
        if detalle.get("mostrarSueldo") and detalle.get("sueldo"):
            renta_texto = f"${detalle['sueldo']:,.0f}".replace(",", ".")
            if detalle.get("nombreMoneda"):
                renta_texto += f" ({detalle['nombreMoneda']})"
            # El sueldo numérico de la API también es dato estructurado:
            # poblar renta_bruta cuando la moneda es CLP (o no se indica).
            moneda = (detalle.get("nombreMoneda") or "").strip().lower()
            if moneda in ("", "peso", "pesos", "peso chileno", "pesos chilenos", "clp"):
                renta_bruta = _monto_pesos(detalle["sueldo"])
        if renta_bruta is None:
            renta_bruta, renta_desc_texto = _extraer_renta(descripcion)
            renta_texto = renta_texto or renta_desc_texto
        tipo_api = (detalle.get("nombreTipoCargo") or "").lower()
        nivel_api = next((v for k, v in _NIVEL_API.items() if k in tipo_api), None)
    if not descripcion:
        descripcion = limpiar_texto(item.get("descripcionOferta") or "")

    ciudad, region = _parse_ubicacion(item.get("ubicacion"))
    contexto = f"{descripcion}\n{requisitos or ''}"

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, str(id_oferta)),
        "institucion_id": fuente_id,  # id del catalogo de instituciones
        "fuente_id": None,            # FK a fuentes: no aplica a scraper standalone
        "url_original": url,
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": fuente.get("sector") or None,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _detectar_tipo_cargo(contexto) or "Código del Trabajo",
        "nivel": nivel_api or _detectar_nivel(cargo),
        "region": region or _detectar_region(contexto) or fuente.get("region") or "Nacional",
        "ciudad": ciudad or _detectar_ciudad(contexto),
        "renta_bruta_min": renta_bruta,
        "renta_bruta_max": None,
        "renta_texto": renta_texto,
        "fecha_publicacion": _parse_fecha(item.get("fechaPublicacion")) or date.today(),
        "fecha_cierre": fecha_cierre or _extraer_fecha_cierre(contexto),
        "requisitos_texto": requisitos,
    }


def _recolectar_via_api(
    fuente: dict[str, Any], max_results: int | None,
    con_detalle: bool, delay: float,
) -> list[dict] | None:
    """Devuelve ofertas vía API, o None si la API no está disponible."""
    base = _base_url(fuente)
    session = requests.Session()
    session.headers.update(_api_headers(base))

    id_dominio = _detectar_id_dominio(session, base)
    if not id_dominio:
        logger.info("  No se detectó idDominio en %s; se usará fallback HTML", base)
        return None
    logger.info("  idDominio=%d", id_dominio)

    try:
        items = _api_listado(session, base, id_dominio, delay, max_results)
    except requests.RequestException as exc:
        logger.info("  API listado falló (%s); fallback HTML", type(exc).__name__)
        return None

    ofertas: list[dict] = []
    for i, item in enumerate(items, 1):
        detalle = None
        if con_detalle:
            detalle = _api_detalle(session, base, item.get("idOferta"), id_dominio)
            if detalle is None:
                logger.info("  Sin detalle para oferta %s", item.get("idOferta"))
            time.sleep(delay)
        ofertas.append(_construir_oferta_api(item, detalle, base, fuente))
        if i % 20 == 0:
            logger.info("  Detalles procesados: %d/%d", i, len(items))
    return ofertas


# ═════════════════════════════════════════════════════════════════════════════
# ESTRATEGIA 2 (fallback): parseo heurístico HTML + Playwright (scraper v1)
# ═════════════════════════════════════════════════════════════════════════════
def _http_get(url: str) -> str | None:
    headers = {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
        resp.encoding = resp.encoding or "utf-8"
        if resp.status_code >= 400:
            logger.info("  HTTP %s en %s", resp.status_code, url)
            return None
        return resp.text
    except Exception as exc:
        logger.info("  HTTP fallo %s: %s", url, type(exc).__name__)
        return None


def _pw_get(url: str) -> str | None:
    if sync_playwright is None:
        return None
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            user_agent=random.choice(config.USER_AGENTS),
            locale="es-CL",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)
            for selector in (
                "a[href*='ficha']", "a[href*='empleo']", "a[href*='oferta']",
                "[class*='oferta']", "[class*='job']", "table tbody tr", "main",
            ):
                try:
                    page.wait_for_selector(selector, timeout=2500)
                    if page.locator(selector).count() > 0:
                        break
                except PwTimeout:
                    continue
            else:
                page.wait_for_timeout(4000)
            for _ in range(3):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)
            return page.content()
        except Exception as exc:
            logger.info("  Playwright error %s: %s", url, type(exc).__name__)
            return None
        finally:
            context.close()
            browser.close()


def _fetch_html(url: str) -> str | None:
    html = _http_get(url)
    if html and _tiene_indicios_de_ofertas(html):
        return html
    logger.info("  HTML HTTP sin indicios, probando Playwright para %s", url)
    return _pw_get(url) or html


def _tiene_indicios_de_ofertas(html: str) -> bool:
    lower = html.lower()
    if len(lower) < 1500:
        return False
    hits = sum(1 for kw in ("ficha", "oferta", "empleo", "postul", "cargo", "vacante")
               if kw in lower)
    return hits >= 2


def parsear_html(html: str, url_fuente: str, allowed_host: str) -> list[tuple]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    ofertas: list[tuple] = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        if not _DETAIL_HREF_RE.search(href):
            continue
        url_abs = urljoin(url_fuente, href)
        parsed = urlparse(url_abs)
        if parsed.netloc and allowed_host not in parsed.netloc:
            continue
        if parsed.path.rstrip("/") in {"", "/empleos", "/ofertas", "/ficha-de-empleo"}:
            continue
        titulo = limpiar_texto(a.get_text(" ", strip=True))
        contenedor = a.find_parent(["article", "li", "tr", "div", "section"])
        contexto = limpiar_texto(contenedor.get_text(" ", strip=True)) if contenedor else titulo
        if not titulo:
            titulo = contexto[:200]
        if not _parece_oferta(titulo, contexto):
            continue
        ofertas.append((titulo, contexto, url_abs))

    if not ofertas:
        for row in soup.select("table tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            texto = limpiar_texto(row.get_text(" ", strip=True))
            if not _parece_oferta(texto, texto):
                continue
            link = row.find("a", href=True)
            href = link["href"] if link else ""
            url_abs = urljoin(url_fuente, href) if href else url_fuente
            cargo = limpiar_texto(cells[0].get_text(" ", strip=True)) or texto[:200]
            ofertas.append((cargo, texto, url_abs))

    if not ofertas:
        for sel in ("[class*='oferta']", "[class*='job']", "[class*='card']", "article"):
            encontrados: list[tuple] = []
            for node in soup.select(sel):
                texto = limpiar_texto(node.get_text(" ", strip=True))
                if not _parece_oferta(texto, texto):
                    continue
                titulo_el = node.select_one("h1, h2, h3, h4, h5, .title, [class*='title']")
                cargo = (limpiar_texto(titulo_el.get_text(" ", strip=True))
                         if titulo_el else texto[:200])
                link = node.find("a", href=True)
                href = link["href"] if link else ""
                url_abs = urljoin(url_fuente, href) if href else url_fuente
                encontrados.append((cargo, texto, url_abs))
            if encontrados:
                ofertas = encontrados
                break

    return _deduplicar(ofertas)


def _parece_oferta(titulo: str, contexto: str) -> bool:
    total = f"{titulo} {contexto}".lower()
    if len(total) < 15:
        return False
    return any(kw in total for kw in _KEYWORDS_OFERTA)


def _construir_oferta_html(cargo: str, contexto: str, url: str, fuente: dict[str, Any]) -> dict:
    """Mapeo del fallback HTML al mismo esquema de columnas."""
    cargo_limpio = (cargo or "").strip()[:500]
    fuente_id = int(fuente["id"])
    nombre = fuente.get("nombre") or ""
    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo_limpio, contexto[:300]),
        "institucion_id": fuente_id,  # id del catalogo de instituciones
        "fuente_id": None,            # FK a fuentes: no aplica a scraper standalone
        "url_original": url,
        "cargo": cargo_limpio,
        "descripcion": contexto[:2000] if len(contexto) > 30 else None,
        "institucion_nombre": nombre,
        "sector": fuente.get("sector") or None,
        "area_profesional": normalizar_area(cargo_limpio),
        "tipo_cargo": _detectar_tipo_cargo(contexto) or "Código del Trabajo",
        "nivel": _detectar_nivel(cargo_limpio),
        "region": _detectar_region(contexto) or fuente.get("region") or "Nacional",
        "ciudad": _detectar_ciudad(contexto),
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": date.today(),
        "fecha_cierre": _extraer_fecha_cierre(contexto),
        "requisitos_texto": None,
    }


# ── Heurísticas compartidas ─────────────────────────────────────────────────
# Calidad contractual: solo menciones EXPLÍCITAS. En trabajando.cl publican
# principalmente empresas del estado (Código del Trabajo); sus avisos hablan
# de "planta de revisión", "planta industrial" o "contratación", que NO son
# calidades jurídicas. Sin matching por substring.
_TIPO_CONTRATA_RE = re.compile(
    r"\ba\s+contrata\b"
    r"|\bcalidad\s+(?:jur[ií]dica|contractual)?\s*:?\s*(?:de\s+|a\s+|la\s+)?contrata\b"
    r"|\b(?:v[ií]nculo|estamento|condici[oó]n)\s*:?\s*(?:a\s+|de\s+|la\s+)?contrata\b"
    r"|\btipo\s+de\s+(?:contrato|cargo|vacante|v[ií]nculo)\s*:?\s*contrata\b"
    r"|\bcontrata\s+(?:asimilad|grado)",
    re.I,
)
_TIPO_PLANTA_RE = re.compile(
    r"\b(?:cargos?|empleos?|titular)\s+(?:de\s+|a\s+|en\s+(?:la\s+)?)planta\b"
    r"|\bcalidad\s+(?:jur[ií]dica|contractual)?\s*:?\s*(?:de\s+|la\s+)?planta\b"
    r"|\b(?:v[ií]nculo|estamento|condici[oó]n)\s*:?\s*(?:de\s+|la\s+)?planta\b"
    r"|\btipo\s+de\s+(?:contrato|cargo|vacante|v[ií]nculo)\s*:?\s*planta\b"
    r"|\bplanta\s+(?:directiv|profesional|t[eé]cnic|administrativ|auxiliar|fiscalizador|municipal|titular)"
    r"|\ba\s+planta\b",
    re.I,
)


def _detectar_tipo_cargo(texto: str) -> str | None:
    t = texto or ""
    if re.search(r"\bc[oó]d(?:igo|\.)?\s+(?:del\s+)?trabajo\b", t, re.I):
        return "Código del Trabajo"
    if re.search(r"\bhonorarios?\b", t, re.I):
        return "Honorarios"
    if _TIPO_CONTRATA_RE.search(t):
        return "Contrata"
    if _TIPO_PLANTA_RE.search(t):
        return "Planta"
    if re.search(r"\b(?:reemplazo|suplencia)\b", t, re.I):
        return "Reemplazo"
    return None


def _detectar_nivel(cargo: str) -> str:
    c = cargo.lower()
    if any(w in c for w in ("decano", "decana", "director", "directora",
                            "vicerrector", "gerente", "subgerente")):
        return "Directivo"
    if any(w in c for w in ("jefe", "jefa", "coordinador", "coordinadora",
                            "supervisor", "supervisora")):
        return "Profesional"
    if any(w in c for w in ("tecnico", "técnico", "auxiliar", "operador",
                            "operario", "administrativo")):
        return "Técnico"
    return "Profesional"


def _detectar_region(texto: str) -> str | None:
    t = texto.lower()
    if not t:
        return None
    rm_cues = ("santiago", "metropolitana", "providencia", "ñuñoa", "nunoa",
               "independencia", "estacion central", "estación central",
               "las condes", "maipu", "maipú")
    if any(x in t for x in rm_cues):
        return "Metropolitana de Santiago"
    for region in ("arica", "tarapaca", "antofagasta", "atacama", "coquimbo",
                   "valparaiso", "valparaíso", "ohiggins", "maule",
                   "nuble", "ñuble", "biobio", "biobío", "araucania",
                   "araucanía", "los rios", "los ríos", "los lagos",
                   "aysen", "aysén", "magallanes", "punta arenas", "talca"):
        if region in t:
            return normalizar_region(region)
    return None


def _detectar_ciudad(texto: str) -> str | None:
    t = texto.lower()
    for ciudad in ("santiago", "providencia", "ñuñoa", "independencia",
                   "estación central", "antofagasta", "valparaíso",
                   "viña del mar", "concepción", "temuco", "valdivia",
                   "puerto montt", "punta arenas", "talca", "iquique"):
        if ciudad in t:
            return ciudad.title()
    return None


def _extraer_fecha_cierre(texto: str) -> date | None:
    m = re.findall(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})\b", texto)
    if m:
        try:
            d, mo, y = m[-1]
            y = int(y)
            if y < 100:
                y += 2000
            return date(y, int(mo), int(d))
        except ValueError:
            pass
    return None


def _deduplicar(items: Iterable[Any]) -> list[Any]:
    vistos: set[str] = set()
    resultado: list[Any] = []
    for item in items:
        if isinstance(item, tuple):
            clave = item[2] + "||" + (item[0] or "")[:50]
        else:
            clave = item["url_original"] + "||" + item["cargo"][:50]
        if clave in vistos:
            continue
        vistos.add(clave)
        resultado.append(item)
    return resultado


# ── Procesamiento por fuente ────────────────────────────────────────────────
def _recolectar_fuente(
    fuente: dict[str, Any], max_results: int | None,
    con_detalle: bool, delay: float,
) -> tuple[list[dict], str]:
    """Devuelve (ofertas, metodo). API primero; HTML heurístico como fallback."""
    ofertas = _recolectar_via_api(fuente, max_results, con_detalle, delay)
    if ofertas is not None:
        return ofertas, "api"

    base = _base_url(fuente)
    if not base:
        return [], "ninguno"
    host = urlparse(base).netloc
    vistos: set[str] = set()
    resultado: list[dict] = []
    for path in _PATH_CANDIDATES:
        url = base + path
        html = _fetch_html(url)
        if not html:
            continue
        tuplas = parsear_html(html, url, allowed_host=host)
        nuevos = 0
        for cargo, contexto, url_abs in tuplas:
            if url_abs in vistos:
                continue
            vistos.add(url_abs)
            resultado.append(_construir_oferta_html(cargo, contexto, url_abs, fuente))
            nuevos += 1
            if max_results and len(resultado) >= max_results:
                return resultado, "html"
        if nuevos and not max_results:
            break
    return resultado, "html"


def _procesar_fuente(
    fuente: dict[str, Any], dry_run: bool, verbose: bool,
    max_results: int | None, con_detalle: bool, delay: float,
) -> tuple[dict[str, int], list[dict]]:
    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    nombre = fuente.get("nombre") or f"id={fuente.get('id')}"
    fuente_id = int(fuente["id"])
    inicio = time.time()

    logger.info("─── %s (id=%s) ───", nombre, fuente_id)
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    urls_activas: list[str] = []
    ofertas: list[dict] = []

    try:
        ofertas, metodo = _recolectar_fuente(
            fuente, max_results=max_results, con_detalle=con_detalle, delay=delay,
        )
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas (método: %s)", len(ofertas), metodo)

        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [{nombre[:30]}] {datos['cargo'][:70]}")
                if verbose:
                    print(f"      {datos['url_original']}")
            if dry_run or db is None:
                continue
            try:
                nueva, actualizada = upsert_oferta(db, datos)
                if nueva:
                    stats["nuevas"] += 1
                elif actualizada:
                    stats["actualizadas"] += 1
            except Exception as exc:
                stats["errores"] += 1
                db.rollback()
                logger.exception("  Error upsert: %s", exc)

        if db is not None and urls_activas:
            stats["cerradas"] = marcar_ofertas_cerradas(db, fuente_id, sorted(urls_activas))
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente %s: %s", nombre, exc)
    finally:
        dur = time.time() - inicio
        if db is not None:
            try:
                db.rollback()
                registrar_log(
                    db, fuente_id,
                    "OK" if stats["errores"] == 0 else "PARCIAL",
                    ofertas_nuevas=stats["nuevas"],
                    ofertas_actualizadas=stats["actualizadas"],
                    ofertas_cerradas=stats["cerradas"],
                    paginas=0, duracion=dur,
                )
            except Exception:
                logger.exception("  No se pudo registrar log para %s", nombre)
            db.close()

    return stats, ofertas


def _exportar(ofertas: list[dict], prefijo: str) -> None:
    """Exporta a JSON y CSV con las mismas columnas del esquema de BD."""
    campos = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
              "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
              "renta_bruta_min", "renta_bruta_max", "renta_texto",
              "fecha_publicacion", "fecha_cierre", "url_original",
              "descripcion", "requisitos_texto"]
    serializable = []
    for o in ofertas:
        fila = {k: o.get(k) for k in campos}
        for k in ("fecha_publicacion", "fecha_cierre"):
            if isinstance(fila[k], date):
                fila[k] = fila[k].isoformat()
        serializable.append(fila)
    Path(prefijo + ".json").write_text(
        json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(prefijo + ".csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(serializable)
    logger.info("Exportado: %s.json / %s.csv (%d ofertas)", prefijo, prefijo, len(ofertas))


# ── Orquestación pública ────────────────────────────────────────────────────
def ejecutar(
    dry_run: bool = False,
    verbose: bool = False,
    max_results: int | None = None,
    solo_id: int | None = None,
    solo_host: str | None = None,
    con_detalle: bool = True,
    delay: float = API_DELAY,
    export: str | None = None,
) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper genérico Trabajando.cl (v2 API-first%s)",
                ", standalone" if STANDALONE else "")
    logger.info("=" * 60)

    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    instituciones = _load_all_instituciones()
    fuentes = _filter_trabajando(instituciones, solo_id=solo_id, solo_host=solo_host)
    logger.info("  %d fuentes Trabajando.cl seleccionadas", len(fuentes))

    agregado = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    todas: list[dict] = []

    for fuente in fuentes:
        stats, ofertas = _procesar_fuente(
            fuente, dry_run=dry_run, verbose=verbose, max_results=max_results,
            con_detalle=con_detalle, delay=delay,
        )
        todas.extend(ofertas)
        for k, v in stats.items():
            agregado[k] = agregado.get(k, 0) + v

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info(
        "RESUMEN Trabajando.cl: fuentes=%d encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
        len(fuentes), agregado["encontradas"], agregado["nuevas"],
        agregado["actualizadas"], agregado["cerradas"], agregado["errores"], dur,
    )
    agregado["duracion_seg"] = round(dur, 2)
    agregado["status"] = "OK" if agregado["errores"] == 0 else "PARCIAL"
    return agregado


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    parser = argparse.ArgumentParser(
        description="Scraper genérico para portales *.trabajando.cl (API-first)"
    )
    parser.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--max", type=int, default=None, help="Tope de ofertas por fuente")
    parser.add_argument("--id", type=int, default=None, help="Procesar solo esta fuente (id)")
    parser.add_argument("--host", default=None,
                        help="Procesar solo la fuente cuyo url_empleo contenga este host")
    parser.add_argument("--sin-detalle", action="store_true",
                        help="No consultar /api/ofertas/{id}; solo listado (más rápido)")
    parser.add_argument("--delay", type=float, default=API_DELAY,
                        help=f"Pausa entre requests a la API (default {API_DELAY}s)")
    parser.add_argument("--export", default=None, metavar="PREFIJO",
                        help="Exportar además a PREFIJO.json y PREFIJO.csv")
    args = parser.parse_args()
    ejecutar(
        dry_run=args.dry_run,
        verbose=args.verbose,
        max_results=args.max,
        solo_id=args.id,
        solo_host=args.host,
        con_detalle=not args.sin_detalle,
        delay=args.delay,
        export=args.export,
    )
