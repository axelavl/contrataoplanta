"""
EmpleoEstado.cl — Scraper multi-universidad para portales propios de
universidades del Estado de Chile. Config-driven: cada universidad se
define en UNIVERSIDADES con su URL, selectores y patrones. Cubre:

  - U. de O'Higgins (258)    — uoh.cl/trabaja-con-nosotros [→ universidades_wp.py]
  - U. de Aysén (259)         — uaysen.cl/trabaja-con-nosotros [→ universidades_wp.py]
  - U. del Bío-Bío (248)     — ubiobio.cl/concursos
  - U. de Valparaíso (244)   — cyl.uv.cl/cargos
  - UTEM (254)               — empleos.utem.cl [→ trabajando.py, plataforma Trabajando]
  - U. de Atacama (253)      — pregrado.uda.cl/.../concurso-academico
  - U. de La Frontera (246)  — extranet.ufro.cl/concursos/
  - U. de Tarapacá (252)     — uta.cl/transparencia/.../concursos/
  - U. de Antofagasta (245)  — trabajosua.uantof.cl
  - U. Arturo Prat (251)     — portal.unap.cl/.../llamado.php
  - UMCE (255)               — umceold.umce.cl/... (Joomla)
  - USACH (243)              — usach.cl/procesos-seleccion-honorarios
                               usach.cl/concurso-publico-contratacion-academicas-y-academicos

El parseo es adaptativo: intenta detectar tablas, listados de links, y posts
WordPress/Joomla. Si un selector configurado no funciona, cae a heurísticas
genéricas. PDFs enlazados se detectan automáticamente vía enrich.py.

NOTA: Este scraper fue escrito sin acceso de red a los portales reales
(entorno cloud con egress restringido a dominios .cl). Los selectores CSS
son estimaciones informadas basadas en la estructura de la URL, notas del
equipo, y patrones comunes de universidades chilenas. Cada fuente debe
verificarse con --dry-run --verbose al primer despliegue, y sus selectores
ajustarse si el HTML real difiere.

Uso:
    python scrapers/universidades.py --dry-run --verbose
    python scrapers/universidades.py --dry-run --max 5 --solo 258
    python scrapers/universidades.py --listar
    python scrapers/universidades.py --dry-run --export ofertas_universidades
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

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
except ImportError:
    STANDALONE = True

    class _Cfg:
        LOG_DIR = "logs"
        LOG_LEVEL = "INFO"
        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
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

    def normalizar_region(r: str) -> str | None:  # type: ignore[misc]
        return (r or "").strip() or None

    def marcar_ofertas_cerradas(*a: Any, **k: Any) -> int:  # type: ignore[misc]
        return 0

    def registrar_log(*a: Any, **k: Any) -> None:  # type: ignore[misc]
        return None

    def upsert_oferta(*a: Any, **k: Any) -> tuple[bool, bool]:  # type: ignore[misc]
        return False, False

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.universidades")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "universidades.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

HTTP_TIMEOUT = 25
DELAY_DEFAULT = 1.5

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

_RE_FECHA_ES = re.compile(
    r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", re.I)
_RE_FECHA_DDMMYYYY = re.compile(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})")
_RE_FECHA_ISO = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

CAMPOS_EXPORT = [
    "id_externo", "institucion_id", "institucion_nombre", "sector",
    "cargo", "area_profesional", "tipo_cargo", "region", "ciudad",
    "renta_texto", "fecha_publicacion", "fecha_cierre",
    "url_original", "url_bases", "descripcion", "requisitos_texto",
]

# ── Palabras clave para identificar enlaces de ofertas laborales ────────────
_KW_OFERTA = re.compile(
    r"concurso|oferta|cargo|llamado|vacante|postula|empleo|convocatoria"
    r"|bases|perfil|resoluci[oó]n|seleccion|selección|honorario|académ|académic",
    re.I,
)

_KW_PDF_BASES = re.compile(
    r"bases|perfil|descriptor|resoluc|convocatoria|t[eé]rminos|cargo|llamado", re.I)


# ══════════════════════════════════════════════════════════════════════════════
# Configuración de universidades
# ══════════════════════════════════════════════════════════════════════════════

UNIVERSIDADES: list[dict[str, Any]] = [
    # UOH (258) y UAysén (259) removidas: portales WordPress con CPT/posts →
    # las cubre scrapers/universidades_wp.py (API REST WP + parseo de estado).
    {
        "id": 248,
        "nombre": "Universidad del Bío-Bío",
        "sigla": "UBB",
        "region": "Ñuble",
        "ciudad": "Chillán",
        "urls": ["https://ubiobio.cl/concursos/"],
        "notas": "PDF al hacer click en título de oferta",
        "verificado": False,
    },
    {
        "id": 244,
        "nombre": "Universidad de Valparaíso",
        "sigla": "UV",
        "region": "Valparaíso",
        "ciudad": "Valparaíso",
        "urls": ["https://cyl.uv.cl/cargos"],
        "notas": "PDFs en 'Bases' (movilidad interna profesional)",
        "verificado": False,
    },
    # UTEM (254) removida: empleos.utem.cl corre plataforma Trabajando.cl →
    # la cubre scrapers/trabajando.py con id_dominio=3836.
    {
        "id": 253,
        "nombre": "Universidad de Atacama",
        "sigla": "UDA",
        "region": "Atacama",
        "ciudad": "Copiapó",
        "urls": [
            "https://pregrado.uda.cl/index.php/concurso-academico/",
            "https://uda.cl/concursos-publicos/",
        ],
        "notas": "PDF en 'ver resolución'. Dos portales: académico y general",
        "verificado": False,
    },
    {
        "id": 246,
        "nombre": "Universidad de La Frontera",
        "sigla": "UFRO",
        "region": "La Araucanía",
        "ciudad": "Temuco",
        "urls": [
            "https://extranet.ufro.cl/concursos/ver_tipo_administrativo.php",
            "https://personas.ufro.cl/servicio/convocatorias/",
        ],
        "notas": "Dos portales: extranet (administrativo) y personas (convocatorias)",
        "verificado": False,
    },
    {
        "id": 252,
        "nombre": "Universidad de Tarapacá",
        "sigla": "UTA",
        "region": "Arica y Parinacota",
        "ciudad": "Arica",
        "urls": [
            "https://www.uta.cl/transparencia/actosyresoluciones_d/concursos/2026/",
        ],
        "notas": "Transparencia: directorio con resoluciones por año",
        "verificado": False,
    },
    {
        "id": 245,
        "nombre": "Universidad de Antofagasta",
        "sigla": "UA",
        "region": "Antofagasta",
        "ciudad": "Antofagasta",
        "urls": ["https://trabajosua.uantof.cl/"],
        "notas": "Portal propio. PDF en 'Documentos > BASES'",
        "verificado": False,
    },
    {
        "id": 251,
        "nombre": "Universidad Arturo Prat",
        "sigla": "UNAP",
        "region": "Tarapacá",
        "ciudad": "Iquique",
        "urls": ["http://portal.unap.cl/kb/dewey/app/sispartime/llamado.php"],
        "notas": "PHP legacy (sispartime). PDF en 'perfil'. HTTP (no HTTPS)",
        "verificado": False,
    },
    {
        "id": 255,
        "nombre": "Universidad Metropolitana de Ciencias de la Educación",
        "sigla": "UMCE",
        "region": "Metropolitana de Santiago",
        "ciudad": "Santiago",
        "urls": [
            "https://umceold.umce.cl/index.php?option=com_content&view=article&id=1126",
        ],
        "notas": "Joomla legacy (umceold). Puede estar obsoleto",
        "verificado": False,
    },
    {
        "id": 243,
        "nombre": "Universidad de Santiago de Chile",
        "sigla": "USACH",
        "region": "Metropolitana de Santiago",
        "ciudad": "Santiago",
        "urls": [
            "https://www.usach.cl/procesos-seleccion-honorarios",
            "https://www.usach.cl/concurso-publico-contratacion-academicas-y-academicos",
        ],
        "notas": "Dos secciones: honorarios y concursos académicos. PDFs en 'bases'/'ver aquí'. "
                 "También publica en usach.trabajando.cl (cubierto por trabajando.py)",
        "verificado": False,
    },
]


# ══════════════════════════════════════════════════════════════════════════════
# HTTP
# ══════════════════════════════════════════════════════════════════════════════

def _session() -> requests.Session:
    s = requests.Session()
    getattr(config, "aplicar_proxy", lambda *_: None)(s)
    s.headers.update({
        "User-Agent": config.USER_AGENTS[0],
        "Accept-Language": "es-CL,es;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def _fetch(s: requests.Session, url: str) -> str | None:
    """GET con reintentos y manejo de HTTP/HTTPS."""
    esquemas = [url]
    if url.startswith("http://"):
        esquemas.append(url.replace("http://", "https://", 1))
    elif url.startswith("https://"):
        esquemas.append(url.replace("https://", "http://", 1))

    for candidato in esquemas:
        try:
            r = s.get(candidato, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text
            if r.status_code == 403:
                logger.info("  403 en %s (posible WAF/IP chilena requerida)", candidato[:60])
                return None
        except requests.RequestException as exc:
            logger.info("  GET %s falló: %s", candidato[:60], type(exc).__name__)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Parseo genérico adaptativo
# ══════════════════════════════════════════════════════════════════════════════

def _parsear_fecha(texto: str) -> date | None:
    """Intenta parsear una fecha en formatos chilenos comunes."""
    if not texto:
        return None
    texto = texto.strip()
    if m := _RE_FECHA_ISO.search(texto):
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    if m := _RE_FECHA_ES.search(texto):
        mes = MESES.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                pass
    if m := _RE_FECHA_DDMMYYYY.search(texto):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


def _extraer_ofertas_tabla(soup: BeautifulSoup, base_url: str) -> list[dict[str, Any]]:
    """Extrae ofertas desde tablas HTML con enlaces."""
    ofertas: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for table in soup.select("table"):
        for tr in table.select("tr"):
            tds = tr.select("td")
            if not tds:
                continue
            enlace = tr.select_one("a[href]")
            if not enlace:
                continue
            href = enlace.get("href", "")
            if not href or href.startswith("#") or href.startswith("javascript"):
                continue
            url = urljoin(base_url, href)
            if url in vistos:
                continue
            texto_fila = limpiar_texto(tr.get_text(" ", strip=True))
            cargo_raw = limpiar_texto(enlace.get_text(" ", strip=True))
            if not cargo_raw or len(cargo_raw) < 5:
                cargo_raw = limpiar_texto(tds[0].get_text(" ", strip=True))
            if not _KW_OFERTA.search(cargo_raw + " " + texto_fila):
                continue
            vistos.add(url)
            fecha_cierre = None
            for td in tds:
                t = td.get_text(strip=True)
                if f := _parsear_fecha(t):
                    fecha_cierre = f
            ofertas.append({
                "cargo": cargo_raw[:500],
                "url": url,
                "url_bases": url if url.lower().endswith(".pdf") else None,
                "fecha_cierre": fecha_cierre,
                "texto_fila": texto_fila,
            })
    return ofertas


def _extraer_ofertas_links(soup: BeautifulSoup, base_url: str) -> list[dict[str, Any]]:
    """Extrae ofertas como enlaces individuales que parecen ser ofertas laborales."""
    ofertas: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue
        texto = limpiar_texto(a.get_text(" ", strip=True))
        if len(texto) < 8:
            continue
        url = urljoin(base_url, href)
        if url in vistos:
            continue
        es_pdf = url.lower().endswith(".pdf")
        if not es_pdf and not _KW_OFERTA.search(texto + " " + href):
            continue
        # Filtrar links de navegación/menú
        if any(w in texto.lower() for w in ("inicio", "contacto", "mapa del sitio",
               "intranet", "webmail", "facebook", "twitter", "instagram")):
            continue
        vistos.add(url)
        parent = a.find_parent(["tr", "li", "div", "article", "section"])
        contexto = limpiar_texto(parent.get_text(" ", strip=True)) if parent else texto
        fecha_cierre = _parsear_fecha(contexto)
        ofertas.append({
            "cargo": texto[:500],
            "url": url,
            "url_bases": url if es_pdf else None,
            "fecha_cierre": fecha_cierre,
            "texto_fila": contexto[:500],
        })
    return ofertas


def _extraer_ofertas_wp_api(s: requests.Session, base_url: str) -> list[dict[str, Any]] | None:
    """Intenta usar la API REST de WordPress si está disponible."""
    parsed = urlparse(base_url)
    wp_api = f"{parsed.scheme}://{parsed.netloc}/wp-json/wp/v2/posts?per_page=50&status=publish"
    try:
        r = s.get(wp_api, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        posts = r.json()
        if not isinstance(posts, list) or not posts:
            return None
    except (requests.RequestException, ValueError):
        return None

    ofertas: list[dict[str, Any]] = []
    for post in posts:
        titulo = limpiar_texto(
            BeautifulSoup(str(post.get("title", {}).get("rendered", "")),
                          "html.parser").get_text())
        if not titulo or not _KW_OFERTA.search(titulo):
            continue
        url = post.get("link", "")
        fecha_pub = None
        if post.get("date"):
            try:
                fecha_pub = date.fromisoformat(str(post["date"])[:10])
            except ValueError:
                pass
        contenido_html = str(post.get("content", {}).get("rendered", ""))
        fecha_cierre = _parsear_fecha(contenido_html)
        # Buscar PDFs en el contenido
        url_bases = None
        for m in re.finditer(r'href="([^"]*\.pdf[^"]*)"', contenido_html, re.I):
            pdf_url = urljoin(url, m.group(1))
            if _KW_PDF_BASES.search(pdf_url):
                url_bases = pdf_url
                break
            if not url_bases:
                url_bases = pdf_url

        ofertas.append({
            "cargo": titulo[:500],
            "url": url,
            "url_bases": url_bases,
            "fecha_cierre": fecha_cierre,
            "fecha_publicacion": fecha_pub,
            "texto_fila": limpiar_texto(
                BeautifulSoup(contenido_html, "html.parser").get_text(" ", strip=True))[:500],
        })
    return ofertas


def _extraer_ofertas_directorio(s: requests.Session, url: str) -> list[dict[str, Any]]:
    """Para URLs tipo directorio (transparencia/concursos/2026/): lista archivos."""
    html = _fetch(s, url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    ofertas: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href or href in (".", "..", "/"):
            continue
        archivo_url = urljoin(url, href)
        if archivo_url in vistos:
            continue
        vistos.add(archivo_url)
        nombre = limpiar_texto(a.get_text(" ", strip=True)) or href.split("/")[-1]
        es_pdf = archivo_url.lower().endswith(".pdf")
        if not es_pdf and not _KW_OFERTA.search(nombre + " " + href):
            continue
        ofertas.append({
            "cargo": nombre[:500],
            "url": archivo_url,
            "url_bases": archivo_url if es_pdf else None,
            "fecha_cierre": None,
            "texto_fila": nombre,
        })
    return ofertas


def parsear_listado(html: str, base_url: str,
                    session: requests.Session | None = None) -> list[dict[str, Any]]:
    """Parseo adaptativo: prueba tabla → WP API → links genéricos."""
    soup = BeautifulSoup(html, "html.parser")
    for el in soup.select("nav, footer, script, style"):
        el.decompose()

    # 1. Tablas con estructura
    ofertas = _extraer_ofertas_tabla(soup, base_url)
    if len(ofertas) >= 2:
        logger.info("  Modo tabla: %d ofertas", len(ofertas))
        return ofertas

    # 2. WP REST API
    if session:
        wp = _extraer_ofertas_wp_api(session, base_url)
        if wp:
            logger.info("  Modo WP API: %d ofertas", len(wp))
            return wp

    # 3. Links genéricos
    ofertas = _extraer_ofertas_links(soup, base_url)
    logger.info("  Modo links: %d ofertas", len(ofertas))
    return ofertas


def parsear_detalle(html: str, base_url: str) -> dict[str, Any]:
    """Extrae datos del detalle de una oferta."""
    soup = BeautifulSoup(html, "html.parser")
    for el in soup.select("nav, header, footer, script, style"):
        el.decompose()

    d: dict[str, Any] = {}

    # Cargo del h1/h2
    for tag in ("h1", "h2"):
        h = soup.select_one(tag)
        if h:
            cargo = limpiar_texto(h.get_text(" ", strip=True))
            if len(cargo) > 10 and _KW_OFERTA.search(cargo):
                d["cargo"] = cargo[:500]
                break

    # Texto completo del body
    texto = limpiar_texto(soup.get_text(" ", strip=True))

    # Fecha de cierre
    d["fecha_cierre"] = _parsear_fecha(texto)

    # Separar descripción y requisitos
    descripcion, requisitos = texto, None
    for patron in (r"Requisitos?\s*:?", r"Perfil requerido\s*:?",
                   r"Formaci[oó]n\s*:?", r"Experiencia\s*:?"):
        if m := re.search(patron, texto, re.I):
            descripcion = texto[:m.start()].strip()
            requisitos = texto[m.end():].strip()
            break
    d["descripcion"] = descripcion[:2000] if len(descripcion) > 30 else None
    d["requisitos_texto"] = requisitos[:2000] if requisitos else None

    # PDFs de bases
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        texto_link = (a.get_text(strip=True) + " " + href).lower()
        if ".pdf" in href.lower() and _KW_PDF_BASES.search(texto_link):
            d["url_bases"] = urljoin(base_url, href)
            break

    return d


def _nivel(cargo: str) -> str | None:
    """Infiere nivel jerárquico desde el cargo."""
    cl = cargo.lower()
    if any(w in cl for w in ("director", "decano", "vicerrector", "rector",
                              "secretario general", "jefe de", "jefa de")):
        return "Directivo"
    if any(w in cl for w in ("profesional", "analista", "ingeniero", "ingeniera",
                              "coordinador", "coordinadora", "investigador",
                              "investigadora", "académico", "académica", "docente")):
        return "Profesional"
    if any(w in cl for w in ("técnico", "técnica", "asistente", "secretario",
                              "secretaria", "auxiliar")):
        return "Técnico"
    return None


def _tipo_cargo(cargo: str, texto: str = "") -> str | None:
    """Infiere tipo de contrato."""
    t = (cargo + " " + texto).lower()
    if "honorario" in t:
        return "Honorarios"
    if "planta" in t:
        return "Planta"
    if "contrata" in t:
        return "Contrata"
    if "reemplazo" in t:
        return "Reemplazo"
    if "concurso" in t:
        return "Concurso público"
    return None


def construir_oferta(univ: dict[str, Any], item: dict[str, Any],
                     det: dict[str, Any]) -> dict:
    """Construye oferta estándar para persistencia."""
    institucion_id = int(univ["id"])
    nombre = univ["nombre"]
    cargo = det.get("cargo") or item.get("cargo") or ""
    texto_contexto = item.get("texto_fila", "")

    slug = urlparse(item["url"]).path.strip("/").split("/")[-1] or cargo
    return {
        "id_externo": generar_id_estable(institucion_id, nombre, slug),
        "institucion_id": institucion_id,
        "fuente_id": None,
        "url_original": item["url"],
        "cargo": cargo[:500],
        "descripcion": det.get("descripcion"),
        "institucion_nombre": nombre,
        "sector": "Universidad/Educación",
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo_cargo(cargo, texto_contexto),
        "nivel": _nivel(cargo),
        "region": univ["region"],
        "ciudad": det.get("ciudad") or univ.get("ciudad"),
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": det.get("fecha_publicacion")
                             or item.get("fecha_publicacion")
                             or date.today(),
        "fecha_cierre": det.get("fecha_cierre") or item.get("fecha_cierre"),
        "requisitos_texto": det.get("requisitos_texto"),
        "url_bases": det.get("url_bases") or item.get("url_bases"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Recolección
# ══════════════════════════════════════════════════════════════════════════════

def recolectar_universidad(univ: dict[str, Any], session: requests.Session,
                           max_results: int | None, delay: float,
                           con_detalle: bool) -> list[dict]:
    """Recolecta ofertas de una universidad."""
    sigla = univ["sigla"]
    todas_ofertas: list[dict] = []
    hoy = date.today()

    for url_base in univ["urls"]:
        logger.info("  [%s] Sondeando %s", sigla, url_base[:70])

        # URL tipo directorio (transparencia con archivos)
        if "/transparencia/" in url_base or url_base.rstrip("/").endswith(("/2025", "/2026")):
            items = _extraer_ofertas_directorio(session, url_base)
        else:
            html = _fetch(session, url_base)
            if html is None:
                logger.warning("  [%s] No se pudo cargar %s", sigla, url_base[:60])
                continue
            items = parsear_listado(html, url_base, session)

        logger.info("  [%s] %d items desde %s", sigla, len(items), url_base[:50])

        for it in items:
            if max_results and len(todas_ofertas) >= max_results:
                break
            # Filtrar vencidas
            if it.get("fecha_cierre") and it["fecha_cierre"] < hoy:
                continue

            det: dict[str, Any] = {}
            texto_detalle = None
            if con_detalle and not it["url"].lower().endswith(".pdf"):
                time.sleep(delay)
                hd = _fetch(session, it["url"])
                if hd:
                    det = parsear_detalle(hd, it["url"])
                    texto_detalle = hd

            oferta = construir_oferta(univ, it, det)

            # Enriquecimiento
            try:
                from scrapers.enrich import enriquecer_oferta, encontrar_pdf_urls
                pdf_urls = encontrar_pdf_urls(texto_detalle, url_base) if texto_detalle else []
                enriquecer_oferta(
                    oferta,
                    texto_html=det.get("descripcion"),
                    pdf_urls=pdf_urls,
                    session=session,
                )
            except Exception:
                pass

            todas_ofertas.append(oferta)

    return todas_ofertas


# ══════════════════════════════════════════════════════════════════════════════
# Persistencia / export
# ══════════════════════════════════════════════════════════════════════════════

def _exportar(ofertas: list[dict], prefijo: str) -> None:
    ser = []
    for o in ofertas:
        f = {k: o.get(k) for k in CAMPOS_EXPORT}
        for k in ("fecha_publicacion", "fecha_cierre"):
            if isinstance(f[k], date):
                f[k] = f[k].isoformat()
        ser.append(f)
    Path(prefijo + ".json").write_text(
        json.dumps(ser, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(prefijo + ".csv", "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS_EXPORT)
        w.writeheader()
        w.writerows(ser)
    logger.info("Exportado: %s.json / %s.csv (%d ofertas)", prefijo, prefijo, len(ser))


def ejecutar(dry_run=False, verbose=False, max_results=None,
             delay=DELAY_DEFAULT, con_detalle=True, export=None,
             solo_id: int | None = None) -> dict[str, Any]:
    """Punto de entrada principal. Recorre todas las universidades configuradas."""
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Universidades%s", " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    stats_global = {"nuevas": 0, "actualizadas": 0, "cerradas": 0,
                    "errores": 0, "encontradas": 0}
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    todas_ofertas: list[dict] = []
    session = _session()

    universidades = UNIVERSIDADES
    if solo_id:
        universidades = [u for u in UNIVERSIDADES if u["id"] == solo_id]
        if not universidades:
            logger.error("ID %d no encontrado en UNIVERSIDADES", solo_id)
            return stats_global

    for univ in universidades:
        sigla = univ["sigla"]
        institucion_id = int(univ["id"])
        logger.info("─── %s (id=%d) ───", sigla, institucion_id)

        urls_activas: list[str] = []
        try:
            ofertas = recolectar_universidad(
                univ, session, max_results, delay, con_detalle)
            stats_global["encontradas"] += len(ofertas)
            todas_ofertas.extend(ofertas)

            for datos in ofertas:
                urls_activas.append(datos["url_original"])
                if verbose or dry_run:
                    print(f"  [{sigla}] {datos['cargo'][:50]:50} | "
                          f"cierre: {datos.get('fecha_cierre')}")
                    if verbose:
                        print(f"      {datos['url_original']}")
                        if datos.get("url_bases"):
                            print(f"      PDF: {datos['url_bases']}")

                if dry_run or db is None:
                    continue
                try:
                    nueva, actualizada = upsert_oferta(db, datos)
                    if nueva:
                        stats_global["nuevas"] += 1
                    elif actualizada:
                        stats_global["actualizadas"] += 1
                except Exception as exc:
                    stats_global["errores"] += 1
                    db.rollback()
                    logger.exception("  [%s] Error upsert: %s", sigla, exc)

            if db is not None and urls_activas:
                cerradas = marcar_ofertas_cerradas(db, institucion_id, sorted(urls_activas))
                stats_global["cerradas"] += cerradas

        except Exception as exc:
            if db is not None:
                db.rollback()
            stats_global["errores"] += 1
            logger.exception("  [%s] Error: %s", sigla, exc)

        if db is not None:
            try:
                registrar_log(db, institucion_id,
                              "OK" if stats_global["errores"] == 0 else "PARCIAL",
                              ofertas_nuevas=stats_global["nuevas"],
                              ofertas_actualizadas=stats_global["actualizadas"],
                              ofertas_cerradas=stats_global["cerradas"],
                              paginas=0, duracion=time.time() - inicio)
            except Exception:
                logger.exception("  [%s] No se pudo registrar log", sigla)

    if db is not None:
        db.close()

    if export and todas_ofertas:
        _exportar(todas_ofertas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN UNIVERSIDADES: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats_global["encontradas"], stats_global["nuevas"],
                stats_global["actualizadas"], stats_global["cerradas"],
                stats_global["errores"], dur)
    stats_global["duracion_seg"] = round(dur, 2)
    stats_global["status"] = "OK" if stats_global["errores"] == 0 else "PARCIAL"
    return stats_global


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper multi-universidad (portales propios)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de ofertas por universidad")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--sin-detalle", action="store_true",
                   help="Solo listado (no abre fichas de detalle)")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    p.add_argument("--solo", type=int, default=None, metavar="ID",
                   help="Solo procesar la universidad con este ID de catálogo")
    p.add_argument("--listar", action="store_true",
                   help="Solo listar universidades configuradas")
    a = p.parse_args()

    if a.listar:
        for u in UNIVERSIDADES:
            estado = "✓ verificado" if u.get("verificado") else "⚠ sin verificar"
            print(f"  {u['id']:3d}  {u['sigla']:8s}  {u['nombre']}")
            for url in u["urls"]:
                print(f"       └─ {url}")
            print(f"       {estado} — {u.get('notas', '')}")
        sys.exit(0)

    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, con_detalle=not a.sin_detalle, export=a.export,
             solo_id=a.solo)
