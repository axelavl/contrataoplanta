"""
EmpleoEstado.cl — Scraper de concursos públicos de la PDI
(https://www.pdichile.cl). Versión 2, patrón estándar de sesión.

Estructura real del sitio (Sitefinity CMS, verificada 06/2026):
    /institución/concursos-publicos          índice con links a páginas por
                                             planta (Oficiales de los
                                             Servicios, Apoyo Científico
                                             Técnico, Apoyo General, ...)
    /institución/concursos-publicos/{slug}   página por planta: artículo largo
                                             donde cada concurso/especialidad
                                             es una sección con heading propio
                                             (ej: "Planta de Oficiales,
                                             Escalafón ... Médico Cirujano",
                                             "Especialidad de Químico
                                             (PROPERQCO-05)"), texto con el
                                             llamado y plazos, y PDFs (bases,
                                             resoluciones, resultados).

Extracción por sección: cargo (heading), fechas de publicación y cierre del
texto ("postulaciones se recibirán desde el 29 de diciembre de 2025 al 06 de
enero de 2026"), descripción, link a bases. Las secciones con "Resultados
finales" / "declarado desierto" / "proceso finalizado" se omiten por cerradas.

LIMITACIONES CONOCIDAS (documentadas, no defectos):
    * Las bases PDF están ESCANEADAS sin capa de texto (verificado: 34
      páginas, 33 caracteres extraíbles), por lo que renta_bruta_* y
      requisitos_texto quedarán vacíos salvo que aparezcan en el HTML. El
      link a las bases se conserva al final de la descripción.
    * El WAF del sitio exige headers completos de navegador y bloquea
      intermitentemente con 403 "The request is blocked"; el scraper usa
      delay alto (default 2s) y reintentos con backoff. pdichile.cl SÍ es
      accesible desde datacenters extranjeros (a diferencia de pdi.cl).

Uso:
    python scrapers/pdi.py --dry-run --verbose
    python scrapers/pdi.py --dry-run --export ofertas_pdi
    python scrapers/pdi.py --incluir-cerrados   # no filtrar finalizados
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
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin

import requests

try:
    from zoneinfo import ZoneInfo
    _TZ_CL = ZoneInfo("America/Santiago")
except Exception:  # pragma: no cover - fallback si no hay tzdata
    _TZ_CL = None


def _hoy_cl() -> date:
    """Fecha 'hoy' en horario de Chile (no UTC).

    Crítico para el filtro de vencimiento: en producción (Railway, UTC) un
    `date.today()` naive se adelanta hasta 4 h respecto de Chile, lo que haría
    desaparecer un cargo que cierra "hoy" durante la franja 00:00–04:00 UTC
    (= 20:00–24:00 de Chile del día anterior). Comparar siempre contra la
    fecha local chilena evita ese cierre prematuro.
    """
    if _TZ_CL is not None:
        return datetime.now(_TZ_CL).date()
    return date.today()
from bs4 import BeautifulSoup

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
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
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

logger = logging.getLogger("scraper.pdi")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "pdi.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://www.pdichile.cl"
INDICE = BASE + "/instituci%C3%B3n/concursos-publicos"
FUENTE = {
    "id": 162,  # ajustar al id real de PDI en el repositorio si difiere
    "nombre": "Policía de Investigaciones de Chile (PDI)",
    "sigla": "PDI",
    "sector": "Orden y Seguridad",
    "region": "Nacional",
    "url_empleo": BASE + "/instituci%C3%B3n/trabaja-con-nosotros",
}

HTTP_TIMEOUT = 30
MAX_RETRIES = 4
DELAY_DEFAULT = 2.0  # el WAF bloquea intermitentemente; ser conservador

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

_RE_FECHA_ES = re.compile(r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", re.I)
_RE_RANGO_POSTULACION = re.compile(
    r"(?:postulaci(?:on|ón)es?[^.]{0,80}?)?desde el\s+"
    r"(\d{1,2}(?:\s+de\s+\w+(?:\s+de\s+\d{4})?)?)\s+(?:al|hasta el)\s+"
    r"(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4})", re.I)
_RE_CERRADO = re.compile(
    r"resultados?\s+finales|proceso finalizado|declarad[oa]s?\s+desiert|concurso cerrado", re.I)
_RE_TITULO_CONCURSO = re.compile(r"planta|escalaf[oó]n|especialidad", re.I)

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]

TRABAJA_URL = BASE + "/instituci%C3%B3n/trabaja-con-nosotros"

# Extracción de texto de PDFs de perfil (estos SÍ tienen capa de texto,
# a diferencia de las bases de planta que están escaneadas).
# pdfplumber es el estándar del proyecto (está en requirements.txt); pypdf
# queda solo como respaldo opcional si estuviera instalado. Antes este módulo
# dependía únicamente de pypdf, que NO está en requirements: en producción la
# extracción de perfiles quedaba silenciosamente vacía.
try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None  # type: ignore[assignment]

try:
    from pypdf import PdfReader as _PdfReader
except ImportError:
    _PdfReader = None  # type: ignore[assignment]


def pdf_a_texto(contenido: bytes) -> str:
    if not contenido or not contenido[:5].startswith(b"%PDF"):
        return ""
    import io
    if pdfplumber is not None:
        try:
            with pdfplumber.open(io.BytesIO(contenido)) as pdf:
                texto = "\n".join((p.extract_text() or "") for p in pdf.pages)
            if texto.strip():
                return texto
        except Exception:
            pass
    if _PdfReader is not None:
        try:
            return "\n".join((p.extract_text() or "")
                             for p in _PdfReader(io.BytesIO(contenido)).pages)
        except Exception:
            return ""
    return ""


_RE_PLAZO = re.compile(r"plazo de postulaci[oó]n\s*:?\s*([^)]{5,80})\)", re.I)
_RE_RANGO_CORTO = re.compile(
    r"(\d{1,2})\s+(?:de\s+\w+\s+)?al\s+(\d{1,2})\s+de\s+(\w+)\s+(?:de(?:l)?\s+)?(\d{4})", re.I)


def _fechas_de_plazo(plazo: str) -> tuple[date | None, date | None]:
    """'05 al 10 de junio de 2026' o '08 al 19 de junio de 2026' -> (ini, fin)."""
    if m := _RE_RANGO_CORTO.search(plazo or ""):
        d1, d2, mes, anio = m.groups()
        mes_n = MESES.get(mes.lower())
        if mes_n:
            try:
                return date(int(anio), mes_n, int(d1)), date(int(anio), mes_n, int(d2))
            except ValueError:
                pass
    fin = _fecha_es(plazo)
    return None, fin


def _titulo_desde_contexto(a_tag, link_text: str, ctx: str) -> str:
    """Extrae el título real del cargo del contexto circundante.

    En la página "trabaja con nosotros" el texto del link suele ser solo el
    código del perfil (ej: "PERFIL PROFCRIORG-01 + RESOLEX 152") pero el
    nombre legible del cargo aparece antes, en un heading o párrafo previo.
    """
    if len(link_text) > 40 and not link_text.upper().startswith("PERFIL"):
        return link_text

    for sib in a_tag.parent.previous_siblings if a_tag.parent else []:
        t = limpiar_texto(getattr(sib, "get_text", lambda *_: str(sib))(" ", strip=True))
        if not t or len(t) < 15:
            continue
        t_low = t.lower()
        if any(w in t_low for w in ("cargos vigentes", "perfil", "resolex",
                                     "descargar", "click")):
            continue
        return t[:500]

    if ctx and len(ctx) > len(link_text) + 20:
        before = ctx.split(link_text)[0].strip() if link_text in ctx else ""
        if len(before) > 20:
            return before[:500]

    return link_text


def parsear_trabaja_con_nosotros(html: str) -> list[dict[str, Any]]:
    """Cargos vigentes (contrata/jornal) publicados como links a PDF de perfil."""
    soup = BeautifulSoup(html, "html.parser")
    cargos: list[dict[str, Any]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" not in href.lower() or "/docs/" not in href:
            continue
        link_text = limpiar_texto(a.get_text(" ", strip=True))
        cont = a.find_parent(["li", "p", "div", "td"])
        ctx = limpiar_texto(cont.get_text(" ", strip=True)) if cont else ""
        plazo_m = _RE_PLAZO.search(ctx)
        es_perfil = "perfil-" in href.lower() or "perfil " in link_text.lower()
        es_cargo = (bool(plazo_m)
                    or ("concurso" in href.lower() and len(link_text) > 25)
                    or es_perfil)
        if not es_cargo or len(link_text) < 10:
            continue
        titulo = _titulo_desde_contexto(a, link_text, ctx)
        url_pdf = urljoin(BASE, quote(href, safe=":/%?=&"))
        ini, fin = _fechas_de_plazo(plazo_m.group(1)) if plazo_m else (None, None)
        grado = None
        if g := re.search(r"asimilad[oa] a grado\s+(\d+)", titulo + " " + ctx, re.I):
            grado = g.group(1)
        cargos.append({"titulo": titulo, "url_pdf": url_pdf, "plazo_ini": ini,
                       "plazo_fin": fin, "grado": grado, "contexto": ctx[:500]})
    return cargos


def parsear_perfil_pdf(texto: str) -> dict[str, Any]:
    """Campos estructurados del PDF de perfil (tiene capa de texto)."""
    d: dict[str, Any] = {}
    if not texto:
        return d
    if m := re.search(r"renta\s+bruta\s*:?\s*\$\s*([\d.]+\d)", texto, re.I):
        try:
            d["renta"] = int(m.group(1).replace(".", ""))
            d["renta_texto"] = f"${m.group(1)} bruto"
        except ValueError:
            pass
    if m := re.search(r"regi[oó]n\s*:?\s*([A-ZÁÉÍÓÚÑ][\wáéíóúñ ]{3,40})", texto, re.I):
        d["region"] = normalizar_region(limpiar_texto(m.group(1)).rstrip("."))
    # Los perfiles PDI usan secciones con numeral romano:
    # I. OBJETIVO DEL CARGO / II. FUNCIONES / IV. REQUISITOS / ...
    def _seccion(nombre: str) -> str | None:
        m = re.search(rf"\n\s*[IVX]+\.\s*{nombre}[^\n]*\n(.{{40,2400}})",
                      texto, re.I | re.S)
        if not m:
            return None
        bloque = m.group(1)
        corte = re.search(r"\n\s*[IVX]+\.\s*[A-ZÁÉÍÓÚÑ]", bloque)
        if corte:
            bloque = bloque[: corte.start()]
        return limpiar_texto(bloque)[:2000] or None

    if v := _seccion("REQUISITOS"):
        d["requisitos"] = v
    if v := _seccion("OBJETIVO DEL CARGO"):
        d["objetivo"] = v[:1500]
    # Fechas: "Fecha de publicación 03 de julio de 2026"
    if m := re.search(r"fecha\s+de\s+publicaci[oó]n\s*:?\s*(.{10,50})", texto, re.I):
        d["fecha_publicacion"] = _fecha_es(m.group(1))
    # Plazo: "Etapa de Reclutamiento 03 al 06 de julio 2026"
    # o "Etapa de Reclutamiento 03 al 06 de julio de 2026"
    if m := re.search(
        r"(?:etapa\s+de\s+reclutamiento|plazo\s+de\s+postulaci[oó]n)\s*:?\s*(.{10,80})",
        texto, re.I,
    ):
        ini, fin = _fechas_de_plazo(m.group(1))
        if ini:
            d["plazo_ini"] = ini
        if fin:
            d["plazo_fin"] = fin
    # Email de postulación: "enviar sus antecedentes al correo, postulapdi3@..."
    if m := re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", texto):
        d["email_postulacion"] = m.group(0).lower()
    t = texto.lower()
    if "jornal" in t:
        d["tipo"] = "Jornal"
    elif "honorario" in t:
        d["tipo"] = "Honorarios"
    elif re.search(r"\bcontrata\b|asimilad[oa] a grado", t):
        d["tipo"] = "Contrata"
    return d


def construir_oferta_contrata(c: dict[str, Any], perfil: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = c["titulo"][:500]

    desc_partes = []
    if perfil.get("objetivo"):
        desc_partes.append(perfil["objetivo"])
    if c.get("grado"):
        desc_partes.append(f"Asimilado a grado {c['grado']}")
    desc_partes.append(f"Perfil del cargo: {c['url_pdf']}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    tipo = perfil.get("tipo")
    if not tipo:
        tipo = "Contrata" if c.get("grado") else "Planta"

    c_low = cargo.lower()
    nivel = "Profesional"
    if any(w in c_low for w in ("auxiliar", "administrativo", "técnico", "tecnico",
                                "conductor", "jornal")):
        nivel = "Técnico"
    elif any(w in c_low for w in ("jefe", "director")):
        nivel = "Directivo"

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, c["url_pdf"]),
        "institucion_id": fuente_id,  # id del catalogo de instituciones
        "fuente_id": None,            # FK a fuentes: no aplica a scraper standalone
        "url_original": c["url_pdf"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": tipo,
        "nivel": nivel,
        "region": perfil.get("region") or FUENTE["region"],
        "ciudad": None,
        "renta_bruta_min": perfil.get("renta"),
        "renta_bruta_max": None,
        "renta_texto": perfil.get("renta_texto"),
        "fecha_publicacion": c["plazo_ini"] or perfil.get("fecha_publicacion") or perfil.get("plazo_ini") or _hoy_cl(),
        "fecha_cierre": c["plazo_fin"] or perfil.get("plazo_fin"),
        "requisitos_texto": perfil.get("requisitos"),
        "email_postulacion": perfil.get("email_postulacion"),
        "url_bases": c.get("url_pdf"),
    }


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _headers() -> dict[str, str]:
    # El WAF de pdichile.cl rechaza requests con headers mínimos (403):
    # enviar siempre el set completo de un navegador.
    return {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
        "Referer": BASE + "/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            bloqueado = (r.status_code == 403
                         or b"request is blocked" in r.content[:2000].lower()
                         or b"service unavailable" in r.content[:2000].lower())
            if bloqueado:
                if intento == MAX_RETRIES:
                    logger.warning("  WAF bloqueó %s tras %d intentos", url, intento)
                    return None
                espera = 4 * intento
                logger.info("  WAF 403 en %s; reintento %d en %ds", url, intento, espera)
                time.sleep(espera)
                continue
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url)
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo definitivo %s: %s", url, type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _fecha_es(texto: str, anio_default: int | None = None) -> date | None:
    """'29 de diciembre de 2025' -> date. Acepta '29 de diciembre' con año default."""
    if m := _RE_FECHA_ES.search(texto or ""):
        d, mes, a = m.groups()
        mes_n = MESES.get(mes.lower())
        if mes_n:
            try:
                return date(int(a), mes_n, int(d))
            except ValueError:
                return None
    if anio_default and (m := re.search(r"(\d{1,2})\s+de\s+(\w+)", texto or "", re.I)):
        mes_n = MESES.get(m.group(2).lower())
        if mes_n:
            try:
                return date(anio_default, mes_n, int(m.group(1)))
            except ValueError:
                return None
    return None


def descubrir_subpaginas(html: str, base_url: str = BASE) -> list[str]:
    """Links a páginas de plantas dentro del índice de concursos."""
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/concursos-publicos/" not in href.replace("%C3%B3", "ó"):
            continue
        url = urljoin(base_url, quote(href, safe=":/%?=&"))
        if url.rstrip("/").endswith("concursos-publicos"):
            continue
        # "portada" es un índice que repite los nombres de las plantas como
        # links; parsearlo genera secciones falsas
        if url.rstrip("/").endswith("/portada"):
            continue
        if url not in urls:
            urls.append(url)
    return urls


def parsear_subpagina(html: str, url_pagina: str) -> list[dict[str, Any]]:
    """Segmenta una página de planta en secciones de concurso."""
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup.body
    if main is None:
        return []

    secciones: list[dict[str, Any]] = []
    actual: dict[str, Any] | None = None
    preambulo: list[str] = []  # texto antes de la primera sección

    for el in main.find_all(["h1", "h2", "h3", "h4", "strong", "p", "li", "a"]):
        if el.name in ("h1", "h2", "h3", "h4", "strong"):
            t = limpiar_texto(el.get_text(" ", strip=True))
            es_titulo = (len(t) > 25 and _RE_TITULO_CONCURSO.search(t)
                         and not t.startswith(">")
                         and "revisar" not in t.lower()
                         and "documentos" not in t.lower()
                         and "cargos titulares" not in t.lower())
            if es_titulo:
                t = t.rstrip(".")
                if secciones and secciones[-1]["titulo"] == t:
                    actual = secciones[-1]  # heading repetido del mismo concurso
                else:
                    actual = {"titulo": t, "texto": [], "pdfs": [],
                              "url_pagina": url_pagina}
                    secciones.append(actual)
                continue
        if actual is None:
            t = limpiar_texto(el.get_text(" ", strip=True))
            if t:
                preambulo.append(t)
            continue
        if el.name == "a" and el.get("href") and ".pdf" in el["href"].lower():
            actual["pdfs"].append(
                (limpiar_texto(el.get_text(" ", strip=True)),
                 urljoin(url_pagina, quote(el["href"], safe=":/%?=&"))))
        elif el.name in ("p", "li", "h1", "h2", "h3", "h4", "strong"):
            t = limpiar_texto(el.get_text(" ", strip=True))
            if t:
                actual["texto"].append(t)

    # Cierre a nivel de página: banner tipo "[Actualización: Resultados
    # finales del proceso]" antes de la primera sección cierra todas.
    pagina_cerrada = bool(_RE_CERRADO.search(limpiar_texto(" ".join(preambulo))))

    padre_cerrado = False
    for s in secciones:
        s["texto_full"] = limpiar_texto(" ".join(s["texto"]))[:4000]
        # El marcador de cierre puede estar en el texto, en headings menores
        # o en los nombres de los PDFs ("Notificación de Resultados Finales")
        texto_pdfs = " ".join(t for t, _ in s["pdfs"])
        propio = bool(_RE_CERRADO.search(s["texto_full"] + " " + texto_pdfs))
        es_especialidad = bool(re.match(r"\s*especialidad", s["titulo"], re.I))
        if not es_especialidad:
            # sección "padre" (Planta/Escalafón): fija el estado heredable
            padre_cerrado = propio
            s["cerrado"] = pagina_cerrada or propio
        else:
            # "Especialidad de X" es subsección del concurso padre anterior
            s["cerrado"] = pagina_cerrada or propio or padre_cerrado
    return secciones


def construir_oferta(s: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = s["titulo"][:500]
    texto = s["texto_full"]

    fecha_pub = _fecha_es(texto)  # primera fecha larga = fecha del llamado
    fecha_cierre = None
    if m := _RE_RANGO_POSTULACION.search(texto):
        fecha_cierre = _fecha_es(m.group(2))
        ini = _fecha_es(m.group(1), anio_default=fecha_cierre.year if fecha_cierre else None)
        if ini:
            fecha_pub = ini

    bases_url = next((u for t, u in s["pdfs"]
                      if "base" in (t + u).lower() and "resultado" not in (t + u).lower()),
                     None)

    descripcion = texto[:1800]
    if bases_url:
        descripcion = limpiar_texto(descripcion) + f" | Bases: {bases_url}"

    t_low = f"{cargo} {texto}".lower()
    tipo = "Planta"  # los llamados PDI publicados son cargos titulares de planta
    if re.search(r"\bcontrata\b", t_low):
        tipo = "Contrata"
    elif "honorario" in t_low:
        tipo = "Honorarios"
    elif "código del trabajo" in t_low or "codigo del trabajo" in t_low:
        tipo = "Código del Trabajo"

    c_low = cargo.lower()
    if any(w in c_low for w in ("médico", "medico", "cirujano", "justicia",
                                "profesional", "perito", "químic", "quimic",
                                "bioquímic", "bioquimic", "informátic", "informatic")):
        nivel = "Profesional"
    elif any(w in c_low for w in ("apoyo general", "auxiliar", "administrativo",
                                  "conductor", "mecánico", "mecanico", "sonido",
                                  "telecomunicaciones", "dibujante")):
        nivel = "Técnico"
    else:
        nivel = "Profesional"

    slug = re.sub(r"[^a-z0-9]+", "-",
                  cargo.lower().replace("á", "a").replace("é", "e")
                  .replace("í", "i").replace("ó", "o").replace("ú", "u")).strip("-")[:80]

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, s["url_pagina"]),
        "institucion_id": fuente_id,  # id del catalogo de instituciones
        "fuente_id": None,            # FK a fuentes: no aplica a scraper standalone
        "url_original": f"{s['url_pagina']}#{slug}",
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": tipo,
        "nivel": nivel,
        "region": FUENTE["region"],  # los llamados PDI no indican región en HTML
        "ciudad": None,
        "renta_bruta_min": None,   # bases en PDF escaneado (sin capa de texto)
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": fecha_pub or _hoy_cl(),
        "fecha_cierre": fecha_cierre,
        "requisitos_texto": None,  # ídem: requisitos viven en las bases escaneadas
        "url_bases": bases_url,
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float,
               incluir_cerrados: bool, con_pdf: bool = True) -> list[dict]:
    session = requests.Session()
    getattr(config, "aplicar_proxy", lambda *_: None)(session)
    session.headers.update(_headers())

    ofertas: list[dict] = []
    vistos: set[str] = set()
    omitidas_cerradas = 0
    hoy = _hoy_cl()

    # ── Fuente 1: trabaja-con-nosotros (cargos contrata/jornal vigentes) ────
    r = _get(session, TRABAJA_URL)
    if r is not None:
        cargos = parsear_trabaja_con_nosotros(r.text)
        logger.info("  trabaja-con-nosotros: %d cargos vigentes", len(cargos))
        for c in cargos:
            if (c["plazo_fin"] and c["plazo_fin"] < hoy and not incluir_cerrados):
                omitidas_cerradas += 1
                continue
            perfil: dict[str, Any] = {}
            if con_pdf:
                time.sleep(delay)
                rp = _get(session, c["url_pdf"])
                if rp is not None and rp.content[:5] == b"%PDF-":
                    perfil = parsear_perfil_pdf(pdf_a_texto(rp.content))
            o = construir_oferta_contrata(c, perfil)
            if o["url_original"] not in vistos:
                vistos.add(o["url_original"])
                try:
                    from scrapers.enrich import enriquecer_oferta
                    enriquecer_oferta(o, texto_html=o.get("descripcion"))
                except Exception:
                    pass
                ofertas.append(o)
    else:
        logger.warning("  No se pudo acceder a trabaja-con-nosotros")

    if max_results and len(ofertas) >= max_results:
        return ofertas[:max_results]

    # ── Fuente 2: concursos públicos (cargos titulares de planta) ───────────
    time.sleep(delay)
    r = _get(session, INDICE)
    if r is None:
        logger.error("No se pudo acceder al índice de concursos (%s)", INDICE)
        logger.info("  Omitidas por proceso finalizado/vencido: %d", omitidas_cerradas)
        return ofertas

    subpaginas = descubrir_subpaginas(r.text, BASE)
    logger.info("  Subpáginas de plantas detectadas: %d", len(subpaginas))
    for url in subpaginas:
        time.sleep(delay)
        rp = _get(session, url)
        if rp is None:
            continue
        secciones = parsear_subpagina(rp.text, url)
        logger.info("  %s: %d secciones", url.rsplit("/", 1)[-1][:45], len(secciones))
        for s in secciones:
            if s["cerrado"] and not incluir_cerrados:
                omitidas_cerradas += 1
                continue
            o = construir_oferta(s)
            hoy = _hoy_cl()
            vencida = (o["fecha_cierre"] and o["fecha_cierre"] < hoy)
            # Sin fecha de cierre pero con llamado de hace más de 8 meses:
            # tratar como vencida (los llamados PDI duran semanas, no años)
            antigua = (o["fecha_cierre"] is None
                       and o["fecha_publicacion"]
                       and (hoy - o["fecha_publicacion"]).days > 240)
            if (vencida or antigua) and not incluir_cerrados:
                omitidas_cerradas += 1
                continue
            if o["url_original"] in vistos:
                continue
            vistos.add(o["url_original"])
            try:
                from scrapers.enrich import enriquecer_oferta
                enriquecer_oferta(o, texto_html=o.get("descripcion"))
            except Exception:
                pass
            ofertas.append(o)
            if max_results and len(ofertas) >= max_results:
                logger.info("  Omitidas por proceso finalizado: %d", omitidas_cerradas)
                return ofertas
    logger.info("  Omitidas por proceso finalizado: %d", omitidas_cerradas)
    return ofertas


# ── Persistencia / export ────────────────────────────────────────────────────
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
             delay=DELAY_DEFAULT, incluir_cerrados=False,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper PDI v2%s", " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    fuente_id = int(FUENTE["id"])
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    urls_activas: list[str] = []
    ofertas: list[dict] = []

    try:
        ofertas = recolectar(max_results, delay, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [PDI] {datos['cargo'][:62]:62} | "
                      f"pub: {datos['fecha_publicacion']} | cierre: {datos['fecha_cierre']}")
                if verbose:
                    print(f"      {datos['url_original'][:110]}")
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
        logger.exception("  Error fuente PDI: %s", exc)
    finally:
        if db is not None:
            try:
                db.rollback()
                registrar_log(db, fuente_id,
                              "OK" if stats["errores"] == 0 else "PARCIAL",
                              ofertas_nuevas=stats["nuevas"],
                              ofertas_actualizadas=stats["actualizadas"],
                              ofertas_cerradas=stats["cerradas"],
                              paginas=0, duracion=time.time() - inicio)
            except Exception:
                logger.exception("  No se pudo registrar log")
            db.close()

    if export and ofertas:
        _exportar(ofertas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN PDI: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"],
                stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper concursos públicos PDI (pdichile.cl), esquema estándar")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de ofertas")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT,
                   help=f"Pausa entre requests (default {DELAY_DEFAULT}s)")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="No omitir concursos con resultados finales publicados")
    p.add_argument("--export", default=None, metavar="PREFIJO",
                   help="Exportar además a PREFIJO.json y PREFIJO.csv")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, incluir_cerrados=a.incluir_cerrados, export=a.export)
