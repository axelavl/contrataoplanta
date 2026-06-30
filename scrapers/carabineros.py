"""
EmpleoEstado.cl — Scraper del Sistema de Postulaciones de Carabineros
(https://postulaciones.carabineros.cl, fuente 161). Versión 3.

Basado en el DOM real del sitio (verificado 06/2026):
    /                       listado en cards `div.concurso-card`, paginado ?page=N
    /concursos/{id}         detalle HTML: remuneración, objetivo, funciones,
                            formación educacional, conocimientos, competencias,
                            experiencia y documentación (todo lo que publique)
    /concursos/download/{id}/Perfil      PDF perfil de cargo (CON capa de texto:
                                         se parsea siempre que falte info)
    /concursos/download/{id}/Descriptor  PDF "Bases del Concurso" (ESCANEADO,
                                         sin capa de texto -> requiere --ocr;
                                         su URL se guarda en url_bases igual)

Antecedentes recogidos por oferta: además de los campos estándar, se persisten
`url_bases` (enlace a Bases/Perfil), `modalidad`, `horas_semanales`, y se
enriquecen `descripcion` (objetivo + funciones + lugar + vacantes) y
`requisitos_texto` (formación + conocimientos + competencias + experiencia +
documentación), combinando detalle HTML y PDFs adjuntos.

Cada card contiene: título/estamento (P.N.S., P.N.I., C.P.R., D.F.L.1),
fechas "Postulación desde el DD/MM/AAAA al DD/MM/AAAA", y pares
"Etiqueta : Valor" (Modalidad, Cargo, Jornada, Remuneración a veces, Lugar,
Ubicación, N° de vacantes). El detalle agrega remuneración y requisitos.

Salida: mismas columnas estándar del proyecto (idénticas a trabajando.py v2).

ADVERTENCIAS OPERATIVAS:
    * El WAF responde 503 "upstream connect error" a IPs extranjeras o de
      datacenter: ejecutar desde IP chilena. El scraper lo detecta y avisa.
    * El robots.txt restringe acceso automatizado: delay default 1.5s y
      ejecución poco frecuente.

Uso:
    python scrapers/carabineros.py --dry-run --verbose
    python scrapers/carabineros.py --sin-detalle      # solo cards (rápido)
    python scrapers/carabineros.py --export salida    # además CSV+JSON
    python scrapers/carabineros.py --ocr --dry-run    # OCR de las Bases (lento)
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
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qs

import requests
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
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
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
        return r.strip()

    def marcar_ofertas_cerradas(*a: Any, **k: Any) -> int:  # type: ignore[misc]
        return 0

    def registrar_log(*a: Any, **k: Any) -> None:  # type: ignore[misc]
        return None

    def upsert_oferta(*a: Any, **k: Any) -> tuple[bool, bool]:  # type: ignore[misc]
        return False, False

# ── Lectura de PDF de perfil (opcional) ─────────────────────────────────────
# El "Perfil" del cargo se publica como PDF con capa de texto; si el detalle
# HTML no trae requisitos, se descarga ese PDF y se extraen. No-op si pdfplumber
# no está o si no hay PDF. El "Descriptor" (bases) está escaneado: se ignora.
import io as _io

try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None  # type: ignore[assignment]

# OCR opcional (solo se usa con --ocr / allow_ocr=True). Requiere el binario
# `tesseract` instalado en el sistema con el idioma español (`tesseract-ocr-spa`)
# y el paquete Python `pytesseract`. Se importa de forma perezosa para no
# obligar a la dependencia cuando el OCR está desactivado.
try:
    import pytesseract  # type: ignore
except Exception:
    pytesseract = None  # type: ignore[assignment]

try:
    from extraction.requirements_extractor import extract_requirements
    from extraction.salary_extractor import extract_salary
except Exception:
    extract_requirements = None  # type: ignore[assignment]
    extract_salary = None  # type: ignore[assignment]


def _ocr_pdf(contenido: bytes, max_paginas: int = 8) -> str:
    """Texto de un PDF ESCANEADO vía OCR (tesseract). '' si falta tesseract,
    pytesseract o el backend de render. Pensado para el 'Descriptor'/Bases de
    Carabineros, que se publica como imagen sin capa de texto."""
    if pdfplumber is None or pytesseract is None:
        logger.info("  OCR no disponible (pytesseract/tesseract no instalado)")
        return ""
    if not contenido[:5].startswith(b"%PDF"):
        return ""
    paginas: list[str] = []
    try:
        with pdfplumber.open(_io.BytesIO(contenido)) as pdf:
            for p in pdf.pages[:max_paginas]:
                try:
                    img = p.to_image(resolution=200).original
                    paginas.append(pytesseract.image_to_string(img, lang="spa"))
                except Exception:
                    logger.exception("  Error OCR en una página")
    except Exception:
        logger.exception("  No se pudo abrir el PDF para OCR")
        return ""
    return "\n".join(paginas)


def _pdf_a_texto(contenido: bytes, max_paginas: int = 10,
                 allow_ocr: bool = False) -> str:
    """Texto del PDF. Primero capa de texto (pdfplumber); si está vacío y
    `allow_ocr`, recurre a OCR. '' si no se puede (cifrado / sin tesseract)."""
    if pdfplumber is None or not contenido[:5].startswith(b"%PDF"):
        return ""
    texto = ""
    try:
        with pdfplumber.open(_io.BytesIO(contenido)) as pdf:
            texto = "\n".join((p.extract_text() or "") for p in pdf.pages[:max_paginas])
    except Exception:
        logger.exception("  Error leyendo capa de texto del PDF")
    if texto.strip():
        return texto
    if allow_ocr:
        logger.info("  PDF sin capa de texto: intentando OCR…")
        return _ocr_pdf(contenido, max_paginas)
    return ""


def _datos_desde_pdf(texto: str) -> dict[str, Any]:
    """De un PDF de perfil/bases en texto saca requisitos_texto, renta y, si el
    documento trae objetivo/funciones, también descripcion_detalle."""
    out: dict[str, Any] = {}
    if not texto or len(texto) < 80:
        return out
    plano = limpiar_texto(texto)

    # 1) Requisitos vía el extractor genérico del proyecto.
    if extract_requirements is not None:
        try:
            req, des, docs = extract_requirements(texto)
            bloques = []
            if req:
                bloques.append("Requisitos: " + "; ".join(req))
            if des:
                bloques.append("Deseables: " + "; ".join(des))
            if docs:
                bloques.append("Documentos: " + "; ".join(docs[:6]))
            if bloques:
                out["requisitos_texto"] = limpiar_texto(" | ".join(bloques))[:1900]
        except Exception:
            logger.exception("  Error extrayendo requisitos del PDF")

    # 2) Secciones narrativas del PDF (mismo motor que el detalle HTML). Sirve
    #    de respaldo cuando el extractor genérico no encuentra requisitos y para
    #    rescatar objetivo/funciones del perfil de cargo.
    secciones = _extraer_secciones(plano)
    if secciones:
        consol = _consolidar(secciones)
        out.setdefault("requisitos_texto", consol.get("requisitos"))
        if consol.get("descripcion_detalle"):
            out["descripcion_detalle"] = consol["descripcion_detalle"]
        # limpia el setdefault si quedó None
        if out.get("requisitos_texto") is None:
            out.pop("requisitos_texto", None)

    # 3) Renta.
    if extract_salary is not None:
        try:
            sal = extract_salary(plano)
            if sal.amount:
                out["renta_bruta_min"] = int(sal.amount)
                if sal.raw:
                    out["renta_texto"] = limpiar_texto(sal.raw)[:200]
        except Exception:
            logger.exception("  Error extrayendo renta del PDF")
    return out


LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.carabineros")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "carabineros.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://postulaciones.carabineros.cl"
FUENTE = {
    "id": 161,
    "nombre": "Carabineros de Chile",
    "sigla": "CARAB",
    "sector": "Orden y Seguridad",
    "region": "Nacional",
    "url_empleo": BASE + "/",
}

HTTP_TIMEOUT = 30
MAX_RETRIES = 3
DELAY_DEFAULT = 1.5
MAX_PAGINAS = 20

_RE_ID = re.compile(r"/concursos/(?:download/|apply/)?(\d+)")
_RE_FECHAS = re.compile(
    r"desde el\s*(\d{1,2}/\d{1,2}/\d{4})\s*al\s*(\d{1,2}/\d{1,2}/\d{4})", re.I)
_RE_MONTO = re.compile(r"\$\s*([\d.]{5,12})")
_RE_REMUNERACION = re.compile(r"Remuneraci[oó]n(?:es)?\s*:?\s*\$\s*([\d.]+)", re.I)
_RE_HORAS = re.compile(r"(\d{1,3})\s*(?:hrs?|horas)\b", re.I)
_BLOQUEO_RE = re.compile(r"upstream connect error|connection timeout", re.I)
_ICONOS = ("contact_mail", "visibility", "check_circle", "insert_chart",
           "warning_amber", "help_outline", "login")

# Encabezados conocidos del detalle / de las bases. Sirven como "muros" para
# cortar cada sección: el contenido de una sección llega hasta el comienzo de la
# siguiente etiqueta conocida (o el fin del texto). Es más mantenible que un
# regex distinto por campo y tolera el orden variable del portal.
_STOP_SECCIONES = (
    r"Objetivo del [Cc]argo|Funciones del [Cc]argo|Funciones|"
    r"Formaci[oó]n Educacional|Formaci[oó]n|Requisitos(?: del [Cc]argo)?|"
    r"Conocimientos(?: Espec[ií]ficos| T[eé]cnicos)?|"
    r"Competencias(?: Conductuales| T[eé]cnicas)?|Experiencia(?: Laboral)?|"
    r"Documentaci[oó]n(?: Requerida)?|Documentos(?: Requeridos)?|"
    r"Remuneraci[oó]n(?:es)?|Modalidad|Jornada|Estamento|"
    r"Lugar de Desempe[nñ]o|Lugar|Ubicaci[oó]n|"
    r"N[°º] de [Vv]acantes|Vacantes|Postulaci[oó]n"
)

# Secciones narrativas largas (objetivo, funciones, requisitos por componente).
_SECCIONES_DETALLE = {
    "objetivo": r"Objetivo del [Cc]argo",
    "funciones": r"Funciones del [Cc]argo|Funciones",
    "formacion": r"Formaci[oó]n Educacional|Formaci[oó]n",
    "conocimientos": r"Conocimientos(?: Espec[ií]ficos| T[eé]cnicos)?",
    "competencias": r"Competencias(?: Conductuales| T[eé]cnicas)?",
    "experiencia": r"Experiencia(?: Laboral)?",
    "documentacion": r"Documentaci[oó]n(?: Requerida)?|Documentos(?: Requeridos)?",
    "requisitos": r"Requisitos(?: del [Cc]argo)?",
}

# Campos cortos "Etiqueta: valor" que el detalle a veces agrega.
_CAMPOS_DETALLE = {
    "modalidad": r"Modalidad",
    "jornada": r"Jornada",
    "lugar": r"Lugar de Desempe[nñ]o|Lugar",
    "vacantes": r"N[°º] de [Vv]acantes|Vacantes",
}


def _seccion(texto: str, inicio_re: str, max_len: int = 1800) -> str | None:
    """Contenido de la sección que empieza en `inicio_re` hasta la próxima
    etiqueta conocida (`_STOP_SECCIONES`) o el fin del texto."""
    m = re.search(
        rf"(?:{inicio_re})\s*:?\s*(.{{20,{max_len}}}?)"
        rf"(?=\s*(?:{_STOP_SECCIONES})\s*:|\Z)",
        texto, re.I | re.S)
    return limpiar_texto(m.group(1)) if m else None


def _campo_corto(texto: str, inicio_re: str, max_len: int = 80) -> str | None:
    """Valor corto de un par 'Etiqueta: valor' (modalidad, jornada, etc.)."""
    m = re.search(
        rf"(?:{inicio_re})\s*:?\s*(.{{2,{max_len}}}?)"
        rf"(?=\s*(?:{_STOP_SECCIONES})\s*:|[|\n]|\Z)",
        texto, re.I)
    return limpiar_texto(m.group(1)) if m else None


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _headers() -> dict[str, str]:
    return {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
        "Referer": BASE + "/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code == 503 and _BLOQUEO_RE.search(r.text or ""):
                logger.error(
                    "BLOQUEO DETECTADO en %s: el WAF responde 503 a IPs "
                    "extranjeras/datacenter. Ejecutar desde una IP chilena.", url)
                return None
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


# ── Utilidades de parseo ─────────────────────────────────────────────────────
def _sin_iconos(texto: str) -> str:
    for ic in _ICONOS:
        texto = texto.replace(ic, " ")
    return limpiar_texto(texto)


def _fecha(s: str) -> date | None:
    try:
        d, m, a = s.strip().split("/")
        return date(int(a), int(m), int(d))
    except (ValueError, AttributeError):
        return None


def _monto(s: str | None) -> int | None:
    if not s:
        return None
    m = _RE_MONTO.search(s) or re.search(r"([\d.]{5,12})", s)
    if not m:
        return None
    try:
        return int(m.group(1).replace(".", "").rstrip("-").rstrip("."))
    except ValueError:
        return None


# ── Parseo del listado (cards) — puro, testeable sin red ───────────────────
def parsear_listado(html: str, base_url: str = BASE) -> list[dict[str, Any]]:
    """Extrae los concursos de las cards `div.concurso-card`."""
    soup = BeautifulSoup(html, "html.parser")
    concursos: list[dict[str, Any]] = []

    for card in soup.select("div.concurso-card"):
        c: dict[str, Any] = {"campos": {}, "docs": {}, "titulo": "",
                             "url": "", "id": None,
                             "fecha_inicio": None, "fecha_cierre": None,
                             "descripcion": ""}
        # Header: título/estamento + fechas (dos variantes de markup)
        hdr = card.select_one(".block-header")
        if hdr:
            hdr_txt = _sin_iconos(hdr.get_text(" ", strip=True))
            if m := _RE_FECHAS.search(hdr_txt):
                c["fecha_inicio"] = _fecha(m.group(1))
                c["fecha_cierre"] = _fecha(m.group(2))
            c["titulo"] = limpiar_texto(hdr_txt.split("Postulación")[0])[:200]

        # Pares "Etiqueta : Valor" en las listas
        for li in card.find_all("li"):
            txt = limpiar_texto(li.get_text(" ", strip=True))
            if ":" in txt:
                k, v = txt.split(":", 1)
                k = limpiar_texto(k).lower().replace("°", "").replace("é", "e")
                c["campos"][k] = limpiar_texto(v)

        # Descripción larga de la card
        if desc := card.select_one(".text-justify"):
            c["descripcion"] = limpiar_texto(desc.get_text(" ", strip=True))[:3000]

        # Links: id, detalle y documentos
        for a in card.find_all("a", href=True):
            href = a["href"]
            m = _RE_ID.search(href)
            if not m:
                continue
            c["id"] = c["id"] or int(m.group(1))
            url_abs = urljoin(base_url, href)
            if "/download/" in href:
                c["docs"][href.rstrip("/").rsplit("/", 1)[-1]] = url_abs
            elif "/apply/" not in href:
                c["url"] = url_abs

        if c["id"] is None:
            continue
        if not c["url"]:
            c["url"] = f"{base_url}/concursos/{c['id']}"
        concursos.append(c)
    return concursos


_RE_GOTOPAGE = re.compile(r"gotoPage\((\d+)")


def extraer_total_paginas(html: str) -> int:
    """Número total de páginas según el paginador (Livewire o links ?page=).

    El sitio usa Laravel Livewire: los botones de paginación no son <a href>
    sino <button wire:click="gotoPage(N, 'page')">. El paginador igualmente
    acepta ?page=N en la URL en carga completa de página.
    """
    total = 1
    for m in _RE_GOTOPAGE.finditer(html):
        total = max(total, int(m.group(1)))
    # Fallback por si en el futuro vuelven a links con href
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        if "page=" not in a["href"]:
            continue
        try:
            n = int(parse_qs(urlparse(urljoin(BASE, a["href"])).query)
                    .get("page", ["0"])[0])
            total = max(total, n)
        except ValueError:
            continue
    return total


# ── Parseo del detalle — puro, testeable sin red ────────────────────────────
def _consolidar(secciones: dict[str, str]) -> dict[str, Any]:
    """Arma `requisitos` (componentes) y `descripcion_detalle` (objetivo +
    funciones) a partir de las secciones narrativas extraídas. Reutilizable
    tanto para el detalle HTML como para los PDFs."""
    out: dict[str, Any] = {}
    bloques = []
    for etiqueta, clave in (
        ("Formación", "formacion"), ("Conocimientos", "conocimientos"),
        ("Competencias", "competencias"), ("Experiencia", "experiencia"),
        ("Documentación", "documentacion"),
    ):
        if secciones.get(clave):
            bloques.append(f"{etiqueta}: {secciones[clave]}")
    if bloques:
        out["requisitos"] = limpiar_texto(" | ".join(bloques))[:2000]
    elif secciones.get("requisitos"):
        out["requisitos"] = secciones["requisitos"][:2000]

    partes = []
    if secciones.get("objetivo"):
        partes.append("Objetivo: " + secciones["objetivo"])
    if secciones.get("funciones"):
        partes.append("Funciones: " + secciones["funciones"])
    if partes:
        out["descripcion_detalle"] = limpiar_texto(" | ".join(partes))[:2000]
    return out


def _extraer_secciones(texto: str) -> dict[str, str]:
    """Todas las secciones narrativas conocidas presentes en `texto`."""
    secciones: dict[str, str] = {}
    for clave, patron in _SECCIONES_DETALLE.items():
        if val := _seccion(texto, patron):
            secciones[clave] = val[:1800]
    return secciones


def parsear_detalle(html: str) -> dict[str, Any]:
    """Extrae del detalle HTML todos los antecedentes disponibles:
    remuneración, objetivo, funciones, formación educacional, conocimientos,
    competencias, experiencia, documentación, además de modalidad, jornada,
    lugar y vacantes cuando el portal los publica en el detalle."""
    soup = BeautifulSoup(html, "html.parser")
    texto = _sin_iconos(soup.get_text(" ", strip=True))
    datos: dict[str, Any] = {}

    # Remuneración (monto + texto crudo)
    if m := _RE_REMUNERACION.search(texto):
        datos["renta"] = _monto(m.group(0))
        datos["renta_texto"] = "$" + m.group(1).rstrip(".").rstrip("-") + " (según portal)"

    # Secciones narrativas + consolidación a requisitos/descripcion_detalle
    secciones = _extraer_secciones(texto)
    if secciones:
        datos["secciones"] = secciones
        datos.update(_consolidar(secciones))

    # Campos cortos etiqueta:valor (no pisan lo que ya trae la card)
    for clave, patron in _CAMPOS_DETALLE.items():
        if val := _campo_corto(texto, patron):
            datos[clave] = val

    return datos


# ── Mapeo al esquema estándar ───────────────────────────────────────────────
def _tipo_cargo(modalidad: str, titulo: str = "") -> str:
    m = f"{modalidad} {titulo}".lower()
    if "código del trabajo" in m or "codigo del trabajo" in m:
        return "Código del Trabajo"
    # D.F.L.1 / "Contratado con Fondos Propios" = régimen Código del Trabajo
    # (según las bases publicadas por la institución)
    if "fondos propios" in m or "d.f.l" in m or "dfl" in m:
        return "Código del Trabajo"
    if re.search(r"\bcontrata\b", m):  # no confundir con "contratado"
        return "Contrata"
    if "honorario" in m:
        return "Honorarios"
    if "planta" in m or "nombramiento" in m:
        return "Planta"
    return limpiar_texto(modalidad)[:50] or "Código del Trabajo"


def _nivel(cargo: str, titulo: str) -> str:
    t = f"{cargo} {titulo}".lower()
    if any(w in t for w in ("director", "directora", "jefe", "jefa", "oficial")):
        return "Directivo"
    if "profesional" in t or any(w in t for w in (
            "cirujano", "médico", "medico", "enfermer", "psicólog", "psicolog",
            "ingenier", "abogad", "periodista", "químic", "quimic")):
        return "Profesional"
    if any(w in t for w in ("técnico", "tecnico", "auxiliar", "administrativo",
                            "administrativa", "conductor", "cociner", "garzón",
                            "garzon", "peluquer", "telefonista", "operador")):
        return "Técnico"
    return "Profesional"


def _horas_semanales(*textos: str | None) -> int | None:
    """Extrae las horas de jornada (p.ej. '44 horas semanales' -> 44)."""
    for t in textos:
        if not t:
            continue
        if m := _RE_HORAS.search(t):
            try:
                h = int(m.group(1))
                if 1 <= h <= 60:
                    return h
            except ValueError:
                pass
    return None


def construir_oferta(c: dict[str, Any], det: dict[str, Any]) -> dict:
    """Card (+detalle +PDFs) -> fila con las columnas estándar del proyecto."""
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    campos = c["campos"]
    cargo = (campos.get("cargo") or c["titulo"] or "")[:500]

    ciudad, region = None, None
    if ubic := campos.get("ubicacion") or campos.get("ubicación"):
        partes = [p.strip() for p in ubic.split(",") if p.strip()]
        if len(partes) >= 2:
            ciudad, region = partes[0], normalizar_region(partes[-1])
        elif partes:
            region = normalizar_region(partes[0])

    renta = _monto(campos.get("remuneracion")) or det.get("renta")
    renta_texto = None
    if campos.get("remuneracion"):
        renta_texto = campos["remuneracion"][:100]
    elif det.get("renta_texto"):
        renta_texto = det["renta_texto"]

    # Modalidad y jornada: la card primero, el detalle como respaldo.
    modalidad = limpiar_texto(campos.get("modalidad") or det.get("modalidad") or "")[:50] or None
    jornada = campos.get("jornada") or det.get("jornada")
    horas = _horas_semanales(jornada, campos.get("modalidad"))

    # Enlace a las bases del concurso (Descriptor); si no hay, el Perfil de cargo.
    docs = c.get("docs") or {}
    url_bases = _buscar_doc(docs, "descriptor", "bases") or _buscar_doc(docs, "perfil")

    # Descripción ampliada: encabezado + campos clave + objetivo/funciones.
    desc_partes = [c["titulo"]]
    for etiqueta, valor in (
        ("Modalidad", modalidad), ("Jornada", jornada),
        ("Lugar", campos.get("lugar") or det.get("lugar")),
        ("Vacantes", campos.get("n de vacantes") or det.get("vacantes")),
    ):
        if valor:
            desc_partes.append(f"{etiqueta}: {valor}")
    if det.get("descripcion_detalle"):
        desc_partes.append(det["descripcion_detalle"])
    elif c["descripcion"]:
        desc_partes.append(c["descripcion"])
    descripcion = limpiar_texto(" | ".join(p for p in desc_partes if p))

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, str(c["id"])),
        "institucion_id": fuente_id,  # id del catalogo de instituciones
        "fuente_id": None,            # FK a fuentes: no aplica a scraper standalone
        "url_original": c["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo_cargo(campos.get("modalidad", ""), c["titulo"]),
        "nivel": _nivel(cargo, c["titulo"]),
        "region": region or FUENTE["region"],
        "ciudad": ciudad,
        "renta_bruta_min": renta,
        "renta_bruta_max": None,
        "renta_texto": renta_texto,
        "url_bases": url_bases,
        "modalidad": modalidad,
        "horas_semanales": horas,
        "fecha_publicacion": c["fecha_inicio"] or date.today(),
        "fecha_cierre": c["fecha_cierre"],
        "requisitos_texto": det.get("requisitos"),
        "url_bases": next(
            (u for k, u in (c.get("docs") or {}).items()
             if any(w in k.lower() for w in ("perfil", "bases", "descriptor"))),
            None,
        ),
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def _buscar_doc(docs: dict[str, str], *claves: str) -> str | None:
    """URL del primer documento cuyo nombre contiene alguna de `claves`."""
    for k, u in (docs or {}).items():
        kl = k.lower()
        if any(cl in kl for cl in claves):
            return u
    return None


def _fusionar_pdf(det: dict[str, Any], pdf_datos: dict[str, Any],
                  concurso_id: Any, etiqueta: str) -> None:
    """Vuelca al detalle los antecedentes de un PDF sin pisar lo ya presente."""
    if not pdf_datos:
        return
    if pdf_datos.get("requisitos_texto") and not det.get("requisitos"):
        det["requisitos"] = pdf_datos["requisitos_texto"]
    if pdf_datos.get("descripcion_detalle") and not det.get("descripcion_detalle"):
        det["descripcion_detalle"] = pdf_datos["descripcion_detalle"]
    if pdf_datos.get("renta_bruta_min") and not det.get("renta"):
        det["renta"] = pdf_datos["renta_bruta_min"]
        det.setdefault("renta_texto", pdf_datos.get("renta_texto"))
    logger.info("  %s PDF concurso %s: %s",
                etiqueta, concurso_id, ", ".join(sorted(pdf_datos)))


def recolectar(max_results: int | None, con_detalle: bool, delay: float,
               allow_ocr: bool = False) -> list[dict]:
    session = requests.Session()
    getattr(config, "aplicar_proxy", lambda *_: None)(session)
    session.headers.update(_headers())

    r = _get(session, BASE + "/")
    if r is None:
        return []

    cards = parsear_listado(r.text, BASE)
    vistos = {c["id"] for c in cards}
    total = min(extraer_total_paginas(r.text), MAX_PAGINAS)
    logger.info("  Paginador: %d página(s) detectadas", total)

    pagina = 2
    while pagina <= total:
        time.sleep(delay)
        rp = _get(session, f"{BASE}/?page={pagina}")
        if rp is None:
            break
        nuevos = 0
        for c in parsear_listado(rp.text, BASE):
            if c["id"] not in vistos:
                vistos.add(c["id"])
                cards.append(c)
                nuevos += 1
        logger.info("  Página %d/%d: %d concursos nuevos", pagina, total, nuevos)
        if nuevos == 0:
            # ?page=N devolvió lo mismo (paginador no URL-driven): detener
            logger.warning(
                "  La página %d no aportó concursos nuevos; se detiene la "
                "paginación. Si el paginador dejó de aceptar ?page=N, avisar "
                "para implementar el protocolo Livewire.", pagina)
            break
        total = min(max(total, extraer_total_paginas(rp.text)), MAX_PAGINAS)
        pagina += 1

    logger.info("  Listado: %d concursos en %d páginas", len(cards), pagina - 1)
    cards.sort(key=lambda c: -c["id"])
    if max_results:
        cards = cards[:max_results]

    ofertas: list[dict] = []
    for i, c in enumerate(cards, 1):
        det: dict[str, Any] = {}
        if con_detalle:
            time.sleep(delay)
            rd = _get(session, c["url"])
            if rd is not None:
                det = parsear_detalle(rd.text)

            docs = c.get("docs") or {}
            perfil_url = _buscar_doc(docs, "perfil")
            bases_url = _buscar_doc(docs, "descriptor", "bases")

            # PDF "Perfil de Cargo": tiene capa de texto. Se baja para completar
            # requisitos / renta / objetivo-funciones que el detalle HTML no
            # haya entregado.
            if perfil_url and not (det.get("requisitos") and det.get("renta")):
                time.sleep(delay)
                rp = _get(session, perfil_url)
                if rp is not None:
                    _fusionar_pdf(det, _datos_desde_pdf(_pdf_a_texto(rp.content)),
                                  c["id"], "Perfil")

            # PDF "Descriptor"/Bases del Concurso: escaneado (sin texto). Solo se
            # procesa con OCR habilitado; de lo contrario solo se enlaza.
            if bases_url and allow_ocr and not det.get("requisitos"):
                time.sleep(delay)
                rb = _get(session, bases_url)
                if rb is not None:
                    _fusionar_pdf(
                        det,
                        _datos_desde_pdf(_pdf_a_texto(rb.content, allow_ocr=True)),
                        c["id"], "Bases(OCR)")
        oferta = construir_oferta(c, det)
        try:
            from scrapers.enrich import enriquecer_oferta
            enriquecer_oferta(oferta, texto_html=oferta.get("descripcion"))
        except Exception:
            pass
        ofertas.append(oferta)
        if i % 10 == 0:
            logger.info("  Procesados %d/%d", i, len(cards))
    return ofertas


# ── Persistencia / export ────────────────────────────────────────────────────
CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "modalidad", "horas_semanales",
                 "fecha_publicacion", "fecha_cierre", "url_original", "url_bases",
                 "descripcion", "requisitos_texto"]


def _exportar(ofertas: list[dict], prefijo: str) -> None:
    serializable = []
    for o in ofertas:
        fila = {k: o.get(k) for k in CAMPOS_EXPORT}
        for k in ("fecha_publicacion", "fecha_cierre"):
            if isinstance(fila[k], date):
                fila[k] = fila[k].isoformat()
        serializable.append(fila)
    Path(prefijo + ".json").write_text(
        json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(prefijo + ".csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_EXPORT)
        w.writeheader()
        w.writerows(serializable)
    logger.info("Exportado: %s.json / %s.csv (%d ofertas)", prefijo, prefijo, len(ofertas))


def ejecutar(
    dry_run: bool = False,
    verbose: bool = False,
    max_results: int | None = None,
    con_detalle: bool = True,
    delay: float = DELAY_DEFAULT,
    export: str | None = None,
    allow_ocr: bool = False,
) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Carabineros v3%s", " (standalone)" if STANDALONE else "")
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
        ofertas = recolectar(max_results=max_results, con_detalle=con_detalle,
                             delay=delay, allow_ocr=allow_ocr)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))

        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [CARAB] {datos['cargo'][:60]:60} | {datos['region'] or '':28} "
                      f"| cierre: {datos['fecha_cierre']}")
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
        logger.exception("  Error fuente Carabineros: %s", exc)
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
                logger.exception("  No se pudo registrar log")
            db.close()

    if export and ofertas:
        _exportar(ofertas, export)

    dur = time.time() - inicio
    logger.info(
        "RESUMEN Carabineros: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
        stats["encontradas"], stats["nuevas"], stats["actualizadas"],
        stats["cerradas"], stats["errores"], dur,
    )
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    parser = argparse.ArgumentParser(
        description="Scraper postulaciones.carabineros.cl (esquema estándar EmpleoEstado)"
    )
    parser.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--max", type=int, default=None, help="Tope de concursos")
    parser.add_argument("--sin-detalle", action="store_true",
                        help="No visitar /concursos/{id}; solo cards del listado")
    parser.add_argument("--delay", type=float, default=DELAY_DEFAULT,
                        help=f"Pausa entre requests (default {DELAY_DEFAULT}s)")
    parser.add_argument("--export", default=None, metavar="PREFIJO",
                        help="Exportar además a PREFIJO.json y PREFIJO.csv")
    parser.add_argument("--ocr", action="store_true",
                        help="Habilita OCR del PDF de Bases (escaneado). Requiere "
                             "tesseract + idioma spa y pytesseract instalados.")
    args = parser.parse_args()
    ejecutar(
        dry_run=args.dry_run,
        verbose=args.verbose,
        max_results=args.max,
        con_detalle=not args.sin_detalle,
        delay=args.delay,
        export=args.export,
        allow_ocr=args.ocr,
    )
