"""
EmpleoEstado.cl — Scraper de concursos de la Armada de Chile
(https://www.admisionarmada.cl/concursos/). Patrón estándar de sesión.

Sitio WordPress, render en servidor. Estructura verificada (06/2026):

  Listado /concursos/ (paginado ?paged=N; filtrable ?status=...):
    a.proceso.{estado}                ← LA CLASE CSS TRAE EL ESTADO:
                                        postulacion-abierta, postulacion-cerrada,
                                        seleccionando-candidatos,
                                        concurso-terminado, concurso-desierto,
                                        concurso-cancelado
      ├ .info-proceso                 → categoría (Profesionales civiles /
      │                                  Gente de Mar / Oficiales de línea)
      ├ h3.titular-articulo           → título (suele terminar en ", Ciudad.")
      └ .info-estado                  → estado en texto

    Fallback (si el tema cambia las clases): se descubren los enlaces
    /concursos/{slug}/ y el estado/categoría se leen del texto visible de
    la card. Así el scraper no depende de una sola clase CSS.

  Detalle /concursos/{codigo|slug}/:
    "POSTULACIÓN HASTA: 29 MAYO 2026 a las 23:59" → fecha de cierre
    "Postulación abierta hasta el 19 de julio del 2026 a las 23:59" → cierre
    "NN VACANTE(S) DISPONIBLE(S)"                 → vacantes
    "Personal a Contrata Renta Global Grado N"    → régimen y grado
    meta article:published_time                   → fecha de publicación real
    link "Bases de Postulación" (.pdf)            → bases

Esto corrige los dos defectos del scraper anterior para esta fuente:
(1) tomaba concursos ya cerrados como vigentes — ahora el estado se lee de
la clase CSS (o del texto) de cada card y solo se importan los
"postulacion-abierta" (--incluir-cerrados los muestra todos); (2) no extraía
detalle — ahora se visita cada concurso abierto y se obtienen cierre,
vacantes, régimen, grado, ciudad/región (mapeada desde el título) y bases.

Uso:
    python scrapers/armada.py --dry-run --verbose
    python scrapers/armada.py --dry-run --export ofertas_armada
    python scrapers/armada.py --incluir-cerrados --max 30
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
from urllib.parse import urljoin, urlparse

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

# ── Lectura de PDF de bases (opcional) ──────────────────────────────────────
# Si la ficha enlaza las bases en PDF, se descarga y se extraen requisitos/renta.
# Es no-op si pdfplumber no está instalado o si no hay PDF en la ficha.
import io as _io

try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None  # type: ignore[assignment]

try:
    from extraction.requirements_extractor import extract_requirements
    from extraction.salary_extractor import extract_salary
except Exception:
    extract_requirements = None  # type: ignore[assignment]
    extract_salary = None  # type: ignore[assignment]


LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.armada")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "armada.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://www.admisionarmada.cl"
LISTADO_URL = BASE + "/concursos/"
# id 158 = "Armada de Chile — Personal Civil" en
# repositorio_instituciones_publicas_chile.json. OJO: 164 es ASMAR, NO la Armada.
FUENTE = {
    "id": 158,
    "nombre": "Armada de Chile",
    "sigla": "ARMADA",
    "sector": "FF.AA. y Orden",
    "region": "Nacional",
    "url_empleo": LISTADO_URL,
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0
MAX_PAGINAS = 25

ESTADOS_CSS = ("postulacion-abierta", "postulacion-cerrada",
               "seleccionando-candidatos", "concurso-terminado",
               "concurso-desierto", "concurso-cancelado")

# Texto visible -> slug de estado (fallback cuando la clase CSS no está)
ESTADOS_TEXTO = {
    "postulacion abierta": "postulacion-abierta",
    "postulación abierta": "postulacion-abierta",
    "postulacion cerrada": "postulacion-cerrada",
    "postulación cerrada": "postulacion-cerrada",
    "seleccionando candidatos": "seleccionando-candidatos",
    "concurso terminado": "concurso-terminado",
    "concurso desierto": "concurso-desierto",
    "concurso cancelado": "concurso-cancelado",
}

CATEGORIAS_TEXTO = ("profesionales civiles", "gente de mar", "oficiales de línea",
                    "oficiales de linea")

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

# Ciudades frecuentes en títulos de la Armada -> región (corrige el
# "Región de Nacional" genérico del scraper anterior)
CIUDAD_REGION = {
    "valparaíso": "Valparaíso", "valparaiso": "Valparaíso",
    "viña del mar": "Valparaíso", "vina del mar": "Valparaíso",
    "con cón": "Valparaíso", "con con": "Valparaíso", "concón": "Valparaíso",
    "concon": "Valparaíso", "quintero": "Valparaíso",
    "santiago": "Metropolitana de Santiago",
    "talcahuano": "Biobío", "concepción": "Biobío", "concepcion": "Biobío",
    "iquique": "Tarapacá", "antofagasta": "Antofagasta",
    "puerto montt": "Los Lagos", "puerto williams": "Magallanes",
    "punta arenas": "Magallanes", "coquimbo": "Coquimbo",
}

_RE_HASTA = re.compile(
    r"POSTULACI[OÓ]N HASTA\s*:?\s*(\d{1,2})\s+DE?\s*(\w+)\s+(?:DE(?:L)?\s+)?(\d{4})", re.I)
_RE_HASTA_DDMM = re.compile(r"hasta el\s+(\d{1,2})/(\d{1,2})/(\d{4})", re.I)
_RE_VACANTES = re.compile(r"(\d{1,3})\s+VACANTES?\s+DISPONIBLES?", re.I)
_RE_GRADO = re.compile(
    r"(contrata|c[oó]digo del trabajo|honorarios?|planta|jornal)[^.]{0,60}?"
    r"(?:grado\s+(\d{1,2}))?", re.I)
_RE_RENTA_GRADO = re.compile(r"renta global grado\s+(\d{1,2})", re.I)

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_id", "institucion_nombre",
                 "sector", "cargo", "area_profesional", "tipo_cargo", "nivel",
                 "region", "ciudad", "renta_bruta_min", "renta_bruta_max",
                 "renta_texto", "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    getattr(config, "aplicar_proxy", lambda *_: None)(s)
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
        "Referer": BASE + "/",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
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
def _es_url_concurso(href: str) -> bool:
    """True si href apunta a un concurso individual /concursos/{slug}/ (no al
    índice, no a una URL con query/filtro/paginación)."""
    if not href:
        return False
    p = urlparse(href)
    if p.query:  # ?status=..., ?paged=...
        return False
    partes = [x for x in p.path.split("/") if x]
    # ['concursos'] = índice; ['concursos', '<slug>'] = concurso
    return len(partes) == 2 and partes[0] == "concursos"


def _estado_desde_texto(texto: str) -> str:
    t = (texto or "").lower()
    for frase, slug in ESTADOS_TEXTO.items():
        if frase in t:
            return slug
    return ""


def _categoria_desde_texto(texto: str) -> str:
    t = (texto or "").lower()
    for cat in CATEGORIAS_TEXTO:
        if cat in t:
            # devolvemos con mayúsculas estándar
            return {"profesionales civiles": "Profesionales civiles",
                    "gente de mar": "Gente de Mar",
                    "oficiales de línea": "Oficiales de línea",
                    "oficiales de linea": "Oficiales de línea"}[cat]
    return ""


def _titulo_desde_card(texto: str, categoria: str, estado_txt: str) -> str:
    """Limpia el texto concatenado de la card del listado para quedarse con el
    título. Ej: 'Gente de MarEscuela de Grumetes 2027 Postulación abierta Más
    información' -> 'Escuela de Grumetes 2027'."""
    t = limpiar_texto(texto)
    # quitar prefijo de categoría (puede venir pegado al título)
    for cat in (categoria, "Profesionales civiles", "Gente de Mar",
                "Oficiales de línea", "Oficiales de linea"):
        if cat and t.lower().startswith(cat.lower()):
            t = t[len(cat):]
            break
    # quitar sufijos de estado y "Más información"
    for frase in list(ESTADOS_TEXTO.keys()) + ["más información", "mas información",
                                               "mas informacion", "más informacion"]:
        idx = t.lower().find(frase)
        if idx != -1:
            t = t[:idx]
    return limpiar_texto(t)


def parsear_listado(html: str, base_url: str = BASE) -> list[dict[str, Any]]:
    """Cards a.proceso del listado -> dicts con estado leído de la clase CSS.
    Si el tema no usa a.proceso, cae a un descubrimiento por enlaces /concursos/
    leyendo estado y categoría del texto visible."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    vistos: set[str] = set()

    # ── Vía primaria: a.proceso (clase CSS con el estado) ──
    for a in soup.select("a.proceso[href]"):
        clases = a.get("class") or []
        estado = next((c for c in clases if c in ESTADOS_CSS), "")
        texto_card = a.get_text(" ", strip=True)
        if not estado:  # clase presente pero sin estado -> leer del texto
            estado = _estado_desde_texto(texto_card)
        titulo_el = a.select_one("h3.titular-articulo, h3, h2")
        cat_el = a.select_one(".info-proceso")
        titulo = limpiar_texto(titulo_el.get_text(" ", strip=True)) if titulo_el else ""
        categoria = limpiar_texto(cat_el.get_text(" ", strip=True)) if cat_el else ""
        if not categoria:
            categoria = _categoria_desde_texto(texto_card)
        if not titulo:
            titulo = _titulo_desde_card(texto_card, categoria, estado)
        if not titulo:
            continue
        url = urljoin(base_url, a["href"])
        vistos.add(url)
        items.append({
            "titulo": titulo[:500],
            "categoria": categoria,
            "estado": estado,
            "abierto": estado == "postulacion-abierta",
            "url": url,
        })

    if items:
        return items

    # ── Fallback: descubrir enlaces de concurso y leer del texto ──
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not _es_url_concurso(href):
            continue
        url = urljoin(base_url, href)
        if url in vistos:
            continue
        texto_card = a.get_text(" ", strip=True)
        estado = _estado_desde_texto(texto_card)
        categoria = _categoria_desde_texto(texto_card)
        titulo = _titulo_desde_card(texto_card, categoria, estado)
        # descartar enlaces de navegación sin contenido real de concurso
        if not titulo or len(titulo) < 6:
            continue
        vistos.add(url)
        items.append({
            "titulo": titulo[:500],
            "categoria": categoria,
            "estado": estado,
            "abierto": estado == "postulacion-abierta",
            "url": url,
        })
    return items


def parsear_detalle(html: str) -> dict[str, Any]:
    """Detalle del concurso: cierre, vacantes, régimen/grado, bases, fechas."""
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True))
    d: dict[str, Any] = {}

    if m := _RE_HASTA.search(texto):
        dia, mes, anio = m.groups()
        mes_n = MESES.get(mes.lower())
        if mes_n:
            try:
                d["fecha_cierre"] = date(int(anio), mes_n, int(dia))
            except ValueError:
                pass
    if "fecha_cierre" not in d and (m := _RE_HASTA_DDMM.search(texto)):
        try:
            d["fecha_cierre"] = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    # variante "Postulación abierta hasta el 19 de julio del 2026"
    if "fecha_cierre" not in d and (m := re.search(
            r"hasta el\s+(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", texto, re.I)):
        mes_n = MESES.get(m.group(2).lower())
        if mes_n:
            try:
                d["fecha_cierre"] = date(int(m.group(3)), mes_n, int(m.group(1)))
            except ValueError:
                pass

    if m := _RE_VACANTES.search(texto):
        d["vacantes"] = int(m.group(1))

    if m := _RE_RENTA_GRADO.search(texto):
        d["grado"] = m.group(1)
        d["renta_texto"] = f"Renta Global Grado {m.group(1)}"
    t_low = texto.lower()
    if "contrata" in t_low and "código del trabajo" not in t_low:
        d["tipo"] = "Contrata"
    elif "código del trabajo" in t_low or "codigo del trabajo" in t_low:
        d["tipo"] = "Código del Trabajo"
    elif "honorario" in t_low:
        d["tipo"] = "Honorarios"
    elif "jornal" in t_low:
        d["tipo"] = "Jornal"

    # fecha de publicación real desde el meta de WordPress
    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and meta.get("content"):
        try:
            d["fecha_publicacion"] = datetime.fromisoformat(
                meta["content"].replace("Z", "+00:00")).date()
        except ValueError:
            pass

    # bases: link cuyo texto sea "Bases de Postulación" o primer PDF de uploads
    for a in soup.find_all("a", href=True):
        if ".pdf" not in a["href"].lower():
            continue
        if "bases" in limpiar_texto(a.get_text(" ", strip=True)).lower() \
                or "bases" in a["href"].lower():
            d["bases_url"] = a["href"]
            break
    if "bases_url" not in d:
        pdfs = [a["href"] for a in soup.find_all("a", href=True)
                if ".pdf" in a["href"].lower() and "/uploads/" in a["href"]]
        if pdfs:
            d["bases_url"] = pdfs[0]

    if m := re.search(r"(Proceso de Selecci[oó]n de .{30,1200}?)(?:Instrucciones|POSTULAR|$)",
                      texto, re.I | re.S):
        d["descripcion"] = limpiar_texto(m.group(1))[:1800]
    d["texto_detalle"] = texto
    return d


def _ciudad_region(titulo: str) -> tuple[str | None, str]:
    t = titulo.lower()
    for ciudad, region in CIUDAD_REGION.items():
        if ciudad in t:
            return ciudad.title(), normalizar_region(region) or region
    return None, FUENTE["region"]  # procesos nacionales (Grumetes, Tropa, etc.)


def _nivel(titulo: str, categoria: str) -> str:
    t = f"{titulo} {categoria}".lower()
    if "profesionales civiles" in t or any(
            w in t for w in ("ingenier", "abogad", "contador", "médic", "medic",
                             "psicolog", "oficial")):
        return "Profesional"
    if any(w in t for w in ("gente de mar", "grumete", "tropa", "mecánico",
                            "mecanico", "conductor", "músico", "musico",
                            "técnico", "tecnico", "maestranza")):
        return "Técnico"
    return "Profesional"


def _pdf_a_texto(contenido: bytes, max_paginas: int = 10) -> str:
    """Texto del PDF con pdfplumber (estándar del proyecto). '' si no se puede."""
    if pdfplumber is None or not contenido[:5].startswith(b"%PDF"):
        return ""
    try:
        with pdfplumber.open(_io.BytesIO(contenido)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages[:max_paginas])
    except Exception:
        return ""


def _datos_desde_pdf(texto: str) -> dict[str, Any]:
    """De las bases en texto saca requisitos_texto y renta (cuando aparecen)."""
    out: dict[str, Any] = {}
    if not texto or len(texto) < 80:
        return out
    plano = limpiar_texto(texto)
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
    if extract_salary is not None:
        try:
            sal = extract_salary(plano)
            if sal.amount:
                out["renta_bruta_min"] = int(sal.amount)
                out["renta_bruta_max"] = int(sal.amount)
                if sal.raw:
                    out["renta_texto"] = limpiar_texto(sal.raw)[:200]
        except Exception:
            logger.exception("  Error extrayendo renta del PDF")
    return out


def construir_oferta(item: dict[str, Any], det: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = item["titulo"].rstrip(".")[:500]
    ciudad, region = _ciudad_region(item["titulo"])

    desc_partes = []
    if det.get("descripcion"):
        desc_partes.append(det["descripcion"])
    if item.get("categoria"):
        desc_partes.append(f"Categoría: {item['categoria']}")
    if det.get("vacantes"):
        desc_partes.append(f"Vacantes: {det['vacantes']}")
    if det.get("bases_url"):
        desc_partes.append(f"Bases: {det['bases_url']}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    # tipo por defecto según categoría: Gente de Mar / Oficiales son carrera
    # naval (no régimen civil); Profesionales civiles -> Contrata salvo detalle
    tipo = det.get("tipo")
    if not tipo:
        cat = item.get("categoria", "").lower()
        tipo = "Contrata" if "civil" in cat else "Carrera Naval"

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, item["url"]),
        # patrón "nuevo estándar" (igual que bcentral.py): institucion_id apunta
        # al catálogo de instituciones; fuente_id (FK a `fuentes`) no aplica al
        # scraper auto-contenido. Esto es lo que permite que
        # marcar_ofertas_cerradas (WHERE institucion_id = :fid) cierre las
        # ofertas viejas de baja calidad de esta misma institución.
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": item["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": tipo,
        "nivel": _nivel(item["titulo"], item.get("categoria", "")),
        "region": region,
        "ciudad": ciudad,
        "renta_bruta_min": det.get("renta_bruta_min"),  # la Armada publica grado; renta solo si está en las bases PDF
        "renta_bruta_max": det.get("renta_bruta_max"),
        "renta_texto": det.get("renta_texto"),
        "fecha_publicacion": det.get("fecha_publicacion") or date.today(),
        "fecha_cierre": det.get("fecha_cierre"),
        "requisitos_texto": det.get("requisitos_texto"),  # desde las bases PDF si están disponibles
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float, con_detalle: bool,
               incluir_cerrados: bool) -> list[dict]:
    session = _session()
    items: list[dict[str, Any]] = []
    vistos: set[str] = set()

    # Para abiertas usamos el filtro del propio sitio (?status=...): es exacto
    # y evita recorrer el histórico. El recorrido completo queda solo para
    # --incluir-cerrados.
    if incluir_cerrados:
        base_listado = LISTADO_URL
    else:
        base_listado = LISTADO_URL + "?status=postulacion-abierta"

    pagina = 1
    while pagina <= MAX_PAGINAS:
        sep = "&" if "?" in base_listado else "?"
        url = base_listado if pagina == 1 else f"{base_listado}{sep}paged={pagina}"
        r = _get(session, url)
        if r is None:
            break
        lote = parsear_listado(r.text, BASE)
        nuevos = 0
        for it in lote:
            if it["url"] in vistos:
                continue
            vistos.add(it["url"])
            items.append(it)
            nuevos += 1
        logger.info("  Página %d: %d concursos nuevos", pagina, nuevos)
        if nuevos == 0:
            break
        pagina += 1
        time.sleep(delay)

    abiertas = [it for it in items if it["abierto"]]
    logger.info("  Listado: %d concursos (%d abiertos)", len(items), len(abiertas))
    seleccion = items if incluir_cerrados else abiertas
    if max_results:
        seleccion = seleccion[:max_results]

    ofertas: list[dict] = []
    hoy = date.today()
    for it in seleccion:
        det: dict[str, Any] = {}
        if con_detalle:
            time.sleep(delay)
            rd = _get(session, it["url"])
            if rd is not None:
                det = parsear_detalle(rd.text)
                if det.get("bases_url") and not det.get("requisitos_texto"):
                    rb = _get(session, urljoin(BASE, det["bases_url"]))
                    if rb is not None:
                        pdf_datos = _datos_desde_pdf(_pdf_a_texto(rb.content))
                        for k, v in pdf_datos.items():
                            det.setdefault(k, v)
                        if pdf_datos:
                            logger.info("  Bases PDF: %s", ", ".join(sorted(pdf_datos)))
        o = construir_oferta(it, det)
        try:
            from scrapers.enrich import enriquecer_oferta
            texto_pdf = det.get("texto_pdf")
            enriquecer_oferta(
                o,
                texto_html=det.get("texto_detalle"),
                texto_detalle=texto_pdf,
            )
        except Exception:
            pass
        # cinturón extra: aunque la card diga abierta, si el plazo ya venció
        if (not incluir_cerrados and o["fecha_cierre"]
                and o["fecha_cierre"] < hoy):
            logger.info("  Omitida por plazo vencido: %s", o["cargo"][:50])
            continue
        ofertas.append(o)
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
             delay=DELAY_DEFAULT, con_detalle=True, incluir_cerrados=False,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Armada de Chile%s", " (standalone)" if STANDALONE else "")
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
        ofertas = recolectar(max_results, delay, con_detalle, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [ARMADA] {datos['cargo'][:52]:52} | "
                      f"{datos['region'][:22]:22} | {datos['tipo_cargo'][:14]:14} "
                      f"| cierre: {datos['fecha_cierre']}")
                if verbose:
                    print(f"      {datos['url_original'][:100]}")
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
        logger.exception("  Error fuente Armada: %s", exc)
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
    logger.info("RESUMEN Armada: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"],
                stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper concursos Armada de Chile (admisionarmada.cl)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de concursos")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT,
                   help=f"Pausa entre requests (default {DELAY_DEFAULT}s)")
    p.add_argument("--sin-detalle", action="store_true",
                   help="Solo cards del listado (sin cierre/vacantes/grado)")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="Incluir también concursos cerrados/terminados")
    p.add_argument("--export", default=None, metavar="PREFIJO",
                   help="Exportar además a PREFIJO.json y PREFIJO.csv")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, con_detalle=not a.sin_detalle,
          incluir_cerrados=a.incluir_cerrados, export=a.export)
