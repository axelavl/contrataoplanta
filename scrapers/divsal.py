"""
EmpleoEstado.cl — Scraper de ofertas laborales de la División de Salud del
Ejército (DIVSAL, https://www.divsal.cl/ofertas-laborales/).
Patrón estándar de sesión.

Sitio WordPress (Divi). Las ofertas son posts clasificados en DOS categorías
que separan el estado de forma exacta (verificado vía API REST 06/2026):
    id 6  "postulaciones"  → llamados VIGENTES (hoy: 0, coherente con el
                              "No se encontraron resultados" de la sección
                              ACTIVOS de la página)
    id 13 "caducadas"      → 198 llamados históricos (referencia de estructura)

Cada post (card y detalle comparten el mismo contenido) publica:
    Título "Cargo | Unidad"        (CMM Maipú, MZS Arica, JEAFOSALE, ...)
    Tipo de Contrato               (Personal a Honorarios (suma alzada) /
                                    Contrata / ...)
    Cantidad de horas              (16 horas semanales, 44 horas, ...)
    Renta Aproximada               ($640.000)
    Inicio del proceso             (19 de mayo de 2026)
    Requisitos                     (títulos, registros SIS, etc.)
    Antecedentes de postulación    (documentos)
    "recepcionados hasta el 05JUN2026" → fecha de cierre (formato militar
                                    DDMMMAAAA o "hasta el D de mes de AAAA")

Estrategia: API REST de WordPress como fuente primaria
(/wp-json/wp/v2/posts?categories={6|13}, paginado), parseando los campos
desde content.rendered; fallback al HTML de /ofertas-laborales/ (secciones
ACTIVOS/CADUCADOS con article.et_pb_post) si la API fallara.

NOTA DE CONECTIVIDAD: desde algunos datacenters extranjeros el puerto 443
del sitio responde de forma intermitente; el scraper alterna https→http y
reintenta. Desde Chile no debería presentarse.

Uso:
    python scrapers/divsal.py --dry-run --verbose
    python scrapers/divsal.py --dry-run --incluir-cerrados --max 15
    python scrapers/divsal.py --dry-run --export ofertas_divsal
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
from urllib.parse import urljoin

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

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.divsal")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "divsal.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

HOST = "www.divsal.cl"
LISTADO_PATH = "/ofertas-laborales/"
API_POSTS = "/wp-json/wp/v2/posts"
CAT_VIGENTES = 6    # slug "postulaciones"
CAT_CADUCADAS = 13  # slug "caducadas"

FUENTE = {
    # id 706 = "Ejército de Chile — División de Salud (DIVSAL)" en
    # repositorio_instituciones_publicas_chile.json (alta nueva). OJO: 165 es FAMAE.
    "id": 706,
    "nombre": "Ejército de Chile — División de Salud (DIVSAL)",
    "sigla": "DIVSAL",
    "sector": "FF.AA. y Orden",
    "region": "Nacional",
    "url_empleo": f"https://{HOST}{LISTADO_PATH}",
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 4
DELAY_DEFAULT = 1.0
PER_PAGE = 50

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}
MESES_ABREV = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
               "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10,
               "nov": 11, "dic": 12}

# Comunas/ciudades en nombres de unidades -> región
COMUNA_REGION = {
    "arica": "Arica y Parinacota", "iquique": "Tarapacá",
    "antofagasta": "Antofagasta", "calama": "Antofagasta",
    "copiapó": "Atacama", "copiapo": "Atacama",
    "la serena": "Coquimbo", "coquimbo": "Coquimbo",
    "valparaíso": "Valparaíso", "valparaiso": "Valparaíso",
    "viña del mar": "Valparaíso", "quillota": "Valparaíso",
    "rancagua": "O'Higgins", "talca": "Maule", "linares": "Maule",
    "chillán": "Ñuble", "chillan": "Ñuble",
    "concepción": "Biobío", "concepcion": "Biobío", "los ángeles": "Biobío",
    "los angeles": "Biobío", "temuco": "La Araucanía",
    "valdivia": "Los Ríos", "osorno": "Los Lagos",
    "puerto montt": "Los Lagos", "coyhaique": "Aysén",
    "punta arenas": "Magallanes",
    "maipú": "Metropolitana de Santiago", "maipu": "Metropolitana de Santiago",
    "san bernardo": "Metropolitana de Santiago",
    "la reina": "Metropolitana de Santiago",
    "peñalolén": "Metropolitana de Santiago",
    "santiago": "Metropolitana de Santiago",
}

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── HTTP con alternancia de esquema ──────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    getattr(config, "aplicar_proxy", lambda *_: None)(s)
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/json,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _get(session: requests.Session, path_or_url: str) -> requests.Response | None:
    """GET probando https y http (el 443 del sitio es intermitente fuera de CL)."""
    if path_or_url.startswith("http"):
        candidatos = [path_or_url]
        alt = path_or_url.replace("https://", "http://", 1) \
            if path_or_url.startswith("https://") else \
            path_or_url.replace("http://", "https://", 1)
        candidatos.append(alt)
    else:
        candidatos = [f"https://{HOST}{path_or_url}", f"http://{HOST}{path_or_url}"]

    for intento in range(1, MAX_RETRIES + 1):
        url = candidatos[(intento - 1) % len(candidatos)]
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:80])
                return None
            return r
        except requests.RequestException as exc:
            logger.info("  Intento %d falló (%s) en %s", intento,
                        type(exc).__name__, url[:70])
            time.sleep(min(8, 2 * intento))
    return None


# ── Parseo de campos (puro, testeable) ───────────────────────────────────────
def _fecha_es(texto: str) -> date | None:
    if m := re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", texto or "", re.I):
        mes = MESES.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                return None
    return None


def _fecha_militar(texto: str) -> date | None:
    """'05JUN2026' -> date."""
    if m := re.search(r"(\d{1,2})\s*([A-Z]{3})\s*(\d{4})", texto or "", re.I):
        mes = MESES_ABREV.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                return None
    return None


def parsear_campos(texto: str) -> dict[str, Any]:
    """Campos del cuerpo de una oferta DIVSAL (card, detalle o content API)."""
    t = limpiar_texto(texto)
    d: dict[str, Any] = {}

    def cap(label: str, stop: str) -> str | None:
        m = re.search(rf"{label}\s*:?\s*(.{{3,300}}?)\s*(?={stop}|$)", t, re.I)
        return limpiar_texto(m.group(1)) if m else None

    stops = (r"Tipo de Contrato|Cantidad de horas|Renta Aproximada|"
             r"Inicio del proceso|Requisitos|Antecedentes|Fecha recepci[oó]n")
    if v := cap("Tipo de Contrato", stops):
        d["tipo_crudo"] = v
        vl = v.lower()
        if "honorario" in vl:
            d["tipo"] = "Honorarios"
        elif re.search(r"\bcontrata\b", vl):
            d["tipo"] = "Contrata"
        elif "código del trabajo" in vl or "codigo del trabajo" in vl:
            d["tipo"] = "Código del Trabajo"
        elif "planta" in vl:
            d["tipo"] = "Planta"
    if v := cap("Cantidad de horas", stops):
        d["horas"] = v
    if v := cap("Renta Aproximada", stops):
        d["renta_texto"] = f"Renta aproximada {v}"
        if m := re.search(r"\$?\s*([\d.]{5,12})", v):
            try:
                d["renta"] = int(m.group(1).replace(".", ""))
            except ValueError:
                pass
    if v := cap("Inicio del proceso", stops):
        d["fecha_inicio"] = _fecha_es(v) or _fecha_militar(v)
    if m := re.search(r"Requisitos\s*:?\s*(.{10,1500}?)(?=Antecedentes|Fecha recepci[oó]n|$)",
                      t, re.I):
        d["requisitos"] = limpiar_texto(m.group(1))[:2000]
    if m := re.search(r"Antecedentes de postulaci[oó]n\s*:?\s*(.{10,1200}?)"
                      r"(?=Fecha recepci[oó]n|$)", t, re.I):
        d["antecedentes"] = limpiar_texto(m.group(1))[:1200]
    if m := re.search(r"recepcionad[oa]s?\s+hasta\s+el\s+([\w/ ]{6,30})", t, re.I):
        d["fecha_cierre"] = _fecha_militar(m.group(1)) or _fecha_es(m.group(1))
    if "fecha_cierre" not in d and (m := re.search(
            r"hasta el\s+(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4}|\d{1,2}[A-Z]{3}\d{4})",
            t, re.I)):
        d["fecha_cierre"] = _fecha_es(m.group(1)) or _fecha_militar(m.group(1))
    if m := re.search(r"(?:enviados? al correo|al correo)\s+([\w.+-]+@[\w.-]+)", t, re.I):
        d["correo"] = m.group(1)
    return d


def _ciudad_region(titulo: str) -> tuple[str | None, str]:
    t = titulo.lower()
    for comuna, region in COMUNA_REGION.items():
        if comuna in t:
            return comuna.title(), normalizar_region(region) or region
    return None, FUENTE["region"]


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("jefe", "jefa", "director", "delegado de control")):
        return "Directivo"
    if any(w in c for w in ("médic", "medic", "psicólog", "psicolog", "psiquiatra",
                            "abogad", "asesor jurídico", "asesor juridico",
                            "ingenier", "enfermer", "nutricionista", "matron",
                            "odontólog", "odontolog", "analista")):
        return "Profesional"
    if any(w in c for w in ("técnico", "tecnico", "asistente", "auxiliar",
                            "administrativo", "conductor")):
        return "Técnico"
    return "Profesional"


def construir_oferta(titulo: str, url: str, campos: dict[str, Any],
                     fecha_post: date | None) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = limpiar_texto(titulo)[:500]
    ciudad, region = _ciudad_region(titulo)

    desc_partes = []
    for k, et in (("tipo_crudo", "Tipo de contrato"), ("horas", "Horas"),
                  ("antecedentes", "Antecedentes"), ("correo", "Postulación al correo")):
        if campos.get(k):
            desc_partes.append(f"{et}: {campos[k]}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, url),
        # patrón "nuevo estándar" (igual que armada.py/bcentral.py): institucion_id
        # apunta al catálogo; fuente_id (FK a `fuentes`) no aplica. Esto permite que
        # marcar_ofertas_cerradas (WHERE institucion_id = :fid) cierre las ofertas
        # caducadas de esta institución sin tocar otras fuentes.
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": url,
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": campos.get("tipo") or "Honorarios",
        "nivel": _nivel(cargo),
        "region": region,
        "ciudad": ciudad,
        "renta_bruta_min": campos.get("renta"),
        "renta_bruta_max": None,
        "renta_texto": campos.get("renta_texto"),
        "fecha_publicacion": campos.get("fecha_inicio") or fecha_post or date.today(),
        "fecha_cierre": campos.get("fecha_cierre"),
        "requisitos_texto": campos.get("requisitos"),
    }


# ── Fuente primaria: API REST de WordPress ───────────────────────────────────
def _api_posts(session: requests.Session, categoria: int,
               max_results: int | None, delay: float) -> list[dict] | None:
    """Posts de una categoría vía /wp-json (None si la API no responde)."""
    resultados: list[dict] = []
    page = 1
    while True:
        path = (f"{API_POSTS}?categories={categoria}&per_page={PER_PAGE}"
                f"&page={page}&_fields=id,date,link,title,content")
        r = _get(session, path)
        if r is None:
            return None if page == 1 else resultados
        try:
            lote = r.json()
        except ValueError:
            return None if page == 1 else resultados
        if not isinstance(lote, list):
            return None if page == 1 else resultados
        resultados.extend(lote)
        total_pages = int(r.headers.get("X-WP-TotalPages", page))
        logger.info("  API cat=%d página %d/%d: %d posts",
                    categoria, page, total_pages, len(lote))
        if page >= total_pages or not lote:
            break
        if max_results and len(resultados) >= max_results:
            break
        page += 1
        time.sleep(delay)
    return resultados


def _limpiar_divi(html: str) -> str:
    """El content.rendered de la API trae shortcodes Divi crudos. Convierte
    [et_pb_text admin_label=»Renta Aproximada» ...]$640.000[/et_pb_text]
    en 'Renta Aproximada : $640.000' y elimina el resto de shortcodes
    (cuyos atributos contienen números como _builder_version=4.23.1 que
    contaminaban el parseo)."""
    s = re.sub(r"\[et_pb_text[^\]]*?admin_label=[»\"“]([^»\"”\]]+)[»\"”][^\]]*\]",
               r" \1 : ", html or "")
    s = re.sub(r"\[/?et_pb_[^\]]*\]", " ", s)
    s = re.sub(r"\[/?et_[^\]]*\]", " ", s)
    return s


def _post_a_oferta(post: dict) -> dict:
    titulo = BeautifulSoup(post.get("title", {}).get("rendered", ""),
                           "html.parser").get_text(" ", strip=True)
    contenido = BeautifulSoup(_limpiar_divi(post.get("content", {}).get("rendered", "")),
                              "html.parser").get_text(" ", strip=True)
    # las cards Divi anidan un módulo con admin_label "info" dentro de cada
    # campo; tras la limpieza queda el token "info :" pegado a los valores
    contenido = re.sub(r"\binfo\s*:\s*", " ", contenido)
    campos = parsear_campos(contenido)
    fecha_post = None
    try:
        # Normaliza 'Z' → '+00:00' por consistencia con fiscalia.py y para no
        # depender de Python ≥3.11 (donde fromisoformat ya acepta 'Z').
        fecha_post = datetime.fromisoformat(
            str(post.get("date", "")).replace("Z", "+00:00")).date()
    except ValueError:
        pass
    return construir_oferta(titulo, post.get("link", ""), campos, fecha_post)


# ── Fallback: parseo del HTML de /ofertas-laborales/ ────────────────────────
def parsear_listado_html(html: str, base_url: str) -> tuple[list[dict], list[dict]]:
    """(activos, caducados) desde las secciones de la página."""
    soup = BeautifulSoup(html, "html.parser")
    activos: list[dict] = []
    caducados: list[dict] = []

    # artículos que están DESPUÉS del heading "CADUCADOS" en el DOM
    marcador = soup.find(string=re.compile(r"^\s*CADUCADOS\s*$", re.I))
    arts_caducados: set[int] = set()
    if marcador:
        nodo = marcador.parent
        arts_caducados = {id(a) for a in nodo.find_all_next("article")}

    for art in soup.select("article.et_pb_post"):
        a = art.find("a", href=True)
        h = art.find(["h2", "h3"])
        if not a or not h:
            continue
        titulo = limpiar_texto(h.get_text(" ", strip=True))
        campos = parsear_campos(art.get_text(" ", strip=True))
        item = {"titulo": titulo, "url": urljoin(base_url, a["href"]),
                "campos": campos}
        if not marcador or id(art) in arts_caducados:
            # sin marcador no podemos garantizar vigencia: tratar como caducado
            caducados.append(item)
        else:
            activos.append(item)
    return activos, caducados


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float,
               incluir_cerrados: bool) -> list[dict]:
    session = _session()
    ofertas: list[dict] = []
    vistos: set[str] = set()

    # 1) API REST (primaria)
    posts_vig = _api_posts(session, CAT_VIGENTES, max_results, delay)
    if posts_vig is not None:
        logger.info("  Vigentes (API, cat 'postulaciones'): %d", len(posts_vig))
        for p in posts_vig:
            o = _post_a_oferta(p)
            if o["url_original"] not in vistos:
                vistos.add(o["url_original"])
                try:
                    from scrapers.enrich import enriquecer_oferta
                    enriquecer_oferta(o, texto_html=o.get("descripcion"))
                except Exception:
                    pass
                ofertas.append(o)
        if incluir_cerrados:
            tope = None if max_results is None else max(0, max_results - len(ofertas))
            posts_cad = _api_posts(session, CAT_CADUCADAS, tope, delay) or []
            logger.info("  Caducadas (API): %d", len(posts_cad))
            for p in posts_cad:
                o = _post_a_oferta(p)
                if o["url_original"] not in vistos:
                    vistos.add(o["url_original"])
                    ofertas.append(o)
        return ofertas[:max_results] if max_results else ofertas

    # 2) Fallback HTML
    logger.info("  API no disponible; usando parseo HTML de %s", LISTADO_PATH)
    r = _get(session, LISTADO_PATH)
    if r is None:
        logger.error("No se pudo acceder a %s por ningún esquema", LISTADO_PATH)
        return []
    activos, caducados = parsear_listado_html(r.text, f"https://{HOST}")
    logger.info("  HTML: %d activos, %d caducados (solo primera página)",
                len(activos), len(caducados))
    seleccion = activos + (caducados if incluir_cerrados else [])
    for it in seleccion:
        o = construir_oferta(it["titulo"], it["url"], it["campos"], None)
        if o["url_original"] not in vistos:
            vistos.add(o["url_original"])
            ofertas.append(o)
    return ofertas[:max_results] if max_results else ofertas


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
    logger.info("INICIO - Scraper DIVSAL%s", " (standalone)" if STANDALONE else "")
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
                renta = f"${datos['renta_bruta_min']:,}".replace(",", ".") \
                    if datos["renta_bruta_min"] else "-"
                print(f"  [DIVSAL] {datos['cargo'][:48]:48} | "
                      f"{(datos['region'] or '')[:20]:20} | "
                      f"{datos['tipo_cargo'][:11]:11} | {renta:>11} "
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
        logger.exception("  Error fuente DIVSAL: %s", exc)
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
    logger.info("RESUMEN DIVSAL: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"],
                stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper ofertas DIVSAL (División de Salud, Ejército)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de ofertas")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT,
                   help=f"Pausa entre requests (default {DELAY_DEFAULT}s)")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="Incluir también llamados caducados (histórico)")
    p.add_argument("--export", default=None, metavar="PREFIJO",
                   help="Exportar además a PREFIJO.json y PREFIJO.csv")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, incluir_cerrados=a.incluir_cerrados, export=a.export)
