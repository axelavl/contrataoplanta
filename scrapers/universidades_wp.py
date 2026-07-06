"""
EmpleoEstado.cl — Scraper de concursos de universidades en WordPress.
Patrón estándar de sesión, config-driven. Cubre universidades que publican
cada concurso como entrada de WordPress.

MODOS (verificados 06/2026):
  cpt_rest   La universidad expone un Custom Post Type vía API REST de WP.
             · U. de Aysén  /wp-json/wp/v2/concursos
               Los títulos llevan el ESTADO entre corchetes:
               [Publicación]/[Públicación] = recibe postulaciones,
               [En evaluación]/[Cierre] = ya cerró → se filtran por defecto.
               content (WPBakery) trae fecha de publicación, plazo y PDF.

  listado_html  El listado /trabaja-con-nosotros/ enlaza posts /concurso/<slug>;
             no hay CPT REST. Se parsea el listado y, opcionalmente, cada
             detalle para fecha de cierre y PDF de bases.
             · U. de O'Higgins  /trabaja-con-nosotros/  → posts /concurso/...

Sector = "Educación Superior". Renta: estas universidades publican el detalle
en PDF/post sin renta estructurada → renta_* en null salvo que el texto la
declare explícitamente bruta. Vigencia: por estado (UAysén) o por fecha de
cierre del detalle (UOH); sin señal, se conserva (presencia = vigente).

Uso:
    python scrapers/universidades_wp.py --dry-run --verbose
    python scrapers/universidades_wp.py --dry-run --solo uaysen --export salida
    python scrapers/universidades_wp.py --dry-run --sin-detalle   # listado sin abrir posts
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
from datetime import date, datetime, timedelta
from html import unescape
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

logger = logging.getLogger("scraper.universidades_wp")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "universidades_wp.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0
MAX_ANTIGUEDAD_DIAS = 90  # para listados sin fecha de cierre

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

# estados (en el título del CPT) que indican que YA NO recibe postulaciones
_ESTADOS_CERRADOS = ("cierre", "en evaluación", "en evaluacion", "finalizado",
                     "cerrado", "resultado", "adjudicado", "desierto")

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto", "url_bases"]
SECTOR = "Educación Superior"

FUENTES: list[dict[str, Any]] = [
    {"clave": "uaysen", "id": 259, "nombre": "Universidad de Aysén",
     "sigla": "UAYSEN", "region": "Aysén", "ciudad": "Coyhaique",
     "modo": "cpt_rest", "base": "https://uaysen.cl", "cpt": "concursos"},
    {"clave": "uoh", "id": 258, "nombre": "Universidad de O'Higgins",
     "sigla": "UOH", "region": "O'Higgins", "ciudad": "Rancagua",
     "modo": "listado_html",
     "url": "https://www.uoh.cl/trabaja-con-nosotros/",
     "detalle_re": r"/concurso/[^/\"']+/?$", "host": "uoh.cl",
     # Los concursos son un CPT de WordPress (posts /concurso/…). Se intenta la
     # API REST (auto-descubierta) antes del listado HTML para no perder avisos.
     "base": "https://www.uoh.cl"},
]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "application/json, text/html, */*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:75])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo %s: %s", url[:70], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── Utilidades de parseo (puras) ─────────────────────────────────────────────
def _texto_html(html: str) -> str:
    """Limpia HTML y shortcodes WPBakery ([vc_row], [vc_column_text]...)."""
    sin_sc = re.sub(r"\[/?[a-z_]+[^\]]*\]", " ", html or "", flags=re.I)
    txt = BeautifulSoup(sin_sc, "html.parser").get_text(" ", strip=True)
    return limpiar_texto(txt.replace("»", "").replace("«", ""))


def _fecha_es(texto: str) -> date | None:
    if m := re.search(r"(\d{1,2})\s+(?:de\s+)?(\w+)(?:\s+(?:de(?:l)?\s+)?(\d{4}))?",
                      texto or "", re.I):
        mes = MESES.get(m.group(2).lower())
        if mes:
            anio = int(m.group(3)) if m.group(3) else date.today().year
            try:
                f = date(anio, mes, int(m.group(1)))
                if not m.group(3) and f < date.today() - timedelta(days=180):
                    f = date(anio + 1, mes, int(m.group(1)))
                return f
            except ValueError:
                return None
    return None


def _fecha_dmy(texto: str) -> date | None:
    if m := re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", texto or ""):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _cierre(texto: str) -> date | None:
    t = limpiar_texto(texto)
    dia_sem = (r"(?:lunes|martes|mi[ée]rcoles|jueves|viernes|s[áa]bado|"
               r"domingo)?\s*")
    fecha = (r"(\d{1,2}\s+(?:de\s+)?\w+(?:\s+de(?:l)?\s+\d{4})?|"
             r"\d{1,2}[/-]\d{1,2}[/-]\d{4})")
    for patron in (
        rf"(?:postulaci[oó]n|postulaciones|recepci[oó]n|cierre|vence|"
        rf"plazo|abiertas?)\b.{{0,60}}?\bhasta\s+(?:el\s+)?{dia_sem}{fecha}",
        rf"\bhasta\s+(?:el\s+)?(?:d[ií]a\s+)?{dia_sem}{fecha}",
    ):
        if m := re.search(patron, t, re.I):
            return _fecha_es(m.group(1)) or _fecha_dmy(m.group(1))
    return None


def _estado_y_cargo(titulo: str) -> tuple[str | None, str]:
    """Separa '[Estado] Cargo' -> (estado_lower|None, cargo_limpio)."""
    t = limpiar_texto(titulo)
    estado = None
    if m := re.match(r"\[([^\]]+)\]\s*(.*)", t):
        estado = limpiar_texto(m.group(1)).lower()
        t = limpiar_texto(m.group(2))
    t = re.sub(r"^(llamado (?:a|de) (?:concurso|antecedentes)|concurso p[úu]blico"
               r"(?: para)?|convocatoria|provisi[óo]n de(?:\s+\w+\s+cargos?)?|"
               r"publicaci[óo]n(?: de)?|llamado a presentar antecedentes(?: para)?|"
               r"1\s+cargo|proceso de selecci[óo]n)\s*[–:-]?\s*", "",
               t, flags=re.I)
    return estado, limpiar_texto(t)


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("decano", "director", "vicerrector", "contralor",
                            "jefe", "jefa")):
        return "Directivo"
    if any(w in c for w in ("docente", "académic", "academic", "profesor",
                            "investigador", "profesional", "ingenier")):
        return "Profesional"
    if any(w in c for w in ("técnic", "tecnic", "administrativ", "auxiliar",
                            "asistente", "secretari")):
        return "Técnico"
    return "Profesional"


def _tipo(texto: str) -> str:
    t = (texto or "").lower()
    if "honorario" in t:
        return "Honorarios"
    if "contrata" in t:
        return "Contrata"
    if "planta" in t:
        return "Planta"
    if "código del trabajo" in t or "codigo del trabajo" in t:
        return "Código del Trabajo"
    return "Concurso Universitario"


_PDF_BASES_RE = re.compile(
    r"(base|perfil|concurso|convocatoria|descriptor|tdr|antecedente|llamado|"
    r"cargo|t[eé]rmino)", re.I)
_PDF_EXCLUIR_RE = re.compile(
    r"(cuenta|memoria|reglamento|manual|calendario|acta|nomina|n[oó]mina|"
    r"resultado|balance|transparencia|formulario|declaracion|organigrama)", re.I)


def _pdf_bases(html_content: str, base: str) -> str | None:
    """PDF de bases del concurso: prefiere nombres que lo delaten (bases/perfil/
    concurso/…) y descarta documentos institucionales (cuenta pública, etc.)."""
    candidatos: list[str] = []
    for m in re.finditer(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html_content or "",
                         re.I):
        url = urljoin(base, m.group(1))
        low = url.lower()
        if _PDF_EXCLUIR_RE.search(low):
            continue
        if _PDF_BASES_RE.search(low):
            return url
        candidatos.append(url)
    return candidatos[0] if candidatos else None


# ── Modo CPT REST ────────────────────────────────────────────────────────────
def _cpt_raw_items(base: str, rest_base: str, session) -> list | None:
    """Trae los items de un CPT vía API REST de WP (o None si no disponible)."""
    url = (f"{base.rstrip('/')}/wp-json/wp/v2/{rest_base}?per_page=50"
           f"&_fields=id,link,title,date,content")
    r = _get(session, url)
    if r is None:
        return None
    try:
        items = r.json()
    except ValueError:
        return None
    return items if isinstance(items, list) else None


def _descubrir_cpt(base: str, session) -> str | None:
    """Descubre el rest_base del CPT de concursos consultando /wp/v2/types, para
    no adivinar el nombre. Devuelve None si no hay REST o no hay un CPT afín."""
    r = _get(session, f"{base.rstrip('/')}/wp-json/wp/v2/types")
    if r is None:
        return None
    try:
        types = r.json()
    except ValueError:
        return None
    if not isinstance(types, dict):
        return None
    for key, t in types.items():
        rb = str((t or {}).get("rest_base") or key or "").strip()
        slug = str((t or {}).get("slug") or key or "").lower()
        if rb and ("concurso" in slug or "concurso" in rb.lower()):
            return rb
    return None


def _procesar_cpt_items(fuente: dict, items: list, base: str, session,
                        incluir_cerrados: bool) -> list[dict]:
    ofertas = []
    omitidas = 0
    for it in items:
        titulo_raw = unescape(BeautifulSoup(
            it.get("title", {}).get("rendered", ""), "html.parser").get_text())
        estado, cargo = _estado_y_cargo(titulo_raw)
        cerrado = estado is not None and any(
            e in estado for e in _ESTADOS_CERRADOS)
        content_html = it.get("content", {}).get("rendered", "")
        texto = _texto_html(content_html)
        cierre = _cierre(texto)
        if not incluir_cerrados:
            if cerrado:
                omitidas += 1
                continue
            if cierre and cierre < date.today():
                omitidas += 1
                continue
        fecha_pub = None
        if it.get("date"):
            try:
                fecha_pub = datetime.fromisoformat(it["date"]).date()
            except ValueError:
                pass
        o = _construir(fuente, cargo or titulo_raw, it.get("link"),
                       texto, cierre, fecha_pub,
                       _pdf_bases(content_html, base), estado)
        _enriquecer(o, texto, session)
        ofertas.append(o)
    logger.info("  → %d vigentes (%d omitidas)", len(ofertas), omitidas)
    return ofertas


def recolectar_cpt(fuente: dict, session, incluir_cerrados: bool) -> list[dict]:
    base = fuente["base"].rstrip("/")
    items = _cpt_raw_items(base, fuente["cpt"], session)
    if items is None:
        return []
    logger.info("  CPT %s: %d items", fuente["cpt"], len(items))
    return _procesar_cpt_items(fuente, items, base, session, incluir_cerrados)


# ── Modo listado HTML ────────────────────────────────────────────────────────
def recolectar_listado(fuente: dict, session, con_detalle: bool,
                       incluir_cerrados: bool) -> list[dict]:
    # Preferimos la API REST de WordPress si el sitio expone el CPT de concursos:
    # trae TODOS los avisos con su contenido (el listado HTML puede omitir
    # algunos por paginación/maquetado). Descubrimos el rest_base vía /types
    # para no adivinarlo; si no hay REST, caemos al parseo del listado HTML.
    base = (fuente.get("base") or "").rstrip("/")
    if base:
        rest_base = _descubrir_cpt(base, session)
        if rest_base:
            items = _cpt_raw_items(base, rest_base, session)
            if items:
                logger.info("  CPT REST '%s' (auto): %d items", rest_base, len(items))
                return _procesar_cpt_items(fuente, items, base, session,
                                           incluir_cerrados)

    r = _get(session, fuente["url"])
    if r is None:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    patron = re.compile(fuente["detalle_re"], re.I)
    vistos: dict[str, dict] = {}
    for a in soup.find_all("a", href=True):
        href = urljoin(fuente["url"], a["href"])
        if not patron.search(href):
            continue
        if fuente.get("host") and fuente["host"] not in href:
            continue
        titulo = limpiar_texto(a.get_text(" ", strip=True))
        if len(titulo) <= 10:
            continue
        card = a.find_parent(["article", "li", "div", "section"])
        ctx = limpiar_texto(card.get_text(" ", strip=True)) if card else titulo
        if href not in vistos or len(titulo) > len(vistos[href]["titulo"]):
            vistos[href] = {"titulo": titulo, "ctx": ctx[:600]}
    logger.info("  Listado: %d posts de concurso", len(vistos))

    hoy = date.today()
    ofertas = []
    omitidas = 0
    for href, info in vistos.items():
        titulo = info["titulo"]
        _, cargo = _estado_y_cargo(titulo)
        texto = ""
        cierre = _cierre(info["ctx"])
        pdf = None
        fecha_pub = None
        if con_detalle:
            time.sleep(DELAY_DEFAULT)
            rd = _get(session, href)
            if rd is not None:
                main = BeautifulSoup(rd.text, "html.parser").select_one(
                    ".entry-content, article, main") or BeautifulSoup(
                    rd.text, "html.parser").body
                if main:
                    texto = limpiar_texto(main.get_text(" ", strip=True))
                    cierre = cierre or _cierre(texto)
                    pdf = _pdf_bases(str(main), fuente.get("host", href))
        o = _construir(fuente, cargo or titulo, href, texto,
                       cierre, fecha_pub, pdf, None)
        # Enriquecer con el detalle + PDF de bases (requisitos, funciones,
        # correo, salario, fecha de cierre). Puede rellenar fecha_cierre.
        _enriquecer(o, texto, session)
        cierre_final = o.get("fecha_cierre")
        if not incluir_cerrados and cierre_final and cierre_final < hoy:
            omitidas += 1
            continue
        ofertas.append(o)
    logger.info("  → %d vigentes (%d omitidas por cierre vencido)",
                len(ofertas), omitidas)
    return ofertas


# ── Construcción de oferta ───────────────────────────────────────────────────
def _construir(fuente, cargo, url, texto, cierre, fecha_pub, pdf, estado) -> dict:
    fuente_id = int(fuente["id"])
    nombre = fuente["nombre"]
    cargo = limpiar_texto(cargo)[:500]
    url = url or fuente.get("url") or fuente.get("base")

    desc_partes = []
    if texto:
        desc_partes.append(texto[:1200])
    if estado:
        desc_partes.append(f"Estado: {estado}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    requisitos = None
    if texto and (m := re.search(r"(Requisitos|Antecedentes|Perfil)[^:]{0,20}:?\s*"
                                 r"(.{40,1200})", texto, re.I)):
        bloque = m.group(2)
        c = re.search(r"(Consultas|Postulaci|Etapas|Calendario|Documentos a)",
                      bloque, re.I)
        requisitos = limpiar_texto(bloque[:c.start()] if c else bloque)[:2000]

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, url),
        "fuente_id": fuente_id,
        "url_original": url,
        "cargo": cargo,
        "descripcion": descripcion[:2000] or None,
        "institucion_nombre": nombre,
        "sector": SECTOR,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo(f"{cargo} {texto}"),
        "nivel": _nivel(cargo),
        "region": normalizar_region(fuente["region"]) or fuente["region"],
        "ciudad": fuente.get("ciudad"),
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": fecha_pub or date.today(),
        "fecha_cierre": cierre,
        "requisitos_texto": requisitos,
        "url_bases": pdf,
    }


def _enriquecer(oferta: dict, texto: str | None, session) -> None:
    """Pasa el detalle (texto del post + PDF de bases) por el pipeline compartido
    para completar lo que el parseo básico no trae: requisitos, funciones,
    correo de postulación, salario y fecha de cierre. Sólo rellena campos vacíos.
    """
    try:
        from scrapers.enrich import enriquecer_oferta
        pdf = oferta.get("url_bases")
        pdf_urls = [pdf] if (pdf and str(pdf).lower().endswith(".pdf")) else None
        enriquecer_oferta(oferta, texto_html=texto or None,
                          pdf_urls=pdf_urls, session=session)
    except Exception:
        pass


# ── Persistencia / export ────────────────────────────────────────────────────
def _exportar(ofertas, prefijo):
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


def _procesar(fuente, session, con_detalle, incluir_cerrados) -> list[dict]:
    logger.info("─── %s [%s] ───", fuente["nombre"], fuente["modo"])
    if fuente["modo"] == "cpt_rest":
        return recolectar_cpt(fuente, session, incluir_cerrados)
    return recolectar_listado(fuente, session, con_detalle, incluir_cerrados)


def ejecutar(dry_run=False, verbose=False, max_results=None, solo=None,
             con_detalle=True, incluir_cerrados=False, export=None) -> dict:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper universidades WP%s",
                " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto: forzando --dry-run")
        dry_run = True

    fuentes = [f for f in FUENTES if not solo or f["clave"] == solo]
    session = _session()
    agg = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0,
           "encontradas": 0}
    todas = []

    for fuente in fuentes:
        db = SessionLocal() if (SessionLocal and not dry_run) else None
        urls_activas = []
        f_stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
        try:
            ofertas = _procesar(fuente, session, con_detalle, incluir_cerrados)
            if max_results:
                ofertas = ofertas[:max_results]
            agg["encontradas"] += len(ofertas)
            todas.extend(ofertas)
            for datos in ofertas:
                urls_activas.append(datos["url_original"])
                if verbose or dry_run:
                    print(f"  [{fuente['sigla']:7}] {datos['cargo'][:50]:50} | "
                          f"{datos['region'][:12]:12} | cierre: {datos['fecha_cierre']}")
                if dry_run or db is None:
                    continue
                try:
                    nueva, act = upsert_oferta(db, datos)
                    if nueva:
                        f_stats["nuevas"] += 1
                    elif act:
                        f_stats["actualizadas"] += 1
                except Exception as exc:
                    f_stats["errores"] += 1
                    db.rollback()
                    logger.exception("  Error upsert: %s", exc)
            if db is not None and urls_activas:
                f_stats["cerradas"] = marcar_ofertas_cerradas(
                    db, int(fuente["id"]), sorted(set(urls_activas)))
        except Exception as exc:
            f_stats["errores"] += 1
            if db is not None:
                db.rollback()
            logger.exception("  Error fuente %s: %s", fuente["clave"], exc)
        finally:
            if db is not None:
                try:
                    db.rollback()
                    registrar_log(db, int(fuente["id"]),
                                  "OK" if f_stats["errores"] == 0 else "PARCIAL",
                                  ofertas_nuevas=f_stats["nuevas"],
                                  ofertas_actualizadas=f_stats["actualizadas"],
                                  ofertas_cerradas=f_stats["cerradas"],
                                  paginas=0, duracion=time.time() - inicio)
                except Exception:
                    logger.exception("  No se pudo registrar log")
                db.close()
        for k in f_stats:
            agg[k] += f_stats[k]

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN universidades WP: fuentes=%d encontradas=%d nuevas=%d "
                "err=%d (%.1fs)", len(fuentes), agg["encontradas"], agg["nuevas"],
                agg["errores"], dur)
    agg["duracion_seg"] = round(dur, 2)
    agg["status"] = "OK" if agg["errores"] == 0 else "PARCIAL"
    return agg


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper concursos de universidades en WordPress")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope por fuente")
    p.add_argument("--solo", default=None, choices=[f["clave"] for f in FUENTES])
    p.add_argument("--sin-detalle", action="store_true",
                   help="No abrir cada post (listado_html): más rápido, sin cierre/PDF")
    p.add_argument("--incluir-cerrados", action="store_true")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max, solo=a.solo,
             con_detalle=not a.sin_detalle, incluir_cerrados=a.incluir_cerrados,
             export=a.export)
