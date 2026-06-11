"""
EmpleoEstado.cl — Scraper de FAMAE (Fábricas y Maestranzas del Ejército),
portal propio "Trabaja con Nosotros"
(http://www.famae.cl/trabaja-con-nosotros/). Patrón estándar de sesión.

NO requiere Playwright: el sitio (WordPress/CodeGearThemes) es server-rendered
y `requests` recibe el HTML completo. El portal se sirve sobre HTTP (el
certificado HTTPS no resuelve para todas las rutas); se usa HTTP con fallback.

Estructura verificada (06/2026):
  LISTADO  /trabaja-con-nosotros/
      cada vacante = <td> con el cargo + <a class="location uno"
      href="…/slug-cargo/">Ver mas</a>  (sólo esos anchors son ofertas; el
      resto de enlaces famae.cl/… son del menú institucional).
  DETALLE  /slug-cargo/
      <h1 class="cg-page-title">Cargo – Comuna</h1>
      cuerpo: descripción + "Requisitos:" … + "Plazo de postulación:
      DD de MES de YYYY." + "…al correo electrónico: seleccion@famae.cl".

FAMAE no publica renta ni postulación online (se postula por correo a
seleccion@famae.cl); renta_bruta_* queda vacío y la URL de postulación es la
ficha. La fecha de cierre real viene del "Plazo de postulación".

Uso:
    python scrapers/famae.py --dry-run --verbose
    python scrapers/famae.py --dry-run --export ofertas_famae
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

    def marcar_ofertas_cerradas(*a: Any, **k: Any) -> int:  # type: ignore[misc]
        return 0

    def registrar_log(*a: Any, **k: Any) -> None:  # type: ignore[misc]
        return None

    def upsert_oferta(*a: Any, **k: Any) -> tuple[bool, bool]:  # type: ignore[misc]
        return False, False

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.famae")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "famae.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "http://www.famae.cl"
LISTADO_URL = BASE + "/trabaja-con-nosotros/"
FUENTE = {
    "id": 165,  # FAMAE — id real del catálogo de instituciones
    "nombre": "FAMAE — Fábricas y Maestranzas del Ejército",
    "sigla": "FAMAE",
    "sector": "FF.AA. y Orden",
    "region": "Metropolitana de Santiago",  # planta en Talagante, RM
    "url_empleo": LISTADO_URL,
}
EMAIL_POSTULACION = "seleccion@famae.cl"

HTTP_TIMEOUT = 25
DELAY_DEFAULT = 1.0

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

_RE_PLAZO = re.compile(
    r"Plazo de postulaci[oó]n\s*:?\s*(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", re.I)
_RE_COMUNA = re.compile(r"[–\-]\s*([A-Za-zÁÉÍÓÚÑáéíóúñ ]{3,40})\s*$")

CAMPOS_EXPORT = ["id_externo", "institucion_id", "institucion_nombre", "sector",
                 "cargo", "area_profesional", "tipo_cargo", "region", "ciudad",
                 "renta_texto", "fecha_publicacion", "fecha_cierre",
                 "url_original", "descripcion", "requisitos_texto"]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": config.USER_AGENTS[0],
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _fetch(s: requests.Session, url: str) -> str | None:
    for candidato in (url, url.replace("http://", "https://", 1)):
        try:
            r = s.get(candidato, timeout=HTTP_TIMEOUT)
            if r.status_code == 200 and len(r.text) > 2000:
                return r.text
        except requests.RequestException as exc:
            logger.info("  GET %s falló: %s", candidato[:70], type(exc).__name__)
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def parsear_listado(html: str, base_url: str = BASE) -> list[dict[str, Any]]:
    """Cada <a class="location uno"> es una oferta. cargo = texto del <td>."""
    soup = BeautifulSoup(html, "html.parser")
    ofertas: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for a in soup.select("a.location.uno, a.location"):
        href = a.get("href")
        if not href:
            continue
        url = urljoin(base_url, href)
        if url in vistos:
            continue
        vistos.add(url)
        cont = a.find_parent(["td", "li", "div", "article"])
        cargo = ""
        if cont:
            cargo = limpiar_texto(cont.get_text(" ", strip=True))
            cargo = re.sub(r"\s*ver\s+m[aá]s\s*$", "", cargo, flags=re.I).strip()
        ofertas.append({"cargo": cargo[:500], "url": url})
    return ofertas


def _fecha_plazo(texto: str) -> date | None:
    if m := _RE_PLAZO.search(texto or ""):
        mes = MESES.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                return None
    return None


def parsear_detalle(html: str) -> dict[str, Any]:
    """Detalle FAMAE: cargo (h1.cg-page-title), descripción, requisitos, plazo."""
    soup = BeautifulSoup(html, "html.parser")
    for el in soup.select("nav, header, footer, script, style"):
        el.decompose()

    d: dict[str, Any] = {}
    h1 = soup.select_one("h1.cg-page-title")
    cargo = limpiar_texto(h1.get_text(" ", strip=True)) if h1 else ""
    if cargo:
        d["cargo"] = cargo[:500]
        if mc := _RE_COMUNA.search(cargo):
            d["ciudad"] = limpiar_texto(mc.group(1))

    # Contenedor de contenido: el ancestro de h1 que tiene cuerpo y "Requisitos".
    cont = None
    for parent in (h1.parents if h1 else []):
        t = parent.get_text(" ", strip=True)
        if "Requisitos" in t and len(t) < 8000:
            cont = parent
            break
    target = cont or soup
    texto = limpiar_texto(target.get_text(" ", strip=True))

    # Acotar al cuerpo real (desde el inicio de la descripción).
    mstart = re.search(
        r"(F[aá]bricas y Maestranzas|FAMAE se encuentra|se encuentra en (?:la )?b[uú]squeda|"
        r"Te invitamos|buscamos|Estamos en (?:la )?b[uú]squeda)", texto, re.I)
    if mstart:
        texto = texto[mstart.start():]

    d["fecha_cierre"] = _fecha_plazo(texto)

    # Separar descripción / requisitos / cola (plazo + correo).
    cuerpo = texto
    if mp := _RE_PLAZO.search(cuerpo):
        cuerpo = cuerpo[:mp.start()].strip()
    descripcion, requisitos = cuerpo, None
    if mr := re.search(r"Requisitos\s*:?", cuerpo, re.I):
        descripcion = cuerpo[:mr.start()].strip()
        requisitos = cuerpo[mr.end():].strip()
    d["descripcion"] = descripcion[:2000] if len(descripcion) > 30 else None
    d["requisitos_texto"] = requisitos[:2000] if requisitos else None
    return d


def construir_oferta(item: dict[str, Any], det: dict[str, Any]) -> dict:
    institucion_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = det.get("cargo") or item.get("cargo") or ""
    descripcion = det.get("descripcion")
    if descripcion:
        descripcion = limpiar_texto(
            f"{descripcion} | Postulación: enviar antecedentes a {EMAIL_POSTULACION}")[:2000]

    slug = urlparse(item["url"]).path.strip("/").split("/")[-1] or cargo
    return {
        "id_externo": generar_id_estable(institucion_id, nombre, slug),
        "institucion_id": institucion_id,
        "fuente_id": None,
        "url_original": item["url"],
        "cargo": cargo[:500],
        "descripcion": descripcion,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": None,
        "nivel": None,
        "region": FUENTE["region"],
        "ciudad": det.get("ciudad") or "Talagante",
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": date.today(),
        "fecha_cierre": det.get("fecha_cierre"),
        "requisitos_texto": det.get("requisitos_texto"),
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float, con_detalle: bool) -> list[dict]:
    s = _session()
    html = _fetch(s, LISTADO_URL)
    if html is None:
        logger.warning("  No se pudo cargar el listado FAMAE")
        return []
    items = parsear_listado(html, BASE)
    logger.info("  Listado FAMAE: %d ofertas", len(items))
    if max_results:
        items = items[:max_results]

    ofertas: list[dict] = []
    for it in items:
        det: dict[str, Any] = {}
        if con_detalle:
            time.sleep(delay)
            hd = _fetch(s, it["url"])
            if hd:
                det = parsear_detalle(hd)
        ofertas.append(construir_oferta(it, det))
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
             delay=DELAY_DEFAULT, con_detalle=True, export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper FAMAE%s", " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    institucion_id = int(FUENTE["id"])
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    urls_activas: list[str] = []
    ofertas: list[dict] = []

    try:
        ofertas = recolectar(max_results, delay, con_detalle)
        stats["encontradas"] = len(ofertas)
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [FAMAE] {datos['cargo'][:50]:50} | cierre: {datos['fecha_cierre']}")
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
            stats["cerradas"] = marcar_ofertas_cerradas(db, institucion_id, sorted(urls_activas))
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente FAMAE: %s", exc)
    finally:
        if db is not None:
            try:
                db.rollback()
                registrar_log(db, institucion_id,
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
    logger.info("RESUMEN FAMAE: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"],
                stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(description="Scraper FAMAE (Trabaja con Nosotros)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de ofertas")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--sin-detalle", action="store_true",
                   help="Solo cards del listado (no abre fichas)")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, con_detalle=not a.sin_detalle, export=a.export)
