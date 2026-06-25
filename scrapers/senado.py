"""
EmpleoEstado.cl — Scraper de concursos del Senado de la República
(www.senado.cl). Patrón estándar de sesión (igual que
tribunal_constitucional.py / bcentral.py).

FUENTE Y ESTRUCTURA (verificado 06/2026):
El Senado publica sus concursos en la sección de Transparencia activa:
    https://www.senado.cl/transparencia/transparencia-activa/concursos
La página es Next.js pero SERVER-RENDERED: el HTML ya trae el listado, sin
necesidad de navegador. Tiene dos secciones bajo <h2>:
    "Concursos Vigentes"  → los que nos interesan
    "Concursos terminados" → histórico, se ignora
Cada ítem vigente es un <a> cuyo texto ES el cargo (en mayúsculas) y cuyo
href apunta al detalle del concurso. El detalle trae el plazo de
postulación en el cuerpo ("...hasta las 15:00 horas del día 28 de junio de
2026...") y enlaces a PDFs de bases.

VIGENCIA: se parsea la fecha de cierre del cuerpo del detalle cuando existe.
Si el plazo está vencido y no se pide --incluir-cerrados, se omite. Si no se
detecta cierre, la oferta entra y la vigencia la da su presencia en la
sección "Vigentes" (marcar_ofertas_cerradas cierra lo que desaparece). El
intake transversal (db.database.upsert_oferta) descarta de todos modos lo
demasiado antiguo o que no sea un cargo real.

Persistencia (nuevo estándar): la oferta se ancla por `institucion_id`
(id 135 del catálogo); `fuente_id` queda en None. marcar_ofertas_cerradas
cierra por WHERE institucion_id = 135 acotado al dominio senado.cl.

Uso:
    python scrapers/senado.py --dry-run --verbose
    python scrapers/senado.py --dry-run --sin-detalle
    python scrapers/senado.py --dry-run --export salida
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

    def generar_id_estable(*partes: Any, largo: int = 20) -> str:  # type: ignore[misc]
        import hashlib
        return hashlib.sha1("||".join(str(p) for p in partes).encode()).hexdigest()[:largo]

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

logger = logging.getLogger("scraper.senado")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "senado.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://www.senado.cl"
LISTADO = BASE + "/transparencia/transparencia-activa/concursos"

FUENTE = {
    "id": 135,  # Senado en repositorio_instituciones_publicas_chile.json
    "nombre": "Senado de la República",
    "sigla": "SENADO",
    "sector": "Legislativo",
    "region": "Nacional",
    "ciudad": "Valparaíso",
    "url_empleo": LISTADO,
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

CAMPOS_EXPORT = ["id_externo", "institucion_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:80])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo definitivo %s: %s", url[:80], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _abs_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return BASE + "/" + href.lstrip("/")


def parsear_listado_vigentes(html: str) -> list[dict[str, str]]:
    """Devuelve [{cargo, url}] de la sección 'Concursos Vigentes'.

    Recorre el documento en orden desde el <h2> 'Concursos Vigentes' hasta el
    <h2> 'Concursos terminados', juntando los <a> con texto de cargo.
    """
    soup = BeautifulSoup(html, "lxml")
    h2s = soup.find_all("h2")
    h2v = next((h for h in h2s if "vigentes" in h.get_text(strip=True).lower()), None)
    h2t = next((h for h in h2s if "terminado" in h.get_text(strip=True).lower()), None)
    if h2v is None:
        return []
    vistos: set[int] = set()
    items: list[dict[str, str]] = []
    for el in h2v.next_elements:
        if h2t is not None and el is h2t:
            break
        if getattr(el, "name", None) == "a" and el.get("href"):
            texto = limpiar_texto(el.get_text(" ", strip=True))
            if id(el) in vistos or len(texto) < 15:
                continue
            vistos.add(id(el))
            items.append({"cargo": texto, "url": _abs_url(el["href"])})
    return items


def _parsear_fecha_es(s: str) -> date | None:
    s = s.strip()
    if m := re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    if m := re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", s, re.I):
        mes = MESES.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                return None
    return None


def extraer_cierre(texto: str) -> date | None:
    """Plazo de recepción de antecedentes del cuerpo del detalle."""
    t = limpiar_texto(texto)
    _FECHA = r"(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})"
    patrones = (
        rf"(?:recepci[oó]n|recibir[áa]n|postulaci[oó]n|antecedentes|plazo)\b"
        rf".{{0,90}}?\bhasta\b.{{0,40}}?\bd[ií]a\s+{_FECHA}",
        rf"\bhasta\b.{{0,40}}?(?:horas?\s+)?(?:del?\s+(?:d[ií]a\s+)?)?{_FECHA}",
        rf"plazo[^.]{{0,60}}?\bhasta\s+(?:el\s+)?{_FECHA}",
    )
    for p in patrones:
        if m := re.search(p, t, re.I):
            f = _parsear_fecha_es(m.group(1))
            if f:
                return f
    return None


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("director", "jefe", "jefa", "secretario general",
                            "secretario ejecutivo", "subdirector")):
        return "Directivo"
    if any(w in c for w in ("auxiliar", "técnico", "tecnico", "administrativo",
                            "secretaria", "secretario", "operador", "chofer",
                            "taquígrafo", "taquigrafo", "fotógrafo", "fotografo")):
        return "Técnico"
    return "Profesional"


def construir_oferta(item: dict[str, str], detalle: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = limpiar_texto(item["cargo"])[:500]
    descripcion = detalle.get("descripcion")
    bases = detalle.get("bases_url")
    if bases:
        descripcion = limpiar_texto((descripcion or "") + f" | Bases: {bases}")
    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, item["url"]),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": item["url"],
        "cargo": cargo,
        "descripcion": (descripcion or None) and descripcion[:2000],
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": "Concurso público",
        "nivel": _nivel(cargo),
        "region": FUENTE["region"],
        "ciudad": FUENTE["ciudad"],
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": detalle.get("fecha_publicacion") or date.today(),
        "fecha_cierre": detalle.get("fecha_cierre"),
        "requisitos_texto": None,
    }


# ── Detalle ──────────────────────────────────────────────────────────────────
def _parsear_detalle(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    main = soup.find("main") or soup.find("article") or soup.body or soup
    texto = main.get_text(" ", strip=True)
    bases = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower() and ("bases" in href.lower() or "concurso" in href.lower()):
            bases = href
            break
    return {
        "descripcion": limpiar_texto(texto)[:2000],
        "fecha_cierre": extraer_cierre(texto),
        "bases_url": bases,
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float, con_detalle: bool,
               incluir_cerrados: bool) -> list[dict]:
    session = _session()
    r = _get(session, LISTADO)
    if r is None:
        logger.warning("  No se pudo cargar el listado del Senado")
        return []
    items = parsear_listado_vigentes(r.text)
    logger.info("  Concursos vigentes en listado: %d", len(items))

    ofertas: list[dict] = []
    hoy = date.today()
    omitidas = 0
    for item in items:
        detalle: dict[str, Any] = {}
        if con_detalle:
            time.sleep(delay)
            rd = _get(session, item["url"])
            if rd is not None:
                detalle = _parsear_detalle(rd.text)
        oferta = construir_oferta(item, detalle)
        if (oferta["fecha_cierre"] and oferta["fecha_cierre"] < hoy
                and not incluir_cerrados):
            omitidas += 1
            continue
        ofertas.append(oferta)
        if max_results and len(ofertas) >= max_results:
            break
    logger.info("  → %d ofertas vigentes (%d omitidas por plazo vencido)",
                len(ofertas), omitidas)
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
    logger.info("INICIO - Scraper Senado%s", " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0,
             "encontradas": 0}
    fuente_id = int(FUENTE["id"])
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    urls_activas: list[str] = []
    ofertas: list[dict] = []

    try:
        ofertas = recolectar(max_results, delay, con_detalle, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [SENADO] {datos['cargo'][:60]:60} | cierre: {datos['fecha_cierre']}")
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
            stats["cerradas"] = marcar_ofertas_cerradas(db, fuente_id,
                                                        sorted(urls_activas))
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente Senado: %s", exc)
    finally:
        if db is not None:
            try:
                db.rollback()
                registrar_log(db, fuente_id,
                              "OK" if stats["errores"] == 0 else "PARCIAL",
                              ofertas_nuevas=stats["nuevas"],
                              ofertas_actualizadas=stats["actualizadas"],
                              ofertas_cerradas=stats["cerradas"],
                              paginas=1, duracion=time.time() - inicio)
            except Exception:
                logger.exception("  No se pudo registrar log")
            db.close()

    if export and ofertas:
        _exportar(ofertas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN SENADO: encontradas=%d nuevas=%d act=%d cerradas=%d "
                "err=%d (%.1fs)", stats["encontradas"], stats["nuevas"],
                stats["actualizadas"], stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(description="Scraper de concursos del Senado")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--sin-detalle", action="store_true",
                   help="Solo listado (sin fecha de cierre del detalle)")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="No filtrar procesos con plazo vencido")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, con_detalle=not a.sin_detalle,
             incluir_cerrados=a.incluir_cerrados, export=a.export)
