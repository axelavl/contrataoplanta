"""
EmpleoEstado.cl — Scraper de ofertas del Ministerio Público / Fiscalía de
Chile (https://fiscaliadechile.trabajando.cl). Patrón estándar de sesión.

OJO CON LA PLATAFORMA: aunque el dominio es *.trabajando.cl, este portal usa
la GENERACIÓN ANTIGUA de Trabajando (jQuery/Angular con widgets AJAX), no la
plantilla Nuxt que autodetecta trabajando.py — por eso va en scraper propio.

Fuente de datos (verificada 06/2026): GET /offers/getAjax devuelve JSON:
    offers[]: id, jobTitle, fiscaliaRegional, vacancies,
              statePublication ("Proceso en Etapa de Postulación"),
              validity ("15/06/2026" = cierre de postulación),
              _creation_date (publicación), lastUpdate, page
    headers: {pages: N}   (páginas adicionales vía POST /offers/ajax_search)

El detalle por oferta se renderiza con un widget cuyo endpoint no está
expuesto de forma estable, así que descripcion/requisitos quedan a nivel de
los campos del listado (que ya cubren cargo, región, vacantes, fechas y
estado). url_original usa el deep-link real del sitio: /offers#detail/{id}.

Vigencia: se importan solo ofertas con validity >= hoy (el sitio mantiene el
estado "Proceso en Etapa de Postulación" como texto, pero la fecha manda);
--incluir-cerrados desactiva el filtro.

Régimen: la Fiscalía publica "Profesional/Técnico/Auxiliar Honorarios" y
"Concurso Interno" (planta propia del Ministerio Público, no Estatuto
Administrativo). tipo_cargo se deriva del título.

Uso:
    python scrapers/fiscalia.py --dry-run --verbose
    python scrapers/fiscalia.py --dry-run --export ofertas_fiscalia
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

logger = logging.getLogger("scraper.fiscalia")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "fiscalia.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://fiscaliadechile.trabajando.cl"
FUENTE = {
    # id 146 = "Ministerio Público — Fiscalía de Chile" en el catálogo.
    "id": 146,
    "nombre": "Ministerio Público — Fiscalía de Chile",
    "sigla": "FISCALIA",
    "sector": "Autónomo/Regulador",
    "region": "Nacional",
    "url_empleo": BASE + "/offers",
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0
MAX_PAGINAS = 10

# "Metropolitana Oriente/Sur/..." son fiscalías de la RM; el resto trae el
# nombre de la región (a veces con el ordinal romano entre paréntesis)
_RE_ORDINAL = re.compile(r"\s*\([IVXL]+\)\s*$")

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "application/json,text/html,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": BASE + "/offers",
    })
    return s


def _json(session: requests.Session, method: str, url: str,
          data: dict | None = None) -> dict | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(method, url, data=data, timeout=HTTP_TIMEOUT)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:80])
                return None
            # la respuesta trae líneas en blanco antes del JSON
            cuerpo = r.text.strip()
            inicio = cuerpo.find("{")
            if inicio < 0:
                return None
            return json.loads(cuerpo[inicio:])
        except (requests.RequestException, ValueError) as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo definitivo %s: %s", url[:80], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _region_desde_fiscalia(fiscalia: str) -> str:
    f = limpiar_texto(fiscalia)
    if not f:
        return FUENTE["region"]
    if f.lower().startswith("metropolitana") or "nacional" in f.lower():
        return ("Metropolitana de Santiago"
                if f.lower().startswith("metropolitana") else "Nacional")
    return normalizar_region(_RE_ORDINAL.sub("", f)) or _RE_ORDINAL.sub("", f)


def _fecha_ddmmyyyy(s: str) -> date | None:
    try:
        return datetime.strptime(limpiar_texto(s), "%d/%m/%Y").date()
    except (ValueError, TypeError):
        return None


def _tipo(cargo: str) -> str:
    c = (cargo or "").lower()
    if "honorario" in c:
        return "Honorarios"
    if "concurso interno" in c:
        return "Planta Ministerio Público"
    if "contrata" in c:
        return "Contrata"
    return "Planta Ministerio Público"


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("jefe", "jefa", "director", "fiscal jefe")):
        return "Directivo"
    if any(w in c for w in ("técnico", "tecnico", "auxiliar", "administrativo",
                            "estafeta", "secretari", "conductor")):
        return "Técnico"
    return "Profesional"


def construir_oferta(o: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = limpiar_texto(str(o.get("jobTitle") or ""))[:500]
    fiscalia = limpiar_texto(str(o.get("fiscaliaRegional") or ""))
    oid = str(o.get("id") or "")

    fecha_pub = None
    if cd := o.get("_creation_date"):
        try:
            fecha_pub = datetime.fromisoformat(
                str(cd).replace("Z", "+00:00")).date()
        except ValueError:
            pass

    desc_partes = []
    if fiscalia:
        desc_partes.append(f"Fiscalía Regional: {fiscalia}")
    if o.get("vacancies"):
        desc_partes.append(f"Vacantes: {o['vacancies']}")
    if o.get("statePublication"):
        desc_partes.append(f"Estado: {limpiar_texto(str(o['statePublication']))}")

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, oid),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": f"{BASE}/offers#detail/{oid}",
        "cargo": cargo,
        "descripcion": limpiar_texto(" | ".join(desc_partes)) or None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo(cargo),
        "nivel": _nivel(cargo),
        "region": _region_desde_fiscalia(fiscalia),
        "ciudad": None,
        "renta_bruta_min": None,   # el listado no publica renta
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": fecha_pub or date.today(),
        "fecha_cierre": _fecha_ddmmyyyy(str(o.get("validity") or "")),
        "requisitos_texto": None,  # el detalle carga por widget sin endpoint expuesto
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float,
               incluir_cerrados: bool) -> list[dict]:
    session = _session()
    d = _json(session, "GET", f"{BASE}/offers/getAjax")
    if not d or "offers" not in d:
        logger.error("No se pudo obtener el listado de %s/offers/getAjax", BASE)
        return []
    crudas: list[dict] = list(d.get("offers") or [])
    pages = int((d.get("headers") or {}).get("pages") or 1)
    logger.info("  Página 1: %d ofertas (páginas totales: %d)", len(crudas), pages)

    ids = {str(o.get("id")) for o in crudas}
    for page in range(2, min(pages, MAX_PAGINAS) + 1):
        time.sleep(delay)
        dp = _json(session, "POST", f"{BASE}/offers/ajax_search", {"page": page})
        lote = (dp or {}).get("offers") or []
        nuevas = [o for o in lote if str(o.get("id")) not in ids]
        if not nuevas:
            break
        for o in nuevas:
            ids.add(str(o.get("id")))
        crudas.extend(nuevas)
        logger.info("  Página %d: %d ofertas nuevas", page, len(nuevas))

    hoy = date.today()
    ofertas: list[dict] = []
    omitidas = 0
    for o in crudas:
        if str(o.get("hidden") or "0") == "1":
            continue
        of = construir_oferta(o)
        if (of["fecha_cierre"] and of["fecha_cierre"] < hoy
                and not incluir_cerrados):
            omitidas += 1
            continue
        try:
            from scrapers.enrich import enriquecer_oferta
            enriquecer_oferta(
                of,
                texto_html=of.get("descripcion"),
                descargar_pdfs=False,
            )
        except Exception:
            pass
        ofertas.append(of)
        if max_results and len(ofertas) >= max_results:
            break
    logger.info("  → %d vigentes (%d omitidas por plazo vencido)",
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
             delay=DELAY_DEFAULT, incluir_cerrados=False,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Fiscalía de Chile%s",
                " (standalone)" if STANDALONE else "")
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
        ofertas = recolectar(max_results, delay, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [FISCALIA] {datos['cargo'][:38]:38} | "
                      f"{datos['region'][:24]:24} | {datos['tipo_cargo'][:24]:24} "
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
            stats["cerradas"] = marcar_ofertas_cerradas(db, fuente_id,
                                                        sorted(urls_activas))
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente Fiscalía: %s", exc)
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
    logger.info("RESUMEN Fiscalía: encontradas=%d nuevas=%d act=%d cerradas=%d "
                "err=%d (%.1fs)", stats["encontradas"], stats["nuevas"],
                stats["actualizadas"], stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper ofertas Ministerio Público (fiscaliadechile.trabajando.cl)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="No filtrar ofertas con plazo vencido")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, incluir_cerrados=a.incluir_cerrados, export=a.export)
