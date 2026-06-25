"""
EmpleoEstado.cl — Scraper de concursos del Poder Judicial
(www.pjud.cl). Patrón estándar de sesión (igual que
tribunal_constitucional.py / senado.py).

FUENTE Y ESTRUCTURA (verificado 06/2026):
El Poder Judicial publica sus llamados en "Trabaje con nosotros":
    https://www.pjud.cl/transparencia/trabaje-con-nosotros
Es HTML server-rendered. Las pestañas "Concursos en postulación / evaluación
/ finalizados" se cargan vía JS y NO traen contenido en el HTML, pero la
sección "Comunicados" SÍ: una lista de párrafos, cada uno describiendo un
llamado a concurso (juez de policía local, notario, conservador, receptor,
procurador, ministro, etc.) con su fecha de publicación, seguido de un
párrafo "Ver Edicto" con el PDF del edicto. Esa es la fuente parseable.

UNIDAD DE EXTRACCIÓN: cada párrafo que contiene "CARGO DE …" es un aviso.
Se extrae el cargo (texto tras "CARGO DE" hasta el ROL/punto), la fecha de
publicación ("CON FECHA dd-mm-yyyy" / "dd.mm.yyyy CON ESTA FECHA") y, cuando
viene en el texto, el plazo de cierre ("…HASTA … dd de mes de yyyy"). El PDF
del edicto siguiente es la URL del aviso (estable y única por concurso).

VIGENCIA: el plazo de cierre del Poder Judicial casi siempre vive DENTRO del
PDF del edicto (escaneado, formato variable), no en el HTML. Por eso, además
de descartar lo que tenga cierre vencido, se aplica un filtro de RECENCIA por
fecha de publicación (los concursos del PJ tienen ventanas cortas, ~10-15
días): un aviso sin cierre detectable y con publicación de más de
``max_edad_dias`` (default 35) se omite. Esto evita arrastrar el histórico de
comunicados. El intake transversal (db.database.upsert_oferta) refuerza el
filtro descartando lo demasiado antiguo, las nóminas/resultados y los
registros (árbitros, martilleros, peritos) que no son cargos.

Persistencia (nuevo estándar): la oferta se ancla por `institucion_id`
(id 138 del catálogo); `fuente_id` queda en None. marcar_ofertas_cerradas
cierra por WHERE institucion_id = 138 acotado al dominio pjud.cl.

Uso:
    python scrapers/poder_judicial.py --dry-run --verbose
    python scrapers/poder_judicial.py --dry-run --max-edad-dias 60
    python scrapers/poder_judicial.py --dry-run --export salida
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

    def normalizar_area(c: str) -> str | None:  # type: ignore[misc]
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

logger = logging.getLogger("scraper.poder_judicial")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "poder_judicial.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://www.pjud.cl"
LISTADO = BASE + "/transparencia/trabaje-con-nosotros"

FUENTE = {
    "id": 138,  # Poder Judicial en repositorio_instituciones_publicas_chile.json
    "nombre": "Poder Judicial de Chile",
    "sigla": "PJ",
    "sector": "Judicial",
    "region": "Nacional",
    "ciudad": None,
    "url_empleo": LISTADO,
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0
MAX_EDAD_DIAS_DEFAULT = 35  # recencia para avisos sin cierre detectable

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
    h = (href or "").replace(" ", "").replace("%20", "")
    if h.startswith("http"):
        return h
    h = re.sub(r"^(\.\./)+", "/", h)            # '../docs/...' → '/docs/...'
    return BASE + "/" + h.lstrip("/")


def _fecha_publicacion(texto: str) -> date | None:
    if m := re.search(r"(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})", texto):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _fecha_cierre(texto: str) -> date | None:
    t = limpiar_texto(texto)
    _F = r"(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4})"
    for p in (rf"(?:hasta|plazo|recibid[ao]s?|postulaci[oó]n)\b.{{0,70}}?{_F}",):
        if m := re.search(p, t, re.I):
            s = m.group(1)
            if mm := re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", s, re.I):
                mes = MESES.get(mm.group(2).lower())
                if mes:
                    try:
                        return date(int(mm.group(3)), mes, int(mm.group(1)))
                    except ValueError:
                        pass
            if mm := re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", s):
                try:
                    return date(int(mm.group(3)), int(mm.group(2)), int(mm.group(1)))
                except ValueError:
                    pass
    return None


def _cargo_de(texto: str) -> str | None:
    """Extrae el nombre del cargo tras 'CARGO DE …' hasta el ROL o el punto."""
    m = re.search(r"CARGO DE\s+(.{5,140}?)\s*(?:\(\s*ROL|\bROL\b|\.|,\s*$|$)",
                  texto, re.I)
    if not m:
        return None
    cargo = limpiar_texto(m.group(1)).strip(" ,.;:(-")
    return cargo or None


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("ministro", "juez", "jueza", "conservador", "notario",
                            "secretario", "director", "fiscal")):
        return "Directivo"
    if any(w in c for w in ("receptor", "procurador", "administrativo",
                            "auxiliar", "técnico", "tecnico", "archivero")):
        return "Profesional"
    return "Profesional"


def parsear_comunicados(html: str) -> list[dict[str, Any]]:
    """Devuelve [{cargo, fecha_publicacion, fecha_cierre, url}] de la sección
    Comunicados. Un aviso por párrafo que contenga 'CARGO DE …', deduplicado
    por nombre de cargo, tomando el primer 'Ver Edicto' que le sigue."""
    soup = BeautifulSoup(html, "lxml")
    recs: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for p in soup.find_all("p"):
        txt = limpiar_texto(p.get_text(" ", strip=True))
        if "CARGO DE" not in txt.upper() or len(txt) < 40:
            continue
        cargo = _cargo_de(txt)
        if not cargo:
            continue
        clave = cargo.upper()[:70]
        if clave in vistos:
            continue
        # primer edicto que sigue, sin cruzar al próximo aviso con CARGO DE
        url = None
        for sib in p.find_all_next(["a", "p"], limit=10):
            if getattr(sib, "name", None) == "a":
                href = sib.get("href") or ""
                if "/docs/download/" in href or href.lower().endswith(".pdf"):
                    url = _abs_url(href)
                    break
            elif getattr(sib, "name", None) == "p":
                if "CARGO DE" in limpiar_texto(sib.get_text()).upper():
                    break
        vistos.add(clave)
        recs.append({
            "cargo": cargo,
            "fecha_publicacion": _fecha_publicacion(txt),
            "fecha_cierre": _fecha_cierre(txt),
            "url": url or LISTADO,
            "descripcion": txt[:600],
        })
    return recs


def construir_oferta(rec: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = limpiar_texto(rec["cargo"])[:500]
    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, rec["url"]),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": rec["url"],
        "cargo": cargo,
        "descripcion": (rec.get("descripcion") or None),
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
        "fecha_publicacion": rec.get("fecha_publicacion"),
        "fecha_cierre": rec.get("fecha_cierre"),
        "requisitos_texto": None,
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, max_edad_dias: int,
               incluir_cerrados: bool) -> list[dict]:
    session = _session()
    r = _get(session, LISTADO)
    if r is None:
        logger.warning("  No se pudo cargar el listado del Poder Judicial")
        return []
    recs = parsear_comunicados(r.text)
    logger.info("  Comunicados con cargo: %d", len(recs))

    hoy = date.today()
    limite_edad = hoy - timedelta(days=max_edad_dias)
    ofertas: list[dict] = []
    omit_venc = omit_viejo = omit_sinfecha = 0
    for rec in recs:
        pub = rec.get("fecha_publicacion")
        cie = rec.get("fecha_cierre")
        if cie and cie < hoy and not incluir_cerrados:
            omit_venc += 1
            continue
        if not cie and not incluir_cerrados:
            # sin cierre: exigir recencia. Sin fecha de publicación no podemos
            # establecer vigencia → se omite (evita arrastrar histórico).
            if pub is None:
                omit_sinfecha += 1
                continue
            if pub < limite_edad:
                omit_viejo += 1
                continue
        oferta = construir_oferta(rec)
        pdf_urls = []
        if rec["url"].lower().endswith(".pdf"):
            pdf_urls.append(rec["url"])
        try:
            from scrapers.enrich import enriquecer_oferta
            enriquecer_oferta(
                oferta,
                texto_html=rec.get("descripcion"),
                pdf_urls=pdf_urls,
                session=session,
            )
        except Exception:
            logger.debug("  Enriquecimiento no disponible para %s", oferta["cargo"][:40])
        ofertas.append(oferta)
        if max_results and len(ofertas) >= max_results:
            break
    logger.info("  → %d vigentes (omitidas: %d vencidas, %d antiguas, %d sin fecha)",
                len(ofertas), omit_venc, omit_viejo, omit_sinfecha)
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
             max_edad_dias=MAX_EDAD_DIAS_DEFAULT, incluir_cerrados=False,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Poder Judicial%s",
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
        ofertas = recolectar(max_results, max_edad_dias, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [PJ] {datos['cargo'][:55]:55} | pub: {datos['fecha_publicacion']} "
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
        logger.exception("  Error fuente Poder Judicial: %s", exc)
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
    logger.info("RESUMEN PJ: encontradas=%d nuevas=%d act=%d cerradas=%d "
                "err=%d (%.1fs)", stats["encontradas"], stats["nuevas"],
                stats["actualizadas"], stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(description="Scraper de concursos del Poder Judicial")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--max-edad-dias", type=int, default=MAX_EDAD_DIAS_DEFAULT,
                   help="Antigüedad máxima (días) para avisos sin cierre detectable")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="No filtrar por cierre vencido ni por recencia")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             max_edad_dias=a.max_edad_dias, incluir_cerrados=a.incluir_cerrados,
             export=a.export)
