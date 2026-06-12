"""
EmpleoEstado.cl — Scraper del portal propio de Fundación Integra
(trabajaconnosotros.integra.cl → tableros Aira Virtual). Patrón estándar.

ARQUITECTURA DE LA FUENTE (verificada 06/2026): trabajaconnosotros.integra.cl
es solo una portada con botones; las ofertas viven en tableros del ATS Aira
(jobs.airavirtual.com/{tenant}): jardines_infantiles_integra,
oficinas_regionales_integra y casa_central_integra (los tableros de
promociones/traslados son movilidad interna con login y se excluyen).

FUENTE DE DATOS PRIMARIA (descubierta 06/2026 al corroborar el conteo de
110 ofertas que la página solo pagina de a 10): cada tablero consume un
FEED JSON público en
  https://gcs-storage.airavirtual.com/public/feeds/aira_{tenant}.json
con el listado COMPLETO y estructurado: id, name, link (/postula/{hash}),
city/region (slugs "chile##metropolitana##santiago"), contract_type
(FIXED_TERM/OPEN_ENDED/INTERNSHIP/OTHER), hire_mode (FULL_TIME/PART_TIME/
FOR_HOURS), area/subarea, publication_days y updated_at. El feed responde
sin firma ni JS, así que NO se requiere Playwright para operar; el render
con Playwright queda solo como FALLBACK si el feed cambiara (si Playwright
no está instalado y el feed falla, la fuente se omite con aviso).

RELACIÓN CON integra.trabajando.cl (cubierta por trabajando.py, id 293):
ambos portales son COMPLEMENTARIOS CON SOLAPE PARCIAL, medido en vivo sobre
los feeds completos (06/2026): Aira 114 ofertas (110 jardines + 4 oficinas)
vs Trabajando 83; ~40%% de los títulos coinciden entre ambos. Se scrapean
ambos y el duplicado se resuelve en BD deduplicando por cargo normalizado
dentro de la institución (los id_externo difieren entre portales).

Uso:
    python scrapers/aira_integra.py --dry-run --verbose
    python scrapers/aira_integra.py --dry-run --sin-internos --export salida
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
from urllib.parse import urljoin

from bs4 import BeautifulSoup

import requests

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None  # type: ignore[assignment]

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

logger = logging.getLogger("scraper.aira_integra")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "aira_integra.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

FUENTE = {
    # id 132 = "Fundación Integra" en el catálogo. OJO: 294 es SASIPA.
    # Misma institución que cubre trabajando.py (integra.trabajando.cl); aquí va
    # el portal propio (tableros Aira). Mismo institucion_id, dominio distinto.
    "id": 132,
    "nombre": "Fundación Integra",
    "sigla": "INTEGRA",
    "sector": "Ejecutivo Central",
    "region": "Nacional",
    "url_empleo": "https://trabajaconnosotros.integra.cl/",
}

BOARDS = (
    "jardines_infantiles_integra",
    "oficinas_regionales_integra",
    "casa_central_integra",
)
BASE_AIRA = "https://jobs.airavirtual.com"
FEED_URL = "https://gcs-storage.airavirtual.com/public/feeds/aira_{tenant}.json"
HTTP_TIMEOUT = 25

REGION_SLUGS = {
    "arica_y_parinacota": "Arica y Parinacota", "tarapaca": "Tarapacá",
    "antofagasta": "Antofagasta", "atacama": "Atacama", "coquimbo": "Coquimbo",
    "valparaiso": "Valparaíso", "metropolitana": "Metropolitana de Santiago",
    "libertador_b_o_higgins": "O'Higgins", "maule": "Maule", "nuble": "Ñuble",
    "biobio": "Biobío", "araucania": "La Araucanía", "los_rios": "Los Ríos",
    "los_lagos": "Los Lagos", "aysen": "Aysén",
    "magallanes_y_antartica_chilena": "Magallanes",
}
CONTRATO_FEED = {"FIXED_TERM": "Plazo fijo", "OPEN_ENDED": "Indefinido",
                 "INTERNSHIP": "Práctica", "OTHER": "Código del Trabajo"}
JORNADA_FEED = {"FULL_TIME": "Jornada Completa", "PART_TIME": "Part Time",
                "FOR_HOURS": "Por horas"}

PAGE_TIMEOUT = 60_000
RENDER_WAIT_MS = 3_500
DELAY_DEFAULT = 1.5

REGIONES_CL = ("Arica y Parinacota", "Tarapacá", "Antofagasta", "Atacama",
               "Coquimbo", "Valparaíso", "Metropolitana", "O'Higgins", "Maule",
               "Ñuble", "Biobío", "Bíobío", "Bio Bio", "Araucanía", "Los Ríos",
               "Los Lagos", "Aysén", "Magallanes")

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── Parseo (puro, testeable sobre HTML renderizado) ──────────────────────────
def parsear_board(html: str, board: str) -> list[dict[str, Any]]:
    """Cards a[href*='/postula/'] del tablero Aira ya renderizado."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for a in soup.select("a[href*='/postula/']"):
        url = a.get("href", "").split("?")[0]
        if not url or url in vistos:
            continue
        h = a.find(["h4", "h5"])
        titulo = limpiar_texto((h or a).get_text(" ", strip=True))
        if len(titulo) < 15:
            continue
        vistos.add(url)
        card = a.find_parent(["div", "li"]) or a
        ctx = limpiar_texto(card.get_text(" ", strip=True))
        items.append({"titulo": titulo[:500], "url": url, "ctx": ctx[:600],
                      "board": board})
    return items


def _ubicacion(ctx: str) -> tuple[str | None, str]:
    """'... 🇨🇱 Iquique, Tarapacá Full Time ...' -> (ciudad, región)."""
    m = re.search(r"🇨🇱\s*([\wÁÉÍÓÚÑáéíóúñ' .]+?),\s*([\wÁÉÍÓÚÑáéíóúñ' ]+?)"
                  r"(?=\s+(?:Full|Part|Por|Presencial|Remoto|H[ií]brido|Temporal|"
                  r"Indefinido|$))", ctx)
    if m:
        ciudad = limpiar_texto(m.group(1))
        region_cruda = limpiar_texto(m.group(2))
        region = next((r for r in REGIONES_CL
                       if region_cruda.lower().startswith(r.lower()[:6])),
                      region_cruda)
        if region.lower().startswith("metropolitana"):
            region = "Metropolitana de Santiago"
        if region in ("Bíobío", "Bio Bio"):
            region = "Biobío"
        return ciudad, normalizar_region(region) or region
    return None, FUENTE["region"]


def _tipo(titulo: str, ctx: str) -> str:
    t = f"{titulo} {ctx}".lower()
    if "concurso interno" in t:
        return "Concurso Interno"
    if "plazo fijo" in t or "temporal" in t or "reemplazo" in t:
        return "Plazo fijo"
    if "indefinido" in t:
        return "Indefinido"
    return "Código del Trabajo"


def _nivel(titulo: str) -> str:
    c = (titulo or "").lower()
    if any(w in c for w in ("director", "jefe", "jefa", "coordinador")):
        return "Directivo"
    if any(w in c for w in ("auxiliar", "asistente", "técnic", "tecnic",
                            "administrativ", "conductor")):
        return "Técnico"
    return "Profesional"


def construir_oferta(it: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = it["titulo"]
    ciudad, region = _ubicacion(it["ctx"])

    desc_partes = [f"Tablero: {it['board'].replace('_', ' ')}"]
    # jornada/modalidad si aparecen en el contexto
    if m := re.search(r"(Full Time|Part Time|Por turnos?)", it["ctx"], re.I):
        desc_partes.append(f"Jornada: {m.group(1)}")
    if m := re.search(r"(Presencial|Remoto|H[ií]brido)", it["ctx"], re.I):
        desc_partes.append(f"Modalidad: {m.group(1)}")

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, it["url"]),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": urljoin(BASE_AIRA, it["url"]),
        "cargo": cargo,
        "descripcion": limpiar_texto(" | ".join(desc_partes)) or None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo(cargo, it["ctx"]),
        "nivel": _nivel(cargo),
        "region": region,
        "ciudad": ciudad,
        "renta_bruta_min": None,   # Aira no publica renta en el tablero
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": date.today(),  # el tablero no publica fecha
        "fecha_cierre": None,
        "requisitos_texto": None,  # el detalle requiere cuenta Aira
    }


def construir_oferta_feed(o: dict[str, Any], board: str) -> dict:
    """Oferta estándar desde un item del feed JSON (fuente primaria)."""
    from datetime import timedelta
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = limpiar_texto(str(o.get("name") or ""))[:500]
    url = str(o.get("link") or "").split("?")[0]

    region = FUENTE["region"]
    ciudad = None
    slug_r = str(o.get("region") or "")
    if "##" in slug_r:
        region = REGION_SLUGS.get(slug_r.split("##")[-1], FUENTE["region"])
        region = normalizar_region(region) or region
    slug_c = str(o.get("city") or "")
    if slug_c.count("##") >= 2:
        ciudad = slug_c.split("##")[-1].replace("_", " ").title()

    tipo = CONTRATO_FEED.get(str(o.get("contract_type") or ""), "Código del Trabajo")
    if "concurso interno" in cargo.lower():
        tipo = "Concurso Interno"

    desc_partes = [f"Tablero: {board.replace('_', ' ')}"]
    if j := JORNADA_FEED.get(str(o.get("hire_mode") or "")):
        desc_partes.append(f"Jornada: {j}")
    if str(o.get("remote_work") or "") not in ("", "NO_REMOTE"):
        desc_partes.append(f"Modalidad: {o['remote_work']}")
    if o.get("area"):
        desc_partes.append(f"Área: {str(o['area']).replace('_', ' ')}")

    fecha_pub = date.today()
    try:
        fecha_pub = date.today() - timedelta(days=int(o.get("publication_days") or 0))
    except (TypeError, ValueError):
        pass

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo,
                                         str(o.get("id") or url)),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": urljoin(BASE_AIRA, url) if url else f"{BASE_AIRA}/{board}",
        "cargo": cargo,
        "descripcion": limpiar_texto(" | ".join(desc_partes)) or None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": tipo,
        "nivel": _nivel(cargo),
        "region": region,
        "ciudad": ciudad,
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": fecha_pub,
        "fecha_cierre": None,   # el feed no publica cierre
        "requisitos_texto": None,
    }


def _feed_board(session: requests.Session, board: str) -> list[dict] | None:
    """Ofertas del feed JSON del tablero (None si el feed no responde)."""
    try:
        r = session.get(FEED_URL.format(tenant=board), timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        return list(r.json().get("offers") or [])
    except (requests.RequestException, ValueError):
        return None


def _render_board_playwright(board: str) -> list[dict[str, Any]]:
    """Fallback: render del tablero y parseo de cards del DOM."""
    if sync_playwright is None:
        logger.warning("  Feed caído y Playwright no instalado: %s omitido", board)
        return []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True,
                                     args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_context(user_agent=config.USER_AGENTS[0],
                                   locale="es-CL").new_page()
        try:
            page.goto(f"{BASE_AIRA}/{board}", wait_until="networkidle",
                      timeout=PAGE_TIMEOUT)
            page.wait_for_timeout(RENDER_WAIT_MS)
            return parsear_board(page.content(), board)
        except Exception as exc:
            logger.warning("  Error renderizando %s: %s", board, type(exc).__name__)
            return []
        finally:
            browser.close()


# ── Recolección (feed JSON primario, Playwright como fallback) ──────────────
def recolectar(max_results: int | None, delay: float,
               sin_internos: bool) -> list[dict]:
    session = requests.Session()
    session.headers["User-Agent"] = config.USER_AGENTS[0]
    ofertas: list[dict] = []
    vistos: set[str] = set()

    for board in BOARDS:
        crudas = _feed_board(session, board)
        if crudas is not None:
            logger.info("  %s (feed): %d ofertas", board, len(crudas))
            lote = [construir_oferta_feed(o, board) for o in crudas]
        else:
            logger.warning("  Feed de %s no disponible; intentando render", board)
            lote = [construir_oferta(it)
                    for it in _render_board_playwright(board)]
            logger.info("  %s (render): %d ofertas", board, len(lote))

        for o in lote:
            if o["url_original"] in vistos or not o["cargo"]:
                continue
            vistos.add(o["url_original"])
            if sin_internos and o["tipo_cargo"] == "Concurso Interno":
                continue
            ofertas.append(o)
            if max_results and len(ofertas) >= max_results:
                return ofertas
        time.sleep(delay)
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
             delay=DELAY_DEFAULT, sin_internos=False,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Integra/Aira%s", " (standalone)" if STANDALONE else "")
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
        ofertas = recolectar(max_results, delay, sin_internos)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [INTEGRA-A] {datos['cargo'][:50]:50} | "
                      f"{(datos['region'] or '')[:20]:20} | {datos['tipo_cargo'][:16]:16}")
                if verbose:
                    print(f"      {datos['url_original'][:95]}")
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
        logger.exception("  Error fuente Integra/Aira: %s", exc)
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
    logger.info("RESUMEN Integra/Aira: encontradas=%d nuevas=%d act=%d "
                "cerradas=%d err=%d (%.1fs)", stats["encontradas"],
                stats["nuevas"], stats["actualizadas"], stats["cerradas"],
                stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper Fundación Integra vía tableros Aira "
                    "(requiere Playwright)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--sin-internos", action="store_true",
                   help="Excluir 'Concurso Interno' (movilidad de funcionarios)")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, sin_internos=a.sin_internos, export=a.export)
