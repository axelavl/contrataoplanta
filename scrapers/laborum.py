"""
EmpleoEstado.cl — Scraper de perfiles de empresa en Laborum
(https://www.laborum.cl/perfiles/empresa_<slug>_<idEmpresa>.html).

Laborum es una PLATAFORMA de empleo (red Navent/Jobint): varias instituciones
públicas publican ahí sus vacantes. Este módulo es paramétrico — recorre el
registro PERFILES (institucion_id → URL de perfil de empresa) y scrapea cada
uno. Para sumar otra institución que publique en Laborum basta agregarla al
registro (y al catálogo de instituciones).

REQUIERE PLAYWRIGHT: la grilla del perfil se renderiza en cliente (SPA);
`requests`/curl reciben sólo el shell ("enable JavaScript"). Chromium headless
ejecuta el JS y entrega la grilla completa. Instalación:
    pip install playwright && python -m playwright install chromium

Estructura verificada (06/2026) en el perfil de Hospital Militar de Santiago:
  cada aviso es un <a href="/empleos/<slug>-<idAviso>.html"> que envuelve la
  tarjeta completa: antigüedad ("Publicado/Actualizado hace…"), TÍTULO, empresa,
  descripción + "REQUISITOS:" …, "RENTA/SUELDO BRUTO: <n>", comuna+región,
  modalidad ("Presencial"/"Remoto"/"Híbrido"). El idAviso numérico final es el
  id estable. La extracción se hace en el navegador (page.evaluate) por robustez
  ante el markup hidratado.

Laborum NO expone fecha de cierre pública; se deja fecha_cierre vacía y el
cierre real se gestiona por ausencia (marcar_ofertas_cerradas: cuando un aviso
deja de aparecer en el perfil, se desactiva). La postulación es vía Laborum
(login), por lo que url_original = la ficha del aviso.

Uso:
    python scrapers/laborum.py --dry-run --verbose
    python scrapers/laborum.py --dry-run --solo 707      # un institucion_id
    python scrapers/laborum.py --dry-run --export ofertas_laborum
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

logger = logging.getLogger("scraper.laborum")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "laborum.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://www.laborum.cl"  # red Navent/Jobint

# ── Registro de perfiles: institucion_id → metadatos de la empresa en Laborum ──
# Para sumar una institución que publique en Laborum, agregar acá su entrada y
# crear/usar su id en el catálogo de instituciones.
PERFILES: dict[int, dict[str, Any]] = {
    707: {
        "nombre": "Hospital Militar de Santiago",
        "sector": "FF.AA. y Orden",
        "region": "Metropolitana de Santiago",
        "perfil_url": BASE + "/perfiles/empresa_hospital-militar-de-santiago_12050996.html",
    },
    279: {
        "nombre": "EFE — Empresa de Ferrocarriles del Estado",
        "sector": "Empresa del Estado",
        "region": "Nacional",
        "perfil_url": BASE + "/perfiles/empresa_efe-trenes-de-chile_12041027.html",
        # Empresa del estado: régimen laboral por defecto
        "tipo_cargo_default": "Código del Trabajo",
    },
}

PAGE_TIMEOUT = 60_000
GRID_WAIT_MS = 30_000
DELAY_DEFAULT = 2.0

_RE_ID_AVISO = re.compile(r"-(\d+)\.html$")
_RE_RENTA = re.compile(
    r"(?:RENTA|SUELDO)\s+BRUT[OA]?\s*:?\s*\$?\s*([\d][\d.\s]{3,12})", re.I)
_RE_REGION = re.compile(r"Regi[oó]n\s+[\wÁÉÍÓÚÑáéíóúñ\.]+", re.I)

# JS que corre en el navegador: una tarjeta por aviso, campos limpios.
_EXTRACT_JS = r"""
() => {
  const links = Array.from(document.querySelectorAll('a[href*="/empleos/"]'))
    .filter(a => /-\d+\.html$/.test(a.getAttribute('href') || ''));
  const seen = new Set();
  const out = [];
  for (const a of links) {
    const href = a.getAttribute('href');
    if (seen.has(href)) continue;
    seen.add(href);
    // Subir hasta el contenedor de la tarjeta (texto sustancial).
    let card = a, node = a;
    for (let k = 0; k < 6; k++) {
      if ((node.textContent || '').length > 200) { card = node; break; }
      node = node.parentElement;
      if (!node) break;
    }
    out.push({ href, text: (card.textContent || '').replace(/\s+/g, ' ').trim() });
  }
  return out;
}
"""


# ── Navegación ───────────────────────────────────────────────────────────────
def _abrir_navegador(pw):
    browser = pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage",
              "--disable-blink-features=AutomationControlled"])
    ctx = browser.new_context(
        user_agent=config.USER_AGENTS[0],
        locale="es-CL", timezone_id="America/Santiago",
        viewport={"width": 1366, "height": 1600})
    ctx.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return browser, ctx


def _fetch_cards(page, perfil_url: str) -> list[dict[str, str]]:
    """Renderiza el perfil y devuelve tarjetas crudas [{href, text}]."""
    try:
        page.goto(perfil_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
    except Exception as exc:
        logger.warning("  goto falló %s: %s", perfil_url[:70], type(exc).__name__)
        return []
    try:
        page.wait_for_selector('a[href*="/empleos/"]', timeout=GRID_WAIT_MS)
    except Exception:
        logger.info("  Grilla no apareció (¿perfil sin avisos?) en %s", perfil_url[:70])
        return []
    page.wait_for_timeout(1500)
    try:
        return page.evaluate(_EXTRACT_JS) or []
    except Exception as exc:
        logger.warning("  evaluate falló: %s", type(exc).__name__)
        return []


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _parsear_renta(texto: str) -> int | None:
    if m := _RE_RENTA.search(texto or ""):
        num = re.sub(r"[^\d]", "", m.group(1))
        if num.isdigit():
            val = int(num)
            return val if 100_000 <= val <= 50_000_000 else None
    return None


def _titulo_desde_tarjeta(texto: str, empresa: str) -> str:
    """Quita el prefijo de antigüedad y deja el título (hasta el nombre empresa).

    Los avisos del sector público en Laborum llevan el título en MAYÚSCULAS y la
    antigüedad en minúsculas ("Publicado hace 3 días", "Actualizado hace más de
    15 días", "Actualizado hace 7 horas"). Se elimina el prefijo
    Publicado/Actualizado + toda la corrida de caracteres no-mayúsculos hasta la
    primera mayúscula (inicio real del título). Robusto ante todas las variantes
    de antigüedad sin enumerarlas.
    """
    t = re.sub(r"^\s*(?:Publicado|Actualizado)\b[^A-ZÁÉÍÓÚÑ]*", "", texto).strip()
    # Etiquetas que a veces preceden al título.
    t = re.sub(r"^(?:Alta revisi[oó]n de perfiles|M[uú]ltiples vacantes|"
               r"\d+\s+vacantes?(?:\s+disponibles?)?)\s*", "", t, flags=re.I).strip()
    if empresa and empresa in t:
        t = t.split(empresa, 1)[0].strip()
    t = limpiar_texto(t)
    return t[:300] if len(t) >= 3 else limpiar_texto(texto)[:300]


def parsear_tarjeta(card: dict[str, str], empresa: dict[str, Any],
                    base_url: str = BASE) -> dict[str, Any] | None:
    href = card.get("href") or ""
    m = _RE_ID_AVISO.search(href)
    if not m:
        return None
    id_aviso = m.group(1)
    url = urljoin(base_url, href)
    texto = limpiar_texto(card.get("text") or "")
    if not texto:
        return None

    nombre_emp = empresa["nombre"]
    titulo = _titulo_desde_tarjeta(texto, nombre_emp)

    # Cuerpo = lo que sigue al nombre de la empresa (descripción + requisitos).
    cuerpo = texto
    if nombre_emp in texto:
        partes = texto.split(nombre_emp)
        cuerpo = partes[-1].strip() if len(partes) > 1 else texto

    descripcion, requisitos = cuerpo, None
    if mr := re.search(r"Requisitos\s*:?", cuerpo, re.I):
        descripcion = cuerpo[:mr.start()].strip()
        requisitos = cuerpo[mr.end():].strip()
        # cortar requisitos antes de la renta/modalidad/cola
        requisitos = re.split(
            r"(?:RENTA|SUELDO)\s+BRUTA?|Presencial|Remoto|H[ií]brido|"
            r"M[uú]ltiples vacantes|\d+\s+vacante", requisitos, maxsplit=1, flags=re.I)[0].strip()

    descripcion = re.split(
        r"(?:RENTA|SUELDO)\s+BRUTA?|Presencial|Remoto|H[ií]brido",
        descripcion, maxsplit=1, flags=re.I)[0].strip()

    modalidad = None
    if mm := re.search(r"\b(Presencial|Remoto|H[ií]brido)\b", texto, re.I):
        modalidad = mm.group(1).capitalize()

    return {
        "id_aviso": id_aviso,
        "url": url,
        "cargo": titulo,
        "descripcion": descripcion[:2000] if len(descripcion) > 20 else None,
        "requisitos_texto": requisitos[:2000] if requisitos else None,
        "renta_bruta": _parsear_renta(texto),
        "modalidad": modalidad,
    }


def construir_oferta(item: dict[str, Any], institucion_id: int,
                     empresa: dict[str, Any]) -> dict:
    nombre = empresa["nombre"]
    cargo = item["cargo"]
    renta = item.get("renta_bruta")
    desc = item.get("descripcion")
    if item.get("modalidad") and desc:
        desc = limpiar_texto(f"{desc} | Modalidad: {item['modalidad']}")[:2000]
    return {
        "id_externo": generar_id_estable(institucion_id, "laborum", item["id_aviso"]),
        "institucion_id": institucion_id,
        "fuente_id": None,
        "url_original": item["url"],
        "cargo": cargo[:500],
        "descripcion": desc,
        "institucion_nombre": nombre,
        "sector": empresa.get("sector"),
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": empresa.get("tipo_cargo_default"),
        "nivel": None,
        "region": empresa.get("region"),
        "ciudad": None,
        "renta_bruta_min": renta,
        "renta_bruta_max": renta,
        "renta_texto": None,
        "fecha_publicacion": date.today(),
        "fecha_cierre": None,  # Laborum no expone cierre; se cierra por ausencia
        "requisitos_texto": item.get("requisitos_texto"),
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar_perfil(page, institucion_id: int, empresa: dict[str, Any],
                      max_results: int | None) -> list[dict]:
    cards = _fetch_cards(page, empresa["perfil_url"])
    logger.info("  [%s] %d tarjetas en el perfil", empresa["nombre"][:30], len(cards))
    ofertas: list[dict] = []
    vistos: set[str] = set()
    for card in cards:
        item = parsear_tarjeta(card, empresa, BASE)
        if not item or item["id_aviso"] in vistos:
            continue
        vistos.add(item["id_aviso"])
        if not item["cargo"]:
            continue
        oferta = construir_oferta(item, institucion_id, empresa)
        try:
            from scrapers.enrich import enriquecer_oferta
            enriquecer_oferta(oferta, texto_html=oferta.get("descripcion"))
        except Exception:
            pass
        ofertas.append(oferta)
        if max_results and len(ofertas) >= max_results:
            break
    return ofertas


# ── Persistencia / export ────────────────────────────────────────────────────
CAMPOS_EXPORT = ["id_externo", "institucion_id", "institucion_nombre", "sector",
                 "cargo", "region", "renta_bruta_min", "fecha_publicacion",
                 "fecha_cierre", "url_original", "descripcion", "requisitos_texto"]


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


def _persistir(db, institucion_id: int, ofertas: list[dict], stats: dict,
               dry_run: bool, verbose: bool) -> list[str]:
    urls_activas: list[str] = []
    for datos in ofertas:
        urls_activas.append(datos["url_original"])
        if verbose or dry_run:
            renta = datos["renta_bruta_min"]
            print(f"  [LAB {institucion_id}] {datos['cargo'][:50]:50} | "
                  f"renta: {renta or '-'}")
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
    return urls_activas


def ejecutar(dry_run=False, verbose=False, max_results=None,
             delay=DELAY_DEFAULT, solo=None, export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Laborum%s", " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    todas: list[dict] = []

    if sync_playwright is None:
        logger.error(
            "Playwright es OBLIGATORIO para laborum.cl (SPA). Instalar: "
            "pip install playwright && python -m playwright install chromium")
        stats["status"] = "ERROR"
        return stats

    perfiles = {solo: PERFILES[solo]} if (solo and solo in PERFILES) else PERFILES
    db = SessionLocal() if (SessionLocal and not dry_run) else None

    try:
        with sync_playwright() as pw:
            browser, ctx = _abrir_navegador(pw)
            page = ctx.new_page()
            try:
                for institucion_id, empresa in perfiles.items():
                    try:
                        ofertas = recolectar_perfil(page, institucion_id, empresa, max_results)
                    except Exception as exc:
                        stats["errores"] += 1
                        logger.exception("  Error perfil %s: %s", institucion_id, exc)
                        continue
                    stats["encontradas"] += len(ofertas)
                    todas.extend(ofertas)
                    urls_activas = _persistir(db, institucion_id, ofertas, stats,
                                              dry_run, verbose)
                    if db is not None and urls_activas:
                        try:
                            stats["cerradas"] += marcar_ofertas_cerradas(
                                db, institucion_id, sorted(urls_activas))
                        except Exception:
                            logger.exception("  Error cierre-por-ausencia %s", institucion_id)
                    time.sleep(delay)
            finally:
                browser.close()
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente Laborum: %s", exc)
    finally:
        if db is not None:
            try:
                db.rollback()
                registrar_log(db, 0,
                              "OK" if stats["errores"] == 0 else "PARCIAL",
                              ofertas_nuevas=stats["nuevas"],
                              ofertas_actualizadas=stats["actualizadas"],
                              ofertas_cerradas=stats["cerradas"],
                              paginas=0, duracion=time.time() - inicio)
            except Exception:
                logger.exception("  No se pudo registrar log")
            db.close()

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN Laborum: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"],
                stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(description="Scraper de perfiles de empresa en Laborum")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de ofertas por perfil")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--solo", type=int, default=None, metavar="INSTITUCION_ID",
                   help="Scrapear solo un institucion_id del registro PERFILES")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, solo=a.solo, export=a.export)
