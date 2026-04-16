"""
EmpleoEstado.cl — Scraper: CODELCO (Corporación Nacional del Cobre)
URL: https://empleos.codelco.cl/search/

Gran volumen de cargos. Sitio propio con JS (SPA).
Requiere Playwright.

Uso:
    python scrapers/codelco.py --dry-run --verbose
"""

import argparse
import logging
import random
import re
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).parent.parent))
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

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
except ImportError:
    sync_playwright = None  # type: ignore[assignment,misc]
    PwTimeout = Exception  # type: ignore[assignment,misc]

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.codelco")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "codelco.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

FUENTE_ID = 275
INST_NOMBRE = "CODELCO — Corporación Nacional del Cobre"
REGION = "Nacional"
SECTOR = "Empresa del Estado"
URL_SEARCH = "https://empleos.codelco.cl/search/"

PAGE_LOAD_TIMEOUT = 30_000
SCROLL_PAUSE = 2000


def _check_playwright():
    if sync_playwright is None:
        raise ImportError(
            "Playwright no instalado. "
            "Instalar con: pip install playwright && playwright install chromium"
        )


def obtener_html_con_js() -> str | None:
    """Navega al portal de empleos de CODELCO y retorna el HTML renderizado."""
    _check_playwright()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            user_agent=random.choice(config.USER_AGENTS),
            locale="es-CL",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        try:
            logger.info("  Navegando a %s", URL_SEARCH)
            page.goto(URL_SEARCH, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)

            # Esperar a que carguen las ofertas
            for selector in [
                "[class*='job']", "[class*='position']", "[class*='vacancy']",
                "table tbody tr", ".card", "article", "main",
            ]:
                try:
                    page.wait_for_selector(selector, timeout=3000)
                    if page.locator(selector).count() > 0:
                        logger.info("  Contenido detectado: '%s'", selector)
                        break
                except PwTimeout:
                    continue

            # Scroll para cargar más resultados (lazy loading)
            for _ in range(3):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(SCROLL_PAUSE)

            html = page.content()
            logger.info("  HTML obtenido: %d caracteres", len(html))
            return html

        except Exception as exc:
            logger.exception("  Error Playwright CODELCO: %s", exc)
            return None
        finally:
            context.close()
            browser.close()


def parsear_html(html: str) -> list[dict]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    ofertas: list[dict] = []

    # Estrategia 1: tablas
    for row in soup.select("table tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        row_text = limpiar_texto(row.get_text(" ", strip=True))
        if not _es_oferta(row_text):
            continue
        link = row.find("a", href=True)
        href = link["href"] if link else ""
        cargo = limpiar_texto(cells[0].get_text(" ", strip=True))
        if not cargo or len(cargo) < 5:
            cargo = limpiar_texto(link.get_text(" ", strip=True)) if link else row_text[:200]
        ofertas.append(_construir(cargo, row_text, urljoin(URL_SEARCH, href)))
    if ofertas:
        return _dedup(ofertas)

    # Estrategia 2: cards/divs
    for sel in ["[class*='job']", "[class*='position']", "[class*='vacancy']",
                ".card", "[class*='listing']", "[class*='item']", "article"]:
        nodes = soup.select(sel)
        if len(nodes) >= 2:
            for node in nodes:
                text = limpiar_texto(node.get_text(" ", strip=True))
                if not _es_oferta(text):
                    continue
                title_el = node.select_one("h1, h2, h3, h4, h5, .title, [class*='title']")
                cargo = limpiar_texto(title_el.get_text(" ", strip=True)) if title_el else ""
                link = node.find("a", href=True)
                href = link["href"] if link else ""
                if not cargo:
                    cargo = limpiar_texto(link.get_text(" ", strip=True)) if link else text[:200]
                ofertas.append(_construir(cargo, text, urljoin(URL_SEARCH, href)))
            if ofertas:
                return _dedup(ofertas)

    # Estrategia 3: enlaces
    vistos = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href in vistos or href.startswith("#") or "javascript:" in href:
            continue
        texto = limpiar_texto(a.get_text(" ", strip=True))
        padre = a.find_parent(["li", "p", "div", "tr", "article"])
        ctx = limpiar_texto(padre.get_text(" ", strip=True)) if padre else texto
        if not _es_oferta(ctx):
            continue
        vistos.add(href)
        ofertas.append(_construir(texto[:300], ctx, urljoin(URL_SEARCH, href)))
    return _dedup(ofertas)


def _es_oferta(texto: str) -> bool:
    if len(texto) < 10:
        return False
    t = texto.lower()
    kw = ["cargo", "vacante", "ingeniero", "analista", "profesional", "técnico",
          "operador", "supervisor", "jefe", "geólogo", "minero", "metalurg",
          "mantenimiento", "postul", "concurso", "seleccion"]
    return sum(1 for k in kw if k in t) >= 1


def _construir(cargo: str, texto: str, url: str) -> dict:
    cargo = cargo.strip()[:500]
    id_ext = generar_id_estable(FUENTE_ID, INST_NOMBRE, cargo, texto)
    if not url or url == URL_SEARCH:
        url = f"{URL_SEARCH}#oferta-{id_ext}"

    region = REGION
    for r in ["calama", "antofagasta", "rancagua", "santiago", "chuquicamata"]:
        if r in texto.lower():
            region = normalizar_region(r) or REGION
            break

    return {
        "id_externo": id_ext,
        "fuente_id": FUENTE_ID,
        "url_original": url,
        "cargo": cargo,
        "descripcion": texto[:2000] if len(texto) > 30 else None,
        "institucion_nombre": INST_NOMBRE,
        "sector": SECTOR,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": "Código del Trabajo",
        "nivel": _nivel(cargo),
        "region": region,
        "ciudad": None,
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": date.today(),
        "fecha_cierre": _fecha(texto),
        "requisitos_texto": None,
    }


def _nivel(cargo: str) -> str:
    c = cargo.lower()
    if any(w in c for w in ["gerente", "director", "superintendente", "vicepresidente"]):
        return "Directivo"
    if any(w in c for w in ["jefe", "supervisor", "coordinador"]):
        return "Profesional"
    if any(w in c for w in ["técnico", "operador", "mecánico", "eléctrico"]):
        return "Técnico"
    return "Profesional"


def _fecha(texto: str) -> date | None:
    m = re.findall(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b", texto)
    if m:
        try:
            d, mo, y = m[-1]
            return date(int(y), int(mo), int(d))
        except ValueError:
            pass
    return None


def _dedup(ofertas: list[dict]) -> list[dict]:
    vistos, res = set(), []
    for o in ofertas:
        k = o["url_original"] + o["cargo"][:30]
        if k not in vistos:
            vistos.add(k)
            res.append(o)
    return res


def ejecutar(dry_run: bool = False, verbose: bool = False) -> dict:
    _check_playwright()
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper CODELCO")
    logger.info("=" * 60)

    db = SessionLocal()
    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
    urls_activas: list[str] = []

    try:
        html = obtener_html_con_js()
        if not html:
            stats["errores"] += 1
            return stats

        ofertas = parsear_html(html)
        logger.info("  → %d ofertas encontradas", len(ofertas))

        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  {datos['cargo'][:70]} | {datos['region']}")
            if not dry_run:
                try:
                    nueva, act = upsert_oferta(db, datos)
                    if nueva:
                        stats["nuevas"] += 1
                    elif act:
                        stats["actualizadas"] += 1
                except Exception as e:
                    stats["errores"] += 1
                    db.rollback()
                    logger.exception("  Error oferta: %s", e)

        if not dry_run and urls_activas:
            stats["cerradas"] = marcar_ofertas_cerradas(db, FUENTE_ID, sorted(urls_activas))

    except Exception as e:
        if not dry_run:
            db.rollback()
        logger.exception("  Error CODELCO: %s", e)
        stats["errores"] += 1
        raise
    finally:
        dur = time.time() - inicio
        logger.info("  Nuevas: %d | Act: %d | Cerradas: %d | Err: %d | %.1fs",
                     stats["nuevas"], stats["actualizadas"], stats["cerradas"],
                     stats["errores"], dur)
        if not dry_run:
            try:
                db.rollback()
                registrar_log(db, FUENTE_ID,
                              "OK" if stats["errores"] == 0 else "PARCIAL",
                              ofertas_nuevas=stats["nuevas"],
                              ofertas_actualizadas=stats["actualizadas"],
                              ofertas_cerradas=stats["cerradas"],
                              paginas=1, duracion=dur)
            except Exception:
                logger.exception("  No se pudo registrar log")
        db.close()
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(description="Scraper CODELCO (requiere Playwright)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    ejecutar(**vars(p.parse_args()))
