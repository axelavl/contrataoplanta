"""
EmpleoEstado.cl — Scraper genérico para portales Trabajando.cl

Cubre todas las instituciones públicas cuyo `url_empleo` corresponda a un
subdominio de `trabajando.cl` (CMF, FACH, UMAG, UTalca, ENAP, BancoEstado,
Metro, Correos, ZOFRI, CDS Providencia) y también el portal externo de la
Universidad de Chile (`externouchile.trabajando.cl`), registrado fuera del
JSON porque la entrada principal de UCH usa `empleos.uchile.cl`.

Estrategia:
    1. HTTP directo sobre varias rutas candidatas (Trabajando.cl renderiza
       sus listados en servidor, no necesita JS en la mayoría de los casos).
    2. Fallback a Playwright si el HTML no trae ofertas (bloqueo WAF o
       render tardío con JS).
    3. Parseo heurístico: enlaces a `/ficha-de-empleo/*` -> tablas -> cards.

Uso:
    python scrapers/trabajando.py                         # todas las fuentes
    python scrapers/trabajando.py --dry-run --verbose
    python scrapers/trabajando.py --id 149                # solo una fuente
    python scrapers/trabajando.py --host externouchile    # solo una fuente por host
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

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

logger = logging.getLogger("scraper.trabajando")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "trabajando.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

REPO_PATH = Path(__file__).resolve().parents[1] / "repositorio_instituciones_publicas_chile.json"

HTTP_TIMEOUT = 20
PAGE_LOAD_TIMEOUT = 30_000

# Rutas candidatas estándar de la plataforma Trabajando.cl.
_PATH_CANDIDATES = (
    "",                     # home normalmente lista ofertas activas
    "/trabajo-empleo",
    "/empleos",
    "/empleos/buscar-empleos",
    "/buscar-empleos",
    "/ofertas-laborales",
)

_KEYWORDS_OFERTA = (
    "cargo", "vacante", "oferta", "empleo", "postul",
    "profesional", "analista", "ingenier", "coordinador", "coordinadora",
    "asistente", "tecnico", "técnico", "academico", "académico", "docente",
    "administrativo", "administrativa", "secretario", "secretaria",
    "auxiliar", "encargado", "encargada", "jefe", "jefa",
    "investigador", "investigadora", "asesor", "asesora",
    "operario", "operaria", "operador", "operadora",
    "ejecutivo", "ejecutiva", "supervisor", "supervisora",
)

_DETAIL_HREF_RE = re.compile(r"(ficha|oferta|empleo|job[-_ ]?offer|postul)", re.I)


# ── Configuración de fuentes Trabajando.cl ──────────────────────────────────
# Algunas fuentes (como la UCH-externo) no están en el JSON maestro con
# subdominio trabajando.cl; se agregan manualmente acá.
_EXTRA_SOURCES: list[dict[str, Any]] = [
    {
        "id": 242,  # Universidad de Chile
        "nombre": "Universidad de Chile (Portal Externo)",
        "sigla": "UCH",
        "sector": "Universidad/Educación",
        "region": "Nacional",
        "url_empleo": "https://externouchile.trabajando.cl/",
    },
]


def _load_all_instituciones(path: Path = REPO_PATH) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload.get("instituciones") if isinstance(payload, dict) else payload


def _filter_trabajando(
    instituciones: list[dict[str, Any]],
    solo_id: int | None = None,
    solo_host: str | None = None,
) -> list[dict[str, Any]]:
    candidatos: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    for fuente in list(instituciones) + _EXTRA_SOURCES:
        url = (fuente.get("url_empleo") or "").lower()
        if "trabajando.cl" not in url:
            continue
        fid = fuente.get("id")
        if fid in seen_ids:
            continue
        seen_ids.add(fid)
        candidatos.append(fuente)

    if solo_id is not None:
        candidatos = [c for c in candidatos if c.get("id") == solo_id]
    if solo_host:
        h = solo_host.lower().replace("https://", "").replace("http://", "").rstrip("/")
        candidatos = [c for c in candidatos if h in (c.get("url_empleo") or "").lower()]

    return candidatos


# ── Fetch HTTP ──────────────────────────────────────────────────────────────
def _http_get(url: str) -> str | None:
    import requests

    headers = {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
        resp.encoding = resp.encoding or "utf-8"
        if resp.status_code >= 400:
            logger.info("  HTTP %s en %s", resp.status_code, url)
            return None
        return resp.text
    except Exception as exc:
        logger.info("  HTTP fallo %s: %s", url, type(exc).__name__)
        return None


# ── Fallback con Playwright ─────────────────────────────────────────────────
def _pw_get(url: str) -> str | None:
    if sync_playwright is None:
        return None

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
            page.goto(url, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)
            for selector in (
                "a[href*='ficha']", "a[href*='empleo']", "a[href*='oferta']",
                "[class*='oferta']", "[class*='job']", "table tbody tr", "main",
            ):
                try:
                    page.wait_for_selector(selector, timeout=2500)
                    if page.locator(selector).count() > 0:
                        break
                except PwTimeout:
                    continue
            else:
                page.wait_for_timeout(4000)
            for _ in range(3):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)
            return page.content()
        except Exception as exc:
            logger.info("  Playwright error %s: %s", url, type(exc).__name__)
            return None
        finally:
            context.close()
            browser.close()


def _fetch_html(url: str) -> str | None:
    html = _http_get(url)
    if html and _tiene_indicios_de_ofertas(html):
        return html
    logger.info("  HTML HTTP sin indicios, probando Playwright para %s", url)
    return _pw_get(url) or html


def _tiene_indicios_de_ofertas(html: str) -> bool:
    lower = html.lower()
    if len(lower) < 1500:
        return False
    hits = sum(1 for kw in ("ficha", "oferta", "empleo", "postul", "cargo", "vacante")
               if kw in lower)
    return hits >= 2


# ── Parseo ──────────────────────────────────────────────────────────────────
def parsear_html(html: str, url_fuente: str, allowed_host: str) -> list[dict]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    ofertas: list[dict] = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        if not _DETAIL_HREF_RE.search(href):
            continue

        url_abs = urljoin(url_fuente, href)
        parsed = urlparse(url_abs)
        if parsed.netloc and allowed_host not in parsed.netloc:
            continue
        if parsed.path.rstrip("/") in {"", "/empleos", "/ofertas", "/ficha-de-empleo"}:
            continue

        titulo = limpiar_texto(a.get_text(" ", strip=True))
        contenedor = a.find_parent(["article", "li", "tr", "div", "section"])
        contexto = limpiar_texto(contenedor.get_text(" ", strip=True)) if contenedor else titulo
        if not titulo:
            titulo = contexto[:200]
        if not _parece_oferta(titulo, contexto):
            continue
        ofertas.append((titulo, contexto, url_abs))

    if not ofertas:
        for row in soup.select("table tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            texto = limpiar_texto(row.get_text(" ", strip=True))
            if not _parece_oferta(texto, texto):
                continue
            link = row.find("a", href=True)
            href = link["href"] if link else ""
            url_abs = urljoin(url_fuente, href) if href else url_fuente
            cargo = limpiar_texto(cells[0].get_text(" ", strip=True)) or texto[:200]
            ofertas.append((cargo, texto, url_abs))

    if not ofertas:
        for sel in ("[class*='oferta']", "[class*='job']", "[class*='card']", "article"):
            encontrados: list[tuple[str, str, str]] = []
            for node in soup.select(sel):
                texto = limpiar_texto(node.get_text(" ", strip=True))
                if not _parece_oferta(texto, texto):
                    continue
                titulo_el = node.select_one("h1, h2, h3, h4, h5, .title, [class*='title']")
                cargo = (limpiar_texto(titulo_el.get_text(" ", strip=True))
                         if titulo_el else texto[:200])
                link = node.find("a", href=True)
                href = link["href"] if link else ""
                url_abs = urljoin(url_fuente, href) if href else url_fuente
                encontrados.append((cargo, texto, url_abs))
            if encontrados:
                ofertas = encontrados
                break

    return _deduplicar(ofertas)


def _parece_oferta(titulo: str, contexto: str) -> bool:
    total = f"{titulo} {contexto}".lower()
    if len(total) < 15:
        return False
    return any(kw in total for kw in _KEYWORDS_OFERTA)


def _construir_oferta(
    cargo: str,
    contexto: str,
    url: str,
    fuente: dict[str, Any],
) -> dict:
    cargo_limpio = (cargo or "").strip()[:500]
    fuente_id = int(fuente["id"])
    nombre = fuente.get("nombre") or ""
    sector = fuente.get("sector") or None
    region_default = fuente.get("region") or "Nacional"

    id_externo = generar_id_estable(fuente_id, nombre, cargo_limpio, contexto[:300])
    region_detectada = _detectar_region(contexto) or region_default
    ciudad = _detectar_ciudad(contexto)

    return {
        "id_externo": id_externo,
        "fuente_id": fuente_id,
        "url_original": url,
        "cargo": cargo_limpio,
        "descripcion": contexto[:2000] if len(contexto) > 30 else None,
        "institucion_nombre": nombre,
        "sector": sector,
        "area_profesional": normalizar_area(cargo_limpio),
        "tipo_cargo": _detectar_tipo_cargo(contexto) or "Código del Trabajo",
        "nivel": _detectar_nivel(cargo_limpio),
        "region": region_detectada,
        "ciudad": ciudad,
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": date.today(),
        "fecha_cierre": _extraer_fecha_cierre(contexto),
        "requisitos_texto": None,
    }


def _detectar_tipo_cargo(texto: str) -> str | None:
    t = texto.lower()
    if "planta" in t:
        return "Planta"
    if "contrata" in t:
        return "Contrata"
    if "honorario" in t:
        return "Honorarios"
    if "reemplazo" in t:
        return "Reemplazo"
    return None


def _detectar_nivel(cargo: str) -> str:
    c = cargo.lower()
    if any(w in c for w in ("decano", "decana", "director", "directora",
                            "vicerrector", "gerente", "subgerente")):
        return "Directivo"
    if any(w in c for w in ("jefe", "jefa", "coordinador", "coordinadora",
                            "supervisor", "supervisora")):
        return "Profesional"
    if any(w in c for w in ("tecnico", "técnico", "auxiliar", "operador",
                            "operario", "administrativo")):
        return "Técnico"
    return "Profesional"


def _detectar_region(texto: str) -> str | None:
    t = texto.lower()
    if not t:
        return None
    rm_cues = ("santiago", "metropolitana", "providencia", "ñuñoa", "nunoa",
               "independencia", "estacion central", "estación central",
               "las condes", "maipu", "maipú")
    if any(x in t for x in rm_cues):
        return "Metropolitana de Santiago"
    for region in ("arica", "tarapaca", "antofagasta", "atacama", "coquimbo",
                   "valparaiso", "valparaíso", "ohiggins", "maule",
                   "nuble", "ñuble", "biobio", "biobío", "araucania",
                   "araucanía", "los rios", "los ríos", "los lagos",
                   "aysen", "aysén", "magallanes", "punta arenas", "talca"):
        if region in t:
            return normalizar_region(region)
    return None


def _detectar_ciudad(texto: str) -> str | None:
    t = texto.lower()
    for ciudad in ("santiago", "providencia", "ñuñoa", "independencia",
                   "estación central", "antofagasta", "valparaíso",
                   "viña del mar", "concepción", "temuco", "valdivia",
                   "puerto montt", "punta arenas", "talca", "iquique"):
        if ciudad in t:
            return ciudad.title()
    return None


def _extraer_fecha_cierre(texto: str) -> date | None:
    m = re.findall(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})\b", texto)
    if m:
        try:
            d, mo, y = m[-1]
            y = int(y)
            if y < 100:
                y += 2000
            return date(y, int(mo), int(d))
        except ValueError:
            pass
    return None


def _deduplicar(items: Iterable[Any]) -> list[Any]:
    vistos: set[str] = set()
    resultado: list[Any] = []
    for item in items:
        if isinstance(item, tuple):
            clave = item[2] + "||" + (item[0] or "")[:50]
        else:
            clave = item["url_original"] + "||" + item["cargo"][:50]
        if clave in vistos:
            continue
        vistos.add(clave)
        resultado.append(item)
    return resultado


# ── Procesamiento por fuente ────────────────────────────────────────────────
def _base_url(fuente: dict[str, Any]) -> str:
    url = (fuente.get("url_empleo") or "").strip()
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return url.rstrip("/")


def _recolectar_fuente(fuente: dict[str, Any], max_results: int | None) -> list[dict]:
    base = _base_url(fuente)
    if not base:
        return []
    host = urlparse(base).netloc
    vistos: set[str] = set()
    ofertas: list[dict] = []

    for path in _PATH_CANDIDATES:
        url = base + path
        html = _fetch_html(url)
        if not html:
            continue
        tuplas = parsear_html(html, url, allowed_host=host)
        nuevos = 0
        for cargo, contexto, url_abs in tuplas:
            if url_abs in vistos:
                continue
            vistos.add(url_abs)
            ofertas.append(_construir_oferta(cargo, contexto, url_abs, fuente))
            nuevos += 1
            if max_results and len(ofertas) >= max_results:
                return ofertas
        if nuevos and not max_results:
            break  # la primera URL con resultados suele bastar

    return ofertas


def _procesar_fuente(
    fuente: dict[str, Any],
    dry_run: bool,
    verbose: bool,
    max_results: int | None,
) -> dict[str, int]:
    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    nombre = fuente.get("nombre") or f"id={fuente.get('id')}"
    fuente_id = int(fuente["id"])
    inicio = time.time()

    logger.info("─── %s (id=%s) ───", nombre, fuente_id)
    db = SessionLocal()
    urls_activas: list[str] = []

    try:
        ofertas = _recolectar_fuente(fuente, max_results=max_results)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))

        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [{nombre[:30]}] {datos['cargo'][:70]}")
                if verbose:
                    print(f"      {datos['url_original']}")
            if dry_run:
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

        if not dry_run and urls_activas:
            stats["cerradas"] = marcar_ofertas_cerradas(db, fuente_id, sorted(urls_activas))
    except Exception as exc:
        if not dry_run:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente %s: %s", nombre, exc)
    finally:
        dur = time.time() - inicio
        if not dry_run:
            try:
                db.rollback()
                registrar_log(
                    db, fuente_id,
                    "OK" if stats["errores"] == 0 else "PARCIAL",
                    ofertas_nuevas=stats["nuevas"],
                    ofertas_actualizadas=stats["actualizadas"],
                    ofertas_cerradas=stats["cerradas"],
                    paginas=len(_PATH_CANDIDATES), duracion=dur,
                )
            except Exception:
                logger.exception("  No se pudo registrar log para %s", nombre)
        db.close()

    return stats


# ── Orquestación pública ────────────────────────────────────────────────────
def ejecutar(
    dry_run: bool = False,
    verbose: bool = False,
    max_results: int | None = None,
    solo_id: int | None = None,
    solo_host: str | None = None,
) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper genérico Trabajando.cl")
    logger.info("=" * 60)

    instituciones = _load_all_instituciones()
    fuentes = _filter_trabajando(instituciones, solo_id=solo_id, solo_host=solo_host)
    logger.info("  %d fuentes Trabajando.cl seleccionadas", len(fuentes))

    agregado = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}

    for fuente in fuentes:
        stats = _procesar_fuente(
            fuente, dry_run=dry_run, verbose=verbose, max_results=max_results,
        )
        for k, v in stats.items():
            agregado[k] = agregado.get(k, 0) + v

    dur = time.time() - inicio
    logger.info(
        "RESUMEN Trabajando.cl: fuentes=%d encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
        len(fuentes), agregado["encontradas"], agregado["nuevas"],
        agregado["actualizadas"], agregado["cerradas"], agregado["errores"], dur,
    )
    agregado["duracion_seg"] = round(dur, 2)
    agregado["status"] = "OK" if agregado["errores"] == 0 else "PARCIAL"
    return agregado


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    parser = argparse.ArgumentParser(
        description="Scraper genérico para portales *.trabajando.cl"
    )
    parser.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--max", type=int, default=None, help="Tope de ofertas por fuente")
    parser.add_argument("--id", type=int, default=None, help="Procesar solo esta fuente (id)")
    parser.add_argument("--host", default=None,
                        help="Procesar solo la fuente cuyo url_empleo contenga este host")
    args = parser.parse_args()
    ejecutar(
        dry_run=args.dry_run,
        verbose=args.verbose,
        max_results=args.max,
        solo_id=args.id,
        solo_host=args.host,
    )
