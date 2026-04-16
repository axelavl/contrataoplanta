"""
EmpleoEstado.cl — Scraper: Banco Central de Chile
URLs:
    - https://www.bcentral.cl/el-banco/trabaja-en-el-bc/oportunidades-de-trabajo
    - https://empleos.bcentral.cl/

Requiere Playwright (el portal carga ofertas dinámicamente con JavaScript).
Instalar:  pip install playwright && playwright install chromium

Uso:
    python scrapers/banco_central.py
    python scrapers/banco_central.py --dry-run
    python scrapers/banco_central.py --dry-run --verbose
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

logger = logging.getLogger("scraper.banco_central")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(
        LOG_DIR / "banco_central.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
logger.propagate = False

FUENTE_ID = 145
INST_NOMBRE = "Banco Central de Chile"
REGION = "Nacional"
SECTOR = "Autónomo/Regulador"

# URLs a intentar en orden de prioridad
URLS_EMPLEO = [
    "https://empleos.bcentral.cl/",
    "https://www.bcentral.cl/el-banco/trabaja-en-el-bc/oportunidades-de-trabajo",
]

# Tiempo máximo de espera para que cargue el contenido dinámico (ms)
PAGE_LOAD_TIMEOUT = 30_000
CONTENT_WAIT_TIMEOUT = 15_000

# Keywords para detectar ofertas laborales en el DOM
KEYWORDS_OFERTA = [
    "concurso", "vacante", "cargo", "postulación", "postulacion",
    "contrata", "planta", "honorario", "selección", "seleccion",
    "profesional", "analista", "ingeniero", "abogado", "economista",
    "asesor", "jefe", "coordinador", "especialista", "técnico",
]


def _check_playwright():
    """Verifica que Playwright esté instalado y disponible."""
    if sync_playwright is None:
        raise ImportError(
            "Playwright no está instalado. "
            "Instalar con: pip install playwright && playwright install chromium"
        )


def obtener_html_con_js(url: str) -> str | None:
    """
    Navega a la URL con un browser headless y retorna el HTML
    después de que el contenido dinámico haya cargado.
    """
    _check_playwright()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        context = browser.new_context(
            user_agent=random.choice(config.USER_AGENTS),
            locale="es-CL",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        try:
            logger.info("  Navegando a %s", url)
            page.goto(url, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)

            # Esperar a que aparezca contenido de ofertas.
            # Intentar varios selectores comunes en portales de empleo.
            selectors_to_try = [
                # Tablas, cards, listas de ofertas
                "table tbody tr",
                "[class*='job'], [class*='oferta'], [class*='vacancy']",
                "[class*='card'], [class*='listing']",
                "article",
                ".entry-content li a",
                # Fallback: cualquier enlace con texto relevante
                "a[href*='concurso'], a[href*='postul'], a[href*='empleo']",
                "main",
            ]

            content_loaded = False
            for selector in selectors_to_try:
                try:
                    page.wait_for_selector(selector, timeout=3000)
                    count = page.locator(selector).count()
                    if count > 0:
                        logger.info(
                            "  Contenido detectado: %d elementos con '%s'",
                            count,
                            selector,
                        )
                        content_loaded = True
                        break
                except PwTimeout:
                    continue

            if not content_loaded:
                # Espera genérica por si el contenido tarda
                logger.warning("  No se detectó selector específico, esperando 5s extra")
                page.wait_for_timeout(5000)

            html = page.content()
            logger.info("  HTML obtenido: %d caracteres", len(html))
            return html

        except PwTimeout:
            logger.error("  Timeout navegando a %s", url)
            return None
        except Exception as exc:
            logger.exception("  Error Playwright en %s: %s", url, exc)
            return None
        finally:
            context.close()
            browser.close()


def parsear_html(html: str, url_fuente: str) -> list[dict]:
    """
    Parsea el HTML renderizado y extrae ofertas laborales.

    Estrategias (en orden):
    1. Tablas con filas de ofertas
    2. Cards/divs estructurados
    3. Listas de enlaces
    4. Fallback: enlaces sueltos con keywords
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    ofertas: list[dict] = []

    # ── Estrategia 1: tablas ──
    ofertas = _parse_tablas(soup, url_fuente)
    if ofertas:
        logger.info("  Parseado con estrategia: tablas (%d ofertas)", len(ofertas))
        return _deduplicar(ofertas)

    # ── Estrategia 2: cards/contenedores estructurados ──
    ofertas = _parse_cards(soup, url_fuente)
    if ofertas:
        logger.info("  Parseado con estrategia: cards (%d ofertas)", len(ofertas))
        return _deduplicar(ofertas)

    # ── Estrategia 3: listas de enlaces ──
    ofertas = _parse_enlaces(soup, url_fuente)
    if ofertas:
        logger.info("  Parseado con estrategia: enlaces (%d ofertas)", len(ofertas))
        return _deduplicar(ofertas)

    logger.warning("  No se encontraron ofertas en el HTML")
    return []


def _parse_tablas(soup, url_fuente: str) -> list[dict]:
    """Extrae ofertas de tablas HTML."""
    ofertas = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        for row in rows[1:]:  # Saltar header
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            row_text = limpiar_texto(row.get_text(" ", strip=True))
            if not _es_oferta(row_text):
                continue

            # Primer enlace de la fila
            link = row.find("a", href=True)
            href = link["href"] if link else ""
            url_oferta = urljoin(url_fuente, href) if href else url_fuente

            # Primer celda suele ser el cargo
            cargo = limpiar_texto(cells[0].get_text(" ", strip=True))
            if not cargo or len(cargo) < 5:
                cargo = limpiar_texto(link.get_text(" ", strip=True)) if link else row_text[:200]

            oferta = _construir_oferta(
                cargo=cargo,
                texto=row_text,
                url=url_oferta,
                url_fuente=url_fuente,
                celdas=[limpiar_texto(c.get_text(" ", strip=True)) for c in cells],
            )
            if oferta:
                ofertas.append(oferta)

    return ofertas


def _parse_cards(soup, url_fuente: str) -> list[dict]:
    """Extrae ofertas de divs/articles tipo card."""
    ofertas = []
    selectors = [
        "div[class*='job']", "div[class*='oferta']", "div[class*='vacancy']",
        "div[class*='card']", "div[class*='listing']", "div[class*='item']",
        "article", "li[class*='job']", "li[class*='item']",
    ]

    containers = []
    for sel in selectors:
        containers = soup.select(sel)
        # Solo usar si hay múltiples (parecen ser listado)
        if len(containers) >= 2:
            break
    else:
        containers = []

    for node in containers:
        node_text = limpiar_texto(node.get_text(" ", strip=True))
        if not _es_oferta(node_text):
            continue

        # Buscar título
        title_el = node.select_one("h1, h2, h3, h4, h5, .title, [class*='title'], [class*='cargo']")
        cargo = limpiar_texto(title_el.get_text(" ", strip=True)) if title_el else ""

        # Buscar enlace
        link = node.find("a", href=True)
        href = link["href"] if link else ""
        url_oferta = urljoin(url_fuente, href) if href else url_fuente

        if not cargo:
            cargo = limpiar_texto(link.get_text(" ", strip=True)) if link else node_text[:200]

        oferta = _construir_oferta(
            cargo=cargo,
            texto=node_text,
            url=url_oferta,
            url_fuente=url_fuente,
        )
        if oferta:
            ofertas.append(oferta)

    return ofertas


def _parse_enlaces(soup, url_fuente: str) -> list[dict]:
    """Fallback: extrae ofertas de enlaces individuales."""
    IGNORAR = {"#", "javascript:", "mailto:", "instagram", "twitter",
               "facebook", "youtube", "linkedin"}
    ofertas = []
    vistos = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(ig in href.lower() for ig in IGNORAR) or href in vistos:
            continue

        texto_link = limpiar_texto(a.get_text(" ", strip=True))
        if len(texto_link) < 5:
            continue

        # Buscar contexto en el padre
        padre = a.find_parent(["li", "p", "div", "tr", "article"])
        contexto = limpiar_texto(padre.get_text(" ", strip=True)) if padre else texto_link

        if not _es_oferta(contexto):
            continue

        url_oferta = urljoin(url_fuente, href)
        vistos.add(href)

        oferta = _construir_oferta(
            cargo=texto_link[:300],
            texto=contexto,
            url=url_oferta,
            url_fuente=url_fuente,
        )
        if oferta:
            ofertas.append(oferta)

    return ofertas


def _es_oferta(texto: str) -> bool:
    """Determina si un texto describe una oferta laboral."""
    if len(texto) < 10:
        return False
    texto_l = texto.lower()
    return sum(1 for kw in KEYWORDS_OFERTA if kw in texto_l) >= 1


def _construir_oferta(
    cargo: str,
    texto: str,
    url: str,
    url_fuente: str,
    celdas: list[str] | None = None,
) -> dict | None:
    """Construye un dict de oferta normalizado."""
    cargo = cargo.strip()
    if not cargo or len(cargo) < 5:
        return None

    id_externo = generar_id_estable(FUENTE_ID, INST_NOMBRE, cargo, texto)

    # Si no hay URL única, generar una con hash
    if not url or url == url_fuente:
        url = f"{url_fuente}#oferta-{id_externo}"

    # Intentar extraer tipo de contrato del texto
    tipo_cargo = _inferir_tipo(texto)
    nivel = _inferir_nivel(cargo)
    area = normalizar_area(cargo)
    fecha_cierre = _extraer_fecha(texto)

    # Intentar extraer renta
    renta_min, renta_max = _extraer_renta(texto)

    # Si hay celdas, intentar mapear campos adicionales
    region = REGION
    if celdas and len(celdas) >= 2:
        for celda in celdas[1:]:
            celda_l = celda.lower()
            # Detectar región en celdas
            if any(r in celda_l for r in ["region", "santiago", "nacional"]):
                region = normalizar_region(celda) or REGION

    return {
        "id_externo": id_externo,
        "fuente_id": FUENTE_ID,
        "url_original": url,
        "cargo": cargo[:500],
        "descripcion": texto[:2000] if len(texto) > 30 else None,
        "institucion_nombre": INST_NOMBRE,
        "sector": SECTOR,
        "area_profesional": area,
        "tipo_cargo": tipo_cargo,
        "nivel": nivel,
        "region": region,
        "ciudad": "Santiago",
        "renta_bruta_min": renta_min,
        "renta_bruta_max": renta_max,
        "renta_texto": None,
        "fecha_publicacion": date.today(),
        "fecha_cierre": fecha_cierre,
        "requisitos_texto": None,
    }


def _inferir_tipo(texto: str) -> str:
    t = texto.lower()
    if "planta" in t:
        return "Planta"
    if "contrata" in t:
        return "Contrata"
    if "honorario" in t:
        return "Honorarios"
    if "código del trabajo" in t or "codigo del trabajo" in t:
        return "Código del Trabajo"
    return "Contrata"


def _inferir_nivel(cargo: str) -> str:
    c = cargo.lower()
    if any(w in c for w in ["gerente", "director", "jefe", "subgerente"]):
        return "Directivo"
    if any(w in c for w in ["economista", "abogado", "ingeniero", "analista senior",
                             "asesor", "investigador", "auditor"]):
        return "Profesional"
    if any(w in c for w in ["técnico", "asistente"]):
        return "Técnico"
    if any(w in c for w in ["administrativo", "secretaria"]):
        return "Administrativo"
    return "Profesional"


def _extraer_fecha(texto: str) -> date | None:
    """Extrae la fecha de cierre del texto."""
    MESES = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }
    # Fecha numérica: dd/mm/yyyy o dd-mm-yyyy
    m = re.findall(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b", texto)
    if m:
        try:
            d, mo, y = m[-1]
            return date(int(y), int(mo), int(d))
        except ValueError:
            pass
    # Fecha textual: "20 de junio de 2026"
    m = re.search(
        r"\b(\d{1,2})\s+de\s+(" + "|".join(MESES) + r")\s+(?:de\s+)?(\d{4})\b",
        texto.lower(),
    )
    if m:
        try:
            return date(int(m.group(3)), MESES[m.group(2)], int(m.group(1)))
        except ValueError:
            pass
    return None


def _extraer_renta(texto: str) -> tuple[int | None, int | None]:
    """Extrae renta bruta del texto."""
    montos = re.findall(r"\$?\s*(\d{1,3}(?:[.,]\d{3})+)", texto)
    limpios = []
    for mo in montos:
        try:
            n = int(mo.replace(".", "").replace(",", ""))
            if 300_000 <= n <= 15_000_000:
                limpios.append(n)
        except ValueError:
            continue
    if not limpios:
        return None, None
    if len(limpios) == 1:
        return limpios[0], limpios[0]
    return min(limpios), max(limpios)


def _deduplicar(ofertas: list[dict]) -> list[dict]:
    vistos, resultado = set(), []
    for o in ofertas:
        k = o["url_original"] + o["cargo"][:30]
        if k not in vistos:
            vistos.add(k)
            resultado.append(o)
    return resultado


# ────────────────────────── Ejecutor principal ──────────────────────────


def ejecutar(dry_run: bool = False, verbose: bool = False) -> dict:
    _check_playwright()

    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Banco Central de Chile")
    logger.info("=" * 60)

    db = SessionLocal()
    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
    urls_activas: list[str] = []
    todas_ofertas: list[dict] = []

    try:
        # Intentar cada URL hasta obtener resultados
        for url in URLS_EMPLEO:
            html = obtener_html_con_js(url)
            if not html:
                stats["errores"] += 1
                continue

            ofertas = parsear_html(html, url)
            logger.info("  → %d ofertas en %s", len(ofertas), url)
            todas_ofertas.extend(ofertas)

            # Si encontramos ofertas, no seguir con URLs adicionales
            if ofertas:
                break

            time.sleep(config.DELAY_ENTRE_REQUESTS)

        todas_ofertas = _deduplicar(todas_ofertas)
        logger.info("  Total (deduplicado): %d ofertas", len(todas_ofertas))

        for datos in todas_ofertas:
            urls_activas.append(datos["url_original"])

            if verbose or dry_run:
                print(f"\n  {'─' * 52}")
                print(f"  Cargo:  {datos['cargo'][:65]}")
                print(f"  Tipo:   {datos['tipo_cargo']} | Nivel: {datos['nivel']}")
                print(f"  Área:   {datos['area_profesional']}")
                print(f"  Cierre: {datos['fecha_cierre']}")
                print(f"  URL:    {datos['url_original'][:75]}")

            if not dry_run:
                try:
                    nueva, actualizada = upsert_oferta(db, datos)
                    if nueva:
                        stats["nuevas"] += 1
                    elif actualizada:
                        stats["actualizadas"] += 1
                except Exception as e:
                    stats["errores"] += 1
                    db.rollback()
                    logger.exception(
                        "  Error procesando oferta %s: %s",
                        datos.get("id_externo") or datos["url_original"],
                        e,
                    )
                    continue

        if not dry_run and urls_activas:
            stats["cerradas"] = marcar_ofertas_cerradas(
                db, FUENTE_ID, sorted(urls_activas)
            )

    except Exception as e:
        if not dry_run:
            db.rollback()
        logger.exception("  Error en scraper Banco Central: %s", e)
        stats["errores"] += 1
        raise
    finally:
        duracion = time.time() - inicio
        logger.info(
            "  Nuevas: %d | Actualizadas: %d | Cerradas: %d | Errores: %d | %.1fs",
            stats["nuevas"],
            stats["actualizadas"],
            stats["cerradas"],
            stats["errores"],
            duracion,
        )
        if not dry_run:
            try:
                db.rollback()
                registrar_log(
                    db,
                    FUENTE_ID,
                    "OK" if stats["errores"] == 0 else "PARCIAL",
                    ofertas_nuevas=stats["nuevas"],
                    ofertas_actualizadas=stats["actualizadas"],
                    ofertas_cerradas=stats["cerradas"],
                    paginas=len(URLS_EMPLEO),
                    duracion=duracion,
                )
            except Exception:
                logger.exception("  No se pudo registrar el log final")
        db.close()

    return stats


if __name__ == "__main__":
    import os

    os.makedirs(config.LOG_DIR, exist_ok=True)
    parser = argparse.ArgumentParser(
        description="Scraper Banco Central de Chile (requiere Playwright)"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    ejecutar(dry_run=args.dry_run, verbose=args.verbose)
