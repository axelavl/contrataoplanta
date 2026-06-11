"""
EmpleoEstado.cl — Scraper de oportunidades de trabajo del Banco Central de
Chile (https://www.bcentral.cl/el-banco/trabaja-en-el-bc/oportunidades-de-trabajo).
Patrón estándar de sesión.

IMPORTANTE — REQUIERE PLAYWRIGHT: bcentral.cl está protegido por Imperva
Incapsula con desafío JavaScript; requests/curl reciben solo el stub del
challenge. Chromium headless con ajustes básicos de stealth lo supera
(verificado: el primer goto carga el challenge, una recarga entrega la
página completa). Instalación:
    pip install playwright && python -m playwright install chromium

Nota: el portal alternativo empleos.bcentral.cl (SuccessFactors) responde
"0 vacantes" — está desactualizado; la fuente canónica es el sitio
corporativo (Liferay), que es la que usa este scraper.

Estructura verificada (06/2026): cada vacante es un div.row con
    h5.fifth-title                 → cargo
    "(Concurso NNNN)"              → id estable
    párrafo "Se llama a postulación, hasta el X de Y de Z, ... a plazo
    indefinido/fijo ... Nivel N ... Gerencia ... División ..."
    "Cierre de publicación: DD-MM-YYYY" / "Estado: Postulación"
    a[href] → /oportunidades-de-trabajo-detalle/-/detalle/oportunidad-de-trabajo-NNNN
El detalle agrega: Gerencia, Tipo de contrato, Vacantes, Fecha de
postulación (publicación), Circular.

El Banco Central no publica renta en estas páginas (renta_bruta_* queda
vacío); el "Nivel de la estructura de cargos" va en la descripción.

Uso:
    python scrapers/bcentral.py --dry-run --verbose
    python scrapers/bcentral.py --dry-run --export ofertas_bc
    python scrapers/bcentral.py --sin-detalle      # solo listado (1 carga)
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

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

logger = logging.getLogger("scraper.bcentral")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "bcentral.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://www.bcentral.cl"
LISTADO_URL = BASE + "/el-banco/trabaja-en-el-bc/oportunidades-de-trabajo"
FUENTE = {
    "id": 145,  # Banco Central de Chile (id real del catálogo)
    "nombre": "Banco Central de Chile",
    "sigla": "BCCH",
    "sector": "Autonomía Constitucional / Banca Central",
    "region": "Metropolitana de Santiago",  # sede única, Agustinas 1180
    "url_empleo": LISTADO_URL,
}

PAGE_TIMEOUT = 60_000
CHALLENGE_WAIT_MS = 8_000
MAX_RELOADS = 4
DELAY_DEFAULT = 2.0

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

_RE_CONCURSO = re.compile(r"\(?\s*Concurso\s+(\d{3,5})\s*\)?", re.I)
_RE_NIVEL = re.compile(r"Nivel\s+(\d{1,2})\s+de la estructura", re.I)
_RE_FECHA_DDMMYYYY = re.compile(r"(\d{1,2})-(\d{1,2})-(\d{4})")
_RE_FECHA_ES = re.compile(r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", re.I)
_RE_VACANTES_TXT = re.compile(
    r"cubrir\s+(una|dos|tres|cuatro|cinco|\d+)\s+vacantes?", re.I)
_NUM_PALABRA = {"una": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5}

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── Navegación con Playwright (Incapsula) ────────────────────────────────────
def _abrir_navegador(pw):
    browser = pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage",
              "--disable-blink-features=AutomationControlled"])
    ctx = browser.new_context(
        user_agent=config.USER_AGENTS[0],
        locale="es-CL", timezone_id="America/Santiago",
        viewport={"width": 1366, "height": 900})
    ctx.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return browser, ctx


def _fetch(page, url: str) -> str | None:
    """Carga una URL superando el challenge de Incapsula (recargas)."""
    for intento in range(1, MAX_RELOADS + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
            page.wait_for_timeout(CHALLENGE_WAIT_MS)
            html = page.content()
            if "Incapsula" not in html and len(html) > 5000:
                return html
            logger.info("  Challenge Incapsula activo en %s (intento %d/%d)",
                        url[:70], intento, MAX_RELOADS)
        except Exception as exc:
            logger.info("  Error de navegación %s: %s", url[:70], type(exc).__name__)
    logger.warning("  No se superó el challenge para %s", url[:80])
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _fecha_ddmmyyyy(texto: str) -> date | None:
    if m := _RE_FECHA_DDMMYYYY.search(texto or ""):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _fecha_es(texto: str) -> date | None:
    if m := _RE_FECHA_ES.search(texto or ""):
        mes = MESES.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                return None
    return None


def parsear_listado(html: str, base_url: str = BASE) -> list[dict[str, Any]]:
    """Cards del listado (Abierto + Cerrado) -> dicts intermedios."""
    soup = BeautifulSoup(html, "html.parser")
    vacantes: list[dict[str, Any]] = []
    vistos: set[int] = set()

    for h5 in soup.select("h5.fifth-title"):
        card = h5.find_parent("div", class_="row")
        if card is None:
            continue
        texto = limpiar_texto(card.get_text(" ", strip=True))
        m = _RE_CONCURSO.search(texto)
        if not m:
            continue
        concurso = int(m.group(1))
        if concurso in vistos:
            continue
        vistos.add(concurso)

        link = card.find("a", href=re.compile(r"oportunidad-de-trabajo-\d+"))
        url = urljoin(base_url, link["href"]) if link else \
            f"{base_url}/oportunidades-de-trabajo-detalle/-/detalle/oportunidad-de-trabajo-{concurso}"

        estado = ""
        if me := re.search(r"Estado\s*:\s*([\wáéíóúñ ]{3,30})", texto, re.I):
            estado = limpiar_texto(me.group(1)).split(" Ver")[0]

        cierre = None
        if mc := re.search(r"Cierre de publicaci[oó]n\s*:\s*([\d-]{8,10})", texto, re.I):
            cierre = _fecha_ddmmyyyy(mc.group(1))
        if cierre is None and (mh := re.search(r"hasta el[^,]{0,40}", texto, re.I)):
            cierre = _fecha_es(mh.group(0))

        nivel_estr = None
        if mn := _RE_NIVEL.search(texto):
            nivel_estr = mn.group(1)

        vacantes_n = None
        if mv := _RE_VACANTES_TXT.search(texto):
            v = mv.group(1).lower()
            vacantes_n = _NUM_PALABRA.get(v) or (int(v) if v.isdigit() else None)

        tipo = None
        if re.search(r"plazo indefinido", texto, re.I):
            tipo = "Indefinido"
        elif re.search(r"plazo fijo", texto, re.I):
            tipo = "Plazo fijo"

        # párrafo del llamado (descripción)
        descripcion = ""
        for p in card.find_all("p"):
            t = limpiar_texto(p.get_text(" ", strip=True))
            if t.lower().startswith("se llama a postulaci"):
                descripcion = t
                break

        vacantes.append({
            "concurso": concurso,
            "cargo": limpiar_texto(h5.get_text(" ", strip=True))[:500],
            "url": url,
            "estado": estado,
            "abierto": "postulaci" in estado.lower(),
            "fecha_cierre": cierre,
            "nivel_estructura": nivel_estr,
            "vacantes": vacantes_n,
            "tipo": tipo,
            "descripcion": descripcion[:2500],
        })
    return vacantes


def parsear_detalle(html: str) -> dict[str, Any]:
    """Detalle: Gerencia, Tipo de contrato, Vacantes, Fecha de postulación..."""
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True))
    d: dict[str, Any] = {}

    def cap(label: str, patron_valor: str = r"([^:]{2,60}?)") -> str | None:
        m = re.search(rf"{label}\s*:\s*{patron_valor}\s*(?=[A-ZÁÉÍÓÚ][\wáéíóú]+\s*:|$)",
                      texto, re.I)
        return limpiar_texto(m.group(1)) if m else None

    if v := cap("Gerencia", r"([\wáéíóúñ, ]{4,70}?)"):
        d["gerencia"] = v
    if v := cap("Tipo de contrato", r"([\wáéíóúñ ]{4,30}?)"):
        d["tipo"] = "Indefinido" if "indefinido" in v.lower() else (
            "Plazo fijo" if "fijo" in v.lower() else v)
    if m := re.search(r"Vacantes\s*:\s*(\d{1,3})", texto, re.I):
        d["vacantes"] = int(m.group(1))
    if m := re.search(r"Fecha de postulaci[oó]n\s*:\s*([\w, ]{6,30})", texto, re.I):
        # formato "Jun 5, 2024" (en inglés) o dd-mm-yyyy
        crudo = m.group(1).strip()
        f = _fecha_ddmmyyyy(crudo)
        if not f:
            try:
                f = datetime.strptime(crudo.replace(",", ""), "%b %d %Y").date()
            except ValueError:
                f = None
        if f:
            d["fecha_publicacion"] = f
    if m := re.search(r"Circular\s*:\s*(\d{3,5})", texto, re.I):
        d["circular"] = m.group(1)
    return d


def _nivel_columna(cargo: str, texto: str) -> str:
    t = f"{cargo} {texto}".lower()
    if any(w in t for w in ("jefe de grupo", "jefe de departamento", "gerente",
                            "subgerente", "director")):
        return "Directivo"
    if any(w in t for w in ("secretari", "administrativo", "operador", "técnico",
                            "tecnico", "asistente ejecutiva", "auxiliar")):
        return "Técnico"
    return "Profesional"


def construir_oferta(v: dict[str, Any], det: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = v["cargo"]

    desc_partes = [v["descripcion"]]
    if v.get("nivel_estructura"):
        desc_partes.append(f"Nivel {v['nivel_estructura']} de la estructura de cargos")
    vac = det.get("vacantes") or v.get("vacantes")
    if vac:
        desc_partes.append(f"Vacantes: {vac}")
    if det.get("gerencia"):
        desc_partes.append(f"Gerencia: {det['gerencia']}")
    descripcion = limpiar_texto(" | ".join(p for p in desc_partes if p))

    tipo = v.get("tipo") or det.get("tipo") or "Indefinido"
    if "definido" in tipo.lower() and "inde" not in tipo.lower():
        tipo = "Plazo fijo"  # el detalle usa "Plazo Definido"

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, str(v["concurso"])),
        "institucion_id": fuente_id,  # id del catalogo de instituciones
        "fuente_id": None,            # FK a fuentes: no aplica a scraper standalone
        "url_original": v["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": tipo,
        "nivel": _nivel_columna(cargo, v["descripcion"]),
        "region": FUENTE["region"],
        "ciudad": "Santiago",
        "renta_bruta_min": None,   # el BC no publica renta en estas páginas
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": det.get("fecha_publicacion") or date.today(),
        "fecha_cierre": v["fecha_cierre"],
        "requisitos_texto": None,  # requisitos detallados van en bases adjuntas
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float, con_detalle: bool,
               incluir_cerrados: bool) -> list[dict]:
    if sync_playwright is None:
        logger.error(
            "Playwright no está instalado y es OBLIGATORIO para bcentral.cl "
            "(Incapsula). Instalar: pip install playwright && "
            "python -m playwright install chromium")
        return []

    ofertas: list[dict] = []
    hoy = date.today()
    with sync_playwright() as pw:
        browser, ctx = _abrir_navegador(pw)
        page = ctx.new_page()
        try:
            html = _fetch(page, LISTADO_URL)
            if html is None:
                return []
            vacantes = parsear_listado(html, BASE)
            abiertas = [v for v in vacantes if v["abierto"]]
            logger.info("  Listado: %d vacantes (%d abiertas, %d cerradas)",
                        len(vacantes), len(abiertas), len(vacantes) - len(abiertas))

            seleccion = vacantes if incluir_cerrados else abiertas
            # vigencia adicional por fecha
            if not incluir_cerrados:
                seleccion = [v for v in seleccion
                             if not (v["fecha_cierre"] and v["fecha_cierre"] < hoy)]
            seleccion.sort(key=lambda v: -v["concurso"])
            if max_results:
                seleccion = seleccion[:max_results]

            for v in seleccion:
                det: dict[str, Any] = {}
                if con_detalle:
                    time.sleep(delay)
                    hd = _fetch(page, v["url"])
                    if hd:
                        det = parsear_detalle(hd)
                ofertas.append(construir_oferta(v, det))
        finally:
            browser.close()
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
    logger.info("INICIO - Scraper Banco Central%s", " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    fuente_id = int(FUENTE["id"])
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    urls_activas: list[str] = []
    ofertas: list[dict] = []

    try:
        ofertas = recolectar(max_results, delay, con_detalle, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [BCCH] {datos['cargo'][:55]:55} | {datos['tipo_cargo']:10} "
                      f"| cierre: {datos['fecha_cierre']}")
                if verbose:
                    print(f"      {datos['url_original'][:105]}")
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
            stats["cerradas"] = marcar_ofertas_cerradas(db, fuente_id, sorted(urls_activas))
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente Banco Central: %s", exc)
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
    logger.info("RESUMEN BCentral: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"],
                stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper oportunidades Banco Central (requiere Playwright)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de vacantes")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT,
                   help=f"Pausa entre páginas (default {DELAY_DEFAULT}s)")
    p.add_argument("--sin-detalle", action="store_true",
                   help="Solo cards del listado (1 carga de página)")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="Incluir también concursos cerrados/finalizados")
    p.add_argument("--export", default=None, metavar="PREFIJO",
                   help="Exportar además a PREFIJO.json y PREFIJO.csv")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, con_detalle=not a.sin_detalle,
             incluir_cerrados=a.incluir_cerrados, export=a.export)
