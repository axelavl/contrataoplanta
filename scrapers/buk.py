"""
EmpleoEstado.cl — Scraper genérico para portales Buk
({empresa}.buk.cl/trabaja-con-nosotros). Versión 2, patrón estándar de sesión.

Buk es Rails con render en servidor (sin JS necesario). Estructura verificada
en producción (06/2026):

  Listado /trabaja-con-nosotros (paginable ?page=N):
    div.jobs__card
      ├ .job__card-name > b           → título real del cargo
      ├ .job__card-name > p.d-none    → área + nombre corto (oculto; causa de
      │                                  los títulos duplicados de la v1)
      ├ .jobs__card-info              → "Ciudad, Región"
      ├ span "$ X - $ Y"              → rango de renta
      ├ span "Proceso iniciado el DD/MM/AAAA" → fecha de publicación real
      └ a[href] → detalle https://{empresa}.buk.cl/s/{hash}

  Detalle /s/{hash}: Jornada, Horas Semanales, Vacantes, Sueldo Máximo/Mínimo,
  Área, Ubicación y Descripción completa.

Selecciona automáticamente todas las fuentes del repositorio cuyo
`url_empleo` contenga "buk.cl" (más _EXTRA_SOURCES). Salida en las mismas
18 columnas estándar del resto de las fuentes de la sesión.

Uso:
    python scrapers/buk.py --dry-run --verbose
    python scrapers/buk.py --host sasipa --dry-run --export salida
    python scrapers/buk.py --sin-detalle          # solo cards del listado
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
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

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
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
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
        return re.sub(r"^[IVXLM]{1,6}\s+de\s+", "", (r or "").strip()) or None

    def marcar_ofertas_cerradas(*a: Any, **k: Any) -> int:  # type: ignore[misc]
        return 0

    def registrar_log(*a: Any, **k: Any) -> None:  # type: ignore[misc]
        return None

    def upsert_oferta(*a: Any, **k: Any) -> tuple[bool, bool]:  # type: ignore[misc]
        return False, False

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.buk")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "buk.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

REPO_PATH = Path(__file__).resolve().parents[1] / "repositorio_instituciones_publicas_chile.json"

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 0.8
MAX_PAGINAS = 10

# Selectores con fallback (Buk renombra clases ocasionalmente)
CARD_SELECTORS = (".jobs__card", "[class*='jobs__card']", ".job-card",
                  "[class*='JobCard']", "article.job", "li.job")
TITLE_SELECTORS = (".job__card-name b", ".job__card-name", "[class*='card-name'] b",
                   "[class*='card-title']", "h3", "h2")

_RE_RENTA_RANGO = re.compile(r"\$\s*([\d.]{5,12})(?:\s*[-–]\s*\$\s*([\d.]{5,12}))?")
_RE_FECHA_PROCESO = re.compile(r"Proceso iniciado el\s+(\d{1,2}/\d{1,2}/\d{4})", re.I)

_EXTRA_SOURCES: list[dict[str, Any]] = [
    {
        "id": 290,
        "nombre": "SASIPA (Sociedad Agrícola y Servicios Isla de Pascua)",
        "sigla": "SASIPA",
        "sector": "Empresa Pública",
        "region": "Valparaíso",
        "url_empleo": "https://sasipa.buk.cl/trabaja-con-nosotros",
    },
]

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── Fuentes ──────────────────────────────────────────────────────────────────
def _load_instituciones(path: Path = REPO_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    return data.get("instituciones") if isinstance(data, dict) else data


def _filter_buk(instituciones, solo_id=None, solo_host=None) -> list[dict[str, Any]]:
    out, vistos = [], set()
    for f in list(instituciones) + _EXTRA_SOURCES:
        url = (f.get("url_empleo") or "").lower()
        if "buk.cl" not in url or f.get("id") in vistos:
            continue
        vistos.add(f.get("id"))
        out.append(f)
    if solo_id is not None:
        out = [f for f in out if f.get("id") == solo_id]
    if solo_host:
        h = solo_host.lower().replace("https://", "").replace("http://", "").rstrip("/")
        out = [f for f in out if h in (f.get("url_empleo") or "").lower()]
    return out


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url)
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo definitivo %s: %s", url, type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


def _with_page(url: str, page: int) -> str:
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params["page"] = str(page)
    return urlunparse(parsed._replace(query=urlencode(params)))


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _monto(s: str | None) -> int | None:
    try:
        return int(s.replace(".", ""))
    except (ValueError, AttributeError):
        return None


def parsear_listado(html: str, base_url: str) -> list[dict[str, Any]]:
    """Cards del listado Buk -> dicts intermedios."""
    soup = BeautifulSoup(html, "html.parser")
    cards = []
    for sel in CARD_SELECTORS:
        cards = soup.select(sel)
        if cards:
            break

    ofertas = []
    for card in cards:
        titulo = ""
        for sel in TITLE_SELECTORS:
            el = card.select_one(sel)
            if el:
                titulo = limpiar_texto(el.get_text(" ", strip=True))
                if titulo:
                    break
        link = card.select_one("a[href]")
        if not titulo or not link:
            continue
        url = urljoin(base_url, link["href"])

        texto_card = limpiar_texto(card.get_text(" ", strip=True))
        info = card.select_one(".jobs__card-info")
        ubicacion = limpiar_texto(info.get_text(" ", strip=True)) if info else ""

        renta_min = renta_max = None
        renta_texto = None
        if m := _RE_RENTA_RANGO.search(texto_card):
            renta_min = _monto(m.group(1))
            renta_max = _monto(m.group(2)) if m.group(2) else None
            renta_texto = limpiar_texto(m.group(0))

        fecha_pub = None
        if m := _RE_FECHA_PROCESO.search(texto_card):
            try:
                fecha_pub = datetime.strptime(m.group(1), "%d/%m/%Y").date()
            except ValueError:
                pass

        ofertas.append({
            "titulo": titulo, "url": url, "ubicacion": ubicacion,
            "renta_min": renta_min, "renta_max": renta_max,
            "renta_texto": renta_texto, "fecha_publicacion": fecha_pub,
        })
    return ofertas


def parsear_detalle(html: str) -> dict[str, Any]:
    """Página /s/{hash} -> jornada, vacantes, sueldos, área, descripción."""
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True))
    d: dict[str, Any] = {}

    def cap(patron: str) -> str | None:
        m = re.search(patron, texto, re.I)
        return limpiar_texto(m.group(1)) if m else None

    if v := cap(r"Sueldo M[aá]ximo\s+\$?\s*([\d.]{5,12})"):
        d["renta_max"] = _monto(v)
    if v := cap(r"Sueldo M[ií]nimo\s+\$?\s*([\d.]{5,12})"):
        d["renta_min"] = _monto(v)
    if v := cap(r"Jornada\s+((?:Completa|Parcial|Art[ií]culo|\w+)(?:\s+\w+)?)"):
        d["jornada"] = v
    if v := cap(r"Vacantes\s+(\d{1,3})"):
        d["vacantes"] = v
    if v := cap(r"[ÁA]rea\s+([\w() .,-]{3,50}?)\s+(?:Ubicaci[oó]n|Sueldo|M[aá]s)"):
        d["area"] = v
    if v := cap(r"Ubicaci[oó]n\s+([\w() .,-]{3,80}?)\s+(?:M[aá]s informaci[oó]n|Descripci[oó]n|close)"):
        d["ubicacion"] = v
    if m := re.search(r"Descripci[oó]n\s+(.{50,2500})", texto, re.S | re.I):
        bloque = m.group(1)
        # recortar en el primer terminador si aparece dentro del bloque
        corte = re.search(r"\b(Requisitos|Postular|Comparte|Beneficios)\b", bloque)
        if corte:
            bloque = bloque[: corte.start()]
        d["descripcion"] = limpiar_texto(bloque)
    if m := re.search(r"Requisitos\s+(.{30,2000})", texto, re.S | re.I):
        bloque = m.group(1)
        corte = re.search(r"\b(Postular|Comparte|Beneficios|Descripci[oó]n)\b", bloque)
        if corte:
            bloque = bloque[: corte.start()]
        d["requisitos"] = limpiar_texto(bloque)
    return d


def _split_ubicacion(ubic: str) -> tuple[str | None, str | None]:
    """'Isla de Pascua, V de Valparaíso' -> (ciudad, región normalizada)."""
    partes = [p.strip() for p in (ubic or "").split(",") if p.strip()]
    if len(partes) >= 2:
        region = re.sub(r"^[IVXLM]{1,6}\s+de\s+", "", partes[-1], flags=re.I)
        return partes[0], normalizar_region(region)
    if partes:
        return None, normalizar_region(partes[0])
    return None, None


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("jefe", "jefa", "director", "gerente", "subgerente")):
        return "Directivo"
    if any(w in c for w in ("ayudante", "auxiliar", "operador", "operario",
                            "técnico", "tecnico", "administrativo", "conductor",
                            "secretari", "asistente")):
        return "Técnico"
    return "Profesional"


def construir_oferta(item: dict[str, Any], det: dict[str, Any],
                     fuente: dict[str, Any]) -> dict:
    fuente_id = int(fuente["id"])
    nombre = fuente.get("nombre") or ""
    cargo = item["titulo"][:500]
    ciudad, region = _split_ubicacion(det.get("ubicacion") or item["ubicacion"])
    renta_min = det.get("renta_min") or item["renta_min"]
    renta_max = det.get("renta_max") or item["renta_max"]

    desc_partes = []
    if det.get("descripcion"):
        desc_partes.append(det["descripcion"])
    for k, et in (("jornada", "Jornada"), ("vacantes", "Vacantes"), ("area", "Área")):
        if det.get(k):
            desc_partes.append(f"{et}: {det[k]}")
    descripcion = limpiar_texto(" | ".join(desc_partes)) or item["ubicacion"]

    texto_total = f"{descripcion} {det.get('requisitos', '')}".lower()
    tipo = "Código del Trabajo"  # Buk lo usan empresas; régimen laboral común
    if re.search(r"\bcontrata\b", texto_total):
        tipo = "Contrata"
    elif "honorario" in texto_total:
        tipo = "Honorarios"

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, item["url"]),
        "fuente_id": fuente_id,
        "url_original": item["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": fuente.get("sector") or None,
        "area_profesional": normalizar_area(cargo) or det.get("area"),
        "tipo_cargo": tipo,
        "nivel": _nivel(cargo),
        "region": region or fuente.get("region") or "Nacional",
        "ciudad": ciudad,
        "renta_bruta_min": renta_min,
        "renta_bruta_max": renta_max,
        "renta_texto": item.get("renta_texto"),
        "fecha_publicacion": item["fecha_publicacion"] or date.today(),
        "fecha_cierre": None,  # Buk no publica fecha de cierre
        "requisitos_texto": det.get("requisitos"),
    }


# ── Recolección por fuente ───────────────────────────────────────────────────
def _recolectar_fuente(fuente: dict[str, Any], max_results: int | None,
                       con_detalle: bool, delay: float) -> list[dict]:
    base = (fuente.get("url_empleo") or "").rstrip("/")
    if not base:
        return []
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })

    items: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for pagina in range(1, MAX_PAGINAS + 1):
        url = base if pagina == 1 else _with_page(base, pagina)
        r = _get(session, url)
        if r is None:
            break
        nuevos = 0
        for it in parsear_listado(r.text, base):
            if it["url"] in vistos:
                continue
            vistos.add(it["url"])
            items.append(it)
            nuevos += 1
        if nuevos == 0:
            break
        time.sleep(delay)
        if max_results and len(items) >= max_results:
            items = items[:max_results]
            break

    ofertas = []
    for it in items:
        det: dict[str, Any] = {}
        if con_detalle:
            time.sleep(delay)
            rd = _get(session, it["url"])
            if rd is not None:
                det = parsear_detalle(rd.text)
        ofertas.append(construir_oferta(it, det, fuente))
    return ofertas


def _procesar_fuente(fuente, dry_run, verbose, max_results, con_detalle, delay):
    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    nombre = fuente.get("nombre") or f"id={fuente.get('id')}"
    fuente_id = int(fuente["id"])
    inicio = time.time()
    logger.info("─── %s (id=%s) ───", nombre, fuente_id)
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    urls_activas: list[str] = []
    ofertas: list[dict] = []

    try:
        ofertas = _recolectar_fuente(fuente, max_results, con_detalle, delay)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [{(fuente.get('sigla') or 'BUK')[:6]}] "
                      f"{datos['cargo'][:55]:55} | {datos['region'] or '':20} "
                      f"| ${datos['renta_bruta_min'] or '-'}")
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
        logger.exception("  Error fuente %s: %s", nombre, exc)
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
                logger.exception("  No se pudo registrar log para %s", nombre)
            db.close()
    return stats, ofertas


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


def ejecutar(dry_run=False, verbose=False, max_results=None, solo_id=None,
             solo_host=None, con_detalle=True, delay=DELAY_DEFAULT,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper genérico Buk (v2%s)", ", standalone" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    fuentes = _filter_buk(_load_instituciones(), solo_id=solo_id, solo_host=solo_host)
    logger.info("  %d fuentes Buk seleccionadas", len(fuentes))

    agregado = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    todas: list[dict] = []
    for fuente in fuentes:
        stats, ofertas = _procesar_fuente(fuente, dry_run, verbose, max_results,
                                          con_detalle, delay)
        todas.extend(ofertas)
        for k, v in stats.items():
            agregado[k] = agregado.get(k, 0) + v

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN Buk: fuentes=%d encontradas=%d nuevas=%d act=%d err=%d (%.1fs)",
                len(fuentes), agregado["encontradas"], agregado["nuevas"],
                agregado["actualizadas"], agregado["errores"], dur)
    agregado["duracion_seg"] = round(dur, 2)
    agregado["status"] = "OK" if agregado["errores"] == 0 else "PARCIAL"
    return agregado


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(description="Scraper genérico portales Buk (*.buk.cl)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--id", type=int, default=None)
    p.add_argument("--host", default=None)
    p.add_argument("--sin-detalle", action="store_true")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             solo_id=a.id, solo_host=a.host, con_detalle=not a.sin_detalle,
             delay=a.delay, export=a.export)
