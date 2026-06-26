"""
EmpleoEstado.cl — Scraper de convocatorias de Alta Dirección Pública
(adp.serviciocivil.cl, Servicio Civil). Patrón estándar de sesión.

FUENTE DE DATOS (verificada 06/2026): el buscador del sitio (OpenCms)
consume una API REST documentada en su propio JavaScript público:
    GET https://apis.serviciocivil.cl/adp/postulacion   (convocatorias vigentes)
    (también /adp/evaluacion y /adp/finalizadas)
con autenticación Basic cuya credencial de CONSULTA viene embebida en el
HTML que el sitio entrega a cualquier visitante (btoa("consulta:...")).
Este scraper extrae esa credencial EN TIEMPO DE EJECUCIÓN desde la página
de resultados —de modo que una rotación no lo rompe— y mantiene la última
credencial observada como respaldo si la página no responde.

Campos por convocatoria (JSON limpio): concurso (código "ADP-37261", id
estable), titulo (cargo), institucion, depende (ministerio), tipo (nivel
jerárquico I/II), region ("Región de La Araucanía"), municipio,
inicio y fin ("dd/mm/yyyy HH:MM" → publicación y cierre), tipocargo.

WAF: adp.serviciocivil.cl está tras AWS WAF con bloqueos 403 ERRÁTICOS por
ruta/momento (verificado: la misma URL alterna 200/403 entre intentos).
El scraper reintenta con pausas; además la API apis.serviciocivil.cl no
mostró ese comportamiento. Si todos los intentos de extraer la credencial
fallan, se usa la de respaldo.

url_original: no existe ficha pública individual descubrible; se usa el
deep-link del buscador del sitio filtrado por código de concurso (la ruta
canónica que ve un usuario).

Renta: no se publica en la convocatoria (las rentas ADP se fijan por
decreto/ADP por cargo), por lo que renta_* queda en null.

Uso:
    python scrapers/adp.py --dry-run --verbose
    python scrapers/adp.py --dry-run --export ofertas_adp
"""

from __future__ import annotations

import argparse
import base64
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

# ── Lectura de PDF de perfil (opcional, detección genérica) ─────────────────
# La API ADP no documenta una URL de perfil/bases en PDF, pero devuelve los
# registros crudos completos. Si en algún campo aparece una URL .pdf, se baja
# y se extraen requisitos/renta. Es no-op si no hay tal URL.
import io as _io

try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None  # type: ignore[assignment]

try:
    from extraction.requirements_extractor import extract_requirements
    from extraction.salary_extractor import extract_salary
except Exception:
    extract_requirements = None  # type: ignore[assignment]
    extract_salary = None  # type: ignore[assignment]


def _pdf_a_texto(contenido: bytes, max_paginas: int = 10) -> str:
    """Texto del PDF con pdfplumber. '' si no se puede (escaneado/cifrado)."""
    if pdfplumber is None or not contenido[:5].startswith(b"%PDF"):
        return ""
    try:
        with pdfplumber.open(_io.BytesIO(contenido)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages[:max_paginas])
    except Exception:
        return ""


def _datos_desde_pdf(texto: str) -> dict[str, Any]:
    """De un PDF de perfil/bases en texto saca requisitos_texto y renta."""
    out: dict[str, Any] = {}
    if not texto or len(texto) < 80:
        return out
    plano = limpiar_texto(texto)
    if extract_requirements is not None:
        try:
            req, des, docs = extract_requirements(texto)
            bloques = []
            if req:
                bloques.append("Requisitos: " + "; ".join(req))
            if des:
                bloques.append("Deseables: " + "; ".join(des))
            if docs:
                bloques.append("Documentos: " + "; ".join(docs[:6]))
            if bloques:
                out["requisitos_texto"] = limpiar_texto(" | ".join(bloques))[:1900]
        except Exception:
            logger.exception("  Error extrayendo requisitos del PDF")
    if extract_salary is not None:
        try:
            sal = extract_salary(plano)
            if sal.amount:
                out["renta_bruta_min"] = int(sal.amount)
                if sal.raw:
                    out["renta_texto"] = limpiar_texto(sal.raw)[:200]
        except Exception:
            logger.exception("  Error extrayendo renta del PDF")
    return out


def _url_pdf_en_registro(c: dict[str, Any]) -> str | None:
    """Busca recursivamente en el registro crudo de la API una URL .pdf
    (perfil/bases). Prioriza nombres con perfil/bases/descriptor."""
    urls: list[str] = []

    def _walk(v: Any) -> None:
        if isinstance(v, str):
            low = v.lower()
            if low.startswith("http") and ".pdf" in low:
                urls.append(v)
        elif isinstance(v, dict):
            for x in v.values():
                _walk(x)
        elif isinstance(v, (list, tuple)):
            for x in v:
                _walk(x)

    _walk(c)
    if not urls:
        return None
    hints = ("perfil", "bases", "descriptor", "convocatoria", "cargo")
    urls.sort(key=lambda u: next(
        (i for i, h in enumerate(hints) if h in u.lower()), len(hints)))
    return urls[0]


LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.adp")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "adp.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

SITIO = "https://adp.serviciocivil.cl"
PAGINA_CREDENCIAL = (SITIO + "/concursos-spl/opencms/resultados_busqueda.html"
                     "?filtroEstado=postulacion&action=buscar")
API_POSTULACION = "https://apis.serviciocivil.cl/adp/postulacion"
# Respaldo: credencial de consulta observada el 12/06/2026 en el JS público
# del sitio (puede rotar; el scraper intenta primero extraerla en runtime)
CREDENCIAL_RESPALDO = "consulta:dPu2yGXST3jWVbMz"

FUENTE = {
    # id 130 = "Dirección Nacional del Servicio Civil" (DNSC) en el catálogo.
    # OJO: 177 es Gobierno Regional de Coquimbo. DNSC también publica en
    # empleospublicos.cl; esta es una fuente ADICIONAL (convocatorias ADP en
    # adp.serviciocivil.cl). Mismo institucion_id, dominio distinto -> sin
    # cierre mutuo con las ofertas de empleospublicos.
    "id": 130,
    "nombre": "Servicio Civil - Alta Dirección Pública",
    "sigla": "ADP",
    "sector": "Ejecutivo Central",
    "region": "Nacional",
    "url_empleo": SITIO + "/",
}

HTTP_TIMEOUT = 25
MAX_RETRIES_WAF = 5   # el WAF de adp.* alterna 200/403 entre intentos
DELAY_DEFAULT = 1.5

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
        "Accept": "text/html,application/json,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    })
    return s


def obtener_credencial(session: requests.Session) -> str:
    """Extrae la credencial Basic del JS público del buscador (con
    reintentos pacientes contra el 403 errático del WAF)."""
    for intento in range(1, MAX_RETRIES_WAF + 1):
        try:
            r = session.get(PAGINA_CREDENCIAL, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                if m := re.search(r'btoa\("([^"]{5,80})"\)', r.text):
                    logger.info("  Credencial de consulta extraída del sitio")
                    return m.group(1)
                break  # página accesible pero sin credencial: usar respaldo
            logger.info("  Intento %d: HTTP %s del WAF en página de credencial",
                        intento, r.status_code)
        except requests.RequestException as exc:
            logger.info("  Intento %d: %s", intento, type(exc).__name__)
        time.sleep(2 * intento)
    logger.warning("  Usando credencial de respaldo (no se pudo extraer en "
                   "runtime; si la API responde 401, la credencial rotó y hay "
                   "que actualizar CREDENCIAL_RESPALDO desde el JS del sitio)")
    return CREDENCIAL_RESPALDO


def consultar_api(session: requests.Session, credencial: str) -> list[dict] | None:
    auth = base64.b64encode(credencial.encode()).decode()
    headers = {"Authorization": f"Basic {auth}",
               "Accept": "application/json",
               "Origin": SITIO, "Referer": SITIO + "/"}
    for intento in range(1, 4):
        try:
            r = session.get(API_POSTULACION, headers=headers, timeout=HTTP_TIMEOUT)
            if r.status_code == 401:
                logger.error("  API respondió 401: la credencial de consulta "
                             "rotó; actualizar CREDENCIAL_RESPALDO")
                return None
            if r.status_code >= 400:
                logger.info("  API HTTP %s (intento %d)", r.status_code, intento)
                time.sleep(2 * intento)
                continue
            d = r.json()
            return list(d.get("data") if isinstance(d, dict) else d or [])
        except (requests.RequestException, ValueError) as exc:
            logger.info("  API intento %d: %s", intento, type(exc).__name__)
            time.sleep(2 * intento)
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _fecha(s: str) -> date | None:
    """'15/06/2026 23:59' o '15/06/2026' -> date."""
    s = limpiar_texto(s).split(" ")[0]
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except (ValueError, TypeError):
        return None


def _region(cruda: str) -> str:
    r = limpiar_texto(cruda)
    r = re.sub(r"^Regi[oó]n\s+(?:de(?:l)?\s+)?", "", r, flags=re.I)
    if r.lower().startswith("metropolitana"):
        r = "Metropolitana de Santiago"
    if "higgins" in r.lower():
        r = "O'Higgins"
    if "magallanes" in r.lower():
        r = "Magallanes"
    if "aysén" in r.lower() or "aysen" in r.lower():
        r = "Aysén"
    return normalizar_region(r) or r or FUENTE["region"]


def construir_oferta(c: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    cargo = limpiar_texto(str(c.get("titulo") or ""))[:500]
    codigo = limpiar_texto(str(c.get("concurso") or ""))
    institucion = limpiar_texto(str(c.get("institucion") or "")) or FUENTE["nombre"]

    desc_partes = [f"Convocatoria {codigo}" if codigo else None,
                   f"Nivel jerárquico: {c['tipo']}" if c.get("tipo") else None,
                   f"Institución: {institucion}",
                   f"Dependencia: {limpiar_texto(str(c['depende']))}"
                   if c.get("depende") else None,
                   "Sistema de Alta Dirección Pública (Servicio Civil)"]
    descripcion = limpiar_texto(" | ".join(p for p in desc_partes if p))

    url = (f"{SITIO}/concursos-spl/opencms/resultados_busqueda.html"
           f"?filtroEstado=postulacion&texto={codigo}") if codigo else FUENTE["url_empleo"]

    return {
        "id_externo": generar_id_estable(fuente_id, FUENTE["nombre"], cargo,
                                         codigo or cargo),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": url,
        "cargo": cargo,
        "descripcion": descripcion[:2000],
        # la institución real del cargo (Servicio de Salud X, etc.) va en el
        # campo institucion_nombre para que el portal la muestre correctamente
        "institucion_nombre": institucion,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": "Alta Dirección Pública",
        "nivel": "Directivo",  # ADP por definición: I y II nivel jerárquico
        "region": _region(str(c.get("region") or "")),
        "ciudad": limpiar_texto(str(c.get("municipio") or "")) or None,
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": _fecha(str(c.get("inicio") or "")) or date.today(),
        "fecha_cierre": _fecha(str(c.get("fin") or "")),
        "requisitos_texto": None,  # el perfil del cargo se publica como PDF en la ficha
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, incluir_cerrados: bool) -> list[dict]:
    session = _session()
    credencial = obtener_credencial(session)
    crudas = consultar_api(session, credencial)
    if crudas is None and credencial != CREDENCIAL_RESPALDO:
        crudas = consultar_api(session, CREDENCIAL_RESPALDO)
    if crudas is None:
        logger.error("No se pudo consultar la API de convocatorias ADP")
        return []
    logger.info("  API /adp/postulacion: %d convocatorias", len(crudas))

    hoy = date.today()
    ofertas: list[dict] = []
    vistos: set[str] = set()
    omitidas = 0
    for c in crudas:
        o = construir_oferta(c)
        if o["id_externo"] in vistos or not o["cargo"]:
            continue
        vistos.add(o["id_externo"])
        if o["fecha_cierre"] and o["fecha_cierre"] < hoy and not incluir_cerrados:
            omitidas += 1
            continue
        # Si la API expone una URL de PDF (perfil/bases), extraer requisitos/renta.
        if not o.get("requisitos_texto"):
            pdf_url = _url_pdf_en_registro(c)
            if pdf_url:
                try:
                    rp = session.get(pdf_url, timeout=HTTP_TIMEOUT)
                    if rp.ok and rp.content[:5].startswith(b"%PDF"):
                        pdf_datos = _datos_desde_pdf(_pdf_a_texto(rp.content))
                        if pdf_datos.get("requisitos_texto"):
                            o["requisitos_texto"] = pdf_datos["requisitos_texto"]
                        if pdf_datos.get("renta_bruta_min") and not o.get("renta_bruta_min"):
                            o["renta_bruta_min"] = pdf_datos["renta_bruta_min"]
                            o["renta_texto"] = o.get("renta_texto") or pdf_datos.get("renta_texto")
                        if pdf_datos:
                            logger.info("  Perfil PDF %s: %s",
                                        o.get("id_externo"), ", ".join(sorted(pdf_datos)))
                except requests.RequestException as exc:
                    logger.info("  No se pudo descargar PDF ADP: %s", type(exc).__name__)
        try:
            from scrapers.enrich import enriquecer_oferta
            enriquecer_oferta(o, texto_html=o.get("descripcion"))
        except Exception:
            pass
        ofertas.append(o)
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
             incluir_cerrados=False, export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper ADP Servicio Civil%s",
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
        ofertas = recolectar(max_results, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [ADP] {datos['cargo'][:46]:46} | "
                      f"{datos['institucion_nombre'][:30]:30} | "
                      f"{(datos['region'] or '')[:18]:18} | cierre: {datos['fecha_cierre']}")
                if verbose:
                    print(f"      {datos['url_original'][:110]}")
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
        logger.exception("  Error fuente ADP: %s", exc)
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
    logger.info("RESUMEN ADP: encontradas=%d nuevas=%d act=%d cerradas=%d "
                "err=%d (%.1fs)", stats["encontradas"], stats["nuevas"],
                stats["actualizadas"], stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper convocatorias Alta Dirección Pública "
                    "(adp.serviciocivil.cl)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="No filtrar convocatorias con cierre vencido")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             incluir_cerrados=a.incluir_cerrados, export=a.export)
