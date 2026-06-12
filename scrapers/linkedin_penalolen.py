"""
EmpleoEstado.cl — Extractor de ofertas desde posts PÚBLICOS de LinkedIn del
perfil "Reclutamiento Municipalidad de Peñalolén Oficial".
Patrón estándar de sesión, con una diferencia importante de alcance.

ALCANCE Y LIMITACIÓN (leer antes de operar):
LinkedIn sirve cada post individual de forma pública (render SEO con
JSON-LD: articleBody, datePublished, author), y eso es lo que este módulo
parsea. Pero el FEED del perfil — necesario para descubrir posts nuevos
automáticamente — está bloqueado sin sesión (HTTP 999 + authwall,
verificado), la página pública de un post no enlaza otros posts del autor,
y scrapear con sesión viola los Términos de Servicio de LinkedIn, arriesga
el bloqueo de la cuenta y es frágil. Por diseño, este módulo NO inicia
sesión.

En consecuencia el descubrimiento es por LISTA DE URLs que tú mantienes
(argumentos --url o archivo --file, por defecto linkedin_urls.txt junto al
script): pegas la URL de cada post nuevo del perfil y el módulo extrae,
normaliza al esquema estándar de 18 columnas y persiste. De cada post
obtiene: cargo (texto entre comillas), unidad (paréntesis), fecha real de
publicación, cierre ("postulaciones abiertas hasta el ..."), texto completo
y el link de postulación resolviendo el acortador lnkd.in al formulario
real.

Contexto: el portal Trabajando.cl de la municipalidad
(municipalidadpenalolen.trabajando.cl, idDominio 4731) existe pero está
vacío — LinkedIn es hoy su canal efectivo para estos llamados. Si algún día
lo activan, basta agregar la fuente a trabajando.py.

Uso:
    python scrapers/linkedin_penalolen.py --dry-run --url "https://www.linkedin.com/posts/..."
    python scrapers/linkedin_penalolen.py --dry-run --file mis_urls.txt --export salida
    # linkedin_urls.txt: una URL por línea, líneas con # se ignoran
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

logger = logging.getLogger("scraper.linkedin_penalolen")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "linkedin_penalolen.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

FUENTE = {
    "id": 401,  # Municipalidad de Peñalolén (id real del catálogo)
    "nombre": "Municipalidad de Peñalolén",
    "sigla": "PENALOLEN",
    "sector": "Municipalidad",
    "region": "Metropolitana de Santiago",
    "ciudad": "Peñalolén",
    "url_empleo": "https://www.linkedin.com/in/reclutamiento-municipalidad-de-pe%C3%B1alol%C3%A9n-oficial-780583357/",
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 2.0
URLS_DEFAULT = Path(__file__).resolve().parent / "linkedin_urls.txt"

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

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
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code == 999 or "authwall" in r.url:
                logger.warning("  LinkedIn bloqueó el acceso (HTTP %s / authwall) "
                               "en %s — el post puede no ser público o la IP "
                               "está limitada; reintentar más tarde",
                               r.status_code, url[:80])
                return None
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:80])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo definitivo %s: %s", url[:80], type(exc).__name__)
                return None
            time.sleep(3 * intento)
    return None


def _resolver_lnkd(session: requests.Session, url: str) -> str | None:
    """lnkd.in sirve una página intersticial; extraer la URL destino real."""
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    if "lnkd.in" not in r.url and "linkedin.com" not in r.url:
        return r.url  # redirigió directo
    # buscar el link externo en la intersticial
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "linkedin.com" not in href \
                and "lnkd.in" not in href:
            return href
    if m := re.search(r'"(https://(?:docs|forms)\.google\.com/[^"]+)"', r.text):
        return m.group(1)
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def parsear_post(html: str, url: str) -> dict[str, Any] | None:
    """Extrae la oferta del JSON-LD de un post público de LinkedIn."""
    m = re.search(r'<script type="application/ld\+json">(.*?)</script>', html, re.S)
    if not m:
        return None
    try:
        ld = json.loads(m.group(1))
    except ValueError:
        return None

    body = limpiar_texto(ld.get("articleBody") or "")
    if not body:
        og = re.search(r'<meta property="og:description" content="([^"]+)"', html)
        body = limpiar_texto(og.group(1)) if og else ""
    if not body:
        return None

    autor = limpiar_texto((ld.get("author") or {}).get("name") or "")

    fecha_pub = None
    if dp := ld.get("datePublished"):
        try:
            fecha_pub = datetime.fromisoformat(dp.replace("Z", "+00:00")).date()
        except ValueError:
            pass

    cargo = None
    if c := re.search(r'[“"\u201c]([^”"\u201d]{4,150})[”"\u201d]', body):
        cargo = limpiar_texto(c.group(1))
    unidad = None
    if u := re.search(r"\(([^)]{5,90})\)", body):
        unidad = limpiar_texto(u.group(1))

    fecha_cierre = None
    if fc := re.search(r"hasta el\s+(\d{1,2})(?:\s+de\s+(\w+))?(?:\s+de(?:l)?\s+(\d{4}))?",
                       body, re.I):
        dia = int(fc.group(1))
        mes = MESES.get((fc.group(2) or "").lower()) or (fecha_pub.month if fecha_pub else None)
        anio = int(fc.group(3)) if fc.group(3) else (fecha_pub.year if fecha_pub else None)
        if mes and anio:
            try:
                fecha_cierre = date(anio, mes, dia)
                # "hasta el 5" publicado el 28: el cierre cae al mes siguiente
                if fecha_pub and fecha_cierre < fecha_pub and not fc.group(2):
                    mes2 = mes + 1 if mes < 12 else 1
                    anio2 = anio if mes < 12 else anio + 1
                    fecha_cierre = date(anio2, mes2, dia)
            except ValueError:
                fecha_cierre = None

    form_url = None
    if f := re.search(r"https://lnkd\.in/\S+", body):
        form_url = f.group(0).rstrip(".,;)")

    return {"body": body, "autor": autor, "cargo": cargo, "unidad": unidad,
            "fecha_publicacion": fecha_pub, "fecha_cierre": fecha_cierre,
            "form_corto": form_url, "url": url.split("?")[0]}


def _tipo_desde_texto(texto: str) -> str:
    t = (texto or "").lower()
    if "honorario" in t:
        return "Honorarios"
    if re.search(r"\bcontrata\b", t):
        return "Contrata"
    if "código del trabajo" in t or "codigo del trabajo" in t:
        return "Código del Trabajo"
    if "planta" in t:
        return "Planta"
    return "No especificado"  # el post no declara régimen


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("director", "jefe", "jefa", "coordinador")):
        return "Directivo"
    if any(w in c for w in ("asistente", "auxiliar", "técnico", "tecnico",
                            "administrativo", "monitor", "conductor", "cuidados")):
        return "Técnico"
    return "Profesional"


def construir_oferta(p: dict[str, Any], form_real: str | None) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = (p.get("cargo") or limpiar_texto(p["body"])[:120])[:500]

    desc_partes = [p["body"][:1500]]
    if p.get("unidad"):
        desc_partes.append(f"Unidad: {p['unidad']}")
    if form_real:
        desc_partes.append(f"Formulario de postulación: {form_real}")
    elif p.get("form_corto"):
        desc_partes.append(f"Formulario de postulación: {p['form_corto']}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, p["url"]),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": p["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000],
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo_desde_texto(p["body"]),
        "nivel": _nivel(cargo),
        "region": FUENTE["region"],
        "ciudad": FUENTE["ciudad"],
        "renta_bruta_min": None,   # los posts no publican renta
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": p.get("fecha_publicacion") or date.today(),
        "fecha_cierre": p.get("fecha_cierre"),
        "requisitos_texto": None,  # el perfil del cargo va adjunto como imagen/PDF
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def _cargar_urls(urls_cli: list[str], archivo: str | None) -> list[str]:
    urls: list[str] = list(urls_cli or [])
    path = Path(archivo) if archivo else URLS_DEFAULT
    if path.exists():
        for linea in path.read_text(encoding="utf-8").splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#"):
                urls.append(linea)
    # dedup conservando orden
    vistos: set[str] = set()
    out = []
    for u in urls:
        k = u.split("?")[0]
        if k not in vistos:
            vistos.add(k)
            out.append(u)
    return out


def recolectar(urls: list[str], delay: float, max_results: int | None,
               incluir_cerrados: bool) -> list[dict]:
    session = _session()
    ofertas: list[dict] = []
    hoy = date.today()
    for url in urls[: max_results or None]:
        time.sleep(delay)
        r = _get(session, url)
        if r is None:
            continue
        p = parsear_post(r.text, url)
        if p is None:
            logger.warning("  Post sin JSON-LD parseable (¿no público?): %s", url[:80])
            continue
        form_real = None
        if p.get("form_corto"):
            time.sleep(delay / 2)
            form_real = _resolver_lnkd(session, p["form_corto"])
        o = construir_oferta(p, form_real)
        if (o["fecha_cierre"] and o["fecha_cierre"] < hoy and not incluir_cerrados):
            logger.info("  Omitida por plazo vencido: %s (cierre %s)",
                        o["cargo"][:45], o["fecha_cierre"])
            continue
        ofertas.append(o)
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


def ejecutar(urls: list[str] | None = None, archivo: str | None = None,
             dry_run=False, verbose=False, max_results=None,
             delay=DELAY_DEFAULT, incluir_cerrados=False,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Extractor LinkedIn Peñalolén%s",
                " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    todas_urls = _cargar_urls(urls or [], archivo)
    if not todas_urls:
        logger.error("Sin URLs de posts. Pásalas con --url o en %s", URLS_DEFAULT)
        return {"status": "SIN_URLS", "encontradas": 0}
    logger.info("  URLs de posts a procesar: %d", len(todas_urls))

    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0, "encontradas": 0}
    fuente_id = int(FUENTE["id"])
    db = SessionLocal() if (SessionLocal and not dry_run) else None
    urls_activas: list[str] = []
    ofertas: list[dict] = []

    try:
        ofertas = recolectar(todas_urls, delay, max_results, incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [PENALOLEN] {datos['cargo'][:48]:48} | "
                      f"{datos['tipo_cargo'][:14]:14} | pub: {datos['fecha_publicacion']} "
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
        # NOTA: no se llama marcar_ofertas_cerradas aquí — la lista de URLs
        # es curada manualmente y su ausencia no implica cierre del llamado.
        # El cierre se determina por fecha_cierre del propio post.
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente LinkedIn Peñalolén: %s", exc)
    finally:
        if db is not None:
            try:
                db.rollback()
                registrar_log(db, fuente_id,
                              "OK" if stats["errores"] == 0 else "PARCIAL",
                              ofertas_nuevas=stats["nuevas"],
                              ofertas_actualizadas=stats["actualizadas"],
                              ofertas_cerradas=0,
                              paginas=0, duracion=time.time() - inicio)
            except Exception:
                logger.exception("  No se pudo registrar log")
            db.close()

    if export and ofertas:
        _exportar(ofertas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN LinkedIn Peñalolén: encontradas=%d nuevas=%d act=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"],
                stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Extractor de ofertas desde posts públicos de LinkedIn "
                    "(Municipalidad de Peñalolén). El descubrimiento de posts "
                    "es manual: --url o archivo de URLs.")
    p.add_argument("--url", action="append", default=[],
                   help="URL de un post (repetible)")
    p.add_argument("--file", default=None,
                   help=f"Archivo con URLs, una por línea (default {URLS_DEFAULT.name})")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT,
                   help=f"Pausa entre requests (default {DELAY_DEFAULT}s; "
                        "no bajar: LinkedIn limita IPs agresivamente)")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="No omitir posts cuyo plazo ya venció")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(urls=a.url, archivo=a.file, dry_run=a.dry_run, verbose=a.verbose,
             max_results=a.max, delay=a.delay,
             incluir_cerrados=a.incluir_cerrados, export=a.export)
