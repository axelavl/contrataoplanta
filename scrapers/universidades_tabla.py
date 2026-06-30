"""
ContrataOPlanta.cl — Scraper de concursos de universidades que publican como
TABLA o PDF de bases. Patrón estándar de sesión, config-driven.

MODOS (verificados 06/2026):
  tabla     Tabla HTML con una fila por concurso.
            · USACH honorarios  /procesos-seleccion-honorarios
              Columnas: Cargo | Bases | Fecha Inicio | Fecha Término | Postulación
              Cierre por fila (Fecha Término). Los códigos (USAnnnnn/USACH)
              coinciden con el canal Trabajando de USACH → SOLAPE (ver nota).

  pdf_nombre  Lista de PDFs de bases; el cargo/área está en el nombre del
            archivo ("CARGO 1 ECIADES.pdf"). Cierre global en la página.
            · USACH académicos  /concurso-publico-contratacion-academicas-...
              (hoy: cierre global 1 de mayo, ya vencido → 0 vigentes)

  joomla    Sitio Joomla con items de concurso (título + página de detalle
            con PDF de bases y plazo).
            · U. de Valparaíso  cyl.uv.cl/cargos

  transparencia  Portal de Transparencia Activa (tabla de decretos).
            · U. de Tarapacá  /transparencia/.../concursos/2026/
            Mismo formato que otros municipios/servicios; distingue llamado
            de resolución por la descripción del acto.

Sector = "Educación Superior". Renta: no se publica → null. SOLAPE USACH:
las ofertas de honorarios y académicas de USACH también pueden aparecer en
usach.trabajando.cl (cubierto por trabajando.py). Deduplicar en BD por
cargo normalizado + institución.

NOTA: todos los selectores se escribieron contra HTML descargado manualmente
en 06/2026. El entorno cloud no tiene egress a dominios .cl, por lo que
no se verificaron contra los sitios en vivo.

Uso:
    python scrapers/universidades_tabla.py --dry-run --verbose
    python scrapers/universidades_tabla.py --dry-run --solo usach_hon --export salida
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
from datetime import date, timedelta
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin

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

logger = logging.getLogger("scraper.universidades_tabla")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "universidades_tabla.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0
SECTOR = "Educación Superior"

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

_RE_LLAMADO = re.compile(r"(ll[áa]mas?e|ap?ru[ée]bans?e\s+las\s+bases|convoca|"
                         r"llamado a concurso)", re.I)
_RE_RESULTADO = re.compile(r"(selecci[óo]nase|n[óo]mbrase|adjud|desierto|"
                           r"retrotrae|resultado|design)", re.I)

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "url_bases", "descripcion", "requisitos_texto"]

FUENTES: list[dict[str, Any]] = [
    {"clave": "usach_hon", "id": 243,
     "nombre": "Universidad de Santiago de Chile",
     "sigla": "USACH", "region": "Metropolitana de Santiago",
     "ciudad": "Estación Central", "modo": "tabla", "tipo_default": "honorarios",
     "url": "https://www.usach.cl/procesos-seleccion-honorarios"},
    {"clave": "usach_aca", "id": 243,
     "nombre": "Universidad de Santiago de Chile",
     "sigla": "USACH", "region": "Metropolitana de Santiago",
     "ciudad": "Estación Central", "modo": "pdf_nombre", "tipo_default": "académico",
     "url": "https://www.usach.cl/concurso-publico-contratacion-academicas-y-academicos"},
    {"clave": "uv", "id": 244, "nombre": "Universidad de Valparaíso",
     "sigla": "UV", "region": "Valparaíso", "ciudad": "Valparaíso",
     "modo": "joomla", "url": "https://cyl.uv.cl/cargos", "host": "cyl.uv.cl"},
    {"clave": "uta", "id": 252, "nombre": "Universidad de Tarapacá",
     "sigla": "UTA", "region": "Arica y Parinacota", "ciudad": "Arica",
     "modo": "transparencia",
     "url": "https://www.uta.cl/transparencia/actosyresoluciones_d/concursos/2026/"},
]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:75])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo %s: %s", url[:70], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── Utilidades de parseo (puras) ─────────────────────────────────────────────
def _fecha_es(texto: str, anio_def: int | None = None) -> date | None:
    if m := re.search(r"(\d{1,2})\s+(?:de\s+)?(\w+)(?:\s+(?:de(?:l)?\s+)?(\d{4}))?",
                      texto or "", re.I):
        mes = MESES.get(m.group(2).lower())
        if mes:
            anio = int(m.group(3)) if m.group(3) else (anio_def or date.today().year)
            try:
                return date(anio, mes, int(m.group(1)))
            except ValueError:
                return None
    return None


def _fecha_dmy(texto: str) -> date | None:
    if m := re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", texto or ""):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("jefe", "jefa", "director", "decano", "vicerrector",
                            "coordinador", "jefatura")):
        return "Directivo"
    if any(w in c for w in ("académic", "academic", "docente", "profesor",
                            "profesional", "ingenier", "analista", "asistente de gestión")):
        return "Profesional"
    if any(w in c for w in ("técnic", "tecnic", "administrativ", "auxiliar",
                            "asistente", "secretari", "operador")):
        return "Técnico"
    return "Profesional"


def _tipo(texto: str, default="Concurso Universitario") -> str:
    t = (texto or "").lower()
    if "honorario" in t:
        return "Honorarios"
    if "contrata" in t:
        return "Contrata"
    if "planta" in t:
        return "Planta"
    if "académic" in t or "academic" in t:
        return "Académico"
    return default


# ── Modo tabla (USACH honorarios) ────────────────────────────────────────────
def parsear_tabla(html: str, fuente: dict) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for tabla in soup.find_all("table"):
        primera = tabla.find("tr")
        enc = [limpiar_texto(c.get_text()).lower()
               for c in (primera.find_all(["th", "td"]) if primera else [])]
        if not any("cargo" in e for e in enc):
            continue

        def idx(*claves):
            for i, e in enumerate(enc):
                if any(k in e for k in claves):
                    return i
            return None

        i_cargo = idx("cargo")
        i_inicio = idx("inicio")
        i_termino = idx("término", "termino", "cierre", "fin")
        i_post = idx("postulación", "postulacion")
        for fila in tabla.find_all("tr")[1:]:
            celdas = fila.find_all("td")
            if len(celdas) < 2:
                continue
            txt = [limpiar_texto(c.get_text(" ", strip=True)) for c in celdas]

            def cel(i):
                return txt[i] if i is not None and i < len(txt) else None

            cargo = cel(i_cargo)
            if not cargo or len(cargo) < 4:
                continue
            enlaces = [a["href"] for a in fila.find_all("a", href=True)]
            anio = date.today().year
            items.append({
                "cargo": cargo,
                "fecha_inicio": _fecha_es(cel(i_inicio) or "", anio),
                "fecha_cierre": _fecha_es(cel(i_termino) or "", anio),
                "url_bases": next((urljoin(fuente["url"], e) for e in enlaces
                                   if ".pdf" in e.lower()), None),
                "url_post": next((e for e in enlaces if "http" in e
                                  and ".pdf" not in e.lower()), None),
            })
        break
    return items


# ── Modo pdf_nombre (USACH académicos) ───────────────────────────────────────
def _cargo_desde_pdf(href: str) -> str:
    base = unquote(href.rsplit("/", 1)[-1])
    base = re.sub(r"_?\d*\.pdf.*$", "", base, flags=re.I)
    base = re.sub(r"^CARGO\s*\d+\s*", "", base, flags=re.I)
    base = re.sub(r"[_\-]+", " ", base)
    return limpiar_texto(base).title()


def parsear_pdf_nombre(html: str, fuente: dict) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True))
    cierre = None
    if m := re.search(r"(?:recepci[oó]n|postulaci[oó]n|plazo|hasta)[^.]{0,80}?"
                      r"hasta\s+(?:el\s+)?(\d{1,2}\s+de\s+\w+(?:\s+de\s+\d{4})?)",
                      texto, re.I):
        cierre = _fecha_es(m.group(1))
    items = []
    for a in soup.find_all("a", href=True):
        if ".pdf" not in a["href"].lower():
            continue
        area = _cargo_desde_pdf(a["href"])
        if not area or len(area) < 3:
            continue
        items.append({
            "cargo": f"Académico/a {area}",
            "fecha_cierre": cierre,
            "url_bases": urljoin(fuente["url"], a["href"]),
        })
    return items


# ── Modo joomla (UV) ─────────────────────────────────────────────────────────
def parsear_joomla(html: str, fuente: dict, session, con_detalle: bool) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    vistos = {}
    for a in soup.find_all("a", href=True):
        if "/cargos/" not in a["href"] or not re.search(r"/\d+", a["href"]):
            continue
        titulo = limpiar_texto(a.get_text(" ", strip=True))
        url = urljoin(fuente["url"], a["href"])
        if len(titulo) > 12 and url not in vistos:
            vistos[url] = titulo
    items = []
    for url, titulo in vistos.items():
        cierre, pdf, texto = None, None, ""
        if con_detalle:
            time.sleep(DELAY_DEFAULT)
            rd = _get(session, url)
            if rd is not None:
                main = BeautifulSoup(rd.text, "html.parser").select_one(
                    ".item-page, article, main, #content") or BeautifulSoup(
                    rd.text, "html.parser").body
                if main:
                    texto = limpiar_texto(main.get_text(" ", strip=True))
                    if m := re.search(r"hasta\s+(?:el\s+)?(?:lunes|martes|"
                                      r"mi[ée]rcoles|jueves|viernes|s[áa]bado|"
                                      r"domingo)?\s*(\d{1,2}\s+de\s+\w+"
                                      r"(?:\s+de\s+\d{4})?)", texto, re.I):
                        cierre = _fecha_es(m.group(1))
                    for aa in main.find_all("a", href=True):
                        if ".pdf" in aa["href"].lower():
                            pdf = urljoin(url, aa["href"].split("#")[0])
                            break
        cargo = re.sub(r"^(bases\s+(?:de\s+)?(?:llamado a\s+)?concurso\s+"
                       r"(?:p[úu]blico\s+)?(?:de\s+)?|convocatoria\s+(?:para\s+)?|"
                       r"oposici[óo]n\s+(?:de\s+)?antecedentes\s+(?:cargo\s+)?|"
                       r"llamado\s+a\s+concurso\s+(?:de\s+)?)", "", titulo, flags=re.I)
        items.append({"cargo": limpiar_texto(cargo) or titulo, "url": url,
                      "fecha_cierre": cierre, "url_bases": pdf, "texto": texto})
    return items


# ── Modo transparencia (UTA) ─────────────────────────────────────────────────
def parsear_transparencia(html_bytes: bytes, fuente: dict) -> list[dict]:
    try:
        html = html_bytes.decode("utf-8")
        if "Año" not in html and "Tipolog" not in html:
            raise UnicodeDecodeError("utf-8", b"", 0, 1, "")
    except (UnicodeDecodeError, AttributeError):
        html = html_bytes.decode("latin-1", errors="replace") \
            if isinstance(html_bytes, bytes) else html_bytes
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for tabla in soup.find_all("table"):
        primera = tabla.find("tr")
        enc = [limpiar_texto(c.get_text()).lower()
               for c in (primera.find_all(["th", "td"]) if primera else [])]
        if not any("denomina" in e or "acto" in e for e in enc):
            continue

        def idx(*claves):
            for i, e in enumerate(enc):
                if any(k in e for k in claves):
                    return i
            return None

        i_mes = idx("mes")
        i_denom = idx("denomina")
        i_num = idx("número", "numero")
        for fila in tabla.find_all("tr")[1:]:
            celdas = fila.find_all("td")
            if len(celdas) < 4:
                continue
            txt = [limpiar_texto(c.get_text(" ", strip=True)) for c in celdas]

            def cel(i):
                return txt[i] if i is not None and i < len(txt) else None

            denom = cel(i_denom) or ""
            if not denom or "sin concurso" in denom.lower():
                continue
            clase = ("resultado" if _RE_RESULTADO.search(denom)
                     else "llamado" if _RE_LLAMADO.search(denom) else "otro")
            link = fila.find("a", href=True)
            items.append({
                "cargo": denom, "mes": cel(i_mes), "numero": cel(i_num),
                "clase": clase,
                "url_bases": urljoin(fuente["url"], link["href"]) if link else None,
            })
        break
    return items


# ── Construcción ─────────────────────────────────────────────────────────────
def _construir(it: dict, fuente: dict) -> dict:
    fuente_id = int(fuente["id"])
    nombre = fuente["nombre"]
    cargo = limpiar_texto(it["cargo"])
    if cargo.isupper():
        cargo = cargo.title()
    cargo = cargo[:500]

    desc_partes = []
    if it.get("numero"):
        desc_partes.append(f"Decreto N° {it['numero']}")
    if it.get("texto"):
        desc_partes.append(it["texto"][:800])
    if it.get("url_bases"):
        desc_partes.append(f"Bases: {it['url_bases']}")
    if it.get("url_post"):
        desc_partes.append(f"Postulación: {it['url_post']}")

    url = it.get("url") or it.get("url_bases") or fuente["url"]
    tipo_base = fuente.get("tipo_default") or ""
    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo,
                                         it.get("numero") or it.get("url_bases")
                                         or cargo),
        "fuente_id": fuente_id,
        "url_original": url,
        "url_bases": it.get("url_bases"),
        "cargo": cargo,
        "descripcion": limpiar_texto(" | ".join(desc_partes))[:2000] or None,
        "institucion_nombre": nombre,
        "sector": SECTOR,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo(f"{cargo} {tipo_base}"),
        "nivel": _nivel(cargo),
        "region": normalizar_region(fuente["region"]) or fuente["region"],
        "ciudad": fuente.get("ciudad"),
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": it.get("fecha_inicio") or date.today(),
        "fecha_cierre": it.get("fecha_cierre"),
        "requisitos_texto": None,
    }


def _procesar(fuente: dict, session, con_detalle: bool, incluir_cerrados: bool,
              incluir_resultados: bool) -> list[dict]:
    logger.info("─── %s [%s] ───", fuente["nombre"], fuente["modo"])
    r = _get(session, fuente["url"])
    if r is None:
        return []

    if fuente["modo"] == "tabla":
        crudos = parsear_tabla(r.text, fuente)
    elif fuente["modo"] == "pdf_nombre":
        crudos = parsear_pdf_nombre(r.text, fuente)
    elif fuente["modo"] == "joomla":
        crudos = parsear_joomla(r.text, fuente, session, con_detalle)
    else:
        crudos = parsear_transparencia(r.content, fuente)
        if not incluir_resultados:
            crudos = [c for c in crudos if c.get("clase") != "resultado"]
        crudos = [c for c in crudos if c.get("clase") != "otro"]

    hoy = date.today()
    ofertas, omitidas, vistos = [], 0, set()
    for it in crudos:
        o = _construir(it, fuente)
        if o["id_externo"] in vistos or not o["cargo"]:
            continue
        vistos.add(o["id_externo"])
        if o["fecha_cierre"] and o["fecha_cierre"] < hoy and not incluir_cerrados:
            omitidas += 1
            continue
        ofertas.append(o)
    logger.info("  → %d vigentes (%d omitidas por plazo vencido)",
                len(ofertas), omitidas)
    return ofertas


# ── Persistencia / export ────────────────────────────────────────────────────
def _exportar(ofertas, prefijo):
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


def ejecutar(dry_run=False, verbose=False, max_results=None, solo=None,
             con_detalle=True, incluir_cerrados=False, incluir_resultados=False,
             export=None) -> dict:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper universidades tabla/PDF%s",
                " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto: forzando --dry-run")
        dry_run = True

    fuentes = [f for f in FUENTES if not solo or f["clave"] == solo]
    session = _session()
    agg = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0,
           "encontradas": 0}
    todas = []

    for fuente in fuentes:
        db = SessionLocal() if (SessionLocal and not dry_run) else None
        urls_activas = []
        f_stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
        try:
            ofertas = _procesar(fuente, session, con_detalle, incluir_cerrados,
                                incluir_resultados)
            if max_results:
                ofertas = ofertas[:max_results]
            agg["encontradas"] += len(ofertas)
            todas.extend(ofertas)
            for datos in ofertas:
                urls_activas.append(datos["url_original"])
                if verbose or dry_run:
                    print(f"  [{fuente['sigla']:7}] {datos['cargo'][:46]:46} | "
                          f"{datos['tipo_cargo'][:12]:12} | cierre: {datos['fecha_cierre']}")
                if dry_run or db is None:
                    continue
                try:
                    nueva, act = upsert_oferta(db, datos)
                    if nueva:
                        f_stats["nuevas"] += 1
                    elif act:
                        f_stats["actualizadas"] += 1
                except Exception as exc:
                    f_stats["errores"] += 1
                    db.rollback()
                    logger.exception("  Error upsert: %s", exc)
            if db is not None and urls_activas:
                f_stats["cerradas"] = marcar_ofertas_cerradas(
                    db, int(fuente["id"]), sorted(set(urls_activas)))
        except Exception as exc:
            f_stats["errores"] += 1
            if db is not None:
                db.rollback()
            logger.exception("  Error fuente %s: %s", fuente["clave"], exc)
        finally:
            if db is not None:
                try:
                    db.rollback()
                    registrar_log(db, int(fuente["id"]),
                                  "OK" if f_stats["errores"] == 0 else "PARCIAL",
                                  ofertas_nuevas=f_stats["nuevas"],
                                  ofertas_actualizadas=f_stats["actualizadas"],
                                  ofertas_cerradas=f_stats["cerradas"],
                                  paginas=0, duracion=time.time() - inicio)
                except Exception:
                    logger.exception("  No se pudo registrar log")
                db.close()
        for k in f_stats:
            agg[k] += f_stats[k]

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN universidades tabla: fuentes=%d encontradas=%d nuevas=%d "
                "err=%d (%.1fs)", len(fuentes), agg["encontradas"], agg["nuevas"],
                agg["errores"], dur)
    agg["duracion_seg"] = round(dur, 2)
    agg["status"] = "OK" if agg["errores"] == 0 else "PARCIAL"
    return agg


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper concursos universidades tabla/PDF de bases")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope por fuente")
    p.add_argument("--solo", default=None, choices=[f["clave"] for f in FUENTES])
    p.add_argument("--sin-detalle", action="store_true",
                   help="No abrir páginas de detalle (joomla): más rápido")
    p.add_argument("--incluir-cerrados", action="store_true")
    p.add_argument("--incluir-resultados", action="store_true",
                   help="Transparencia: incluir decretos de selección/resultado")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max, solo=a.solo,
             con_detalle=not a.sin_detalle, incluir_cerrados=a.incluir_cerrados,
             incluir_resultados=a.incluir_resultados, export=a.export)
