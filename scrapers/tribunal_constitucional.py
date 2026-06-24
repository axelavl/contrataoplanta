"""
EmpleoEstado.cl — Scraper de pasantías y concursos del Tribunal
Constitucional (www1.tribunalconstitucional.cl). Patrón estándar de sesión.

Sitio WordPress (Elementor). Las ofertas son un CUSTOM POST TYPE con API
REST pública (verificada 06/2026):
    GET /wp-json/wp/v2/pasantias-y-concurso?per_page=N&_fields=...
    cada item: id, title, date (publicación), modified, slug, link,
               categoria-pyc[]  (taxonomía: 194 pasantía, 195 concurso-publico,
                                  268 otros)
El contenido sustantivo (renta, modalidad, requisitos, plazo) NO viaja en
la API (acf vacío, content no expuesto) ni en un bloque de texto estable de
la página Elementor: vive en el PDF "Perfil de cargo / Bases" enlazado en
cada detalle. Ese PDF es DIGITAL y rico, así que el scraper lo descarga y
extrae: renta líquida, tipo de contrato ("plazo fijo 12 meses") y requisitos.

VIGENCIA: el sitio no publica fecha de cierre de forma estructurada (a veces
está en el PDF en redacciones variables; cuando se detecta se usa, si no
queda en null y la vigencia la da la presencia del item en el CPT —
marcar_ofertas_cerradas cierra lo que desaparece). Por eso NO hay filtro de
vencidas por fecha; --sin-pasantias permite excluir la categoría pasantía
si tu portal es solo de empleos.

Estándar de persistencia (igual que bcentral.py / codelco.py): la oferta se
ancla por `institucion_id` (FK al catálogo `instituciones`, id 139 del TC);
`fuente_id` queda en None. `marcar_ofertas_cerradas` cierra por
`WHERE institucion_id = :fid`, así que ese mismo id 139 es el que se le pasa.

Uso:
    python scrapers/tribunal_constitucional.py --dry-run --verbose
    python scrapers/tribunal_constitucional.py --dry-run --sin-detalle
    python scrapers/tribunal_constitucional.py --dry-run --export salida
"""

from __future__ import annotations

import argparse
import csv
import io
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

# El proyecto usa pdfplumber (arrastra pdfminer.six); no pypdf. Importación
# tolerante: si no está disponible, el enriquecimiento por PDF se omite y la
# oferta se construye igual con los datos de la API del CPT.
try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None  # type: ignore[assignment]

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

logger = logging.getLogger("scraper.tribunal_constitucional")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "tribunal_constitucional.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

BASE = "https://www1.tribunalconstitucional.cl"
API_CPT = BASE + "/wp-json/wp/v2/pasantias-y-concurso"
CAT_PASANTIA = 194
CAT_CONCURSO = 195

FUENTE = {
    "id": 139,  # id del Tribunal Constitucional en repositorio_instituciones_publicas_chile.json
    "nombre": "Tribunal Constitucional",
    "sigla": "TC",
    "sector": "Autonomía Constitucional / Justicia",
    "region": "Metropolitana de Santiago",  # sede única en Santiago
    "ciudad": "Santiago",
    "url_empleo": BASE + "/pasantias-y-concurso/",
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0
PER_PAGE = 50

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

CAMPOS_EXPORT = ["id_externo", "institucion_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "application/json,text/html,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:80])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo definitivo %s: %s", url[:80], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── PDF de perfil/bases (enriquecimiento) ────────────────────────────────────
def _pdf_a_texto(contenido: bytes) -> str:
    """Extrae texto del PDF con pdfplumber (estándar del proyecto)."""
    if pdfplumber is None or contenido[:5] != b"%PDF-":
        return ""
    try:
        with pdfplumber.open(io.BytesIO(contenido)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception:
        return ""


def parsear_perfil_pdf(texto: str) -> dict[str, Any]:
    """Renta, tipo de contrato, requisitos y (si está) cierre, del PDF."""
    t = limpiar_texto(texto)
    d: dict[str, Any] = {}
    if m := re.search(r"Renta\s+(?:l[íi]quida|bruta|mensual)?\s*\$?\s*([\d.]{5,12})",
                      t, re.I):
        try:
            monto = int(m.group(1).replace(".", ""))
            d["renta"] = monto
            es_bruta = "bruta" in t[max(0, m.start() - 5):m.start() + 12].lower()
            d["renta_es_bruta"] = es_bruta
            d["renta_texto"] = limpiar_texto(t[m.start():m.start() + 60])
        except ValueError:
            pass
    if m := re.search(r"(plazo fijo|contrata|honorarios|c[oó]digo del trabajo|planta)"
                      r"\s*(?:de\s*)?(\d+\s*meses)?", t, re.I):
        crudo = m.group(1).lower()
        d["tipo"] = ("Plazo fijo" if "plazo fijo" in crudo else
                     "Contrata" if "contrata" in crudo else
                     "Honorarios" if "honorario" in crudo else
                     "Código del Trabajo" if "código" in crudo or "codigo" in crudo
                     else "Planta")
    if m := re.search(r"Requisitos[^:]{0,25}:?\s*(.{40,1600})", t, re.I):
        bloque = m.group(1)
        corte = re.search(r"(Competencias|Funciones|Postulaci|Proceso de selec|"
                          r"Etapas|Documentos)", bloque, re.I)
        d["requisitos"] = limpiar_texto(
            bloque[: corte.start()] if corte else bloque)[:2000]
    # Fecha límite de postulación: es el plazo de ENVÍO de antecedentes
    # ("los antecedentes deben enviarse ... hasta el 19 de junio de 2026"),
    # NO la fecha de disponibilidad de las bases ni la de consultas. Se acota
    # el ámbito cortando en "Consultas:" para no capturar su fecha, y se usa
    # .{0,N}? (no [^.]) porque el correo de contacto trae puntos (tcchile.cl).
    corte_consultas = re.search(r"\bConsultas?\s*:", t, re.I)
    ambito = t[: corte_consultas.start()] if corte_consultas else t
    _FECHA = r"(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})"
    _HORA = (r"(?:las?\s+[\d:.]+\s*(?:horas?|hrs\.?)?\s*"
             r"(?:del?\s+(?:d[ií]a\s+)?)?)?")
    for patron in (
        rf"antecedentes\b.{{0,60}}?(?:enviad[oa]s?|enviarse|deben enviar|"
        rf"remit[ai]|ser[áa]n recibidos|recepci)\b.{{0,80}}?\bhasta\s+"
        rf"(?:el\s+)?{_HORA}{_FECHA}",
        rf"(?:enviad[oa]s?|enviarse|deben enviar)\b.{{0,80}}?\bhasta\s+"
        rf"(?:el\s+)?{_HORA}{_FECHA}",
        rf"recepci[oó]n de (?:postulaciones|antecedentes)\b.{{0,90}}?\bhasta\s+"
        rf"{_HORA}{_FECHA}",
        rf"plazo[^.]{{0,40}}?postulaci[oó]n\b.{{0,60}}?\bhasta\s+"
        rf"(?:el\s+)?{_HORA}{_FECHA}",
    ):
        if m := re.search(patron, ambito, re.I):
            s = m.group(1)
            if "/" in s:
                try:
                    d["fecha_cierre"] = datetime.strptime(s, "%d/%m/%Y").date()
                except ValueError:
                    pass
            elif mm := re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})",
                                 s, re.I):
                mes = MESES.get(mm.group(2).lower())
                if mes:
                    try:
                        d["fecha_cierre"] = date(int(mm.group(3)), mes,
                                                 int(mm.group(1)))
                    except ValueError:
                        pass
            if d.get("fecha_cierre"):
                break
    return d


def _pdf_bases_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        if ".pdf" in a["href"].lower():
            return a["href"]
    return None


# ── Parseo (puro, testeable sin red) ─────────────────────────────────────────
def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("director", "jefe", "jefa", "secretario", "secretaria",
                            "relator")):
        return "Directivo"
    if any(w in c for w in ("oficial", "analista", "encargado", "encargada",
                            "asistente", "administrativo", "técnico", "tecnico",
                            "auxiliar", "community manager")):
        return "Técnico"
    return "Profesional"


def _tipo_oferta(cats: list[int]) -> str:
    if CAT_PASANTIA in cats:
        return "Pasantía"
    return "Concurso público"


def construir_oferta(item: dict[str, Any], perfil: dict[str, Any],
                     bases_url: str | None) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = limpiar_texto(BeautifulSoup(
        item.get("title", {}).get("rendered", ""), "html.parser").get_text())[:500]
    cats = item.get("categoria-pyc") or []

    fecha_pub = None
    if item.get("date"):
        try:
            # Normaliza 'Z' → '+00:00' (consistencia con fiscalia.py; no
            # depende de Python ≥3.11).
            fecha_pub = datetime.fromisoformat(
                str(item["date"]).replace("Z", "+00:00")).date()
        except ValueError:
            pass

    es_pasantia = CAT_PASANTIA in cats
    tipo = "Pasantía" if es_pasantia else (perfil.get("tipo") or "Concurso público")

    # renta: solo a renta_bruta_min si el PDF dice explícitamente "bruta"
    renta_min = perfil["renta"] if perfil.get("renta_es_bruta") else None

    desc_partes = [f"Tipo de proceso: {_tipo_oferta(cats)}"]
    if perfil.get("tipo"):
        desc_partes.append(f"Contrato: {perfil['tipo']}")
    if perfil.get("renta_texto"):
        desc_partes.append(perfil["renta_texto"])
    if bases_url:
        desc_partes.append(f"Bases/Perfil: {bases_url}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo,
                                         str(item.get("id") or item.get("link"))),
        # Nuevo estándar (igual que bcentral.py / codelco.py): institucion_id
        # apunta al catálogo; fuente_id (FK a `fuentes`) no aplica.
        # marcar_ofertas_cerradas cierra por WHERE institucion_id = :fid cuando
        # un aviso desaparece del CPT.
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": item.get("link") or FUENTE["url_empleo"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] or None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": tipo,
        "nivel": "Profesional" if es_pasantia else _nivel(cargo),
        "region": FUENTE["region"],
        "ciudad": FUENTE["ciudad"],
        "renta_bruta_min": renta_min,
        "renta_bruta_max": None,
        "renta_texto": perfil.get("renta_texto"),
        "fecha_publicacion": fecha_pub or date.today(),
        "fecha_cierre": perfil.get("fecha_cierre"),
        "requisitos_texto": perfil.get("requisitos"),
    }


# ── Recolección ──────────────────────────────────────────────────────────────
def recolectar(max_results: int | None, delay: float, con_detalle: bool,
               sin_pasantias: bool, incluir_cerrados: bool) -> list[dict]:
    session = _session()
    crudas: list[dict] = []
    page = 1
    while True:
        url = (f"{API_CPT}?per_page={PER_PAGE}&page={page}"
               f"&_fields=id,date,modified,link,title,categoria-pyc")
        r = _get(session, url)
        if r is None:
            break
        try:
            lote = r.json()
        except ValueError:
            break
        if not isinstance(lote, list) or not lote:
            break
        crudas.extend(lote)
        total_pages = int(r.headers.get("X-WP-TotalPages", page))
        if page >= total_pages:
            break
        page += 1
        time.sleep(delay)
    logger.info("  CPT pasantias-y-concurso: %d items", len(crudas))

    ofertas: list[dict] = []
    hoy = date.today()
    omitidas = 0
    for item in crudas:
        cats = item.get("categoria-pyc") or []
        if sin_pasantias and CAT_PASANTIA in cats:
            continue
        perfil: dict[str, Any] = {}
        bases_url = None
        if con_detalle and item.get("link"):
            time.sleep(delay)
            rd = _get(session, item["link"])
            if rd is not None:
                bases_url = _pdf_bases_url(rd.text)
                if bases_url:
                    time.sleep(delay / 2)
                    rp = _get(session, bases_url)
                    if rp is not None:
                        perfil = parsear_perfil_pdf(_pdf_a_texto(rp.content))
        oferta = construir_oferta(item, perfil, bases_url)
        if (oferta["fecha_cierre"] and oferta["fecha_cierre"] < hoy
                and not incluir_cerrados):
            omitidas += 1
            continue
        ofertas.append(oferta)
        if max_results and len(ofertas) >= max_results:
            break
    logger.info("  → %d ofertas vigentes (%d omitidas por plazo vencido)",
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
             delay=DELAY_DEFAULT, con_detalle=True, sin_pasantias=False,
             incluir_cerrados=False, export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Tribunal Constitucional%s",
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
        ofertas = recolectar(max_results, delay, con_detalle, sin_pasantias,
                             incluir_cerrados)
        stats["encontradas"] = len(ofertas)
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                renta = (f"${datos['renta_bruta_min']:,}".replace(",", ".")
                         if datos["renta_bruta_min"] else "-")
                print(f"  [TC] {datos['cargo'][:46]:46} | {datos['tipo_cargo'][:16]:16} "
                      f"| {renta:>11} | cierre: {datos['fecha_cierre']}")
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
        if db is not None and urls_activas:
            stats["cerradas"] = marcar_ofertas_cerradas(db, fuente_id,
                                                        sorted(urls_activas))
    except Exception as exc:
        if db is not None:
            db.rollback()
        stats["errores"] += 1
        logger.exception("  Error fuente TC: %s", exc)
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
    logger.info("RESUMEN TC: encontradas=%d nuevas=%d act=%d cerradas=%d "
                "err=%d (%.1fs)", stats["encontradas"], stats["nuevas"],
                stats["actualizadas"], stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper pasantías y concursos del Tribunal Constitucional")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--sin-detalle", action="store_true",
                   help="Solo API (sin renta/requisitos del PDF de perfil)")
    p.add_argument("--sin-pasantias", action="store_true",
                   help="Excluir la categoría pasantía (solo concursos/empleos)")
    p.add_argument("--incluir-cerrados", action="store_true",
                   help="No filtrar procesos con plazo de postulación vencido")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, con_detalle=not a.sin_detalle,
             sin_pasantias=a.sin_pasantias,
             incluir_cerrados=a.incluir_cerrados, export=a.export)
