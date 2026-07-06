"""
EmpleoEstado.cl — Scraper de empleos de Codelco
(https://empleos.codelco.cl/search/).

Portal SAP SuccessFactors (jobs2web). Extrae listado, detalle, requisitos,
formación requerida y condiciones ofrecidas.

Issue #241:
- Extrae formación/títulos desde el bloque "Requisitos de Postulación".
- Sólo usa formación reconocible como dato estructurado; lo dudoso queda en
  requisitos_texto y no se fuerza como área profesional.
- Mantiene compatibilidad con ejecutar(), dry-run y export.
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
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

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

BASE = "https://empleos.codelco.cl"
SEARCH_PATH = "/search/?q="
FUENTE = {
    "id": 275,
    "nombre": "CODELCO — Corporación Nacional del Cobre",
    "sigla": "CODELCO",
    "sector": "Empresa del Estado",
    "region": "Nacional",
    "url_empleo": BASE + "/search/",
}

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.0
PAGE_SIZE = 25
MAX_PAGINAS = 20

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}
MESES_ABREV = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
               "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10,
               "nov": 11, "dic": 12}

_RE_ID_PROCESO = re.compile(r"ID de proceso\s*:?\s*(\d{3,8})", re.I)
_RE_FECHA_TILE = re.compile(r"Fecha\s*:?\s*(\d{1,2})\s+(\w{3})\.?\s+(\d{4})", re.I)
_RE_REGION_TILE = re.compile(r"Regi[oó]n\s*:?\s*([\w.°' ]{3,30}?)\s*(?:C[oó]digo|$)", re.I)
_RE_CIERRE = re.compile(
    r"(?:Cierre de |t[ée]rmino de )?Postulaci[oó]n(?:es)?\s*:?\s*"
    r"(?:lunes|martes|mi[ée]rcoles|jueves|viernes|s[áa]bado|domingo)?\s*,?\s*"
    r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", re.I)
_RE_CIERRE_STRICT = re.compile(
    r"Cierre de Postulaci[oó]n(?:es)?\s*:?\s*"
    r"(?:lunes|martes|mi[ée]rcoles|jueves|viernes|s[áa]bado|domingo)?\s*,?\s*"
    r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", re.I)
_RE_VACANTES_DET = re.compile(r"N[úu]mero de vacantes\s*:?\s*(\d{1,3})", re.I)
# Los avisos usan indistintamente "Requisitos de Postulación" o "Requisitos del
# Cargo"; lo mismo para "Funciones Principales" / "Funciones del Cargo".
_RE_REQUISITOS = re.compile(
    r"Requisitos\s+(?:de Postulaci[oó]n|del Cargo)\s*:?\s*(.{30,2600})", re.I | re.S)
_RE_DESEABLES = re.compile(
    r"Aspectos Deseables\s*:?\s*(.*?)\s*(?:Condiciones Ofrecidas|Declaro en este acto|"
    r"Para asegurarnos|\*?Recuerda|NOTA\s*:|En Codelco estamos)", re.I | re.S)
_RE_DIVISION = re.compile(
    r"\b(Divisi[oó]n [A-ZÁÉÍÓÚ][\wáéíóúñ ]{2,30}?|Vicepresidencia[\wáéíóúñ ]{0,40}?|Casa Matriz)\b(?=[,.;]|\s[a-z¿])")
_RE_LUGAR = re.compile(r"Lugar de trabajo\s*:?\s*(.{2,60}?)\s*(?:\.|Jornada|N[úu]mero|Cierre|Hora)", re.I)
_RE_JORNADA = re.compile(r"Jornada laboral\s*:?\s*(.{2,60}?)\s*(?:\.|N[úu]mero|Cierre|Hora|Lugar)", re.I)
_RE_CONTRATO = re.compile(r"\bContrato\s*:?\s*(Indefinido|Plazo\s+Fijo|A\s+plazo\s+fijo|Reemplazo|Honorarios?|Faena)", re.I)
_RE_CARGO_CONTRACTUAL = re.compile(r"Cargo contractual\s*:?\s*(.{3,60}?)\s*(?:\.|Lugar|Jornada|N[úu]mero|Cierre|Hora)", re.I)
_RE_PROPOSITO = re.compile(
    r"Prop[óo]sito del Cargo\s*:?\s*(.*?)\s*(?:¡Porque|Funciones\s+(?:Principales|del Cargo)|"
    r"Requisitos\s+(?:de Postulaci[oó]n|del Cargo)|Condiciones Ofrecidas)", re.I | re.S)
_RE_FUNCIONES = re.compile(
    r"Funciones\s+(?:Principales|del Cargo)\s*:?\s*(.*?)\s*(?:Requisitos\s+(?:de Postulaci[oó]n|del Cargo)|"
    r"Aspectos Deseables|Condiciones Ofrecidas)", re.I | re.S)

# Formación requerida: sólo patrones de alta confianza. No se inventan títulos.
_RE_FORMACION_LINEAS = [
    re.compile(r"(?:t[íi]tulo|carrera|formaci[oó]n|profesi[oó]n)\s*(?:profesional|t[ée]cnica)?\s*(?:de|en|:)?\s+([^.;\n]{8,220})", re.I),
    re.compile(r"(?:ingenier[íi]a|ingeniero|ingeniera|t[ée]cnico|t[ée]cnica|contador|contadora|ge[oó]logo|ge[oó]loga|qu[íi]mico|qu[íi]mica|prevenci[oó]n de riesgos|psic[oó]logo|psic[oó]loga|abogado|abogada|arquitecto|arquitecta)[^.;\n]{0,180}", re.I),
]
_FORMACION_STOP = re.compile(r"\b(?:experiencia|conocimiento|licencia|salud compatible|disponibilidad|turno|residencia|manejo|office|sap|ingl[eé]s|deseable)\b", re.I)

CODELCO_LUGAR_REGION: dict[str, tuple[str | None, str]] = {
    "el salvador": ("El Salvador", "Atacama"), "potrerillos": ("Potrerillos", "Atacama"),
    "diego de almagro": ("Diego de Almagro", "Atacama"), "salvador": ("El Salvador", "Atacama"),
    "chuquicamata": ("Calama", "Antofagasta"), "calama": ("Calama", "Antofagasta"),
    "radomiro tomic": ("Calama", "Antofagasta"), "ministro hales": ("Calama", "Antofagasta"),
    "gabriela mistral": ("Calama", "Antofagasta"), "gaby": ("Calama", "Antofagasta"),
    "tocopilla": ("Tocopilla", "Antofagasta"), "antofagasta": (None, "Antofagasta"),
    "el teniente": ("Rancagua", "O'Higgins"), "rancagua": ("Rancagua", "O'Higgins"),
    "machalí": ("Machalí", "O'Higgins"), "machali": ("Machalí", "O'Higgins"),
    "sewell": ("Machalí", "O'Higgins"), "coya": ("Machalí", "O'Higgins"),
    "andina": ("Los Andes", "Valparaíso"), "los andes": ("Los Andes", "Valparaíso"),
    "saladillo": ("Los Andes", "Valparaíso"), "río blanco": ("Los Andes", "Valparaíso"),
    "rio blanco": ("Los Andes", "Valparaíso"), "ventanas": ("Puchuncaví", "Valparaíso"),
    "puchuncaví": ("Puchuncaví", "Valparaíso"), "puchuncavi": ("Puchuncaví", "Valparaíso"),
    "quintero": ("Quintero", "Valparaíso"), "valparaíso": (None, "Valparaíso"),
    "valparaiso": (None, "Valparaíso"), "casa matriz": ("Santiago", "Metropolitana de Santiago"),
    "santiago": ("Santiago", "Metropolitana de Santiago"),
}

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
        "Referer": BASE + "/",
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


def _region_desde_tile(crudo: str) -> str | None:
    s = limpiar_texto(crudo)
    s = re.sub(r"^\d{1,2}(?:ra|da|ta|va|na|ma)?\s*\.?\s*", "", s, flags=re.I)
    s = re.sub(r"^Reg(?:i[oó]n)?\.?\s*", "", s, flags=re.I).strip(" .")
    if not s:
        return None
    if "metropolitana" in s.lower():
        s = "Metropolitana de Santiago"
    return normalizar_region(s) or s


def _lugar_a_ciudad_region(lugar: str) -> tuple[str | None, str | None]:
    l = (lugar or "").lower()
    for clave, (ciudad, region) in CODELCO_LUGAR_REGION.items():
        if clave in l:
            return (ciudad or lugar.strip().title()), region
    return (lugar.strip().title() or None), None


def _parse_fecha_match(m: re.Match[str]) -> date | None:
    mes = MESES.get(m.group(2).lower())
    if not mes:
        return None
    try:
        return date(int(m.group(3)), mes, int(m.group(1)))
    except ValueError:
        return None


def _limpiar_bloque_requisitos(bloque: str) -> str:
    corte = re.search(
        r"(?:Aspectos Deseables|Condiciones Ofrecidas|Declaro en este acto|Postulan?do|"
        r"En Codelco|Diversidad e Inclusi[oó]n|Fecha de t[ée]rmino|Hora de cierre|Si quieres|¿Cómo postular)",
        bloque, re.I)
    if corte:
        bloque = bloque[: corte.start()]
    return limpiar_texto(bloque)[:2000]


def _formacion_requerida(requisitos: str) -> str | None:
    """Extrae formación sólo cuando hay señal explícita de título/carrera.

    Si el texto contiene sólo experiencia, licencias o conocimientos, retorna
    None para que no aparezca como dato estructurado dudoso.
    """
    texto = limpiar_texto(requisitos)
    if not texto:
        return None
    candidatos: list[str] = []
    for rx in _RE_FORMACION_LINEAS:
        for m in rx.finditer(texto):
            cand = limpiar_texto(m.group(1) if m.groups() else m.group(0))
            cand = re.split(r"\b(?:experiencia|conocimiento|licencia|deseable|excluyente)\b", cand, 1, flags=re.I)[0]
            cand = cand.strip(" .,:;-/")
            if len(cand) < 8 or len(cand) > 180:
                continue
            if _FORMACION_STOP.fullmatch(cand):
                continue
            if not re.search(r"ingenier|ingenier[oa]|t[ée]cnic|contador|ge[oó]log|qu[íi]mic|prevenci[oó]n|psic[oó]log|abogad|arquitect|administraci[oó]n|industrial|minas?|metalurg|mec[aá]nic|el[ée]ctric|inform[aá]tic|ambiental|civil", cand, re.I):
                continue
            if cand.lower() not in {c.lower() for c in candidatos}:
                candidatos.append(cand)
    if not candidatos:
        return None
    return "; ".join(candidatos[:4])[:300]


def _area_desde_formacion(cargo: str, formacion: str | None) -> str | None:
    texto = f"{cargo} {formacion or ''}".lower()
    reglas = [
        ("Prevención de Riesgos", r"prevenci[oó]n de riesgos|higiene y seguridad|seguridad laboral"),
        ("Minería", r"mina|minas|minero|minera|metalurg|ge[oó]log"),
        ("Ingeniería", r"ingenier|civil|industrial|mec[aá]nic|el[ée]ctric|electr[oó]nic|automatiz|mantenci[oó]n"),
        ("Tecnología", r"inform[aá]tic|computaci[oó]n|software|datos|ciber|ti\b|tic\b"),
        ("Administración", r"administraci[oó]n|comercial|contador|auditor|finanzas|control de gesti[oó]n"),
        ("Legal", r"abogad|jur[íi]dic|legal"),
        ("Personas", r"psic[oó]log|recursos humanos|rrhh|personas"),
        ("Medio Ambiente", r"ambiental|medio ambiente|sustentabilidad"),
    ]
    for area, patron in reglas:
        if re.search(patron, texto, re.I):
            return area
    return None


def parsear_listado(html: str, base_url: str = BASE) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    for tile in soup.select("li.job-tile"):
        data_url = tile.get("data-url") or ""
        a = tile.select_one(".tiletitle a[href], a.jobTitle-link[href]")
        href = data_url or (a["href"] if a else "")
        if not href:
            continue
        titulo = limpiar_texto(a.get_text(" ", strip=True)) if a else ""
        if not titulo:
            continue
        texto = limpiar_texto(tile.get_text(" ", strip=True))
        id_sf = None
        if m := re.search(r"/(\d{6,12})/?$", href):
            id_sf = m.group(1)
        id_proceso = None
        if m := _RE_ID_PROCESO.search(texto):
            id_proceso = m.group(1)
        fecha_pub = None
        if m := _RE_FECHA_TILE.search(texto):
            mes = MESES_ABREV.get(m.group(2).lower()[:3])
            if mes:
                try:
                    fecha_pub = date(int(m.group(3)), mes, int(m.group(1)))
                except ValueError:
                    pass
        region = None
        if m := _RE_REGION_TILE.search(texto):
            region = _region_desde_tile(m.group(1))
        items.append({
            "titulo": titulo[:500],
            "url": urljoin(base_url, href),
            "id_sf": id_sf,
            "id_proceso": id_proceso,
            "fecha_publicacion": fecha_pub,
            "region": region,
        })
    return items


def parsear_detalle(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    d: dict[str, Any] = {}
    desc_el = soup.select_one("span[itemprop=description], .jobdescription")
    if not desc_el:
        return d
    texto = limpiar_texto(desc_el.get_text(" ", strip=True))
    d["aviso"] = texto

    m = _RE_CIERRE_STRICT.search(texto) or _RE_CIERRE.search(texto)
    if m:
        cierre = _parse_fecha_match(m)
        if cierre and cierre >= date.today():
            d["fecha_cierre"] = cierre

    if m := _RE_CONTRATO.search(texto):
        sub = limpiar_texto(m.group(1)).title()
        d["contrato"] = sub
        d["tipo"] = f"Código del Trabajo ({sub})"[:50]

    if m := _RE_LUGAR.search(texto):
        lugar = limpiar_texto(m.group(1)).strip(" .:-")
        if lugar and len(lugar) <= 60:
            d["lugar"] = lugar
            ciudad, region = _lugar_a_ciudad_region(lugar)
            if ciudad:
                d["ciudad"] = ciudad
            if region:
                d["region"] = region

    if m := _RE_JORNADA.search(texto):
        jornada = limpiar_texto(m.group(1)).strip(" .:-")
        if jornada and len(jornada) <= 60:
            d["jornada"] = jornada

    if m := _RE_VACANTES_DET.search(texto):
        d["vacantes"] = int(m.group(1))
    if m := _RE_DIVISION.search(texto):
        d["division"] = limpiar_texto(m.group(1))
    if m := _RE_PROPOSITO.search(texto):
        prop = limpiar_texto(m.group(1))
        if len(prop) > 20:
            d["proposito"] = prop[:1200]
    if m := _RE_FUNCIONES.search(texto):
        d["funciones"] = limpiar_texto(m.group(1))[:1200]

    if m := _RE_CARGO_CONTRACTUAL.search(texto):
        cargo_contractual = limpiar_texto(m.group(1)).strip(" .:-")
        if cargo_contractual and len(cargo_contractual) <= 60:
            d["cargo_contractual"] = cargo_contractual

    if m := _RE_REQUISITOS.search(texto):
        requisitos = _limpiar_bloque_requisitos(m.group(1))
        formacion = _formacion_requerida(requisitos)
        d["requisitos"] = requisitos
        if formacion:
            d["formacion"] = formacion

    if m := _RE_DESEABLES.search(texto):
        deseables = limpiar_texto(m.group(1)).strip(" .:-")
        if deseables and len(deseables) > 10:
            d["deseables"] = deseables[:1200]
    return d


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("director", "gerente", "subgerente", "jefe", "jefa", "superintendente")):
        return "Directivo"
    if any(w in c for w in ("operador", "operadora", "mantenedor", "mecánico", "mecanico",
                            "eléctrico de mantenimiento", "soldador", "técnico", "tecnico",
                            "administrativo", "asistente", "rigger", "chofer", "conductor")):
        return "Técnico"
    return "Profesional"


def construir_oferta(item: dict[str, Any], det: dict[str, Any]) -> dict:
    fuente_id = int(FUENTE["id"])
    nombre = FUENTE["nombre"]
    cargo = item["titulo"]
    formacion = det.get("formacion")

    # La descripción se reserva para la narrativa (Propósito → Objetivo y
    # Funciones); jornada/vacantes/contrato/lugar viajan en sus columnas
    # estructuradas para no ensuciar la clasificación semántica del frontend.
    desc_partes = []
    if det.get("proposito"):
        desc_partes.append(f"Propósito del cargo: {det['proposito']}")
    if det.get("funciones"):
        desc_partes.append(f"Funciones principales: {det['funciones']}")
    if not det.get("proposito") and not det.get("funciones") and det.get("aviso"):
        aviso = det["aviso"]
        corte = re.search(r"(?:Diversidad e Inclusi[oó]n|En Codelco estamos llamados)", aviso, re.I)
        desc_partes.append(aviso[: corte.start()] if corte else aviso[:1200])
    descripcion = limpiar_texto("\n\n".join(p for p in desc_partes if p))

    requisitos = det.get("requisitos")
    if formacion and requisitos and "Formación requerida" not in requisitos:
        requisitos = f"Formación requerida: {formacion}. {requisitos}"
    if det.get("deseables"):
        deseables = f"Aspectos deseables: {det['deseables']}"
        requisitos = f"{requisitos}\n{deseables}" if requisitos else deseables

    area = _area_desde_formacion(cargo, formacion) or normalizar_area(cargo)
    id_estable = item.get("id_sf") or item.get("id_proceso") or item["url"]

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, str(id_estable)),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": item["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": FUENTE["sector"],
        "area_profesional": area,
        "tipo_cargo": det.get("tipo") or "Código del Trabajo",
        "nivel": _nivel(cargo),
        "region": det.get("region") or item.get("region") or FUENTE["region"],
        "ciudad": det.get("ciudad"),
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": item.get("fecha_publicacion") or date.today(),
        "fecha_cierre": det.get("fecha_cierre"),
        "requisitos_texto": requisitos,
        "url_bases": None,
        "jornada": det.get("jornada"),
        "numero_vacantes": det.get("vacantes"),
    }


def recolectar(max_results: int | None, delay: float, con_detalle: bool) -> list[dict]:
    session = _session()
    items: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for pagina in range(MAX_PAGINAS):
        startrow = pagina * PAGE_SIZE
        url = f"{BASE}{SEARCH_PATH}" + (f"&startrow={startrow}" if startrow else "")
        r = _get(session, url)
        if r is None:
            break
        nuevos = 0
        for it in parsear_listado(r.text, BASE):
            if it["url"] in vistos:
                continue
            vistos.add(it["url"])
            items.append(it)
            nuevos += 1
        logger.info("  startrow=%d: %d ofertas nuevas", startrow, nuevos)
        if nuevos == 0:
            break
        if max_results and len(items) >= max_results:
            items = items[:max_results]
            break
        time.sleep(delay)
    logger.info("  Listado: %d ofertas vigentes", len(items))

    ofertas: list[dict] = []
    for it in items:
        det: dict[str, Any] = {}
        if con_detalle:
            time.sleep(delay)
            rd = _get(session, it["url"])
            if rd is not None:
                det = parsear_detalle(rd.text)
        oferta = construir_oferta(it, det)
        try:
            from scrapers.enrich import enriquecer_oferta, encontrar_pdf_urls
            pdf_urls = encontrar_pdf_urls(rd.text, it["url"]) if (con_detalle and rd) else []
            enriquecer_oferta(
                oferta,
                texto_html=det.get("aviso"),
                pdf_urls=pdf_urls,
                session=session,
            )
        except Exception:
            logger.debug("  Enriquecimiento no disponible para %s", oferta["cargo"][:40])
        ofertas.append(oferta)
    return ofertas


def _exportar(ofertas: list[dict], prefijo: str) -> None:
    ser = []
    for o in ofertas:
        f = {k: o.get(k) for k in CAMPOS_EXPORT}
        for k in ("fecha_publicacion", "fecha_cierre"):
            if isinstance(f[k], date):
                f[k] = f[k].isoformat()
        ser.append(f)
    Path(prefijo + ".json").write_text(json.dumps(ser, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(prefijo + ".csv", "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS_EXPORT)
        w.writeheader()
        w.writerows(ser)
    logger.info("Exportado: %s.json / %s.csv (%d ofertas)", prefijo, prefijo, len(ser))


def ejecutar(dry_run=False, verbose=False, max_results=None, delay=DELAY_DEFAULT,
             con_detalle=True, export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper Codelco%s", " (standalone)" if STANDALONE else "")
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
        ofertas = recolectar(max_results, delay, con_detalle)
        stats["encontradas"] = len(ofertas)
        logger.info("  → %d ofertas", len(ofertas))
        for datos in ofertas:
            urls_activas.append(datos["url_original"])
            if verbose or dry_run:
                print(f"  [CODELCO] {datos['cargo'][:50]:50} | {(datos['region'] or '')[:22]:22} | {datos['nivel']:11} | cierre: {datos['fecha_cierre']}")
                if verbose:
                    print(f"      {datos['url_original'][:105]}")
                    if datos.get("area_profesional"):
                        print(f"      área: {datos['area_profesional']}")
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
        logger.exception("  Error fuente Codelco: %s", exc)
    finally:
        if db is not None:
            try:
                db.rollback()
                registrar_log(db, fuente_id, "OK" if stats["errores"] == 0 else "PARCIAL",
                              ofertas_nuevas=stats["nuevas"], ofertas_actualizadas=stats["actualizadas"],
                              ofertas_cerradas=stats["cerradas"], paginas=0, duracion=time.time() - inicio)
            except Exception:
                logger.exception("  No se pudo registrar log")
            db.close()

    if export and ofertas:
        _exportar(ofertas, export)
    dur = time.time() - inicio
    logger.info("RESUMEN Codelco: encontradas=%d nuevas=%d act=%d cerradas=%d err=%d (%.1fs)",
                stats["encontradas"], stats["nuevas"], stats["actualizadas"], stats["cerradas"], stats["errores"], dur)
    stats["duracion_seg"] = round(dur, 2)
    stats["status"] = "OK" if stats["errores"] == 0 else "PARCIAL"
    return stats


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(description="Scraper empleos Codelco (SuccessFactors)")
    p.add_argument("--dry-run", action="store_true", help="No guarda en BD")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope de ofertas")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT, help=f"Pausa entre requests (default {DELAY_DEFAULT}s)")
    p.add_argument("--sin-detalle", action="store_true", help="Solo listado (sin requisitos/cierre del aviso)")
    p.add_argument("--export", default=None, metavar="PREFIJO", help="Exportar además a PREFIJO.json y PREFIJO.csv")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             delay=a.delay, con_detalle=not a.sin_detalle, export=a.export)
