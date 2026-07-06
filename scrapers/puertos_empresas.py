"""
EmpleoEstado.cl — Scraper agrupado para portales institucionales SIMPLES de
empresas estatales (puertos y ENAER). Patrón estándar de sesión.

Agrupa fuentes cuyos sitios publican pocas ofertas en una página estática,
con tres MODOS de extracción según la estructura real (verificada 06/2026):

  pdf_links   La página lista cargos como links a PDF.
              · ENAER /bolsa-de-trabajo/: anchors "Cargo (CÓDIGO)" → PDF de
                aviso (escaneado, sin texto; el cargo sale del anchor).
              · EMPORMONTT /vacantes: anchors genéricos a /bases/*.pdf; el
                cargo se deriva del nombre de archivo y el PDF (digital)
                aporta requisitos.

  secciones   La página publica cada proceso como bloque de texto bajo un
              heading ("PROCESO DE SELECCIÓN - ...", "Llamado a Concurso").
              · EPAUSTRAL /procesos-de-seleccion/
              · PUERTO TALCAHUANO /llamado-a-concurso/ (vigencia por
                "Postulaciones hasta el D de mes del AAAA"; los llamados
                vencidos se omiten).

  lista_o_vacio  La página muestra "No existen postulaciones vigentes" o
              una lista de items.
              · EPI (Puerto Iquique) /postulaciones/

  bloqueada   Sitios tras WAF estatal que exige IP chilena (mismo caso
              Carabineros/pdi.cl/ejercito.cl):
              · EMPORCHA (chacabucoport.cl) — "upstream connect error" desde
                datacenter. Correr desde Chile; si el parseo no calza,
                capturar el HTML y ajustar el modo correspondiente.

NO incluidos aquí: SASIPA y PUERTO ANTOFAGASTA van por buk.py (plataforma
Buk); PUERTO SAN ANTONIO renderiza por JavaScript (SPA) y requiere análisis
con navegador — pendiente.

Vigencia: en pdf_links y lista la presencia en página es la señal (estos
sitios retiran lo cerrado); en secciones se filtra además por fecha de
cierre vencida. marcar_ofertas_cerradas cierra en BD lo que desaparece.

Uso:
    python scrapers/puertos_empresas.py --dry-run --verbose
    python scrapers/puertos_empresas.py --dry-run --solo enaer --export salida
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
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None  # type: ignore[assignment]

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

logger = logging.getLogger("scraper.puertos_empresas")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "puertos_empresas.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
DELAY_DEFAULT = 1.5

MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
         "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
         "octubre": 10, "noviembre": 11, "diciembre": 12}

# PDFs que no son ofertas (fichas, formularios, memorias, políticas)
_PDF_EXCLUIR = ("ficha", "formulario", "discapacidad", "memoria", "politica",
                "política", "manual", "reglamento", "instructivo", "iso-",
                "iso_", "iso 9001", "sgc", "alcance", "certificad",
                "presentacion-iso", "resultado")

# ── Configuración de fuentes (ajustar ids al repositorio) ────────────────────
FUENTES: list[dict[str, Any]] = [
    {
        "clave": "enaer", "id": 166,
        "nombre": "ENAER (Empresa Nacional de Aeronáutica)",
        "sigla": "ENAER", "sector": "FF.AA. y Orden",
        "region": "Metropolitana de Santiago", "ciudad": "El Bosque",
        "url": "https://www.enaer.cl/bolsa-de-trabajo/",
        "modo": "pdf_links",
        "pdf_path": "/pdf/btrabajo/",
        "pdf_con_texto": False,  # avisos escaneados (verificado)
    },
    {
        "clave": "empormontt", "id": 291,
        "nombre": "EMPORMONTT (Empresa Portuaria Puerto Montt)",
        "sigla": "EMPORMONTT", "sector": "Empresa del Estado",
        "region": "Los Lagos", "ciudad": "Puerto Montt",
        "url": "https://www.empormontt.cl/vacantes",
        "modo": "pdf_links",
        "pdf_path": "/bases/",
        "pdf_con_texto": True,  # descriptores digitales (verificado)
        "cargo_desde_archivo": True,  # los anchors son genéricos
    },
    {
        "clave": "epaustral", "id": 293,
        "nombre": "EPAUSTRAL (Empresa Portuaria Austral)",
        "sigla": "EPAUSTRAL", "sector": "Empresa del Estado",
        "region": "Magallanes", "ciudad": "Punta Arenas",
        "url": "https://www.epaustral.cl/procesos-de-seleccion/",
        "modo": "secciones",
        "titulo_re": r"PROCESO DE SELECCI[ÓO]N[ :\-–]+[^|]{3,90}",
    },
    {
        "clave": "talcahuano", "id": 290,
        "nombre": "Puertos de Talcahuano (Empresa Portuaria Talcahuano San Vicente)",
        "sigla": "EPTSV", "sector": "Empresa del Estado",
        "region": "Biobío", "ciudad": "Talcahuano",
        "url": "https://www.puertotalcahuano.cl/llamado-a-concurso/",
        "modo": "secciones",
        "titulo_re": r"(?:LLAMADO A CONCURSO|CONCURSO P[ÚU]BLICO|PROCESO DE SELECCI[ÓO]N)[ :\-–]*[^|]{0,90}",
    },
    {
        "clave": "epi", "id": 285,
        "nombre": "EPI (Empresa Portuaria Iquique)",
        "sigla": "EPI", "sector": "Empresa del Estado",
        "region": "Tarapacá", "ciudad": "Iquique",
        "url": "https://epi.cl/postulaciones/",
        "modo": "lista_o_vacio",
        "msg_vacio": "no existen postulaciones vigentes",
    },
    {
        "clave": "foji", "id": 708,
        "nombre": "FOJI (Fundación de Orquestas Juveniles e Infantiles de Chile)",
        "sigla": "FOJI", "sector": "Ejecutivo Central",
        "region": "Nacional", "ciudad": None,
        "url": "https://foji.cl/trabaja-con-nosotros/",
        "modo": "pdf_links",
        "pdf_path": "/wp-content/uploads/",
        "pdf_con_texto": True,
        "cargo_desde_archivo": True,  # anchors genéricos ("Revisa las Bases")
    },
    {
        "clave": "prodemu", "id": 709,
        "nombre": "Fundación PRODEMU",
        "sigla": "PRODEMU", "sector": "Ejecutivo Central",
        "region": "Nacional", "ciudad": None,
        "url": "https://www.prodemu.cl/trabaja-con-nosotras/",
        "modo": "lista_o_vacio",
        "msg_vacio": "no hay llamados a concurso",
    },
    {
        "clave": "chacabuco", "id": 292,
        "nombre": "EMPORCHA (Empresa Portuaria Chacabuco)",
        "sigla": "EMPORCHA", "sector": "Empresa del Estado",
        "region": "Aysén", "ciudad": "Puerto Chacabuco",
        "url": "https://chacabucoport.cl/index.php/trabaja-con-nosotros/",
        "modo": "bloqueada",
        "nota": "WAF estatal: requiere IP chilena (correr local; desde "
                "datacenter responde 'upstream connect error')",
    },
]

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
        "Accept": "text/html,application/pdf,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400 or b"upstream connect error" in r.content[:300]:
                logger.info("  HTTP %s en %s", r.status_code, url[:80])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo definitivo %s: %s", url[:80], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── Utilitarios de parseo (puros) ────────────────────────────────────────────
def _fecha_es(texto: str) -> date | None:
    if m := re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de(?:l)?\s+(\d{4})", texto or "", re.I):
        mes = MESES.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                return None
    return None


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("jefe", "jefa", "gerente", "subgerente", "director",
                            "supervisor")):
        return "Directivo"
    if any(w in c for w in ("administrativo", "asistente", "auxiliar", "operador",
                            "inspector", "técnico", "tecnico", "secretari")):
        return "Técnico"
    return "Profesional"


def _cargo_desde_filename(href: str) -> str:
    base = unquote(href.rsplit("/", 1)[-1])
    base = re.sub(r"\.pdf.*$", "", base, flags=re.I)
    base = re.sub(r"(?:_|\b)(?:actualizado|descriptor|cargo|GIT|bases?|concurso|"
                  r"[uú]ltima|versi[oó]n|v\d+|ene|feb|mar|abr|may|jun|jul|ago|"
                  r"sep|oct|nov|dic)(?:_|\b|\.)", " ", base, flags=re.I)
    base = re.sub(r"\b[A-Z]\.?\s*[A-Z]?\.?$", " ", base)  # iniciales finales
    base = re.sub(r"\b\d{1,2}\b", " ", base)
    base = re.sub(r"\d{6,8}", " ", base)          # fechas tipo 20250820
    base = re.sub(r"[_\-]+", " ", base)
    return limpiar_texto(base).title()


def pdf_a_texto(contenido: bytes) -> str:
    if PdfReader is None or contenido[:5] != b"%PDF-":
        return ""
    try:
        return "\n".join((p.extract_text() or "")
                         for p in PdfReader(io.BytesIO(contenido)).pages)
    except Exception:
        return ""


# ── Modos de extracción ──────────────────────────────────────────────────────
def extraer_pdf_links(html: str, fuente: dict, session: requests.Session,
                      delay: float) -> list[dict]:
    """Cargos publicados como links a PDF."""
    soup = BeautifulSoup(html, "html.parser")

    def es_generico(t: str) -> bool:
        return (len(t) < 12 or
                bool(re.match(r"(descarga|ver|bases|postula|pdf|aqu[ií])", t, re.I)))

    # 1ª pasada: el mejor anchor por URL (algunos cargos tienen un anchor
    # ícono/botón genérico ANTES del anchor con el título real)
    mejor_texto: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" not in href.lower() or fuente["pdf_path"] not in href:
            continue
        url_pdf = urljoin(fuente["url"], href)
        dominio_fuente = urlparse(fuente["url"]).netloc.replace("www.", "")
        if dominio_fuente not in urlparse(url_pdf).netloc:
            continue  # PDFs de otros dominios (certificadoras, etc.) no son ofertas
        t = limpiar_texto(a.get_text(" ", strip=True))
        actual = mejor_texto.get(url_pdf, "")
        if (not es_generico(t) and len(t) > len(actual)) or url_pdf not in mejor_texto:
            mejor_texto[url_pdf] = t if not es_generico(t) else actual

    items: list[dict] = []
    for url_pdf, texto_anchor in mejor_texto.items():
        low = unquote(url_pdf).lower()
        if any(x in low for x in _PDF_EXCLUIR):
            continue

        if fuente.get("cargo_desde_archivo") or es_generico(texto_anchor):
            cargo = _cargo_desde_filename(url_pdf)
        else:
            cargo = texto_anchor
        # quitar prefijos administrativos del aviso
        cargo = re.sub(r"^(aviso\s+)?(concurso\s+(?:abierto|p[úu]blico)\s+|"
                       r"descriptor\s+|llamado\s+a?\s*)", "", cargo, flags=re.I)
        cargo = limpiar_texto(re.sub(r"\s+\d$", "", cargo)).strip(" .-,")
        cargo = limpiar_texto(re.sub(r"\s*\.\s*\.?\s*$", "", cargo))
        if not cargo or len(cargo) < 4:
            continue

        codigo = None
        if m := re.search(r"\(([A-Z]{1,4}\.?[\d/.\-]{4,15})\)", texto_anchor):
            codigo = m.group(1)
            cargo = limpiar_texto(cargo.replace(f"({codigo})", ""))

        requisitos = None
        descripcion_pdf = None
        item_cierre = None
        item_tipo = None
        if fuente.get("pdf_con_texto"):
            time.sleep(delay / 2)
            rp = _get(session, url_pdf)
            if rp is not None:
                t = limpiar_texto(pdf_a_texto(rp.content))
                if t:
                    descripcion_pdf = t[:1200]
                    # cierre desde el PDF (fecha larga con día opcional o dd/mm/aaaa)
                    if m := re.search(
                            r"hasta el\s+(?:lunes|martes|mi[eé]rcoles|jueves|"
                            r"viernes|s[aá]bado|domingo)?\s*,?\s*"
                            r"(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4})", t, re.I):
                        item_cierre = _fecha_es(m.group(1))
                    elif m := re.search(r"hasta el\s+(\d{1,2})/(\d{1,2})/(\d{4})", t, re.I):
                        try:
                            item_cierre = date(int(m.group(3)), int(m.group(2)),
                                               int(m.group(1)))
                        except ValueError:
                            item_cierre = None
                    tl = t.lower()
                    if "honorario" in tl:
                        item_tipo = "Honorarios"
                    elif re.search(r"\bcontrata\b", tl):
                        item_tipo = "Contrata"
                    if m := re.search(r"Requisitos[^:]{0,30}:?\s*(.{30,1500})", t, re.I):
                        bloque = m.group(1)
                        corte = re.search(r"(Funciones|Competencias|Postulaci)", bloque, re.I)
                        requisitos = limpiar_texto(
                            bloque[: corte.start()] if corte else bloque)[:2000]

        items.append({"cargo": cargo[:500], "url": url_pdf, "codigo": codigo,
                      "requisitos": requisitos, "descripcion_extra": descripcion_pdf,
                      "fecha_cierre": item_cierre, "tipo": item_tipo})
    return items


def extraer_secciones(html: str, fuente: dict) -> list[dict]:
    """Procesos publicados como bloques de texto bajo un heading."""
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup.body
    texto = limpiar_texto(main.get_text(" ", strip=True)) if main else ""
    items: list[dict] = []
    patron = re.compile(fuente["titulo_re"], re.I)

    posiciones = [(m.start(), limpiar_texto(m.group(0))) for m in patron.finditer(texto)]
    # descartar falsos positivos ANTES de segmentar, para que no recorten el
    # bloque del título real (típico: la frase en minúsculas dentro del cuerpo)
    posiciones = [(p, t) for p, t in posiciones if any(c.isupper() for c in t[:25])]
    for idx, (pos, titulo) in enumerate(posiciones):
        fin = posiciones[idx + 1][0] if idx + 1 < len(posiciones) else min(
            len(texto), pos + 2500)
        bloque = texto[pos:fin]
        cierre = None
        if m := re.search(r"hasta el\s+(\d{1,2})/(\d{1,2})/(\d{4})", bloque, re.I):
            try:
                cierre = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass
        if cierre is None and (m := re.search(
                r"hasta el\s+(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4})", bloque, re.I)):
            cierre = _fecha_es(m.group(1))
        correo = None
        if m := re.search(r"[\w.+-]+@[\w.-]+\.\w{2,}", bloque):
            correo = m.group(0)
        # cargo: lo que sigue al prefijo del título, cortado donde el heading
        # se funde con el cuerpo ("... Empresa Portuaria Austral, tiene...")
        cargo = re.sub(r"^(PROCESO DE SELECCI[ÓO]N|LLAMADO A CONCURSO|"
                       r"CONCURSO P[ÚU]BLICO)[ :\-–]*", "", titulo, flags=re.I)
        cargo = re.split(r"\s+(?:Empresa|La |El |tiene|invita|informa)\b",
                         cargo, maxsplit=1)[0]
        cargo = limpiar_texto(cargo) or limpiar_texto(titulo)
        items.append({"cargo": cargo[:500], "bloque": bloque[:2000],
                      "fecha_cierre": cierre, "correo": correo,
                      "url": fuente["url"] + "#" + re.sub(r"[^a-z0-9]+", "-",
                                                          cargo.lower())[:60]})
    return items


def extraer_lista_o_vacio(html: str, fuente: dict) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True)).lower()
    if fuente.get("msg_vacio") and fuente["msg_vacio"] in texto:
        return []
    # hay contenido: tomar links/headings de items dentro del main
    main = soup.select_one("main") or soup.body
    items: list[dict] = []
    vistos: set[str] = set()
    for a in (main.find_all("a", href=True) if main else []):
        t = limpiar_texto(a.get_text(" ", strip=True))
        h = a["href"]
        if len(t) < 12 or not any(k in (t + h).lower() for k in
                                  ("concurso", "cargo", "postula", "vacante",
                                   "proceso", ".pdf", "llamado")):
            continue
        url = urljoin(fuente["url"], h)
        if url in vistos or url.rstrip("/") == fuente["url"].rstrip("/"):
            continue
        vistos.add(url)
        items.append({"cargo": t[:500], "url": url})
    return items


# ── Construcción de oferta estándar ──────────────────────────────────────────
def construir_oferta(item: dict, fuente: dict) -> dict:
    fuente_id = int(fuente["id"])
    nombre = fuente["nombre"]
    cargo = item["cargo"]

    desc_partes = []
    if item.get("bloque"):
        desc_partes.append(item["bloque"][:1500])
    if item.get("descripcion_extra"):
        desc_partes.append(item["descripcion_extra"][:800])
    if item.get("codigo"):
        desc_partes.append(f"Código del concurso: {item['codigo']}")
    if item.get("correo"):
        desc_partes.append(f"Postulación al correo: {item['correo']}")
    if item["url"].lower().endswith(".pdf"):
        desc_partes.append(f"Aviso/Bases: {item['url']}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, item["url"]),
        "institucion_id": fuente_id,
        "fuente_id": None,
        "url_original": item["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 30 else None,
        "institucion_nombre": nombre,
        "sector": fuente["sector"],
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": item.get("tipo") or "Código del Trabajo",
        "nivel": _nivel(cargo),
        "region": normalizar_region(fuente["region"]) or fuente["region"],
        "ciudad": fuente.get("ciudad"),
        "renta_bruta_min": None,  # estas fuentes no publican renta en página
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": date.today(),  # sin fecha publicada: fecha de captura
        "fecha_cierre": item.get("fecha_cierre"),
        "requisitos_texto": item.get("requisitos"),
        "url_bases": item["url"] if item["url"].lower().endswith(".pdf") else item.get("url_bases"),
    }


# ── Proceso por fuente ───────────────────────────────────────────────────────
def _procesar_fuente(fuente: dict, session: requests.Session, delay: float,
                     incluir_cerrados: bool) -> list[dict]:
    logger.info("─── %s [%s] ───", fuente["nombre"][:50], fuente["modo"])
    if fuente["modo"] == "bloqueada":
        logger.warning("  OMITIDA: %s", fuente.get("nota", "fuente bloqueada"))
        return []
    r = _get(session, fuente["url"])
    if r is None:
        logger.warning("  Sin acceso a %s", fuente["url"])
        return []

    if fuente["modo"] == "pdf_links":
        items = extraer_pdf_links(r.text, fuente, session, delay)
    elif fuente["modo"] == "secciones":
        items = extraer_secciones(r.text, fuente)
    else:
        items = extraer_lista_o_vacio(r.text, fuente)

    hoy = date.today()
    ofertas = []
    omitidas = 0
    for it in items:
        o = construir_oferta(it, fuente)
        try:
            from scrapers.enrich import enriquecer_oferta
            # Minar el PDF de bases (url_bases) además del texto: requisitos,
            # renta, funciones, correo y fecha de cierre. Antes se llamaba sin
            # pdf_urls ni session, así que el PDF nunca se descargaba aquí.
            pdf = o.get("url_bases")
            pdf_urls = [pdf] if (pdf and str(pdf).lower().endswith(".pdf")) else None
            enriquecer_oferta(o, texto_html=o.get("descripcion"),
                              pdf_urls=pdf_urls, session=session)
        except Exception:
            pass
        if (o.get("fecha_cierre") and o["fecha_cierre"] < hoy
                and not incluir_cerrados):
            omitidas += 1
            continue
        ofertas.append(o)
    logger.info("  → %d ofertas vigentes (%d omitidas por plazo vencido)",
                len(ofertas), omitidas)
    return ofertas


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


def ejecutar(dry_run=False, verbose=False, max_results=None, solo=None,
             delay=DELAY_DEFAULT, incluir_cerrados=False,
             export=None) -> dict[str, Any]:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper agrupado puertos/empresas%s",
                " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    fuentes = [f for f in FUENTES if not solo or f["clave"] == solo]
    session = _session()
    agregado = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0,
                "encontradas": 0}
    todas: list[dict] = []

    for fuente in fuentes:
        db = SessionLocal() if (SessionLocal and not dry_run) else None
        urls_activas: list[str] = []
        f_stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
        try:
            ofertas = _procesar_fuente(fuente, session, delay, incluir_cerrados)
            if max_results:
                ofertas = ofertas[:max_results]
            agregado["encontradas"] += len(ofertas)
            todas.extend(ofertas)
            for datos in ofertas:
                urls_activas.append(datos["url_original"])
                if verbose or dry_run:
                    print(f"  [{fuente['sigla'][:10]:10}] {datos['cargo'][:48]:48} "
                          f"| {datos['region'][:14]:14} | cierre: {datos['fecha_cierre']}")
                if dry_run or db is None:
                    continue
                try:
                    nueva, actualizada = upsert_oferta(db, datos)
                    if nueva:
                        f_stats["nuevas"] += 1
                    elif actualizada:
                        f_stats["actualizadas"] += 1
                except Exception as exc:
                    f_stats["errores"] += 1
                    db.rollback()
                    logger.exception("  Error upsert: %s", exc)
            if db is not None and fuente["modo"] != "bloqueada":
                f_stats["cerradas"] = marcar_ofertas_cerradas(
                    db, int(fuente["id"]), sorted(urls_activas))
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
            agregado[k] = agregado.get(k, 0) + f_stats[k]
        time.sleep(delay)

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN puertos/empresas: fuentes=%d encontradas=%d nuevas=%d "
                "act=%d err=%d (%.1fs)", len(fuentes), agregado["encontradas"],
                agregado["nuevas"], agregado["actualizadas"],
                agregado["errores"], dur)
    agregado["duracion_seg"] = round(dur, 2)
    agregado["status"] = "OK" if agregado["errores"] == 0 else "PARCIAL"
    return agregado


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper agrupado: ENAER, EMPORMONTT, EPAUSTRAL, "
                    "Talcahuano, EPI (+EMPORCHA con IP chilena)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope por fuente")
    p.add_argument("--solo", default=None,
                   choices=[f["clave"] for f in FUENTES],
                   help="Procesar solo una fuente")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--incluir-cerrados", action="store_true")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max,
             solo=a.solo, delay=a.delay, incluir_cerrados=a.incluir_cerrados,
             export=a.export)
