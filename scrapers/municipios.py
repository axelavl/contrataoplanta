"""
EmpleoEstado.cl — Scraper agrupado para CONCURSOS Y EMPLEOS MUNICIPALES.
Patrón estándar de sesión, config-driven (mismo enfoque que
puertos_empresas.py): una sola lógica con modos de extracción y una lista de
fuentes, verificada sitio por sitio (06/2026).

MODOS:
  pdf_links   La página lista cargos como links a PDF de bases/perfil bajo
              un directorio propio (/wp-content/uploads/...). El cargo sale
              del anchor o del nombre de archivo; si el PDF es digital, se
              extrae cierre/renta/requisitos.
              · Peñalolén  /concurso-publicos/          (BASES CONCURSO 2026)
              · Los Ángeles /trabaja-con-nosotros/       (muchas bases)
              · San Miguel  /category/empleo/            (perfiles + bases)
              · Viña del Mar /ofertas-laborales/         (ofertas en PDF)

  secciones   Procesos como bloques de texto bajo un heading.
              · Puente Alto /trabaje-con-nosotros/       ("LLAMADO A CONCURSO")

  enlaces_detalle  Cada proceso es una página propia enlazada desde el
              listado; se entra a cada una.
              · Renca       /trabaja-con-nosotros-en-renca/  (/proceso-de-seleccion-*)
              · E-Central   /bolsa-de-empleo-2026/       (headings de cargo)

  lista_o_vacio  Página con "no hay..." o un listado simple.
              · Cerro Navia /concurso-publicos-y-ofertas-laborales/
              · Santa Bárbara /municipalidad/concursos-publicos/

  tabla_empleos  Cargos vigentes en una TABLA (una fila por cargo), con las
              bases enlazadas ("AQUÍ" → PDF) y el correo de postulación en
              OBSERVACIONES. Se descarta la tabla de seleccionados por cabecera.
              · Maipú /empleos-municipales/  (CARGO/OFICINA/RECEPCIÓN/BASES/OBS.)

  bloqueada / spa  No accesibles con requests puro:
              · Maipú /concursos-publicos (SPA Vue, sin API detectable) → navegador
              · San Ignacio, Melipilla, San Bernardo (503 WAF) → IP chilena
              · Recoleta (403 WAF) → IP chilena
              · Talagante (Wix, contenido por JS) → requiere navegador
              Quedan declaradas para correr con Playwright o desde Chile, y/o
              para capturar HTML y asignarles un modo en una iteración.

Vigencia: en pdf_links/secciones se filtra por fecha de cierre si se detecta;
si no, la presencia en la página es la señal (marcar_ofertas_cerradas cierra
lo que desaparece). Estas fuentes rara vez publican renta en la página (va
en las bases PDF). tipo_cargo por defecto del estamento municipal.

Uso:
    python scrapers/municipios.py --dry-run --verbose
    python scrapers/municipios.py --dry-run --solo penalolen --export salida
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

logger = logging.getLogger("scraper.municipios")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "municipios.log", encoding="utf-8")
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

RM = "Metropolitana de Santiago"
_PDF_EXCLUIR = ("cuenta-publica", "cuenta_publica", "cuenta-pública", "memoria",
                "manual", "reglamento", "ayudas-sociales", "ayudas_sociales",
                "presentacion", "presentación", "informe", "acta", "balance",
                "transparencia", "organigrama", "folleto", "instructivo",
                "plan-", "pladeco", "ordenanza", "decreto", "declaracion",
                "declaraciones", "modificatorio", "modificacion", "anexo",
                "formulario", "resultado", "acta-", "nomina", "calendario",
                "aclaracion", "respuesta", "errata", "prorroga", "fe-de-erratas")
# títulos de proceso (modo secciones)
_RE_TITULO_PROC = re.compile(
    r"(?:LLAMADO A CONCURSO|CONCURSO P[ÚU]BLICO|PROCESO DE SELECCI[ÓO]N|"
    r"CONVOCATORIA)(?:\s+(?:PARA|DE|P[ÚU]BLICO|MUNICIPAL))?[^.|]{0,70}", re.I)

# ── Configuración de fuentes (ids placeholder, ajustar al repositorio) ──────
FUENTES: list[dict[str, Any]] = [
    {"clave": "penalolen", "id": 401, "nombre": "Municipalidad de Peñalolén",
     "region": RM, "ciudad": "Peñalolén", "modo": "pdf_links",
     "url": "https://www.penalolen.cl/concurso-publicos/",
     "pdf_host": "penalolen.cl", "pdf_con_texto": True},
    {"clave": "losangeles", "id": 527, "nombre": "Municipalidad de Los Ángeles",
     "region": "Biobío", "ciudad": "Los Ángeles", "modo": "pdf_links",
     "url": "https://www.losangeles.cl/trabaja-con-nosotros/",
     "pdf_host": "losangeles.cl", "pdf_con_texto": True, "cargo_desde_archivo": True},
    {"clave": "sanmiguel", "id": 409, "nombre": "Municipalidad de San Miguel",
     "region": RM, "ciudad": "San Miguel", "modo": "pdf_links",
     "url": "https://web.sanmiguel.cl/category/empleo/",
     "pdf_host": "sanmiguel.cl", "pdf_con_texto": True},
    {"clave": "vina", "id": 345, "nombre": "Municipalidad de Viña del Mar",
     "region": "Valparaíso", "ciudad": "Viña del Mar", "modo": "pdf_links",
     "url": "https://www.munivina.cl/ofertas-laborales/",
     "pdf_host": "munivina.cl", "pdf_con_texto": True, "cargo_desde_archivo": True},
    # Segunda página de la misma muni (concursos de planta/contrata; la anterior
    # es "ofertas laborales"). Mismo CMS y host → mismo modo pdf_links. El cierre
    # por ausencia se agrupa por institución (345), así que ambas no se cierran
    # ofertas entre sí. institucion_nombre se deja canónico (sin sufijo) para el
    # display; la clave distingue la fuente en logs.
    {"clave": "vina_cp", "id": 345, "nombre": "Municipalidad de Viña del Mar",
     "region": "Valparaíso", "ciudad": "Viña del Mar", "modo": "concursos_da",
     "url": "https://www.munivina.cl/concursos-publicos/",
     "pdf_host": "munivina.cl"},
    # La Cisterna (388): el sitio es una SPA (mejormunicipio.com) → no scrapeable
    # con requests. El concurso de planta publica todas las vacantes en un PDF de
    # bases; se fija su URL (bases_pdf) y se descarga directo, separando una oferta
    # por estamento+grado. Al abrirse un concurso nuevo, actualizar bases_pdf.
    {"clave": "lacisterna", "id": 388, "nombre": "Municipalidad de La Cisterna",
     "region": RM, "ciudad": "La Cisterna", "modo": "bases_estamento_grado",
     "url": "https://cisterna.cl/news/municipalidad-de-la-cisterna-abre-concurso-publico-para-proveer-cargos-vacantes-en-la-planta-municipal",
     "bases_pdf": "https://uploads.mejormunicipio.com/websites/news/attachments/"
                  "e504a94e-5d11-4abc-8f54-26c86b3293a4/bases_llamado_a_concurso___adm_aux_jef_profdocx.pdf"},
    {"clave": "puentealto", "id": 419, "nombre": "Municipalidad de Puente Alto",
     "region": RM, "ciudad": "Puente Alto", "modo": "secciones",
     "url": "https://www.mpuentealto.cl/trabaje-con-nosotros/"},
    {"clave": "renca", "id": 407, "nombre": "Municipalidad de Renca",
     "region": RM, "ciudad": "Renca", "modo": "enlaces_detalle",
     "url": "https://renca.cl/trabaja-con-nosotros-en-renca/",
     "detalle_re": r"/proceso-de-seleccion[^/\"']*"},
    {"clave": "ecentral", "id": 385, "nombre": "Municipalidad de Estación Central",
     "region": RM, "ciudad": "Estación Central", "modo": "headings",
     "url": "https://www.ecentral.cl/bolsa-de-empleo-2026/"},
    {"clave": "cerronavia", "id": 382, "nombre": "Municipalidad de Cerro Navia",
     "region": RM, "ciudad": "Cerro Navia", "modo": "enlaces_detalle",
     "url": "https://www.cerronavia.cl/concurso-publicos-y-ofertas-laborales/",
     "detalle_re": r"/(?:oferta-laboral|oferta-de-trabajo|concurso-publico-para|"
                   r"proceso-de-seleccion)[^/\"']*",
     "pdf_host": "cerronavia.cl"},
    {"clave": "independencia", "id": 387, "nombre": "Municipalidad de Independencia",
     "region": RM, "ciudad": "Independencia", "modo": "enlaces_detalle",
     "url": "https://www.independencia.cl/concursos-publicos/",
     "detalle_re": r"/concurso-publico[^/\"']*",
     "pdf_host": "independencia.cl", "pdf_con_texto": True},
    {"clave": "santabarbara", "id": 537, "nombre": "Municipalidad de Santa Bárbara",
     "region": "Biobío", "ciudad": "Santa Bárbara", "modo": "lista_o_vacio",
     "url": "https://www.santabarbara.cl/municipalidad/concursos-publicos/",
     "pdf_host": "santabarbara.cl"},
    {"clave": "elbosque", "id": 384, "nombre": "Municipalidad de El Bosque",
     "region": RM, "ciudad": "El Bosque", "modo": "enlaces_detalle",
     "url": "https://www.municipalidadelbosque.cl/concursospublicos/",
     "detalle_re": r"(drive\.google|/concurso|\.pdf)"},
    # ── Lote 2 (sesión de ampliación): corporaciones y municipios ──────────
    {"clave": "cmvalpo", "id": 647, "nombre": "Corporación Municipal de Valparaíso (Salud)",
     "region": "Valparaíso", "ciudad": "Valparaíso", "modo": "enlaces_detalle",
     "url": "https://cmvalparaiso.cl/trabaja-con-nosotros/",
     "detalle_re": r"/oferta-(?:laboral|trabajo)[^/\"']*"},
    {"clave": "villalemana", "id": 363, "nombre": "Municipalidad de Villa Alemana",
     "region": "Valparaíso", "ciudad": "Villa Alemana", "modo": "enlaces_detalle",
     "url": "https://munivillalemana.gob.cl/ofertas-de-empleos/",
     "detalle_re": r"/ofertas-de-empleos/(?!$)[^/\"']+/?$"},
    {"clave": "valdivia", "id": 580, "nombre": "Municipalidad de Valdivia",
     "region": "Los Ríos", "ciudad": "Valdivia", "modo": "pdf_links",
     "url": "https://munivaldivia.cl/trabaja-con-nosotros/",
     "pdf_host": "munivaldivia.cl", "pdf_con_texto": True, "cargo_desde_archivo": True},
    {"clave": "coresam", "id": 670, "nombre": "CORESAM Conchalí (Corp. Educación y Salud)",
     "region": "Metropolitana de Santiago", "ciudad": "Conchalí", "modo": "pdf_links",
     "url": "https://coresam.cl/oferta-laboral/",
     "pdf_host": "coresam.cl", "pdf_con_texto": True, "cargo_desde_archivo": True},
    {"clave": "sanfernando", "id": 456, "nombre": "Municipalidad de San Fernando",
     "region": "O'Higgins", "ciudad": "San Fernando", "modo": "lista_o_vacio",
     "url": "https://municipalidadsanfernando.cl/trabaja-con-nosotros/",
     "pdf_host": "municipalidadsanfernando.cl"},
    {"clave": "saludpm", "id": 676, "nombre": "Salud Puente Alto (Corp. Municipal)",
     "region": "Metropolitana de Santiago", "ciudad": "Puente Alto", "modo": "headings",
     "url": "https://saludpm.cl/contactanos/trabaja-con-nosotros/"},
    {"clave": "buin", "id": 416, "nombre": "Municipalidad de Buin",
     "region": "Metropolitana de Santiago", "ciudad": "Buin", "modo": "pdf_links",
     "url": "https://www.buin.cl/trabaja-con-nosotros/",
     "pdf_host": "buin.cl", "pdf_con_texto": True, "cargo_desde_archivo": True},
    {"clave": "renca_cat", "id": 407, "nombre": "Municipalidad de Renca",
     "region": "Metropolitana de Santiago", "ciudad": "Renca", "modo": "enlaces_detalle",
     "url": "https://renca.cl/category/trabaja-con-nosotros/",
     "detalle_re": r"/proceso-de-seleccion[^/\"']*"},

    # ── No accesibles con requests puro (declaradas, no construidas) ────────
    {"clave": "maipu_cp", "id": 398, "nombre": "Municipalidad de Maipú (concursos)",
     "region": RM, "ciudad": "Maipú", "modo": "spa",
     "url": "https://www.municipalidadmaipu.cl/concursos-publicos",
     "nota": "SPA Vue sin API pública detectable; requiere navegador (Playwright)"},
    {"clave": "maipu_em", "id": 398, "nombre": "Municipalidad de Maipú (empleos)",
     "region": RM, "ciudad": "Maipú", "modo": "tabla_empleos",
     "url": "https://www.municipalidadmaipu.cl/empleos-municipales",
     "nota": "Tabla server-side de cargos vigentes (CARGO/OFICINA/RECEPCIÓN/BASES/OBSERVACIONES). "
             "La tabla de seleccionados se descarta por cabecera. Correo de postulación en OBSERVACIONES"},
    {"clave": "talagante", "id": 427, "nombre": "Municipalidad de Talagante",
     "region": RM, "ciudad": "Talagante", "modo": "spa",
     "url": "https://www.munitalagante.cl/ofertas-laborales",
     "nota": "Sitio Wix; contenido renderizado por JS, requiere navegador"},
    {"clave": "sanignacio", "id": 501, "nombre": "Municipalidad de San Ignacio",
     "region": "Ñuble", "ciudad": "San Ignacio", "modo": "bloqueada",
     "url": "https://munisanignacio.cl/concursos-evaluacion/",
     "nota": "WAF 503: requiere IP chilena"},
    {"clave": "melipilla", "id": 422, "nombre": "Municipalidad de Melipilla",
     "region": RM, "ciudad": "Melipilla", "modo": "bloqueada",
     "url": "https://melipilla.cl/trabaja-con-nosotros/",
     "nota": "WAF 503: requiere IP chilena"},
    {"clave": "sanbernardo", "id": 415, "nombre": "Municipalidad de San Bernardo",
     "region": RM, "ciudad": "San Bernardo", "modo": "bloqueada",
     "url": "https://www.sanbernardo.cl/concursos-publicos/",
     "nota": "WAF 503: requiere IP chilena"},
    {"clave": "recoleta", "id": 406, "nombre": "Municipalidad de Recoleta",
     "region": RM, "ciudad": "Recoleta", "modo": "bloqueada",
     "url": "https://www.recoleta.cl/trabaje-con-nosotros/",
     "nota": "WAF 403: requiere IP chilena"},
    {"clave": "saludstgo", "id": 380, "nombre": "Corp. Salud y Educación Santiago (SALUDSTGO)",
     "region": RM, "ciudad": "Santiago", "modo": "bloqueada",
     "url": "https://www.saludstgo.cl/quienes-somos/trabaja-con-nosotros/",
     "nota": "WAF 503: requiere IP chilena"},
    {"clave": "sanjoaquin", "id": 679, "nombre": "Corp. Salud San Joaquín",
     "region": RM, "ciudad": "San Joaquín", "modo": "bloqueada",
     "url": "https://ww2.sanjoaquinsaludable.cl/trabaja-con-nosotros/",
     "nota": "WAF 503: requiere IP chilena"},
    {"clave": "cmsm", "id": 680, "nombre": "Corp. Municipal San Miguel (Salud)",
     "region": RM, "ciudad": "San Miguel", "modo": "spa",
     "url": "https://cmsm.cl/trabaja-con-nosotros/",
     "nota": "Contenido cargado dinámicamente; sin links de oferta en HTML estático"},
    {"clave": "codep", "id": 666, "nombre": "CODEP Pudahuel (Corp. Desarrollo Social)",
     "region": RM, "ciudad": "Pudahuel", "modo": "spa",
     "url": "https://www.codep.cl/portal-laboral/",
     "nota": "Portal por ID de postulación; sin listado público de ofertas en HTML"},
    {"clave": "corplascondes", "id": 668, "nombre": "Corp. Las Condes (TCN)",
     "region": RM, "ciudad": "Las Condes", "modo": "spa",
     "url": "https://tcn.corplascondes.cl/",
     "nota": "Portal propio (Laravel) con ofertas por JS; API no pública (404 en rutas probadas)"},
    {"clave": "talagante2", "id": 427, "nombre": "Municipalidad de Talagante (programas)",
     "region": RM, "ciudad": "Talagante", "modo": "spa",
     "url": "https://www.munitalagante.cl/cargosprogramasmunicipales",
     "nota": "Sitio Wix; contenido renderizado por JS, requiere navegador"},
    {"clave": "puntaarenas", "id": 632, "nombre": "Municipalidad de Punta Arenas",
     "region": "Magallanes", "ciudad": "Punta Arenas", "modo": "spa",
     "url": "https://www.puntaarenas.cl/municipio/concursos-publicos",
     "nota": "Cuerpo vacío en HTML estático; requiere navegador"},
    {"clave": "providencia", "id": 402, "nombre": "Municipalidad de Providencia",
     "region": RM, "ciudad": "Providencia", "modo": "bloqueada",
     "url": "https://tramites.providencia.cl/Portal/",
     "nota": "Portal de trámites autenticado; sin listado público de concursos"},
]

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto"]
SECTOR = "Municipalidad"


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/pdf,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
        "Upgrade-Insecure-Requests": "1",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400 or b"upstream connect error" in r.content[:300]:
                logger.info("  HTTP %s en %s", r.status_code, url[:75])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo %s: %s", url[:70], type(exc).__name__)
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


def _fecha_dmy(texto: str) -> date | None:
    if m := re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", texto or ""):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


# Cues de PERIODO DE CONTRATO: un "hasta el 31 de diciembre" precedido por
# esto es la vigencia del contrato, NO el plazo de postulación.
_CONTRATO_CTX = re.compile(
    r"(periodo\s+de\s+contrataci|renovaci|renovable|vigencia|"
    r"duraci[oó]n\s+del\s+contrato|contrato\s+(?:hasta|por|a)|meses?\s+iniciales)",
    re.I)


def _cierre_envio(texto: str) -> date | None:
    """Fecha límite: 'hasta el ...' tras envío/recepción de antecedentes."""
    t = limpiar_texto(texto)
    corte = re.search(r"\bConsultas?\s*:", t, re.I)
    ambito = t[: corte.start()] if corte else t
    fecha = r"(\d{1,2}\s+de\s+\w+\s+de(?:l)?\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{4})"
    for i, patron in enumerate((
        rf"(?:enviad[oa]s?|enviarse|recepci[oó]n|recibid[oa]s?|postulaci[oó]n|"
        rf"plazo|vence|cierre)\b.{{0,90}}?\bhasta\s+(?:el\s+)?"
        rf"(?:las?\s+[\d:.]+\s*(?:horas?|hrs\.?)?\s*(?:del?\s+(?:d[ií]a\s+)?)?)?{fecha}",
        rf"hasta\s+el\s+(?:d[ií]a\s+)?{fecha}",
    )):
        if m := re.search(patron, ambito, re.I):
            # El 2º patrón ("hasta el X" suelto) NO debe tomar la vigencia del
            # contrato como plazo de postulación (Cerro Navia: "3 meses iniciales
            # … hasta el 31 de diciembre de 2026").
            if i == 1 and _CONTRATO_CTX.search(ambito[max(0, m.start() - 60):m.start()]):
                continue
            return _fecha_es(m.group(1)) or _fecha_dmy(m.group(1))
    return None


# Rango de fechas "02 al 07 de julio [de 2026]" (recepción de antecedentes).
# El cierre es el ÚLTIMO día del rango. El año es opcional en muchos avisos
# municipales (Cerro Navia: "Recepción de antecedentes: 02 al 07 de julio").
_RANGO_DIAS = (r"(?:del?\s+)?\d{1,2}\s+al\s+(\d{1,2})\s+de\s+([a-záéíóúñ]+)"
               r"(?:\s+de(?:l)?\s+(\d{4}))?")
_RE_RANGO_CTX = re.compile(
    r"(?:recepci[oó]n|postulaci[oó]n|antecedentes|plazo|curr[ií]culum|\bcv\b|"
    r"vence|cierre)[^\n]{0,70}?" + _RANGO_DIAS, re.I)


# Renta publicada en el HTML del aviso (no en el PDF de bases). Cerro Navia:
# "Remuneración bruta: $1.000.000.-". Se valida el monto con parse_renta.
_RE_RENTA_LINEA = re.compile(
    r"(?:remuneraci[oó]n|renta|sueldo|honorarios?)\s*"
    r"(?:bruta?|l[ií]quida?|mensual|aproximada?)?\s*:?\s*\$?\s*[\d][\d.\s]{3,12}", re.I)


def _renta_html(texto: str) -> tuple[str | None, int | None, int | None]:
    """Renta desde el HTML del aviso → (renta_texto, min, max). Solo la devuelve
    si parse_renta valida un monto (evita arrastrar texto sin cifra útil)."""
    for m in _RE_RENTA_LINEA.finditer(texto or ""):
        linea = limpiar_texto(m.group(0))
        try:
            from scrapers.base import parse_renta
            minimo, maximo, _ = parse_renta(linea)
        except Exception:
            minimo, maximo = None, None
        if minimo:
            return linea, minimo, maximo
    return None, None, None


def _cierre_rango(texto: str, hoy: date | None = None) -> date | None:
    """'Recepción de antecedentes: 02 al 07 de julio [de 2026]' -> último día.

    Exige contexto de plazo/recepción/postulación (_RE_RANGO_CTX): un rango
    suelto "02 al 07 de julio" sin ese contexto suele ser un horario u otra cosa,
    y tomarlo como cierre inventaría un plazo (peor: futuro → saltaría revisión).
    Si el aviso no trae año, se asume el en curso; si quedó >60 días atrás, el
    año siguiente (aviso de diciembre para un cierre de enero).
    """
    hoy = hoy or date.today()
    t = limpiar_texto(texto)
    m = _RE_RANGO_CTX.search(t)
    if not m:
        return None
    mes = MESES.get(m.group(2).lower())
    if not mes:
        return None
    dia = int(m.group(1))
    anio = int(m.group(3)) if m.group(3) else hoy.year
    try:
        cand = date(anio, mes, dia)
    except ValueError:
        return None
    if not m.group(3) and (hoy - cand).days > 60:
        try:
            cand = date(anio + 1, mes, dia)
        except ValueError:
            return None
    return cand


# Correos citados junto a la instrucción de envío de antecedentes. Anclamos a la
# instrucción ("Enviar antecedentes al correo …") para no capturar el correo del
# webmaster del pie de página. Cerro Navia lista dos correos de postulación.
_RE_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
# Buzones genéricos que nunca son de postulación (evita falsos positivos del pie).
_EMAIL_GENERICO = re.compile(r"^(webmaster|no-?reply|noresponder|mailer-daemon|"
                             r"postmaster|info|soporte\.?web)@", re.I)


# Señal de postulación/envío en el texto que PRECEDE al correo. Se evalúa por
# cada correo (no se ancla en la primera aparición de una palabra clave): en
# avisos largos el correo real aparece al final, lejos del primer "antecedentes".
_EMAIL_CTX = re.compile(
    r"(enviar|remitir|enviad[oa]s?|correo|e-?mail|\bmail\b|postul|"
    r"antecedente|dirigir|recepci[oó]n|adjuntar|hacer llegar)", re.I)


def _emails_postulacion(texto: str) -> str | None:
    t = texto or ""
    vistos: set[str] = set()
    out: list[str] = []
    for m in _RE_EMAIL.finditer(t):
        e = m.group(0).rstrip(".").lower()
        if e in vistos or not (5 < len(e) <= 90) or _EMAIL_GENERICO.match(e):
            continue
        # Contexto de postulación en los ~150 chars previos al correo. Cubre
        # "Enviar antecedentes al correo X y Y": para el 2º correo el cue "correo"
        # sigue estando dentro de la ventana previa.
        if not _EMAIL_CTX.search(t[max(0, m.start() - 150):m.start()]):
            continue
        vistos.add(e)
        out.append(e)
        if len(out) >= 3:
            break
    return ", ".join(out) if out else None


# Prefijos de aviso que NO son parte del cargo:
# "Oferta laboral para proveer el cargo de X" -> "X".
_RE_CARGO_PREFIJO = re.compile(
    r"^(oferta laboral|oferta de trabajo|oferta de empleo|oferta|"
    r"proceso de selecci[óo]n|llamado a concurso|concurso p[úu]blico|"
    r"convocatoria|se necesita|se requiere|requiere|b[úu]squeda de)\b[ :\-–|]*", re.I)
_RE_CARGO_PROVEER = re.compile(
    r"^(?:para\s+)?(?:proveer\s+(?:el\s+)?)?(?:el\s+|un\s+|la\s+|una\s+)?"
    r"cargo\s+de\s+", re.I)


def _limpiar_cargo(cargo: str) -> str:
    """Deja sólo el nombre del cargo, sin el envoltorio del aviso."""
    c = limpiar_texto(cargo or "")
    prev = None
    while c and c != prev:
        prev = c
        c = _RE_CARGO_PREFIJO.sub("", c)
        c = _RE_CARGO_PROVEER.sub("", c)
    # "Concurso público para Coordinador/a …" -> tras quitar el prefijo queda
    # "para Coordinador…"; sacamos ese "para (proveer el cargo de)" residual.
    c = re.sub(r"^para\s+(?:proveer\s+(?:el\s+|la\s+)?)?(?:cargo\s+de\s+)?", "",
               c, flags=re.I)
    # Cola con fecha ("… 12 de julio 2026") que a veces arrastra el título.
    c = re.sub(r"\s+\d{1,2}\s+(?:de\s+)?(?:ene|feb|mar|abr|may|jun|jul|ago|sep|"
               r"oct|nov|dic)\w*\s*\d{0,4}$", "", c, flags=re.I)
    return limpiar_texto(c)


def _bases_pdf(soup, base_url: str) -> str | None:
    """PDF de BASES/perfil del concurso, excluyendo formularios y declaraciones
    juradas (Formulario 1-A/1-B/1-C NO son bases). Si no hay ninguno cuyo nombre
    lo delate pero sí hay UN único PDF no-excluido en la página, se asume que ese
    es la base (caso Independencia: la página enlaza sólo el PDF del concurso)."""
    candidatos: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" not in href.lower():
            continue
        url = urljoin(base_url, href)
        low = unquote(url).lower()
        if any(x in low for x in _PDF_EXCLUIR):
            continue  # formulario / declaracion / anexo / solicitud → no es base
        texto_a = limpiar_texto(a.get_text(" ", strip=True)).lower()
        if re.search(r"(base|perfil|descriptor|convocatoria|concurso|t[ée]rminos de ref)",
                     low + " " + texto_a):
            return url
        if url not in candidatos:
            candidatos.append(url)
    return candidatos[0] if len(candidatos) == 1 else None


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("director", "jefe", "jefa", "secretario municipal",
                            "administrador municipal", "encargad")):
        return "Directivo"
    if any(w in c for w in ("auxiliar", "asistente", "administrativo", "operativo",
                            "técnico", "tecnico", "conductor", "oficio",
                            "ayudante", "secretari", "inspector")):
        return "Técnico"
    return "Profesional"


def _tipo(texto: str) -> str:
    t = (texto or "").lower()
    if "honorario" in t:
        return "Honorarios"
    if re.search(r"\bcontrata\b", t):
        return "Contrata"
    if "planta" in t:
        return "Planta"
    if "código del trabajo" in t or "codigo del trabajo" in t:
        return "Código del Trabajo"
    return "Concurso Público Municipal"


def _cargo_desde_filename(href: str) -> str:
    base = unquote(href.rsplit("/", 1)[-1])
    base = re.sub(r"\.pdf.*$", "", base, flags=re.I)
    base = re.sub(r"(?:_|\b|-)(?:bases?|cargo|perfil|concurso|p[úu]blico|"
                  r"llamado|descriptor|remuneracion|final|v\d+|"
                  r"[uú]ltima|version|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|"
                  r"nov|dic)(?:_|\b|-|\.)", " ", base, flags=re.I)
    base = re.sub(r"\d{4,8}", " ", base)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+\d{1,2}\s+(?:de\s+)?(?:ene|feb|mar|abr|may|jun|jul|ago|"
                  r"sep|oct|nov|dic)\w*\s*\d{0,4}$", "", base, flags=re.I)
    return limpiar_texto(base).title()


def _pdf_texto(contenido: bytes) -> str:
    if PdfReader is None or contenido[:5] != b"%PDF-":
        return ""
    try:
        return "\n".join((p.extract_text() or "")
                         for p in PdfReader(io.BytesIO(contenido)).pages)
    except Exception:
        return ""


# ── Modos de extracción ──────────────────────────────────────────────────────
def _extraer_de_pdf(session, url_pdf, fuente, delay):
    """Parsea el PDF de bases y devuelve requisitos, cierre, tipo, renta, correos
    y un extracto de texto (`texto_pdf`) para páginas de detalle con HTML pobre
    (p.ej. Independencia, donde el aviso real vive en el PDF)."""
    if not fuente.get("pdf_con_texto"):
        return {}
    time.sleep(delay / 2)
    rp = _get(session, url_pdf)
    if rp is None:
        return {}
    t = limpiar_texto(_pdf_texto(rp.content))
    if not t:
        return {}
    d: dict[str, Any] = {"texto_pdf": t[:1800]}
    if m := re.search(r"Requisitos[^:]{0,25}:?\s*(.{30,1500})", t, re.I):
        bloque = m.group(1)
        c = re.search(r"(Funciones|Competencias|Postulaci|Proceso de selec|Etapas)",
                      bloque, re.I)
        d["requisitos"] = limpiar_texto(bloque[: c.start()] if c else bloque)[:2000]
    # Cierre: "hasta el …" o rango "02 al 07 de julio" dentro de las bases.
    if cierre := (_cierre_envio(t) or _cierre_rango(t)):
        d["fecha_cierre"] = cierre
    d["tipo"] = _tipo(t)
    if m := re.search(r"(Renta|Remuneraci[oó]n|Sueldo|Grado)[^.\n]{0,60}"
                      r"\$?\s*[\d.]{4,12}", t, re.I):
        d["renta_texto"] = limpiar_texto(m.group(0))[:120]
    # Correos de postulación citados en las bases.
    if correos := _emails_postulacion(t):
        d["email_postulacion"] = correos
    return d


def extraer_pdf_links(html, fuente, session, delay):
    soup = BeautifulSoup(html, "html.parser")
    host = fuente.get("pdf_host", "")
    # señales de que un PDF ES una oferta/base (no un documento institucional)
    _ES_OFERTA = re.compile(
        r"(base|cargo|perfil|concurso|llamado|profesional|t[ée]cnic|"
        r"administrativ|auxiliar|directivo|jefatura|jefe|honorario|contrata|"
        r"planta|oferta|vacante|reemplazo|seleccion|selección|grado)", re.I)

    def generico(t: str) -> bool:
        return (len(t) < 10 or bool(re.match(
            r"(descarga|ver|aqu[ií]|bases|postula|pdf|m[áa]s informaci|link|clic)",
            t, re.I)))

    # mejor anchor por URL de PDF
    mejor: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" not in href.lower():
            continue
        url_pdf = urljoin(fuente["url"], href)
        if host and host not in urlparse(url_pdf).netloc:
            continue
        low = unquote(url_pdf).lower()
        if any(x in low for x in _PDF_EXCLUIR):
            continue
        t = limpiar_texto(a.get_text(" ", strip=True))
        if not (_ES_OFERTA.search(low) or _ES_OFERTA.search(t)):
            continue  # PDF institucional (cuenta pública, etc.), no es oferta
        if url_pdf not in mejor or (not generico(t) and len(t) > len(mejor[url_pdf])):
            mejor[url_pdf] = "" if generico(t) else t

    items = []
    for url_pdf, anchor in mejor.items():
        cargo = (_cargo_desde_filename(url_pdf)
                 if fuente.get("cargo_desde_archivo") or generico(anchor) else anchor)
        cargo = limpiar_texto(re.sub(
            r"^(aviso|bases?|concurso|llamado a?|perfil|vr)\s+", "", cargo, flags=re.I))
        if not cargo or len(cargo) < 4:
            continue
        if re.match(r"(requisitos|c[óo]mo postular|informaci|documentos|"
                    r"antecedentes|proceso|etapas|importante)\b", cargo, re.I):
            continue  # anexo genérico, no un cargo
        extra = _extraer_de_pdf(session, url_pdf, fuente, delay)
        items.append({"cargo": cargo[:500], "url": url_pdf, **extra})
    return items


def extraer_secciones(html, fuente):
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main, .entry-content, article") or soup.body
    texto = limpiar_texto(main.get_text(" ", strip=True)) if main else ""
    pos = [(m.start(), limpiar_texto(m.group(0)))
           for m in _RE_TITULO_PROC.finditer(texto)]
    pos = [(p, t) for p, t in pos if any(c.isupper() for c in t[:25])]
    items = []
    for idx, (p, titulo) in enumerate(pos):
        fin = pos[idx + 1][0] if idx + 1 < len(pos) else min(len(texto), p + 2200)
        bloque = texto[p:fin]
        cargo = re.sub(r"^(LLAMADO A CONCURSO|CONCURSO P[ÚU]BLICO|"
                       r"PROCESO DE SELECCI[ÓO]N|CONVOCATORIA)[ :\-–]*", "",
                       titulo, flags=re.I)
        cargo = re.split(r"\s+(?:Municipalidad|La |El |para|tiene|invita)\b",
                         cargo, maxsplit=1)[0]
        cargo = limpiar_texto(cargo) or limpiar_texto(titulo)
        # rechazar si el "cargo" arrastró el menú de navegación del sitio
        if len(cargo) > 90 or re.search(r"(Alcald|Director|Unidades|Tr[áa]mites|"
                                        r"Inicio|Men[úu]).*(Director|Unidades|"
                                        r"Salud|Educaci)", cargo, re.I):
            cargo = limpiar_texto(titulo)[:120]
        items.append({"cargo": cargo[:500], "bloque": bloque,
                      "fecha_cierre": _cierre_envio(bloque), "tipo": _tipo(bloque),
                      "url": fuente["url"] + "#" + re.sub(
                          r"[^a-z0-9]+", "-", cargo.lower())[:50]})
    return items


def _norm_cabecera(texto: str) -> str:
    t = limpiar_texto(texto).lower()
    for a, b in (("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u")):
        t = t.replace(a, b)
    return t


def extraer_tabla_empleos(html, fuente):
    """Ofertas publicadas en una TABLA — una fila por cargo.

    Municipalidad de Maipú (/empleos-municipales) lista los cargos vigentes en
    una tabla: CARGO · OFICINA/DEPTO. · RECEPCIÓN DOCUMENTOS · BASES DE
    POSTULACIÓN ("AQUÍ" → PDF/enlace de bases) · OBSERVACIONES (correo y asunto
    de postulación). La otra tabla de la página, "CARGOS / SELECCIONADOS(AS)"
    (procesos ya resueltos), se descarta por cabecera: no trae recepción/bases.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for table in soup.find_all("table"):
        filas = table.find_all("tr")
        if len(filas) < 2:
            continue
        cab = [_norm_cabecera(c.get_text(" ", strip=True))
               for c in filas[0].find_all(("td", "th"))]
        # Solo la tabla de ofertas vigentes trae recepción de documentos + bases.
        if not (any("recepcion" in h for h in cab) and any("bases" in h for h in cab)):
            continue

        def _idx(*claves):
            for i, h in enumerate(cab):
                if any(k in h for k in claves):
                    return i
            return None

        i_cargo = _idx("cargo")
        i_cargo = 0 if i_cargo is None else i_cargo
        i_ofic = _idx("oficina", "depto", "departamento", "unidad")
        i_recep = _idx("recepcion", "plazo", "cierre")
        i_bases = _idx("bases", "postulacion")
        i_obs = _idx("observacion")

        for tr in filas[1:]:
            celdas = tr.find_all(("td", "th"))
            if len(celdas) < 3:
                continue

            def _celda(i):
                return celdas[i] if (i is not None and i < len(celdas)) else None

            c_cargo = _celda(i_cargo)
            # get_text SIN separador: el cargo viene partido en <strong> adyacentes
            # ("<strong>A</strong><strong>DMI</strong>…"); unir sin espacios lo
            # reconstruye ("ADMINISTRATIVO") y limpiar_texto colapsa el resto.
            cargo = (_limpiar_cargo(limpiar_texto(c_cargo.get_text("", strip=True)))
                     if c_cargo else "")
            if not cargo or len(cargo) < 3:
                continue

            oficina = (limpiar_texto(_celda(i_ofic).get_text(" ", strip=True))
                       if _celda(i_ofic) else "")
            fecha = (_fecha_dmy(_celda(i_recep).get_text(" ", strip=True))
                     if _celda(i_recep) else None)

            bases_href = None
            c_bases = _celda(i_bases)
            if c_bases and (a := c_bases.find("a", href=True)):
                href = a["href"].strip()
                if href and not href.lower().startswith(("javascript:", "#")):
                    bases_href = urljoin(fuente["url"], href)

            obs = (limpiar_texto(_celda(i_obs).get_text(" ", strip=True))
                   if _celda(i_obs) else "")
            email = _emails_postulacion(obs)

            # url_original único por cargo (todas las filas comparten página):
            # ancla con slug del cargo → hash de dedup distinto por oferta.
            slug = re.sub(r"[^a-z0-9]+", "-", cargo.lower()).strip("-")[:60] or "cargo"
            bloque = " | ".join(p for p in (
                f"Oficina/Depto.: {oficina}" if oficina else "", obs) if p)

            items.append({
                "cargo": cargo[:500],
                "url": f'{fuente["url"]}#{slug}',
                "url_bases": bases_href,
                "fecha_cierre": fecha,
                "bloque": bloque or None,
                "email_postulacion": email,
            })
    if not items:
        # Diagnóstico: distinguir "no hay concursos publicados" de "la tabla la
        # inyecta JS" (el HTML estático viene sin tablas → requeriría navegador).
        n_tablas = len(soup.find_all("table"))
        logger.info("  tabla_empleos: 0 ofertas (tablas en el HTML: %d%s)",
                    n_tablas,
                    "" if n_tablas else " — posible render por JS, revisar con navegador")
    return items


# Conectores que no se capitalizan en el título del cargo.
_CONECTORES_TITULO = {"de", "del", "la", "el", "los", "las", "y", "e", "en", "a"}


def _titulo_cargo(texto: str) -> str:
    """Title-case respetando conectores (…"del"…"de"…) y siglas cortas."""
    palabras = limpiar_texto(texto).split()
    out = []
    for i, w in enumerate(palabras):
        wl = w.lower()
        if i > 0 and wl in _CONECTORES_TITULO:
            out.append(wl)
        elif len(w) <= 4 and w.isupper():   # siglas: SECPLA, DIDECO, PDI…
            out.append(w)
        else:
            out.append(wl.capitalize())
    return " ".join(out)


# Viña del Mar publica los concursos como bloques de texto planos:
# "BASES CONCURSO PÚBLICO PARA PROVEER 1 CARGO PLANTA PROFESIONAL GRADO 11
#  DEPARTAMENTO SERVICIOS DEL AMBIENTE D.A 8001", y cada bloque enlaza un PDF de
# bases nombrado por el número D.A (8001.26.pdf). El PDF suele ser un escaneo
# (sin texto), así que el cargo/estamento/grado/vacantes salen del HTML y el PDF
# queda como url_bases.
_RE_CONCURSO_DA = re.compile(
    r"BASES\s+CONCURSO\s+P[UÚ]BLICO\s+PARA\s+PROVEER\s+(\d+)\s+CARGOS?\s+"
    r"PLANTA\s+([A-ZÁÉÍÓÚÑ]+)\s+GRADO\s+(\d+)\s+(.{3,90}?)\s+D\.?\s*A\.?\s*(\d{3,5})",
    re.I)
_RE_UNIDAD_INICIAL = re.compile(
    r"^(departamento|direcci[oó]n|unidad|oficina|secci[oó]n|juzgado)\b", re.I)


def extraer_concursos_da(html, fuente):
    """Concursos de planta listados como bloques 'BASES … D.A NNNN' + PDF por
    número D.A (Viña del Mar /concursos-publicos/). Una oferta por cargo."""
    soup = BeautifulSoup(html, "html.parser")
    # Mapa número D.A → URL del PDF de bases (NNNN.YY.pdf, host propio).
    da_pdf: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        m = re.search(r"/(\d{3,5})\.\d+\.pdf", a["href"], re.I)
        if m:
            da_pdf.setdefault(m.group(1), urljoin(fuente["url"], a["href"]))
    cont = soup.select_one("main, .entry-content, article") or soup.body or soup
    texto = limpiar_texto(cont.get_text(" ", strip=True))
    items, vistos = [], set()
    for m in _RE_CONCURSO_DA.finditer(texto):
        vac, estamento, grado, titulo, da = m.groups()
        if da in vistos:
            continue
        vistos.add(da)
        est = estamento.title()
        titulo_c = _titulo_cargo(titulo)
        # Si el título es una unidad ("Departamento …") anteponer el estamento;
        # si ya empieza por el estamento (p.ej. "Profesional Contabilidad"), no.
        if _RE_UNIDAD_INICIAL.match(titulo) or not titulo_c.lower().startswith(est.lower()):
            cargo = f"{est} {titulo_c}"
        else:
            cargo = titulo_c
        pdf = da_pdf.get(da)
        bloque = (f"Planta {est} · Grado {grado} · "
                  f"{vac} vacante(s) · Concurso D.A {da}.")
        items.append({
            "cargo": limpiar_texto(cargo)[:500],
            "url": pdf or (fuente["url"] + "#da-" + da),
            "url_bases": pdf,
            "bloque": bloque,
            "numero_vacantes": int(vac) if vac.isdigit() else None,
            "tipo": "Planta (Concurso Público Municipal)",
        })
    return items


# La Cisterna publica UN concurso de planta con muchas vacantes en un PDF de
# bases (uploads.mejormunicipio.com). El resumen enumera los cargos por
# estamento y grado: "5 cargos vacantes en Planta Profesionales Grado 9°; …".
# El sitio es una SPA (no scrapeable con requests), así que el PDF de bases se
# fija en la fuente (bases_pdf) y se descarga directo; una oferta por
# estamento+grado.
_ESTAMENTO_SINGULAR = {
    "profesionales": "Profesional", "jefaturas": "Jefatura",
    "tecnicos": "Técnico", "técnicos": "Técnico",
    "administrativos": "Administrativo", "auxiliares": "Auxiliar", "directivos": "Directivo",
}
_RE_GRUPO_GRADO = re.compile(
    r"(\d+)\s+cargos?\s+vacantes?\s+en\s+Planta\s+([A-Za-zÁÉÍÓÚáéíóúñ]+)\s+Grado\s+(\d+)", re.I)
_RE_GRUPO_MEDICO = re.compile(
    r"(\d+)\s+cargos?\s+vacantes?\s+en\s+Planta\s+[A-Za-zÁÉÍÓÚáéíóúñ]+\s+"
    r"(M[eé]dico\s+Psicot[eé]cnico(?:\s+de\s+\d+\s+horas)?)", re.I)


def parsear_bases_estamento_grado(texto: str) -> list[dict[str, Any]]:
    """Separa el resumen del concurso en grupos estamento+grado (una oferta por
    grupo). Robusto a saltos de página (se aplana el texto)."""
    t = re.sub(r"\s+", " ", texto or "")
    out: list[dict[str, Any]] = []
    vistos: set[tuple[str, str]] = set()
    for m in _RE_GRUPO_GRADO.finditer(t):
        vac, est, grado = int(m.group(1)), m.group(2), m.group(3)
        est_sing = _ESTAMENTO_SINGULAR.get(est.lower(), est.title())
        clave = (est_sing, grado)
        if clave in vistos:
            continue
        vistos.add(clave)
        out.append({
            "estamento": est_sing, "grado": grado, "vacantes": vac,
            "cargo": f"{est_sing} Grado {grado}",
            "slug": f"{est_sing.lower()}-g{grado}",
        })
    for m in _RE_GRUPO_MEDICO.finditer(t):
        vac = int(m.group(1))
        horas = re.search(r"(\d+)\s*horas", m.group(2))
        cargo = "Profesional Médico Psicotécnico" + (f" ({horas.group(1)} horas)" if horas else "")
        clave = (cargo, "")
        if clave in vistos:
            continue
        vistos.add(clave)
        out.append({
            "estamento": "Profesional Médico Psicotécnico", "grado": None,
            "vacantes": vac, "cargo": cargo, "slug": "medico-psicotecnico",
        })
    return out


def extraer_bases_estamento_grado(html, fuente, session, delay):
    """Descarga el PDF de bases fijado en la fuente (bases_pdf) y emite una
    oferta por estamento+grado. El HTML de la página se ignora (SPA)."""
    pdf_url = fuente.get("bases_pdf")
    if not pdf_url:
        return []
    time.sleep(delay / 2)
    rp = _get(session, pdf_url)
    if rp is None:
        logger.warning("  No se pudo bajar bases_pdf %s", pdf_url[:70])
        return []
    texto = _pdf_texto(rp.content)
    if not texto:
        logger.info("  PDF de bases sin texto extraíble: %s", pdf_url[:70])
        return []
    grupos = parsear_bases_estamento_grado(texto)
    items = []
    for g in grupos:
        det = f"Planta {g['estamento']}"
        if g["grado"]:
            det += f" · Grado {g['grado']}"
        det += f" · {g['vacantes']} vacante(s)"
        items.append({
            "cargo": g["cargo"][:500],
            "url": f"{pdf_url}#{g['slug']}",   # único por grupo
            "url_bases": pdf_url,
            "bloque": f"{det}. Concurso público de ingreso a la planta municipal.",
            "numero_vacantes": g["vacantes"],
            "tipo": "Planta (Concurso Público Municipal)",
        })
    return items


def extraer_headings(html, fuente):
    """E-Central: cargos publicados como headings; agrupar por encabezado."""
    soup = BeautifulSoup(html, "html.parser")
    items, vistos = [], set()
    for el in soup.find_all(["h2", "h3", "h4"]):
        t = limpiar_texto(el.get_text(" ", strip=True))
        if not (8 < len(t) < 80):
            continue
        if not re.search(r"(profesional|t[ée]cnic|administrativ|auxiliar|encargad|"
                         r"jefe|asistente|conductor|operario|educador|trabajador|"
                         r"cargo de|concurso p[úu]blico para)", t, re.I):
            continue
        if re.match(r"(perfil|requisitos|competencias|funciones|c[óo]mo postular|"
                    r"informaci[óo]n|documentos|antecedentes|proceso de postulaci|"
                    r"etapas|bases del|importante|nota)", t, re.I):
            continue
        clave = t.lower()
        if clave in vistos:
            continue
        vistos.add(clave)
        cont = el.find_parent(["section", "div", "article"])
        bloque = limpiar_texto(cont.get_text(" ", strip=True))[:1500] if cont else t
        cargo = re.sub(r"^(concurso p[úu]blico para proveer cargo de|"
                       r"cargo de)\s+", "", t, flags=re.I)
        # Mismo estándar de extracción que enlaces_detalle: el plazo de recepción
        # (rango) prima sobre "hasta el …" (que puede ser vigencia de contrato),
        # y se recogen correo de postulación y renta publicada en el bloque.
        cierre = _cierre_rango(bloque) or _cierre_envio(bloque)
        renta_texto, renta_min, renta_max = _renta_html(bloque)
        items.append({"cargo": limpiar_texto(cargo)[:500], "bloque": bloque,
                      "fecha_cierre": cierre, "tipo": _tipo(bloque),
                      "email_postulacion": _emails_postulacion(bloque),
                      "renta_texto": renta_texto,
                      "renta_bruta_min": renta_min,
                      "renta_bruta_max": renta_max,
                      "url": fuente["url"] + "#" + re.sub(
                          r"[^a-z0-9]+", "-", cargo.lower())[:50]})
    return items


# Botones de "compartir en redes" cuyo href lleva la URL de la oferta en un
# parámetro (facebook sharer, twitter/x intent, whatsapp, linkedin, mail…). El
# detalle_re matcheaba esa URL embebida y el scraper terminaba pidiendo el share
# (HTTP 400) en vez de la ficha. Se descartan por host/patrón.
_URL_COMPARTIR_RE = re.compile(
    r"(facebook\.com/sharer|/sharer\.php|twitter\.com/(?:share|intent)|x\.com/intent|"
    r"/intent/tweet|wa\.me/|api\.whatsapp\.com|whatsapp\.com/send|linkedin\.com/share|"
    r"t\.me/share|mailto:|pinterest\.com/pin|telegram\.me)", re.I)


def extraer_enlaces_detalle(html, fuente, session, delay):
    soup = BeautifulSoup(html, "html.parser")
    patron = re.compile(fuente["detalle_re"], re.I)
    urls = []
    vistos = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if _URL_COMPARTIR_RE.search(href):
            continue  # botón de compartir, no la ficha
        if patron.search(href):
            url = urljoin(fuente["url"], href)
            if url not in vistos and url.rstrip("/") != fuente["url"].rstrip("/"):
                vistos.add(url)
                urls.append((url, limpiar_texto(a.get_text(" ", strip=True))))
    items = []
    for url, titulo in urls:
        if ".pdf" in url.lower() or "drive.google" in url:
            # bases directas: el "detalle" es el documento
            items.append({"cargo": (titulo or "Concurso público")[:500],
                          "url": url, "tipo": "Concurso Público Municipal"})
            continue
        time.sleep(delay)
        rd = _get(session, url)
        if rd is None:
            items.append({"cargo": titulo[:500] or "Proceso de selección",
                          "url": url})
            continue
        s2 = BeautifulSoup(rd.text, "html.parser")
        main = s2.select_one("main, .entry-content, article") or s2.body
        # Texto COMPLETO del detalle para resolver cierre/correos aunque estén al
        # final del aviso; la descripción se recorta aparte a 1800.
        texto_full = limpiar_texto(main.get_text(" ", strip=True)) if main else ""
        bloque = texto_full[:1800]
        # El título de la página suele ser más completo que el anchor del listado
        # ("Oferta laboral para proveer el cargo de …"). Preferimos el h1.
        h1 = s2.find(["h1", "h2"])
        cargo_bruto = (limpiar_texto(h1.get_text(" ", strip=True)) if h1 else "") or titulo
        cargo = _limpiar_cargo(cargo_bruto) or _limpiar_cargo(titulo) or "Proceso de selección"
        url_bases = _bases_pdf(s2, url)
        # Cuando la página de detalle enlaza el PDF de bases y la fuente lo marca
        # con texto (Independencia), el aviso REAL vive en el PDF: lo parseamos
        # para completar cierre / requisitos / tipo / renta / correos. El HTML de
        # estas páginas suele ser sólo el título + "descargar bases".
        pdf = _extraer_de_pdf(session, url_bases, fuente, delay) if url_bases else {}
        # Descripción: usa el HTML si es sustancioso; si es pobre, el extracto PDF.
        if len(texto_full) < 200 and pdf.get("texto_pdf"):
            bloque = pdf["texto_pdf"][:1800]
        # Cierre: el rango de RECEPCIÓN de antecedentes ("02 al 07 de julio") es
        # el plazo real y tiene prioridad sobre un "hasta el …" suelto, que puede
        # ser la vigencia del contrato (Cerro Navia: "hasta el 31 de diciembre").
        cierre = (_cierre_rango(texto_full) or _cierre_envio(texto_full)
                  or pdf.get("fecha_cierre"))
        correo = _emails_postulacion(texto_full) or pdf.get("email_postulacion")
        # Renta: la de las bases PDF si existe; si no, la publicada en el HTML.
        renta_texto = pdf.get("renta_texto")
        renta_min = renta_max = None
        if not renta_texto:
            renta_texto, renta_min, renta_max = _renta_html(texto_full)
        items.append({"cargo": cargo[:500], "url": url,
                      "bloque": bloque, "fecha_cierre": cierre,
                      "tipo": pdf.get("tipo") or _tipo(texto_full),
                      "requisitos": pdf.get("requisitos"),
                      "renta_texto": renta_texto,
                      "renta_bruta_min": renta_min,
                      "renta_bruta_max": renta_max,
                      "email_postulacion": correo,
                      "url_bases": url_bases})
    return items


def extraer_lista_o_vacio(html, fuente, session, delay):
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True)).lower()
    if re.search(r"(no hay (?:concursos|ofertas|vacantes|llamados|procesos)|"
                 r"no existen (?:concursos|ofertas|postulaciones)|"
                 r"sin (?:concursos|ofertas) (?:vigentes|disponibles)|"
                 r"actualmente no (?:hay|existen|se))", texto):
        return []
    # como respaldo, headings de cargo dentro del contenido principal
    return extraer_headings(html, fuente)


# ── Construcción de oferta ───────────────────────────────────────────────────
def construir_oferta(item, fuente):
    fuente_id = int(fuente["id"])
    nombre = fuente["nombre"]
    cargo = item["cargo"]

    desc_partes = []
    if item.get("bloque"):
        desc_partes.append(item["bloque"][:1200])
    elif item.get("texto_pdf"):
        # Modo pdf_links: el detalle vive en el PDF de bases. Antes su texto se
        # extraía y se descartaba (construir sólo leía "bloque"); ahora lo usamos
        # como descripción para que el enriquecimiento vea requisitos/renta/cierre.
        desc_partes.append(item["texto_pdf"][:1200])
    if item.get("renta_texto"):
        desc_partes.append(item["renta_texto"])
    if item["url"].lower().endswith(".pdf"):
        desc_partes.append(f"Bases: {item['url']}")
    descripcion = limpiar_texto(" | ".join(desc_partes))

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo, item["url"]),
        "fuente_id": fuente_id,
        "url_original": item["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if len(descripcion) > 25 else None,
        "institucion_nombre": nombre,
        "sector": SECTOR,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": item.get("tipo") or "Concurso Público Municipal",
        "nivel": _nivel(cargo),
        "region": normalizar_region(fuente["region"]) or fuente["region"],
        "ciudad": fuente.get("ciudad"),
        # La renta municipal suele vivir en las bases PDF; cuando el aviso la
        # publica en el HTML (Cerro Navia) el extractor la pasa como número.
        "renta_bruta_min": item.get("renta_bruta_min"),
        "renta_bruta_max": item.get("renta_bruta_max"),
        "renta_texto": item.get("renta_texto"),
        "numero_vacantes": item.get("numero_vacantes"),
        "fecha_publicacion": date.today(),
        "fecha_cierre": item.get("fecha_cierre"),
        "requisitos_texto": item.get("requisitos"),
        "url_bases": item["url"] if item["url"].lower().endswith(".pdf") else item.get("url_bases"),
        "email_postulacion": item.get("email_postulacion"),
        "email_consultas": item.get("email_consultas"),
    }


def _procesar_fuente(fuente, session, delay, incluir_cerrados):
    logger.info("─── %s [%s] ───", fuente["nombre"][:45], fuente["modo"])
    if fuente["modo"] in ("bloqueada", "spa"):
        logger.warning("  OMITIDA: %s", fuente.get("nota", "no accesible"))
        return []
    if fuente["modo"] == "bases_estamento_grado":
        # Este modo NO usa el HTML de la página (SPA; además cisterna.cl responde
        # 403 a IPs de datacenter): se va directo al PDF de bases fijado en la
        # fuente. Pedir la página primero abortaba la corrida con "Sin acceso"
        # aunque el PDF (uploads.mejormunicipio.com) sí fuera descargable.
        items = extraer_bases_estamento_grado("", fuente, session, delay)
        return _filtrar_items(items, fuente, session, delay, incluir_cerrados)
    r = _get(session, fuente["url"])
    if r is None:
        logger.warning("  Sin acceso a %s", fuente["url"])
        return []

    if fuente["modo"] == "pdf_links":
        items = extraer_pdf_links(r.text, fuente, session, delay)
    elif fuente["modo"] == "tabla_empleos":
        items = extraer_tabla_empleos(r.text, fuente)
    elif fuente["modo"] == "concursos_da":
        items = extraer_concursos_da(r.text, fuente)
    elif fuente["modo"] == "secciones":
        items = extraer_secciones(r.text, fuente)
    elif fuente["modo"] == "headings":
        items = extraer_headings(r.text, fuente)
    elif fuente["modo"] == "enlaces_detalle":
        items = extraer_enlaces_detalle(r.text, fuente, session, delay)
    else:
        items = extraer_lista_o_vacio(r.text, fuente, session, delay)

    return _filtrar_items(items, fuente, session, delay, incluir_cerrados)


def _filtrar_items(items, fuente, session, delay, incluir_cerrados):
    """Post-procesamiento común: dedup, filtro de basura/vencidas, enriquecido."""
    hoy = date.today()
    ofertas, omitidas = [], 0
    vistos = set()
    _BASURA = re.compile(r"(javascript|no est[áa] disponible|^proceso de selecci[óo]n$|"
                         r"^concurso p[úu]blico$|^llamado a concurso|cookies?|"
                         r"men[úu]|navegaci|^municipalidad )", re.I)
    for it in items:
        o = construir_oferta(it, fuente)
        if o["id_externo"] in vistos:
            continue
        if _BASURA.search(o["cargo"]) or len(o["cargo"]) > 95:
            continue
        vistos.add(o["id_externo"])
        if o["fecha_cierre"] and o["fecha_cierre"] < hoy and not incluir_cerrados:
            omitidas += 1
            continue
        try:
            from scrapers.enrich import enriquecer_oferta
            # Si la oferta apunta a un PDF de bases y la fuente NO lo parseó ya
            # (pdf_con_texto lo hace en _extraer_de_pdf), dejamos que enrich lo
            # descargue y mine: fecha de cierre, renta, requisitos y funciones.
            # Así avisos cuyo detalle vive en el PDF dejan de ir a revisión.
            pdf_urls = None
            ub = (o.get("url_bases") or "")
            if not fuente.get("pdf_con_texto") and ub.lower().endswith(".pdf"):
                pdf_urls = [ub]
            enriquecer_oferta(o, texto_html=o.get("descripcion"),
                              pdf_urls=pdf_urls, session=session)
        except Exception:
            pass
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
             delay=DELAY_DEFAULT, incluir_cerrados=False, export=None):
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper municipios%s", " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto (config/db): forzando --dry-run")
        dry_run = True

    fuentes = [f for f in FUENTES if not solo or f["clave"] == solo]
    session = _session()
    agg = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0,
           "encontradas": 0}
    todas = []
    # URLs vistas por institucion_id; el cierre por ausencia se hace una vez
    # por institución tras el loop (varias fuentes comparten institucion_id).
    urls_por_inst: dict[int, set[str]] = {}

    for fuente in fuentes:
        db = SessionLocal() if (SessionLocal and not dry_run) else None
        urls_activas = []
        f_stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
        try:
            ofertas = _procesar_fuente(fuente, session, delay, incluir_cerrados)
            if max_results:
                ofertas = ofertas[:max_results]
            agg["encontradas"] += len(ofertas)
            todas.extend(ofertas)
            for datos in ofertas:
                urls_activas.append(datos["url_original"])
                if verbose or dry_run:
                    print(f"  [{fuente['clave'][:11]:11}] {datos['cargo'][:46]:46} "
                          f"| {datos['region'][:14]:14} | cierre: {datos['fecha_cierre']}")
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
            if fuente["modo"] not in ("bloqueada", "spa"):
                # No cerramos aqui: acumulamos las URLs vistas y cerramos una
                # vez por institucion al final (evita que dos fuentes de la
                # misma muni -p.ej. Renca- se cierren ofertas entre si).
                urls_por_inst.setdefault(int(fuente["id"]), set()).update(urls_activas)
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
        time.sleep(delay)

    # Cierre por ausencia agrupado por institucion (union de todas sus fuentes).
    if not dry_run and SessionLocal and urls_por_inst:
        db_cierre = SessionLocal()
        try:
            for iid, urls in urls_por_inst.items():
                if not urls:
                    continue
                try:
                    agg["cerradas"] += marcar_ofertas_cerradas(db_cierre, iid, sorted(urls))
                except Exception:
                    db_cierre.rollback()
                    logger.exception("  Error cerrando ausentes inst=%s", iid)
        finally:
            db_cierre.close()

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN municipios: fuentes=%d encontradas=%d nuevas=%d act=%d "
                "err=%d (%.1fs)", len(fuentes), agg["encontradas"], agg["nuevas"],
                agg["actualizadas"], agg["errores"], dur)
    agg["duracion_seg"] = round(dur, 2)
    agg["status"] = "OK" if agg["errores"] == 0 else "PARCIAL"
    return agg


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper agrupado de concursos/empleos municipales")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope por fuente")
    p.add_argument("--solo", default=None, choices=[f["clave"] for f in FUENTES],
                   help="Procesar solo una fuente")
    p.add_argument("--delay", type=float, default=DELAY_DEFAULT)
    p.add_argument("--incluir-cerrados", action="store_true")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max, solo=a.solo,
             delay=a.delay, incluir_cerrados=a.incluir_cerrados, export=a.export)
