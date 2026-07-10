# -*- coding: utf-8 -*-
"""
scrapers/campos_comunes.py — Funciones canónicas de extracción de campos

Consolida las implementaciones duplicadas en base.py, trabajando.py,
carabineros.py, wordpress.py, generic_site.py y db/database.py en un
solo módulo.  Todos los scrapers y el extractor manual deben usar estas
funciones en lugar de sus copias locales.

Convenciones de retorno:
  - tipo_cargo → lowercase canónico: planta|contrata|honorarios|reemplazo|codigo_trabajo
  - nivel      → Title-case: Directivo|Profesional|Técnico|Auxiliar
  - area       → Title-case con tildes: Derecho, Salud, Ingeniería, …
  - region     → Nombre oficial de la región con tildes
  - ciudad     → Title-case
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from scrapers.base import normalizar_texto, strip_accents, normalizar_region, REGION_MAP


# ═══════════════════════════════════════════════════════════════════
#  TIPO DE CARGO / CALIDAD CONTRACTUAL
# ═══════════════════════════════════════════════════════════════════

CATEGORIAS_CODIGO_TRABAJO: frozenset[str] = frozenset({
    "empresa del estado",
    "empresa ffaa",
    "empresa",
    "banco central",
})

_RE_TIPO_CODIGO_TRABAJO = re.compile(
    r"\bc[oó]d(?:igo|\.)?\s+(?:del\s+)?trabajo\b", re.I,
)
_RE_TIPO_HONORARIOS = re.compile(r"\bhonorarios?\b", re.I)
_RE_TIPO_REEMPLAZO = re.compile(r"\b(?:reemplazo|suplencia)\b", re.I)
_RE_TIPO_CONTRATA = re.compile(
    r"(?:"
    r"\ba\s+contrata\b"
    r"|\bcalidad\s+(?:jur[ií]dica|contractual)?\s*:?\s*(?:de\s+|a\s+|la\s+)?contrata\b"
    r"|\b(?:v[ií]nculo|estamento|condici[oó]n)\s*:?\s*(?:a\s+|de\s+|la\s+)?contrata\b"
    r"|\btipo\s+de\s+(?:contrato|cargo|vacante|v[ií]nculo)\s*:?\s*contrata\b"
    r"|\bcontrata\s+(?:asimilad|grado)"
    r"|\|\s*contrata\s*(?=\||\n|$)"
    r")",
    re.I,
)
_RE_TIPO_PLANTA = re.compile(
    r"(?:"
    r"\b(?:cargos?|empleos?|titular)\s+(?:de\s+|a\s+|en\s+(?:la\s+)?)planta\b"
    r"|\bcalidad\s+(?:jur[ií]dica|contractual)?\s*:?\s*(?:de\s+|la\s+)?planta\b"
    r"|\b(?:v[ií]nculo|estamento|condici[oó]n)\s*:?\s*(?:de\s+|la\s+)?planta\b"
    r"|\btipo\s+de\s+(?:contrato|cargo|vacante|v[ií]nculo)\s*:?\s*planta\b"
    r"|\bplanta\s+(?:directiv|profesional|t[eé]cnic|administrativ|auxiliar|fiscalizador|municipal|titular)"
    r"|\ba\s+planta\b"
    r"|\|\s*planta\s*(?=\||\n|$)"
    r")",
    re.I,
)
# Carabineros usa "fondos propios" y "D.F.L." como señal de Código del Trabajo
_RE_FONDOS_PROPIOS = re.compile(r"\b(?:fondos\s+propios|d\.?f\.?l\.?)\b", re.I)

_TIPO_LABEL_MAX = 60


def detectar_tipo_cargo(
    texto: str | None,
    categoria: str | None = None,
) -> str | None:
    """Detecta calidad contractual → lowercase canónico.

    Unifica la lógica de base.py, trabajando.py, carabineros.py y
    contract_extractor.py.  Acepta texto largo (descripción) o etiqueta
    corta ("Contrata", "Planta titular").
    """
    limpio = normalizar_texto(texto)
    if not limpio:
        if categoria and normalizar_texto(categoria) in CATEGORIAS_CODIGO_TRABAJO:
            return "codigo_trabajo"
        return None

    # Ya normalizado (idempotencia)
    if limpio in {"planta", "contrata", "honorarios", "reemplazo", "codigo_trabajo"}:
        resultado = limpio
    elif _RE_TIPO_CODIGO_TRABAJO.search(limpio):
        resultado = "codigo_trabajo"
    elif _RE_FONDOS_PROPIOS.search(limpio):
        resultado = "codigo_trabajo"
    elif _RE_TIPO_HONORARIOS.search(limpio):
        resultado = "honorarios"
    elif _RE_TIPO_CONTRATA.search(limpio):
        resultado = "contrata"
    elif _RE_TIPO_PLANTA.search(limpio):
        resultado = "planta"
    elif _RE_TIPO_REEMPLAZO.search(limpio):
        resultado = "reemplazo"
    elif len(limpio) <= _TIPO_LABEL_MAX:
        if re.search(r"\bcontrata\b", limpio):
            resultado = "contrata"
        elif re.search(r"\bplanta\b", limpio):
            resultado = "planta"
        else:
            resultado = None
    else:
        resultado = None

    if resultado is None and categoria and normalizar_texto(categoria) in CATEGORIAS_CODIGO_TRABAJO:
        return "codigo_trabajo"
    return resultado


# ═══════════════════════════════════════════════════════════════════
#  NIVEL / ESTAMENTO
# ═══════════════════════════════════════════════════════════════════

_KEYWORDS_DIRECTIVO = (
    "director", "directora", "decano", "decana", "vicerrector",
    "gerente", "subgerente", "oficial", "secretario general",
    "subsecretario", "intendente", "gobernador",
)
_KEYWORDS_PROFESIONAL = (
    "jefe", "jefa", "coordinador", "coordinadora",
    "supervisor", "supervisora", "profesional",
    "cirujano", "médico", "medico", "enfermer",
    "psicólog", "psicolog", "ingenier", "abogad",
    "periodista", "químic", "quimic", "contador",
    "arquitect", "analista",
)
_KEYWORDS_TECNICO = (
    "técnico", "tecnico", "auxiliar", "administrativo", "administrativa",
    "operador", "operario", "conductor", "cociner", "garzón", "garzon",
    "peluquer", "telefonista", "secretaria", "asistente",
)


def detectar_nivel(cargo: str | None) -> str:
    """Infiere estamento/nivel desde el nombre del cargo → Title-case."""
    if not cargo:
        return "Profesional"
    c = cargo.lower()
    if any(w in c for w in _KEYWORDS_DIRECTIVO):
        return "Directivo"
    if any(w in c for w in _KEYWORDS_TECNICO):
        return "Técnico"
    if any(w in c for w in _KEYWORDS_PROFESIONAL):
        return "Profesional"
    return "Profesional"


# ═══════════════════════════════════════════════════════════════════
#  ÁREA PROFESIONAL
# ═══════════════════════════════════════════════════════════════════

_AREA_MAP: list[tuple[str, list[str]]] = [
    ("Derecho", ["abogad", "juridic", "jurídc", "legal", "fiscal"]),
    ("Salud", [
        "médic", "medic", "enfermer", "kinesiol", "matron", "salud",
        "psiquiatr", "farmac", "odontolog", "quimic", "químic",
    ]),
    ("Ingeniería", [
        "ingenier", "técnic", "tecnolog", "arquitect", "constructor",
        "desarrollador",
    ]),
    ("Ciencias Sociales", ["trabajador social", "asistente social"]),
    ("Psicología", ["psicolog", "psicólog"]),
    ("Finanzas", ["contador", "contabilidad", "auditor", "finanz"]),
    ("Economía", ["econom"]),
    ("TI", [
        "sistem", "informát", "informatic", "computaci", "software",
        "datos", "programador", "ti ",
    ]),
    ("Arquitectura/Diseño", ["diseñ"]),
    ("Educación", ["educad", "docent", "profesor", "pedagog"]),
    ("Comunicaciones", ["comunicacion", "comunicación", "periodist", "relacione"]),
    ("Agropecuario/Forestal", [
        "agrón", "agron", "veterinar", "forestal", "agropecuar",
    ]),
    ("Medioambiente", ["geolog", "geógraf", "ambiental", "ambient"]),
    ("Fiscalización", ["fiscaliz", "inspector"]),
    ("Social", ["social", "sociolog", "terapeuta"]),
    ("Administración", [
        "administr", "gestión", "gestion", "rrhh", "recursos humanos",
    ]),
]


def inferir_area_profesional(cargo: str | None) -> str:
    """Infiere área profesional desde el nombre del cargo → Title-case.

    Consolida db/database.py:normalizar_area, wordpress._inferir_area_profesional,
    generic_site._infer_area.  Retorna la unión de keywords de todas las fuentes
    con nombres de área normalizados con tildes.
    """
    if not cargo:
        return "Administración"
    c = cargo.lower()
    for area, keywords in _AREA_MAP:
        if any(kw in c for kw in keywords):
            return area
    return "Administración"


# ═══════════════════════════════════════════════════════════════════
#  CIUDAD
# ═══════════════════════════════════════════════════════════════════

_CIUDADES_CONOCIDAS = (
    "santiago", "providencia", "ñuñoa", "independencia",
    "estación central", "estacion central", "las condes", "maipú",
    "maipu", "la florida", "puente alto",
    "antofagasta", "valparaíso", "valparaiso", "viña del mar",
    "concepción", "concepcion", "temuco", "valdivia",
    "puerto montt", "punta arenas", "talca", "iquique",
    "arica", "coquimbo", "la serena", "rancagua", "chillán",
    "chillan", "osorno", "copiapó", "copiapo",
)

_PREFIJOS_MUNI = (
    "municipalidad de ",
    "i. municipalidad de ",
    "ilustre municipalidad de ",
    "corporación municipal de ",
    "corporacion municipal de ",
)


def inferir_ciudad_desde_texto(texto: str | None) -> str | None:
    """Busca una ciudad chilena conocida en el texto libre."""
    if not texto:
        return None
    t = texto.lower()
    for ciudad in _CIUDADES_CONOCIDAS:
        if ciudad in t:
            return ciudad.title()
    return None


def inferir_ciudad_desde_institucion(nombre_institucion: str | None) -> str | None:
    """Extrae la ciudad desde el nombre de una municipalidad/corporación."""
    if not nombre_institucion:
        return None
    text = nombre_institucion.strip()
    if not text:
        return None
    lower = text.lower()
    for prefijo in _PREFIJOS_MUNI:
        if lower.startswith(prefijo):
            return text[len(prefijo):].strip().title() or None
    return None


# ═══════════════════════════════════════════════════════════════════
#  REGIÓN
# ═══════════════════════════════════════════════════════════════════

_RM_CUES = (
    "santiago", "metropolitana", "providencia", "ñuñoa", "nunoa",
    "independencia", "estacion central", "estación central",
    "las condes", "maipu", "maipú", "la florida", "puente alto",
)


def detectar_region(texto: str | None) -> str | None:
    """Detecta región chilena desde texto libre (ubicación, dirección, etc.).

    Más agresiva que base.py:normalizar_region, porque también reconoce
    nombres de ciudades como pista de la RM (ej. "Santiago").
    Usa normalizar_region() de base.py como fallback.
    """
    if not texto:
        return None
    t = texto.lower()

    if any(x in t for x in _RM_CUES):
        return "Metropolitana de Santiago"

    for cue in ("arica", "tarapaca", "tarapacá", "antofagasta", "atacama",
                "coquimbo", "valparaiso", "valparaíso", "ohiggins",
                "maule", "nuble", "ñuble", "biobio", "biobío",
                "araucania", "araucanía", "los rios", "los ríos",
                "los lagos", "aysen", "aysén", "magallanes",
                "punta arenas", "talca", "temuco", "concepción",
                "concepcion", "valdivia", "puerto montt", "la serena",
                "iquique", "rancagua", "chillán", "chillan",
                "osorno", "copiapó", "copiapo"):
        if cue in t:
            region = normalizar_region(cue)
            if region:
                return region
            # Ciudades que no están en REGION_MAP pero pertenecen a una región
            city_to_region = {
                "punta arenas": "Magallanes",
                "talca": "Maule",
                "temuco": "La Araucanía",
                "concepción": "Biobío", "concepcion": "Biobío",
                "valdivia": "Los Ríos",
                "puerto montt": "Los Lagos",
                "la serena": "Coquimbo",
                "iquique": "Tarapacá",
                "rancagua": "O'Higgins",
                "chillán": "Ñuble", "chillan": "Ñuble",
                "osorno": "Los Lagos",
                "copiapó": "Atacama", "copiapo": "Atacama",
            }
            return city_to_region.get(cue)

    return normalizar_region(texto)


# ═══════════════════════════════════════════════════════════════════
#  VACANTES
# ═══════════════════════════════════════════════════════════════════

_RE_VACANTES = re.compile(
    r"(?:n[°º.]?\s*(?:de\s+)?)?vacantes?\s*[:\|]?\s*(\d{1,3})",
    re.I,
)
_RE_VACANTES_ALT = re.compile(
    r"(\d{1,3})\s+vacantes?",
    re.I,
)


def extraer_vacantes(texto: str | None) -> int | None:
    """Extrae número de vacantes del texto."""
    if not texto:
        return None
    m = _RE_VACANTES.search(texto)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 200:
            return n
    m = _RE_VACANTES_ALT.search(texto)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 200:
            return n
    return None


# ═══════════════════════════════════════════════════════════════════
#  JORNADA
# ═══════════════════════════════════════════════════════════════════

_RE_HORAS = re.compile(r"(\d{1,2})\s*(?:hrs?|horas?)\s*(?:semanales?)?", re.I)


def detectar_jornada(texto: str | None) -> str | None:
    """Detecta tipo de jornada: completa|parcial|None."""
    if not texto:
        return None
    t = texto.lower()
    if "jornada completa" in t or "44 horas" in t or "45 horas" in t:
        return "completa"
    if "media jornada" in t or "jornada parcial" in t or "22 horas" in t:
        return "parcial"
    m = _RE_HORAS.search(t)
    if m:
        horas = int(m.group(1))
        if horas >= 40:
            return "completa"
        if 10 <= horas < 40:
            return "parcial"
    return None


def extraer_horas_semanales(texto: str | None) -> int | None:
    """Extrae horas de jornada semanal (ej. '44 horas semanales' → 44)."""
    if not texto:
        return None
    m = _RE_HORAS.search(texto)
    if m:
        h = int(m.group(1))
        if 1 <= h <= 50:
            return h
    return None


# ═══════════════════════════════════════════════════════════════════
#  MODALIDAD DE TRABAJO
# ═══════════════════════════════════════════════════════════════════

def detectar_modalidad(texto: str | None) -> str | None:
    """Detecta modalidad: presencial|remota|hibrida|None."""
    if not texto:
        return None
    t = texto.lower()
    if "híbrid" in t or "hibrid" in t:
        return "hibrida"
    if "remot" in t or "teletrabajo" in t:
        return "remota"
    if "presencial" in t:
        return "presencial"
    return None
