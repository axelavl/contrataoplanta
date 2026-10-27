"""Área profesional de una oferta (columna `ofertas.area_profesional`).

Reemplaza a los dos clasificadores anteriores (`db.database.normalizar_area`
y `EmpleosPublicosScraper._inferir_area_profesional`), que buscaban
fragmentos sueltos en el cargo y caían en "Administración" por defecto:

* "Base de **Datos** de reemplazo… asistente de párvulos" → "TI"
* "Ingeniero/a en Automatización y Sistemas de **Medic**ión" → "Salud"
* "TENS UCI" (sin palabra reconocida) → "Administración" por defecto

Ahora:

1. Se detecta la PROFESIÓN con el catálogo de `classification.profesiones`
   (el mismo del matching de CV), primero en el cargo y, si no hay, en lo
   que los requisitos dicen de título/carrera.
2. Si no hay profesión, palabras clave con límite de palabra (no
   fragmentos) sobre el cargo.
3. Si nada calza, `None` ("Sin clasificar"). Un área inventada es peor que
   ninguna: alimenta filtros, estadísticas y recomendaciones.

Las etiquetas son las que ya usaba el sitio (API, panel de mercado, cursos).
"""
from __future__ import annotations

import re
import unicodedata

from classification import profesiones as PROF

#: Profesión del catálogo → etiqueta de área del sitio.
AREA_POR_PROFESION: dict[str, str] = {
    "psiquiatra": "Salud", "tecnologo_medico": "Salud", "tens": "Salud",
    "instrumentista": "Salud", "enfermera": "Salud", "matrona": "Salud",
    "medico": "Salud", "kinesiologo": "Salud", "nutricionista": "Salud",
    "fonoaudiologo": "Salud", "terapeuta_ocupacional": "Salud",
    "quimico_farmaceutico": "Salud", "odontologo": "Salud",
    "psicologo": "Psicología",
    "auxiliar_servicio": "Servicios Generales",
    "educadora_parvulos": "Educación", "tecnico_parvulos": "Educación",
    "profesor": "Educación",
    "ing_electrico": "Ingeniería", "tec_electrico": "Ingeniería",
    "ing_industrial": "Ingeniería", "ing_civil": "Ingeniería",
    "prevencionista": "Ingeniería", "laboratorio": "Ingeniería",
    "informatica": "TI",
    "biologia": "Ciencias", "ambiental": "Medioambiente",
    "guardaparque": "Medioambiente",
    "veterinario": "Agropecuario/Forestal", "agronomo": "Agropecuario/Forestal",
    "tec_contabilidad": "Finanzas", "contador": "Finanzas",
    "administracion": "Administración", "tec_administracion": "Administración",
    "abogado": "Derecho", "tecnico_juridico": "Derecho",
    "trabajador_social": "Ciencias Sociales", "sociologo": "Ciencias Sociales",
    "periodista": "Comunicaciones", "disenador": "Arquitectura/Diseño",
}

AREAS = tuple(sorted(set(AREA_POR_PROFESION.values()) | {
    "Economía", "Fiscalización", "Agropecuario/Forestal"}))

# Respaldo por palabras clave, siempre con límite de palabra al inicio
# (`\b`), para no calzar dentro de otra palabra ("medic" en "medición").
_RESPALDO: tuple[tuple[str, re.Pattern], ...] = tuple(
    (area, re.compile(rx)) for area, rx in (
        ("Fiscalización", r"\bfiscaliz\w*|\binspector\w*"),
        ("Derecho", r"\bjuridic\w*|\blegal(es)?\b|\bnotari\w*|"
                    r"\bfiscal(es)? (?:regional|adjunt\w*|nacional|jefe)"),
        ("Salud", r"\bsalud\b|\bhospital\w*|\bclinic[oa]s?\b|\bpaciente\w*|\bcesfam\b"),
        ("Educación", r"\bacademic[oa]s?\b|\beducativ\w*|\bescolar\w*|\bjardin infantil\b"),
        ("TI", r"\b(?:analista|ciencia|cientific[oa]|ingenier[oa]) de datos\b|"
               r"\badministrador\w* de bases? de datos\b|\bdba\b|\bti\b|\bsoftware\b|\bciberseguridad\b|\bdesarroll\w* web\b|\bredes informatic\w*"),
        ("Economía", r"\beconom\w*"),
        ("Finanzas", r"\bfinanz\w*|\btesorer\w*|\bpresupuest\w*|\bremuneracion\w*"),
        ("Agropecuario/Forestal", r"\bagronom\w*|\bveterinari\w*|\bforestal\w*|\bagropecuari\w*|\bagricol\w*"),
        ("Medioambiente", r"\bambiental\w*|\bmedioambient\w*|\bgeolog\w*"),
        ("Comunicaciones", r"\bcomunicacion\w*|\bprensa\b|\bredes sociales\b|\bdifusion\b"),
        ("Administración", r"\badministracion\b|\bgestion\b|\brecursos humanos\b|\brrhh\b|"
                           r"\bgestion de personas\b|\babastecimiento\b|\badquisicion\w*|"
                           r"\bcompras\b|\bsecretari\w*|\boficina de partes\b"),
        ("Servicios Generales", r"\bconductor\w*|\bchofer\w*|\bjunior\b|\bmayordom\w*|"
                                r"\bguardia\w*|\bvigilante\w*|\bportero\w*|\bmantencion\b"),
    )
)

_RE_DIACRITICOS = re.compile(r"[̀-ͯ]")
_RE_FORMACION = re.compile(r"titul|carrera|formacion|estudios|profesion|egresad")
_RE_FIN_FRASE = re.compile(r"[a-z]{5,}\.\s")


def _norm(s: str | None) -> str:
    s = _RE_DIACRITICOS.sub("", unicodedata.normalize("NFD", (s or "").lower()))
    return re.sub(r"\s+", " ", s).strip()


def _formacion(req: str) -> str:
    """Fragmentos de los requisitos que hablan de título/carrera."""
    partes = []
    for m in _RE_FORMACION.finditer(req):
        fin = m.start() + 260
        if f := _RE_FIN_FRASE.search(req, m.start(), fin):
            fin = f.end()
        partes.append(req[m.start():fin])
    return " ".join(partes)


def clasificar_area(cargo: str | None, requisitos: str | None = None) -> str | None:
    """Área profesional de la oferta, o None si no se puede determinar."""
    c = _norm(cargo)
    if not c:
        return None
    if prof := PROF.principal(c):
        return AREA_POR_PROFESION.get(prof)
    req = _norm(requisitos)[:4000]
    if req:
        # Si las bases aceptan varias carreras, manda el área más repetida
        # (empate: la primera mencionada). "Tecnología Médica, Biotecnología,
        # Biología o Ingeniería en Biotecnología" → Ciencias, no Salud.
        votos: dict[str, int] = {}
        for _, clave in PROF.detectar(_formacion(req)):
            if area := AREA_POR_PROFESION.get(clave):
                votos[area] = votos.get(area, 0) + 1
        if votos:
            return max(votos, key=votos.get)   # dict conserva orden: empate → primera
    for area, patron in _RESPALDO:
        if patron.search(c):
            return area
    return None
