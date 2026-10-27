"""Catálogo de profesiones: matching CV ↔ oferta y área profesional de las ofertas.

Por qué existe
--------------
Comparar por "área" (Salud, Educación…) es demasiado grueso: una enfermera,
un TENS y un médico son todos "Salud", pero no pueden postular a los cargos
de los otros. Aquí se reconoce la PROFESIÓN concreta, tanto en el CV como en
el cargo y los requisitos de la oferta.

Cómo detecta
------------
Los patrones se aplican sobre texto normalizado (minúsculas, sin tildes) y en
orden de especificidad. Cada coincidencia se "consume" (se borra del texto)
para que un patrón general no la vuelva a contar: "técnico en enfermería"
queda como TENS y ya no dispara "enfermera"; "tecnólogo médico" no dispara
"médico". Antes se consumen frases-ruido como "servicio médico quirúrgico".

`campo` agrupa profesiones afines. En campos no regulados (electricidad,
administración…) un cargo de la misma familia cuenta como "relacionado"; en
salud, educación y derecho, donde el título habilita o no, la coincidencia
tiene que ser exacta.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Profesion:
    clave: str
    etiqueta: str
    area: str
    nivel: str          # "Profesional" | "Técnico" | "Operativo"
    campo: str
    patron: re.Pattern


def _p(clave, etiqueta, area, nivel, campo, regex) -> Profesion:
    return Profesion(clave, etiqueta, area, nivel, campo, re.compile(regex))


# Frases que contienen nombres de profesión sin referirse a una.
_RUIDO = re.compile(
    r"servicio medico|medico[- ]quirurgic\w*|licencias? medicas?|examen(es)? medic\w*|"
    r"seguro medic\w*|centro medico|alta medica|"
    r"primeros auxilios psicologic\w*|evaluacion psicologica|"
    r"educacion (superior|basica|media)|ministerio de educacion|"
    r"institucion de educacion|ensenanza media|"
    r"ingenieria en (el |la )?(area|unidad|departamento)|area ingenieria"
)

_RE_GENERO = re.compile(r"(\w)(?:/(?:a|o|as|os)|\((?:a|o|as|os)\))(?=\W|$)")

_T = r"tecnic[oa]s?\s+(?:de\s+|en\s+)?(?:nivel\s+(?:superior|medio)\s+)?(?:en\s+|de\s+)?"

# Orden = prioridad: lo específico primero.
PROFESIONES: tuple[Profesion, ...] = (
    # ── Salud ────────────────────────────────────────────────────────────
    _p("psiquiatra", "Psiquiatría", "Salud", "Profesional", "salud",
       r"psiquiatr[ao]s?\b|psiquiatria"),
    _p("tecnologo_medico", "Tecnología médica", "Salud", "Profesional", "salud",
       r"tecnolog[oa]s? medic[oa]s?|tecnologia medica"),
    _p("tens", "Técnico en enfermería (TENS)", "Salud", "Técnico", "salud",
       r"\btens\b|" + _T + r"enfermeria(?:\s+(?:de\s+)?nivel\s+superior)?|"
       r"tecnic[oa]s? paramedic\w*|auxiliar(es)? paramedic\w*"),
    _p("instrumentista", "Instrumentación quirúrgica", "Salud", "Técnico", "salud",
       r"instrumentista\w*|instrumentacion quirurgica"),
    _p("enfermera", "Enfermería", "Salud", "Profesional", "salud",
       r"\benfermer[oa]s?\b|licenciad[oa]s? en enfermeria|licenciatura en enfermeria|"
       r"carrera de enfermeria|titulo (profesional )?de enfermeria|\beu\b"),
    _p("matrona", "Obstetricia (matrona/ón)", "Salud", "Profesional", "salud",
       r"\bmatron[ao]s?\b|obstetricia y puericultura"),
    _p("veterinario", "Medicina veterinaria", "Agropecuario", "Profesional", "agro",
       r"medic[oa]s? veterinari\w*|veterinari[oa]s?\b|medicina veterinaria"),
    _p("agronomo", "Agronomía / forestal", "Agropecuario", "Profesional", "agro",
       r"ingenier\w* (?:agronom\w*|forestal\w*)|\bagronom[oa]s?\b|tecnic[oa]s? (?:agricol|forestal|agropecuari)\w*"),
    _p("medico", "Medicina", "Salud", "Profesional", "salud",
       r"\bmedic[oa]s?\b(?:\s+cirujan[oa]s?)?|medicina general|\beunacom\b"),
    _p("kinesiologo", "Kinesiología", "Salud", "Profesional", "salud",
       r"kinesiolog\w*"),
    _p("psicologo", "Psicología", "Salud", "Profesional", "salud",
       r"\bpsicolog[oa]s?\b|\bpsicologia\b"),
    _p("nutricionista", "Nutrición", "Salud", "Profesional", "salud",
       r"nutricionist\w*|nutricion y dietetica"),
    _p("fonoaudiologo", "Fonoaudiología", "Salud", "Profesional", "salud",
       r"fonoaudiolog\w*"),
    _p("terapeuta_ocupacional", "Terapia ocupacional", "Salud", "Profesional", "salud",
       r"terap\w+ ocupacional"),
    _p("quimico_farmaceutico", "Química y farmacia", "Salud", "Profesional", "salud",
       r"quimic[oa]s? farmaceutic\w*|quimica y farmacia"),
    _p("odontologo", "Odontología", "Salud", "Profesional", "salud",
       r"odontolog\w*|cirujan[oa]s? dentista|\bdentista"),
    _p("auxiliar_servicio", "Auxiliar de servicio", "Servicios generales", "Operativo",
       "servicios", r"auxiliar(?:es)? de (?:servicios?|aseo)|personal de aseo"),

    # ── Educación ────────────────────────────────────────────────────────
    _p("educadora_parvulos", "Educación de párvulos", "Educación", "Profesional",
       "educacion", r"educador[ae]?s? (?:de )?parvulos|educacion parvularia"),
    _p("tecnico_parvulos", "Técnico/asistente de párvulos", "Educación", "Técnico",
       "educacion",
       r"(?:" + _T + r"|asistentes?\s+(?:de\s+)?)(?:atencion\s+(?:de\s+)?)?parvulos|"
       r"asist\.? de parvulos|tardes de juego|\btjmj\b|asistentes? de aula"),
    _p("profesor", "Pedagogía / docencia", "Educación", "Profesional", "educacion",
       r"\bprofesor[ae]?s?\b|pedagog\w*|\bdocentes?\b"),

    # ── Ingeniería, técnica y ciencias ───────────────────────────────────
    _p("ing_electrico", "Ingeniería eléctrica / electrónica / automatización",
       "Ingeniería y Construcción", "Profesional", "electricidad",
       r"ingenier\w*\s+(?:civil\s+)?(?:en\s+)?(?:electric\w*|electronic\w*|"
       r"automatizacion\w*|instrumentacion|electricidad|mantenimiento industrial)"),
    _p("tec_electrico", "Técnico eléctrico", "Ingeniería y Construcción", "Técnico",
       "electricidad",
       _T + r"(?:electricidad|electric\w*|electromecanic\w*)|electricistas?\b|"
       r"instalador(?:es)? electric\w*|instalaciones electricas"),
    _p("ing_industrial", "Ingeniería civil industrial", "Ingeniería y Construcción",
       "Profesional", "gestion", r"ingenier\w* civil industrial"),
    _p("informatica", "Informática / TI", "Informática", "Técnico", "informatica",
       r"ingenier\w*\s+(?:civil\s+)?(?:en\s+)?(?:informatic\w*|computacion|sistemas)|"
       + _T + r"(?:informatic\w*|computacion|redes|mantencion red\w*|"
       r"administracion de redes|soporte)|analistas? programador\w*|"
       r"desarrollador\w*|programador\w*|soporte (?:ti|tecnico|computacional)|"
       r"mesa de (?:servicio|ayuda)|departamento (?:de )?ti\b|analistas? ti\b"),
    _p("ing_civil", "Ingeniería civil / construcción", "Ingeniería y Construcción",
       "Profesional", "construccion",
       r"ingenier\w* civil(?: en obras civiles)?\b|constructor(?:a)? civil|"
       r"ingenieria en construccion|arquitect[oa]s?\b"),
    _p("prevencionista", "Prevención de riesgos", "Ingeniería y Construcción",
       "Profesional", "construccion", r"prevencion de riesgos|prevencionista"),
    _p("laboratorio", "Laboratorio / ensayos", "Ingeniería y Construcción", "Técnico",
       "laboratorio", r"laboratorista\w*|" + _T + r"laboratorio|ayudantes? de laboratorio"),
    _p("biologia", "Biología / biotecnología", "Ciencias", "Profesional", "ciencias",
       r"\bbiolog[oa]s?\b|biologia|biotecnolog\w*|bioquimic\w*"),
    _p("ambiental", "Medio ambiente", "Medio ambiente", "Profesional", "ambiente",
       r"ingenieria ambiental|ciencias ambientales|educacion ambiental|"
       r"gestion ambiental|ecolog\w*|geograf\w*"),
    _p("guardaparque", "Guardaparque", "Medio ambiente", "Operativo", "ambiente",
       r"guardaparques?|guardabosques?"),

    # ── Administración, finanzas y derecho ───────────────────────────────
    _p("tec_contabilidad", "Técnico/a en contabilidad", "Administración y Finanzas",
       "Técnico", "gestion", _T + r"(?:contabilidad|contable)\w*|asistentes? contable\w*"),
    _p("contador", "Contabilidad / auditoría", "Administración y Finanzas",
       "Profesional", "gestion",
       r"contador(?:a|es)? (?:auditor|public)\w*|contador(?:a)?\b|contabilidad|auditor(?:a|es)?\b"),
    _p("administracion", "Administración / ingeniería comercial",
       "Administración y Finanzas", "Profesional", "gestion",
       r"ingenier\w* comercial|ingenieria en (?:administracion|control de gestion|marketing)|"
       r"administra(?:cion|dor[a]?) public[oa]|administracion de empresas|"
       r"control de gestion"),
    _p("tec_administracion", "Técnico/a en administración", "Administración y Finanzas",
       "Técnico", "gestion",
       _T + r"(?:administracion|gestion comercial|marketing|contabilidad|logistica)|"
       r"(?<!gestion )\badministrativ[oa]s?\b|secretari[oa]s?\b"),
    _p("abogado", "Derecho (abogacía)", "Jurídica", "Profesional", "juridica",
       r"\babogad[oa]s?\b|licenciad[oa]s? en (?:ciencias juridicas|derecho)"),
    _p("tecnico_juridico", "Técnico jurídico", "Jurídica", "Técnico", "juridica",
       r"tecnic[oa]s? juridic\w*|asistentes? juridic\w*|procurador\w*"),

    # ── Social y comunicaciones ──────────────────────────────────────────
    _p("trabajador_social", "Trabajo social", "Social y Comunitaria", "Profesional",
       "social", r"trabajador(?:a)?(?:es)? social(?:es)?|asistentes? social(?:es)?|trabajo social"),
    _p("sociologo", "Sociología", "Social y Comunitaria", "Profesional", "social",
       r"sociolog\w*|antropolog\w*"),
    _p("periodista", "Periodismo / comunicaciones", "Comunicaciones", "Profesional",
       "comunicaciones", r"periodis\w*|comunicador\w* audiovisual|relacionador\w* public\w*"),
    _p("disenador", "Diseño", "Comunicaciones", "Profesional", "comunicaciones",
       r"disenador\w*|diseno grafico"),
)

POR_CLAVE = {p.clave: p for p in PROFESIONES}

_ORDEN_NIVEL = {"Operativo": 0, "Técnico": 1, "Profesional": 2}


def puede_ejercer(propia: str, requerida: str) -> bool:
    """Un perfil de nivel igual o superior puede postular a un cargo afín
    (un ingeniero a un cargo técnico); al revés no (técnico → ingeniero)."""
    return _ORDEN_NIVEL[POR_CLAVE[propia].nivel] >= _ORDEN_NIVEL[POR_CLAVE[requerida].nivel]


#: Campos donde el título habilita (o no) para ejercer: la coincidencia de
#: profesión tiene que ser exacta. Fuera de ellos, misma familia = relacionado.
CAMPOS_ESTRICTOS = {"salud", "educacion", "juridica", "social"}


def detectar(texto_norm: str) -> list[tuple[int, str]]:
    """[(posición, clave)] de las profesiones mencionadas, ordenado por posición."""
    # "ingeniero/a", "técnico(a)" → forma base, para que los patrones calcen.
    t = _RE_GENERO.sub(r"\1", texto_norm)
    t = _RUIDO.sub(lambda m: " " * len(m.group(0)), t)
    hallazgos: list[tuple[int, str]] = []
    for p in PROFESIONES:
        def _consumir(m: re.Match, clave=p.clave) -> str:
            hallazgos.append((m.start(), clave))
            return " " * len(m.group(0))
        t = p.patron.sub(_consumir, t)
    hallazgos.sort()
    return hallazgos


def principal(texto_norm: str) -> str | None:
    """La primera profesión que aparece en el texto (p. ej. en un cargo o
    una línea del CV: "Enfermera supervisora de TENS" → enfermera)."""
    h = detectar(texto_norm)
    return h[0][1] if h else None


def todas(texto_norm: str) -> list[str]:
    vistas: list[str] = []
    for _, c in detectar(texto_norm):
        if c not in vistas:
            vistas.append(c)
    return vistas
