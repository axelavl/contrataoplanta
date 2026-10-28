"""Áreas de trabajo (funciones) para el matching CV ↔ oferta.

Por qué existe
--------------
Muchas ofertas públicas NO piden una profesión concreta: piden un área de
trabajo ("Analista de Datos — Estudios", "Jefatura Unidad de Personal",
"Departamento de Gestión y Desarrollo de las Personas") y requisitos legales
generales (DS 69/04). Comparar solo por profesión dejaba fuera a quien tiene
justamente esa experiencia. Esta segunda dimensión detecta la función en el
CV (experiencia, habilidades) y en el cargo de la oferta.

Los patrones se aplican sobre texto normalizado (minúsculas, sin tildes).
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Funcion:
    clave: str
    etiqueta: str
    patron: re.Pattern


def _f(clave: str, etiqueta: str, rx: str) -> Funcion:
    return Funcion(clave, etiqueta, re.compile(rx))


FUNCIONES: tuple[Funcion, ...] = (
    _f("gestion_personas", "Gestión de personas / RR.HH.",
       r"gestion y desarrollo de (?:las )?personas|gestion de (?:las )?personas|"
       r"desarrollo de (?:las )?personas|recursos humanos|\brr\.? ?hh\b|"
       r"(?:unidad|oficina|departamento|seccion|jef\w*|encargad\w*|administracion) de personal\b|"
       r"desarrollo organizacional|reclutamiento|seleccion de personal|"
       r"remuneraciones|clima (?:laboral|organizacional)|evaluacion del (?:desempeno|personal)|"
       r"calificaciones del personal|\bsiaper\b|carrera funcionaria|ascensos|"
       r"concursos? (?:publicos? )?de (?:ingreso|personal|academicos)|bienestar del personal"),
    _f("datos_estudios", "Análisis de datos y estudios",
       r"analisis de datos|analista de datos|ciencia de datos|cientific\w* de datos|"
       r"power ?bi|\btableau\b|\bsql\b|business intelligence|estadistic\w*|"
       r"(?:departamento|unidad|area|division) de estudios|estudios y desarrollo|"
       r"estudios, monitoreo|monitoreo y datos|reportes? (?:estrategicos|de gestion)|dashboards?"),
    _f("control_gestion", "Control de gestión y planificación",
       r"control de gestion|planificacion(?: estrategica)?|mejora continua|"
       r"optimizacion de procesos|gestion de procesos|levantamiento de procesos|"
       r"seguimiento de (?:metas|compromisos|indicadores)|indicadores de gestion|"
       r"evaluacion de (?:programas|proyectos|politicas)"),
    _f("proyectos", "Gestión de proyectos",
       r"gestion de proyectos|jef\w* de proyectos?|formulacion de proyectos|"
       r"cartera de proyectos|unidad de proyectos|project manag\w*"),
    _f("compras", "Compras y abastecimiento",
       r"compras publicas|\bcompras\b|adquisiciones|abastecimiento|licitacion\w*|"
       r"mercado publico|chilecompra|gestion de contratos"),
    _f("finanzas", "Finanzas, presupuesto y contabilidad",
       r"contabilidad|tesoreria|\bfinanzas\b|presupuest\w*|\bsigfe\b|rendicion de cuentas|"
       r"administracion y finanzas"),
    _f("juridico", "Asesoría jurídica",
       r"asesoria juridica|unidad juridica|fiscalia administrativa|sumarios? administrativos?|"
       r"investigaciones sumarias"),
    _f("comunicaciones", "Comunicaciones",
       r"\bcomunicaciones\b|\bprensa\b|redes sociales|difusion|contenidos digitales|community manag\w*"),
    _f("atencion_ciudadana", "Atención y participación ciudadana",
       r"atencion (?:ciudadana|de publico|a (?:usuarios|personas usuarias))|\boirs\b|"
       r"participacion ciudadana|transparencia|relacion con la ciudadania"),
)

POR_CLAVE = {f.clave: f for f in FUNCIONES}


def detectar(texto_norm: str) -> dict[str, int]:
    """{clave: número de menciones} de cada función presente en el texto."""
    conteo: dict[str, int] = {}
    for f in FUNCIONES:
        n = len(f.patron.findall(texto_norm))
        if n:
            conteo[f.clave] = n
    return conteo


def del_cv(texto_norm: str, tope: int = 3) -> list[str]:
    """Funciones principales de un CV: las más mencionadas (≥2 menciones),
    para que una línea suelta no defina el perfil."""
    conteo = detectar(texto_norm)
    ordenadas = sorted(conteo.items(), key=lambda kv: -kv[1])
    return [k for k, n in ordenadas if n >= 2][:tope]


def del_cargo(cargo_norm: str) -> list[str]:
    """Funciones que nombra el cargo de una oferta (una mención basta)."""
    return list(detectar(cargo_norm))
