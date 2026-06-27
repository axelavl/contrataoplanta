"""Helpers compartidos de estructuración de texto para scrapers.

Centraliza dos heurísticas que antes vivían en ``scrapers/bcentral.py`` y que
ahora aplican a CUALQUIER scraper (vía ``scrapers/enrich.py``):

1. ``unir_lineas_envueltas`` — reúne renglones que un PDF/HTML parte por ancho
   de página al ítem (bullet/requisito) que continúan. Evita que un título como
   "Título profesional de Ingeniería…, o carrera profesional afín…" se trate
   como dos requisitos y se pierda la parte de los títulos.

2. ``extraer_pasos_postulacion`` — detecta por palabras/frases clave los pasos
   concretos para postular (formulario online, recepción de antecedentes,
   adjuntar CV, expectativas de renta, correo de dudas) y los devuelve limpios.

Sin dependencias del proyecto (solo ``re``), para poder importarse desde
extractores y tests sin tocar la BD.
"""
from __future__ import annotations

import re

__all__ = [
    "unir_lineas_envueltas",
    "extraer_pasos_postulacion",
    "RE_PASO_POSTULACION",
]


def _limpiar(texto: str | None) -> str:
    return re.sub(r"\s+", " ", texto or "").strip()


# ── 1. Reunir renglones envueltos ───────────────────────────────────────────
_RE_BULLET_INICIO = re.compile(
    r"^\s*(?:[-*•·▪◦‣∙]|[–—]|\(?\d{1,2}[.)]|[a-zA-Z]\))\s+")
_RE_ENCABEZADO_SECCION = re.compile(
    r"^\s*(?:los\s+|las\s+)?(?:requisitos|funciones|deseables?|documentos|"
    r"antecedentes|perfil|competencias|formaci[oó]n|experiencia|condiciones|"
    r"objetivo|conocimientos|habilidades|remuneraci[oó]n|renta|"
    r"c[oó]mo\s+postular|proceso\s+de\s+postulaci[oó]n)\b[^\n]{0,60}:?\s*$",
    re.I)


def unir_lineas_envueltas(texto: str) -> str:
    """Une renglones partidos por ancho de página al ítem que continúan.

    Conservador: solo fusiona cuando la línea previa NO termina como oración
    (sin '.'/':' final) y la actual es claramente una continuación —empieza en
    MINÚSCULA ("o carrera…", "y experiencia…", "a SAP S/4HANA")— o la previa
    terminó en coma/punto y coma. Una línea que empieza en mayúscula se trata
    como ítem/oración nueva (no funde pasos distintos). Nunca fusiona bullets
    nuevos ni encabezados de sección.
    """
    out: list[str] = []
    for cruda in (texto or "").replace("\r", "\n").split("\n"):
        s = cruda.strip()
        if not s:
            out.append("")
            continue
        es_bullet = bool(_RE_BULLET_INICIO.match(s))
        es_encab = bool(_RE_ENCABEZADO_SECCION.match(s))
        prev_idx = len(out) - 1
        while prev_idx >= 0 and not out[prev_idx].strip():
            prev_idx -= 1
        prev = out[prev_idx].rstrip() if prev_idx >= 0 else ""
        if prev and not es_bullet and not es_encab and not prev.endswith((".", ":")):
            continuacion = s[:1].islower() or prev.endswith((",", ";", "/"))
            if continuacion:
                out[prev_idx] = prev + " " + s
                continue
        out.append(cruda.rstrip())
    return "\n".join(out)


# ── 2. Pasos para postular (detección por palabras/frases clave) ─────────────
RE_PASO_POSTULACION = re.compile(
    r"(formulario\s+de\s+postulaci[oó]n"
    r"|recepci[oó]n\s+de\s+antecedentes"
    r"|adjunt\w*\s+(?:su\s+|el\s+|sus\s+)?(?:curr[ií]cul\w+|cv)"
    r"|expectativas?\s+de\s+renta"
    r"|completar\s+el\s+(?:siguiente\s+)?formulario"
    r"|dudas?\s+o\s+consultas?"
    r"|enviar\s+(?:sus\s+|los\s+)?antecedentes"
    r"|postular\s+(?:a\s+trav[eé]s|en\s+el\s+portal|en\s+l[ií]nea)"
    r"|hacer\s+llegar\s+(?:sus\s+)?antecedentes"
    r"|plazo\s+(?:de|para)\s+postulaci[oó]n)",
    re.I)


def extraer_pasos_postulacion(texto: str, max_pasos: int = 6) -> list[str]:
    """Extrae los pasos concretos para postular del texto de las bases.

    Trabaja por líneas (tras reunir las envueltas) porque los pasos suelen vivir
    en renglones propios. Devuelve hasta ``max_pasos`` pasos limpios y sin
    duplicados.
    """
    if not texto:
        return []
    pasos: list[str] = []
    vistos: set[str] = set()
    for cruda in unir_lineas_envueltas(texto).split("\n"):
        s = _limpiar(cruda)
        s = re.sub(r"^[-*•·▪◦–—]\s+", "", s).strip()  # quita viñeta inicial
        if not (12 <= len(s) <= 240):
            continue
        if not RE_PASO_POSTULACION.search(s):
            continue
        clave = s.lower()
        if clave in vistos:
            continue
        vistos.add(clave)
        pasos.append(s.rstrip(" .") + ".")
        if len(pasos) >= max_pasos:
            break
    return pasos
