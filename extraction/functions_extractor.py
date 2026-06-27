from __future__ import annotations

import re

SECTION_PATTERNS = [
    r"funciones(?:\s+del\s+cargo)?",
    r"principales funciones",
    r"objetivo del cargo",
    r"prop[óo]sito del cargo",
    r"descripci[óo]n del cargo",
    r"responsabilidades",
]

# Encabezados de OTRAS secciones: cortan la ventana de funciones para no
# arrastrar pasos de postulación, requisitos o documentos como si fueran
# funciones del cargo.
_STOP_HEADER_RE = re.compile(
    r"^\s*(?:c[oó]mo\s+postular|proceso\s+de\s+postulaci[oó]n|requisitos|"
    r"documentos|antecedentes|condiciones|beneficios|renta|remuneraci[oó]n|"
    r"etapas|calendario|formaci[oó]n|experiencia|competencias|perfil)\b",
    re.I)


def _es_encabezado_corte(linea: str) -> bool:
    """True si la línea parece el encabezado de otra sección (corta la ventana)."""
    if len(linea.split()) > 8:
        return False
    return linea.rstrip().endswith(":") or bool(_STOP_HEADER_RE.search(linea))


def extract_functions(text: str) -> list[str]:
    normalized = text.replace("\r", "\n")
    lines = [line.strip(" -*\t") for line in normalized.splitlines() if line.strip()]
    matches: list[str] = []
    for idx, line in enumerate(lines):
        if any(re.search(pattern, line, flags=re.IGNORECASE) for pattern in SECTION_PATTERNS):
            window: list[str] = []
            for w in lines[idx + 1 : idx + 9]:
                if _es_encabezado_corte(w):
                    break  # llegó otra sección → dejar de acumular funciones
                window.append(w)
            bullets = [w for w in window if len(w.split()) >= 2]
            matches.extend(bullets)
    seen = set()
    ordered = []
    for item in matches:
        if item.lower() not in seen:
            seen.add(item.lower())
            ordered.append(item)
    return ordered[:12]
