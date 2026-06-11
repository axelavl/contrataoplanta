from __future__ import annotations

import re


# Patrones explícitos de calidad contractual. Requieren contexto o límites
# de palabra para evitar falsos positivos: "contratación"/"se contrata" no es
# "contrata"; "planta industrial"/"planta de tratamiento" no es "planta".
# Relevante para empresas del estado (Código del Trabajo), cuyos avisos
# mencionan plantas físicas o el verbo contratar sin declarar calidad jurídica.
CONTRACT_PATTERNS = {
    "honorarios": r"\bhonorarios?\b",
    "codigo_trabajo": r"\bc[oó]d(?:igo|\.)?\s+(?:del\s+)?trabajo\b",
    "contrata": (
        r"(?:"
        r"\ba\s+contrata\b"
        r"|\bcalidad\s+(?:jur[ií]dica|contractual)?\s*:?\s*(?:de\s+|a\s+|la\s+)?contrata\b"
        r"|\b(?:v[ií]nculo|estamento|condici[oó]n)\s*:?\s*(?:a\s+|de\s+|la\s+)?contrata\b"
        r"|\btipo\s+de\s+(?:contrato|cargo|vacante|v[ií]nculo)\s*:?\s*contrata\b"
        r"|\bcontrata\s+asimilad"
        r"|\bcontrata\s+grado\b"
        r")"
    ),
    "planta": (
        r"(?:"
        r"\b(?:cargos?|empleos?|titular)\s+(?:de\s+|a\s+|en\s+(?:la\s+)?)planta\b"
        r"|\bcalidad\s+(?:jur[ií]dica|contractual)?\s*:?\s*(?:de\s+|la\s+)?planta\b"
        r"|\b(?:v[ií]nculo|estamento|condici[oó]n)\s*:?\s*(?:de\s+|la\s+)?planta\b"
        r"|\btipo\s+de\s+(?:contrato|cargo|vacante|v[ií]nculo)\s*:?\s*planta\b"
        r"|\bplanta\s+(?:directiv|profesional|t[eé]cnic|administrativ|auxiliar|fiscalizador|municipal|titular)"
        r"|\ba\s+planta\b"
        r")"
    ),
    "plazo_fijo": r"\bplazo\s+fijo\b",
    "reemplazo": r"\b(?:reemplazo|suplencia)\b",
}


def extract_contract_info(text: str) -> tuple[str | None, str | None, str | None]:
    source = text or ""
    contract = None
    for label, pattern in CONTRACT_PATTERNS.items():
        if re.search(pattern, source, re.IGNORECASE):
            contract = label
            break

    workday = None
    if re.search(r"jornada completa", source, re.IGNORECASE):
        workday = "completa"
    elif re.search(r"media jornada|jornada parcial", source, re.IGNORECASE):
        workday = "parcial"

    modality = None
    if re.search(r"h[ií]brid", source, re.IGNORECASE):
        modality = "hibrida"
    elif re.search(r"remot|teletrabajo", source, re.IGNORECASE):
        modality = "remota"
    elif re.search(r"presencial", source, re.IGNORECASE):
        modality = "presencial"

    return contract, workday, modality
