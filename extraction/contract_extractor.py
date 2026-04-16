from __future__ import annotations

import re


CONTRACT_PATTERNS = {
    "honorarios": r"honorarios?",
    "contrata": r"contrata",
    "planta": r"planta",
    "codigo_trabajo": r"c[oó]digo del trabajo",
    "plazo_fijo": r"plazo fijo",
    "reemplazo": r"reemplazo",
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
