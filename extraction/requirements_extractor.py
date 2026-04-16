from __future__ import annotations

import re

REQ_PATTERNS = [r"requisitos(?: del cargo)?", r"requisitos exigibles"]
DESIRABLE_PATTERNS = [r"requisitos deseables", r"deseable", r"plus"]
DOCUMENT_PATTERNS = [r"cv", r"certificad", r"t[ií]tulo", r"anexo", r"declaraci[oó]n jurada", r"c[eé]dula"]


def _extract_section(lines: list[str], patterns: list[str], limit: int = 10) -> list[str]:
    result: list[str] = []
    for idx, line in enumerate(lines):
        if any(re.search(p, line, re.IGNORECASE) for p in patterns):
            for next_line in lines[idx + 1 : idx + 1 + limit]:
                if re.match(r"^[A-ZÁÉÍÓÚ].{0,40}:$", next_line):
                    break
                if len(next_line.split()) >= 2:
                    result.append(next_line.strip(" -*\t"))
    dedup = []
    seen = set()
    for item in result:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            dedup.append(item)
    return dedup


def extract_requirements(text: str) -> tuple[list[str], list[str], list[str]]:
    lines = [line.strip() for line in text.replace("\r", "\n").splitlines() if line.strip()]
    required = _extract_section(lines, REQ_PATTERNS)
    desirable = _extract_section(lines, DESIRABLE_PATTERNS)

    documents = [line for line in lines if any(re.search(p, line, re.IGNORECASE) for p in DOCUMENT_PATTERNS)]
    return required[:12], desirable[:10], documents[:10]
