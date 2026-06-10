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


def extract_functions(text: str) -> list[str]:
    normalized = text.replace("\r", "\n")
    lines = [line.strip(" -*\t") for line in normalized.splitlines() if line.strip()]
    matches: list[str] = []
    for idx, line in enumerate(lines):
        if any(re.search(pattern, line, flags=re.IGNORECASE) for pattern in SECTION_PATTERNS):
            window = lines[idx + 1 : idx + 8]
            bullets = [w for w in window if len(w.split()) >= 2]
            matches.extend(bullets)
    seen = set()
    ordered = []
    for item in matches:
        if item.lower() not in seen:
            seen.add(item.lower())
            ordered.append(item)
    return ordered[:12]
