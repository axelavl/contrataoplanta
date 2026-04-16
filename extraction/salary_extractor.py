from __future__ import annotations

import re


def extract_salary(text: str) -> tuple[float | None, str | None, str | None]:
    pattern = re.compile(r"((?:\$|clp|pesos?)\s*[\d\.,]{4,})", flags=re.IGNORECASE)
    match = pattern.search(text or "")
    if not match:
        return None, None, None

    raw = match.group(1)
    number = re.sub(r"[^\d]", "", raw)
    amount = float(number) if number else None
    currency = "CLP" if re.search(r"\$|clp|peso", raw, re.IGNORECASE) else None
    return amount, currency, raw
