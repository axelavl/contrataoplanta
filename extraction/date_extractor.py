from __future__ import annotations

import re
from datetime import datetime, timezone

from dateutil import parser

from models.date_models import DateEvidence, DateResolution


def _safe_parse(fragment: str) -> datetime | None:
    try:
        dt = parser.parse(fragment, dayfirst=True, fuzzy=True, default=datetime.now())
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def extract_dates_from_text(text: str) -> list[DateEvidence]:
    evidences: list[DateEvidence] = []
    source = text or ""
    patterns = {
        "application_end": r"(?:hasta el|cierre de postulaci[oó]n|plazo de postulaci[oó]n|recepci[oó]n de antecedentes hasta|postulaciones hasta)\s*:?\s*[^\n\.]{0,60}",
        "application_start": r"(?:desde el|inicio de postulaci[oó]n|apertura de postulaci[oó]n|recepci[oó]n de antecedentes desde)\s*:?\s*[^\n\.]{0,60}",
        "published": r"(?:publicad[oa]|fecha de publicaci[oó]n|publicaci[oó]n)\s*:?\s*[^\n\.]{0,60}",
    }
    for label, pattern in patterns.items():
        for chunk in re.findall(pattern, source, flags=re.IGNORECASE):
            parsed = _safe_parse(chunk)
            evidences.append(
                DateEvidence(
                    label=label,
                    raw_text=chunk,
                    value=parsed,
                    confidence=0.85 if parsed else 0.35,
                    source="html_text",
                )
            )
    return evidences


def extract_dates_from_tables(tables_text: list[str]) -> list[DateEvidence]:
    joined = "\n".join(tables_text or [])
    out = extract_dates_from_text(joined)
    for evidence in out:
        evidence.source = "tables"
        evidence.confidence = max(evidence.confidence, 0.9)
    return out


def extract_dates_from_attachments(attachment_texts: list[str]) -> list[DateEvidence]:
    joined = "\n".join(attachment_texts or [])
    out = extract_dates_from_text(joined)
    for evidence in out:
        evidence.source = "attachments"
        evidence.confidence = max(evidence.confidence, 0.8)
    return out


def resolve_best_dates(evidences: list[DateEvidence], now: datetime | None = None) -> DateResolution:
    now = now or datetime.now(timezone.utc)
    published = _pick_best(evidences, "published")
    start = _pick_best(evidences, "application_start")
    end = _pick_best(evidences, "application_end")

    is_expired = None
    expiration_reason = None
    confidence = "low"
    if end and end.value:
        is_expired = end.value < now
        expiration_reason = "application_end_at_in_past" if is_expired else None
        confidence = "high"

    texts = " ".join(e.raw_text.lower() for e in evidences)
    if re.search(r"proceso finalizado|cerrado|adjudicado|n[oó]mina de seleccionados", texts):
        is_expired = True
        expiration_reason = "explicit_closed_signal"
        confidence = "high"

    if re.search(r"\b(2020|2021|2022|2023|2024)\b", texts) and not re.search(r"\b2025|2026|vigente|abierto|en proceso\b", texts):
        is_expired = True
        expiration_reason = "historical_year_without_current_signals"
        confidence = "medium"

    return DateResolution(
        published_at=published.value if published else None,
        application_start_at=start.value if start else None,
        application_end_at=end.value if end else None,
        is_expired=is_expired,
        expiration_reason=expiration_reason,
        date_confidence=confidence,
        evidence=evidences,
    )


def _pick_best(evidences: list[DateEvidence], label: str) -> DateEvidence | None:
    options = [e for e in evidences if e.label == label and e.value]
    if not options:
        return None
    return sorted(options, key=lambda e: e.confidence, reverse=True)[0]
