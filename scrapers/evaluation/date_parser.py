from __future__ import annotations

import json
import re
import unicodedata
from datetime import date, datetime
from typing import Any, Iterable

from bs4 import BeautifulSoup

from .models import DateEvidence, DateExtractionResult


MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

DATE_PATTERNS = [
    re.compile(r"\b(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})\b"),
    re.compile(r"\b(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})\b"),
    re.compile(r"\b(?P<day>\d{1,2})-(?P<month>\d{1,2})-(?P<year>\d{4})\b"),
    re.compile(r"\b(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})\b"),
    re.compile(
        r"\b(?:lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo)?"
        r"\s*(?P<day>\d{1,2})\s+de\s+(?P<month_name>[a-zA-Záéíóúñ]+)"
        r"(?:\s+de\s+(?P<year>\d{4}))?\b",
        re.IGNORECASE,
    ),
]

PUBLICATION_LABELS = (
    "fecha de publicacion",
    "publicado el",
    "publicacion",
    "posted on",
)
DEADLINE_LABELS = (
    "fecha limite de postulacion",
    "recepcion de antecedentes hasta",
    "postulaciones hasta",
    "plazo de postulacion",
    "cierre de postulacion",
    "se recibiran antecedentes hasta",
    "se recibiran los antecedentes hasta",
    "hasta el",
    "recepcion hasta",
)
CLOSING_LABELS = (
    "fecha de cierre",
    "fecha cierre",
    "cierre",
    "fecha_cierre",
)


def _norm(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    return " ".join(normalized.encode("ascii", "ignore").decode("ascii").lower().split())


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _infer_year(day: int, month: int, reference_date: date, *, prefer_future: bool) -> date | None:
    candidates = [
        _safe_date(reference_date.year - 1, month, day),
        _safe_date(reference_date.year, month, day),
        _safe_date(reference_date.year + 1, month, day),
    ]
    valid = [item for item in candidates if item is not None]
    windowed = [item for item in valid if -30 <= (item - reference_date).days <= 365]
    if not windowed:
        return None
    if prefer_future:
        future = [item for item in windowed if item >= reference_date]
        if future:
            return min(future)
    return min(windowed, key=lambda item: abs((item - reference_date).days))


def parse_date_string(raw_text: str, *, reference_date: date | None = None, prefer_future: bool = False) -> date | None:
    reference_date = reference_date or date.today()
    raw_norm = _norm(raw_text)
    for pattern in DATE_PATTERNS:
        match = pattern.search(raw_norm)
        if not match:
            continue
        groups = match.groupdict()
        if groups.get("month_name"):
            month = MONTHS_ES.get(groups["month_name"])
            if not month:
                return None
            day = int(groups["day"])
            year = groups.get("year")
            if year:
                return _safe_date(int(year), month, day)
            return _infer_year(day, month, reference_date, prefer_future=prefer_future)
        return _safe_date(int(groups["year"]), int(groups["month"]), int(groups["day"]))
    return None


def _scan_contextual_dates(
    text: str,
    *,
    labels: Iterable[str],
    source: str,
    label_name: str,
    reference_date: date,
    prefer_future: bool = False,
) -> list[DateEvidence]:
    evidences: list[DateEvidence] = []
    text_norm = _norm(text)
    for label in labels:
        start = 0
        while True:
            idx = text_norm.find(label, start)
            if idx < 0:
                break
            snippet = text_norm[idx : idx + 120]
            parsed = parse_date_string(snippet, reference_date=reference_date, prefer_future=prefer_future)
            if parsed:
                evidences.append(
                    DateEvidence(
                        raw_text=snippet,
                        parsed_date=parsed,
                        source=source,
                        label=label_name,
                        confidence=0.84 if prefer_future else 0.78,
                    )
                )
            start = idx + len(label)
    return evidences


def _extract_meta_dates(soup: BeautifulSoup, reference_date: date) -> list[DateEvidence]:
    evidences: list[DateEvidence] = []
    meta_labels = {
        "article:published_time": "publication_date",
        "datepublished": "publication_date",
        "datecreated": "publication_date",
        "date": "publication_date",
        "fecha_publicacion": "publication_date",
        "article:modified_time": "publication_date",
        "fecha_cierre": "closing_date",
        "closingdate": "closing_date",
        "applicationdeadline": "application_deadline",
        "validthrough": "application_deadline",
    }
    for meta in soup.find_all("meta"):
        key = _norm(meta.get("property") or meta.get("name") or meta.get("itemprop"))
        value = meta.get("content") or meta.get("value")
        if key not in meta_labels or not value:
            continue
        parsed = parse_date_string(str(value), reference_date=reference_date, prefer_future=True)
        if not parsed and isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
            except ValueError:
                parsed = None
        if parsed:
            evidences.append(
                DateEvidence(
                    raw_text=str(value),
                    parsed_date=parsed,
                    source="html_meta",
                    label=meta_labels[key],
                    confidence=0.92,
                )
            )
    return evidences


def _extract_time_tag_dates(soup: BeautifulSoup, reference_date: date) -> list[DateEvidence]:
    evidences: list[DateEvidence] = []
    for time_node in soup.find_all("time"):
        raw = time_node.get("datetime") or time_node.get_text(" ", strip=True)
        parsed = parse_date_string(raw, reference_date=reference_date)
        if not parsed and raw:
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
            except ValueError:
                parsed = None
        if parsed:
            evidences.append(
                DateEvidence(
                    raw_text=str(raw),
                    parsed_date=parsed,
                    source="time_tag",
                    label="publication_date",
                    confidence=0.86,
                )
            )
    return evidences


def _iter_json_ld_objects(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        yield payload
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                if isinstance(item, dict):
                    yield item
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield from _iter_json_ld_objects(item)


def _extract_json_ld_dates(soup: BeautifulSoup, reference_date: date) -> list[DateEvidence]:
    evidences: list[DateEvidence] = []
    label_map = {
        "datepublished": "publication_date",
        "datecreated": "publication_date",
        "dateposted": "publication_date",
        "validthrough": "application_deadline",
        "closingdate": "closing_date",
        "dateclosing": "closing_date",
    }
    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        raw_text = script.string or script.get_text(" ", strip=True)
        if not raw_text:
            continue
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            continue
        for item in _iter_json_ld_objects(payload):
            for original_key, value in item.items():
                normalized_key = _norm(original_key)
                if normalized_key not in label_map:
                    continue
                parsed = parse_date_string(str(value), reference_date=reference_date, prefer_future=True)
                if not parsed:
                    try:
                        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
                    except ValueError:
                        parsed = None
                if parsed:
                    evidences.append(
                        DateEvidence(
                            raw_text=str(value),
                            parsed_date=parsed,
                            source="json_ld",
                            label=label_map[normalized_key],
                            confidence=0.95,
                        )
                    )
    return evidences


def _choose_best(evidences: list[DateEvidence], label: str) -> date | None:
    matches = [item for item in evidences if item.label == label]
    if not matches:
        return None
    return sorted(matches, key=lambda item: (-item.confidence, item.parsed_date))[0].parsed_date


def extract_dates(*, html: str | None, text: str, reference_date: date | None = None) -> DateExtractionResult:
    reference_date = reference_date or date.today()
    soup = BeautifulSoup(html or "", "html.parser")
    evidences: list[DateEvidence] = []
    evidences.extend(_extract_meta_dates(soup, reference_date))
    evidences.extend(_extract_json_ld_dates(soup, reference_date))
    evidences.extend(_extract_time_tag_dates(soup, reference_date))

    visible_text = text or soup.get_text(" ", strip=True)
    evidences.extend(
        _scan_contextual_dates(
            visible_text,
            labels=PUBLICATION_LABELS,
            source="visible_text",
            label_name="publication_date",
            reference_date=reference_date,
        )
    )
    evidences.extend(
        _scan_contextual_dates(
            visible_text,
            labels=CLOSING_LABELS,
            source="visible_text",
            label_name="closing_date",
            reference_date=reference_date,
            prefer_future=True,
        )
    )
    evidences.extend(
        _scan_contextual_dates(
            visible_text,
            labels=DEADLINE_LABELS,
            source="visible_text",
            label_name="application_deadline",
            reference_date=reference_date,
            prefer_future=True,
        )
    )

    return DateExtractionResult(
        publication_date=_choose_best(evidences, "publication_date"),
        closing_date=_choose_best(evidences, "closing_date"),
        application_deadline=_choose_best(evidences, "application_deadline"),
        evidences=evidences,
        metadata={
            "future_deadlines_found": sum(
                1
                for item in evidences
                if item.label == "application_deadline" and item.parsed_date >= reference_date
            ),
            "expired_deadlines_found": sum(
                1
                for item in evidences
                if item.label == "application_deadline" and item.parsed_date < reference_date
            ),
        },
    )
