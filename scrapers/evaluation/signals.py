from __future__ import annotations

import re
import unicodedata
from datetime import date
from typing import Iterable
from urllib.parse import urlparse

from .models import PageType, SignalBundle, SourceProfile


POSITIVE_CLUSTER = (
    "concurso publico",
    "convocatoria",
    "postulacion",
    "bases",
    "perfil del cargo",
    "requisitos",
    "funciones",
    "honorarios",
    "contrata",
    "planta",
    "remuneracion",
    "vacante",
    "recepcion de antecedentes",
)

NEGATIVE_CLUSTER = (
    "subsidio",
    "beca",
    "fondo concursable",
    "noticia",
    "actividad",
    "taller",
    "operativo",
    "beneficio",
    "tramite",
    "cuenta publica",
    "licitacion",
    "programa",
    "curso",
    "feria",
)

GENERIC_LISTING_TITLES = {
    "concursos",
    "concursos abiertos",
    "concursos abiertos y cerrados",
    "concursos cerrados",
    "licitaciones",
    "tramites",
    "trabaja con nosotros",
    "ofertas laborales",
}

JOB_HINT_RE = re.compile(r"(concurso|trabaja|trabaje|empleo|oferta|convocatoria|postul)", re.I)
PDF_HINT_RE = re.compile(r"(bases|perfil|descriptor)", re.I)
NEGATIVE_RE = re.compile("|".join(re.escape(item) for item in NEGATIVE_CLUSTER), re.I)
POSITIVE_RE = re.compile("|".join(re.escape(item) for item in POSITIVE_CLUSTER), re.I)

WEIGHTS = {
    "jobposting_jsonld": 0.35,
    "future_deadline": 0.30,
    "closing_metadata": 0.25,
    "pdf_bases_or_profile": 0.20,
    "known_ats": 0.25,
    "job_cluster": 0.15,
    "listing_only_title": -0.30,
    "negative_cluster": -0.35,
    "old_without_open_signal": -0.30,
    "generic_landing_page": -0.25,
    "bot_waf_js": -0.20,
}


def _norm(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    return " ".join(
        normalized.encode("ascii", "ignore").decode("ascii").lower().split()
    )


def build_signal_bundle(
    *,
    source_url: str,
    title: str | None,
    text: str,
    page_type: PageType,
    profile: SourceProfile | None,
    publication_date: date | None,
    closing_date: date | None,
    application_deadline: date | None,
    has_jobposting_jsonld: bool,
    pdf_links: Iterable[str] = (),
    known_ats: bool = False,
    bot_or_js: bool = False,
    open_signal_count: int = 0,
    cms: str | None = None,
    today: date | None = None,
) -> SignalBundle:
    today = today or date.today()
    title_norm = _norm(title)
    text_norm = _norm(text)
    url_norm = _norm(urlparse(source_url).path)

    positives: list[str] = []
    negatives: list[str] = []
    raw_score = 0.0
    overrides = profile.signal_weight_overrides if profile else {}

    if has_jobposting_jsonld:
        positives.append("jobposting_jsonld")
        raw_score += overrides.get("jobposting_jsonld", WEIGHTS["jobposting_jsonld"])

    if application_deadline and application_deadline >= today:
        positives.append("future_deadline")
        raw_score += overrides.get("future_deadline", WEIGHTS["future_deadline"])

    if closing_date:
        positives.append("closing_metadata")
        raw_score += overrides.get("closing_metadata", WEIGHTS["closing_metadata"])

    if any(PDF_HINT_RE.search(link) for link in pdf_links):
        positives.append("pdf_bases_or_profile")
        raw_score += overrides.get("pdf_bases_or_profile", WEIGHTS["pdf_bases_or_profile"])

    if known_ats:
        positives.append("known_ats")
        raw_score += overrides.get("known_ats", WEIGHTS["known_ats"])

    positive_hits = len(set(POSITIVE_RE.findall(text_norm + " " + url_norm)))
    if positive_hits >= 2 or JOB_HINT_RE.search(url_norm):
        positives.append("job_cluster")
        raw_score += overrides.get("job_cluster", WEIGHTS["job_cluster"])

    if title_norm in GENERIC_LISTING_TITLES or (page_type == PageType.LISTING_PAGE and positive_hits < 2):
        negatives.append("listing_only_title")
        raw_score += overrides.get("listing_only_title", WEIGHTS["listing_only_title"])

    negative_hits = len(set(NEGATIVE_RE.findall(text_norm + " " + title_norm)))
    if negative_hits >= 2 or page_type == PageType.NEWS_PAGE:
        negatives.append("negative_cluster")
        raw_score += overrides.get("negative_cluster", WEIGHTS["negative_cluster"])

    age_days = None
    if publication_date is not None:
        age_days = (today - publication_date).days
        if age_days > 90 and open_signal_count == 0 and application_deadline is None and closing_date is None:
            negatives.append("old_without_open_signal")
            raw_score += overrides.get("old_without_open_signal", WEIGHTS["old_without_open_signal"])

    if page_type == PageType.GENERAL_PAGE and positive_hits == 0:
        negatives.append("generic_landing_page")
        raw_score += overrides.get("generic_landing_page", WEIGHTS["generic_landing_page"])

    if bot_or_js:
        negatives.append("bot_waf_js")
        raw_score += overrides.get("bot_waf_js", WEIGHTS["bot_waf_js"])

    confidence = max(0.0, min(1.0, 0.5 + raw_score))
    return SignalBundle(
        positive_signals=positives,
        negative_signals=negatives,
        confidence=round(confidence, 4),
        raw_score=round(raw_score, 4),
        metadata={
            "cms": cms,
            "publication_date": publication_date.isoformat() if publication_date else None,
            "closing_date": closing_date.isoformat() if closing_date else None,
            "application_deadline": application_deadline.isoformat() if application_deadline else None,
            "age_days": age_days,
            "positive_keyword_hits": positive_hits,
            "negative_keyword_hits": negative_hits,
            "open_signals_found": open_signal_count,
        },
    )
