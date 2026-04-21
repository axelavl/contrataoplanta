from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable

RULESET_VERSION = "2026.04.21"

POSITIVE_KEYWORDS: tuple[str, ...] = (
    "concurso",
    "concurso publico",
    "convocatoria",
    "vacante",
    "llamado",
    "cargo",
    "puesto",
    "postulacion",
    "postular",
    "recepcion de antecedentes",
    "trabaja con nosotros",
    "trabaje con nosotros",
    "empleo",
    "oportunidad laboral",
    "requisitos",
    "funciones",
    "renta bruta mensual",
    "remuneracion mensual",
    "honorarios mensuales",
    "contrata",
    "planta",
    "codigo del trabajo",
    "calidad juridica",
    "reemplazo",
    "suplencia",
    "fecha de cierre",
    "cierre de postulacion",
    "postulaciones hasta",
    "bases",
    "perfil de cargo",
    "tdr",
)

NEGATIVE_PATTERNS: tuple[str, ...] = (
    r"\bresultados? del concurso\b",
    r"\bresultados? proceso\b",
    r"\bn[oó]mina de seleccionad[oa]s\b",
    r"\bn[oó]mina final\b",
    r"\blista de seleccionad[oa]s\b",
    r"\badjudicaci[oó]n\b",
    r"\bproceso adjudicado\b",
    r"\bproceso finalizado\b",
    r"\bproceso cerrado\b",
    r"\bconcurso cerrado\b",
    r"\bconvocatoria cerrada\b",
    r"\bpostulaciones cerradas\b",
    r"\bnoticias?\b",
    r"\bcomunicado\b",
    r"\bbolet[ií]n\b",
    r"\bnovedades?\b",
    r"\bprensa\b",
    r"\bdeclaraci[oó]n p[uú]blica\b",
    r"\bagenda institucional\b",
    r"\bcuenta p[uú]blica\b",
    r"\bevento\b",
    r"\bseminario\b",
    r"\bcharla\b",
    r"\bworkshop\b",
    r"\blicitaci[oó]n\b",
    r"\bcompra p[uú]blica\b",
    r"\bmercado p[uú]blico\b",
    r"\bsubvenci[oó]n\b",
    r"\bfondos? concursables?\b",
    r"\bconcurso art[ií]stico\b",
    r"\bconcurso escolar\b",
    r"\bconcurso de proyectos?\b",
    r"\bproceso del a[nñ]o (?:201\d|20[12]0)\b",
    r"\bconcursos? anteriores?\b",
    r"\barchivo hist[oó]rico\b",
    r"\bhist[oó]rico de concursos?\b",
    r"\bpublicaci[oó]n institucional\b",
    r"\bart[ií]culo\b",
    r"\bblog\b",
    r"\bmemoria anual\b",
)

NEGATIVE_URL_PARTS: tuple[str, ...] = (
    "/noticias",
    "/news",
    "/prensa",
    "/blog",
    "/comunicados",
    "/actas",
    "/cuenta-publica",
    "/cuenta_publica",
    "/cuentapublica",
    "/agenda",
    "/eventos",
    "/galeria",
    "/galería",
    "/historico",
    "/histórico",
    "/anteriores",
    "/resultados-concurso",
    "/nomina-",
    "/adjudicacion",
    "/licitacion",
    "/licitaciones",
    "/compras",
    "/mercadopublico",
    "/subvenciones",
    "/fondos-concursables",
)

INTERNAL_ONLY_PATTERNS: tuple[str, ...] = (
    r"\bsolo difusi[oó]n\b",
    r"\bsolo difusi[oó]n interna\b",
    r"\bdifusi[oó]n interna\b",
)

NEGATIVE_RE = re.compile("|".join(NEGATIVE_PATTERNS), re.IGNORECASE)
INTERNAL_ONLY_RE = re.compile("|".join(INTERNAL_ONLY_PATTERNS), re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class OfferPolicyEvaluation:
    ruleset_version: str
    score: float
    likely_offer: bool
    reason_codes: tuple[str, ...]


def _norm(text: str | None) -> str:
    if not text:
        return ""
    v = unicodedata.normalize("NFKD", text)
    return v.encode("ascii", "ignore").decode("ascii").lower().strip()


def classify_offer_candidate(
    *,
    title: str | None,
    content_text: str | None,
    url: str | None,
    extra_text: str | None = None,
    extra_positive_keywords: Iterable[str] = (),
) -> OfferPolicyEvaluation:
    """Clasificador central basado en política versionada y reason codes."""
    title_n = _norm(title)
    content_n = _norm(content_text)
    extra_n = _norm(extra_text)
    blob = " ".join(part for part in (title_n, content_n, extra_n) if part)
    url_n = _norm(url)

    score = 0.0
    reason_codes: list[str] = []

    positives = POSITIVE_KEYWORDS + tuple(_norm(k) for k in extra_positive_keywords if _norm(k))
    positive_hits = [kw for kw in positives if kw and kw in blob]
    negative_url_hit = next((part for part in NEGATIVE_URL_PARTS if part in url_n), None)
    internal_hit = bool(INTERNAL_ONLY_RE.search(blob))
    negative_text_hit = bool(NEGATIVE_RE.search(blob))

    if positive_hits:
        score += min(0.75, 0.10 * len(set(positive_hits)))
        reason_codes.append("positive_signals_detected")

    if negative_url_hit:
        score -= 0.45
        reason_codes.append("negative_url_pattern")

    if negative_text_hit:
        score -= 0.35
        reason_codes.append("negative_text_pattern")

    if internal_hit:
        score -= 0.60
        reason_codes.append("internal_only_signal")

    if len(blob) < 8:
        score -= 0.25
        reason_codes.append("insufficient_text")

    likely_offer = score >= 0.20 and bool(positive_hits) and not internal_hit
    if likely_offer:
        reason_codes.append("policy_accept")
    else:
        reason_codes.append("policy_reject")

    return OfferPolicyEvaluation(
        ruleset_version=RULESET_VERSION,
        score=max(-1.0, min(1.0, score)),
        likely_offer=likely_offer,
        reason_codes=tuple(reason_codes),
    )


__all__ = [
    "INTERNAL_ONLY_PATTERNS",
    "INTERNAL_ONLY_RE",
    "NEGATIVE_PATTERNS",
    "NEGATIVE_RE",
    "NEGATIVE_URL_PARTS",
    "OfferPolicyEvaluation",
    "POSITIVE_KEYWORDS",
    "RULESET_VERSION",
    "classify_offer_candidate",
]
