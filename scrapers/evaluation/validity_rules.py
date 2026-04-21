from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import date

from .models import OpenCallsStatus, PageType, ValidityStatus
from .reason_codes import ReasonCode, reason_detail


OPEN_TEXT_SIGNALS = (
    "convocatoria vigente",
    "proceso abierto",
    "postulacion abierta",
    "recepcion de antecedentes",
    "en curso",
    "se extiende el plazo",
)

EXPIRED_TEXT_SIGNALS = (
    "proceso cerrado",
    "convocatoria finalizada",
    "postulaciones cerradas",
    "plazo vencido",
    "resultado final",
    "nomina de seleccionados",
    "acta de seleccion",
)


def _norm(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    return " ".join(normalized.encode("ascii", "ignore").decode("ascii").lower().split())


@dataclass(slots=True)
class ValidityAssessment:
    status: ValidityStatus
    open_calls_status: OpenCallsStatus
    reason_code: ReasonCode | None
    reason_detail: str | None
    open_signal_count: int
    expired_signal_count: int
    age_expiry_evidence: dict[str, object] | None = None


def assess_validity(
    *,
    page_type: PageType,
    text: str,
    publication_date: date | None,
    closing_date: date | None,
    application_deadline: date | None,
    expanded_text: str | None = None,
    has_pdf_bases_or_profile: bool = False,
    reference_date: date | None = None,
) -> ValidityAssessment:
    today = reference_date or date.today()
    text_norm = _norm(text)
    expanded_text_norm = _norm(expanded_text)
    effective_text_norm = text_norm
    if expanded_text_norm:
        effective_text_norm = f"{text_norm} {expanded_text_norm}".strip()
    open_signal_count = sum(1 for signal in OPEN_TEXT_SIGNALS if signal in effective_text_norm)
    expired_signal_count = sum(1 for signal in EXPIRED_TEXT_SIGNALS if signal in effective_text_norm)

    if application_deadline is not None:
        if application_deadline < today:
            return ValidityAssessment(
                status=ValidityStatus.EXPIRED_CONFIRMED,
                open_calls_status=OpenCallsStatus.ONLY_EXPIRED_CALLS,
                reason_code=ReasonCode.DEADLINE_PASSED,
                reason_detail=reason_detail(ReasonCode.DEADLINE_PASSED),
                open_signal_count=open_signal_count,
                expired_signal_count=expired_signal_count,
            )
        return ValidityAssessment(
            status=ValidityStatus.OPEN_CONFIRMED,
            open_calls_status=OpenCallsStatus.HAS_OPEN_CALLS,
            reason_code=None,
            reason_detail=None,
            open_signal_count=open_signal_count,
            expired_signal_count=expired_signal_count,
        )

    if closing_date is not None:
        if closing_date < today:
            return ValidityAssessment(
                status=ValidityStatus.EXPIRED_CONFIRMED,
                open_calls_status=OpenCallsStatus.ONLY_EXPIRED_CALLS,
                reason_code=ReasonCode.CLOSING_DATE_PASSED,
                reason_detail=reason_detail(ReasonCode.CLOSING_DATE_PASSED),
                open_signal_count=open_signal_count,
                expired_signal_count=expired_signal_count,
            )
        return ValidityAssessment(
            status=ValidityStatus.OPEN_CONFIRMED,
            open_calls_status=OpenCallsStatus.HAS_OPEN_CALLS,
            reason_code=None,
            reason_detail=None,
            open_signal_count=open_signal_count,
            expired_signal_count=expired_signal_count,
        )

    if expired_signal_count > 0 and open_signal_count == 0:
        return ValidityAssessment(
            status=ValidityStatus.EXPIRED_CONFIRMED,
            open_calls_status=OpenCallsStatus.ONLY_EXPIRED_CALLS,
            reason_code=ReasonCode.EXPIRED_TEXT_SIGNAL,
            reason_detail=reason_detail(ReasonCode.EXPIRED_TEXT_SIGNAL),
            open_signal_count=open_signal_count,
            expired_signal_count=expired_signal_count,
        )

    if open_signal_count > 0 and expired_signal_count == 0:
        return ValidityAssessment(
            status=ValidityStatus.OPEN_CONFIRMED,
            open_calls_status=OpenCallsStatus.HAS_OPEN_CALLS,
            reason_code=None,
            reason_detail=None,
            open_signal_count=open_signal_count,
            expired_signal_count=expired_signal_count,
        )

    if open_signal_count > 0 and expired_signal_count > 0:
        return ValidityAssessment(
            status=ValidityStatus.MANUAL_REVIEW,
            open_calls_status=OpenCallsStatus.UNKNOWN_STATUS,
            reason_code=ReasonCode.MANUAL_REVIEW_REQUIRED,
            reason_detail=reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED),
            open_signal_count=open_signal_count,
            expired_signal_count=expired_signal_count,
        )

    age_expiry_evidence = {
        "has_pdf_bases_or_profile": has_pdf_bases_or_profile,
        "expanded_text_used": bool(expanded_text_norm),
        "open_signal_count": open_signal_count,
        "expired_signal_count": expired_signal_count,
    }
    if (
        publication_date is not None
        and page_type in {PageType.WORDPRESS_POST, PageType.NEWS_PAGE, PageType.GENERAL_PAGE}
        and (today - publication_date).days > 90
        and open_signal_count == 0
        and expired_signal_count == 0
        and application_deadline is None
        and closing_date is None
        and not has_pdf_bases_or_profile
    ):
        return ValidityAssessment(
            status=ValidityStatus.EXPIRED_BY_PUBLICATION_AGE,
            open_calls_status=OpenCallsStatus.NO_CALLS_FOUND,
            reason_code=ReasonCode.PUBLICATION_TOO_OLD,
            reason_detail=reason_detail(ReasonCode.PUBLICATION_TOO_OLD),
            open_signal_count=open_signal_count,
            expired_signal_count=expired_signal_count,
            age_expiry_evidence=age_expiry_evidence,
        )

    return ValidityAssessment(
        status=ValidityStatus.UNKNOWN_VALIDITY,
        open_calls_status=OpenCallsStatus.UNKNOWN_STATUS,
        reason_code=None,
        reason_detail=None,
        open_signal_count=open_signal_count,
        expired_signal_count=expired_signal_count,
        age_expiry_evidence=age_expiry_evidence,
    )
