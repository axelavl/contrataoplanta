from __future__ import annotations

from datetime import date

from scrapers.evaluation.date_parser import extract_dates, parse_date_string
from scrapers.evaluation.models import PageType, ValidityStatus
from scrapers.evaluation.reason_codes import ReasonCode
from scrapers.evaluation.validity_rules import assess_validity


REFERENCE_DATE = date(2026, 4, 18)


def test_deadline_priority_over_closing_date():
    assessment = assess_validity(
        page_type=PageType.DETAIL_PAGE,
        text=(
            "Fecha limite de postulacion: 10-04-2026. "
            "Fecha de cierre estructurada: 30-04-2026."
        ),
        publication_date=date(2026, 4, 1),
        closing_date=date(2026, 4, 30),
        application_deadline=date(2026, 4, 10),
        reference_date=REFERENCE_DATE,
    )
    assert assessment.status == ValidityStatus.EXPIRED_CONFIRMED
    assert assessment.reason_code == ReasonCode.DEADLINE_PASSED


def test_wordpress_old_without_open_signals_expires_by_publication_age():
    assessment = assess_validity(
        page_type=PageType.WORDPRESS_POST,
        text="Concurso finalizado. Bases del proceso anterior.",
        publication_date=date(2025, 12, 1),
        closing_date=None,
        application_deadline=None,
        reference_date=REFERENCE_DATE,
    )
    assert assessment.status == ValidityStatus.EXPIRED_BY_PUBLICATION_AGE
    assert assessment.reason_code == ReasonCode.PUBLICATION_TOO_OLD


def test_wordpress_old_with_bases_pdf_does_not_expire_by_age():
    assessment = assess_validity(
        page_type=PageType.WORDPRESS_POST,
        text="Publicacion historica sin fecha de cierre en la pagina.",
        expanded_text="Publicacion historica. Adjuntos: bases-concurso.pdf perfil-del-cargo.pdf",
        publication_date=date(2025, 12, 1),
        closing_date=None,
        application_deadline=None,
        has_pdf_bases_or_profile=True,
        reference_date=REFERENCE_DATE,
    )
    assert assessment.status == ValidityStatus.UNKNOWN_VALIDITY
    assert assessment.reason_code is None


def test_wordpress_old_but_future_deadline_stays_open():
    assessment = assess_validity(
        page_type=PageType.WORDPRESS_POST,
        text="Postulacion abierta. Recepcion de antecedentes hasta el 30 de abril de 2026.",
        publication_date=date(2025, 12, 1),
        closing_date=None,
        application_deadline=date(2026, 4, 30),
        reference_date=REFERENCE_DATE,
    )
    assert assessment.status == ValidityStatus.OPEN_CONFIRMED
    assert assessment.reason_code is None


def test_date_parser_infers_missing_year_with_future_window():
    parsed = parse_date_string(
        "recepcion hasta el viernes 12 de mayo",
        reference_date=REFERENCE_DATE,
        prefer_future=True,
    )
    assert parsed == date(2026, 5, 12)


def test_contradictory_text_signals_go_to_manual_review():
    assessment = assess_validity(
        page_type=PageType.DETAIL_PAGE,
        text="Proceso abierto, pero tambien figura proceso cerrado por actualizacion del portal.",
        publication_date=date(2026, 4, 5),
        closing_date=None,
        application_deadline=None,
        reference_date=REFERENCE_DATE,
    )
    assert assessment.status == ValidityStatus.MANUAL_REVIEW
    assert assessment.reason_code == ReasonCode.MANUAL_REVIEW_REQUIRED


def test_old_publication_with_mixed_signals_prioritizes_manual_review():
    assessment = assess_validity(
        page_type=PageType.WORDPRESS_POST,
        text="Proceso abierto con nota previa del proceso cerrado.",
        publication_date=date(2025, 12, 1),
        closing_date=None,
        application_deadline=None,
        reference_date=REFERENCE_DATE,
    )
    assert assessment.status == ValidityStatus.MANUAL_REVIEW
    assert assessment.reason_code == ReasonCode.MANUAL_REVIEW_REQUIRED
