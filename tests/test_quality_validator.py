from __future__ import annotations

from datetime import date

from scrapers.evaluation.models import QualityDecision
from scrapers.evaluation.reason_codes import ReasonCode
from scrapers.evaluation.quality_validator import QualityValidator


def test_landing_page_concursos_is_not_publishable():
    validator = QualityValidator(valid_institution_ids={1})
    result = validator.validate(
        {
            "institucion_id": 1,
            "institucion_nombre": "Municipalidad X",
            "cargo": "Concursos",
            "descripcion": "Listado general de concursos y noticias.",
            "url_oferta": "https://muni-x.cl/concursos",
            "fecha_publicacion": "2026-04-10",
            "estado": "activo",
        }
    )
    assert result.decision == QualityDecision.REJECT
    assert ReasonCode.LISTING_PAGE_ONLY in result.reason_codes


def test_placeholder_privacy_pdf_is_invalid_url_bases():
    validator = QualityValidator(valid_institution_ids={1})
    result = validator.validate(
        {
            "institucion_id": 1,
            "institucion_nombre": "Servicio X",
            "cargo": "Analista",
            "descripcion": "Convocatoria a contrata.",
            "url_oferta": "https://servicio.cl/ofertas/analista",
            "fecha_publicacion": "2026-04-10",
            "url_bases": "https://www.empleospublicos.cl/documentos/politicaprivacidad.pdf",
        }
    )
    assert result.decision == QualityDecision.REJECT
    assert ReasonCode.PLACEHOLDER_BASES_URL in result.reason_codes


def test_invalid_institution_id_is_rejected():
    validator = QualityValidator(valid_institution_ids={1, 2, 3})
    result = validator.validate(
        {
            "institucion_id": 705,
            "institucion_nombre": "Institucion Fantasma",
            "cargo": "Profesional",
            "descripcion": "Cargo a contrata",
            "url_oferta": "https://fantasma.cl/ofertas/1",
            "estado": "vigente",
        }
    )
    assert result.decision == QualityDecision.REJECT
    assert ReasonCode.INVALID_INSTITUTION_REFERENCE in result.reason_codes


def test_salary_outlier_is_rejected():
    validator = QualityValidator(valid_institution_ids={1})
    result = validator.validate(
        {
            "institucion_id": 1,
            "institucion_nombre": "Municipalidad Y",
            "cargo": "Ingeniero",
            "descripcion": "Cargo profesional.",
            "url_oferta": "https://muni-y.cl/oferta/100",
            "estado": "activo",
            "renta_bruta_max": 18000000,
        }
    )
    assert result.decision == QualityDecision.REJECT
    assert ReasonCode.SALARY_OUTLIER in result.reason_codes


def test_stale_active_offer_is_rejected():
    validator = QualityValidator(valid_institution_ids={1})
    result = validator.validate(
        {
            "institucion_id": 1,
            "institucion_nombre": "Servicio Civil",
            "cargo": "Abogado",
            "descripcion": "Concurso a contrata.",
            "url_oferta": "https://serviciocivil.cl/ofertas/abogado",
            "fecha_cierre": "2026-04-01",
            "estado": "activo",
        },
        today=date(2026, 4, 18),
    )
    assert result.decision == QualityDecision.REJECT
    assert ReasonCode.STALE_ACTIVE_OFFER in result.reason_codes


def test_missing_publishability_url_is_rejected():
    validator = QualityValidator(valid_institution_ids={1})
    result = validator.validate(
        {
            "institucion_id": 1,
            "institucion_nombre": "Servicio A",
            "cargo": "Analista",
            "fecha_cierre": "2026-05-10",
        }
    )
    assert result.decision == QualityDecision.REJECT
    assert ReasonCode.MISSING_PUBLISHABLE_URL in result.reason_codes


def test_missing_validity_signal_moves_to_review():
    validator = QualityValidator(valid_institution_ids={1})
    result = validator.validate(
        {
            "institucion_id": 1,
            "institucion_nombre": "Servicio B",
            "cargo": "Analista",
            "url_oferta": "https://servicio-b.cl/ofertas/analista",
        }
    )
    assert result.decision == QualityDecision.REVIEW
    assert ReasonCode.MISSING_VALIDITY_SIGNAL in result.reason_codes


def test_salary_unit_mixed_moves_to_review_with_trace():
    validator = QualityValidator(valid_institution_ids={1})
    result = validator.validate(
        {
            "institucion_id": 1,
            "institucion_nombre": "Servicio C",
            "cargo": "Profesional TI",
            "url_oferta": "https://servicio-c.cl/ofertas/ti",
            "fecha_cierre": "2026-06-01",
            "renta_bruta_max": 2400000,
            "renta_texto": "Renta mensual referencial, monto anual total según convenio.",
        }
    )
    assert result.decision == QualityDecision.REVIEW
    assert ReasonCode.SALARY_UNIT_INCONSISTENT in result.reason_codes
    assert result.signals_json["salary_unit"] == "mixed"
