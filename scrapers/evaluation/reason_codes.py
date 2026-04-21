from __future__ import annotations

from enum import Enum


class ReasonCode(str, Enum):
    HTTP_404 = "http_404"
    HTTP_403 = "http_403"
    HTTP_500 = "http_500"
    TIMEOUT = "timeout"
    DNS_ERROR = "dns_error"
    SSL_ERROR = "ssl_error"
    REDIRECT_LOOP = "redirect_loop"
    EMPTY_RESPONSE = "empty_response"
    JS_REQUIRED = "js_required"
    BLOCKED_BY_BOT_PROTECTION = "blocked_by_bot_protection"
    NOT_JOB_RELATED = "not_job_related"
    MIXED_CONTENT_LOW_CONFIDENCE = "mixed_content_low_confidence"
    NO_CALLS_FOUND = "no_calls_found"
    ONLY_EXPIRED_CALLS = "only_expired_calls"
    LISTING_WITHOUT_OFFER_DETAIL = "listing_without_offer_detail"
    DOCUMENT_WITHOUT_JOB_DATA = "document_without_job_data"
    NO_MEANINGFUL_TEXT = "no_meaningful_text"
    DUPLICATE_CANDIDATE = "duplicate_candidate"
    UNSUPPORTED_STRUCTURE = "unsupported_structure"
    NO_MATCHING_EXTRACTOR = "no_matching_extractor"
    EXTRACTOR_FAILED_VALIDATION = "extractor_failed_validation"
    MANUAL_REVIEW_REQUIRED = "manual_review_required"
    DEADLINE_PASSED = "deadline_passed"
    CLOSING_DATE_PASSED = "closing_date_passed"
    EXPIRED_TEXT_SIGNAL = "expired_text_signal"
    PUBLICATION_TOO_OLD = "publication_too_old"
    PLACEHOLDER_BASES_URL = "placeholder_bases_url"
    INVALID_INSTITUTION_REFERENCE = "invalid_institution_reference"
    MISSING_PUBLISHABLE_URL = "missing_publishable_url"
    MISSING_VALIDITY_SIGNAL = "missing_validity_signal"
    CONTACT_CHANNEL_AMBIGUOUS = "contact_channel_ambiguous"
    SALARY_OUTLIER = "salary_outlier"
    SALARY_UNIT_INCONSISTENT = "salary_unit_inconsistent"
    LISTING_PAGE_ONLY = "listing_page_only"
    STALE_ACTIVE_OFFER = "stale_active_offer"
    CATALOG_MISMATCH = "catalog_mismatch"
    PLAYWRIGHT_RUNTIME_UNAVAILABLE = "playwright_runtime_unavailable"


DEFAULT_REASON_DETAILS: dict[ReasonCode, str] = {
    ReasonCode.HTTP_404: "La URL respondio 404 y no existe contenido scrapable.",
    ReasonCode.HTTP_403: "La fuente respondio 403 y no pudo evaluarse en forma util.",
    ReasonCode.HTTP_500: "La fuente respondio 500 durante la evaluacion.",
    ReasonCode.TIMEOUT: "La evaluacion expiro antes de obtener una respuesta estable.",
    ReasonCode.DNS_ERROR: "No fue posible resolver el dominio de la fuente.",
    ReasonCode.SSL_ERROR: "La evaluacion encontro un error SSL/TLS.",
    ReasonCode.REDIRECT_LOOP: "La URL entro en un bucle de redireccion.",
    ReasonCode.EMPTY_RESPONSE: "La fuente respondio sin cuerpo util.",
    ReasonCode.JS_REQUIRED: "El contenido util requiere JavaScript para renderizarse.",
    ReasonCode.BLOCKED_BY_BOT_PROTECTION: "Se detecto proteccion anti-bot o WAF.",
    ReasonCode.NOT_JOB_RELATED: "El contenido analizado no parece una oferta laboral.",
    ReasonCode.MIXED_CONTENT_LOW_CONFIDENCE: "La fuente mezcla avisos laborales y ruido con baja confianza.",
    ReasonCode.NO_CALLS_FOUND: "No se detectaron convocatorias en la fuente evaluada.",
    ReasonCode.ONLY_EXPIRED_CALLS: "La fuente es valida, pero solo contiene convocatorias vencidas.",
    ReasonCode.LISTING_WITHOUT_OFFER_DETAIL: "Se detecto un listado generico sin fichas publicables.",
    ReasonCode.DOCUMENT_WITHOUT_JOB_DATA: "El documento encontrado no contiene datos laborales suficientes.",
    ReasonCode.NO_MEANINGFUL_TEXT: "No se encontro texto visible suficiente para clasificar la fuente.",
    ReasonCode.DUPLICATE_CANDIDATE: "La oferta candidata ya existe con la misma huella operativa.",
    ReasonCode.UNSUPPORTED_STRUCTURE: "La estructura de la pagina no calza con extractores soportados.",
    ReasonCode.NO_MATCHING_EXTRACTOR: "No existe extractor confiable para la estructura evaluada.",
    ReasonCode.EXTRACTOR_FAILED_VALIDATION: "La oferta extraida no paso la validacion de calidad.",
    ReasonCode.MANUAL_REVIEW_REQUIRED: "La evidencia es contradictoria y requiere revision manual.",
    ReasonCode.DEADLINE_PASSED: "La fecha limite de postulacion ya paso.",
    ReasonCode.CLOSING_DATE_PASSED: "La fecha de cierre estructurada ya paso.",
    ReasonCode.EXPIRED_TEXT_SIGNAL: "El texto contiene senales explicitas de proceso cerrado.",
    ReasonCode.PUBLICATION_TOO_OLD: "La publicacion es antigua y no hay evidencia vigente suficiente.",
    ReasonCode.PLACEHOLDER_BASES_URL: "La URL de bases apunta a un placeholder no publicable.",
    ReasonCode.INVALID_INSTITUTION_REFERENCE: "La oferta referencia una institucion fuera del catalogo.",
    ReasonCode.MISSING_PUBLISHABLE_URL: "Falta una URL postulable para publicar la oferta.",
    ReasonCode.MISSING_VALIDITY_SIGNAL: "No hay fecha de cierre ni una senal explicita de vigencia.",
    ReasonCode.CONTACT_CHANNEL_AMBIGUOUS: "No fue posible distinguir correo de postulacion y de contacto.",
    ReasonCode.SALARY_OUTLIER: "La renta parseada es inverosimil para el dominio objetivo.",
    ReasonCode.SALARY_UNIT_INCONSISTENT: "La unidad de renta detectada no es consistente (mensual/anual).",
    ReasonCode.LISTING_PAGE_ONLY: "La entidad es solo una portada de listados y no una oferta.",
    ReasonCode.STALE_ACTIVE_OFFER: "La oferta seguia activa aunque su fecha de cierre ya paso.",
    ReasonCode.CATALOG_MISMATCH: "Los datos de la oferta no son consistentes con el catalogo operativo.",
    ReasonCode.PLAYWRIGHT_RUNTIME_UNAVAILABLE: "El runtime no tiene Playwright operativo para renderizar JavaScript real.",
}


def reason_detail(reason_code: ReasonCode | None, *, fallback: str | None = None) -> str | None:
    if reason_code is None:
        return fallback
    return DEFAULT_REASON_DETAILS.get(reason_code, fallback)
