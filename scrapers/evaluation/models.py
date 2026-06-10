from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any

from .reason_codes import ReasonCode


class Availability(str, Enum):
    OK = "ok"
    HTTP_404 = "http_404"
    HTTP_403 = "http_403"
    HTTP_500 = "http_500"
    TIMEOUT = "timeout"
    DNS_ERROR = "dns_error"
    SSL_ERROR = "ssl_error"
    REDIRECT_LOOP = "redirect_loop"
    BLOCKED_BY_BOT_PROTECTION = "blocked_by_bot_protection"
    EMPTY_RESPONSE = "empty_response"
    JS_REQUIRED = "js_required"


class PageType(str, Enum):
    LISTING_PAGE = "listing_page"
    DETAIL_PAGE = "detail_page"
    DOCUMENT_PAGE = "document_page"
    NEWS_PAGE = "news_page"
    GENERAL_PAGE = "general_page"
    ATS_EXTERNAL = "ats_external"
    WORDPRESS_POST = "wordpress_post"
    WORDPRESS_LISTING = "wordpress_listing"
    UNKNOWN_PAGE_TYPE = "unknown_page_type"


class JobRelevance(str, Enum):
    JOB_LIKE = "job_like"
    MIXED = "mixed"
    NON_JOB = "non_job"
    UNCERTAIN = "uncertain"


class OpenCallsStatus(str, Enum):
    HAS_OPEN_CALLS = "has_open_calls"
    ONLY_EXPIRED_CALLS = "only_expired_calls"
    NO_CALLS_FOUND = "no_calls_found"
    UNKNOWN_STATUS = "unknown_status"


class ValidityStatus(str, Enum):
    OPEN_CONFIRMED = "open_confirmed"
    EXPIRED_CONFIRMED = "expired_confirmed"
    EXPIRED_BY_PUBLICATION_AGE = "expired_by_publication_age"
    UNKNOWN_VALIDITY = "unknown_validity"
    MANUAL_REVIEW = "manual_review"


class Decision(str, Enum):
    EXTRACT = "extract"
    SKIP = "skip"
    SOURCE_STATUS_ONLY = "source_status_only"
    MANUAL_REVIEW = "manual_review"


class RetryPolicy(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    EVENTUAL = "eventual"
    EXPLORATORY = "exploratory"


class ExtractorKind(str, Enum):
    SCRAPER_EMPLEOS_PUBLICOS = "scraper_empleos_publicos"
    SCRAPER_WORDPRESS_JOBS = "scraper_wordpress_jobs"
    SCRAPER_WORDPRESS_NEWS_FILTER = "scraper_wordpress_news_filter"
    SCRAPER_PDF_JOBS = "scraper_pdf_jobs"
    SCRAPER_CUSTOM_DETAIL = "scraper_custom_detail"
    SCRAPER_EXTERNAL_ATS = "scraper_external_ats"
    SCRAPER_PLAYWRIGHT = "scraper_playwright"
    SCRAPER_GENERIC_FALLBACK = "scraper_generic_fallback"
    MANUAL_REVIEW = "manual_review"


class QualityDecision(str, Enum):
    PUBLISH = "publish"
    REVIEW = "review"
    REJECT = "reject"
    MANUAL_REVIEW = "review"


@dataclass(slots=True)
class DateEvidence:
    raw_text: str
    parsed_date: date
    source: str
    label: str
    confidence: float


@dataclass(slots=True)
class DateExtractionResult:
    publication_date: date | None = None
    closing_date: date | None = None
    application_deadline: date | None = None
    evidences: list[DateEvidence] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "publication_date": self.publication_date.isoformat() if self.publication_date else None,
            "closing_date": self.closing_date.isoformat() if self.closing_date else None,
            "application_deadline": self.application_deadline.isoformat() if self.application_deadline else None,
            "evidences": [
                {
                    "raw_text": item.raw_text,
                    "parsed_date": item.parsed_date.isoformat(),
                    "source": item.source,
                    "label": item.label,
                    "confidence": item.confidence,
                }
                for item in self.evidences
            ],
            **self.metadata,
        }


@dataclass(slots=True)
class SignalBundle:
    positive_signals: list[str] = field(default_factory=list)
    negative_signals: list[str] = field(default_factory=list)
    confidence: float = 0.0
    raw_score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "positive_signals": self.positive_signals,
            "negative_signals": self.negative_signals,
            "confidence": round(self.confidence, 4),
            "raw_score": round(self.raw_score, 4),
            **self.metadata,
        }


@dataclass(slots=True)
class SourceProfile:
    name: str
    threshold_family: str = "generic"
    domains: tuple[str, ...] = ()
    platform_markers: tuple[str, ...] = ()
    institution_ids: tuple[int, ...] = ()
    candidate_urls: tuple[str, ...] = ()
    max_candidate_urls: int | None = None
    warmup_required: bool = False
    supports_pdf_enrichment: bool = False
    supports_playwright: bool = False
    trusted_job_source: bool = False
    page_type_priors: dict[PageType, float] = field(default_factory=dict)
    signal_weight_overrides: dict[str, float] = field(default_factory=dict)
    retry_policy: RetryPolicy = RetryPolicy.MEDIUM
    extractor_hint: ExtractorKind = ExtractorKind.SCRAPER_GENERIC_FALLBACK
    extract_threshold: float = 0.75
    manual_threshold: float = 0.55
    notes: str = ""


@dataclass(slots=True)
class FetchedPage:
    source_url: str
    final_url: str
    status: int | None
    headers: dict[str, str]
    body: str
    content_type: str | None = None
    error_type: str | None = None
    error_detail: str | None = None
    elapsed_ms: int | None = None


@dataclass(slots=True)
class EvaluationResult:
    source_url: str
    availability: Availability
    http_status: int | None
    page_type: PageType
    job_relevance: JobRelevance
    open_calls_status: OpenCallsStatus
    validity_status: ValidityStatus
    recommended_extractor: ExtractorKind | None
    decision: Decision
    reason_code: ReasonCode | None
    reason_detail: str | None
    confidence: float
    retry_policy: RetryPolicy
    signals_json: dict[str, Any]
    evaluated_at: datetime
    profile_name: str | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "source_url": self.source_url,
            "availability": self.availability.value,
            "http_status": self.http_status,
            "page_type": self.page_type.value,
            "job_relevance": self.job_relevance.value,
            "open_calls_status": self.open_calls_status.value,
            "validity_status": self.validity_status.value,
            "recommended_extractor": self.recommended_extractor.value if self.recommended_extractor else None,
            "decision": self.decision.value,
            "reason_code": self.reason_code.value if self.reason_code else None,
            "reason_detail": self.reason_detail,
            "confidence": round(self.confidence, 4),
            "retry_policy": self.retry_policy.value,
            "signals_json": self.signals_json,
            "evaluated_at": self.evaluated_at.isoformat(),
            "profile_name": self.profile_name,
        }


@dataclass(slots=True)
class QualityValidationResult:
    decision: QualityDecision
    reason_codes: list[ReasonCode] = field(default_factory=list)
    reason_detail: str | None = None
    quality_score: float = 0.0
    signals_json: dict[str, Any] = field(default_factory=dict)

    @property
    def primary_reason_code(self) -> ReasonCode | None:
        return self.reason_codes[0] if self.reason_codes else None


@dataclass(slots=True)
class CatalogBundle:
    instituciones: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)
    source: str = "json"
