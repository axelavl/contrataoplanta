from __future__ import annotations

import asyncio
import json
import re
import unicodedata
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scrapers.base import HttpClient

from .date_parser import extract_dates
from .extractor_router import select_extractor
from .models import (
    Availability,
    Decision,
    EvaluationResult,
    ExtractorKind,
    FetchedPage,
    JobRelevance,
    OpenCallsStatus,
    PageType,
    SourceProfile,
    ValidityStatus,
)
from .reason_codes import ReasonCode, reason_detail
from .signals import build_signal_bundle
from .source_profiles import classify_source_profile
from .validity_rules import assess_validity


BOT_MARKERS = ("cloudflare", "captcha", "access denied", "bot detection", "waf")
JS_MARKERS = ("enable javascript", "requires javascript", "__next", "window.__", "hydration")
WORDPRESS_MARKERS = ("wp-content", "wp-json", "wordpress", "wp-includes")
NEWS_PATH_MARKERS = ("/noticias", "/news", "/prensa", "/blog", "/comunicados")
ATS_HOST_MARKERS = ("trabajando.cl", "hiringroom", "buk.cl")
RUNTIME_WORDPRESS_MARKERS = ("wp-content", "wp-includes", "wp-json", "wordpress")


def _norm(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    return " ".join(normalized.encode("ascii", "ignore").decode("ascii").lower().split())


def _availability_from_fetch(page: FetchedPage) -> Availability:
    body_norm = _norm(page.body)
    if page.error_type == "timeout":
        return Availability.TIMEOUT
    if page.error_type == "dns_error":
        return Availability.DNS_ERROR
    if page.error_type == "ssl_error":
        return Availability.SSL_ERROR
    if page.error_type == "redirect_loop":
        return Availability.REDIRECT_LOOP
    if page.status == 404:
        return Availability.HTTP_404
    if page.status == 403:
        if any(marker in body_norm for marker in BOT_MARKERS):
            return Availability.BLOCKED_BY_BOT_PROTECTION
        return Availability.HTTP_403
    if page.status and page.status >= 500:
        return Availability.HTTP_500
    if not body_norm and "pdf" not in (page.content_type or ""):
        return Availability.EMPTY_RESPONSE
    if any(marker in body_norm for marker in BOT_MARKERS):
        return Availability.BLOCKED_BY_BOT_PROTECTION
    if any(marker in body_norm for marker in JS_MARKERS) and len(body_norm) < 500:
        return Availability.JS_REQUIRED
    return Availability.OK


def _has_jobposting_jsonld(soup: BeautifulSoup) -> bool:
    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        text = _norm(json.dumps(payload, ensure_ascii=False))
        if '"@type": "jobposting"' in raw.lower() or '"@type":"jobposting"' in raw.lower() or "jobposting" in text:
            return True
    return False


def _extract_pdf_attachment_context(soup: BeautifulSoup, source_url: str, body_text: str) -> tuple[list[str], str]:
    links: list[str] = []
    attachment_fragments: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if href.lower().endswith(".pdf"):
            links.append(urljoin(source_url, href))
            anchor_text = anchor.get_text(" ", strip=True)
            fragment = f"{anchor_text} {href}".strip()
            if fragment:
                attachment_fragments.append(fragment)
    if not attachment_fragments:
        return links, body_text
    return links, f"{body_text} {' '.join(attachment_fragments)}".strip()


def _has_pdf_bases_or_profile(pdf_links: list[str], expanded_text: str) -> bool:
    expanded_norm = _norm(expanded_text)
    return any("bases" in _norm(link) or "perfil" in _norm(link) for link in pdf_links) or (
        "bases" in expanded_norm and "pdf" in expanded_norm
    ) or ("perfil del cargo" in expanded_norm and "pdf" in expanded_norm)


def _build_pre_discovery_urls(*, source_url: str, profile: SourceProfile) -> list[str]:
    parsed_source = urlparse(source_url)
    source_origin = f"{parsed_source.scheme}://{parsed_source.netloc}" if parsed_source.scheme and parsed_source.netloc else source_url
    urls: list[str] = [source_url]
    for candidate in profile.candidate_urls:
        if len(urls) >= 3:
            break
        if not candidate:
            continue
        parsed = urlparse(candidate)
        if parsed.scheme and parsed.netloc:
            # La ruta debe ser "derivada de perfil": proyectamos el path del candidate
            # sobre el origen de la fuente base para evitar mezclas de hosts.
            derived = urljoin(source_origin, parsed.path or "/")
        else:
            derived = urljoin(source_origin, candidate)
        if derived not in urls:
            urls.append(derived)
    return urls[:3]


def _page_type_priority(page_type: PageType) -> int:
    order = {
        PageType.DOCUMENT_PAGE: 8,
        PageType.ATS_EXTERNAL: 7,
        PageType.DETAIL_PAGE: 6,
        PageType.WORDPRESS_POST: 5,
        PageType.LISTING_PAGE: 4,
        PageType.WORDPRESS_LISTING: 3,
        PageType.NEWS_PAGE: 2,
        PageType.GENERAL_PAGE: 1,
        PageType.UNKNOWN_PAGE_TYPE: 0,
    }
    return order.get(page_type, 0)


def _runtime_hints(page: FetchedPage, soup: BeautifulSoup) -> tuple[str, ...]:
    hints: set[str] = set()
    blobs: list[str] = [page.final_url.lower()]
    blobs.extend(f"{k}:{v}".lower() for k, v in (page.headers or {}).items())
    for script in soup.find_all("script", src=True):
        blobs.append(str(script.get("src") or "").lower())
    for meta in soup.find_all("meta"):
        blobs.append(str(meta.get("name") or "").lower())
        blobs.append(str(meta.get("property") or "").lower())
        blobs.append(str(meta.get("content") or "").lower())
    for anchor in soup.find_all("a", href=True):
        blobs.append(str(anchor.get("href") or "").lower())

    if any(marker in blob for blob in blobs for marker in RUNTIME_WORDPRESS_MARKERS):
        hints.add("cms_wordpress")
    if any("trabajando.cl" in blob for blob in blobs):
        hints.add("ats_trabajando")
    if any("hiringroom" in blob for blob in blobs):
        hints.add("ats_hiringroom")
    if any("buk.cl" in blob for blob in blobs):
        hints.add("ats_buk")
    return tuple(sorted(hints))


def _infer_page_type(*, page: FetchedPage, soup: BeautifulSoup, profile: SourceProfile) -> tuple[PageType, str | None]:
    final_url = page.final_url.lower()
    content_type = (page.content_type or "").lower()
    body_norm = _norm(page.body)
    title = soup.title.get_text(" ", strip=True) if soup.title else None

    if "pdf" in content_type or final_url.endswith(".pdf"):
        return PageType.DOCUMENT_PAGE, None
    if any(marker in final_url for marker in ATS_HOST_MARKERS):
        return PageType.ATS_EXTERNAL, None

    is_wordpress = any(marker in body_norm or marker in final_url for marker in WORDPRESS_MARKERS) or profile.name == "wordpress"
    if is_wordpress:
        article = soup.find("article")
        if article or soup.find("body", class_=re.compile("single|post-template", re.I)):
            return PageType.WORDPRESS_POST, "wordpress"
        return PageType.WORDPRESS_LISTING, "wordpress"

    if any(marker in final_url for marker in NEWS_PATH_MARKERS):
        return PageType.NEWS_PAGE, None

    if soup.find_all("article") and len(soup.find_all("article")) > 1:
        return PageType.LISTING_PAGE, None

    if len(soup.find_all("a", href=True)) >= 12 and len(soup.find_all(["table", "li"])) >= 8:
        return PageType.LISTING_PAGE, None

    if soup.find("h1") and (soup.find(text=re.compile("requisitos|funciones|remuneraci", re.I)) or soup.find("table")):
        return PageType.DETAIL_PAGE, None

    if title and _norm(title) in {"concursos", "trabaja con nosotros", "ofertas laborales"}:
        return PageType.LISTING_PAGE, None

    return PageType.GENERAL_PAGE, None


def _infer_job_relevance(text: str, page_type: PageType, confidence: float) -> JobRelevance:
    text_norm = _norm(text)
    positive_terms = sum(1 for token in ("concurso publico", "postulacion", "bases", "perfil del cargo", "vacante", "honorarios", "contrata", "planta", "recepcion de antecedentes") if token in text_norm)
    negative_terms = sum(1 for token in ("subsidio", "beca", "fondo concursable", "noticia", "actividad", "taller", "beneficio", "tramite", "licitacion", "curso", "feria") if token in text_norm)
    if positive_terms >= 3 and negative_terms == 0:
        return JobRelevance.JOB_LIKE
    if negative_terms >= 3 and positive_terms == 0:
        return JobRelevance.NON_JOB
    if positive_terms >= 2 and negative_terms >= 1:
        return JobRelevance.MIXED
    if confidence < 0.55 and page_type in {PageType.NEWS_PAGE, PageType.GENERAL_PAGE}:
        return JobRelevance.NON_JOB
    return JobRelevance.UNCERTAIN


class SourceEvaluator:
    def __init__(self, http_client: HttpClient | None = None, *, reference_date: date | None = None) -> None:
        self.http = http_client
        self.reference_date = reference_date or date.today()

    async def evaluate(
        self,
        source: dict[str, Any],
        *,
        historical_noise_ratio: float = 0.0,
        historical_source_metrics: dict[str, Any] | None = None,
    ) -> EvaluationResult:
        source_url = str(source.get("url_empleo") or source.get("sitio_web") or "").strip()
        profile_match = classify_source_profile(source)
        profile = profile_match.profile
        if not source_url:
            signals_json = {
                "profile": profile.name,
                "profile_matched_by": profile_match.matched_by,
                "runtime_hints": [],
            }
            if profile_match.source_requires_override:
                signals_json.update(
                    {
                        "source_requires_override": True,
                        "override_backlog_severity": profile_match.backlog_severity,
                        "override_evidence": profile_match.evidence,
                    }
                )
            return EvaluationResult(
                source_url="",
                availability=Availability.EMPTY_RESPONSE,
                http_status=None,
                page_type=PageType.UNKNOWN_PAGE_TYPE,
                job_relevance=JobRelevance.UNCERTAIN,
                open_calls_status=OpenCallsStatus.UNKNOWN_STATUS,
                validity_status=assess_validity(
                    page_type=PageType.UNKNOWN_PAGE_TYPE,
                    text="",
                    publication_date=None,
                    closing_date=None,
                    application_deadline=None,
                    reference_date=self.reference_date,
                ).status,
                recommended_extractor=None,
                decision=Decision.SOURCE_STATUS_ONLY,
                reason_code=ReasonCode.CATALOG_MISMATCH,
                reason_detail="La fuente no trae url_empleo ni sitio_web.",
                confidence=0.0,
                retry_policy=profile.retry_policy,
                signals_json=signals_json,
                evaluated_at=datetime.now(),
                profile_name=profile.name,
            )

        owns_http = self.http is None
        client = self.http or HttpClient()
        if owns_http:
            await client.__aenter__()
        try:
            pre_discovery_urls = _build_pre_discovery_urls(source_url=source_url, profile=profile)
            fetched_pages = await asyncio.gather(*(client.fetch(url) for url in pre_discovery_urls))
        finally:
            if owns_http:
                await client.__aexit__(None, None, None)

        pages: list[FetchedPage] = [
            FetchedPage(
                source_url=source_url,
                final_url=fetched.final_url,
                status=fetched.status,
                headers=fetched.headers,
                body=fetched.body or "",
                content_type=fetched.headers.get("Content-Type"),
                error_type=fetched.error_type,
                error_detail=fetched.error_detail,
            )
            for fetched in fetched_pages
        ]
        page_with_availability = [(page, _availability_from_fetch(page)) for page in pages]
        ok_pages = [page for page, availability in page_with_availability if availability == Availability.OK]
        first_page = pages[0]
        availability = (
            Availability.OK
            if ok_pages
            else next(
                (item_availability for item_page, item_availability in page_with_availability if item_page.final_url == first_page.final_url),
                Availability.EMPTY_RESPONSE,
            )
        )
        if availability != Availability.OK:
            signals_json = {
                "profile": profile.name,
                "profile_matched_by": profile_match.matched_by,
                "runtime_hints": [],
                "error_type": first_page.error_type,
                "error_detail": first_page.error_detail,
                "content_type": first_page.content_type,
                "pre_discovery_urls": [page.final_url for page in pages],
            }
            if profile_match.source_requires_override:
                signals_json.update(
                    {
                        "source_requires_override": True,
                        "override_backlog_severity": profile_match.backlog_severity,
                        "override_evidence": profile_match.evidence,
                    }
                )
            selection = select_extractor(
                profile,
                availability=availability,
                page_type=PageType.UNKNOWN_PAGE_TYPE,
                job_relevance=JobRelevance.UNCERTAIN,
                validity_status=assess_validity(
                    page_type=PageType.UNKNOWN_PAGE_TYPE,
                    text="",
                    publication_date=None,
                    closing_date=None,
                    application_deadline=None,
                    reference_date=self.reference_date,
                ).status,
                confidence=0.0,
                source_quality_metrics=historical_source_metrics,
            )
            signals_json.update(
                {
                    "extract_threshold_applied": selection.extract_threshold_applied,
                    "manual_threshold_applied": selection.manual_threshold_applied,
                    "threshold_validation": selection.threshold_validation,
                }
            )
            return EvaluationResult(
                source_url=source_url,
                availability=availability,
                http_status=first_page.status,
                page_type=PageType.UNKNOWN_PAGE_TYPE,
                job_relevance=JobRelevance.UNCERTAIN,
                open_calls_status=OpenCallsStatus.UNKNOWN_STATUS,
                validity_status=assess_validity(
                    page_type=PageType.UNKNOWN_PAGE_TYPE,
                    text="",
                    publication_date=None,
                    closing_date=None,
                    application_deadline=None,
                    reference_date=self.reference_date,
                ).status,
                recommended_extractor=selection.recommended_extractor,
                decision=selection.decision,
                reason_code=selection.reason_code,
                reason_detail=selection.reason_detail,
                confidence=0.0,
                retry_policy=profile.retry_policy,
                signals_json=signals_json,
                evaluated_at=datetime.now(),
                profile_name=profile.name,
            )

        aggregated_html_parts: list[str] = []
        aggregated_text_parts: list[str] = []
        aggregated_expanded_parts: list[str] = []
        aggregated_titles: list[str] = []
        aggregated_pdf_links: list[str] = []
        aggregated_links: list[str] = []
        inferred_page_types: list[PageType] = []
        aggregated_runtime_hints: list[str] = []
        has_jobposting_jsonld = False
        cms: str | None = None

        for page in ok_pages:
            page_soup = BeautifulSoup(page.body, "html.parser")
            for hint in _runtime_hints(page, page_soup):
                if hint not in aggregated_runtime_hints:
                    aggregated_runtime_hints.append(hint)
            page_type, page_cms = _infer_page_type(page=page, soup=page_soup, profile=profile)
            inferred_page_types.append(page_type)
            if cms is None and page_cms:
                cms = page_cms
            if page_type == PageType.DOCUMENT_PAGE and page.final_url not in aggregated_pdf_links:
                aggregated_pdf_links.append(page.final_url)
            page_body_text = page_soup.get_text(" ", strip=True)
            page_pdf_links, page_expanded_text = _extract_pdf_attachment_context(
                page_soup, page.final_url, page_body_text
            )
            for pdf in page_pdf_links:
                if pdf not in aggregated_pdf_links:
                    aggregated_pdf_links.append(pdf)
            for anchor in page_soup.find_all("a", href=True):
                full_link = urljoin(page.final_url, anchor["href"])
                if full_link not in aggregated_links:
                    aggregated_links.append(full_link)
            if page_body_text:
                aggregated_text_parts.append(page_body_text)
            if page_expanded_text:
                aggregated_expanded_parts.append(page_expanded_text)
            if page.body:
                aggregated_html_parts.append(page.body)
            has_jobposting_jsonld = has_jobposting_jsonld or _has_jobposting_jsonld(page_soup)
            if page_soup.title:
                title_text = page_soup.title.get_text(" ", strip=True)
                if title_text:
                    aggregated_titles.append(title_text)

        runtime_hints = tuple(aggregated_runtime_hints)
        profile_match = classify_source_profile(source, runtime_hints=runtime_hints)
        profile = profile_match.profile
        representative_page_type = (
            sorted(inferred_page_types, key=_page_type_priority, reverse=True)[0]
            if inferred_page_types
            else PageType.UNKNOWN_PAGE_TYPE
        )
        body_text = " ".join(aggregated_text_parts)
        expanded_text = " ".join(aggregated_expanded_parts) if aggregated_expanded_parts else body_text
        has_pdf_bases_or_profile = _has_pdf_bases_or_profile(aggregated_pdf_links, expanded_text)
        combined_html = "\n".join(aggregated_html_parts)
        title = aggregated_titles[0] if aggregated_titles else None
        dates = extract_dates(html=combined_html, text=expanded_text, reference_date=self.reference_date)
        validity = assess_validity(
            page_type=representative_page_type,
            text=body_text,
            publication_date=dates.publication_date,
            closing_date=dates.closing_date,
            application_deadline=dates.application_deadline,
            expanded_text=expanded_text,
            has_pdf_bases_or_profile=has_pdf_bases_or_profile,
            reference_date=self.reference_date,
        )
        signal_bundle = build_signal_bundle(
            source_url=ok_pages[0].final_url if ok_pages else source_url,
            title=title,
            text=body_text,
            page_type=representative_page_type,
            profile=profile,
            publication_date=dates.publication_date,
            closing_date=dates.closing_date,
            application_deadline=dates.application_deadline,
            has_jobposting_jsonld=has_jobposting_jsonld,
            pdf_links=aggregated_pdf_links,
            known_ats=representative_page_type == PageType.ATS_EXTERNAL or profile.extractor_hint == ExtractorKind.SCRAPER_EXTERNAL_ATS,
            bot_or_js=availability in {Availability.JS_REQUIRED, Availability.BLOCKED_BY_BOT_PROTECTION},
            open_signal_count=validity.open_signal_count,
            cms=cms,
            today=self.reference_date,
        )
        confidence = signal_bundle.confidence
        if historical_noise_ratio > 0:
            penalty = min(0.2, historical_noise_ratio * 0.25)
            confidence = max(0.0, round(confidence - penalty, 4))
            signal_bundle.negative_signals.append("historical_noise_penalty")
            signal_bundle.metadata["historical_noise_ratio"] = round(historical_noise_ratio, 4)

        job_relevance = _infer_job_relevance(body_text, representative_page_type, confidence)
        open_calls_status = validity.open_calls_status
        if job_relevance == JobRelevance.NON_JOB:
            open_calls_status = OpenCallsStatus.NO_CALLS_FOUND

        effective_confidence = confidence
        if profile.trusted_job_source and validity.status != ValidityStatus.EXPIRED_CONFIRMED:
            effective_confidence = max(confidence, 0.8)

        selection = select_extractor(
            profile,
            availability=availability,
            page_type=representative_page_type,
            job_relevance=job_relevance,
            validity_status=validity.status,
            confidence=effective_confidence,
            source_quality_metrics=historical_source_metrics,
        )

        if selection.decision == Decision.SKIP and selection.reason_code is None:
            if job_relevance == JobRelevance.NON_JOB:
                selection.reason_code = ReasonCode.NOT_JOB_RELATED
            elif open_calls_status == OpenCallsStatus.NO_CALLS_FOUND:
                selection.reason_code = ReasonCode.NO_CALLS_FOUND
            elif open_calls_status == OpenCallsStatus.ONLY_EXPIRED_CALLS:
                selection.reason_code = ReasonCode.ONLY_EXPIRED_CALLS
            selection.reason_detail = reason_detail(selection.reason_code)
        if validity.status == ValidityStatus.MANUAL_REVIEW and selection.decision == Decision.SKIP:
            selection.decision = Decision.MANUAL_REVIEW
            selection.reason_code = ReasonCode.MANUAL_REVIEW_REQUIRED
            selection.reason_detail = reason_detail(ReasonCode.MANUAL_REVIEW_REQUIRED)

        signals_json = signal_bundle.to_json()
        signals_json.update(
            {
                "profile": profile.name,
                "profile_matched_by": profile_match.matched_by,
                "runtime_hints": list(runtime_hints),
                "page_title": title,
                "pdf_links_count": len(aggregated_pdf_links),
                "pdf_links": aggregated_pdf_links[:5],
                "discovered_links_count": len(aggregated_links),
                "pre_discovery": {
                    "institucion_id": source.get("id"),
                    "evaluated_urls_snapshot": [
                        {
                            "url": page.final_url,
                            "status": page.status,
                            "availability": item_availability.value,
                        }
                        for page, item_availability in page_with_availability
                    ],
                },
                "evaluated_urls_snapshot": [
                    {
                        "url": page.final_url,
                        "status": page.status,
                        "availability": item_availability.value,
                    }
                    for page, item_availability in page_with_availability
                ],
                "institucion_id": source.get("id"),
                **dates.to_json(),
                "open_calls_status": open_calls_status.value,
                "age_expiry_evidence": validity.age_expiry_evidence,
                "extract_threshold_applied": selection.extract_threshold_applied,
                "manual_threshold_applied": selection.manual_threshold_applied,
                "threshold_validation": selection.threshold_validation,
            }
        )
        if profile_match.source_requires_override:
            signals_json.update(
                {
                    "source_requires_override": True,
                    "override_backlog_severity": profile_match.backlog_severity,
                    "override_evidence": profile_match.evidence,
                }
            )

        return EvaluationResult(
            source_url=source_url,
            availability=availability,
            http_status=first_page.status,
            page_type=representative_page_type,
            job_relevance=job_relevance,
            open_calls_status=open_calls_status,
            validity_status=validity.status,
            recommended_extractor=selection.recommended_extractor,
            decision=selection.decision,
            reason_code=selection.reason_code or validity.reason_code,
            reason_detail=selection.reason_detail or validity.reason_detail,
            confidence=effective_confidence,
            retry_policy=profile.retry_policy,
            signals_json=signals_json,
            evaluated_at=datetime.now(),
            profile_name=profile.name,
        )
