from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from scrapers.base import HttpFetchResult, clean_text

from .pdf_first import PdfFirstScraper
from .generic_site import RawCandidate


CARABINEROS_URLS = (
    "/",
    "/concursos",
    "/convocatorias",
)

CARABINEROS_KEYWORDS = (
    "personal civil",
    "descriptor",
    "perfil",
    "dotacion",
    "dotación",
    "postulacion",
)


class CarabinerosScraper(PdfFirstScraper):
    """Scraper robusto para postulaciones de Carabineros (fuente 161)."""

    PRIMARY_HOST = "postulaciones.carabineros.cl"
    SECONDARY_FALLBACK_URLS = (
        "https://www.carabineros.cl/transparencia/concursos/",
        "https://www.carabineros.cl/transparencia/trabaje-con-nosotros/",
    )

    def __init__(self, *, fuente_id: int, institucion: dict[str, Any]) -> None:
        self.enable_secondary_fallback = bool(institucion.get("enable_secondary_host_fallback"))
        super().__init__(
            fuente_id=fuente_id,
            institucion=institucion,
            candidate_paths=CARABINEROS_URLS,
            extra_keywords=CARABINEROS_KEYWORDS,
            max_candidate_urls=4,
            detail_fetch_limit=10,
            trusted_host_only=True,
        )

    async def descubrir_ofertas(self) -> list[Any]:
        if self.http is None:
            raise RuntimeError("CarabinerosScraper requiere un HttpClient activo.")

        candidates: list[RawCandidate] = []
        seen_urls: set[str] = set()
        successful_source_url: str | None = None
        network_failures = 0

        preferred_url = self._load_preferred_success_url()
        for source_url in self._candidate_urls(max_urls=self.max_candidate_urls, preferred_url=preferred_url):
            fetch = await self._fetch_listing(source_url)
            if not fetch.body:
                network_failures += 1
                continue

            page_candidates = self._extract_candidates_from_listing(fetch.body, source_url)
            if page_candidates and successful_source_url is None:
                successful_source_url = source_url

            for candidate in page_candidates:
                key = candidate.url or candidate.title
                if not key or key in seen_urls:
                    continue
                seen_urls.add(key)
                candidates.append(candidate)

        if successful_source_url:
            self._save_successful_source_url(successful_source_url)

        enriched = await self._enrich_candidates_with_trace(candidates)
        offers = []
        seen_offer_urls: set[str] = set()
        for candidate in enriched:
            oferta = self._candidate_to_oferta(candidate)
            if oferta is None or oferta.url in seen_offer_urls:
                continue
            seen_offer_urls.add(oferta.url)
            offers.append(oferta)

        self.log.info(
            "evento=carabineros_summary fuente_id=%s candidates=%s network_failures=%s offers=%s secondary_fallback=%s",
            self.fuente_id,
            len(candidates),
            network_failures,
            len(offers),
            self.enable_secondary_fallback,
        )
        return offers

    def _candidate_urls(self, *, max_urls: int, preferred_url: str | None = None) -> list[str]:
        empleo = clean_text(self.url_empleo)
        seeds = [empleo] if empleo else []

        urls: list[str] = []
        if preferred_url and self._is_primary_host(preferred_url):
            urls.append(preferred_url)
        urls.extend(seeds)

        base = self._base_url(empleo) if empleo else ""
        if base:
            urls.extend(f"{base}{suffix}" for suffix in self.candidate_paths)

        if self.enable_secondary_fallback:
            urls.extend(self.SECONDARY_FALLBACK_URLS)

        return self._deduplicate_urls(urls)[:max_urls]

    async def _fetch_listing(self, source_url: str) -> HttpFetchResult:
        if self.http is None:
            raise RuntimeError("HttpClient no inicializado")

        result = await self.http.fetch(source_url)
        if self._should_retry_once(source_url, result):
            self.log.warning(
                "evento=carabineros_retry_once fase=listing url=%s error_type=%s",
                source_url,
                result.error_type,
            )
            result = await self.http.fetch(source_url)

        self.log.info(
            "evento=carabineros_fetch fase=listing url=%s status=%s error_type=%s final_url=%s",
            source_url,
            result.status,
            result.error_type,
            result.final_url,
        )
        return result

    async def _enrich_candidates_with_trace(self, candidates: list[RawCandidate]) -> list[RawCandidate]:
        if self.http is None:
            return candidates

        enriched: list[RawCandidate] = []
        limit = 0

        for candidate in candidates:
            if limit >= self.detail_fetch_limit:
                enriched.append(candidate)
                continue
            if not candidate.url or candidate.url.lower().endswith(".pdf"):
                enriched.append(candidate)
                continue
            if candidate.url.rstrip("/") == (self.url_empleo or "").rstrip("/"):
                enriched.append(candidate)
                continue

            result = await self.http.fetch(candidate.url)
            if self._should_retry_once(candidate.url, result):
                self.log.warning(
                    "evento=carabineros_retry_once fase=detalle url=%s error_type=%s",
                    candidate.url,
                    result.error_type,
                )
                result = await self.http.fetch(candidate.url)

            self.log.info(
                "evento=carabineros_fetch fase=detalle url=%s status=%s error_type=%s",
                candidate.url,
                result.status,
                result.error_type,
            )

            if not result.body:
                enriched.append(candidate)
                continue

            limit += 1
            soup = BeautifulSoup(result.body, "html.parser")
            body_text = clean_text(soup.get_text(" ", strip=True))
            pdf_links = list(candidate.pdf_links or [])
            pdf_links.extend(self._extract_pdf_links_from_node(soup, candidate.url))
            heading = soup.find(["h1", "h2"])
            title = candidate.title
            if heading:
                title = clean_text(heading.get_text(" ", strip=True)) or title

            enriched.append(
                RawCandidate(
                    title=title,
                    content_text=body_text or candidate.content_text,
                    url=candidate.url,
                    date_value=candidate.date_value or self._extract_date_hint(result.body),
                    closing_value=candidate.closing_value or self._extract_closing_hint(body_text),
                    pdf_links=self._deduplicate_urls(pdf_links),
                )
            )

        return enriched

    def _should_retry_once(self, url: str, result: HttpFetchResult) -> bool:
        if not self._is_primary_host(url):
            return False
        return result.error_type in {"timeout", "client_error", "dns_error"} and not result.body

    def _is_primary_host(self, url: str | None) -> bool:
        if not url:
            return False
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host == self.PRIMARY_HOST
