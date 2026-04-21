from __future__ import annotations

import json
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import OfertaRaw, clean_text, normalize_region, normalize_tipo_contrato, parse_date, parse_renta

from .generic_site import GenericSiteScraper, RawCandidate


class PlaywrightScraper(GenericSiteScraper):
    """Scraper real para fuentes JS intensivas usando Playwright async."""

    CARD_SELECTORS: tuple[str, ...] = (
        "article",
        "li",
        "div.card",
        "div.job",
        "div.vacante",
        "tr",
        "a[href*='job']",
        "a[href*='emple']",
    )

    def __init__(self, *, fuente_id: int, institucion: dict[str, Any]) -> None:
        super().__init__(
            fuente_id=fuente_id,
            institucion=institucion,
            candidate_paths=("/careers", "/trabaja-con-nosotros", "/empleos", "/jobs"),
            extra_keywords=("vacante", "position", "job", "careers"),
            max_candidate_urls=4,
            detail_fetch_limit=10,
            trusted_host_only=False,
        )
        self.signals_json: dict[str, Any] = {
            "source": {
                "fuente_id": fuente_id,
                "institucion_id": institucion.get("id"),
                "nombre": institucion.get("nombre") or institucion.get("sigla"),
            },
            "playwright": {
                "attempted": False,
                "status": "not_started",
            },
        }

    async def descubrir_ofertas(self) -> list[OfertaRaw]:
        entry_url = self.url_empleo or self.sitio_web
        if not entry_url:
            return await super().descubrir_ofertas()

        timeout_ms = self._source_timeout_ms()
        user_agent = self._source_user_agent()

        self.signals_json["playwright"].update(
            {
                "attempted": True,
                "entry_url": entry_url,
                "timeout_ms": timeout_ms,
                "user_agent": user_agent,
            }
        )

        try:
            candidates = await self._extract_with_playwright(
                entry_url=entry_url,
                timeout_ms=timeout_ms,
                user_agent=user_agent,
            )
            enriched = await self._enrich_candidates(candidates)
            offers: list[OfertaRaw] = []
            seen_offer_urls: set[str] = set()
            for candidate in enriched:
                oferta = self._candidate_to_oferta_playwright(candidate)
                if oferta is None or oferta.url in seen_offer_urls:
                    continue
                seen_offer_urls.add(oferta.url)
                offers.append(oferta)

            self.signals_json["playwright"].update(
                {
                    "status": "ok",
                    "raw_candidates": len(candidates),
                    "offers": len(offers),
                }
            )
            self.log.info("playwright signals_json fuente=%s %s", self.fuente_id, json.dumps(self.signals_json, ensure_ascii=False))
            return offers
        except Exception as exc:
            self.signals_json["playwright"].update(
                {
                    "status": "fallback_generic",
                    "reason_code": "js_render_failed",
                    "error": str(exc)[:240],
                }
            )
            self.log.warning(
                "Playwright fallo para fuente_id=%s reason_code=js_render_failed: %s", self.fuente_id, exc
            )
            self.log.info("playwright signals_json fuente=%s %s", self.fuente_id, json.dumps(self.signals_json, ensure_ascii=False))
            return await super().descubrir_ofertas()

    async def _extract_with_playwright(self, *, entry_url: str, timeout_ms: int, user_agent: str) -> list[RawCandidate]:
        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
            from playwright.async_api import async_playwright
        except Exception as exc:  # pragma: no cover - depende del entorno
            raise RuntimeError("Playwright no disponible en entorno") from exc

        candidates: list[RawCandidate] = []
        seen_urls: set[str] = set()

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=user_agent)
            page = await context.new_page()
            page.set_default_timeout(timeout_ms)

            try:
                await page.goto(entry_url, wait_until="networkidle", timeout=timeout_ms)
                await self._wait_card_selector(page, timeout_ms)

                # Snapshot HTML y extracción híbrida (DOM renderizado + parser reutilizable)
                html = await page.content()
                parser_candidates = self._extract_candidates_from_listing(html, entry_url)
                for candidate in parser_candidates:
                    key = candidate.url or candidate.title
                    if key and key not in seen_urls:
                        seen_urls.add(key)
                        candidates.append(candidate)

                card_candidates = await self._extract_cards_from_page(page, entry_url)
                for candidate in card_candidates:
                    key = candidate.url or candidate.title
                    if key and key not in seen_urls:
                        seen_urls.add(key)
                        candidates.append(candidate)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(f"Timeout renderizando {entry_url}") from exc
            finally:
                await context.close()
                await browser.close()

        return candidates

    async def _wait_card_selector(self, page: Any, timeout_ms: int) -> None:
        for selector in self.CARD_SELECTORS:
            try:
                await page.wait_for_selector(selector, timeout=max(1500, timeout_ms // 3), state="attached")
                return
            except Exception:
                continue
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _extract_cards_from_page(self, page: Any, source_url: str) -> list[RawCandidate]:
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        results: list[RawCandidate] = []

        for selector in self.CARD_SELECTORS:
            for node in soup.select(selector):
                text = clean_text(node.get_text(" ", strip=True))
                is_offer, _ = self._score_offer_candidate("", text, url=source_url)
                if not text or not is_offer:
                    continue
                anchor = node.select_one("a[href]")
                href = clean_text(anchor.get("href") if anchor else "")
                title = clean_text(anchor.get_text(" ", strip=True) if anchor else "")
                if href and not self._trusted_url(href, source_url):
                    continue
                results.append(
                    RawCandidate(
                        title=title or self._crop_title(text),
                        content_text=text,
                        url=urljoin(source_url, href) if href else source_url,
                        date_value=self._extract_date_hint(text),
                        pdf_links=self._extract_pdf_links_from_node(node, source_url),
                    )
                )
        return self._deduplicate(results)

    def _candidate_to_oferta_playwright(self, candidate: RawCandidate) -> OfertaRaw | None:
        title = clean_text(candidate.title)
        content_text = clean_text(candidate.content_text)
        is_offer, _ = self._score_offer_candidate(title, content_text, url=candidate.url)
        if not is_offer:
            return None

        fecha_publicacion = parse_date(candidate.date_value)
        fecha_cierre = parse_date(candidate.closing_value) or self._extract_closing_hint(content_text)
        renta_min, renta_max, grado_eus = parse_renta(content_text)

        url_bases = None
        if candidate.pdf_links:
            url_bases = candidate.pdf_links[0]
        elif candidate.url.lower().endswith(".pdf"):
            url_bases = candidate.url

        return OfertaRaw(
            url=candidate.url,
            cargo=title or self._crop_title(content_text),
            institucion_nombre=str(self.institucion.get("nombre") or self.nombre_fuente),
            descripcion=content_text,
            sector=self.institucion.get("sector"),
            tipo_cargo=normalize_tipo_contrato(f"{title} {content_text}"),
            region=normalize_region(self.institucion.get("region")),
            ciudad=self._infer_city(self.institucion.get("nombre")),
            renta_texto=None,
            renta_min=renta_min,
            renta_max=renta_max,
            grado_eus=grado_eus,
            fecha_publicacion=fecha_publicacion,
            fecha_cierre=fecha_cierre,
            area_profesional=self._infer_area(title or content_text),
            url_bases=url_bases,
        )

    def _source_timeout_ms(self) -> int:
        for key in ("playwright_timeout_ms", "timeout_ms", "timeout"):
            value = self.institucion.get(key)
            if value is None:
                continue
            try:
                parsed = int(value)
                if parsed > 0:
                    return parsed
            except (TypeError, ValueError):
                continue
        return 45_000

    def _source_user_agent(self) -> str:
        ua = clean_text(self.institucion.get("user_agent") or self.institucion.get("playwright_user_agent"))
        if ua:
            return ua
        return (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
