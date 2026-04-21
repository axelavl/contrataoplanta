from __future__ import annotations

import io
import re
from typing import Any
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup

from scrapers.base import OfertaRaw, clean_text, normalize_key, parse_date, parse_renta

from .generic_site import GenericSiteScraper, RawCandidate

try:  # pragma: no cover - depende del entorno
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover - depende del entorno
    pdfplumber = None


PDF_TITLE_HINTS = ("perfil", "descriptor", "bases", "concurso", "cargo", "postulacion")


class PdfFirstScraper(GenericSiteScraper):
    """Variante generica donde PDFs de bases/perfil son la evidencia principal."""

    def _extract_candidates_from_listing(self, html: str, source_url: str) -> list[RawCandidate]:
        soup = BeautifulSoup(html, "html.parser")
        candidates = super()._extract_candidates_from_listing(html, source_url)
        candidates.extend(self._parse_pdf_blocks(soup, source_url))
        return self._deduplicate(candidates)

    async def _enrich_candidates(self, candidates: list[RawCandidate]) -> list[RawCandidate]:
        enriched = await super()._enrich_candidates(candidates)
        if self.http is None:
            return enriched

        results: list[RawCandidate] = []
        for candidate in enriched:
            pdf_links = list(candidate.pdf_links or [])
            merged_text = candidate.content_text
            for pdf_url in pdf_links[:2]:
                pdf_text = await self._extract_pdf_text(pdf_url)
                if pdf_text:
                    merged_text = clean_text(f"{merged_text} {pdf_text}")
            results.append(
                RawCandidate(
                    title=candidate.title,
                    content_text=merged_text,
                    url=candidate.url,
                    date_value=candidate.date_value,
                    closing_value=candidate.closing_value,
                    pdf_links=pdf_links,
                )
            )
        return results

    def _parse_pdf_blocks(self, soup: BeautifulSoup, source_url: str) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for anchor in soup.select("a[href]"):
            href = clean_text(anchor.get("href"))
            if ".pdf" not in href.lower():
                continue
            title = clean_text(anchor.get_text(" ", strip=True))
            if not any(hint in normalize_key(f"{title} {href}") for hint in PDF_TITLE_HINTS):
                continue
            parent = anchor.find_parent(["li", "p", "div", "tr", "article", "section"])
            context = clean_text(parent.get_text(" ", strip=True)) if parent else title
            url = urljoin(source_url, href)
            is_offer, _ = self._score_offer_candidate(title, context, url=url)
            if not is_offer:
                filename_title = self._title_from_pdf_url(url)
                is_offer_filename, _ = self._score_offer_candidate(filename_title, context, url=url)
                if not is_offer_filename:
                    continue
                title = filename_title
            candidates.append(
                RawCandidate(
                    title=title or self._title_from_pdf_url(url),
                    content_text=context,
                    url=url,
                    date_value=self._extract_date_hint(context),
                    closing_value=None,
                    pdf_links=[url],
                )
            )
        return candidates

    async def _extract_pdf_text(self, pdf_url: str) -> str:
        if self.http is None or pdfplumber is None:
            return ""
        binary = await self.http.get_bytes(pdf_url)
        if not binary:
            return ""
        try:
            with pdfplumber.open(io.BytesIO(binary)) as pdf:
                text_parts = []
                for page in pdf.pages[:6]:
                    page_text = page.extract_text() or ""
                    if page_text:
                        text_parts.append(page_text)
                return clean_text(" ".join(text_parts))
        except Exception:
            return ""

    @staticmethod
    def _title_from_pdf_url(pdf_url: str) -> str:
        filename = unquote(pdf_url.rsplit("/", 1)[-1])
        filename = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
        filename = re.sub(r"[-_]+", " ", filename)
        return clean_text(filename)

    def _candidate_to_oferta(self, candidate: RawCandidate) -> OfertaRaw | None:
        title = clean_text(candidate.title or (candidate.pdf_links[0] if candidate.pdf_links else ""))
        content_text = clean_text(candidate.content_text)
        is_offer, _ = self._score_offer_candidate(title, content_text, url=candidate.url)
        if not is_offer:
            return None
        fecha_publicacion = parse_date(candidate.date_value)
        fecha_cierre = parse_date(candidate.closing_value) or self._extract_closing_hint(content_text)
        renta_min, renta_max, grado_eus = parse_renta(content_text)
        url_bases = candidate.pdf_links[0] if candidate.pdf_links else None

        return OfertaRaw(
            url=candidate.url,
            cargo=title or self._crop_title(content_text),
            institucion_nombre=str(self.institucion.get("nombre") or self.nombre_fuente),
            descripcion=content_text,
            sector=self.institucion.get("sector"),
            tipo_cargo=None,
            region=self.institucion.get("region"),
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
