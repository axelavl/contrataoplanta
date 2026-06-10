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
except Exception as _pdfplumber_import_error:  # pragma: no cover
    pdfplumber = None
    _PDFPLUMBER_IMPORT_ERROR: Exception | None = _pdfplumber_import_error
else:
    _PDFPLUMBER_IMPORT_ERROR = None


PDF_TITLE_HINTS = ("perfil", "descriptor", "bases", "concurso", "cargo", "postulacion")
# Para priorizar qué PDF leer cuando una candidata adjunta varios.
_PDF_RELEVANCE_HINTS = ("bases", "perfil", "descriptor", "concurso", "tdr", "convocatoria")


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
        if pdfplumber is None and _PDFPLUMBER_IMPORT_ERROR is not None:
            self.log.warning(
                "evento=pdfplumber_no_disponible scraper=%s error=%s — los PDFs no se enriquecerán.",
                self.nombre_fuente,
                _PDFPLUMBER_IMPORT_ERROR,
            )

        results: list[RawCandidate] = []
        for candidate in enriched:
            pdf_links = list(candidate.pdf_links or [])
            ordered = self._sort_pdfs_by_relevance(pdf_links)
            merged_text = candidate.content_text
            for pdf_url in ordered[:2]:
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

    @staticmethod
    def _sort_pdfs_by_relevance(pdf_links: list[str]) -> list[str]:
        """Pone primero los PDFs cuyo nombre contenga hints de bases/perfil/etc.

        Antes se leían los primeros 2 PDFs en orden de aparición; cuando el PDF
        relevante venía en posición 3+ (típico cuando el primero es un genérico
        institucional o de privacidad), se ignoraba.
        """
        def score(url: str) -> int:
            name = unquote(url).lower()
            for i, hint in enumerate(_PDF_RELEVANCE_HINTS):
                if hint in name:
                    return i
            return len(_PDF_RELEVANCE_HINTS)
        return sorted(pdf_links, key=score)

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
        try:
            binary = await self.http.get_bytes(pdf_url)
        except Exception as exc:  # red, timeout, WAF, redirección rota
            self.log.info(
                "evento=pdf_fetch_error scraper=%s url=%s error=%s:%s",
                self.nombre_fuente, pdf_url, type(exc).__name__, exc,
            )
            return ""
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
        except Exception as exc:
            # PDF cifrado/corrupto/imagen escaneada: preferimos seguir vivos
            # con un texto vacío antes que abortar la fuente, pero queremos
            # rastro mínimo para distinguir esto de un PDF realmente vacío.
            self.log.info(
                "evento=pdf_parse_error scraper=%s url=%s error=%s:%s",
                self.nombre_fuente, pdf_url, type(exc).__name__, exc,
            )
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
