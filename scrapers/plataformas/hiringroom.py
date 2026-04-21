from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scrapers.base import OfertaRaw, clean_text, normalize_region, normalize_tipo_contrato, parse_date
from .generic_site import GenericSiteScraper


HIRINGROOM_KEYWORDS = (
    "hiringroom",
    "vacante",
    "job",
    "empleo",
    "position",
    "modalidad",
)


class HiringRoomScraper(GenericSiteScraper):
    """Adaptador async para portales HiringRoom."""

    def __init__(self, *, fuente_id: int, institucion: dict[str, Any]) -> None:
        super().__init__(
            fuente_id=fuente_id,
            institucion=institucion,
            candidate_paths=("/jobs", "/empleos", "/trabajos", "/careers"),
            extra_keywords=HIRINGROOM_KEYWORDS,
            max_candidate_urls=3,
            detail_fetch_limit=16,
            trusted_host_only=False,
        )
        self.hiringroom_structured_hits = 0
        self.fallback_hits = 0

    async def descubrir_ofertas(self) -> list[OfertaRaw]:
        if self.http is None:
            raise RuntimeError("HiringRoomScraper requiere HttpClient activo.")

        structured_candidates = []
        seen_urls: set[str] = set()
        for source_url in self._candidate_urls():
            html = await self.http.get(source_url)
            if not isinstance(html, str) or not html.strip():
                continue
            parsed = self._parse_structured_hiringroom(html, source_url)
            for candidate in parsed:
                if not candidate.url or candidate.url in seen_urls:
                    continue
                seen_urls.add(candidate.url)
                structured_candidates.append(candidate)

        if structured_candidates:
            enriched = await self._enrich_candidates(structured_candidates)
            ofertas: list[OfertaRaw] = []
            for candidate in enriched:
                oferta = self._candidate_to_oferta(candidate)
                if oferta:
                    ofertas.append(oferta)
            self.hiringroom_structured_hits = len(ofertas)
            self.fallback_hits = 0
            self.log.info(
                "evento=hiringroom_metrics structured_hits=%s fallback_hits=%s",
                self.hiringroom_structured_hits,
                self.fallback_hits,
            )
            return ofertas

        fallback_offers = await super().descubrir_ofertas()
        self.hiringroom_structured_hits = 0
        self.fallback_hits = len(fallback_offers)
        self.log.info(
            "evento=hiringroom_metrics structured_hits=%s fallback_hits=%s",
            self.hiringroom_structured_hits,
            self.fallback_hits,
        )
        return fallback_offers

    def _parse_structured_hiringroom(self, html: str, source_url: str):
        soup = BeautifulSoup(html, "html.parser")
        candidates = []
        candidates.extend(self._parse_jobs_json_endpoints(soup, source_url))
        candidates.extend(self._parse_serialized_blocks(soup, source_url))
        return self._deduplicate(candidates)

    def _parse_jobs_json_endpoints(self, soup: BeautifulSoup, source_url: str):
        results = []
        for script in soup.select('script[type="application/ld+json"]'):
            data = self._safe_json_load(script.string or script.get_text(" ", strip=True))
            for item in self._walk_json(data):
                if not isinstance(item, dict):
                    continue
                title = clean_text(item.get("title") or item.get("name") or item.get("jobTitle"))
                detail_url = clean_text(item.get("url") or item.get("applyUrl") or item.get("jobUrl"))
                if not title or not detail_url:
                    continue
                location = self._extract_location_text(item)
                modality = self._extract_modality_text(item)
                description = clean_text(
                    " ".join(
                        part
                        for part in [
                            item.get("description"),
                            location,
                            modality,
                        ]
                        if clean_text(part)
                    )
                )
                is_offer, _ = self._score_offer_candidate(title, description, url=detail_url)
                if not is_offer:
                    continue
                results.append(
                    self._build_raw_candidate(
                        source_url=source_url,
                        title=title,
                        detail_url=detail_url,
                        location=location,
                        modality=modality,
                        date_value=clean_text(item.get("datePosted")) or None,
                        content_text=description,
                    )
                )
        return results

    def _parse_serialized_blocks(self, soup: BeautifulSoup, source_url: str):
        results = []
        scripts = soup.find_all("script")
        for script in scripts:
            raw = script.string or script.get_text(" ", strip=False)
            if not raw:
                continue
            if "hiringroom" not in raw.lower() and "job" not in raw.lower() and "vacan" not in raw.lower():
                continue
            for job in self._extract_jobs_from_text(raw):
                title = clean_text(job.get("title") or job.get("name") or job.get("position"))
                if not title:
                    continue
                detail_url = clean_text(
                    job.get("url")
                    or job.get("jobUrl")
                    or job.get("applyUrl")
                    or job.get("publicUrl")
                    or job.get("slug")
                )
                if detail_url and detail_url.startswith("/"):
                    detail_url = urljoin(source_url, detail_url)
                elif detail_url and not detail_url.startswith("http"):
                    detail_url = urljoin(source_url, f"/jobs/{detail_url.strip('/')}")
                else:
                    detail_url = self._infer_detail_url(source_url, title, job)

                location = clean_text(job.get("location") or job.get("city") or job.get("region"))
                modality = clean_text(job.get("modality") or job.get("workplace") or job.get("workMode"))
                date_value = clean_text(
                    job.get("datePosted")
                    or job.get("publicationDate")
                    or job.get("createdAt")
                    or job.get("publishedAt")
                ) or None
                description = clean_text(
                    " ".join(
                        part
                        for part in [
                            title,
                            location,
                            modality,
                            job.get("description"),
                        ]
                        if clean_text(part)
                    )
                )
                is_offer, _ = self._score_offer_candidate(title, description, url=detail_url)
                if not is_offer:
                    continue
                results.append(
                    self._build_raw_candidate(
                        source_url=source_url,
                        title=title,
                        detail_url=detail_url,
                        location=location,
                        modality=modality,
                        date_value=date_value,
                        content_text=description,
                    )
                )
        return results

    def _extract_jobs_from_text(self, text: str) -> list[dict[str, Any]]:
        objects: list[dict[str, Any]] = []
        decoder = json.JSONDecoder()
        for match in re.finditer(r"[\[{]", text):
            start = match.start()
            try:
                obj, _ = decoder.raw_decode(text[start:])
            except Exception:
                continue
            for item in self._walk_json(obj):
                if not isinstance(item, dict):
                    continue
                key_blob = " ".join(item.keys()).lower()
                if any(token in key_blob for token in ("title", "position", "job", "vacan", "modality", "location")):
                    objects.append(item)
        return objects

    def _build_raw_candidate(
        self,
        *,
        source_url: str,
        title: str,
        detail_url: str,
        location: str,
        modality: str,
        date_value: str | None,
        content_text: str,
    ):
        from .generic_site import RawCandidate

        enriched_text = clean_text(" ".join([title, location, modality, content_text]))
        return RawCandidate(
            title=clean_text(title),
            content_text=enriched_text,
            url=urljoin(source_url, detail_url or source_url),
            date_value=date_value,
            pdf_links=[],
        )

    def _extract_location_text(self, item: dict[str, Any]) -> str:
        job_location = item.get("jobLocation")
        if isinstance(job_location, dict):
            address = job_location.get("address")
            if isinstance(address, dict):
                parts = [
                    clean_text(address.get("addressLocality")),
                    clean_text(address.get("addressRegion")),
                    clean_text(address.get("addressCountry")),
                ]
                return clean_text(", ".join([p for p in parts if p]))
        if isinstance(job_location, list):
            parts = [self._extract_location_text({"jobLocation": loc}) for loc in job_location]
            return clean_text(", ".join([p for p in parts if p]))
        return clean_text(item.get("location") or item.get("address"))

    def _extract_modality_text(self, item: dict[str, Any]) -> str:
        modality = clean_text(item.get("employmentType"))
        if modality:
            return modality
        base = clean_text(item.get("description"))
        m = re.search(r"(remoto|hibrid[oa]|presencial)", base, re.IGNORECASE)
        return m.group(1) if m else ""

    def _infer_detail_url(self, source_url: str, title: str, job: dict[str, Any]) -> str:
        slug = clean_text(job.get("slug"))
        if slug:
            return urljoin(source_url, f"/jobs/{slug}")
        job_id = clean_text(job.get("id") or job.get("jobId"))
        if job_id:
            return urljoin(source_url, f"/jobs/{job_id}")
        safe_title = re.sub(r"[^a-z0-9]+", "-", clean_text(title).lower()).strip("-")
        if safe_title:
            return urljoin(source_url, f"/jobs/{safe_title}")
        parsed = urlparse(source_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _candidate_to_oferta(self, candidate) -> OfertaRaw | None:
        oferta = super()._candidate_to_oferta(candidate)
        if oferta is None:
            return None

        description = clean_text(oferta.descripcion)
        modality_match = re.search(r"\b(remoto|hibrid[oa]|presencial)\b", description, re.IGNORECASE)
        city = oferta.ciudad
        if not city:
            loc_match = re.search(r"(?:ubicaci[oó]n|location)\s*[:\-]\s*([^\n|]+)", description, re.IGNORECASE)
            if loc_match:
                city = clean_text(loc_match.group(1))

        return OfertaRaw(
            url=oferta.url,
            cargo=oferta.cargo,
            institucion_nombre=oferta.institucion_nombre,
            descripcion=description,
            sector=oferta.sector,
            tipo_cargo=normalize_tipo_contrato(f"{oferta.tipo_cargo or ''} {modality_match.group(1) if modality_match else ''}"),
            region=oferta.region or normalize_region(self.institucion.get("region")),
            ciudad=city,
            renta_texto=oferta.renta_texto,
            renta_min=oferta.renta_min,
            renta_max=oferta.renta_max,
            grado_eus=oferta.grado_eus,
            fecha_publicacion=oferta.fecha_publicacion or parse_date(candidate.date_value),
            fecha_cierre=oferta.fecha_cierre,
            area_profesional=oferta.area_profesional,
            url_bases=oferta.url_bases,
        )
