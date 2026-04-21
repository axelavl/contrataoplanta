from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from classification.policy import classify_offer_candidate
from scrapers.base import (
    BaseScraper,
    OfertaRaw,
    clean_text,
    normalize_key,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
    conexion,
)
from scrapers.evaluation.audit_store import AuditStore


DEFAULT_PATH_CANDIDATES: tuple[str, ...] = (
    "/concursos-publicos",
    "/trabaja-con-nosotros",
    "/transparencia/trabaje-con-nosotros",
    "/oportunidades-laborales",
    "/portal-laboral",
    "/empleos",
    "/concursos",
)


@dataclass(slots=True)
class RawCandidate:
    title: str
    content_text: str
    url: str
    date_value: str | None = None
    closing_value: str | None = None
    pdf_links: list[str] | None = None


class GenericSiteScraper(BaseScraper):
    """Scraper async y compatible con el runtime actual para sitios propios."""

    def __init__(
        self,
        *,
        fuente_id: int,
        institucion: dict[str, Any],
        candidate_paths: tuple[str, ...] | None = None,
        extra_keywords: tuple[str, ...] = (),
        max_candidate_urls: int = 4,
        detail_fetch_limit: int = 12,
        trusted_host_only: bool = True,
    ) -> None:
        self.institucion = institucion
        self.institucion_id = institucion.get("id")
        self.url_empleo = clean_text(institucion.get("url_empleo") or institucion.get("url_portal_empleos"))
        self.sitio_web = clean_text(institucion.get("sitio_web"))
        self.candidate_paths = candidate_paths or DEFAULT_PATH_CANDIDATES
        self.extra_keywords = tuple(extra_keywords)
        self.max_candidate_urls = max_candidate_urls
        self.detail_fetch_limit = detail_fetch_limit
        self.trusted_host_only = trusted_host_only
        self.host_scope = {
            host
            for host in (
                self._host(self.url_empleo),
                self._host(self.sitio_web),
            )
            if host
        }
        super().__init__(
            fuente_id=fuente_id,
            nombre_fuente=str(institucion.get("nombre") or institucion.get("sigla") or f"fuente-{fuente_id}"),
        )

    async def descubrir_ofertas(self) -> list[OfertaRaw]:
        if self.http is None:
            raise RuntimeError("GenericSiteScraper requiere un HttpClient activo.")

        preferred_url = self._load_preferred_success_url()
        candidates: list[RawCandidate] = []
        seen_urls: set[str] = set()
        successful_source_url: str | None = None

        short_limit = max(1, min(2, self.max_candidate_urls))
        for max_urls in (short_limit, self.max_candidate_urls):
            if candidates:
                break
            for source_url in self._candidate_urls(max_urls=max_urls, preferred_url=preferred_url):
                html = await self.http.get(source_url)
                if not isinstance(html, str) or not html.strip():
                    continue
                page_candidates = self._extract_candidates_from_listing(html, source_url)
                if page_candidates and successful_source_url is None:
                    successful_source_url = source_url
                for candidate in page_candidates:
                    key = candidate.url or candidate.title
                    if not key or key in seen_urls:
                        continue
                    seen_urls.add(key)
                    candidates.append(candidate)

        enriched = await self._enrich_candidates(candidates)
        offers: list[OfertaRaw] = []
        seen_offer_urls: set[str] = set()
        for candidate in enriched:
            oferta = self._candidate_to_oferta(candidate)
            if oferta is None:
                continue
            if oferta.url in seen_offer_urls:
                continue
            seen_offer_urls.add(oferta.url)
            offers.append(oferta)
        if successful_source_url:
            self._save_successful_source_url(successful_source_url)
        return offers

    def _candidate_urls(self, *, max_urls: int, preferred_url: str | None = None) -> list[str]:
        urls: list[str] = []
        if preferred_url:
            urls.append(preferred_url)
        urls.extend(self._seed_urls())
        urls.extend(self._candidate_path_urls())
        return self._deduplicate_urls(urls)[:max_urls]

    def _seed_urls(self) -> list[str]:
        urls: list[str] = []
        empleo = self.url_empleo
        sitio = self.sitio_web

        if empleo:
            urls.append(empleo)

        if sitio and sitio not in urls:
            urls.append(sitio)
        return urls

    def _candidate_path_urls(self) -> list[str]:
        bases = [self._base_url(url) for url in (self.sitio_web, self.url_empleo) if url]
        urls: list[str] = []
        for base in self._deduplicate_urls(bases):
            for suffix in self.candidate_paths:
                urls.append(f"{base}{suffix}")
        return self._deduplicate_urls(urls)

    def _load_preferred_success_url(self) -> str | None:
        try:
            with conexion() as conn:
                return AuditStore().get_generic_site_last_success_path(conn, self.institucion_id)
        except Exception:
            return None

    def _save_successful_source_url(self, source_url: str) -> None:
        try:
            with conexion() as conn:
                AuditStore().save_generic_site_success_path(
                    conn,
                    institucion_id=self.institucion_id,
                    fuente_id=self.fuente_id,
                    source_url=source_url,
                )
                conn.commit()
        except Exception:
            return

    def _extract_candidates_from_listing(self, html: str, source_url: str) -> list[RawCandidate]:
        soup = BeautifulSoup(html, "html.parser")
        candidates = []
        candidates.extend(self._parse_json_ld_jobs(soup, source_url))
        candidates.extend(self._parse_structured_nodes(soup, source_url))
        candidates.extend(self._parse_table_rows(soup, source_url))
        candidates.extend(self._parse_anchor_fallback(soup, source_url))
        return self._deduplicate(candidates)

    def _parse_json_ld_jobs(self, soup: BeautifulSoup, source_url: str) -> list[RawCandidate]:
        results: list[RawCandidate] = []
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(" ", strip=True)
            data = self._safe_json_load(raw)
            for item in self._walk_json(data):
                if not isinstance(item, dict):
                    continue
                if normalize_key(item.get("@type")) != "jobposting":
                    continue
                title = clean_text(item.get("title"))
                description = self._html_to_text(item.get("description"))
                url = clean_text(item.get("url")) or source_url
                is_offer, _ = self._score_offer_candidate(title, description, url=url)
                if not is_offer:
                    continue
                results.append(
                    RawCandidate(
                        title=title or self._crop_title(description),
                        content_text=description,
                        url=url,
                        date_value=clean_text(item.get("datePosted")) or None,
                        closing_value=clean_text(item.get("validThrough")) or None,
                        pdf_links=[],
                    )
                )
        return results

    def _parse_structured_nodes(self, soup: BeautifulSoup, source_url: str) -> list[RawCandidate]:
        containers = soup.select(
            "article, div.card, div.box, div.panel, div.post, div.item, div.entry, li, section"
        )
        results: list[RawCandidate] = []
        for node in containers:
            content_text = clean_text(node.get_text(" ", strip=True))
            is_offer, _ = self._score_offer_candidate("", content_text, url=source_url)
            if not is_offer:
                continue
            title_el = node.select_one("h1 a, h2 a, h3 a, h4 a, .title a, .job-title a, a[href]")
            title = clean_text(title_el.get_text(" ", strip=True) if title_el else "")
            href = clean_text(title_el.get("href") if title_el else "")
            if href and not self._trusted_url(href, source_url):
                continue
            date_el = node.select_one("time[datetime], .date, .fecha, .entry-date")
            date_value = None
            if date_el:
                date_value = clean_text(date_el.get("datetime") or date_el.get_text(" ", strip=True)) or None
            results.append(
                RawCandidate(
                    title=title or self._crop_title(content_text),
                    content_text=content_text,
                    url=urljoin(source_url, href) if href else source_url,
                    date_value=date_value,
                    pdf_links=self._extract_pdf_links_from_node(node, source_url),
                )
            )
        return results

    def _parse_table_rows(self, soup: BeautifulSoup, source_url: str) -> list[RawCandidate]:
        results: list[RawCandidate] = []
        for row in soup.select("table tr"):
            row_text = clean_text(row.get_text(" ", strip=True))
            is_offer, _ = self._score_offer_candidate("", row_text, url=source_url)
            if not is_offer:
                continue
            link = row.select_one("a[href]")
            href = clean_text(link.get("href") if link else "")
            if href and not self._trusted_url(href, source_url):
                continue
            title = clean_text(link.get_text(" ", strip=True) if link else row_text)
            results.append(
                RawCandidate(
                    title=title or self._crop_title(row_text),
                    content_text=row_text,
                    url=urljoin(source_url, href) if href else source_url,
                    pdf_links=self._extract_pdf_links_from_node(row, source_url),
                )
            )
        return results

    def _parse_anchor_fallback(self, soup: BeautifulSoup, source_url: str) -> list[RawCandidate]:
        results: list[RawCandidate] = []
        for anchor in soup.select("a[href]"):
            href = clean_text(anchor.get("href"))
            if not href or href.startswith("#") or href.startswith("mailto:"):
                continue
            if not self._trusted_url(href, source_url):
                continue
            title = clean_text(anchor.get_text(" ", strip=True))
            parent = anchor.find_parent(["li", "p", "div", "tr", "article", "section"])
            context = clean_text(parent.get_text(" ", strip=True)) if parent else title
            is_offer, _ = self._score_offer_candidate(title, context, url=urljoin(source_url, href))
            if not is_offer:
                continue
            pdf_links = [urljoin(source_url, href)] if ".pdf" in href.lower() else []
            results.append(
                RawCandidate(
                    title=title or self._crop_title(context),
                    content_text=context,
                    url=urljoin(source_url, href),
                    pdf_links=pdf_links,
                )
            )
        return results

    async def _enrich_candidates(self, candidates: list[RawCandidate]) -> list[RawCandidate]:
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
            if candidate.content_text and len(candidate.content_text) >= 400 and candidate.pdf_links:
                enriched.append(candidate)
                continue
            if candidate.url.rstrip("/") in {self.url_empleo.rstrip("/"), self.sitio_web.rstrip("/")}:
                enriched.append(candidate)
                continue
            html = await self.http.get(candidate.url)
            if not isinstance(html, str) or not html.strip():
                enriched.append(candidate)
                continue
            limit += 1
            soup = BeautifulSoup(html, "html.parser")
            body_text = clean_text(soup.get_text(" ", strip=True))
            pdf_links = list(candidate.pdf_links or [])
            pdf_links.extend(self._extract_pdf_links_from_node(soup, candidate.url))
            title = candidate.title
            heading = soup.find(["h1", "h2"])
            if heading:
                title = clean_text(heading.get_text(" ", strip=True)) or title
            enriched.append(
                RawCandidate(
                    title=title,
                    content_text=body_text or candidate.content_text,
                    url=candidate.url,
                    date_value=candidate.date_value or self._extract_date_hint(html),
                    closing_value=candidate.closing_value or self._extract_closing_hint(body_text),
                    pdf_links=self._deduplicate_urls(pdf_links),
                )
            )
        return enriched

    def _candidate_to_oferta(self, candidate: RawCandidate) -> OfertaRaw | None:
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

    def _score_offer_candidate(
        self,
        title: str,
        content: str,
        *,
        url: str | None = None,
    ) -> tuple[bool, str | None]:
        hay_texto = clean_text(f"{title} {content}")
        if len(hay_texto) < 8:
            return False, "texto_insuficiente"

        evaluation = classify_offer_candidate(
            title=title,
            content_text=content,
            url=url or self.url_empleo or self.sitio_web,
            extra_positive_keywords=self.extra_keywords,
        )
        if evaluation.likely_offer:
            return True, None
        reason = f"policy_reject score={evaluation.score:.2f} reasons={list(evaluation.reason_codes)}"
        return False, reason

    def _looks_like_offer(self, title: str, content: str) -> bool:
        """
        Wrapper de compatibilidad para scrapers que aún esperan retorno booleano.

        Mantiene contrato previo y delega en `_score_offer_candidate`, descartando
        la razón de rechazo.
        """
        is_offer, _ = self._score_offer_candidate(title, content)
        return is_offer

    def _extract_pdf_links_from_node(self, node: Any, source_url: str) -> list[str]:
        links: list[str] = []
        for anchor in node.select("a[href]"):
            href = clean_text(anchor.get("href"))
            text = clean_text(anchor.get_text(" ", strip=True)).lower()
            if ".pdf" in href.lower() or "bases" in text or "perfil" in text or "descriptor" in text:
                absolute = urljoin(source_url, href)
                if self._trusted_url(absolute, source_url):
                    links.append(absolute)
        return self._deduplicate_urls(links)

    def _extract_date_hint(self, html: str) -> str | None:
        match = re.search(r"\b(\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{4})\b", html)
        return match.group(1) if match else None

    def _extract_closing_hint(self, text: str | None) -> date | None:
        content = clean_text(text)
        if not content:
            return None
        patterns = [
            r"(?:fecha limite de postulacion|recepcion de antecedentes hasta|postulaciones hasta|plazo de postulacion|cierre de postulacion)[^\d]{0,20}(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})",
            r"(?:hasta el|hasta|postular hasta el)[^\d]{0,10}(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                parsed = parse_date(match.group(1))
                if parsed:
                    return parsed
        return None

    def _infer_area(self, text: str | None) -> str | None:
        key = normalize_key(text)
        if not key:
            return None
        area_map = {
            "salud": ["medic", "enfermer", "matron", "salud", "odontolog", "quimico"],
            "derecho": ["abogad", "jurid", "legal", "fiscal"],
            "ingenieria": ["ingenier", "arquitect", "informatic", "desarrollador", "constructor"],
            "educacion": ["docente", "profesor", "educador", "pedagog"],
            "administracion": ["analista", "administr", "gestion", "rrhh", "finanzas"],
            "social": ["social", "psicolog", "terapeuta", "sociolog"],
        }
        for area, keywords in area_map.items():
            if any(keyword in key for keyword in keywords):
                return area
        return "administracion"

    @staticmethod
    def _infer_city(nombre_institucion: str | None) -> str | None:
        text = clean_text(nombre_institucion)
        if not text:
            return None
        text = re.sub(
            r"^(Municipalidad|Corporacion Municipal|Corporación Municipal) de\s+",
            "",
            text,
            flags=re.IGNORECASE,
        )
        return text or None

    def _trusted_url(self, href: str, source_url: str) -> bool:
        if not href:
            return False
        absolute = urljoin(source_url, href)
        if not self.trusted_host_only:
            return True
        return self._host(absolute) in self.host_scope or absolute.lower().endswith(".pdf")

    @staticmethod
    def _deduplicate(candidates: list[RawCandidate]) -> list[RawCandidate]:
        seen: set[str] = set()
        result: list[RawCandidate] = []
        for candidate in candidates:
            key = candidate.url or candidate.title
            if not key or key in seen:
                continue
            seen.add(key)
            result.append(candidate)
        return result

    @staticmethod
    def _deduplicate_urls(urls: list[str]) -> list[str]:
        seen: set[str] = set()
        results: list[str] = []
        for url in urls:
            cleaned = clean_text(url)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            results.append(cleaned)
        return results

    @staticmethod
    def _host(url: str | None) -> str:
        if not url:
            return ""
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = parsed.netloc.lower()
        return host[4:] if host.startswith("www.") else host

    @staticmethod
    def _base_url(url: str) -> str:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return url.rstrip("/")
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")

    @staticmethod
    def _safe_json_load(raw: str | None) -> Any:
        if not raw:
            return None
        try:
            import json

            return json.loads(raw)
        except Exception:
            return None

    @staticmethod
    def _walk_json(data: Any):
        if isinstance(data, dict):
            yield data
            for value in data.values():
                yield from GenericSiteScraper._walk_json(value)
        elif isinstance(data, list):
            for item in data:
                yield from GenericSiteScraper._walk_json(item)

    @staticmethod
    def _html_to_text(html: str | None) -> str:
        soup = BeautifulSoup(html or "", "html.parser")
        return clean_text(soup.get_text(" ", strip=True))

    @staticmethod
    def _crop_title(text: str) -> str:
        cleaned = clean_text(text)
        if len(cleaned) <= 180:
            return cleaned
        return cleaned[:177].rstrip() + "..."
