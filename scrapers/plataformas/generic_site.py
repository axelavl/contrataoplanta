from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scrapers.base import (
    BaseScraper,
    clean_text,
    extract_host_like_pattern,
    normalize_key,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
)

from scrapers.job_pipeline import JobExtractionPipeline, build_raw_page_from_generic

KEYWORDS_OFERTA = (
    "concurso",
    "vacante",
    "llamado",
    "cargo",
    "postulacion",
    "postulación",
    "honorario",
    "contrata",
    "planta",
    "bases",
    "seleccion",
    "selección",
    "trabaja con nosotros",
    "empleo",
    "oportunidad laboral",
)


# Perfiles operativos del scraper genérico.
# - production: corridas diarias. Poca tolerancia, rápido, pocas URLs.
# - exploration: descubrimiento manual de fuentes nuevas. Más URLs, más tiempo.
SCRAPER_MODES: dict[str, dict[str, Any]] = {
    "production": {
        "timeout": 5,
        "delay": 0.5,
        "max_retries": 1,
        "max_candidate_urls": 2,
    },
    "exploration": {
        "timeout": 10,
        "delay": 1.5,
        "max_retries": 2,
        "max_candidate_urls": 7,
    },
}


# Rutas candidatas ordenadas por probabilidad empírica de contener ofertas.
# En production sólo se usan las primeras ``max_candidate_urls``.
_DEFAULT_PATH_CANDIDATES: tuple[str, ...] = (
    "/concursos-publicos",
    "/trabaja-con-nosotros",
    "/transparencia/trabaje-con-nosotros",
    "/oportunidades-laborales",
    "/portal-laboral",
    "/empleos",
    "/concursos",
)


class GenericSiteScraper(BaseScraper):
    """Scraper heuristico para portales propios y secciones de transparencia."""

    def __init__(
        self,
        institucion: dict[str, Any],
        instituciones_catalogo: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        max_results: int | None = None,
        mode: str = "production",
        max_candidate_urls: int | None = None,
    ) -> None:
        self.institucion = institucion
        self.url_empleo = clean_text(
            institucion.get("url_empleo") or institucion.get("url_portal_empleos")
        )
        self.sitio_web = clean_text(institucion.get("sitio_web"))

        profile = SCRAPER_MODES.get(mode) or SCRAPER_MODES["production"]
        self.mode = mode if mode in SCRAPER_MODES else "production"
        self._max_candidate_urls = (
            max_candidate_urls
            if max_candidate_urls is not None
            else int(profile["max_candidate_urls"])
        )

        super().__init__(
            nombre=f"scraper.generic.{self._slug(institucion.get('nombre'))}",
            instituciones=instituciones_catalogo or [institucion],
            dry_run=dry_run,
            delay=float(profile["delay"]),
            timeout=int(profile["timeout"]),
            max_results=max_results,
            max_retries=int(profile["max_retries"]),
        )
        if institucion.get("id") is not None:
            self.scope_institucion_ids = [institucion["id"]]
        patterns = []
        for url in (self.url_empleo, self.sitio_web):
            pattern = extract_host_like_pattern(url)
            if pattern and pattern not in patterns:
                patterns.append(pattern)
        self.scope_url_patterns = patterns
        self.pipeline = JobExtractionPipeline()

    def fetch_ofertas(self) -> list[dict[str, Any]]:
        ofertas: list[dict[str, Any]] = []
        seen: set[str] = set()
        started_at = time.monotonic()
        urls_visited = 0
        urls_failed = 0

        candidates = self._candidate_urls()
        for url in candidates:
            urls_visited += 1
            try:
                html = self.request_text(url)
            except Exception as exc:
                urls_failed += 1
                self.logger.info(
                    "evento=generic_html_skip scraper=%s url=%s error=%s",
                    self.nombre,
                    url,
                    type(exc).__name__,
                )
                continue

            ofertas_fuente = self._parse_html_listing(html, url)
            prev_count = len(ofertas)
            for oferta in ofertas_fuente:
                key = clean_text(oferta.get("url") or oferta.get("title"))
                if not key or key in seen:
                    continue
                seen.add(key)
                ofertas.append(oferta)

            # Short-circuit: si la primera URL devolvió datos, en modo production
            # no seguimos visitando rutas adicionales.
            if self.mode == "production" and len(ofertas) > prev_count:
                break

            if self.max_results and len(ofertas) >= self.max_results:
                break

        elapsed = round(time.monotonic() - started_at, 2)
        self.logger.info(
            "evento=generic_done scraper=%s mode=%s candidatos=%s visitadas=%s fallidas=%s ofertas=%s duracion=%s",
            self.nombre,
            self.mode,
            len(candidates),
            urls_visited,
            urls_failed,
            len(ofertas),
            elapsed,
        )

        return ofertas[: self.max_results] if self.max_results else ofertas

    def parse_oferta(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        raw_page = build_raw_page_from_generic(
            source_id=str(self.institucion.get("id") or self.nombre),
            source_name=str(self.institucion.get("nombre") or self.nombre),
            source_url=self.url_empleo or self.sitio_web or "",
            raw=raw,
            platform="generic_site",
        )
        posting, trace = self.pipeline.run(raw_page)
        if not posting:
            self.logger.info(
                "evento=generic_reject scraper=%s url=%s reasons=%s",
                self.nombre,
                raw_page.url,
                trace.get("rejection_reasons"),
            )
            return None

        oferta = {
            "institucion_id": self.institucion.get("id"),
            "institucion_nombre": self.institucion.get("nombre"),
            "cargo": posting.get("job_title") or raw.get("title") or self._crop_title(raw.get("content_text")),
            "descripcion": posting.get("description") or raw.get("content_text"),
            "requisitos": "\n".join(posting.get("requirements") or []) or raw.get("content_text"),
            "tipo_contrato": posting.get("contract_type") or normalize_tipo_contrato(raw.get("content_text")),
            "region": normalize_region(self.institucion.get("region")),
            "ciudad": self._infer_city(self.institucion.get("nombre")),
            "renta_bruta_min": int(posting.get("salary_amount")) if posting.get("salary_amount") else None,
            "renta_bruta_max": int(posting.get("salary_amount")) if posting.get("salary_amount") else None,
            "grado_eus": None,
            "jornada": posting.get("workday") or self._extract_jornada(raw.get("content_text")),
            "area_profesional": self._infer_area(posting.get("job_title") or raw.get("content_text")),
            "fecha_publicacion": posting.get("published_at") or parse_date(raw.get("date")),
            "fecha_cierre": posting.get("application_end_at") or parse_date(raw.get("fecha_cierre")),
            "url_oferta": posting.get("job_url") or raw.get("url"),
            "url_bases": (posting.get("attachments") or [raw.get("url")])[0],
            "estado": "cerrado" if posting.get("is_expired") else "activo",
        }
        return self.normalize_offer(oferta)

    def _candidate_urls(self) -> list[str]:
        """
        Construye la lista de URLs candidatas respetando ``self._max_candidate_urls``.

        Prioridad:
          1. url_empleo del JSON (si es específica, va primera y pesa más que nada).
          2. sitio_web raíz (fallback si no hay URL de empleo específica).
          3. Rutas heurísticas comunes sobre el host base.

        En modo production el tope suele ser 2, así que en la práctica visitamos
        sólo (url_empleo) o, si no hay, (sitio_web, /concursos-publicos).
        """
        urls: list[str] = []
        empleo = clean_text(self.url_empleo)
        sitio = clean_text(self.sitio_web)

        if empleo:
            urls.append(empleo)

        base = self._base_url(sitio) if sitio else None
        empleo_es_especifica = bool(empleo and base and empleo.rstrip("/") != base.rstrip("/"))

        if sitio and sitio not in urls and not empleo_es_especifica:
            urls.append(sitio)

        # Rutas heurísticas sólo si el JSON no trajo una URL de empleo específica.
        if base and not empleo_es_especifica:
            for suffix in _DEFAULT_PATH_CANDIDATES:
                candidate = f"{base}{suffix}"
                if candidate not in urls:
                    urls.append(candidate)

        if self._max_candidate_urls > 0:
            return urls[: self._max_candidate_urls]
        return urls

    def _parse_html_listing(self, html: str, source_url: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        ofertas = self._parse_json_ld_jobs(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        ofertas = self._parse_structured_nodes(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        ofertas = self._parse_table_rows(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        return self._deduplicate(self._parse_anchor_fallback(soup, source_url))

    def _parse_json_ld_jobs(self, soup: BeautifulSoup, source_url: str) -> list[dict[str, Any]]:
        ofertas: list[dict[str, Any]] = []
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(" ", strip=True)
            payload = self._safe_json_load(raw)
            for item in self._walk_json(payload):
                if not isinstance(item, dict):
                    continue
                if normalize_key(item.get("@type")) != "jobposting":
                    continue
                title = clean_text(item.get("title"))
                description = self._html_to_text(item.get("description"))
                valid_through = item.get("validThrough")
                date_posted = item.get("datePosted")
                identifier = item.get("identifier")
                identifier_value = None
                if isinstance(identifier, dict):
                    identifier_value = identifier.get("value")
                url = clean_text(item.get("url")) or source_url
                if not self._looks_like_offer(title, description):
                    continue
                ofertas.append(
                    {
                        "title": title,
                        "content_text": description,
                        "date": date_posted,
                        "fecha_cierre": valid_through,
                        "url": url,
                        "pdf_links": [],
                        "id_externo": clean_text(identifier_value) or None,
                    }
                )
        return ofertas

    def _parse_structured_nodes(self, soup: BeautifulSoup, source_url: str) -> list[dict[str, Any]]:
        containers = soup.select(
            "article, div.card, div.box, div.panel, div.post, div.item, div.entry, li, section"
        )
        ofertas: list[dict[str, Any]] = []
        for node in containers:
            content_text = clean_text(node.get_text(" ", strip=True))
            if not self._looks_like_offer("", content_text):
                continue
            title_el = node.select_one("h1 a, h2 a, h3 a, h4 a, .title a, a[href]")
            title = clean_text(title_el.get_text(" ", strip=True) if title_el else "")
            href = clean_text(title_el.get("href") if title_el else "")
            date_el = node.select_one("time[datetime], .date, .fecha, .entry-date")
            date_value = None
            if date_el:
                date_value = date_el.get("datetime") or date_el.get_text(" ", strip=True)
            ofertas.append(
                {
                    "title": title or self._crop_title(content_text),
                    "content_text": content_text,
                    "date": date_value,
                    "fecha_cierre": None,
                    "url": urljoin(source_url, href) if href else source_url,
                    "pdf_links": self._extract_pdf_links_from_node(node, source_url),
                }
            )
        return ofertas

    def _parse_table_rows(self, soup: BeautifulSoup, source_url: str) -> list[dict[str, Any]]:
        ofertas: list[dict[str, Any]] = []
        for row in soup.select("table tr"):
            row_text = clean_text(row.get_text(" ", strip=True))
            if not self._looks_like_offer("", row_text):
                continue
            link = row.select_one("a[href]")
            href = clean_text(link.get("href") if link else "")
            title = clean_text(link.get_text(" ", strip=True) if link else row_text)
            ofertas.append(
                {
                    "title": title or self._crop_title(row_text),
                    "content_text": row_text,
                    "date": None,
                    "fecha_cierre": None,
                    "url": urljoin(source_url, href) if href else source_url,
                    "pdf_links": self._extract_pdf_links_from_node(row, source_url),
                }
            )
        return ofertas

    def _parse_anchor_fallback(self, soup: BeautifulSoup, source_url: str) -> list[dict[str, Any]]:
        ofertas: list[dict[str, Any]] = []
        for anchor in soup.select("a[href]"):
            href = clean_text(anchor.get("href"))
            if not href or href.startswith("#") or href.startswith("mailto:"):
                continue
            title = clean_text(anchor.get_text(" ", strip=True))
            parent = anchor.find_parent(["li", "p", "div", "tr", "article", "section"])
            context = clean_text(parent.get_text(" ", strip=True)) if parent else title
            if not self._looks_like_offer(title, context):
                continue
            ofertas.append(
                {
                    "title": title or self._crop_title(context),
                    "content_text": context,
                    "date": None,
                    "fecha_cierre": None,
                    "url": urljoin(source_url, href),
                    "pdf_links": [urljoin(source_url, href)] if ".pdf" in href.lower() else [],
                }
            )
        return ofertas

    def _extract_pdf_links_from_node(self, node: Any, source_url: str) -> list[str]:
        links: list[str] = []
        for anchor in node.select("a[href]"):
            href = clean_text(anchor.get("href"))
            text = clean_text(anchor.get_text(" ", strip=True)).lower()
            if ".pdf" in href.lower() or "bases" in text:
                links.append(urljoin(source_url, href))
        return self._deduplicate_urls(links)

    def _extract_fecha_cierre(self, text: str | None):
        content = clean_text(text)
        if not content:
            return None
        patterns = [
            r"(?:cierre|vence|plazo de postulacion|plazo de postulación|recepcion de antecedentes)[^\d]{0,20}(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
            r"(?:hasta el|hasta|postular hasta el)[^\d]{0,10}(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                fecha = parse_date(match.group(1))
                if fecha:
                    return fecha
        return None

    def _extract_jornada(self, text: str | None) -> str | None:
        content = clean_text(text)
        if not content:
            return None
        match = re.search(r"\b(\d{1,2})\s*horas\b", content, re.IGNORECASE)
        if match:
            return f"{match.group(1)} horas"
        if "jornada completa" in normalize_key(content):
            return "jornada completa"
        if "media jornada" in normalize_key(content):
            return "media jornada"
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

    def _infer_city(self, nombre_institucion: str | None) -> str | None:
        text = clean_text(nombre_institucion)
        if not text:
            return None
        text = re.sub(r"^(Municipalidad|Corporación Municipal|Corporacion Municipal) de\s+", "", text, flags=re.IGNORECASE)
        return text or None

    def _looks_like_offer(self, title: str, content: str) -> bool:
        hay_texto = clean_text(f"{title} {content}")
        if len(hay_texto) < 8:
            return False
        key = normalize_key(hay_texto)
        return any(normalize_key(keyword) in key for keyword in KEYWORDS_OFERTA)

    def _crop_title(self, text: str) -> str:
        cleaned = clean_text(text)
        if len(cleaned) <= 180:
            return cleaned
        return cleaned[:177].rstrip() + "..."

    def _deduplicate(self, offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        results: list[dict[str, Any]] = []
        for offer in offers:
            key = clean_text(offer.get("url")) or clean_text(offer.get("title"))
            if not key or key in seen:
                continue
            seen.add(key)
            results.append(offer)
        return results

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
    def _slug(value: str | None) -> str:
        key = normalize_key(value)
        return re.sub(r"[^a-z0-9]+", "_", key).strip("_") or "generic"


def load_instituciones(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return payload.get("instituciones") if isinstance(payload, dict) else payload


def ejecutar(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
    max_results: int | None = None,
    mode: str = "production",
    max_candidate_urls: int | None = None,
) -> dict[str, Any]:
    scraper = GenericSiteScraper(
        institucion=institucion,
        instituciones_catalogo=instituciones_catalogo,
        dry_run=dry_run,
        max_results=max_results,
        mode=mode,
        max_candidate_urls=max_candidate_urls,
    )
    return scraper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper generico para portales propios")
    parser.add_argument("--json", required=True, help="Ruta al JSON maestro")
    parser.add_argument("--id", type=int, required=True, help="ID de la institucion")
    parser.add_argument("--dry-run", action="store_true", help="No guarda en PostgreSQL")
    parser.add_argument("--max", type=int, default=None, help="Limite de ofertas")
    args = parser.parse_args()

    instituciones = load_instituciones(args.json)
    objetivo = next((item for item in instituciones if item.get("id") == args.id), None)
    if not objetivo:
        raise SystemExit(f"No se encontro la institucion con id={args.id}")

    print(
        ejecutar(
            institucion=objetivo,
            instituciones_catalogo=instituciones,
            dry_run=args.dry_run,
            max_results=args.max,
        )
    )
