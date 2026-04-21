"""
Scraper dedicado para portales de postulaciones de Carabineros de Chile y
Policía de Investigaciones (PDI).

Ambas instituciones publican cargos civiles en portales propios que suelen
tener protección WAF/anti-bot (Cloudflare, firewalls institucionales).
Este scraper extiende GenericSiteScraper con:

1. Warmup de sesión: visita la página principal para obtener cookies antes
   de intentar las rutas de ofertas.
2. Headers de navegador completos (Sec-Fetch-*, Accept, etc.).
3. URLs candidatas específicas por institución.
4. Timeouts y reintentos más tolerantes.
5. Fallback a portales de transparencia cuando el portal de postulaciones
   no responde.
"""

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
    DEFAULT_BROWSER_HEADERS,
    BaseScraper,
    clean_text,
    extract_host_like_pattern,
    normalize_key,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
)
from scrapers.plataformas.generic_site import (
    KEYWORDS_OFERTA,
    GenericSiteScraper,
)


# ────────────────────── Configuración por institución ───────────────────

# URLs candidatas ordenadas por probabilidad.  Se prueban secuencialmente
# hasta encontrar ofertas.

_CARABINEROS_CANDIDATES: tuple[str, ...] = (
    "https://postulaciones.carabineros.cl/",
    "https://postulaciones.carabineros.cl/ofertas",
    "https://postulaciones.carabineros.cl/concursos",
    "https://postulaciones.carabineros.cl/convocatorias",
    "https://www.carabineros.cl/transparencia/concursos/",
    "https://www.carabineros.cl/transparencia/trabaje-con-nosotros/",
    "https://www.carabineros.cl/transparencia/personal-y-remuneraciones/",
    "https://www.carabineros.cl/concursos-publicos/",
)

_PDI_CANDIDATES: tuple[str, ...] = (
    "https://postulaciones.investigaciones.cl/",
    "https://postulaciones.investigaciones.cl/ofertas",
    "https://postulaciones.investigaciones.cl/concursos",
    "https://postulaciones.investigaciones.cl/convocatorias",
    "https://www.pdichile.cl/transparencia/concursos/",
    "https://www.pdichile.cl/transparencia/trabaje-con-nosotros/",
    "https://www.pdichile.cl/transparencia/personal-y-remuneraciones/",
    "https://www.pdichile.cl/concursos-publicos/",
)

_WARMUP_URLS: dict[int, list[str]] = {
    161: [
        "https://postulaciones.carabineros.cl/",
        "https://www.carabineros.cl/",
    ],
    162: [
        "https://postulaciones.investigaciones.cl/",
        "https://www.pdichile.cl/",
    ],
}

_CANDIDATE_URLS_BY_ID: dict[int, tuple[str, ...]] = {
    161: _CARABINEROS_CANDIDATES,
    162: _PDI_CANDIDATES,
}

# Keywords adicionales propios de publicaciones policiales.
_POLICIA_KEYWORDS: tuple[str, ...] = (
    "personal civil",
    "dotacion",
    "dotación",
    "grado",
    "escalafon",
    "escalafón",
    "calidad juridica",
    "calidad jurídica",
    "nombramiento",
    "provision",
    "provisión",
    "convocatoria",
)

# Máximo de URLs candidatas a visitar.
_MAX_CANDIDATES = 6


class PoliciaScraper(GenericSiteScraper):
    """Scraper dedicado para portales policiales (Carabineros / PDI)."""

    def __init__(
        self,
        institucion: dict[str, Any],
        instituciones_catalogo: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        max_results: int | None = None,
    ) -> None:
        # Forzamos modo exploration con parámetros tolerantes para sitios
        # gubernamentales pesados.
        super().__init__(
            institucion=institucion,
            instituciones_catalogo=instituciones_catalogo,
            dry_run=dry_run,
            max_results=max_results,
            mode="exploration",
            max_candidate_urls=_MAX_CANDIDATES,
        )
        # Sobrescribimos timeouts: los portales policiales suelen ser lentos.
        self.timeout = 15
        self.max_retries = 3
        self.delay = 1.5
        self._warmed_up = False

    # ──────────────── Warmup de sesión ──────────────────────────────

    def _warmup_session(self) -> None:
        """Visita las páginas principales para obtener cookies de sesión."""
        if self._warmed_up:
            return
        self._warmed_up = True
        inst_id = self.institucion.get("id")
        warmup_urls = _WARMUP_URLS.get(inst_id, [])
        for url in warmup_urls:
            try:
                self.logger.info(
                    "evento=policia_warmup scraper=%s url=%s",
                    self.nombre,
                    url,
                )
                self.request(url, timeout=10)
            except Exception as exc:
                self.logger.info(
                    "evento=policia_warmup_fail scraper=%s url=%s error=%s",
                    self.nombre,
                    url,
                    type(exc).__name__,
                )
            time.sleep(0.5)

    # ──────────────── Override: URLs candidatas ──────────────────────

    def _candidate_urls(self) -> list[str]:
        """URLs candidatas específicas por institución policial."""
        inst_id = self.institucion.get("id")
        specific = _CANDIDATE_URLS_BY_ID.get(inst_id)

        if specific:
            urls = list(specific)
        else:
            # Fallback al comportamiento genérico si la institución no
            # está en el mapeo (no debería pasar).
            urls = super()._candidate_urls()

        # Agregar url_empleo del JSON si no está ya en la lista.
        empleo = clean_text(self.url_empleo)
        if empleo and empleo not in urls:
            urls.insert(0, empleo)

        return urls[: self._max_candidate_urls]

    # ──────────────── Override: fetch con warmup ────────────────────

    def fetch_ofertas(self) -> list[dict[str, Any]]:
        self._warmup_session()

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
                    "evento=policia_html_skip scraper=%s url=%s error=%s",
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

            # Si encontramos ofertas, seguimos buscando en más URLs
            # (a diferencia del genérico en production que para al primer hit).
            if self.max_results and len(ofertas) >= self.max_results:
                break

        elapsed = round(time.monotonic() - started_at, 2)
        self.logger.info(
            "evento=policia_done scraper=%s candidatos=%s visitadas=%s "
            "fallidas=%s ofertas=%s duracion=%s",
            self.nombre,
            len(candidates),
            urls_visited,
            urls_failed,
            len(ofertas),
            elapsed,
        )

        return ofertas[: self.max_results] if self.max_results else ofertas

    # ──────────────── Override: parsing más tolerante ────────────────

    def _score_offer_candidate(
        self,
        title: str,
        content: str,
        *,
        url: str | None = None,
    ) -> tuple[bool, str | None]:
        """Extiende la detección con keywords propios del ámbito policial."""
        is_offer, reason = super()._score_offer_candidate(title, content, url=url)
        if is_offer:
            return True, None

        hay_texto = clean_text(f"{title} {content}")
        key = normalize_key(hay_texto)
        if any(normalize_key(kw) in key for kw in KEYWORDS_OFERTA):
            return True, None
        if any(normalize_key(kw) in key for kw in _POLICIA_KEYWORDS):
            return True, "policia_keyword_match"
        return False, reason

    def _parse_html_listing(self, html: str, source_url: str) -> list[dict[str, Any]]:
        """Extiende el parsing con detección de secciones de transparencia."""
        soup = BeautifulSoup(html, "html.parser")

        # 1. Intentar JSON-LD (estándar).
        ofertas = self._parse_json_ld_jobs(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        # 2. Intentar tabla con datos de concurso (común en transparencia).
        ofertas = self._parse_concurso_tables(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        # 3. Nodos estructurados (cards, articles, etc.).
        ofertas = self._parse_structured_nodes(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        # 4. Tablas genéricas.
        ofertas = self._parse_table_rows(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        # 5. Links como fallback.
        ofertas = self._parse_anchor_fallback(soup, source_url)
        if ofertas:
            return self._deduplicate(ofertas)

        # 6. Último recurso: buscar links a PDFs de bases de concurso.
        return self._deduplicate(self._parse_pdf_bases(soup, source_url))

    def _parse_concurso_tables(
        self, soup: BeautifulSoup, source_url: str
    ) -> list[dict[str, Any]]:
        """
        Parsing específico para tablas de concursos/transparencia.

        Muchos portales gubernamentales chilenos usan tablas con columnas como:
        N° | Cargo | Grado | Calidad Jurídica | Fecha Cierre | Bases
        """
        ofertas: list[dict[str, Any]] = []

        for table in soup.select("table"):
            headers = []
            for th in table.select("thead th, tr:first-child th, tr:first-child td"):
                headers.append(normalize_key(th.get_text(" ", strip=True)))

            if not headers:
                continue

            # Detectar si es una tabla de concursos.
            header_text = " ".join(headers)
            is_concurso_table = any(
                marker in header_text
                for marker in (
                    "cargo",
                    "concurso",
                    "vacante",
                    "grado",
                    "bases",
                    "postulacion",
                    "calidad juridica",
                )
            )
            if not is_concurso_table:
                continue

            # Mapear columnas por nombre.
            col_map = self._map_columns(headers)

            rows = table.select("tbody tr")
            if not rows:
                rows = table.select("tr")[1:]  # saltar header

            for row in rows:
                cells = row.select("td")
                if not cells:
                    continue

                cargo = self._cell_text(cells, col_map.get("cargo"))
                if not cargo:
                    # Intentar con la primera celda que tenga texto significativo.
                    cargo = self._first_meaningful_cell(cells)
                if not cargo or len(cargo) < 4:
                    continue

                fecha_cierre_raw = self._cell_text(cells, col_map.get("fecha_cierre"))
                grado_raw = self._cell_text(cells, col_map.get("grado"))

                # Buscar link a bases (PDF u otra URL).
                bases_link = None
                detail_link = None
                for cell in cells:
                    for a in cell.select("a[href]"):
                        href = clean_text(a.get("href"))
                        if not href or href.startswith("#"):
                            continue
                        full_url = urljoin(source_url, href)
                        if ".pdf" in href.lower():
                            bases_link = full_url
                        elif not detail_link:
                            detail_link = full_url

                row_text = clean_text(row.get_text(" ", strip=True))
                content_parts = [cargo]
                if grado_raw:
                    content_parts.append(f"Grado {grado_raw}")
                content_parts.append(row_text)

                ofertas.append(
                    {
                        "title": cargo,
                        "content_text": " ".join(content_parts),
                        "date": None,
                        "fecha_cierre": fecha_cierre_raw,
                        "url": detail_link or bases_link or source_url,
                        "pdf_links": [bases_link] if bases_link else [],
                    }
                )

        return ofertas

    def _parse_pdf_bases(
        self, soup: BeautifulSoup, source_url: str
    ) -> list[dict[str, Any]]:
        """Extrae ofertas a partir de links a PDFs de bases de concurso."""
        ofertas: list[dict[str, Any]] = []
        for anchor in soup.select("a[href]"):
            href = clean_text(anchor.get("href"))
            if not href:
                continue
            text = clean_text(anchor.get_text(" ", strip=True))
            full_url = urljoin(source_url, href)
            is_pdf = ".pdf" in href.lower()
            is_bases = any(
                kw in normalize_key(f"{text} {href}")
                for kw in ("bases", "concurso", "convocatoria", "cargo", "contrata")
            )
            if is_pdf and is_bases and len(text) > 5:
                ofertas.append(
                    {
                        "title": text,
                        "content_text": text,
                        "date": None,
                        "fecha_cierre": None,
                        "url": full_url,
                        "pdf_links": [full_url],
                    }
                )
        return ofertas

    # ──────────────── Helpers de tablas ──────────────────────────────

    @staticmethod
    def _map_columns(headers: list[str]) -> dict[str, int]:
        """Mapea nombres semánticos a índices de columna."""
        col_map: dict[str, int] = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            if any(w in header for w in ("cargo", "funcion", "denominacion", "puesto")):
                col_map.setdefault("cargo", idx)
            elif any(w in header for w in ("grado", "eus")):
                col_map.setdefault("grado", idx)
            elif any(
                w in header
                for w in ("cierre", "vencimiento", "plazo", "termino", "término")
            ):
                col_map.setdefault("fecha_cierre", idx)
            elif any(w in header for w in ("bases", "documento", "pdf", "descargar")):
                col_map.setdefault("bases", idx)
            elif any(
                w in header
                for w in ("calidad", "tipo", "contrato", "juridica", "jurídica")
            ):
                col_map.setdefault("tipo_contrato", idx)
            elif any(w in header for w in ("region", "localidad", "lugar")):
                col_map.setdefault("region", idx)
        return col_map

    @staticmethod
    def _cell_text(
        cells: list[Any], col_idx: int | None
    ) -> str | None:
        if col_idx is None or col_idx >= len(cells):
            return None
        return clean_text(cells[col_idx].get_text(" ", strip=True)) or None

    @staticmethod
    def _first_meaningful_cell(cells: list[Any]) -> str | None:
        for cell in cells:
            text = clean_text(cell.get_text(" ", strip=True))
            if text and len(text) > 5 and not text.isdigit():
                return text
        return None


# ──────────────── Funciones de entrada ──────────────────────────────

def load_instituciones(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return payload.get("instituciones") if isinstance(payload, dict) else payload


def ejecutar(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
    max_results: int | None = None,
) -> dict[str, Any]:
    scraper = PoliciaScraper(
        institucion=institucion,
        instituciones_catalogo=instituciones_catalogo,
        dry_run=dry_run,
        max_results=max_results,
    )
    return scraper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraper para portales policiales (Carabineros / PDI)"
    )
    parser.add_argument("--json", required=True, help="Ruta al JSON maestro")
    parser.add_argument("--id", type=int, required=True, help="ID de la institucion (161 o 162)")
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
