from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scrapers.base import (
    OfertaRaw,
    clean_text,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
)

from .generic_site import GenericSiteScraper


# ── Constantes ─────────────────────────────────────────────────────────────────

BUK_KEYWORDS = (
    "buk",
    "vacante",
    "job",
    "empleo",
    "position",
    "postula",
)

_MAX_PAGINAS_BUK = 10


class BukScraper(GenericSiteScraper):
    """
    Scraper para portales Buk (*.buk.cl/trabaja-con-nosotros).

    Buk es una plataforma Rails + Turbolinks que renderiza las ofertas en el
    servidor.  Las tarjetas de trabajo usan la clase CSS `.jobs__card` y los
    títulos `.job__card-name`.  Los links de detalle son URLs absolutas del
    tipo `https://{empresa}.buk.cl/s/{hash}`.

    La paginación funciona con `?page=N`.
    """

    def __init__(self, *, fuente_id: int, institucion: dict[str, Any]) -> None:
        super().__init__(
            fuente_id=fuente_id,
            institucion=institucion,
            candidate_paths=("/trabaja-con-nosotros", "/jobs", "/careers", "/empleos"),
            extra_keywords=BUK_KEYWORDS,
            max_candidate_urls=2,
            detail_fetch_limit=20,
            trusted_host_only=False,
        )

    # ── Punto de entrada ──────────────────────────────────────────────────────

    async def descubrir_ofertas(self) -> list[OfertaRaw]:
        """Extrae ofertas iterando páginas del portal Buk."""
        if self.http is None:
            raise RuntimeError("BukScraper requiere HttpClient activo.")

        base_url = self._canonical_base()
        if not base_url:
            return []

        offers: list[OfertaRaw] = []
        seen_urls: set[str] = set()

        for pagina in range(1, _MAX_PAGINAS_BUK + 1):
            url = f"{base_url}?page={pagina}" if pagina > 1 else base_url
            html = await self.http.get(url)
            if not isinstance(html, str) or not html.strip():
                break

            page_offers = self._parse_buk_page(html, base_url)
            nuevas = 0
            for oferta in page_offers:
                if oferta.url not in seen_urls:
                    seen_urls.add(oferta.url)
                    offers.append(oferta)
                    nuevas += 1

            # Buk no suele paginar — si no hay ofertas nuevas, terminar
            if nuevas == 0:
                break

        # Fallback: si no encontramos nada con el parser Buk, intentar genérico
        if not offers:
            return await super().descubrir_ofertas()

        return offers

    # ── Parseo de página de listado Buk ──────────────────────────────────────

    def _parse_buk_page(self, html: str, base_url: str) -> list[OfertaRaw]:
        """
        Parsea una página de listado de Buk extrayendo las tarjetas `.jobs__card`.
        """
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select(".jobs__card")
        if not cards:
            return []

        ofertas: list[OfertaRaw] = []
        for card in cards:
            # Título del cargo
            name_el = card.select_one(".job__card-name")
            if not name_el:
                continue
            # El nombre a veces está duplicado (desktop + mobile), tomar primera parte
            raw_title = name_el.get_text(" ", strip=True)
            # Buk a veces duplica: "Cargo Depto Cargo" — quedarse con la primera mitad
            title = self._deduplicate_title(raw_title)
            if not title:
                continue

            # Link al detalle
            link_el = card.select_one("a[href]")
            href = link_el["href"] if link_el else ""
            url_oferta = href if href.startswith("http") else urljoin(base_url, href)
            if not url_oferta:
                continue

            # Metadata adicional (ubicación, tipo de contrato, área)
            info_el = card.select_one(".jobs__card-info")
            info_text = info_el.get_text(" ", strip=True) if info_el else ""

            region = normalize_region(
                self._extract_region_from_info(info_text)
                or self.institucion.get("region")
            )
            renta_min, renta_max, grado = parse_renta(info_text)

            ofertas.append(
                OfertaRaw(
                    url=url_oferta,
                    cargo=clean_text(title),
                    institucion_nombre=str(
                        self.institucion.get("nombre") or self.nombre_fuente
                    ),
                    descripcion=info_text or None,
                    sector=self.institucion.get("sector"),
                    tipo_cargo=normalize_tipo_contrato(title),
                    region=region,
                    ciudad=self._extract_ciudad_from_info(info_text),
                    renta_texto=None,
                    renta_min=renta_min,
                    renta_max=renta_max,
                    grado_eus=grado,
                    fecha_publicacion=None,
                    fecha_cierre=None,
                    area_profesional=self._infer_area(title),
                    url_bases=None,
                )
            )

        return ofertas

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _canonical_base(self) -> str:
        """URL base para el portal de empleo Buk de la institución."""
        url = self.url_empleo or self.sitio_web
        if not url:
            return ""
        # Si ya apunta a buk.cl, usarla directamente
        if "buk.cl" in url:
            return url.rstrip("/")
        # Si apunta a otra URL (p.ej. SASIPA), también aceptarla
        return url.rstrip("/")

    @staticmethod
    def _deduplicate_title(raw: str) -> str:
        """
        Buk a veces duplica el texto del cargo en móvil+escritorio.
        Ejemplo: "Ingeniero Senior Depto X Ingeniero Senior" → "Ingeniero Senior"
        Estrategia: si el texto contiene una segunda repetición de las primeras
        palabras, devolver sólo la primera mitad limpia.
        """
        words = raw.split()
        n = len(words)
        if n < 4:
            return raw
        # Intentar encontrar repetición en las primeras n//2 palabras
        for split in range(2, n // 2 + 1):
            prefix = " ".join(words[:split])
            rest = " ".join(words[split:])
            if rest.endswith(prefix) or rest.startswith(prefix):
                return prefix
        return raw

    @staticmethod
    def _extract_region_from_info(info: str) -> str | None:
        """Extrae región del texto de info de una tarjeta Buk."""
        if not info:
            return None
        # Busca patrones tipo "Región Metropolitana", "Región de Valparaíso"
        m = re.search(
            r"regi[oó]n\s+(?:de\s+|del\s+)?([a-záéíóúñ\s]+?)(?:\s*[,\|•]|$)",
            info,
            re.IGNORECASE,
        )
        if m:
            return m.group(1).strip()
        return None

    @staticmethod
    def _extract_ciudad_from_info(info: str) -> str | None:
        """Extrae ciudad del texto de info de una tarjeta Buk."""
        if not info:
            return None
        # Ciudad suele estar antes de la región
        m = re.search(
            r"^([A-ZÁÉÍÓÚ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚ][a-záéíóúñ]+)?)\s*[,\|•]",
            info.strip(),
        )
        if m:
            candidate = m.group(1).strip()
            if len(candidate) > 3:
                return candidate
        return None
