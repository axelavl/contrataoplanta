from __future__ import annotations

import re
from datetime import date
from typing import Any
from urllib.parse import urlparse

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

ATS_TRABAJANDO_KEYWORDS = (
    "trabajando",
    "vacante",
    "trabajo",
    "postula",
    "oferta",
    "cargo",
)

_MAX_PAGINAS = 10  # máximo de páginas a iterar por empresa


class TrabajandoCLScraper(GenericSiteScraper):
    """
    Scraper para portales Trabajando.cl de instituciones públicas.

    Trabajando.cl es un SPA Nuxt.js que incluye estado del servidor (SSR) en
    un script inline.  Parseamos ese estado para extraer las ofertas sin
    necesitar Playwright.

    URL típica: https://{empresa}.trabajando.cl/trabajo-empleo
    Paginación: ?pagina=2, ?pagina=3, …
    Detalle:    https://{empresa}.trabajando.cl/oferta/{id}
    """

    def __init__(self, *, fuente_id: int, institucion: dict[str, Any]) -> None:
        super().__init__(
            fuente_id=fuente_id,
            institucion=institucion,
            candidate_paths=("/trabajo-empleo", "/ofertas", "/empleos"),
            extra_keywords=ATS_TRABAJANDO_KEYWORDS,
            max_candidate_urls=2,
            detail_fetch_limit=20,
            trusted_host_only=False,
        )

    # ── Punto de entrada ──────────────────────────────────────────────────────

    async def descubrir_ofertas(self) -> list[OfertaRaw]:
        """Extrae todas las ofertas iterando las páginas del SSR de Nuxt."""
        if self.http is None:
            raise RuntimeError("TrabajandoCLScraper requiere HttpClient activo.")

        base_url = self._canonical_base()
        if not base_url:
            return []

        offers: list[OfertaRaw] = []
        seen_ids: set[str] = set()

        for pagina in range(1, _MAX_PAGINAS + 1):
            url = f"{base_url}?pagina={pagina}" if pagina > 1 else base_url
            html = await self.http.get(url)
            if not isinstance(html, str) or not html.strip():
                break

            page_offers, total_paginas = self._parse_nuxt_state(html, base_url)
            nuevas = 0
            for oferta in page_offers:
                if oferta.url not in seen_ids:
                    seen_ids.add(oferta.url)
                    offers.append(oferta)
                    nuevas += 1

            if nuevas == 0 or pagina >= total_paginas:
                break

        return offers

    # ── Parseo del estado SSR de Nuxt ─────────────────────────────────────────

    def _parse_nuxt_state(
        self, html: str, base_url: str
    ) -> tuple[list[OfertaRaw], int]:
        """
        Extrae ofertas del estado SSR embebido por Nuxt.
        Devuelve (lista_de_ofertas, total_paginas).
        """
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        nuxt_state = ""
        for s in scripts:
            if "ShallowReactive" in s:
                nuxt_state = s
                break

        if not nuxt_state:
            # Fallback: parseo HTML genérico
            return self._parse_html_fallback(html, base_url), 1

        # Total de páginas
        m_pages = re.search(r"cantidadPaginas[^:]*[:.,]\s*(\d+)", nuxt_state)
        total_paginas = int(m_pages.group(1)) if m_pages else 1

        # Extraer pares (id_oferta, nombre_cargo) del estado serializado.
        # El estado de Nuxt serializa como: ID_OFERTA,"NombreCargo",...
        # Los IDs de oferta son números de 7-8 dígitos; los títulos son strings ≥10 chars.
        pairs = re.findall(r"(\d{6,8}),\"([^\"]{6,180})\"", nuxt_state)
        # Filtrar ruido: solo pares cuyo título tiene letras
        pairs = [(id_v, t) for id_v, t in pairs if re.search(r"[a-zA-ZáéíóúüñÁÉÍÓÚÜÑ]", t)]

        # Extraer más campos por oferta (descripción, ubicación, fecha publicación)
        # A veces aparecen embebidos en el mismo bloque
        desc_map: dict[str, str] = {}
        loc_map: dict[str, str] = {}
        fecha_map: dict[str, str] = {}

        # Ubicaciones suelen estar después del título
        for m in re.finditer(
            r"(\d{6,8}),\"[^\"]{6,180}\",\"[^\"]*\",\"[^\"]*\",\"([^\"]{5,200})\",",
            nuxt_state,
        ):
            loc_map[m.group(1)] = m.group(2)

        # Fechas de publicación (formato "YYYY-MM-DD HH:MM")
        for m in re.finditer(
            r"(\d{6,8}).*?\"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\"", nuxt_state
        ):
            fecha_map.setdefault(m.group(1), m.group(2))

        ofertas: list[OfertaRaw] = []
        for id_oferta, nombre_cargo in pairs:
            url_oferta = f"{base_url.rstrip('/')}/../oferta/{id_oferta}"
            # Normalizar URL: reemplazar /trabajo-empleo/../oferta por /oferta
            url_oferta = self._resolve_offer_url(base_url, id_oferta)
            descripcion = desc_map.get(id_oferta, "")
            ubicacion_raw = loc_map.get(id_oferta, "")
            fecha_raw = fecha_map.get(id_oferta, "")
            region = self._region_from_ubicacion(ubicacion_raw)
            fecha_pub = parse_date(fecha_raw.split(" ")[0] if fecha_raw else "")
            renta_min, renta_max, grado = parse_renta(descripcion)

            ofertas.append(
                OfertaRaw(
                    url=url_oferta,
                    cargo=clean_text(nombre_cargo),
                    institucion_nombre=str(
                        self.institucion.get("nombre") or self.nombre_fuente
                    ),
                    descripcion=descripcion or None,
                    sector=self.institucion.get("sector"),
                    tipo_cargo=normalize_tipo_contrato(nombre_cargo),
                    region=region or normalize_region(self.institucion.get("region")),
                    ciudad=self._ciudad_from_ubicacion(ubicacion_raw),
                    renta_texto=None,
                    renta_min=renta_min,
                    renta_max=renta_max,
                    grado_eus=grado,
                    fecha_publicacion=fecha_pub,
                    fecha_cierre=None,
                    area_profesional=self._infer_area(nombre_cargo),
                    url_bases=None,
                )
            )

        return ofertas, total_paginas

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _canonical_base(self) -> str:
        """Devuelve la URL base canónica (sin ?query) para la empresa."""
        url = self.url_empleo or self.sitio_web
        if not url:
            return ""
        # Si la URL ya apunta a /trabajo-empleo, usarla directamente
        if "trabajando.cl" in url:
            return url.rstrip("/")
        return ""

    def _resolve_offer_url(self, base_url: str, id_oferta: str) -> str:
        """Construye la URL de detalle de una oferta."""
        parsed = urlparse(base_url)
        domain_base = f"{parsed.scheme}://{parsed.netloc}"
        return f"{domain_base}/oferta/{id_oferta}"

    def _region_from_ubicacion(self, ubicacion: str) -> str | None:
        """Extrae nombre de región a partir de texto 'Ciudad, Región'."""
        if not ubicacion:
            return None
        parts = ubicacion.split(",")
        region_hint = parts[-1].strip() if len(parts) > 1 else ubicacion.strip()
        return normalize_region(region_hint)

    def _ciudad_from_ubicacion(self, ubicacion: str) -> str | None:
        if not ubicacion:
            return None
        parts = ubicacion.split(",")
        return clean_text(parts[0]) if parts else None

    def _parse_html_fallback(self, html: str, base_url: str) -> list[OfertaRaw]:
        """Fallback: parseo HTML genérico si no hay estado Nuxt."""
        soup = BeautifulSoup(html, "html.parser")
        offers: list[OfertaRaw] = []
        for card in soup.select("article, .job-card, .oferta-card, .card"):
            title_el = card.select_one("h2, h3, .job-title, .titulo")
            if not title_el:
                continue
            title = clean_text(title_el.get_text(" ", strip=True))
            link = card.select_one("a[href]")
            href = clean_text(link.get("href") if link else "")
            from urllib.parse import urljoin
            url = urljoin(base_url, href) if href else base_url
            is_offer, _ = self._score_offer_candidate(title, "", url=url)
            if not title or not is_offer:
                continue
            offers.append(
                OfertaRaw(
                    url=url,
                    cargo=title,
                    institucion_nombre=str(
                        self.institucion.get("nombre") or self.nombre_fuente
                    ),
                    descripcion=None,
                    sector=self.institucion.get("sector"),
                    tipo_cargo=None,
                    region=normalize_region(self.institucion.get("region")),
                    ciudad=None,
                    renta_texto=None,
                    renta_min=None,
                    renta_max=None,
                    grado_eus=None,
                    fecha_publicacion=None,
                    fecha_cierre=None,
                    area_profesional=None,
                    url_bases=None,
                )
            )
        return offers
