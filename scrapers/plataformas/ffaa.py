from __future__ import annotations

from typing import Any

from .generic_site import GenericSiteScraper


FFAA_KEYWORDS = (
    "personal civil",
    "dotacion",
    "dotación",
    "grado",
    "escalafon",
    "escalafón",
    "calidad juridica",
    "calidad jurídica",
    "reclutamiento",
    "admision",
    "admisión",
)

FFAA_PATHS = (
    "/ofertas",
    "/concursos",
    "/convocatorias",
    "/personal-civil",
    "/transparencia/concursos/",
    "/transparencia/trabaje-con-nosotros/",
)


class FfaaScraper(GenericSiteScraper):
    """Scraper heuristico con rutas candidatas especificas para FFAA."""

    def __init__(self, *, fuente_id: int, institucion: dict[str, Any]) -> None:
        super().__init__(
            fuente_id=fuente_id,
            institucion=institucion,
            candidate_paths=FFAA_PATHS,
            extra_keywords=FFAA_KEYWORDS,
            max_candidate_urls=6,
            detail_fetch_limit=14,
            trusted_host_only=False,
        )
