from __future__ import annotations

from typing import Any

from .pdf_first import PdfFirstScraper


PDI_URLS = (
    "/institucion/concursos-publicos/portada",
    "/transparencia/concursos/",
    "/transparencia/trabaje-con-nosotros/",
)

PDI_KEYWORDS = (
    "perfil",
    "bases",
    "concurso publico",
    "postulacion",
    "postulación",
    "cargo",
)


class PdiScraper(PdfFirstScraper):
    """Adaptador runtime-compatible para concursos publicos de PDI."""

    def __init__(self, *, fuente_id: int, institucion: dict[str, Any]) -> None:
        super().__init__(
            fuente_id=fuente_id,
            institucion=institucion,
            candidate_paths=PDI_URLS,
            extra_keywords=PDI_KEYWORDS,
            max_candidate_urls=4,
            detail_fetch_limit=10,
            trusted_host_only=False,
        )
