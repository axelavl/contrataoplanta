"""
Meilisearch — Motor de búsqueda ultra-rápido
Proporciona autocompletado, búsqueda con sinónimos y resultados en ~10ms.

Indexa las ofertas en Meilisearch para:
- Autocompletado de cargos instantáneo
- Búsqueda con sinónimos ("RRHH" → "Recursos Humanos")
- Tolerancia a typos
- Filtros por faceta (región, sector, tipo)
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("api.meilisearch")

_MEILI_URL = os.getenv("MEILISEARCH_URL", "http://localhost:7700")
_MEILI_KEY = os.getenv("MEILISEARCH_API_KEY", "")
_INDEX_OFERTAS = "ofertas"

# Sinónimos comunes en empleo público chileno
SINONIMOS = {
    "RRHH": ["Recursos Humanos", "Gestión de Personas", "Capital Humano"],
    "Recursos Humanos": ["RRHH", "Gestión de Personas", "Capital Humano"],
    "TI": ["Tecnología", "Informática", "Sistemas", "IT", "Computación"],
    "Informática": ["TI", "Tecnología", "Sistemas", "IT"],
    "IT": ["TI", "Tecnología", "Informática", "Sistemas"],
    "Contabilidad": ["Contable", "Finanzas", "Contador"],
    "Contador": ["Contabilidad", "Contable", "Finanzas"],
    "Abogado": ["Jurídico", "Legal", "Derecho"],
    "Legal": ["Abogado", "Jurídico", "Derecho"],
    "Jurídico": ["Abogado", "Legal", "Derecho"],
    "Enfermería": ["Enfermera", "Enfermero", "TENS"],
    "TENS": ["Técnico en Enfermería", "Enfermería"],
    "Psicólogo": ["Psicología", "Salud Mental"],
    "Psicología": ["Psicólogo", "Salud Mental"],
    "Asistente Social": ["Trabajo Social", "Trabajadora Social", "Trabajador Social"],
    "Trabajo Social": ["Asistente Social", "Trabajadora Social"],
    "Ingeniero": ["Ingeniería", "Ingeniera"],
    "Ingeniera": ["Ingeniería", "Ingeniero"],
    "Administrativo": ["Administrativa", "Administración", "Secretaria"],
    "Secretaria": ["Secretario", "Administrativo", "Administrativa"],
    "Médico": ["Medicina", "Doctor", "Doctora", "Médica"],
    "Kinesiólogo": ["Kinesiología", "Kinesióloga"],
    "Nutricionista": ["Nutrición"],
    "Odontólogo": ["Odontología", "Dentista"],
    "Paramédico": ["Paramédica", "TENS"],
    "Matrona": ["Matrón", "Obstetricia"],
    "Conductor": ["Chofer"],
    "Chofer": ["Conductor"],
    "Auxiliar": ["Auxiliar de Servicio", "Auxiliar de Aseo"],
    "Planta": ["Titular", "Titularidad"],
    "Contrata": ["Contrato a contrata"],
    "Honorarios": ["Honorario", "A honorarios", "Boleta de honorarios"],
    "ADP": ["Alta Dirección Pública"],
    "Municipalidad": ["Municipio", "Municipal", "Ilustre Municipalidad"],
    "Municipio": ["Municipalidad", "Municipal"],
}


def _get_client():
    """Get Meilisearch client, lazy import."""
    try:
        import meilisearch
        return meilisearch.Client(_MEILI_URL, _MEILI_KEY)
    except ImportError:
        logger.warning("meilisearch-python no instalado")
        return None
    except Exception as exc:
        logger.warning("Error conectando a Meilisearch: %s", exc)
        return None


def configurar_indice() -> bool:
    """Configure the ofertas index with settings, synonyms, and filterable attributes."""
    client = _get_client()
    if not client:
        return False

    try:
        index = client.index(_INDEX_OFERTAS)

        # Update settings
        index.update_settings({
            "searchableAttributes": [
                "cargo",
                "institucion",
                "descripcion",
                "area_profesional",
                "sector",
                "region",
                "ciudad",
            ],
            "filterableAttributes": [
                "region",
                "sector",
                "tipo_contrato",
                "activo",
                "institucion_id",
            ],
            "sortableAttributes": [
                "fecha_publicacion",
                "fecha_cierre",
                "renta_bruta_max",
                "renta_bruta_min",
                "fecha_scraped",
            ],
            "rankingRules": [
                "words",
                "typo",
                "proximity",
                "attribute",
                "sort",
                "exactness",
            ],
            "synonyms": SINONIMOS,
            "typoTolerance": {
                "enabled": True,
                "minWordSizeForTypos": {
                    "oneTypo": 4,
                    "twoTypos": 8,
                },
            },
        })
        logger.info("Índice Meilisearch configurado correctamente")
        return True
    except Exception as exc:
        logger.error("Error configurando índice Meilisearch: %s", exc)
        return False


def indexar_ofertas(ofertas: list[dict[str, Any]]) -> bool:
    """Index or update a batch of ofertas in Meilisearch."""
    client = _get_client()
    if not client:
        return False

    try:
        docs = []
        for o in ofertas:
            doc = {
                "id": o.get("id"),
                "cargo": o.get("cargo", ""),
                "institucion": o.get("institucion") or o.get("institucion_nombre", ""),
                "institucion_id": o.get("institucion_id"),
                "descripcion": o.get("descripcion", ""),
                "area_profesional": o.get("area_profesional", ""),
                "tipo_contrato": o.get("tipo_contrato") or o.get("tipo_cargo", ""),
                "region": o.get("region", ""),
                "ciudad": o.get("ciudad", ""),
                "sector": o.get("sector", ""),
                "renta_bruta_min": o.get("renta_bruta_min"),
                "renta_bruta_max": o.get("renta_bruta_max"),
                "fecha_publicacion": str(o["fecha_publicacion"]) if o.get("fecha_publicacion") else None,
                "fecha_cierre": str(o["fecha_cierre"]) if o.get("fecha_cierre") else None,
                "fecha_scraped": str(o["fecha_scraped"]) if o.get("fecha_scraped") else None,
                "url_oferta": o.get("url_oferta", ""),
                "activo": o.get("estado") == "activo" if o.get("estado") else o.get("activa", True),
            }
            docs.append(doc)

        index = client.index(_INDEX_OFERTAS)
        index.add_documents(docs, primary_key="id")
        logger.info("Indexadas %d ofertas en Meilisearch", len(docs))
        return True
    except Exception as exc:
        logger.error("Error indexando en Meilisearch: %s", exc)
        return False


def buscar(
    q: str,
    filtros: dict[str, str] | None = None,
    limite: int = 10,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Search ofertas with Meilisearch (~10ms response time).
    Returns results with highlights and facets.
    """
    client = _get_client()
    if not client:
        return {"hits": [], "total": 0, "ms": 0, "disponible": False}

    try:
        filter_parts = []
        if filtros:
            if filtros.get("region"):
                filter_parts.append(f'region = "{filtros["region"]}"')
            if filtros.get("sector"):
                filter_parts.append(f'sector = "{filtros["sector"]}"')
            if filtros.get("tipo_contrato"):
                filter_parts.append(f'tipo_contrato = "{filtros["tipo_contrato"]}"')
            if filtros.get("activo"):
                filter_parts.append("activo = true")

        search_params: dict[str, Any] = {
            "limit": limite,
            "offset": offset,
            "attributesToHighlight": ["cargo", "institucion", "descripcion"],
            "highlightPreTag": "<mark>",
            "highlightPostTag": "</mark>",
        }
        if filter_parts:
            search_params["filter"] = " AND ".join(filter_parts)

        index = client.index(_INDEX_OFERTAS)
        result = index.search(q, search_params)

        return {
            "hits": result.get("hits", []),
            "total": result.get("estimatedTotalHits", 0),
            "ms": result.get("processingTimeMs", 0),
            "disponible": True,
        }
    except Exception as exc:
        logger.warning("Error buscando en Meilisearch: %s", exc)
        return {"hits": [], "total": 0, "ms": 0, "disponible": False}


def autocompletar(q: str, limite: int = 8) -> list[dict[str, str]]:
    """
    Fast autocomplete for job titles.
    Returns list of {cargo, institucion} suggestions.
    """
    client = _get_client()
    if not client:
        return []

    try:
        index = client.index(_INDEX_OFERTAS)
        result = index.search(q, {
            "limit": limite,
            "attributesToRetrieve": ["cargo", "institucion", "region"],
            "attributesToHighlight": ["cargo"],
            "highlightPreTag": "<b>",
            "highlightPostTag": "</b>",
            "filter": "activo = true",
        })

        suggestions = []
        seen = set()
        for hit in result.get("hits", []):
            cargo = hit.get("cargo", "")
            key = cargo.lower()
            if key not in seen:
                seen.add(key)
                suggestions.append({
                    "cargo": cargo,
                    "cargo_highlight": hit.get("_formatted", {}).get("cargo", cargo),
                    "institucion": hit.get("institucion", ""),
                    "region": hit.get("region", ""),
                })
        return suggestions
    except Exception as exc:
        logger.warning("Error en autocompletado Meilisearch: %s", exc)
        return []
