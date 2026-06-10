"""
Biblioteca del Congreso Nacional — Ley Chile
Enlaza cada institución con la ley que la rige.

Usa la base pública de leychile.cl para buscar la normativa
asociada a una institución del Estado.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import aiohttp

logger = logging.getLogger("api.leyes")

_BCN_SEARCH = "https://www.bcn.cl/leychile/consulta/busqueda_j"
_BCN_LINK = "https://www.bcn.cl/leychile/navegar?idNorma="
_CACHE_TTL = 3600 * 24 * 7  # 7 días — las leyes no cambian seguido

# Mapeo conocido: instituciones → ley orgánica que las rige
# Esto evita consultas redundantes para las más comunes
_LEYES_CONOCIDAS: dict[str, dict[str, Any]] = {
    "Servicio Civil": {
        "ley": "Ley 19.882",
        "titulo": "Ley de Nuevo Trato Laboral y Alta Dirección Pública",
        "url": f"{_BCN_LINK}211480",
        "id_norma": "211480",
    },
    "Contraloría General de la República": {
        "ley": "Ley 10.336",
        "titulo": "Ley de Organización y Atribuciones de la Contraloría General de la República",
        "url": f"{_BCN_LINK}25808",
        "id_norma": "25808",
    },
    "Poder Judicial": {
        "ley": "DFL 1 (Justicia)",
        "titulo": "Código Orgánico de Tribunales",
        "url": f"{_BCN_LINK}25563",
        "id_norma": "25563",
    },
    "Ministerio Público": {
        "ley": "Ley 19.640",
        "titulo": "Ley Orgánica Constitucional del Ministerio Público",
        "url": f"{_BCN_LINK}145437",
        "id_norma": "145437",
    },
    "Banco Central de Chile": {
        "ley": "Ley 18.840",
        "titulo": "Ley Orgánica Constitucional del Banco Central de Chile",
        "url": f"{_BCN_LINK}30105",
        "id_norma": "30105",
    },
    "Dirección del Trabajo": {
        "ley": "DFL 2 (Trabajo)",
        "titulo": "Ley Orgánica de la Dirección del Trabajo",
        "url": f"{_BCN_LINK}5765",
        "id_norma": "5765",
    },
    "Servicio de Impuestos Internos": {
        "ley": "DFL 7 (Hacienda)",
        "titulo": "Ley Orgánica del Servicio de Impuestos Internos",
        "url": f"{_BCN_LINK}5618",
        "id_norma": "5618",
    },
    "Carabineros de Chile": {
        "ley": "Ley 18.961",
        "titulo": "Ley Orgánica Constitucional de Carabineros de Chile",
        "url": f"{_BCN_LINK}30329",
        "id_norma": "30329",
    },
    "Policía de Investigaciones": {
        "ley": "DL 2.460",
        "titulo": "Ley Orgánica de la Policía de Investigaciones de Chile",
        "url": f"{_BCN_LINK}6956",
        "id_norma": "6956",
    },
    "Registro Civil": {
        "ley": "DFL 1 (Justicia, 2000)",
        "titulo": "Ley Orgánica del Servicio de Registro Civil e Identificación",
        "url": f"{_BCN_LINK}176076",
        "id_norma": "176076",
    },
    "Gendarmería de Chile": {
        "ley": "DL 2.859",
        "titulo": "Ley Orgánica de Gendarmería de Chile",
        "url": f"{_BCN_LINK}7024",
        "id_norma": "7024",
    },
    "Servicio Nacional de Aduanas": {
        "ley": "DFL 329 (Hacienda)",
        "titulo": "Ley Orgánica del Servicio Nacional de Aduanas",
        "url": f"{_BCN_LINK}5328",
        "id_norma": "5328",
    },
    "Tesorería General de la República": {
        "ley": "DFL 1 (Hacienda, 1994)",
        "titulo": "Estatuto Orgánico del Servicio de Tesorerías",
        "url": f"{_BCN_LINK}6536",
        "id_norma": "6536",
    },
    "CONAF": {
        "ley": "DL 701",
        "titulo": "Ley de Fomento Forestal",
        "url": f"{_BCN_LINK}6294",
        "id_norma": "6294",
    },
    "SERNAC": {
        "ley": "Ley 19.496",
        "titulo": "Ley de Protección de los Derechos de los Consumidores",
        "url": f"{_BCN_LINK}61438",
        "id_norma": "61438",
    },
    "Defensoría Penal Pública": {
        "ley": "Ley 19.718",
        "titulo": "Ley de la Defensoría Penal Pública",
        "url": f"{_BCN_LINK}182533",
        "id_norma": "182533",
    },
    "FONASA": {
        "ley": "DFL 1 (Salud, 2005)",
        "titulo": "Ley que refunde textos sobre régimen de salud (FONASA)",
        "url": f"{_BCN_LINK}249177",
        "id_norma": "249177",
    },
    "Servicio Médico Legal": {
        "ley": "DFL 196 (Justicia)",
        "titulo": "Ley Orgánica del Servicio Médico Legal",
        "url": f"{_BCN_LINK}4832",
        "id_norma": "4832",
    },
    "ChileCompra": {
        "ley": "Ley 19.886",
        "titulo": "Ley de Bases sobre Contratos Administrativos de Suministro y Prestación de Servicios",
        "url": f"{_BCN_LINK}213004",
        "id_norma": "213004",
    },
    "INDAP": {
        "ley": "DFL 13 (Agricultura)",
        "titulo": "Ley Orgánica del Instituto de Desarrollo Agropecuario",
        "url": f"{_BCN_LINK}3297",
        "id_norma": "3297",
    },
    "SAG": {
        "ley": "Ley 18.755",
        "titulo": "Ley Orgánica del Servicio Agrícola y Ganadero",
        "url": f"{_BCN_LINK}29961",
        "id_norma": "29961",
    },
    "SENAME": {
        "ley": "DL 2.465",
        "titulo": "Ley Orgánica del Servicio Nacional de Menores",
        "url": f"{_BCN_LINK}6929",
        "id_norma": "6929",
    },
    "INE": {
        "ley": "DL 3.551",
        "titulo": "Ley del Instituto Nacional de Estadísticas",
        "url": f"{_BCN_LINK}7121",
        "id_norma": "7121",
    },
    "Servicio Electoral": {
        "ley": "Ley 18.556",
        "titulo": "Ley Orgánica del Servicio Electoral",
        "url": f"{_BCN_LINK}29951",
        "id_norma": "29951",
    },
    "Superintendencia de Salud": {
        "ley": "DFL 1 (Salud, 2005)",
        "titulo": "Ley sobre régimen de prestaciones de salud",
        "url": f"{_BCN_LINK}249177",
        "id_norma": "249177",
    },
    "Superintendencia de Educación": {
        "ley": "Ley 20.529",
        "titulo": "Sistema Nacional de Aseguramiento de la Calidad de la Educación",
        "url": f"{_BCN_LINK}1028635",
        "id_norma": "1028635",
    },
    "CORFO": {
        "ley": "DFL 211 (Hacienda)",
        "titulo": "Ley Orgánica de CORFO",
        "url": f"{_BCN_LINK}4804",
        "id_norma": "4804",
    },
    "Servicio de Salud": {
        "ley": "DFL 1 (Salud, 2005)",
        "titulo": "Ley que fija texto refundido del DL 2.763 (Salud)",
        "url": f"{_BCN_LINK}249177",
        "id_norma": "249177",
    },
}

# Ley base que rige a la mayoría de los funcionarios públicos
_LEY_ESTATUTO_ADMIN = {
    "ley": "Ley 18.834",
    "titulo": "Estatuto Administrativo",
    "url": f"{_BCN_LINK}30099",
    "id_norma": "30099",
}

_LEY_ESTATUTO_MUNICIPAL = {
    "ley": "Ley 18.883",
    "titulo": "Estatuto Administrativo para Funcionarios Municipales",
    "url": f"{_BCN_LINK}30132",
    "id_norma": "30132",
}

_LEY_CODIGO_TRABAJO = {
    "ley": "DFL 1 (Trabajo, 2002)",
    "titulo": "Código del Trabajo",
    "url": f"{_BCN_LINK}207436",
    "id_norma": "207436",
}

# Cache for dynamic BCN lookups
_search_cache: dict[str, dict[str, Any] | None] = {}
_search_cache_ts: dict[str, float] = {}


def get_ley_institucion(
    nombre: str,
    sigla: str | None = None,
    sector: str | None = None,
    tipo_cargo: str | None = None,
) -> dict[str, Any]:
    """
    Returns the law governing an institution.
    Checks known mappings first, then falls back to sector-based defaults.
    """
    # Check exact matches
    for key, ley in _LEYES_CONOCIDAS.items():
        if key.lower() in nombre.lower() or (sigla and key.lower() == sigla.lower()):
            return {**ley, "fuente": "bcn_directo"}

    # Check by sigla
    if sigla:
        for key, ley in _LEYES_CONOCIDAS.items():
            if sigla.upper() == key.upper():
                return {**ley, "fuente": "bcn_directo"}

    # Sector-based fallback
    sector_lower = (sector or "").lower()
    tipo_lower = (tipo_cargo or "").lower()

    if "municipal" in sector_lower or "municipalidad" in nombre.lower():
        return {**_LEY_ESTATUTO_MUNICIPAL, "fuente": "estatuto_sector"}

    if "honorario" in tipo_lower:
        return {**_LEY_CODIGO_TRABAJO, "fuente": "estatuto_sector"}

    # Default: Estatuto Administrativo
    return {**_LEY_ESTATUTO_ADMIN, "fuente": "estatuto_sector"}


async def buscar_ley_bcn(termino: str) -> list[dict[str, Any]]:
    """
    Search BCN LeyChile for a legal term. Returns top results.
    Cached for 7 days.
    """
    now = time.time()
    cached_ts = _search_cache_ts.get(termino, 0.0)
    if termino in _search_cache and (now - cached_ts) < _CACHE_TTL:
        result = _search_cache[termino]
        return [result] if result else []

    try:
        url = f"{_BCN_SEARCH}?q={termino}&limit=5"
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

        results = []
        for item in (data if isinstance(data, list) else data.get("resultados", [])):
            id_norma = str(item.get("idNorma", ""))
            results.append({
                "ley": item.get("titulo", ""),
                "titulo": item.get("nombre", ""),
                "url": f"{_BCN_LINK}{id_norma}" if id_norma else "",
                "id_norma": id_norma,
                "fuente": "bcn_busqueda",
            })

        if results:
            _search_cache[termino] = results[0]
            _search_cache_ts[termino] = now
        return results

    except Exception as exc:
        logger.warning("Error buscando en BCN LeyChile '%s': %s", termino, exc)
        return []
