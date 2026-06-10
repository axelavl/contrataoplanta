"""
Regiones y Comunas de Chile — División Político-Administrativa (DPA)
Consume la API oficial del Estado: https://apis.digital.gob.cl/dpa/

Cachea los datos en memoria para no golpear la API en cada request.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import aiohttp

logger = logging.getLogger("api.regiones")

_DPA_BASE = "https://apis.digital.gob.cl/dpa"
_CACHE_TTL = 3600 * 24  # 24 horas

_regiones_cache: dict[str, Any] = {"ts": 0.0, "data": None}
_comunas_cache: dict[str, Any] = {}  # keyed by region code


async def _fetch_json(url: str) -> Any:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.json()


async def get_regiones() -> list[dict[str, Any]]:
    """Return list of Chilean regions from the DPA API, cached 24h."""
    now = time.time()
    if _regiones_cache["data"] and (now - _regiones_cache["ts"]) < _CACHE_TTL:
        return _regiones_cache["data"]

    try:
        raw = await _fetch_json(f"{_DPA_BASE}/regiones")
        regiones = [
            {
                "codigo": r["codigo"],
                "nombre": r["nombre"],
                "numero": r.get("numero"),
            }
            for r in raw
        ]
        # Sort by official number
        regiones.sort(key=lambda r: r.get("numero") or "99")
        _regiones_cache["data"] = regiones
        _regiones_cache["ts"] = now
        logger.info("Regiones DPA cargadas: %d", len(regiones))
        return regiones
    except Exception as exc:
        logger.warning("Error al obtener regiones DPA: %s", exc)
        if _regiones_cache["data"]:
            return _regiones_cache["data"]
        return _regiones_fallback()


async def get_comunas(codigo_region: str) -> list[dict[str, Any]]:
    """Return comunas for a given region code, cached 24h."""
    now = time.time()
    cached = _comunas_cache.get(codigo_region)
    if cached and (now - cached["ts"]) < _CACHE_TTL:
        return cached["data"]

    try:
        raw = await _fetch_json(f"{_DPA_BASE}/regiones/{codigo_region}/comunas")
        comunas = sorted(
            [{"codigo": c["codigo"], "nombre": c["nombre"]} for c in raw],
            key=lambda c: c["nombre"],
        )
        _comunas_cache[codigo_region] = {"data": comunas, "ts": now}
        logger.info("Comunas para region %s: %d", codigo_region, len(comunas))
        return comunas
    except Exception as exc:
        logger.warning("Error al obtener comunas DPA para %s: %s", codigo_region, exc)
        if cached:
            return cached["data"]
        return []


def _regiones_fallback() -> list[dict[str, Any]]:
    """Fallback hardcoded if the DPA API is unreachable on first call."""
    return [
        {"codigo": "15", "nombre": "Arica y Parinacota", "numero": "XV"},
        {"codigo": "01", "nombre": "Tarapacá", "numero": "I"},
        {"codigo": "02", "nombre": "Antofagasta", "numero": "II"},
        {"codigo": "03", "nombre": "Atacama", "numero": "III"},
        {"codigo": "04", "nombre": "Coquimbo", "numero": "IV"},
        {"codigo": "05", "nombre": "Valparaíso", "numero": "V"},
        {"codigo": "13", "nombre": "Metropolitana de Santiago", "numero": "XIII"},
        {"codigo": "06", "nombre": "O'Higgins", "numero": "VI"},
        {"codigo": "07", "nombre": "Maule", "numero": "VII"},
        {"codigo": "16", "nombre": "Ñuble", "numero": "XVI"},
        {"codigo": "08", "nombre": "Biobío", "numero": "VIII"},
        {"codigo": "09", "nombre": "La Araucanía", "numero": "IX"},
        {"codigo": "14", "nombre": "Los Ríos", "numero": "XIV"},
        {"codigo": "10", "nombre": "Los Lagos", "numero": "X"},
        {"codigo": "11", "nombre": "Aysén", "numero": "XI"},
        {"codigo": "12", "nombre": "Magallanes", "numero": "XII"},
    ]
