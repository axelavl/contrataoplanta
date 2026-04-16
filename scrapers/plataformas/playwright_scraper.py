"""
EmpleoEstado.cl — Módulo plataforma: scrapers que requieren Playwright (JS).

Despacha la ejecución al scraper custom correspondiente según el ID
de la institución. Cada scraper Playwright tiene su propio archivo
en scrapers/ con lógica de parseo específica.

Instituciones soportadas:
    - ID 145: Banco Central de Chile → scrapers/banco_central.py
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


# Mapeo de ID de institución → módulo de scraper Playwright.
# Agregar aquí nuevas instituciones que requieran JS.
PLAYWRIGHT_SCRAPERS: dict[int, str] = {
    145: "scrapers.banco_central",
    275: "scrapers.codelco",
    280: "scrapers.tvn",
}


def ejecutar(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
    max_results: int | None = None,
) -> dict[str, Any]:
    """
    Punto de entrada estándar para el orquestador (run_all.py).
    Despacha al scraper Playwright correcto según el ID de la institución.
    """
    inst_id = institucion.get("id")
    module_name = PLAYWRIGHT_SCRAPERS.get(inst_id)

    if not module_name:
        return {
            "status": "SKIP",
            "found": 0,
            "nuevas": 0,
            "actualizadas": 0,
            "cerradas": 0,
            "errores": 0,
            "duracion_seg": 0.0,
            "detalle": f"No hay scraper Playwright para ID {inst_id}",
        }

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        return {
            "status": "ERROR",
            "found": 0,
            "nuevas": 0,
            "actualizadas": 0,
            "cerradas": 0,
            "errores": 1,
            "duracion_seg": 0.0,
            "detalle": f"No se pudo importar {module_name}: {exc}",
        }

    # Todos los scrapers Playwright exponen ejecutar(dry_run, verbose)
    stats = module.ejecutar(dry_run=dry_run)

    # Normalizar el formato de stats al esquema esperado por run_all
    return {
        "status": "OK" if stats.get("errores", 0) == 0 else "PARCIAL",
        "found": stats.get("nuevas", 0) + stats.get("actualizadas", 0),
        "nuevas": stats.get("nuevas", 0),
        "actualizadas": stats.get("actualizadas", 0),
        "cerradas": stats.get("cerradas", 0),
        "errores": stats.get("errores", 0),
        "duracion_seg": 0.0,
    }


def load_instituciones(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return payload.get("instituciones") if isinstance(payload, dict) else payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper Playwright (requiere JS)")
    parser.add_argument("--json", required=True, help="Ruta al JSON maestro")
    parser.add_argument("--id", type=int, required=True, help="ID de la institucion")
    parser.add_argument("--dry-run", action="store_true", help="No guarda en PostgreSQL")
    args = parser.parse_args()

    instituciones = load_instituciones(args.json)
    objetivo = next((item for item in instituciones if item.get("id") == args.id), None)
    if not objetivo:
        raise SystemExit(f"No se encontro la institucion con id={args.id}")

    print(ejecutar(institucion=objetivo, instituciones_catalogo=instituciones, dry_run=args.dry_run))
