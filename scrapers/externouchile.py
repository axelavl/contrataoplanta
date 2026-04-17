"""
EmpleoEstado.cl — Scraper: Universidad de Chile (Portal Externo Trabajando.cl)
URL: https://externouchile.trabajando.cl/

Wrapper fino sobre el scraper genérico `scrapers.trabajando`, que cubre
todos los subdominios *.trabajando.cl. Mantenido por compatibilidad con
el orquestador existente.

Uso:
    python scrapers/externouchile.py
    python scrapers/externouchile.py --dry-run --verbose
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.trabajando import ejecutar as ejecutar_trabajando

FUENTE_ID = 242  # Universidad de Chile


def ejecutar(
    dry_run: bool = False,
    verbose: bool = False,
    max_results: int | None = None,
) -> dict[str, Any]:
    return ejecutar_trabajando(
        dry_run=dry_run,
        verbose=verbose,
        max_results=max_results,
        solo_id=FUENTE_ID,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraper UCH — externouchile.trabajando.cl (wrapper de scrapers.trabajando)"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--max", type=int, default=None)
    args = parser.parse_args()
    ejecutar(dry_run=args.dry_run, verbose=args.verbose, max_results=args.max)
