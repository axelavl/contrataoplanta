"""
Scraper para Gobiernos Regionales (GORE) y Delegaciones Presidenciales (DPR).

Todas estas instituciones publican en empleospublicos.cl (cubierto por el
batch central). Este módulo complementa esa cobertura scrapeando los portales
propios de las instituciones que mantienen páginas de concursos o empleo
en sus sitios web.

Uso standalone:
    python scrapers/gobiernos_regionales.py
    python scrapers/gobiernos_regionales.py --dry-run --max 5
    python scrapers/gobiernos_regionales.py --id 169       # solo GORE Arica
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.base import LOG_DIR, build_file_handler, clean_text
from scrapers.plataformas.generic_site import GenericSiteScraper

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scrapers.gobiernos_regionales")
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = build_file_handler(
        LOG_DIR / f"scraper_{time.strftime('%Y%m%d')}.log"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
logger.propagate = False

# IDs de instituciones con portal propio de empleo (además de empleospublicos.cl).
# Solo se incluyen las que tienen una URL de empleo específica (no homepage genérica).
_IDS_CON_PORTAL_PROPIO: frozenset[int] = frozenset({
    169,  # GORE Arica y Parinacota — concursos-publicos
    170,  # DPR Arica y Parinacota — noticias
    175,  # GORE Atacama — concursos-publicos
    179,  # GORE Valparaíso — concursosPersonal.php
    185,  # GORE Maule — concursos-publicos
    187,  # GORE Ñuble — trabaje-con-nosotros
    189,  # GORE Biobío — concursopublicoplanta
    195,  # GORE Los Lagos — concurso_publico.html
    197,  # GORE Aysén — trabaje-con-nosotros
    199,  # GORE Magallanes — trabaja-en-el-gore
})

REPO_PATH = Path(__file__).resolve().parents[1] / "repositorio_instituciones_publicas_chile.json"


def _load_instituciones(path: Path = REPO_PATH) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    instituciones = payload.get("instituciones") if isinstance(payload, dict) else payload
    return instituciones or []


def _filtrar_con_portal(
    instituciones: list[dict[str, Any]],
    solo_id: int | None = None,
) -> list[dict[str, Any]]:
    """Filtra instituciones del sector Gobierno Regional que tienen portal propio."""
    resultado = []
    for inst in instituciones:
        iid = inst.get("id")
        if solo_id is not None and iid != solo_id:
            continue
        if iid in _IDS_CON_PORTAL_PROPIO:
            resultado.append(inst)
    return resultado


def ejecutar(
    dry_run: bool = False,
    max_results: int | None = None,
    solo_id: int | None = None,
) -> dict[str, Any]:
    """Punto de entrada para el orquestador (run_scrapers.py)."""
    instituciones = _load_instituciones()
    fuentes = _filtrar_con_portal(instituciones, solo_id=solo_id)

    if not fuentes:
        logger.info("evento=gob_regional_sin_fuentes")
        return {"status": "OK", "found": 0, "nuevas": 0, "actualizadas": 0,
                "cerradas": 0, "errores": 0, "duracion_seg": 0}

    total_found = 0
    total_nuevas = 0
    total_actualizadas = 0
    total_cerradas = 0
    total_errores = 0
    start = time.time()

    for inst in fuentes:
        nombre = clean_text(inst.get("nombre")) or f"id_{inst.get('id')}"
        logger.info("evento=gob_regional_inicio institucion=%s id=%s", nombre, inst.get("id"))

        try:
            scraper = GenericSiteScraper(
                institucion=inst,
                instituciones_catalogo=instituciones,
                dry_run=dry_run,
                max_results=max_results,
                mode="production",
            )
            stats = scraper.run()

            found = stats.get("found", 0)
            total_found += found
            total_nuevas += stats.get("nuevas", 0)
            total_actualizadas += stats.get("actualizadas", 0)
            total_cerradas += stats.get("cerradas", 0)
            total_errores += stats.get("errores", 0)

            logger.info(
                "evento=gob_regional_ok institucion=%s found=%s status=%s",
                nombre, found, stats.get("status"),
            )
        except Exception as exc:
            total_errores += 1
            logger.exception(
                "evento=gob_regional_error institucion=%s error=%s",
                nombre, exc,
            )

    duracion = round(time.time() - start, 2)
    status = "OK" if total_errores == 0 else "PARCIAL"

    logger.info(
        "evento=gob_regional_fin fuentes=%s found=%s nuevas=%s errores=%s duracion=%s",
        len(fuentes), total_found, total_nuevas, total_errores, duracion,
    )

    return {
        "status": status,
        "found": total_found,
        "nuevas": total_nuevas,
        "actualizadas": total_actualizadas,
        "cerradas": total_cerradas,
        "errores": total_errores,
        "duracion_seg": duracion,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraper de portales propios de Gobiernos Regionales y Delegaciones Presidenciales"
    )
    parser.add_argument("--dry-run", action="store_true", help="No guarda en PostgreSQL")
    parser.add_argument("--max", type=int, default=None, help="Límite de ofertas por institución")
    parser.add_argument("--id", type=int, default=None, help="Ejecutar solo una institución")
    args = parser.parse_args()

    started = time.time()
    result = ejecutar(dry_run=args.dry_run, max_results=args.max, solo_id=args.id)
    print(result)
    print(f"Duración total: {round(time.time() - started, 2)}s")
