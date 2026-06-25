#!/usr/bin/env python3
"""Backfill de `renta_tipo` para ofertas ya cargadas.

La columna `renta_tipo` es nueva (migración 20260624_0001_renta_tipo). Las
filas existentes quedan con `renta_tipo IS NULL` hasta que el scraper las vuelve
a tocar. Este script reclasifica esas filas a partir del texto disponible
(`renta_texto`, `descripcion`, `cargo`) y, además, deja señalada en
`renta_texto` la marca explícita cuando la renta es líquida.

Regla de negocio: la renta informada debe ser BRUTA; si la fuente solo trae
LÍQUIDA hay que señalarlo (no se asume bruta cuando no hay señal).

Uso:
    python scripts/backfill_renta_tipo.py --dry-run        # solo informa
    python scripts/backfill_renta_tipo.py                  # aplica cambios
    python scripts/backfill_renta_tipo.py --solo-activas   # limita a activas
    python scripts/backfill_renta_tipo.py --rehacer        # reprocesa también
                                                           # filas ya clasificadas

Requiere las mismas variables de entorno que la API/​scrapers (DATABASE_URL o
DB_HOST/DB_PORT/...), resueltas por `db.config`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Permite ejecutar el script directamente (`python scripts/backfill_renta_tipo.py`)
# resolviendo los imports del paquete del proyecto.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2
import psycopg2.extras

from db.config import DB_CONFIG
from extraction.renta_tipo import (
    TIPO_INDETERMINADA,
    anotar_renta_texto,
    resolver_tipo_renta,
)


def _build_query(solo_activas: bool, rehacer: bool) -> str:
    filtros = []
    if solo_activas:
        # `activa` es el flag del esquema de producción; algunos despliegues
        # legacy usan `estado`. Toleramos ambos.
        filtros.append("(activa IS TRUE OR estado IN ('activo', 'active'))")
    if not rehacer:
        filtros.append("renta_tipo IS NULL")
    where = (" WHERE " + " AND ".join(filtros)) if filtros else ""
    return (
        "SELECT id, renta_texto, descripcion, cargo, renta_tipo "
        f"FROM ofertas{where}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill de renta_tipo")
    parser.add_argument("--dry-run", action="store_true", help="No escribe; solo informa")
    parser.add_argument("--solo-activas", action="store_true", help="Limita a ofertas activas")
    parser.add_argument(
        "--rehacer",
        action="store_true",
        help="Reprocesa también filas que ya tienen renta_tipo",
    )
    parser.add_argument("--batch", type=int, default=500, help="Tamaño de commit")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    leidas = 0
    actualizadas = 0
    por_tipo: dict[str, int] = {}

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_build_query(args.solo_activas, args.rehacer))
            filas = cur.fetchall()

        with conn.cursor() as upd:
            for fila in filas:
                leidas += 1
                tipo = resolver_tipo_renta(
                    fila.get("renta_texto"),
                    fila.get("descripcion"),
                    fila.get("cargo"),
                )
                nuevo_texto = anotar_renta_texto(fila.get("renta_texto"), tipo)

                cambia_tipo = (fila.get("renta_tipo") or None) != tipo
                cambia_texto = (fila.get("renta_texto") or None) != (nuevo_texto or None)
                if not (cambia_tipo or cambia_texto):
                    continue

                por_tipo[tipo] = por_tipo.get(tipo, 0) + 1
                actualizadas += 1
                if not args.dry_run:
                    upd.execute(
                        "UPDATE ofertas SET renta_tipo = %s, renta_texto = %s WHERE id = %s",
                        (tipo, nuevo_texto, fila["id"]),
                    )
                    if actualizadas % args.batch == 0:
                        conn.commit()

        if not args.dry_run:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    modo = "DRY-RUN (sin cambios)" if args.dry_run else "APLICADO"
    print(f"[{modo}] filas leídas={leidas} a_actualizar={actualizadas}")
    for tipo in (por_tipo.keys() if por_tipo else [TIPO_INDETERMINADA]):
        if tipo in por_tipo:
            print(f"  {tipo}: {por_tipo[tipo]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
