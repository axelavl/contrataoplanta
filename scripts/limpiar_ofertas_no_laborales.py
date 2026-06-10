#!/usr/bin/env python3
"""Limpieza única: desactiva ofertas activas que el intake actual rechazaría.

Las mejoras de ``scrapers/intake.py`` (rechazo de actas/decretos/etiquetas,
fecha de publicación desde la URL, cap de fecha_cierre futura inverosímil)
evitan NUEVAS inserciones de basura, pero las filas ya guardadas en la BD
siguen activas: al re-scrapear, una oferta rechazada simplemente no se
actualiza, no se borra. Este script las pasa de nuevo por el intake y
desactiva (``activa = FALSE``) las que hoy serían descartadas.

Uso:
    python scripts/limpiar_ofertas_no_laborales.py            # dry-run (no escribe)
    python scripts/limpiar_ofertas_no_laborales.py --apply    # aplica los cambios
    python scripts/limpiar_ofertas_no_laborales.py --apply --limit 500
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402

from db.config import DB_CONFIG  # noqa: E402
from scrapers.intake import intake_validate_offer  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Desactiva ofertas activas que el intake rechazaría.")
    p.add_argument("--apply", action="store_true", help="Aplica los cambios (por defecto: dry-run).")
    p.add_argument("--limit", type=int, default=None, help="Procesar a lo sumo N ofertas.")
    args = p.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sql = """
        SELECT id, cargo, url_oferta, url_original, descripcion, requisitos,
               renta_texto, renta_bruta_min, renta_bruta_max,
               fecha_publicacion, fecha_cierre, institucion_nombre
        FROM ofertas
        WHERE activa = TRUE
        ORDER BY id
    """
    if args.limit:
        sql += f"\n        LIMIT {int(args.limit)}"
    cur.execute(sql)
    filas = cur.fetchall()
    print(f"Ofertas activas analizadas: {len(filas)}")

    a_desactivar: list[int] = []
    motivos: Counter[str] = Counter()
    for row in filas:
        offer = dict(row)
        offer.setdefault("url_oferta", offer.get("url_original"))
        decision = intake_validate_offer(offer)
        if decision.discard:
            a_desactivar.append(row["id"])
            motivos[decision.motivo_descarte or "desconocido"] += 1

    print(f"\nSerían desactivadas: {len(a_desactivar)}")
    for motivo, n in motivos.most_common():
        print(f"  {n:5}  {motivo}")

    if not a_desactivar:
        print("\nNada que hacer.")
        return 0

    if not args.apply:
        print("\n(dry-run) No se escribió nada. Re-ejecuta con --apply para aplicar.")
        print("Ejemplos (primeros 15 ids):", a_desactivar[:15])
        return 0

    cur.execute(
        "UPDATE ofertas SET activa = FALSE, actualizada_en = NOW() WHERE id = ANY(%s)",
        (a_desactivar,),
    )
    conn.commit()
    print(f"\nDesactivadas {cur.rowcount} ofertas.")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
