#!/usr/bin/env python3
"""
Audita la tabla `ofertas` contra las CHECK constraints de la migración
`db/migrations/005_data_quality_constraints.sql`. Útil en dos momentos:

1. **Antes** de aplicar 005: estimar cuántas filas quedarían "en deuda"
   (las constraints usan `NOT VALID` → nuevas filas se validan, las
   existentes se tolerarán; este script dice cuánto hay que limpiar
   antes de poder hacer `VALIDATE CONSTRAINT`).

2. **Después** de aplicar: verificar que los scrapers nuevos no
   introducen violaciones (el count debería mantenerse en el mismo
   número histórico o bajar si hay UPDATE sobre ofertas vencidas).

Uso:
    python scripts/qa/check_data_quality.py             # humano legible
    python scripts/qa/check_data_quality.py --json      # salida JSON
    python scripts/qa/check_data_quality.py --samples 5 # muestra 5 ids

Requiere las mismas env vars que la API (`DB_HOST`, `DB_PORT`, ...).
Exit code 0 siempre — es reporte, no gate.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # pragma: no cover
    sys.stderr.write("psycopg2 no está instalado (pip install psycopg2-binary)\n")
    sys.exit(2)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "empleospublicos"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD") or "",
}


# Cada chequeo = (nombre legible, SQL WHERE que selecciona filas violatorias).
CHECKS: tuple[tuple[str, str], ...] = (
    (
        "renta_bruta_min fuera de rango (300k–20M)",
        "renta_bruta_min IS NOT NULL AND renta_bruta_min NOT BETWEEN 300000 AND 20000000",
    ),
    (
        "renta_bruta_max fuera de rango (300k–20M)",
        "renta_bruta_max IS NOT NULL AND renta_bruta_max NOT BETWEEN 300000 AND 20000000",
    ),
    (
        "renta_bruta_min > renta_bruta_max",
        "renta_bruta_min IS NOT NULL AND renta_bruta_max IS NOT NULL AND renta_bruta_min > renta_bruta_max",
    ),
    (
        "fecha_publicacion fuera de rango (2020 – current+1y)",
        "fecha_publicacion IS NOT NULL AND "
        "(fecha_publicacion < DATE '2020-01-01' OR fecha_publicacion > CURRENT_DATE + INTERVAL '1 year')",
    ),
    (
        "fecha_cierre fuera de rango (2020 – current+3y)",
        "fecha_cierre IS NOT NULL AND "
        "(fecha_cierre < DATE '2020-01-01' OR fecha_cierre > CURRENT_DATE + INTERVAL '3 years')",
    ),
    (
        "horas_semanales fuera de rango (1–88)",
        "horas_semanales IS NOT NULL AND horas_semanales NOT BETWEEN 1 AND 88",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="salida JSON")
    parser.add_argument(
        "--samples",
        type=int,
        default=0,
        help="ids de muestra a imprimir por cada chequeo",
    )
    args = parser.parse_args()

    if not DB_CONFIG["password"]:
        sys.stderr.write(
            "ERROR: DB_PASSWORD no definido. Configúralo o exporta las vars DB_*.\n"
        )
        return 2

    with closing(psycopg2.connect(**DB_CONFIG)) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS total FROM ofertas")
            total = cur.fetchone()["total"]

            reporte: list[dict] = []
            for nombre, where in CHECKS:
                cur.execute(f"SELECT COUNT(*) AS c FROM ofertas WHERE {where}")
                c = cur.fetchone()["c"]
                entrada: dict = {"check": nombre, "violaciones": c}
                if c and args.samples:
                    cur.execute(
                        f"SELECT id, cargo, institucion_nombre, "
                        f"renta_bruta_min, renta_bruta_max, "
                        f"fecha_publicacion, fecha_cierre, horas_semanales "
                        f"FROM ofertas WHERE {where} LIMIT %s",
                        [args.samples],
                    )
                    entrada["ejemplos"] = [dict(r) for r in cur.fetchall()]
                reporte.append(entrada)

    if args.json:
        print(json.dumps({"total_ofertas": total, "checks": reporte},
                         ensure_ascii=False, indent=2, default=str))
        return 0

    print(f"Total ofertas: {total:,}")
    print("-" * 72)
    any_viol = False
    for r in reporte:
        n = r["violaciones"]
        marker = "✓" if n == 0 else "✗"
        pct = (n / total * 100) if total else 0
        print(f"  {marker} {r['check']:<55} {n:>6,} ({pct:5.2f}%)")
        if r.get("ejemplos"):
            for ex in r["ejemplos"]:
                print(f"      id={ex['id']} cargo={ex['cargo']!r:<40.40} "
                      f"renta=({ex['renta_bruta_min']},{ex['renta_bruta_max']}) "
                      f"fpub={ex['fecha_publicacion']} fcie={ex['fecha_cierre']} "
                      f"hrs={ex['horas_semanales']}")
        if n:
            any_viol = True
    print("-" * 72)
    if any_viol:
        print(
            "\nHay filas que violan las constraints. La migración 005 usa NOT\n"
            "VALID, así que el ADD CONSTRAINT no fallará — sólo los nuevos\n"
            "INSERT/UPDATE se validarán. Cuando limpies estas filas, corre:\n"
            "   ALTER TABLE ofertas VALIDATE CONSTRAINT <nombre>;"
        )
    else:
        print("\nSin violaciones. Podés validar las constraints existentes:")
        print("   ALTER TABLE ofertas VALIDATE CONSTRAINT chk_ofertas_renta_min_rango;")
        print("   (y el resto — ver db/migrations/005_*.sql)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
