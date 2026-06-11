#!/usr/bin/env python3
"""Guardrail de calidad del catálogo de ofertas (solo lectura).

Corre una batería de chequeos sobre las ofertas activas y deja un resumen.
Pensado para correr programado (p.ej. diario) y avisar de regresiones —en vez
de descubrirlas navegando el sitio—. No modifica nada; el actuador es
``scripts/limpiar_ofertas_no_laborales.py``.

Uso:
    python scripts/reporte_calidad_catalogo.py            # reporte legible
    python scripts/reporte_calidad_catalogo.py --json     # salida JSON (alertas/cron)
    python scripts/reporte_calidad_catalogo.py --muestras 5

Sale con código 1 si hay hallazgos de severidad ALTA (útil para cron/alertas).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402

from db.config import DB_CONFIG  # noqa: E402
from scrapers.intake import intake_validate_offer  # noqa: E402

# Umbrales (alineados con el validador)
DIAS_CIERRE_FUTURO_MAX = 150
BUCKET_ACTIVAS_ALERTA = 60   # una institución con más de N activas: revisar
DESC_MIN = 30


def _coerce_date(v):
    return v if isinstance(v, date) else None


def main() -> int:
    ap = argparse.ArgumentParser(description="Reporte de calidad del catálogo (solo lectura).")
    ap.add_argument("--json", action="store_true", help="Salida JSON en vez de texto.")
    ap.add_argument("--muestras", type=int, default=8, help="Cuántos ejemplos mostrar por hallazgo.")
    args = ap.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT o.id, o.cargo, o.descripcion, o.requisitos, o.renta_texto,
               o.renta_bruta_min, o.renta_bruta_max, o.fecha_publicacion, o.fecha_cierre,
               o.url_oferta, o.url_original, o.url_oferta_valida,
               o.institucion_id, o.institucion_nombre, o.region, o.ciudad,
               COALESCE(i.sector, o.sector) AS sector_efectivo, i.nombre AS inst_nombre
        FROM ofertas o
        LEFT JOIN instituciones i ON i.id = o.institucion_id
        WHERE o.activa = TRUE
        """
    )
    filas = cur.fetchall()
    hoy = date.today()
    total = len(filas)

    hall: dict[str, dict] = {}

    def add(clave, severidad, descripcion, rows):
        hall[clave] = {
            "severidad": severidad,
            "descripcion": descripcion,
            "cantidad": len(rows),
            "ejemplos": [
                {"id": r["id"], "cargo": (r["cargo"] or "")[:60],
                 "institucion": (r["inst_nombre"] or r["institucion_nombre"] or "")[:40]}
                for r in rows[: args.muestras]
            ],
        }

    # --- Chequeos ---
    add("sin_institucion", "ALTA",
        "Ofertas activas sin institución asignada (no se muestran con organismo).",
        [r for r in filas if r["institucion_id"] is None])

    add("sin_sector", "MEDIA",
        "Sin sector efectivo (no aparecen al filtrar por sector).",
        [r for r in filas if not (r["sector_efectivo"] or "").strip()])

    add("cierre_vencido_activo", "ALTA",
        "fecha_cierre ya pasada pero la oferta sigue activa.",
        [r for r in filas if _coerce_date(r["fecha_cierre"]) and r["fecha_cierre"] < hoy])

    add("cierre_inverosimil", "MEDIA",
        f"fecha_cierre a más de {DIAS_CIERRE_FUTURO_MAX} días (probable año mal parseado).",
        [r for r in filas if _coerce_date(r["fecha_cierre"])
         and (r["fecha_cierre"] - hoy).days > DIAS_CIERRE_FUTURO_MAX])

    add("url_rota", "ALTA",
        "url_oferta_valida = false (el enlace mostrado está roto).",
        [r for r in filas if r["url_oferta_valida"] is False])

    add("url_sin_chequear", "INFO",
        "url_oferta_valida = null (el validador de URLs aún no las revisó).",
        [r for r in filas if r["url_oferta_valida"] is None])

    add("campos_clave_vacios", "MEDIA",
        "Sin cargo, sin región o con descripción muy corta.",
        [r for r in filas if not (r["cargo"] or "").strip()
         or not (r["region"] or "").strip()
         or len(r["descripcion"] or "") < DESC_MIN])

    # Basura que el validador rechazaría hoy (consistente con la limpieza)
    basura = []
    motivos = Counter()
    for r in filas:
        o = dict(r)
        o.setdefault("url_oferta", o.get("url_original"))
        d = intake_validate_offer(o)
        if d.discard:
            basura.append(r)
            motivos[d.motivo_descarte or "?"] += 1
    add("basura_activa", "ALTA",
        "Ofertas activas que el validador descartaría (deberían limpiarse).", basura)
    hall["basura_activa"]["por_motivo"] = dict(motivos.most_common())

    # Buckets anómalos (sobre-recolección)
    por_inst = Counter((r["institucion_id"], r["inst_nombre"]) for r in filas if r["institucion_id"])
    buckets = [{"institucion_id": iid, "institucion": nom, "activas": n}
               for (iid, nom), n in por_inst.most_common() if n > BUCKET_ACTIVAS_ALERTA]
    hall["buckets_anomalos"] = {
        "severidad": "MEDIA",
        "descripcion": f"Instituciones con más de {BUCKET_ACTIVAS_ALERTA} ofertas activas (revisar sobre-recolección).",
        "cantidad": len(buckets), "ejemplos": buckets[: args.muestras],
    }

    # Duplicados por (cargo, institucion)
    dups = Counter((str(r["cargo"])[:60], r["institucion_id"]) for r in filas)
    grupos_dup = [(k, v) for k, v in dups.items() if v > 1]
    hall["duplicados"] = {
        "severidad": "INFO",
        "descripcion": "Grupos de ofertas con mismo cargo+institución (posibles duplicados o títulos genéricos).",
        "cantidad": sum(v for _, v in grupos_dup),
        "ejemplos": [{"cargo": cg, "institucion_id": iid, "repite": v}
                     for (cg, iid), v in sorted(grupos_dup, key=lambda x: -x[1])[: args.muestras]],
    }

    resultado = {"total_activas": total, "fecha": hoy.isoformat(), "hallazgos": hall}
    hay_alta = any(h.get("severidad") == "ALTA" and h.get("cantidad", 0) > 0 for h in hall.values())

    if args.json:
        print(json.dumps(resultado, ensure_ascii=False, indent=2))
    else:
        print(f"\n=== Calidad del catálogo — {hoy} — {total} ofertas activas ===\n")
        orden = {"ALTA": 0, "MEDIA": 1, "INFO": 2}
        for clave, h in sorted(hall.items(), key=lambda kv: (orden.get(kv[1]["severidad"], 9), -kv[1]["cantidad"])):
            n = h["cantidad"]
            marca = "🔴" if h["severidad"] == "ALTA" and n else ("🟡" if h["severidad"] == "MEDIA" and n else "⚪")
            print(f"{marca} [{h['severidad']:5}] {clave:22} {n:5}  — {h['descripcion']}")
            if "por_motivo" in h and h["por_motivo"]:
                print(f"        motivos: {h['por_motivo']}")
            for e in h["ejemplos"][:3]:
                if "cargo" in e and "id" in e:
                    print(f"          ej [{e['id']}] {e['cargo']}  ({e.get('institucion','')})")
                else:
                    print(f"          ej {e}")
        print(f"\n{'🔴 Hay hallazgos de severidad ALTA.' if hay_alta else '✅ Sin hallazgos de severidad ALTA.'}\n")

    cur.close()
    conn.close()
    return 1 if hay_alta else 0


if __name__ == "__main__":
    raise SystemExit(main())
