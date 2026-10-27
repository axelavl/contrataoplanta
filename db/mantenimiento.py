"""Retención de datos: borra filas viejas de tablas que crecen en cada corrida.

Por qué existe
--------------
En sep-2026 el volumen de Postgres en Railway (500 MB) se llenó y la base
entró en recovery mode: el sitio quedó caído. Varias tablas reciben filas
en CADA corrida de scrapers (2 al día) y nada las borraba:

* source_evaluations: una fila con JSON por cada una de las ~650 fuentes.
* offer_quality_events: una por cada oferta procesada.
* scraper_descartes: cada aviso descartado, repetido todos los días.

Este módulo aplica una retención por tabla. Lo llama `scrapers/run_all.py`
al final de cada corrida de producción y también se puede correr a mano:

    python -m db.mantenimiento            # aplica
    python -m db.mantenimiento --dry-run  # solo cuenta

Borra en lotes (por id) para no tomar locks largos. Cada tabla va aparte:
si una falla (no existe, permisos), las demás siguen.
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

logger = logging.getLogger("db.mantenimiento")

LOTE = 5_000


@dataclass(frozen=True)
class Retencion:
    tabla: str
    columna_fecha: str
    dias: int
    #: condición extra que deben cumplir las filas para borrarse (SQL, alias t)
    condicion: str = "TRUE"


RETENCIONES: tuple[Retencion, ...] = (
    # El orquestador lee la ÚLTIMA evaluación de cada fuente (cooldowns),
    # sin importar su antigüedad: esa nunca se borra.
    Retencion("source_evaluations", "evaluated_at", 30,
              "(t.institucion_id IS NULL OR EXISTS ("
              " SELECT 1 FROM source_evaluations n"
              " WHERE n.institucion_id = t.institucion_id"
              "   AND n.evaluated_at > t.evaluated_at))"),
    Retencion("offer_quality_events", "created_at", 30),
    Retencion("scraper_descartes", "fecha_descarte", 30),
    Retencion("catalog_integrity_events", "created_at", 90),
    Retencion("logs_scraping", "iniciado_en", 90),
    Retencion("scraper_runs", "COALESCE(t.started_at, t.ejecutado_en)", 180),
    Retencion("email_eventos", "ts", 180),
    Retencion("ofertas_snapshots", "capturado_en", 365),
    Retencion("ofertas_historico", "versionado_en", 365),
    Retencion("admin_audit_log", "ts", 365),
    Retencion("alertas_enviadas", "enviada_en", 365),
    # La analítica compara hasta 365 días contra el período anterior.
    Retencion("web_eventos", "ts", 730),
)


def _col(r: Retencion) -> str:
    return r.columna_fecha if "(" in r.columna_fecha else f"t.{r.columna_fecha}"


def purgar(conn, *, dry_run: bool = False) -> dict[str, int]:
    """Aplica las retenciones. Devuelve {tabla: filas borradas (o a borrar)}.

    `conn` es una conexión psycopg2; se hace commit por lote."""
    resultado: dict[str, int] = {}
    for r in RETENCIONES:
        where = (f"{_col(r)} < NOW() - make_interval(days => {int(r.dias)}) "
                 f"AND {r.condicion}")
        try:
            with conn.cursor() as cur:
                if dry_run:
                    cur.execute(f"SELECT COUNT(*) FROM {r.tabla} t WHERE {where}")
                    resultado[r.tabla] = int(cur.fetchone()[0])
                    conn.rollback()
                    continue
                total = 0
                while True:
                    cur.execute(
                        f"DELETE FROM {r.tabla} WHERE id IN ("
                        f" SELECT t.id FROM {r.tabla} t WHERE {where} LIMIT {LOTE})"
                    )
                    borradas = cur.rowcount or 0
                    conn.commit()
                    total += borradas
                    if borradas < LOTE:
                        break
                resultado[r.tabla] = total
        except Exception as exc:
            conn.rollback()
            logger.warning("mantenimiento: %s omitida (%s)", r.tabla,
                           str(exc).splitlines()[0][:160])
    if any(resultado.values()):
        logger.info("mantenimiento: %s %s", "a borrar" if dry_run else "borradas",
                    {k: v for k, v in resultado.items() if v})
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="Solo contar, no borrar")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    import psycopg2
    from db.config import DB_CONFIG
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        res = purgar(conn, dry_run=args.dry_run)
        print({k: v for k, v in res.items()})
    finally:
        conn.close()


if __name__ == "__main__":
    main()
