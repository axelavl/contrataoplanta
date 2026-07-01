"""Índices compuestos para queries frecuentes de ofertas

Los filtros más comunes (activa + region, activa + sector, activa + institución)
y el orden por fecha_cierre usan índices individuales que PostgreSQL debe
combinar con bitmap scans. Índices compuestos parciales (WHERE activa = TRUE)
cubren estos patrones directamente y eliminan el merge.

También agrega índice en `nivel` y trigram en `cargo`/`institucion` para
búsquedas ILIKE.

Revision ID: 20260701_0001_composite_indexes_performance
Revises: 20260627_0001_oferta_experiencia_anos
Create Date: 2026-07-01
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260701_0001_composite_indexes_performance"
down_revision: Union[str, None] = "20260627_0001_oferta_experiencia_anos"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_ofertas_activa_cierre
            ON ofertas(fecha_cierre ASC NULLS LAST, id DESC)
            WHERE activa = TRUE;

        CREATE INDEX IF NOT EXISTS idx_ofertas_activa_region_cierre
            ON ofertas(region, fecha_cierre ASC NULLS LAST)
            WHERE activa = TRUE;

        CREATE INDEX IF NOT EXISTS idx_ofertas_activa_sector_cierre
            ON ofertas(sector, fecha_cierre ASC NULLS LAST)
            WHERE activa = TRUE;

        CREATE INDEX IF NOT EXISTS idx_ofertas_activa_inst_cierre
            ON ofertas(institucion_id, fecha_cierre ASC NULLS LAST)
            WHERE activa = TRUE;

        CREATE INDEX IF NOT EXISTS idx_ofertas_nivel
            ON ofertas(nivel);
    """)

    op.execute("""
        CREATE EXTENSION IF NOT EXISTS pg_trgm;

        CREATE INDEX IF NOT EXISTS idx_ofertas_cargo_trgm
            ON ofertas USING gin (cargo gin_trgm_ops);

        CREATE INDEX IF NOT EXISTS idx_ofertas_institucion_trgm
            ON ofertas USING gin (institucion_nombre gin_trgm_ops);
    """)


def downgrade() -> None:
    op.execute("""
        DROP INDEX IF EXISTS idx_ofertas_activa_cierre;
        DROP INDEX IF EXISTS idx_ofertas_activa_region_cierre;
        DROP INDEX IF EXISTS idx_ofertas_activa_sector_cierre;
        DROP INDEX IF EXISTS idx_ofertas_activa_inst_cierre;
        DROP INDEX IF EXISTS idx_ofertas_nivel;
        DROP INDEX IF EXISTS idx_ofertas_cargo_trgm;
        DROP INDEX IF EXISTS idx_ofertas_institucion_trgm;
    """)
