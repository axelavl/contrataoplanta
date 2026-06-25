"""Incluir `renta_tipo` en el histórico de snapshots

Acompaña a 20260624_0001_renta_tipo. La tabla `ofertas_snapshots` y el trigger
`snapshot_oferta()` deben capturar también el tipo de renta (bruta/líquida) para
no perder esa señal en el histórico.

- ``ofertas_snapshots.renta_tipo VARCHAR(20)`` (aditiva, nullable).
- ``snapshot_oferta()`` recreado para copiar ``NEW.renta_tipo``.

Todo es idempotente y tolerante a despliegues donde el feature de snapshots aún
no exista (``ALTER TABLE IF EXISTS`` / ``CREATE OR REPLACE FUNCTION``).

Revision ID: 20260624_0002_snapshot_renta_tipo
Revises: 20260624_0001_renta_tipo
Create Date: 2026-06-24
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260624_0002_snapshot_renta_tipo"
down_revision: Union[str, None] = "20260624_0001_renta_tipo"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_FN_CON_TIPO = """
CREATE OR REPLACE FUNCTION snapshot_oferta() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ofertas_snapshots (
        oferta_id, codigo, capturado_en, origen,
        fuente_id, cargo, descripcion, institucion_nombre, sector,
        area_profesional, tipo_cargo, nivel, region, ciudad,
        renta_bruta_min, renta_bruta_max, renta_texto, renta_tipo,
        fecha_publicacion, fecha_cierre, activa
    ) VALUES (
        NEW.id, NEW.codigo, NOW(), TG_OP,
        NEW.fuente_id, NEW.cargo, NEW.descripcion, NEW.institucion_nombre, NEW.sector,
        NEW.area_profesional, NEW.tipo_cargo, NEW.nivel, NEW.region, NEW.ciudad,
        NEW.renta_bruta_min, NEW.renta_bruta_max, NEW.renta_texto, NEW.renta_tipo,
        NEW.fecha_publicacion, NEW.fecha_cierre, NEW.activa
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_FN_SIN_TIPO = """
CREATE OR REPLACE FUNCTION snapshot_oferta() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ofertas_snapshots (
        oferta_id, codigo, capturado_en, origen,
        fuente_id, cargo, descripcion, institucion_nombre, sector,
        area_profesional, tipo_cargo, nivel, region, ciudad,
        renta_bruta_min, renta_bruta_max, renta_texto,
        fecha_publicacion, fecha_cierre, activa
    ) VALUES (
        NEW.id, NEW.codigo, NOW(), TG_OP,
        NEW.fuente_id, NEW.cargo, NEW.descripcion, NEW.institucion_nombre, NEW.sector,
        NEW.area_profesional, NEW.tipo_cargo, NEW.nivel, NEW.region, NEW.ciudad,
        NEW.renta_bruta_min, NEW.renta_bruta_max, NEW.renta_texto,
        NEW.fecha_publicacion, NEW.fecha_cierre, NEW.activa
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""


def _funcion_existe() -> bool:
    """True si la función snapshot_oferta() está definida en la BD."""
    bind = op.get_bind()
    row = bind.exec_driver_sql(
        "SELECT 1 FROM pg_proc WHERE proname = 'snapshot_oferta' LIMIT 1"
    ).fetchone()
    return row is not None


def upgrade() -> None:
    # Columna nueva, aditiva. Tolerante a despliegues sin el feature de snapshots.
    op.execute(
        "ALTER TABLE IF EXISTS ofertas_snapshots "
        "ADD COLUMN IF NOT EXISTS renta_tipo VARCHAR(20)"
    )
    # Solo recreamos la función si ya existía (no introducimos el feature aquí).
    if _funcion_existe():
        op.execute(_FN_CON_TIPO)


def downgrade() -> None:
    if _funcion_existe():
        op.execute(_FN_SIN_TIPO)
    op.execute("ALTER TABLE IF EXISTS ofertas_snapshots DROP COLUMN IF EXISTS renta_tipo")
