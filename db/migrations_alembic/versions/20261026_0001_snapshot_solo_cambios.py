"""Snapshots de ofertas solo cuando la oferta cambia de verdad

El trigger `tr_snapshot_oferta` corría AFTER INSERT OR UPDATE sin condición.
Cada corrida de scrapers hace UPDATE de todas las ofertas vistas (al menos
`ultima_vista_en = NOW()`), así que cada corrida copiaba la tabla `ofertas`
completa —descripción incluida— a `ofertas_snapshots`. Con corridas
frecuentes eso llenó el volumen de 500 MB en Railway y dejó Postgres en
recovery mode (sept-oct 2026).

Ahora:
- INSERT: se guarda el snapshot inicial (igual que antes).
- UPDATE: solo si cambia alguno de los campos que el snapshot registra. Tocar
  `ultima_vista_en`, hashes o flags de validación ya no genera filas.

La función `snapshot_oferta()` no cambia (ya incluye renta_tipo desde
20260624_0002). Solo se reemplaza el trigger por dos con condición.

Revision ID: 20261026_0001_snapshot_solo_cambios
Revises: 20260710_0003_analitica_geo
Create Date: 2026-10-26
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20261026_0001_snapshot_solo_cambios"
down_revision: Union[str, None] = "20260710_0003_analitica_geo"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Campos que el snapshot copia. Si ninguno cambió, no hay nada que registrar.
_CAMPOS = (
    "codigo", "fuente_id", "cargo", "descripcion", "institucion_nombre",
    "sector", "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
    "renta_bruta_min", "renta_bruta_max", "renta_texto", "renta_tipo",
    "fecha_publicacion", "fecha_cierre", "activa",
)

_CONDICION = " OR ".join(f"OLD.{c} IS DISTINCT FROM NEW.{c}" for c in _CAMPOS)


def upgrade() -> None:
    # Bases donde el schema legado nunca creó la función: no hay nada que ajustar.
    op.execute(f"""
    DO $$
    BEGIN
        IF to_regclass('public.ofertas') IS NULL
           OR NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'snapshot_oferta') THEN
            RETURN;
        END IF;

        DROP TRIGGER IF EXISTS tr_snapshot_oferta ON ofertas;
        DROP TRIGGER IF EXISTS tr_snapshot_oferta_insert ON ofertas;
        DROP TRIGGER IF EXISTS tr_snapshot_oferta_update ON ofertas;

        CREATE TRIGGER tr_snapshot_oferta_insert
            AFTER INSERT ON ofertas
            FOR EACH ROW
            EXECUTE FUNCTION snapshot_oferta();

        CREATE TRIGGER tr_snapshot_oferta_update
            AFTER UPDATE ON ofertas
            FOR EACH ROW
            WHEN ({_CONDICION})
            EXECUTE FUNCTION snapshot_oferta();
    END $$;
    """)


def downgrade() -> None:
    op.execute("""
    DO $$
    BEGIN
        IF to_regclass('public.ofertas') IS NULL
           OR NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'snapshot_oferta') THEN
            RETURN;
        END IF;

        DROP TRIGGER IF EXISTS tr_snapshot_oferta_insert ON ofertas;
        DROP TRIGGER IF EXISTS tr_snapshot_oferta_update ON ofertas;
        CREATE TRIGGER tr_snapshot_oferta
            AFTER INSERT OR UPDATE ON ofertas
            FOR EACH ROW
            EXECUTE FUNCTION snapshot_oferta();
    END $$;
    """)
