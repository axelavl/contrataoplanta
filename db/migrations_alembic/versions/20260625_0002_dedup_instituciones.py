"""Deduplica instituciones del catálogo y consolida sus sectores

El catálogo maestro tenía 7 instituciones duplicadas (misma entidad cargada dos
veces, a veces con sector distinto), lo que rompía el filtro por sector del
buscador: una oferta podía quedar enlazada a cualquiera de las dos filas y
mostrar un sector u otro. Se consolidan hacia la fila canónica:

    70  Consejo Nacional de Educación   -> 155 (Autónomo/Regulador)
    74  Gendarmería de Chile            -> 163 (FF.AA. y Orden)
    41  Caja de Previsión Defensa Nac.  -> 168 (CAPREDENA, FF.AA. y Orden)
    71  Agencia de Calidad              ->  68 (Agencia de Calidad de la Educación)
    377 Municipalidad de Limache        -> 360
    378 Municipalidad de Olmué          -> 362
    379 Municipalidad de Villa Alemana  -> 363

Por cada par se repunta `institucion_id` en todas las tablas que referencian
`instituciones(id)` antes de borrar la fila huérfana. Además se reclasifica
ENAMI (115) de "Ejecutivo Central" a "Empresa del Estado", por coherencia con
CODELCO/ENAP.

Esta revisión también FUSIONA las dos cabezas de Alembic que existían
(`alertas_uniq` y `cursos_cea_enlaces`), dejando un único head.

Revision ID: 20260625_0002_dedup_inst
Revises: 20260625_0001_alertas_uniq, 20260625_0001_cursos_cea_enlaces
Create Date: 2026-06-25
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260625_0002_dedup_inst"
down_revision: Union[str, Sequence[str], None] = (
    "20260625_0001_alertas_uniq",
    "20260625_0001_cursos_cea_enlaces",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (duplicado, canónico). Mismo mapa que aplicó la limpieza del catálogo JSON.
_DEDUP = [(70, 155), (74, 163), (41, 168), (71, 68), (377, 360), (378, 362), (379, 363)]
_DUP_IDS = tuple(d for d, _ in _DEDUP)

# Tablas con FK a instituciones(id). Si se agrega una nueva, sumarla aquí
# para que el repunte sea exhaustivo y el DELETE no falle por FK.
_FK_TABLES = (
    "ofertas",
    "scraper_descartes",
    "source_evaluations",
    "offer_quality_events",
    "catalog_integrity_events",
)


def _values_clause() -> str:
    return ", ".join(f"({dup}, {can})" for dup, can in _DEDUP)


def _solo_si_tiene_institucion_id(tabla: str, sql_interno: str) -> str:
    """Envuelve `sql_interno` para que solo corra si `tabla.institucion_id`
    existe en este despliegue.

    Reconciliación de drift de schema: algunas tablas (`scraper_descartes` y las
    de auditoría) se crearon FUERA de Alembic —vía el viejo `ensure_api_schema()`
    o `db/schema.sql`— antes de que el schema añadiera `institucion_id`. En esos
    despliegues (p.ej. Railway) la columna no existe y el UPDATE crudo rompía la
    migración con `column institucion_id does not exist`, dejando la cadena de
    Alembic atascada. Saltar la tabla no pierde nada: sin esa columna no hay
    referencias a repuntar (y el scraper no la usa al registrar descartes).
    El PL/pgSQL solo planea el cuerpo del IF cuando se ejecuta, así que también
    tolera que la tabla no exista del todo.
    """
    return f"""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = '{tabla}' AND column_name = 'institucion_id'
        ) THEN
            {sql_interno}
        END IF;
    END $$;
    """


def upgrade() -> None:
    values = _values_clause()
    ids_csv = ", ".join(str(i) for i in _DUP_IDS)

    # 1. Repuntar institucion_id de cada duplicado hacia su canónico (solo en las
    #    tablas que tengan la columna en este despliegue — ver helper).
    for tabla in _FK_TABLES:
        op.execute(_solo_si_tiene_institucion_id(tabla, f"""
            UPDATE {tabla} t
            SET institucion_id = m.canonico
            FROM (VALUES {values}) AS m(dup, canonico)
            WHERE t.institucion_id = m.dup;
        """))

    # 2. Limpiar overrides apuntando a IDs que dejarán de existir (PK, sin FK).
    op.execute(_solo_si_tiene_institucion_id(
        "source_overrides",
        f"DELETE FROM source_overrides WHERE institucion_id IN ({ids_csv});",
    ))

    # 3. Borrar las filas huérfanas del catálogo en la DB.
    op.execute(f"DELETE FROM instituciones WHERE id IN ({ids_csv})")

    # 4. Reclasificar ENAMI -> Empresa del Estado (coherente con CODELCO/ENAP).
    op.execute(
        "UPDATE instituciones SET sector = 'Empresa del Estado' WHERE id = 115"
    )


def downgrade() -> None:
    # Las filas duplicadas y los enlaces originales no se pueden reconstruir de
    # forma fiable. La reclasificación de ENAMI sí se revierte.
    op.execute(
        "UPDATE instituciones SET sector = 'Ejecutivo Central' WHERE id = 115"
    )
