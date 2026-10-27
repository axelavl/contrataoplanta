"""Reclasificar ofertas.area_profesional con el clasificador común

Los clasificadores anteriores (`db.database.normalizar_area` y
`EmpleosPublicosScraper._inferir_area_profesional`) buscaban fragmentos
sueltos y caían en "Administración" por defecto: asistentes de párvulos
quedaban en "TI" (por "Base de Datos"), TENS en "Administración" y un
ingeniero en automatización en "Salud" (por "medic" dentro de "Medición").

El upsert de los scrapers hace `COALESCE(EXCLUDED.area_profesional,
ofertas.area_profesional)`, así que sin esta migración los valores malos
sobrevivirían a las próximas corridas.

Qué filas se tocan
------------------
Solo las que tienen NULL o una etiqueta que producían los clasificadores
viejos. Un valor fuera de ese vocabulario viene del sitio de origen y se
respeta. En Empleos Públicos el área suele venir del portal ("Área de
trabajo"), que es dato oficial: ahí solo se reclasifican NULL y las
etiquetas en minúscula que generaba el clasificador viejo de ese scraper.

Si el clasificador nuevo no puede determinar el área, queda NULL ("Sin
clasificar") en vez del "Administración" inventado.

Revision ID: 20261027_0002_reclasificar_area_profesional
Revises: 20261027_0001_cache_textos_pdf
Create Date: 2026-10-27
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "20261027_0002_reclasificar_area_profesional"
down_revision: Union[str, None] = "20261027_0001_cache_textos_pdf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Etiquetas de `db.database.normalizar_area` (versión anterior).
ETIQUETAS_LEGADO = (
    "Derecho", "Salud", "Ingeniería", "Ciencias Sociales", "Psicología",
    "Finanzas", "Economía", "TI", "Arquitectura/Diseño", "Educación",
    "Comunicaciones", "Administración", "Agropecuario/Forestal",
    "Medioambiente", "Fiscalización",
)
#: Etiquetas de `EmpleosPublicosScraper._inferir_area_profesional` (anterior).
ETIQUETAS_LEGADO_EP = (
    "salud", "derecho", "ingenieria", "educacion", "administracion",
    "finanzas", "ciencias sociales",
)

LOTE = 500


def upgrade() -> None:
    from classification.area_profesional import clasificar_area

    conn = op.get_bind()
    filas = conn.execute(text("""
        SELECT o.id, o.cargo,
               COALESCE(NULLIF(o.requisitos, ''), o.requisitos_texto, o.descripcion) AS requisitos,
               o.area_profesional
        FROM ofertas o
        LEFT JOIN fuentes f ON f.id = o.fuente_id
        WHERE o.area_profesional IS NULL
           OR o.area_profesional = ANY(:legado_ep)
           OR (o.area_profesional = ANY(:legado)
               AND COALESCE(f.tipo_plataforma, '') <> 'empleospublicos')
    """), {"legado": list(ETIQUETAS_LEGADO), "legado_ep": list(ETIQUETAS_LEGADO_EP)}).fetchall()

    cambios = []
    for id_, cargo, requisitos, actual in filas:
        nueva = clasificar_area(cargo, requisitos)
        if nueva != actual:
            cambios.append({"id": id_, "area": nueva})

    for i in range(0, len(cambios), LOTE):
        conn.execute(
            text("UPDATE ofertas SET area_profesional = :area WHERE id = :id"),
            cambios[i:i + LOTE],
        )
    print(f"[area_profesional] revisadas {len(filas)}, reclasificadas {len(cambios)}")


def downgrade() -> None:
    # Irreversible: los valores anteriores eran erróneos y no se guardan.
    pass
