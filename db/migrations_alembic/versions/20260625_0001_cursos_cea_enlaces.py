"""Enlaces directos a los cursos de AcademiaCEA (Contraloría) en vez del catálogo

Hasta ahora los 8 cursos de AcademiaCEA apuntaban todos al catálogo genérico
`.../aulavirtual/course/index.php`, que en el aula virtual actual redirige a la
portada (no abre el curso). Esta migración fija, por `curso_id`, el enlace más
directo posible:

  - Cursos con instancia vigente identificable → página de inscripción directa
    `.../aulavirtual/enrol/index.php?id=N` (la misma que abre "Matricúlate aquí").
  - Cursos cuya instancia rota cada año y no es fijable con seguridad → página
    de la categoría específica `.../aulavirtual/course/index.php?categoryid=N`,
    que sí carga (no redirige a la portada) y aterriza en el tema correcto.

Los IDs de inscripción del aula virtual cambian cada año (sufijos _2026, etc.):
si un enlace `enrol/index.php?id=N` deja de resolver, basta actualizar el `id`
aquí y en `web/cursos-data.js`, o editar la URL desde el panel admin.

Solo toca la columna `url` (y `actualizada_en`) de esos `curso_id`; cualquier
curso editado luego desde el admin no se ve afectado salvo coincidencia de id.

Revision ID: 20260625_0001_cursos_cea_enlaces
Revises: 20260624_0002_snap_renta_tipo
Create Date: 2026-06-25
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "20260625_0001_cursos_cea_enlaces"
down_revision: Union[str, None] = "20260624_0002_snap_renta_tipo"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_BASE = "https://www.ceacgr.cl/aulavirtual/"

# (curso_id, url)
_ENLACES = [
    ("cea-induccion-estado", _BASE + "enrol/index.php?id=679"),
    ("cea-estatuto-administrativo", _BASE + "enrol/index.php?id=727"),
    ("cea-etica-transparencia-lobby", _BASE + "enrol/index.php?id=707"),
    ("cea-ley-karin", _BASE + "enrol/index.php?id=682"),
    ("cea-ciberseguridad", _BASE + "enrol/index.php?id=703"),
    # Sin instancia vigente fijable → categoría específica (no redirige a la portada).
    ("cea-derecho-administrativo", _BASE + "course/index.php?categoryid=202"),
    ("cea-administracion-financiera", _BASE + "course/index.php?categoryid=185"),
    ("cea-induccion-municipal", _BASE + "course/index.php?categoryid=179"),
]


def upgrade() -> None:
    bind = op.get_bind()
    upd = text(
        "UPDATE cursos SET url = :url, actualizada_en = NOW() WHERE curso_id = :cid"
    )
    for cid, url in _ENLACES:
        bind.execute(upd, {"cid": cid, "url": url})


def downgrade() -> None:
    # Revierte al catálogo genérico anterior (estado previo a esta migración).
    bind = op.get_bind()
    upd = text(
        "UPDATE cursos SET url = :url, actualizada_en = NOW() WHERE curso_id = :cid"
    )
    catalogo = _BASE + "course/index.php"
    for cid, _ in _ENLACES:
        bind.execute(upd, {"cid": cid, "url": catalogo})
