"""Cursos para TI, Educación, Servicios Generales y Ciencias/Medio ambiente

Tras la reclasificación de `ofertas.area_profesional` (20261027_0002)
aparecieron áreas sin cursos asociados en /cursos.html:

* TI: solo tenía 1 curso.
* Educación y párvulos: 0 cursos, siendo la segunda área con más ofertas.
* Servicios Generales (auxiliares, conductores, guardias): área nueva; se
  asocia a la categoría existente "Prevención de riesgos", que tenía 0.
* Ciencias / Medioambiente / Agropecuario: categoría nueva "ambiente".

Fuentes verificadas el 27-oct-2026 en los sitios oficiales (catálogo del
Aula Virtual CEA, isl.gob.cl, academia.sea.gob.cl, academiadeformacion.mma
.gob.cl y ChileAtiende). La plataforma del CPEIP no respondía desde el
entorno de verificación; su URL oficial está confirmada por indexación y
por la documentación del propio CPEIP.

La Academia del MMA no declara gratuidad en su sitio: se carga con
gratuito = FALSE para no mostrar la etiqueta "Gratis" sin respaldo.

Idempotente (ON CONFLICT DO NOTHING): no pisa ediciones hechas en el panel.

Revision ID: 20261027_0003_cursos_ti_educacion_ambiente
Revises: 20261027_0002_reclasificar_area_profesional
Create Date: 2026-10-27
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "20261027_0003_cursos_ti_educacion_ambiente"
down_revision: Union[str, None] = "20261027_0002_reclasificar_area_profesional"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

CEA = "AcademiaCEA · Contraloría (CGR)"

# (curso_id, titulo, proveedor, categoria, modalidad, duracion, url, descripcion, gratuito, orden)
CURSOS = [
    ("cea-sql", "Introducción a SQL", CEA, "ti", "Online · Clave Única", "Curso introductorio",
     "https://www.ceacgr.cl/aulavirtual/enrol/index.php?id=705",
     "Curso de la Contraloría para aprender a consultar, organizar y trabajar con información "
     "guardada en bases de datos usando SQL. Dirigido a personas funcionarias del sector público.",
     True, 200),
    ("cea-python", "Introducción a Python", CEA, "ti", "Online · Clave Única", "Curso introductorio",
     "https://www.ceacgr.cl/aulavirtual/enrol/index.php?id=706",
     "Bases de programación en Python y su uso para procesar y analizar información. Dirigido a "
     "personas funcionarias del sector público.",
     True, 210),
    ("cea-phishing", "Ciberseguridad: Phishing y Fraude Digital", CEA, "ti", "Online · Clave Única", "3 h",
     "https://www.ceacgr.cl/aulavirtual/enrol/index.php?id=716",
     "Continuación de «Introducción a la Ciberseguridad»: identificar riesgos, prevenir ataques y "
     "reconocer fraudes digitales. Pide conocimientos básicos o haber aprobado el curso introductorio.",
     True, 220),
    ("cpeip-desarrollo-docente", "Cursos gratuitos para educadoras, docentes y asistentes de la educación",
     "CPEIP · Ministerio de Educación", "educacion", "Online", "Varía por curso",
     "https://desarrollodocenteenlinea.cpeip.cl/",
     "Plataforma del Ministerio de Educación para postular a cursos gratuitos, principalmente a "
     "distancia. Para educadoras de párvulos, docentes, directivos y asistentes de la educación de "
     "establecimientos con financiamiento del Estado.",
     True, 230),
    ("isl-campus-prevencion", "Campus Prevención: cursos de seguridad laboral",
     "Instituto de Seguridad Laboral · ISL", "prevencion", "Online · con tu RUT", "Varía por curso",
     "https://campusprevencionisl.cl/",
     "Más de 50 cursos en línea y gratuitos del organismo público de seguridad laboral: uso de "
     "extintores, prevención de riesgos generales y específicos, gestión de desastres y normativa "
     "laboral. Al terminar descargas un diploma de participación.",
     True, 240),
    ("mma-academia-adriana-hoffmann", "Academia de Formación Ambiental Adriana Hoffmann",
     "Ministerio del Medio Ambiente", "ambiente", "Online (e-learning)", "Según calendario",
     "https://academiadeformacion.mma.gob.cl/",
     "Cursos en línea del Ministerio del Medio Ambiente sobre cambio climático, economía circular, "
     "calidad del aire, biodiversidad y humedales. Dirigidos a funcionarios públicos y municipales, "
     "con cupos por calendario. Revisa las condiciones de inscripción en el sitio.",
     False, 250),
    ("sea-academia", "Academia SEA: evaluación ambiental de proyectos",
     "Servicio de Evaluación Ambiental · SEA", "ambiente", "Online · Clave Única", "Según calendario",
     "https://academia.sea.gob.cl/",
     "Cursos gratuitos sobre el Sistema de Evaluación de Impacto Ambiental (SEIA): participación "
     "ciudadana, áreas de influencia y descripción de proyectos. Abiertos a funcionarios, "
     "consultores y ciudadanía.",
     True, 260),
]


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(text(
        "INSERT INTO cursos_categorias (slug, etiqueta, orden, activo) "
        "VALUES ('ambiente', 'Medio ambiente y ciencias', 95, TRUE) "
        "ON CONFLICT (slug) DO NOTHING"
    ))
    ins = text("""
        INSERT INTO cursos
          (curso_id, titulo, proveedor, categoria, modalidad, duracion,
           nivel, url, descripcion, gratuito, demo, orden, activo)
        VALUES
          (:cid, :tit, :prov, :cat, :mod, :dur,
           'estandar', :url, :desc, :grat, FALSE, :ord, TRUE)
        ON CONFLICT (curso_id) DO NOTHING
    """)
    for c in CURSOS:
        bind.execute(ins, {"cid": c[0], "tit": c[1], "prov": c[2], "cat": c[3], "mod": c[4],
                           "dur": c[5], "url": c[6], "desc": c[7], "grat": c[8], "ord": c[9]})


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("DELETE FROM cursos WHERE curso_id = ANY(:ids)"),
                 {"ids": [c[0] for c in CURSOS]})
    bind.execute(text("DELETE FROM cursos_categorias WHERE slug = 'ambiente'"))
