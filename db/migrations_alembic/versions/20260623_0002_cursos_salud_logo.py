"""Columna `logo` en cursos + seed de cursos de Salud pública

Dos cosas en una sola migración para no multiplicar los `alembic upgrade`:

1. `ALTER TABLE cursos ADD COLUMN logo TEXT` — permite fijar un logo
   institucional en alta resolución por curso desde el panel admin. Si queda
   NULL, el frontend usa el ícono del dominio (servicio de DuckDuckGo) como
   respaldo automático, así que la columna es opcional.

2. Seed de cursos GRATUITOS del área Salud pública (categoria 'salud'), que
   antes no tenía ninguno: SIAD del MINSAL, e-learning del ISP y las
   capacitaciones de Salud Digital. Idempotente vía ON CONFLICT (curso_id).

Revision ID: 20260623_0002_cursos_salud_logo
Revises: 20260623_0001_auth_shared_state
Create Date: 2026-06-23
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "20260623_0002_cursos_salud_logo"
down_revision: Union[str, None] = "20260623_0001_auth_shared_state"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Columna para logo institucional en alta (opcional, override del favicon).
    op.execute("ALTER TABLE cursos ADD COLUMN IF NOT EXISTS logo TEXT")

    # 2) Cursos del área Salud pública (gratuitos, oficiales).
    cursos = [
        # (curso_id, titulo, proveedor, categoria, modalidad, duracion, url, descripcion, orden)
        (
            "minsal-siad",
            "Cursos a distancia del Ministerio de Salud (SIAD)",
            "MINSAL · Subsecretaría de Salud Pública",
            "salud",
            "Online · Cuenta institucional",
            "Varía por curso",
            "https://siadsps.minsal.cl/course/",
            "La plataforma de formación a distancia del MINSAL para los equipos de salud: esterilización y desinfección, salud mental en emergencias y desastres, interculturalidad con población migrante y más. Acceso con tu cuenta institucional.",
            170,
        ),
        (
            "isp-elearning",
            "Cursos e-learning del Instituto de Salud Pública (ISP)",
            "Instituto de Salud Pública · ISP",
            "salud",
            "Online",
            "Varía por curso",
            "https://www.ispch.gob.cl/productos-y-servicios/cursos-e-learning/",
            "Catálogo de cursos en línea de la autoridad sanitaria, en temas de laboratorio, salud ambiental y ocupacional, vigilancia y uso seguro de productos. Formación técnica para quienes trabajan en salud pública.",
            180,
        ),
        (
            "minsal-salud-digital",
            "Capacitaciones en Salud Digital para funcionarios",
            "MINSAL · Departamento de Salud Digital",
            "salud",
            "Online",
            "Todo el año",
            "https://portalsaluddigital.minsal.cl/difusion-y-documentos-de-interes/capacitaciones-funcionarios/",
            "Capacitaciones para funcionarios del sector sobre las herramientas y sistemas de salud digital del Estado: registros clínicos, interoperabilidad y buenas prácticas en el uso de datos de salud.",
            190,
        ),
    ]
    bind = op.get_bind()
    insert_sql = text(
        """
        INSERT INTO cursos
          (curso_id, titulo, proveedor, categoria, modalidad, duracion,
           nivel, url, descripcion, gratuito, demo, orden, activo)
        VALUES
          (:cid, :tit, :prov, :cat, :mod, :dur,
           'estandar', :url, :desc, TRUE, FALSE, :ord, TRUE)
        ON CONFLICT (curso_id) DO NOTHING
        """
    )
    for c in cursos:
        bind.execute(insert_sql, {
            "cid": c[0], "tit": c[1], "prov": c[2], "cat": c[3], "mod": c[4],
            "dur": c[5], "url": c[6], "desc": c[7], "ord": c[8],
        })


def downgrade() -> None:
    op.execute(
        "DELETE FROM cursos WHERE curso_id IN "
        "('minsal-siad', 'isp-elearning', 'minsal-salud-digital')"
    )
    op.execute("ALTER TABLE cursos DROP COLUMN IF EXISTS logo")
