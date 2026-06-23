"""Tabla `cursos_categorias` para gestionar las categorías de cursos desde el admin

Mueve las categorías del directorio (antes fijas en `web/cursos-data.js`) a una
tabla, para poder agregar / renombrar / eliminar desde el panel. El frontend lee
`GET /api/cursos/categorias` y cae al archivo estático si la API no responde.

Seed: las categorías actuales (incluida "Otros").

Revision ID: 20260622_0005_cursos_cat
Revises: 20260622_0004_cursos_tipo
Create Date: 2026-06-22
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "20260622_0005_cursos_cat"
down_revision: Union[str, None] = "20260622_0004_cursos_tipo"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS cursos_categorias (
            id         SERIAL PRIMARY KEY,
            slug       VARCHAR(40) UNIQUE,
            etiqueta   VARCHAR(120) NOT NULL,
            orden      INTEGER DEFAULT 100,
            activo     BOOLEAN DEFAULT TRUE,
            creado_en  TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    cats = [
        ("admin-publica", "Administración y gestión pública", 10),
        ("finanzas", "Finanzas y presupuesto público (SIGFE)", 20),
        ("compras", "Compras públicas (ChileCompra · Ley 19.886)", 30),
        ("derecho", "Derecho administrativo y probidad", 40),
        ("rrhh", "Recursos humanos del Estado", 50),
        ("salud", "Salud pública", 60),
        ("educacion", "Educación y párvulos", 70),
        ("ti", "TI y transformación digital del Estado", 80),
        ("prevencion", "Prevención de riesgos", 90),
        ("atencion", "Atención ciudadana", 100),
        ("otros", "Otros", 999),
    ]
    bind = op.get_bind()
    ins = text(
        "INSERT INTO cursos_categorias (slug, etiqueta, orden, activo) "
        "VALUES (:slug, :et, :ord, TRUE) ON CONFLICT (slug) DO NOTHING"
    )
    for slug, et, ordn in cats:
        bind.execute(ins, {"slug": slug, "et": et, "ord": ordn})


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cursos_categorias")
