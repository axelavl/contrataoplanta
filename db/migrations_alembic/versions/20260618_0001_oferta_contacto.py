"""Campos de contacto y datos estructurados de empleospublicos en `ofertas`

Agrega columnas para:

1. Correos de contacto reconocidos en la oferta:
   - `email_postulacion`: correo al que se envían antecedentes / se postula.
   - `email_consultas`: correo para consultas o contacto.
   Hoy el frontend reconoce correos en el cliente (extraerCorreoOferta sobre
   descripción/requisitos). Estas columnas permiten capturarlos en el scraper
   —incluido el dato estructurado de empleospublicos.cl que no siempre va en
   la descripción— y filtrar por "postular por correo" desde la API.

2. Datos estructurados que empleospublicos.cl expone y antes se perdían:
   - `numero_vacantes`: N° de vacantes del concurso.
   - `calidad_juridica`: planta / contrata / honorarios / código del trabajo.
   - `estamento`: directivo / profesional / técnico / administrativo / auxiliar.
   - `lugar_desempenio`: lugar de desempeño (más específico que región/ciudad).

Todas las columnas son nullable y aditivas: aplicar esta migración sobre una
DB en producción no afecta filas ni código existente. Idempotente vía
`ADD COLUMN IF NOT EXISTS`.

Revision ID: 20260618_0001_oferta_contacto
Revises: 20260612_0004_overrides_email
Create Date: 2026-06-18

Nota: el ID de revisión se mantiene corto (<=32 chars) porque
`alembic_version.version_num` es VARCHAR(32).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20260618_0001_oferta_contacto"
down_revision: Union[str, None] = "20260612_0004_overrides_email"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_COLUMNAS = (
    "email_postulacion VARCHAR(200)",
    "email_consultas   VARCHAR(200)",
    "numero_vacantes   INTEGER",
    "calidad_juridica  VARCHAR(60)",
    "estamento         VARCHAR(60)",
    "lugar_desempenio  VARCHAR(200)",
)


def upgrade() -> None:
    for col in _COLUMNAS:
        op.execute(f"ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS {col}")
    # Índice parcial para el filtro "solo con correo de postulación".
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ofertas_email_postulacion "
        "ON ofertas (email_postulacion) WHERE email_postulacion IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_ofertas_email_postulacion")
    for col in ("email_postulacion", "email_consultas", "numero_vacantes",
                "calidad_juridica", "estamento", "lugar_desempenio"):
        op.execute(f"ALTER TABLE ofertas DROP COLUMN IF EXISTS {col}")
