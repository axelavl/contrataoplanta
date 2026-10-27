"""Tabla cache_textos_pdf: texto extraído de PDFs de bases/perfiles

La usan `scrapers/cache_pdf.py` y los scrapers que salen por el proxy
residencial (Carabineros, PDI, Armada) para no volver a descargar, en cada
corrida, PDFs que ya leyeron. Guarda solo el texto extraído (pocos KB por
PDF), no el archivo. Las entradas vencen a los 60 días y se purgan solas.

`modo` distingue la extracción por capa de texto ("texto") de la hecha con
OCR ("ocr"): un PDF escaneado leído sin OCR queda como "" en modo texto, y eso
no debe impedir intentarlo luego con OCR.

Revision ID: 20261027_0001_cache_textos_pdf
Revises: 20261026_0001_snapshot_solo_cambios
Create Date: 2026-10-27
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "20261027_0001_cache_textos_pdf"
down_revision: Union[str, None] = "20261026_0001_snapshot_solo_cambios"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS cache_textos_pdf (
            url_hash   CHAR(64)     NOT NULL,
            modo       VARCHAR(10)  NOT NULL DEFAULT 'texto',
            url        TEXT         NOT NULL,
            texto      TEXT         NOT NULL DEFAULT '',
            bytes_pdf  INTEGER,
            creado_en  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            PRIMARY KEY (url_hash, modo)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_cache_textos_pdf_creado "
        "ON cache_textos_pdf (creado_en)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cache_textos_pdf")
