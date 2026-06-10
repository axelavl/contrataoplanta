"""Extracción del detalle de empleospublicos cuando el portal renderiza inline
por encabezados (sin el contenedor viejo #lblAvisoTrabajoDatos del iframe).

Regresión de los issues 1 (institución vacía) y 4 (jornada/grado vacíos):
la ficha real (verificada 06/2026) trae los datos bajo encabezados
"Institución", "Renta Bruta", "Condiciones", etc.; el extractor debe leerlos
por etiqueta como fallback.
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from scrapers.base import parse_renta
from scrapers.empleos_publicos import EmpleosPublicosScraper

_FICHA_INLINE = """
<html><body>
<h3>Institución</h3><p>Ministerio de Seguridad Pública / Gendarmería de Chile /</p>
<h3>Convocatoria</h3><p>TECNICO GRADO 13° E.U.S., GESTOR ASEGURAMIENTO DE CALIDAD</p>
<h3>Área de Trabajo</h3><p>Area Soporte/apoyo a la Gestión</p>
<h3>Región</h3><p>Región Metropolitana de Santiago</p>
<h3>Ciudad</h3><p>Santiago</p>
<h3>Tipo de Vacante</h3><p>Contrata</p>
<h3>Renta Bruta</h3><p>1.269.217</p>
<h3>Condiciones</h3><p>La renta indicada corresponde al bruto, asimilada al
estamento Técnico Grado 13° E.U.S. Jornada Laboral 44 horas (PRESENCIAL).</p>
<h3>Objetivo del cargo</h3><p>Apoyar la ejecución...</p>
</body></html>
"""


def _scraper() -> EmpleosPublicosScraper:
    return EmpleosPublicosScraper(instituciones=[], dry_run=True)


def test_detalle_inline_extrae_institucion_jornada_y_grado():
    sc = _scraper()
    soup = BeautifulSoup(_FICHA_INLINE, "html.parser")
    meta = sc._extraer_metadata_detalle(soup)

    assert meta["institucion_nombre"] == "Gendarmería de Chile"
    assert meta["region"] == "Metropolitana de Santiago"
    assert meta["ciudad"] == "Santiago"
    assert meta["tipo_contrato"] == "contrata"

    jornada = meta.get("jornada") or sc._extraer_jornada(meta.get("condiciones"))
    assert jornada == "44 horas"

    _, _, grado = parse_renta(meta.get("condiciones"))
    assert grado == "EUS-13"
