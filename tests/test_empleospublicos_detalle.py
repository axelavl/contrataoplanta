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


# ── PDF oficial de bases (/Repositoriof/PDFConcursos.../...pdf) ────────────────
# empleospublicos publica las bases como un PDF en su repositorio. El extractor
# debe preferir ese enlace directo por sobre el .aspx de la ficha (que el
# usuario reportó como "roto"), encontrándolo aunque venga en un onclick/script
# y no en un <a href>.

_FICHA_URL = "https://www.empleospublicos.cl/pub/convocatorias/avisopizarronficha.aspx?i=141142"
_PDF_BASES = (
    "https://www.empleospublicos.cl/Repositoriof/PDFConcursosResumidos/"
    "OtrosEmpleos/Concurso_OtrosEmpleos_141142_23062026_18111.pdf"
)

_FICHA_PDF_HREF = """
<html><body>
<h3>Convocatoria</h3><p>PROFESIONAL DE GESTION DE LA INNOVACION</p>
<a href="/Repositoriof/PDFConcursosResumidos/OtrosEmpleos/Concurso_OtrosEmpleos_141142_23062026_18111.pdf">Descargar bases</a>
<a href="/pub/privacidad.pdf">Política de privacidad</a>
</body></html>
"""

_FICHA_PDF_ONCLICK = """
<html><body>
<button type="button" onclick="window.open('https://www.empleospublicos.cl/Repositoriof/PDFConcursosResumidos/OtrosEmpleos/Concurso_OtrosEmpleos_141142_23062026_18111.pdf')">Ver bases</button>
</body></html>
"""

_FICHA_SIN_PDF_REPO = """
<html><body>
<a href="/pub/privacidad.pdf">Política de privacidad</a>
<a href="avisopizarronficha.aspx?i=141142#contenido">Ver aviso</a>
</body></html>
"""


def test_url_bases_prefiere_pdf_repositorio_en_href():
    sc = _scraper()
    soup = BeautifulSoup(_FICHA_PDF_HREF, "html.parser")
    assert sc._extraer_url_bases(soup, _FICHA_URL) == _PDF_BASES


def test_url_bases_detecta_pdf_repositorio_en_onclick():
    """El enlace al PDF puede venir en un onclick/script, no sólo en un href."""
    sc = _scraper()
    soup = BeautifulSoup(_FICHA_PDF_ONCLICK, "html.parser")
    assert sc._extraer_url_bases(soup, _FICHA_URL) == _PDF_BASES


def test_url_bases_sin_pdf_repositorio_no_inventa_enlace():
    """Sin PDF del repositorio y sin anchor de bases real, devuelve None
    (no cae al .pdf de privacidad ni al .aspx muerto)."""
    sc = _scraper()
    soup = BeautifulSoup(_FICHA_SIN_PDF_REPO, "html.parser")
    assert sc._extraer_url_bases(soup, _FICHA_URL) is None
