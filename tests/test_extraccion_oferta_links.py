"""Tests del importador de URL con los casos reales que motivaron la mejora:

1. Post WordPress municipal (muniloncoche.cl, mlonquimay.cl): el cuerpo casi
   no trae datos; cargo/cierre/renta/requisitos viven en el PDF de las bases
   enlazado. El extractor debe SEGUIR ese PDF y fusionar los campos.
2. PDF del Portal de Transparencia: la URL no termina en .pdf
   ('.../BASES+CONCURSO+-+DAF.pdf_<ts>/<uuid>') — debe reconocerse como link
   a bases dentro de una página y como url_bases al ingresarla directo.

Sin red: descarga y lectura de PDF monkeypatcheadas.
"""
from __future__ import annotations

import pytest

from api.services import extraccion_oferta as E

URL_TRANSPARENCIA = (
    "https://www.portaltransparencia.cl/PortalPdT/documents/10179/62801/"
    "BASES+CONCURSO+-+DAF.pdf_1783454876591/1271595b-deed-405f-be0c-072a0aad2717"
)

TEXTO_BASES_PDF = """
MUNICIPALIDAD DE LONCOCHE
BASES LLAMADO A CONCURSO PÚBLICO

Nombre del cargo: Director de Administración y Finanzas
Modalidad: Planta
N° de vacantes: 1
Renta bruta mensual: $2.450.000.-

REQUISITOS MÍNIMOS
Título profesional de Contador Auditor o Ingeniero Comercial.

Cierre de postulación: 28 de agosto de 2026
Postulaciones al correo postulaciones@muniloncoche.cl
"""

HTML_POST_MUNICIPAL = f"""
<html><head><title>Bases de llamado a Concurso Público para proveer cargos
vacantes en Planta — Municipalidad de Loncoche</title></head><body>
<h1>Bases de llamado a Concurso Público Municipalidad de Loncoche</h1>
<p>Se informa a la comunidad el llamado a concurso. Descarga las bases y sus
anexos en los siguientes enlaces oficiales del municipio.</p>
<a href="{URL_TRANSPARENCIA}">Descargar bases del concurso</a>
<a href="/anexos/formulario.docx">Formulario</a>
</body></html>
"""


# ── Detección de links PDF con rutas "raras" ─────────────────────────────────
def test_parece_url_pdf_sin_extension_final():
    assert E._parece_url_pdf(URL_TRANSPARENCIA)
    assert E._parece_url_pdf("https://x.cl/wp-content/uploads/BASES-2026.PDF")
    assert not E._parece_url_pdf("https://x.cl/noticia/concurso-publico/")


def test_extraer_texto_html_detecta_pdf_transparencia():
    pagina = E.extraer_texto_html(HTML_POST_MUNICIPAL, base_url="https://muniloncoche.cl/post/")
    assert URL_TRANSPARENCIA in pagina["pdf_links"]


def test_elegir_pdf_bases_prefiere_keyword_en_toda_la_ruta():
    links = [
        "https://x.cl/uploads/anexo-formulario.pdf",
        URL_TRANSPARENCIA,   # 'BASES+CONCURSO' va en el penúltimo segmento
    ]
    assert E._elegir_pdf_bases(links) == URL_TRANSPARENCIA
    # Sin keyword, cae al primero absoluto (mejor que nada).
    assert E._elegir_pdf_bases(["https://x.cl/d/otro.pdf"]) == "https://x.cl/d/otro.pdf"
    assert E._elegir_pdf_bases([]) is None


# ── Post municipal: seguir el PDF de bases y fusionar campos ─────────────────
def _mock_descarga_pdf(monkeypatch):
    monkeypatch.setattr(
        E, "descargar_recurso",
        lambda url: (b"%PDF-1.7 fake", "application/pdf", url))
    from scrapers import bases_pdf
    monkeypatch.setattr(
        bases_pdf, "leer_pdf",
        lambda contenido, allow_ocr=True, **kw: (TEXTO_BASES_PDF, "texto"))


def test_post_municipal_sigue_pdf_y_fusiona(monkeypatch):
    _mock_descarga_pdf(monkeypatch)
    res = E.extraer_desde_contenido(
        HTML_POST_MUNICIPAL.encode(), "text/html",
        url="https://muniloncoche.cl/bases-de-llamado-a-concurso-publico-2/")
    campos, meta = res["campos"], res["meta"]
    assert meta["metodo"] == "html+pdf"
    assert meta["pdf_seguido"] == URL_TRANSPARENCIA
    # El cargo real viene de las bases, no del título genérico del post.
    assert campos["cargo"] == "Director de Administración y Finanzas"
    assert campos["fecha_cierre"] == "2026-08-28"
    assert campos["renta_bruta_min"] == 2450000
    assert campos["email_postulacion"] == "postulaciones@muniloncoche.cl"
    assert campos["url_bases"] == URL_TRANSPARENCIA
    assert campos["url_oferta"].startswith("https://muniloncoche.cl/")
    # La vista previa incluye el texto de las bases.
    assert "Bases (PDF adjunto)" in res["texto"]


def test_post_casi_vacio_con_pdf_no_falla(monkeypatch):
    """Antes: 'La página casi no tiene texto' aunque hubiera bases enlazadas."""
    _mock_descarga_pdf(monkeypatch)
    html = (f'<html><head><title>Llamado a concurso</title></head><body>'
            f'<a href="{URL_TRANSPARENCIA}">Bases</a></body></html>')
    res = E.extraer_desde_contenido(
        html.encode(), "text/html", url="https://www.mlonquimay.cl/web/2026/07/llamado/")
    assert res["campos"]["cargo"] == "Director de Administración y Finanzas"
    assert res["campos"]["fecha_cierre"] == "2026-08-28"


def test_post_sin_texto_ni_pdf_sigue_fallando():
    html = '<html><body><div id="app"></div></body></html>'
    with pytest.raises(E.ExtraccionError, match="casi no tiene texto"):
        E.extraer_desde_contenido(html.encode(), "text/html", url="https://spa.cl/")


def test_pdf_enlazado_roto_degrada_con_advertencia(monkeypatch):
    """Si las bases enlazadas no bajan, el HTML igual entrega lo suyo."""
    def _falla(url):
        raise E.ExtraccionError("No se pudo descargar la URL: timeout")
    monkeypatch.setattr(E, "descargar_recurso", _falla)
    res = E.extraer_desde_contenido(
        HTML_POST_MUNICIPAL.encode(), "text/html",
        url="https://muniloncoche.cl/post/")
    assert res["meta"]["metodo"] == "html"
    assert any("bases enlazadas" in a for a in res["meta"]["advertencias"])
    assert res["campos"]["url_bases"] == URL_TRANSPARENCIA  # el link igual queda


# ── PDF directo del Portal de Transparencia (URL sin .pdf al final) ──────────
def test_pdf_directo_transparencia_marca_url_bases(monkeypatch):
    from scrapers import bases_pdf
    monkeypatch.setattr(
        bases_pdf, "leer_pdf",
        lambda contenido, allow_ocr=True, **kw: (TEXTO_BASES_PDF, "texto"))
    res = E.extraer_desde_contenido(
        b"%PDF-1.7 fake", "application/pdf", url=URL_TRANSPARENCIA)
    assert res["meta"]["tipo"] == "pdf"
    assert res["campos"]["url_bases"] == URL_TRANSPARENCIA
    assert res["campos"]["cargo"] == "Director de Administración y Finanzas"
