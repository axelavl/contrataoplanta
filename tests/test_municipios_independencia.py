"""Independencia: el aviso real vive en el PDF de bases enlazado desde cada
página de concurso (/concurso-publico-*). El scraper entra al detalle, ubica el
PDF (no un formulario) y extrae de él cierre / requisitos / renta / correos.
"""
from datetime import date

import pytest

import scrapers.municipios as M

_BASES = (
    "BASES CONCURSO PÚBLICO Coordinador/a Albergue Público. "
    "Requisitos: título profesional del área social, experiencia en albergues, "
    "disponibilidad inmediata. Funciones: coordinar el albergue. "
    "Renta: $1.200.000 mensuales. "
    "Las postulaciones deben enviarse al correo reclutamiento@independencia.cl. "
    "Recepción de antecedentes: del 10 al 15 de julio de 2026."
)
_DETALLE = (
    "<html><body><main>"
    "<h1>Concurso Público para Coordinador/a Albergue Público</h1>"
    "<p>Descarga las bases:</p>"
    "<a href='/wp-content/uploads/bases-coordinador-albergue.pdf'>Bases del concurso (PDF)</a>"
    "</main></body></html>"
)
_LISTADO = (
    "<html><body><main>"
    "<a href='/concurso-publico-para-coordinador-a-albergue-publico/'>Coordinador Albergue</a>"
    "</main></body></html>"
)
_FUENTE = next(f for f in M.FUENTES if f["clave"] == "independencia")


class _Resp:
    def __init__(self, text="", content=b"%PDF-fake"):
        self.status_code = 200
        self.text = text
        self.content = content


class _Sess:
    def get(self, url, **kw):
        return _Resp(content=b"%PDF-fake") if url.endswith(".pdf") else _Resp(text=_DETALLE)


@pytest.fixture(autouse=True)
def _fake_pdf(monkeypatch):
    # Evita depender de pypdf/OCR: el parseo del PDF devuelve texto conocido.
    monkeypatch.setattr(M, "_pdf_texto", lambda content: _BASES)


def _item():
    return M.extraer_enlaces_detalle(_LISTADO, _FUENTE, _Sess(), delay=0)[0]


def test_cargo_limpio_sin_para():
    assert _item()["cargo"] == "Coordinador/a Albergue Público"


def test_cierre_desde_pdf_rango():
    assert _item()["fecha_cierre"] == date(2026, 7, 15)


def test_requisitos_desde_pdf():
    assert "título profesional del área social" in (_item()["requisitos"] or "")


def test_correo_desde_pdf():
    assert _item()["email_postulacion"] == "reclutamiento@independencia.cl"


def test_url_bases_apunta_al_pdf():
    assert _item()["url_bases"].endswith("bases-coordinador-albergue.pdf")


def test_renta_texto_desde_pdf():
    assert "$1.200.000" in (_item()["renta_texto"] or "")


def test_construir_oferta_completa():
    o = M.construir_oferta(_item(), _FUENTE)
    assert o["cargo"] == "Coordinador/a Albergue Público"
    assert o["fecha_cierre"] == date(2026, 7, 15)
    assert o["email_postulacion"] == "reclutamiento@independencia.cl"
    assert o["url_bases"].endswith(".pdf")
    assert o["descripcion"]  # se pobló desde el PDF (HTML pobre)


def test_bases_pdf_ignora_formulario():
    from bs4 import BeautifulSoup
    html = (
        "<a href='/uploads/formulario-1-a-declaracion.pdf'>Formulario 1-A</a>"
        "<a href='/uploads/bases-concurso.pdf'>Bases</a>"
    )
    soup = BeautifulSoup(html, "html.parser")
    got = M._bases_pdf(soup, "https://x.cl/")
    assert got.endswith("bases-concurso.pdf")


def test_bases_pdf_unico_no_excluido():
    # Sin nombre delator pero un único PDF no-excluido → se asume base.
    from bs4 import BeautifulSoup
    soup = BeautifulSoup("<a href='/uploads/documento-concurso-2026.pdf'>Descargar</a>",
                         "html.parser")
    assert M._bases_pdf(soup, "https://x.cl/").endswith("documento-concurso-2026.pdf")
