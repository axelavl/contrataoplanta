"""UOH (uoh.cl/trabaja-con-nosotros): recoge toda la info del concurso.

Los concursos son un CPT de WordPress (posts /concurso/…). El scraper ahora:
- Prefiere la API REST de WP (auto-descubierta vía /wp/v2/types) para no perder
  avisos que el listado HTML podría omitir; si no hay REST, cae al listado.
- Enriquece cada aviso con el detalle + PDF de bases (requisitos, cierre, etc.).
- Elige el PDF de BASES, no un documento institucional.
"""
from datetime import date

import pytest

import scrapers.universidades_wp as W

_FUENTE = next(f for f in W.FUENTES if f["clave"] == "uoh")

_CONTENT = (
    "<p>Descripción del cargo docente de cátedra.</p>"
    "<p>Requisitos: Doctor en Ciencias con experiencia docente universitaria "
    "y publicaciones indexadas.</p>"
    "<p>Postulaciones hasta el 30 de diciembre de 2026.</p>"
    "<a href='/cuenta-publica-2025.pdf'>Cuenta pública</a>"
    "<a href='/bases-concurso-docente.pdf'>Bases del concurso</a>"
)
_TITULO = ("Docente de Cátedra: La Tierra como Sistema Complejo - "
           "Escuela de Educación")
_LINK = ("https://www.uoh.cl/concurso/"
         "docente-de-catedra-la-tierra-como-sistema-complejo-escuela-de-educacion/")

_TYPES_JSON = {
    "post": {"slug": "post", "rest_base": "posts"},
    "concurso": {"slug": "concurso", "rest_base": "concurso"},
}
_CPT_JSON = [{
    "id": 1, "link": _LINK, "date": "2026-07-01T10:00:00",
    "title": {"rendered": _TITULO},
    "content": {"rendered": _CONTENT},
}]

# Listado HTML (para el fallback cuando no hay REST).
_LISTADO_HTML = (
    "<html><body><article>"
    f"<a href='{_LINK}'>{_TITULO}</a>"
    "</article></body></html>"
)
_DETALLE_HTML = f"<html><body><div class='entry-content'>{_CONTENT}</div></body></html>"


class _Resp:
    def __init__(self, *, js=None, text="", content=b"x", status=200):
        self.status_code = status
        self._js = js
        self.text = text
        self.content = content

    def json(self):
        if self._js is None:
            raise ValueError("no json")
        return self._js


def _session(rest=True):
    class _Sess:
        def get(self, url, **kw):
            if "/wp-json/wp/v2/types" in url:
                return _Resp(js=_TYPES_JSON) if rest else _Resp(status=404)
            if "/wp-json/wp/v2/concurso" in url:
                return _Resp(js=_CPT_JSON)
            if url.lower().endswith(".pdf"):
                return _Resp(content=b"not-a-pdf")   # enrich lo ignora
            if "/concurso/" in url:
                return _Resp(text=_DETALLE_HTML)
            return _Resp(text=_LISTADO_HTML)          # /trabaja-con-nosotros/
    return _Sess()


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(W.time, "sleep", lambda *_a, **_k: None)


# ── PDF de bases ─────────────────────────────────────────────────────────────
def test_pdf_bases_prefiere_bases_no_cuenta_publica():
    got = W._pdf_bases(_CONTENT, "https://www.uoh.cl")
    assert got.endswith("bases-concurso-docente.pdf")


# ── Descubrimiento del CPT ───────────────────────────────────────────────────
def test_descubrir_cpt_encuentra_concurso():
    assert W._descubrir_cpt("https://www.uoh.cl", _session()) == "concurso"


def test_descubrir_cpt_none_si_no_hay_types():
    assert W._descubrir_cpt("https://www.uoh.cl", _session(rest=False)) is None


# ── Camino REST (preferido) ──────────────────────────────────────────────────
def test_listado_usa_rest_y_arma_oferta_completa():
    ofertas = W.recolectar_listado(_FUENTE, _session(rest=True),
                                   con_detalle=True, incluir_cerrados=False)
    assert len(ofertas) == 1
    o = ofertas[0]
    assert "Docente de Cátedra" in o["cargo"]
    assert o["fecha_cierre"] == date(2026, 12, 30)
    assert o["requisitos_texto"] and "Doctor en Ciencias" in o["requisitos_texto"]
    assert o["url_bases"].endswith("bases-concurso-docente.pdf")
    assert o["fecha_publicacion"] == date(2026, 7, 1)


# ── Fallback HTML (sin REST) ─────────────────────────────────────────────────
def test_listado_fallback_html_cuando_no_hay_rest():
    ofertas = W.recolectar_listado(_FUENTE, _session(rest=False),
                                   con_detalle=True, incluir_cerrados=False)
    assert len(ofertas) == 1
    o = ofertas[0]
    assert "Docente de Cátedra" in o["cargo"]
    assert o["fecha_cierre"] == date(2026, 12, 30)      # del detalle
    assert o["url_bases"].endswith("bases-concurso-docente.pdf")
