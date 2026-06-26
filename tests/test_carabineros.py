"""Tests puros (sin red ni BD) del scraper de Carabineros.

Cubren la mejora de "recoger más antecedentes": el detalle HTML debe rendir
objetivo, funciones y requisitos por componente; `construir_oferta` debe emitir
`url_bases`, `modalidad` y `horas_semanales`, y enriquecer descripción y
requisitos combinando card + detalle.
"""
from __future__ import annotations

from datetime import date

from scrapers import carabineros as cb


LISTADO_HTML = """
<div class="concurso-card">
  <div class="block-header">P.N.S. Profesional de Apoyo
    Postulación desde el 01/06/2026 al 30/06/2026</div>
  <ul>
    <li>Modalidad : Contrata</li>
    <li>Cargo : Profesional de Apoyo Jurídico</li>
    <li>Jornada : 44 horas semanales</li>
    <li>Ubicación : Santiago, Región Metropolitana</li>
    <li>N° de vacantes : 2</li>
  </ul>
  <p class="text-justify">Apoyo a la gestión administrativa de la repartición.</p>
  <a href="/concursos/977">Ver detalle</a>
  <a href="/concursos/download/977/Perfil">Perfil</a>
  <a href="/concursos/download/977/Descriptor">Bases del Concurso</a>
  <a href="/concursos/apply/977">Postular</a>
</div>
"""

DETALLE_HTML = """
<html><body>
  <h1>Profesional de Apoyo Jurídico</h1>
  <p>Objetivo del Cargo: Brindar asesoría jurídica a la repartición en
     materias administrativas y de personal.</p>
  <p>Funciones del Cargo: Revisar contratos, emitir informes en derecho y
     apoyar sumarios administrativos.</p>
  <p>Formación Educacional: Título profesional de Abogado/a con a lo menos
     ocho semestres de duración.</p>
  <p>Conocimientos: Derecho administrativo y estatuto administrativo.</p>
  <p>Competencias: Trabajo en equipo, orientación al servicio.</p>
  <p>Experiencia: Tres años en el sector público.</p>
  <p>Remuneración: $ 1.450.000 mensuales</p>
</body></html>
"""


def test_parsear_listado_extrae_docs_y_campos():
    cards = cb.parsear_listado(LISTADO_HTML, cb.BASE)
    assert len(cards) == 1
    c = cards[0]
    assert c["id"] == 977
    assert c["url"].endswith("/concursos/977")
    assert "Perfil" in c["docs"] and "Descriptor" in c["docs"]
    assert c["campos"]["cargo"].startswith("Profesional de Apoyo")
    assert c["fecha_cierre"] == date(2026, 6, 30)


def test_parsear_detalle_captura_secciones_y_requisitos():
    det = cb.parsear_detalle(DETALLE_HTML)
    # Objetivo + Funciones consolidados a descripcion_detalle
    assert "Objetivo:" in det["descripcion_detalle"]
    assert "Funciones:" in det["descripcion_detalle"]
    # Requisitos compuestos por varios componentes
    req = det["requisitos"]
    assert "Formación:" in req
    assert "Conocimientos:" in req
    assert "Competencias:" in req
    assert "Experiencia:" in req
    # Renta del detalle
    assert det["renta"] == 1450000


def test_construir_oferta_emite_url_bases_modalidad_horas():
    c = cb.parsear_listado(LISTADO_HTML, cb.BASE)[0]
    det = cb.parsear_detalle(DETALLE_HTML)
    oferta = cb.construir_oferta(c, det)

    # url_bases prioriza el Descriptor (Bases) sobre el Perfil
    assert oferta["url_bases"].endswith("/concursos/download/977/Descriptor")
    assert oferta["modalidad"] == "Contrata"
    assert oferta["horas_semanales"] == 44
    assert oferta["renta_bruta_min"] == 1450000
    # Descripción enriquecida con objetivo/funciones y campos clave
    assert "Objetivo:" in oferta["descripcion"]
    assert "Jornada:" in oferta["descripcion"]
    # Requisitos provienen del detalle
    assert "Formación:" in oferta["requisitos_texto"]
    assert oferta["region"] == "Región Metropolitana"
    assert oferta["ciudad"] == "Santiago"


def test_url_bases_cae_a_perfil_si_no_hay_descriptor():
    html = LISTADO_HTML.replace(
        '<a href="/concursos/download/977/Descriptor">Bases del Concurso</a>', "")
    c = cb.parsear_listado(html, cb.BASE)[0]
    oferta = cb.construir_oferta(c, {})
    assert oferta["url_bases"].endswith("/concursos/download/977/Perfil")


def test_horas_semanales_descarta_valores_absurdos():
    assert cb._horas_semanales("jornada de 44 horas") == 44
    assert cb._horas_semanales("turno de 200 horas") is None
    assert cb._horas_semanales(None, "") is None
