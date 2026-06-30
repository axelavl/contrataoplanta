"""Tests del enriquecimiento compartido (scrapers/enrich.py).

Verifican que los criterios de extracción (títulos profesionales, funciones,
cómo postular) apliquen a cualquier scraper que use ``enriquecer_oferta``, sin
red (``descargar_pdfs=False``).
"""
from __future__ import annotations

import pytest

from scrapers.enrich import enriquecer_oferta

# El detalle/PDF de una oferta cualquiera, con el título partido en dos líneas,
# funciones bajo encabezado, y un recuadro de cómo postular.
TEXTO_DETALLE = """\
Requisitos del cargo
- Título profesional de Ingeniería Comercial, Administración Pública o Auditoría,
o carrera afín de al menos 8 semestres de duración.
- Experiencia mínima de 3 años en el sector público.

Funciones del cargo
- Coordinar la ejecución presupuestaria de la unidad.
- Elaborar informes de gestión mensuales.
- Supervisar al equipo administrativo.

Cómo postular
Cada postulante deberá completar el Formulario de Postulación Online, adjuntando su Currículum en formato PDF.
Recepción de antecedentes: hasta el 30 de julio de 2026
En caso de dudas o consultas, escribir a seleccion@institucion.cl
"""


@pytest.fixture
def oferta_base():
    return {
        "cargo": "Analista de Presupuesto",
        "descripcion": "Cargo del área de administración y finanzas.",
        "tipo_cargo": "",
        "requisitos_texto": None,
    }


def test_titulos_no_se_pierden(oferta_base):
    enriquecer_oferta(oferta_base, texto_html=TEXTO_DETALLE, descargar_pdfs=False)
    req = oferta_base.get("requisitos_texto") or ""
    # El título completo sobrevive (no solo "o carrera afín…").
    assert "Ingeniería Comercial" in req
    assert "carrera afín de al menos 8 semestres" in req


def test_funciones_se_anexan_como_bloque(oferta_base):
    enriquecer_oferta(oferta_base, texto_html=TEXTO_DETALLE, descargar_pdfs=False)
    desc = oferta_base.get("descripcion") or ""
    assert "Funciones del cargo:" in desc
    assert "Coordinar la ejecución presupuestaria" in desc
    # En líneas propias (no fundido con " | ").
    assert "Funciones del cargo:\n- " in desc


def test_como_postular_se_extrae(oferta_base):
    enriquecer_oferta(oferta_base, texto_html=TEXTO_DETALLE, descargar_pdfs=False)
    desc = oferta_base.get("descripcion") or ""
    assert "Cómo postular:" in desc
    assert "Formulario de Postulación Online" in desc
    assert "Recepción de antecedentes" in desc


def test_idempotente_no_duplica_funciones(oferta_base):
    enriquecer_oferta(oferta_base, texto_html=TEXTO_DETALLE, descargar_pdfs=False)
    primera = oferta_base["descripcion"]
    enriquecer_oferta(oferta_base, texto_html=TEXTO_DETALLE, descargar_pdfs=False)
    # Ya tiene "Funciones"/"Cómo postular" → no se vuelven a anexar.
    assert oferta_base["descripcion"].count("Funciones del cargo:") == 1
    assert oferta_base["descripcion"].count("Cómo postular:") == 1
