"""Tests del scraper Banco Central — reconstrucción de líneas del PDF de bases.

Caso real (Circular 3399, Especialista en Integraciones de Sistemas SAP): el
requisito de títulos se parte en dos renglones en el PDF y antes se perdía la
parte de los títulos profesionales, dejando huérfano "o carrera profesional
afín…". Estos tests fijan que la línea se reúna y el requisito completo
sobreviva en ``requisitos_texto``.
"""
from __future__ import annotations

from scrapers.bcentral import _unir_lineas_envueltas, parsear_pdf_bases


# Texto tal como pdfplumber lo entrega: el bullet de títulos parte en 2 líneas.
PDF_BASES_3399 = """\
Los requisitos obligatorios del cargo son:
- Título profesional de Ingeniería Civil Industrial, Civil Informática, Ejecución en Computación o Informática,
o carrera profesional afín de al menos 8 semestres de duración.
- Experiencia comprobable en proyectos de implementación, conversión o migración técnica de SAP ECC
a SAP S/4HANA.
- Experiencia mínima de 2 años en diseño, construcción o migración de integraciones entre sistemas SAP
y no SAP, utilizando SAP BTP Integration Suite/SAP Cloud Integration (CPI), SAP PI/PO o plataformas
equivalentes.
"""


class TestUnirLineasEnvueltas:
    def test_reune_bullet_de_titulos(self):
        unido = _unir_lineas_envueltas(PDF_BASES_3399)
        assert (
            "Título profesional de Ingeniería Civil Industrial, Civil Informática, "
            "Ejecución en Computación o Informática, o carrera profesional afín de "
            "al menos 8 semestres de duración." in unido
        )
        # La continuación ya no queda como renglón propio.
        assert "\no carrera profesional afín" not in unido

    def test_no_fusiona_bullets_distintos(self):
        unido = _unir_lineas_envueltas(PDF_BASES_3399)
        lineas = [ln for ln in unido.split("\n") if ln.strip()]
        bullets = [ln for ln in lineas if ln.lstrip().startswith("-")]
        # Tres bullets de origen → tres bullets tras reunir las envolturas.
        assert len(bullets) == 3

    def test_respeta_encabezado_de_seccion(self):
        unido = _unir_lineas_envueltas(PDF_BASES_3399)
        assert unido.split("\n")[0].strip() == "Los requisitos obligatorios del cargo son:"

    def test_oracion_completa_no_se_une_a_la_siguiente(self):
        txt = "Primera idea completa.\nSegunda idea que arranca aparte."
        assert _unir_lineas_envueltas(txt) == txt


class TestParsearPdfBases:
    def test_requisitos_texto_conserva_titulos(self):
        out = parsear_pdf_bases(PDF_BASES_3399)
        req = out.get("requisitos_texto") or ""
        assert "Ingeniería Civil Industrial" in req
        assert "Ejecución en Computación" in req
        # El requisito completo, no el fragmento huérfano.
        assert "afín de al menos 8 semestres" in req
