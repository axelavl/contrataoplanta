"""Tests del scraper Banco Central — reconstrucción de líneas del PDF de bases.

Caso real (Circular 3399, Especialista en Integraciones de Sistemas SAP): el
requisito de títulos se parte en dos renglones en el PDF y antes se perdía la
parte de los títulos profesionales, dejando huérfano "o carrera profesional
afín…". Estos tests fijan que la línea se reúna y el requisito completo
sobreviva en ``requisitos_texto``.
"""
from __future__ import annotations

from scrapers.bcentral import (
    _extraer_pasos_postulacion,
    _unir_lineas_envueltas,
    construir_oferta,
    parsear_pdf_bases,
)


# Recuadro de postulación tal como aparece en las bases (cada paso en su línea).
PDF_POSTULACION = """\
La primera pre-selección de los candidatos se realizará en consideración a la
información manifestada en el currículum vitae (CV).
Para favorecer la entrega de información será requisito excluyente completar el
siguiente formulario de postulación: Formulario de Postulación Online
Recepción de antecedentes: hasta el 28 de junio de 2026
En caso de dudas o consultas, escribirnos al siguiente correo: atracciontalentos@bcentral.cl
Se deberá indicar expectativas de renta líquida.
Cada postulante deberá completar el Formulario de Postulación Online, adjuntando su Currículum en formato pdf.
"""


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


class TestPasosPostulacion:
    def test_extrae_pasos_clave(self):
        pasos = _extraer_pasos_postulacion(PDF_POSTULACION)
        joined = " || ".join(pasos)
        assert any("Formulario de Postulación Online" in p for p in pasos)
        assert any("Recepción de antecedentes" in p for p in pasos)
        assert any("expectativas de renta" in p.lower() for p in pasos)
        assert any("atracciontalentos@bcentral.cl" in p for p in pasos)
        # No arrastra la frase de la pre-selección (no es un paso para postular).
        assert "pre-selección" not in joined.lower()

    def test_no_pasos_sin_senales(self):
        assert _extraer_pasos_postulacion("Texto sin nada relevante para postular.") == []

    def test_descripcion_incluye_bloque_como_postular(self):
        v = {
            "concurso": 3399,
            "cargo": "Especialista en Integraciones de Sistemas SAP",
            "url": "https://www.bcentral.cl/x",
            "descripcion": "Se llama a postulación, hasta el 28 de junio de 2026, "
                           "para cubrir una vacante a plazo fijo en el cargo de "
                           "Especialista en Integraciones de Sistemas SAP.",
            "nivel_estructura": "16",
            "vacantes": 1,
            "tipo": "Plazo fijo",
            "fecha_cierre": None,
        }
        pdf = parsear_pdf_bases(PDF_POSTULACION + "\n" + PDF_BASES_3399)
        oferta = construir_oferta(v, {}, pdf)
        desc = oferta["descripcion"] or ""
        assert "Cómo postular:" in desc
        assert "Formulario de Postulación Online" in desc
        # El bloque va en líneas propias (no en el viejo formato " | ").
        assert "\nCómo postular:\n- " in desc
