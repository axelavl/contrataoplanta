"""Tests de los helpers compartidos de estructuración de texto."""
from __future__ import annotations

from extraction.text_sections import (
    extraer_pasos_postulacion,
    unir_lineas_envueltas,
)


class TestUnirLineasEnvueltas:
    def test_reune_continuacion_en_minuscula(self):
        txt = ("- Título profesional de Ingeniería Civil Industrial, Civil Informática,\n"
               "o carrera profesional afín de al menos 8 semestres de duración.")
        out = unir_lineas_envueltas(txt)
        assert "afín de al menos 8 semestres" in out
        assert "\no carrera" not in out

    def test_no_funde_oraciones_completas(self):
        txt = "Primera idea completa.\nSegunda idea aparte."
        assert unir_lineas_envueltas(txt) == txt

    def test_no_funde_mayuscula_nueva(self):
        txt = ("Recepción de antecedentes: hasta el 28 de junio de 2026\n"
               "En caso de dudas o consultas, escribir al correo x@y.cl")
        out = unir_lineas_envueltas(txt)
        assert out.count("\n") == 1  # siguen en líneas separadas


class TestExtraerPasosPostulacion:
    def test_detecta_pasos_clave(self):
        txt = (
            "Cada postulante deberá completar el Formulario de Postulación Online, "
            "adjuntando su Currículum en formato PDF.\n"
            "Recepción de antecedentes: hasta el 28 de junio de 2026\n"
            "Se deberá indicar expectativas de renta líquida.\n"
            "En caso de dudas o consultas, escribir a rrhh@servicio.cl\n"
        )
        pasos = extraer_pasos_postulacion(txt)
        assert any("Formulario de Postulación Online" in p for p in pasos)
        assert any("Recepción de antecedentes" in p for p in pasos)
        assert any("expectativas de renta" in p.lower() for p in pasos)

    def test_sin_senales_devuelve_vacio(self):
        assert extraer_pasos_postulacion("Texto irrelevante sobre el clima.") == []
