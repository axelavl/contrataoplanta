"""Clasificador de `ofertas.area_profesional` (classification/area_profesional.py).

Los casos con id son ofertas reales de producción (27-oct-2026) que el
clasificador anterior dejaba mal.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from classification.area_profesional import AREAS, clasificar_area


@pytest.mark.parametrize("cargo,esperada", [
    # "datos" ya no dispara TI
    ("Base de Datos de Reemplazo de Asistente de Tardes de Juego en mi Jardín", "Educación"),
    ("Base de Datos Reemplazos Educadora Párvulos, JI Temuco", "Educación"),
    # TENS ya no cae en "Administración" por defecto
    ("TENS UCI (HC641)", "Salud"),
    ("TENS CDT", "Salud"),
    ("INSTRUMENTISTA QUIRURGICO-DIURNO", "Salud"),
    ("EU CLINICA SERVICIO CIRUGIA-4TO TURNO", "Salud"),
    # "medic" dentro de "Medición" ya no es Salud
    ("Ingeniero/a en Automatización y Sistemas de Medición, Idiem", "Ingeniería"),
    ("ANALISTA MESA DE SERVICIO DEPARTAMENTO TI", "TI"),
    ("AUXILIAR DE SERVICIO- DIURNO O 4TO TURNO", "Servicios Generales"),
    ("Guardaparque Monumento Natural Quebrada de la Plata", "Medioambiente"),
    ("Especialista en Instalaciones Eléctricas y CCDD", "Ingeniería"),
])
def test_casos_reales_mal_clasificados_antes(cargo, esperada):
    assert clasificar_area(cargo) == esperada


def test_usa_requisitos_si_el_cargo_no_dice_la_profesion():
    assert clasificar_area("Supervisor/a de Higiene Hospitalaria",
                           "Título Técnico en Enfermería nivel Superior (TENS)") == "Salud"
    assert clasificar_area("Coordinador Recursos Físicos",
                           "Formación Académica: Título de Ingeniería Eléctrica") == "Ingeniería"


def test_varias_carreras_manda_la_mayoria():
    req = ("Título Profesional como Licenciatura en Tecnología Médica, Licenciatura "
           "en Biotecnología, Licenciatura en Biología, Ingeniería en Biotecnología.")
    assert clasificar_area("Profesional Asistente de Investigación", req) == "Ciencias"


def test_sin_senal_queda_sin_clasificar():
    assert clasificar_area("Ayudante, Idiem, Universidad de Chile") is None
    assert clasificar_area("") is None
    assert clasificar_area(None) is None


def test_etiquetas_dentro_del_vocabulario():
    datos = json.loads((Path(__file__).parent / "fixtures" / "cv_matching_eval.json")
                       .read_text(encoding="utf-8"))
    for o in datos["ofertas"]:
        area = clasificar_area(o["cargo"], o["requisitos"])
        assert area is None or area in AREAS, (o["cargo"], area)
