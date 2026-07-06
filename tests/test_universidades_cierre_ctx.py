"""universidades.py: la fecha de cierre del listado sólo se toma si está
etiquetada (cierre/término/plazo/hasta). Una fecha suelta suele ser de
publicación; tomarla como cierre descartaba ofertas vigentes como "vencidas".
"""
from datetime import date

import scrapers.universidades as U


def test_fecha_suelta_no_es_cierre():
    assert U._fecha_cierre_ctx("Publicado el 01/03/2026. Cargo docente vigente.") is None


def test_cierre_etiquetado_se_toma():
    assert U._fecha_cierre_ctx("Postulaciones hasta el 30/12/2026.") == date(2026, 12, 30)


def test_termino_en_texto_es():
    assert U._fecha_cierre_ctx("Fecha de término: 15 de agosto de 2026") == date(2026, 8, 15)


def test_sin_fecha_devuelve_none():
    assert U._fecha_cierre_ctx("Cargo docente de jornada completa.") is None
