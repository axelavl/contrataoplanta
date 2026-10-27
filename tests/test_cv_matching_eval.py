"""Banco de evaluación del matching CV ↔ ofertas.

`fixtures/cv_matching_eval.json` trae las ofertas vigentes de producción del
27-oct-2026 y 14 CV sintéticos (TENS, enfermería, medicina, párvulos,
ingeniería, informática, administración, contabilidad, derecho, etc.) con
las ofertas pertinentes marcadas a mano. Incluye CV sin ofertas afines
(abogado, kinesiólogo): ahí lo correcto es no sugerir nada.

Antes de la detección por profesión (oct-2026) la precisión era 0,31: una
enfermera recibía cargos de TENS y médico, y una ingeniera comercial cargos
de ingeniería. Si un cambio la baja de 0,9, este test lo frena.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from api.services import cv_matching as cm

DATOS = json.loads((Path(__file__).parent / "fixtures" / "cv_matching_eval.json")
                   .read_text(encoding="utf-8"))


def _evaluar():
    tp = fp = fn = 0
    detalle = {}
    for nombre, cv in DATOS["cvs"].items():
        perfil = cm.perfil_desde_texto(cv["texto"], hoy=date(2026, 10, 27))
        sugeridas = {r.oferta["id"] for r in cm.rankear(perfil, DATOS["ofertas"])}
        pertinentes = set(cv["pertinentes"])
        tp += len(sugeridas & pertinentes)
        fp += len(sugeridas - pertinentes)
        fn += len(pertinentes - sugeridas)
        detalle[nombre] = (sugeridas, pertinentes)
    return tp, fp, fn, detalle


def test_precision_y_cobertura_sobre_ofertas_reales():
    tp, fp, fn, detalle = _evaluar()
    precision = tp / (tp + fp)
    cobertura = tp / (tp + fn)
    assert precision >= 0.9, (precision, detalle)
    assert cobertura >= 0.9, (cobertura, detalle)


@pytest.mark.parametrize("nombre", ["abogado", "kinesiologo"])
def test_sin_ofertas_afines_no_inventa(nombre):
    perfil = cm.perfil_desde_texto(DATOS["cvs"][nombre]["texto"])
    assert cm.rankear(perfil, DATOS["ofertas"]) == []
