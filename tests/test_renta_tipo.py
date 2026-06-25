"""Pruebas de clasificación de tipo de renta (bruta vs líquida).

Regla de negocio: la renta informada debe ser bruta; si la fuente solo trae
líquida, debe quedar señalado en ``renta_tipo`` y en ``renta_texto``.
"""

from extraction.renta_tipo import (
    TIPO_BRUTA,
    TIPO_INDETERMINADA,
    TIPO_LIQUIDA,
    anotar_renta_texto,
    clasificar_tipo_renta,
    resolver_tipo_renta,
)
from extraction.salary_extractor import extract_salary


# ── clasificar_tipo_renta ────────────────────────────────────────────────────

def test_clasifica_bruta():
    assert clasificar_tipo_renta("Renta bruta mensual $1.800.000") == TIPO_BRUTA
    assert clasificar_tipo_renta("Sueldo bruto $2.000.000") == TIPO_BRUTA


def test_clasifica_liquida_con_y_sin_tilde():
    assert clasificar_tipo_renta("Renta líquida $1.200.000") == TIPO_LIQUIDA
    assert clasificar_tipo_renta("renta liquida aproximada de $900.000") == TIPO_LIQUIDA
    assert clasificar_tipo_renta("Sueldo líquido $1.000.000") == TIPO_LIQUIDA


def test_bruta_tiene_prioridad_si_aparecen_ambas():
    # Bruta es el valor canónico que se reporta.
    txt = "Renta bruta $2.000.000 (líquido aprox. $1.500.000)"
    assert clasificar_tipo_renta(txt) == TIPO_BRUTA


def test_sin_senal_es_indeterminada():
    assert clasificar_tipo_renta("Remuneración $1.500.000") == TIPO_INDETERMINADA
    assert clasificar_tipo_renta("") == TIPO_INDETERMINADA
    assert clasificar_tipo_renta(None) == TIPO_INDETERMINADA


def test_no_confunde_palabras_parecidas():
    # "distributo"/"absoluto" no deben gatillar 'brut'; \b ancla el inicio.
    assert clasificar_tipo_renta("monto absoluto $1.000.000") == TIPO_INDETERMINADA


def test_resolver_prioriza_primera_fuente_concluyente():
    assert resolver_tipo_renta(None, "renta líquida $900.000", "cargo") == TIPO_LIQUIDA
    assert resolver_tipo_renta("Renta bruta $1.000.000", "líquida") == TIPO_BRUTA
    assert resolver_tipo_renta("", None) == TIPO_INDETERMINADA


# ── anotar_renta_texto ───────────────────────────────────────────────────────

def test_anota_liquida_cuando_falta_marca():
    assert anotar_renta_texto("$900.000", TIPO_LIQUIDA) == "$900.000 (renta líquida)"


def test_no_duplica_marca_si_ya_existe():
    # El texto ya dice "líquida" → no se vuelve a anexar.
    assert anotar_renta_texto("Renta líquida $900.000", TIPO_LIQUIDA) == "Renta líquida $900.000"
    assert anotar_renta_texto("Renta bruta $1.800.000", TIPO_BRUTA) == "Renta bruta $1.800.000"


def test_indeterminada_no_inventa_marca():
    assert anotar_renta_texto("$1.500.000", TIPO_INDETERMINADA) == "$1.500.000"


def test_liquida_sin_texto_deja_constancia():
    assert anotar_renta_texto(None, TIPO_LIQUIDA) == "(renta líquida)"


def test_respeta_tope_200_caracteres():
    largo = "x" * 210
    out = anotar_renta_texto(largo, TIPO_LIQUIDA)
    assert out is not None
    assert len(out) <= 200
    assert out.endswith("(renta líquida)")


# ── integración con extract_salary ───────────────────────────────────────────

def test_extract_salary_marca_liquida():
    res = extract_salary("La renta líquida mensual es de $1.200.000.")
    assert res.amount == 1_200_000
    assert res.kind == TIPO_LIQUIDA


def test_extract_salary_marca_bruta():
    res = extract_salary("Renta bruta mensual $1.800.000 según bases.")
    assert res.amount == 1_800_000
    assert res.kind == TIPO_BRUTA
