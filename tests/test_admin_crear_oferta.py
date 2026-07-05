"""Recorte de campos VARCHAR al crear/editar ofertas manuales.

Regresión: el editor del panel escribe campos de texto libre (jornada, grado,
correos, etc.) sin recortarlos. Un valor más largo que la columna hacía que el
INSERT/UPDATE lanzara StringDataRightTruncation; la excepción escapaba sin
headers CORS y el navegador mostraba "Failed to fetch". `_recortar_varchar` los
recorta a la longitud de columna antes de escribir.
"""
from api.routers.admin import _LIMITES_VARCHAR, _recortar_varchar


def test_recorta_grado_eus_a_limite():
    fila = {"grado_eus": "G" * 50}  # columna VARCHAR(20)
    _recortar_varchar(fila)
    assert len(fila["grado_eus"]) == 20


def test_recorta_jornada_a_limite():
    fila = {"jornada": "J" * 300}  # VARCHAR(100)
    _recortar_varchar(fila)
    assert len(fila["jornada"]) == 100


def test_valores_cortos_no_se_tocan():
    fila = {"cargo": "Analista", "region": "Metropolitana de Santiago"}
    _recortar_varchar(fila)
    assert fila["cargo"] == "Analista"
    assert fila["region"] == "Metropolitana de Santiago"


def test_ignora_no_strings_y_columnas_desconocidas():
    fila = {"numero_vacantes": 3, "renta_bruta_min": 800000, "descripcion": "x" * 5000}
    out = _recortar_varchar(fila)
    # descripcion es TEXT (sin límite en el mapa) → no se recorta.
    assert out["descripcion"] == "x" * 5000
    assert out["numero_vacantes"] == 3


def test_todos_los_limites_son_positivos():
    assert all(isinstance(v, int) and v > 0 for v in _LIMITES_VARCHAR.values())
