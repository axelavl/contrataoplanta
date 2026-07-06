"""El script de auditoría del catálogo corre sin BD y clasifica el catálogo real."""
import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "auditar_catalogo.py"


def _cargar_modulo():
    spec = importlib.util.spec_from_file_location("auditar_catalogo", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_clasificar_catalogo_real():
    aud = _cargar_modulo()
    insts = aud._cargar()
    assert len(insts) > 400
    filas = aud._clasificar(insts)
    assert len(filas) == len(insts)
    coberturas = {f["_cobertura"] for f in filas}
    assert "efectiva" in coberturas
    assert coberturas <= {"efectiva", "empleos_publicos", "url_debil",
                          "no_runnable", "sin_url"}
