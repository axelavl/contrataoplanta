"""Municipalidad de La Cisterna (id 388): concurso de planta por estamento+grado.

El sitio es una SPA (mejormunicipio.com), no scrapeable con requests. El
concurso publica todas las vacantes en un PDF de bases; se fija su URL en la
fuente y se descarga directo, separando UNA oferta por estamento+grado.
"""
from __future__ import annotations

import scrapers.municipios as M
import scrapers.run_all as R

_FUENTE = next(f for f in M.FUENTES if f["clave"] == "lacisterna")

# Resumen tal como aparece en el PDF de bases real.
_RESUMEN = (
    "correspondientes a: 5 cargos vacantes en Planta Profesionales Grado 9°; "
    "1 cargo vacante en Planta Profesionales Grado 10°; 1 cargo vacante en Planta "
    "Profesionales Grado 11°; 1 cargo vacante en Planta Profesionales Médico "
    "Psicotécnico de 33 horas; 5 cargos vacantes en Planta Jefaturas Grado 11°; "
    "2 cargos vacantes en Planta Jefaturas Grado 12°; 7 cargos vacantes en Planta "
    "Técnicos Grado 13°; 14 cargos vacantes en Planta Administrativos Grado 14°; "
    "4 cargos vacantes en Planta Administrativos Grado 15°; 2 cargos vacantes en "
    "Planta Administrativos Grado 16°; y 20 cargos vacantes en Planta Auxiliares "
    "Grado 14°. Este concurso de ingreso a la Planta se ajusta a la normativa…"
)


def test_fuente_registrada_modo_y_pdf():
    assert _FUENTE["id"] == 388
    assert _FUENTE["modo"] == "bases_estamento_grado"
    assert _FUENTE["bases_pdf"].endswith(".pdf")
    assert 388 in R._IDS_NUEVO_ESTANDAR


def test_parser_separa_por_estamento_y_grado():
    grupos = M.parsear_bases_estamento_grado(_RESUMEN)
    porcargo = {g["cargo"]: g for g in grupos}
    # 10 grupos por grado + 1 médico psicotécnico = 11.
    assert len(grupos) == 11
    assert sum(g["vacantes"] for g in grupos) == 62
    # Singulariza el estamento (incluye "Técnicos" con tilde → "Técnico").
    assert porcargo["Profesional Grado 9"]["vacantes"] == 5
    assert porcargo["Técnico Grado 13"]["vacantes"] == 7
    assert porcargo["Auxiliar Grado 14"]["vacantes"] == 20
    assert "Profesional Médico Psicotécnico (33 horas)" in porcargo


def test_parser_sin_grupos_devuelve_vacio():
    assert M.parsear_bases_estamento_grado("Texto sin vacantes de planta.") == []


class _RespPDF:
    status_code = 200

    def __init__(self, content):
        self.content = content


class _SessPDF:
    """Devuelve un PDF con texto extraíble (mínimo válido no sirve; usamos el
    parser directamente, así que aquí sólo verificamos el fallback sin texto)."""
    def __init__(self, content):
        self._c = content

    def get(self, *a, **k):
        return _RespPDF(self._c)


def test_extractor_sin_pdf_texto_no_crashea(monkeypatch):
    # Un PDF sin texto extraíble (o no-PDF) → sin ofertas, sin excepción.
    monkeypatch.setattr(M.time, "sleep", lambda *a, **k: None)
    items = M.extraer_bases_estamento_grado("", _FUENTE, _SessPDF(b"no-es-pdf"), 0)
    assert items == []


def test_construir_oferta_estamento_grado():
    grupos = M.parsear_bases_estamento_grado(_RESUMEN)
    item = {
        "cargo": grupos[0]["cargo"],
        "url": _FUENTE["bases_pdf"] + "#" + grupos[0]["slug"],
        "url_bases": _FUENTE["bases_pdf"],
        "bloque": "Planta Profesional · Grado 9 · 5 vacante(s).",
        "numero_vacantes": grupos[0]["vacantes"],
        "tipo": "Planta (Concurso Público Municipal)",
    }
    o = M.construir_oferta(item, _FUENTE)
    assert o["ciudad"] == "La Cisterna"
    assert o["region"] == "Metropolitana de Santiago"
    assert o["numero_vacantes"] == 5
    assert o["url_bases"].endswith(".pdf")


def test_ids_municipios_a_nivel_de_modulo():
    # El panel (mode="municipios" en admin) arma el --ids desde este set; debe
    # existir a nivel de módulo e incluir las fuentes funcionales del batch.
    assert hasattr(R, "IDS_MUNICIPIOS")
    assert {345, 382, 388, 398} <= set(R.IDS_MUNICIPIOS)
    # El gate del batch usa el mismo set (sin lista duplicada dentro de main).
    import pathlib
    src = pathlib.Path(R.__file__).read_text(encoding="utf-8")
    assert src.count("345, 363, 382") <= 2   # constante + _IDS_NUEVO_ESTANDAR
