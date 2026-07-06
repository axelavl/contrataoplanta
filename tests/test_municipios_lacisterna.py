"""Municipalidad de La Cisterna (id 388): registro de la fuente.

PHP estático (requiere_js=No) → requests sirve. Se scrapea la página de
concursos en modo pdf_links (bases enlazadas como PDF en el host propio).
El detalle de extracción queda pendiente de verificar con el HTML real; este
test fija el cableado: fuente registrada y excluida del dispatch genérico.
"""
from __future__ import annotations

import scrapers.municipios as M
import scrapers.run_all as R


def test_lacisterna_registrada():
    f = next(x for x in M.FUENTES if x["clave"] == "lacisterna")
    assert f["id"] == 388
    assert f["modo"] == "pdf_links"
    assert f["url"] == "http://www.cisterna.cl/022-concurso-publico.php"
    assert f["pdf_host"] == "cisterna.cl"
    assert f["region"] == M.RM


def test_lacisterna_cableada_en_run_all():
    # En el gate del batch de municipios (para --ids 388) y excluida del genérico
    # (388 se clasifica como generic; sin esto correría dos veces).
    assert 388 in R._IDS_NUEVO_ESTANDAR
