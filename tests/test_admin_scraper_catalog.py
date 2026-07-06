"""/scraper/catalog: el listado de instituciones que alimenta el selector.

Regresión del 500 en producción: el JSON del catálogo tiene top-level dict
{"metadata":…, "instituciones":[…]}; el endpoint iteraba el dict (strings) y
reventaba con "'str' object has no attribute 'get'" → el panel quedaba sin
listado de instituciones ni autocompletado.
"""
from __future__ import annotations

import api.routers.admin as A


def _catalogo():
    return A.admin_scraper_catalog(_user="tester")


def test_catalogo_no_revienta_y_trae_todo():
    items = _catalogo()
    assert len(items) > 500                       # catálogo completo (~700)
    primero = items[0]
    for campo in ("id", "nombre", "sector"):
        assert campo in primero, campo


def test_catalogo_trae_sector_para_filtrar_grupos():
    items = _catalogo()
    munis = [i for i in items if "municipal" in (i.get("sector") or "").lower()]
    assert len(munis) > 100                       # el filtro del tipo Municipios
    nombres = {i["nombre"] for i in items}
    assert "Municipalidad de Viña del Mar" in nombres
    assert "Municipalidad de La Cisterna" in nombres


def test_catalogo_trae_clasificacion_para_los_tipos():
    items = _catalogo()
    kinds = {i.get("kind") for i in items if i.get("kind")}
    # Con source_status disponible, los tipos reales del selector están.
    assert {"wordpress", "generic", "empleos_publicos"} <= kinds
