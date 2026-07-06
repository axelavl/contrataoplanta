"""Municipalidad de Maipú (/empleos-municipales): tabla de cargos vigentes.

La página publica las ofertas en una tabla (CARGO · OFICINA/DEPTO. · RECEPCIÓN
DOCUMENTOS · BASES DE POSTULACIÓN · OBSERVACIONES). Regresión: el cargo viene
partido en <strong> adyacentes (WYSIWYG) y hay una segunda tabla de
"SELECCIONADOS(AS)" que NO son ofertas vigentes y debe descartarse.
"""
from __future__ import annotations

from datetime import date

import scrapers.municipios as M

_FUENTE = {
    "clave": "maipu_em",
    "id": 398,
    "nombre": "Municipalidad de Maipú (empleos)",
    "region": M.RM,
    "ciudad": "Maipú",
    "modo": "tabla_empleos",
    "url": "https://www.municipalidadmaipu.cl/empleos-municipales",
}

# Tabla de ofertas vigentes + tabla de seleccionados (a descartar).
# El cargo "ADMINISTRATIVO" viene partido en <strong> adyacentes.
_HTML = """
<html><body>
<h2>Empleos Municipales</h2>
<table>
  <tr><th>CARGO</th><th>OFICINA/DEPTO.</th><th>RECEPCIÓN DOCUMENTOS</th>
      <th>BASES DE POSTULACIÓN</th><th>OBSERVACIONES</th></tr>
  <tr><td><strong>TERAPEUTA OCUPACIONAL</strong></td>
      <td>PROGRAMA CENTROS DIURNOS COMUNITARIOS</td><td>09/07/2026</td>
      <td><a href="https://media.municipalidadmaipu.cl/media/documentos/2026/7/17831220424644.pdf">AQUÍ</a></td>
      <td>Los(as) interesados(as) deberán enviar la documentación en PDF al correo
          ccisterna@maipu.cl indicando en asunto "Concurso público Terapeuta Ocupacional CDC".</td></tr>
  <tr><td><strong>A</strong><strong>DMI</strong><strong>NISTRATIVO DE FACTURACIÓN</strong></td>
      <td>DIRECCIÓN DE TECNOLOGÍA Y COMUNICACIONES</td><td>03/07/2026</td>
      <td><a href="/media/documentos/2026/6/1782489887.pdf">AQUÍ</a></td>
      <td>Enviar antecedentes en PDF al correo procesoditec@maipu.cl indicando en el asunto:
          "Postulación ADMINISTRATIVO DE FACTURACIÓN".</td></tr>
</table>
<h2>Cargos seleccionados</h2>
<table>
  <tr><th>CARGOS</th><th>SELECCIONADOS (AS)</th></tr>
  <tr><td>TENS Cuidador(a) SCD</td><td>Juana Pérez</td></tr>
  <tr><td>Programa Familias</td><td>Pedro Soto</td></tr>
</table>
</body></html>
"""


def _items():
    return M.extraer_tabla_empleos(_HTML, _FUENTE)


def test_extrae_solo_tabla_de_ofertas_vigentes():
    items = _items()
    # Dos filas de la tabla de ofertas; la de "SELECCIONADOS" se descarta.
    assert len(items) == 2
    cargos = {it["cargo"] for it in items}
    assert "TENS Cuidador(a) SCD" not in cargos


def test_cargo_partido_en_strong_se_reconstruye():
    items = _items()
    cargos = [it["cargo"] for it in items]
    # "<strong>A</strong><strong>DMI</strong>…" → "ADMINISTRATIVO DE FACTURACIÓN"
    assert "ADMINISTRATIVO DE FACTURACIÓN" in cargos
    assert "TERAPEUTA OCUPACIONAL" in cargos


def test_campos_por_fila():
    it = next(i for i in _items() if i["cargo"] == "TERAPEUTA OCUPACIONAL")
    assert it["fecha_cierre"] == date(2026, 7, 9)
    assert it["url_bases"].endswith("/17831220424644.pdf")
    assert it["email_postulacion"] == "ccisterna@maipu.cl"
    # href relativo se resuelve contra la URL de la fuente.
    it2 = next(i for i in _items() if i["cargo"].startswith("ADMINISTRATIVO"))
    assert it2["url_bases"] == (
        "https://www.municipalidadmaipu.cl/media/documentos/2026/6/1782489887.pdf")


def test_url_original_unico_por_cargo():
    urls = [it["url"] for it in _items()]
    assert len(urls) == len(set(urls))          # sin colisiones de dedup
    assert all(u.startswith(_FUENTE["url"] + "#") for u in urls)


def test_construir_oferta_mapea_campos():
    it = next(i for i in _items() if i["cargo"] == "TERAPEUTA OCUPACIONAL")
    o = M.construir_oferta(it, _FUENTE)
    assert o["region"] == "Metropolitana de Santiago"
    assert o["ciudad"] == "Maipú"
    assert o["email_postulacion"] == "ccisterna@maipu.cl"
    assert o["url_bases"].endswith(".pdf")
    assert "PROGRAMA CENTROS DIURNOS" in o["descripcion"]


def test_tabla_sin_columnas_esperadas_no_produce_nada():
    # Una tabla cualquiera (sin recepción/bases) no debe generar ofertas.
    html = ("<table><tr><th>Nombre</th><th>Rut</th></tr>"
            "<tr><td>Juan</td><td>1-9</td></tr></table>")
    assert M.extraer_tabla_empleos(html, _FUENTE) == []
