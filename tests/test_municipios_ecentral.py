"""Estación Central (ecentral.cl/bolsa-de-empleo-2026): modo `headings`.

Los cargos se publican como encabezados (h2/h3) con su bloque de detalle. Este
test fija que el extractor de headings recoge, además del cargo, el plazo de
recepción (rango), el correo de postulación y la renta del bloque — el mismo
estándar que enlaces_detalle (Cerro Navia).
"""
from __future__ import annotations

from datetime import date

import scrapers.municipios as M

_FUENTE = next(f for f in M.FUENTES if f["clave"] == "ecentral")

# Página estilo "bolsa de empleo": cada cargo es un heading con su <section>.
_HTML = """
<html><body>
<h1>Bolsa de Empleo 2026</h1>

<section>
  <h2>Profesional de Apoyo Social</h2>
  <p>Perfil: profesional del área social para la Dirección de Desarrollo Comunitario.</p>
  <p>Remuneración bruta: $1.200.000.-</p>
  <p>Recepción de antecedentes: 01 al 09 de julio de 2026.</p>
  <p>Enviar antecedentes al correo reclutamiento@ecentral.cl indicando el cargo.</p>
</section>

<section>
  <h3>Técnico en Enfermería de Nivel Superior</h3>
  <p>Para el Departamento de Salud. Contrato a honorarios.</p>
  <p>Los interesados deben remitir sus antecedentes al correo salud@ecentral.cl.</p>
</section>

<section>
  <h2>Requisitos generales</h2>
  <p>No es un cargo — encabezado de sección que debe ignorarse.</p>
</section>
</body></html>
"""


def _items():
    return M.extraer_headings(_HTML, _FUENTE)


def test_detecta_cargos_y_descarta_secciones():
    cargos = [it["cargo"] for it in _items()]
    assert "Profesional de Apoyo Social" in cargos
    assert "Técnico en Enfermería de Nivel Superior" in cargos
    # "Requisitos generales" no es un cargo.
    assert not any("requisitos" in c.lower() for c in cargos)


def test_headings_extrae_plazo_correo_y_renta():
    it = next(i for i in _items() if i["cargo"] == "Profesional de Apoyo Social")
    assert it["fecha_cierre"] == date(2026, 7, 9)          # último día del rango
    assert it["email_postulacion"] == "reclutamiento@ecentral.cl"
    assert it["renta_bruta_min"] == 1_200_000


def test_headings_correo_sin_renta_ni_rango():
    it = next(i for i in _items()
              if i["cargo"].startswith("Técnico en Enfermería"))
    assert it["email_postulacion"] == "salud@ecentral.cl"
    assert it["renta_bruta_min"] is None                    # no publica monto
    assert it["tipo"] == "Honorarios"


def test_construir_oferta_desde_headings():
    it = next(i for i in _items() if i["cargo"] == "Profesional de Apoyo Social")
    o = M.construir_oferta(it, _FUENTE)
    assert o["ciudad"] == "Estación Central"
    assert o["region"] == "Metropolitana de Santiago"
    assert o["renta_bruta_min"] == 1_200_000
    assert o["email_postulacion"] == "reclutamiento@ecentral.cl"
