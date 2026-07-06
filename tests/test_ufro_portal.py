"""UFRO (extranet.ufro.cl/concursos): recorre administrativos y académicos,
entra a la ficha de cada concurso y arma la oferta con Cargo, Descripción,
Fecha Término (→ cierre), Vacantes, Estado y Requisitos.

Estructura de la ficha reconstruida de una captura real (cargo JARDINERO).
"""
from datetime import date

import pytest

import scrapers.universidades_portal as U

_FUENTE = next(f for f in U.FUENTES if f["clave"] == "ufro")

# Listado administrativo: tabla con enlace a la ficha por fila.
_LISTADO_ADMIN = """
<html><body>
<table>
  <tr><th>Código</th><th>Descripción</th><th>Link</th><th>Tipo</th><th>Estado</th></tr>
  <tr>
    <td>A-123</td>
    <td>JARDINERO - DIVISIÓN DE SERVICIOS</td>
    <td><a href="ver_concurso.php?id=5">Ver</a></td>
    <td>Administrativo</td>
    <td>ABIERTO</td>
  </tr>
</table>
</body></html>
"""

_LISTADO_ACADEMICO = "<html><body><p>No hay Ofertas Laborales Disponibles</p></body></html>"

_DETALLE = """<html><body>
<b>Cargo:</b> JARDINERO -DIVISIÓN DE SERVICIOS
<b>Descripción:</b> Revise atentamente las bases publicadas en https://personas.ufro.cl/servicio/convocatorias/
<b>Vacantes:</b> 1 <b>Fecha Término:</b> 14/07/2026
<b>Estado:</b> ABIERTO
<b>Requisitos:</b> CUMPLIR CON LO ESTABLECIDO EN LOS ARTICULOS 12 Y 13 DE LA LEY N18.834.
<b>Requisitos Específicos:</b> ENSEÑANZA BÁSICA/MEDIA COMPLETA.
<b>Preguntas:</b> Responder aquí.
</body></html>"""

_DETALLE_CERRADO = _DETALLE.replace("<b>Estado:</b> ABIERTO", "<b>Estado:</b> CERRADO")


class _Resp:
    def __init__(self, text):
        self.status_code = 200
        self.text = text
        self.encoding = "utf-8"


def _make_session(detalle=_DETALLE):
    class _Sess:
        def get(self, url, **kw):
            if url.endswith("ver_tipo_administrativo.php"):
                return _Resp(_LISTADO_ADMIN)
            if url.endswith("ver_tipo_academico.php"):
                return _Resp(_LISTADO_ACADEMICO)
            if "ver_concurso.php" in url:
                return _Resp(detalle)
            return _Resp("<html><body></body></html>")
    return _Sess()


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(U.time, "sleep", lambda *_a, **_k: None)


# ── Parseo de la ficha ───────────────────────────────────────────────────────
def test_detalle_extrae_todos_los_campos():
    d = U.parsear_ufro_detalle(_DETALLE)
    assert d["cargo"] == "JARDINERO -DIVISIÓN DE SERVICIOS"
    assert d["fecha_cierre"] == date(2026, 7, 14)
    assert d["vacantes"] == 1
    assert d["estado"] == "ABIERTO"
    assert "CUMPLIR CON LO ESTABLECIDO" in d["requisitos"]
    assert "ENSEÑANZA BÁSICA" in d["requisitos"]


def test_listado_tabla_captura_enlace_a_ficha():
    items = U.parsear_ufro(_LISTADO_ADMIN, _FUENTE)
    assert len(items) == 1
    assert items[0]["url"].endswith("ver_concurso.php?id=5")


def test_listado_vacio_devuelve_lista_vacia():
    assert U.parsear_ufro(_LISTADO_ACADEMICO, _FUENTE) == []


def test_listado_prefiere_enlace_real_sobre_ancla():
    # Fila con una ancla "#" (no navega) y el VER real: se toma el real.
    html = """<html><body><table>
      <tr><th>Código</th><th>Descripción</th><th>Link</th><th>Tipo</th><th>Estado</th></tr>
      <tr><td>C-1</td><td>Analista</td>
          <td><a href="#">detalle</a> <a href="ver_concurso.php?id=7">VER</a></td>
          <td>Administrativo</td><td>ABIERTO</td></tr>
    </table></body></html>"""
    items = U.parsear_ufro(html, _FUENTE)
    assert items[0]["url"].endswith("ver_concurso.php?id=7")


def test_listado_respaldo_por_enlaces():
    # Sin tabla, se toman los enlaces a fichas de concurso.
    html = ('<html><body><ul>'
            '<a href="ver_concurso.php?id=9">Analista de Sistemas</a>'
            '</ul></body></html>')
    items = U.parsear_ufro(html, _FUENTE)
    assert len(items) == 1
    assert items[0]["url"].endswith("ver_concurso.php?id=9")


# ── Flujo completo ───────────────────────────────────────────────────────────
def test_procesar_arma_oferta_completa():
    ofertas = U._procesar_ufro(_FUENTE, _make_session(), incluir_cerrados=False)
    assert len(ofertas) == 1
    o = ofertas[0]
    assert o["cargo"] == "JARDINERO -DIVISIÓN DE SERVICIOS"
    assert o["fecha_cierre"] == date(2026, 7, 14)          # de "Fecha Término"
    assert o["requisitos_texto"] and "ENSEÑANZA" in o["requisitos_texto"]
    assert "personas.ufro.cl" in o["descripcion"]
    assert "_estado" not in o                               # se limpia antes de emitir


def test_procesar_omite_estado_cerrado():
    ofertas = U._procesar_ufro(_FUENTE, _make_session(_DETALLE_CERRADO),
                               incluir_cerrados=False)
    assert ofertas == []


def test_procesar_incluir_cerrados_los_conserva():
    ofertas = U._procesar_ufro(_FUENTE, _make_session(_DETALLE_CERRADO),
                               incluir_cerrados=True)
    assert len(ofertas) == 1
