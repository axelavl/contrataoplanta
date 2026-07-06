"""Cerro Navia: extracción de detalle de ofertas laborales municipales.

El scraper pasó de `lista_o_vacio` (sólo escaneaba headings del índice, baja
calidad → needs_review) a `enlaces_detalle`, entrando a cada página de oferta.
Estos tests fijan la extracción de los campos que el usuario reportó faltantes:
cargo limpio, fecha de cierre (rango "02 al 07 de julio"), correos de
postulación, y que los Formularios 1-A/1-B/1-C NO se tomen como bases.
"""
from datetime import date

import scrapers.municipios as M


# ── Helpers puros ────────────────────────────────────────────────────────────
def test_cierre_rango_sin_anio():
    assert M._cierre_rango("Recepción de antecedentes: 02 al 07 de julio",
                           hoy=date(2026, 7, 1)) == date(2026, 7, 7)


def test_cierre_rango_con_anio():
    assert M._cierre_rango("Postulaciones del 02 al 07 de julio de 2026") == date(2026, 7, 7)


def test_cierre_rango_bare_sin_contexto_no_inventa_fecha():
    # Un rango suelto sin contexto de plazo (p.ej. un horario) NO debe tomarse
    # como cierre: inventar un plazo (peor: futuro) saltaría la revisión.
    assert M._cierre_rango("Atención de lunes 02 al 07 de julio") is None


def test_cierre_rango_envuelve_al_proximo_anio():
    # Aviso de diciembre para un cierre de enero → año siguiente.
    assert M._cierre_rango("recepción 02 al 05 de enero",
                           hoy=date(2026, 12, 20)) == date(2027, 1, 5)


def test_limpiar_cargo_quita_envoltorio():
    assert M._limpiar_cargo(
        "Oferta laboral para proveer el cargo de Administradora de Servidores, "
        "Redes y Soporte TI"
    ) == "Administradora de Servidores, Redes y Soporte TI"


def test_emails_postulacion_dos_correos():
    txt = ("Enviar antecedentes al correo alvaro.pino@cerronavia.cl y "
           "tomas.marticorena@cerronavia.cl indicando en el asunto Reclutamiento")
    assert M._emails_postulacion(txt) == \
        "alvaro.pino@cerronavia.cl, tomas.marticorena@cerronavia.cl"


def test_emails_postulacion_ignora_webmaster():
    assert M._emails_postulacion("Contáctenos en webmaster@cerronavia.cl") is None


# ── Integración: extraer_enlaces_detalle con sesión simulada ─────────────────
_DETALLE_HTML = """
<html><body><main>
  <h1>Oferta laboral para proveer el cargo de Administradora de Servidores, Redes y Soporte TI</h1>
  <p>La Municipalidad de Cerro Navia requiere proveer el cargo indicado.</p>
  <p>Requisitos: título profesional del área informática, experiencia en redes.</p>
  <p>Enviar antecedentes al correo alvaro.pino@cerronavia.cl y
     tomas.marticorena@cerronavia.cl indicando en el asunto o título:
     "Reclutamiento Administrador(a) de Servidores, Redes y Soporte TI".</p>
  <p>Recepción de antecedentes: 02 al 07 de julio.</p>
  <a href="/wp-content/uploads/formulario-1-a-declaracion-jurada.pdf">Formulario 1-A</a>
  <a href="/wp-content/uploads/formulario-1-b-solicitud-antecedentes.pdf">Formulario 1-B</a>
  <a href="/wp-content/uploads/bases-concurso-administrador-ti.pdf">Bases del concurso</a>
</main></body></html>
"""

_LISTADO_HTML = """
<html><body><main>
  <h2>Ofertas laborales</h2>
  <a href="/oferta-laboral-para-proveer-el-cargo-de-administradora-de-servidores-redes-y-soporte-ti/">
    Administradora de Servidores</a>
</main></body></html>
"""

_FUENTE_CN = next(f for f in M.FUENTES if f["clave"] == "cerronavia")


class _FakeResp:
    def __init__(self, text):
        self.status_code = 200
        self.text = text
        self.content = text.encode("utf-8")


class _FakeSession:
    def get(self, url, **kw):
        return _FakeResp(_DETALLE_HTML)


def _extraer():
    return M.extraer_enlaces_detalle(_LISTADO_HTML, _FUENTE_CN, _FakeSession(), delay=0)


def test_detalle_extrae_un_item():
    items = _extraer()
    assert len(items) == 1


def test_detalle_cargo_limpio():
    it = _extraer()[0]
    assert it["cargo"] == "Administradora de Servidores, Redes y Soporte TI"


def test_detalle_fecha_cierre_por_rango():
    it = _extraer()[0]
    esperado = M._cierre_rango("Recepción de antecedentes: 02 al 07 de julio",
                               hoy=date.today())
    assert it["fecha_cierre"] == esperado
    assert esperado.month == 7 and esperado.day == 7


def test_detalle_correos_postulacion():
    it = _extraer()[0]
    assert it["email_postulacion"] == \
        "alvaro.pino@cerronavia.cl, tomas.marticorena@cerronavia.cl"


def test_detalle_bases_excluye_formularios():
    it = _extraer()[0]
    # url_bases apunta a las BASES, no a los Formularios 1-A/1-B.
    assert it["url_bases"].endswith("bases-concurso-administrador-ti.pdf")
    assert "formulario" not in it["url_bases"].lower()


def test_construir_oferta_propaga_correo_y_cierre():
    it = _extraer()[0]
    o = M.construir_oferta(it, _FUENTE_CN)
    assert o["email_postulacion"] == \
        "alvaro.pino@cerronavia.cl, tomas.marticorena@cerronavia.cl"
    assert o["cargo"] == "Administradora de Servidores, Redes y Soporte TI"
    assert o["url_bases"].endswith("bases-concurso-administrador-ti.pdf")
