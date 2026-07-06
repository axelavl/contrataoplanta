"""UFRO (extranet.ufro.cl/concursos): portal por POST.

La vista carga con POST cod_concar=1/2/3 (Administrativos/Académicos/Post-Doctoral);
cada concurso abre sus cargos (Vacantes, Fecha Término) y cada cargo su ficha
(Descripción, Requisitos), todo por POST. Fixtures replican el HTML real (tablas
Bootstrap con enlaces javascript:ver_consulta(...)).
"""
from datetime import date

import pytest

import scrapers.universidades_portal as U

_FUENTE = next(f for f in U.FUENTES if f["clave"] == "ufro")

# Nivel 1: concursos (ver_tipo_administrativo.php)
_L1 = (
    "<table class='table'><thead><tr>"
    "<th>Código</th><th>Descripción</th><th>Link</th><th>Tipo</th><th>Estado</th>"
    "</tr></thead><tbody>"
    "<tr><td>CON0806</td><td>JARDINERO -DIVISIÓN DE SERVICIOS</td>"
    "<td><a href=\"javascript:ver_consulta('806','CON0806')\">VER</a></td>"
    "<td>INTERNO Y EXTERNO</td><td>ABIERTO</td></tr>"
    "</tbody></table>"
).encode("utf-8")

# Nivel 2: cargos del concurso (ver_cargos_concurso.php)
_L2 = (
    "<table class='table'><thead><tr>"
    "<th>Código</th><th>Nombre</th><th>Link</th><th>Vacantes</th>"
    "<th>Fecha Inicio</th><th>Fecha Término</th>"
    "</tr></thead><tbody>"
    "<tr><td>P321726030</td><td>JARDINERO -DIVISIÓN DE SERVICIOS</td>"
    "<td><a href=\"javascript:ver_consulta('788','806','P321726030','CON0806')\">VER</a></td>"
    "<td>1</td><td>03/07/2026</td><td>14/07/2026</td></tr>"
    "</tbody></table>"
).encode("utf-8")

# Nivel 3: ficha del cargo (ver_postulacion_cargo.php)
_L3 = (
    "<b>Cargo:</b> JARDINERO -DIVISIÓN DE SERVICIOS "
    "<b>Descripción:</b> Revise las bases en https://personas.ufro.cl/servicio/convocatorias/ "
    "<b>Vacantes:</b> 1 <b>Fecha Término:</b> 14/07/2026 "
    "<b>Estado:</b> ABIERTO "
    "<b>Requisitos:</b> CUMPLIR CON LO ESTABLECIDO EN LOS ARTICULOS 12 Y 13. "
    "<b>Requisitos Específicos:</b> ENSEÑANZA BÁSICA/MEDIA COMPLETA."
).encode("utf-8")

_EMPTY = b"<html><body>Sin datos</body></html>"


class _Resp:
    def __init__(self, content):
        self.status_code = 200
        self.content = content
        self.text = content.decode("utf-8", "replace")


class _Sess:
    def post(self, url, data=None, **kw):
        d = data or {}
        if url.endswith("ver_tipo_administrativo.php"):
            return _Resp(_L1 if d.get("Formulario1[cod_concar]") == "1" else _EMPTY)
        if url.endswith("ver_cargos_concurso.php"):
            return _Resp(_L2 if d.get("Formulario[cod_concur]") == "806" else _EMPTY)
        if url.endswith("ver_postulacion_cargo.php"):
            return _Resp(_L3)
        return _Resp(_EMPTY)

    def get(self, url, **kw):
        return _Resp(_EMPTY)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(U.time, "sleep", lambda *_a, **_k: None)


# ── Parsers por nivel ────────────────────────────────────────────────────────
def test_nivel1_concursos():
    conc = U.parsear_ufro_concursos(_L1)
    assert len(conc) == 1
    assert conc[0]["codigo"] == "CON0806"
    assert conc[0]["estado"] == "ABIERTO"
    assert (conc[0]["cod_concur"], conc[0]["id_concur"]) == ("806", "CON0806")


def test_nivel2_cargos():
    cg = U.parsear_ufro_cargos(_L2)[0]
    assert cg["nombre"].startswith("JARDINERO")
    assert cg["vacantes"] == 1
    assert cg["fecha_inicio"] == date(2026, 7, 3)
    assert cg["fecha_termino"] == date(2026, 7, 14)
    assert [cg["cod_concar"], cg["cod_concur"], cg["id_concar"], cg["id_concur"]] == \
        ["788", "806", "P321726030", "CON0806"]


def test_args_ver_consulta():
    assert U._args_ver_consulta("javascript:ver_consulta('806','CON0806')") == ["806", "CON0806"]
    assert U._args_ver_consulta("#") == []


def test_detalle_ficha():
    d = U.parsear_ufro_detalle(_L3)
    assert d["fecha_cierre"] == date(2026, 7, 14)
    assert d["vacantes"] == 1
    assert "ENSEÑANZA BÁSICA" in d["requisitos"]


# ── Flujo completo por POST ──────────────────────────────────────────────────
def test_procesar_flujo_post_completo():
    ofertas = U._procesar_ufro(_FUENTE, _Sess(), incluir_cerrados=False)
    assert len(ofertas) == 1
    o = ofertas[0]
    assert o["cargo"] == "JARDINERO -DIVISIÓN DE SERVICIOS"
    assert o["fecha_cierre"] == date(2026, 7, 14)      # de Fecha Término (nivel 2)
    assert o["numero_vacantes"] == 1
    assert o["requisitos_texto"] and "ENSEÑANZA" in o["requisitos_texto"]
    assert o["fecha_publicacion"] == date(2026, 7, 3)  # Fecha Inicio
    assert "_estado" not in o


def test_procesar_omite_concurso_cerrado():
    l1_cerrado = _L1.replace(b"ABIERTO", b"CERRADO")

    class _S(_Sess):
        def post(self, url, data=None, **kw):
            if url.endswith("ver_tipo_administrativo.php"):
                d = data or {}
                return _Resp(l1_cerrado if d.get("Formulario1[cod_concar]") == "1" else _EMPTY)
            return super().post(url, data=data, **kw)

    assert U._procesar_ufro(_FUENTE, _S(), incluir_cerrados=False) == []
