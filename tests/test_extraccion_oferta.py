"""Tests del servicio de extracción de ofertas desde URL/PDF para el panel
(`api/services/extraccion_oferta.py`, endpoint `POST /ofertas/extraer`).

Sin red ni BD: el fetch se prueba con validaciones locales y la lectura de
PDF se simula monkeypatcheando `scrapers.bases_pdf.leer_pdf`.
"""
from __future__ import annotations

import pytest

from api.services import extraccion_oferta as E


# ── Texto realista de una convocatoria (estilo perfil de cargo municipal) ────
TEXTO_OFERTA = """
Municipalidad de Independencia requiere contratar "Profesional Apoyo Jurídico"
(Dirección de Asesoría Jurídica).

Nombre del cargo: Profesional Apoyo Jurídico
Modalidad: Honorarios
Jornada: 44 horas semanales
N° de vacantes: 2
Renta bruta mensual: $1.850.000.-

REQUISITOS MÍNIMOS
Título profesional de Abogado otorgado por una universidad del Estado.
Experiencia mínima de 2 años en el sector público.

FUNCIONES PRINCIPALES
Redacción de informes jurídicos y apoyo en sumarios administrativos.

Cierre de postulación: 25 de agosto de 2026
Postulaciones al correo postulaciones@independencia.cl
Consultas a consultas@independencia.cl
Región Metropolitana
"""


# ══════════════════════════════════════════════════════════════════════════════
# Detección de PDF y utilitarios
# ══════════════════════════════════════════════════════════════════════════════
def test_es_pdf_por_magic_bytes():
    assert E.es_pdf(b"%PDF-1.7 blah")
    assert not E.es_pdf(b"<html><body>hola</body></html>")


def test_es_pdf_por_content_type_y_nombre():
    assert E.es_pdf(b"", "application/pdf; charset=binary")
    assert E.es_pdf(b"", "", "bases_concurso.PDF")
    assert not E.es_pdf(b"", "text/html", "oferta.html")


def test_renta_desde_texto():
    assert E._renta_desde_texto("Renta bruta: $1.850.000.-") == 1_850_000
    assert E._renta_desde_texto("renta 900000 mensual") == 900_000
    assert E._renta_desde_texto("Ley 18.695") is None      # fuera de rango
    assert E._renta_desde_texto(None) is None


def test_mapear_estamento():
    assert E._mapear_estamento("Profesionales") == "Profesional"
    assert E._mapear_estamento("Técnicos") == "Técnico"
    assert E._mapear_estamento("otra cosa") is None


# ══════════════════════════════════════════════════════════════════════════════
# HTML → texto estructurado
# ══════════════════════════════════════════════════════════════════════════════
HTML = """
<html><head><title>Concurso Profesional — Municipalidad Demo</title>
<script>var x=1;</script></head>
<body>
  <nav>menú que no debe salir</nav>
  <h1>Llamado a concurso público</h1>
  <table><tr><th>Cargo</th><td>Analista</td></tr><tr><th>Grado</th><td>12</td></tr></table>
  <p>Postulaciones hasta el 30/09/2026.</p>
  <a href="/docs/bases_concurso.pdf">Descargar bases</a>
  <a href="otro.html">otra página</a>
</body></html>
"""


def test_extraer_texto_html():
    pagina = E.extraer_texto_html(HTML, base_url="https://demo.cl/empleos/")
    assert pagina["titulo"].startswith("Concurso Profesional")
    assert "Llamado a concurso público" in pagina["headings"]
    assert any("Cargo | Analista" in t for t in pagina["tablas"])
    assert pagina["pdf_links"] == ["https://demo.cl/docs/bases_concurso.pdf"]
    assert "menú que no debe salir" not in pagina["texto"]
    assert "var x=1" not in pagina["texto"]


# ══════════════════════════════════════════════════════════════════════════════
# Texto → campos del formulario
# ══════════════════════════════════════════════════════════════════════════════
def test_extraer_campos_oferta_completo():
    campos, advertencias = E.extraer_campos_oferta(
        TEXTO_OFERTA, url="https://www.independencia.cl/oferta")
    assert campos["cargo"] == "Profesional Apoyo Jurídico"
    assert "Independencia" in campos["institucion_nombre"]
    assert campos["fecha_cierre"] == "2026-08-25"
    assert campos["tipo_contrato"] == "Honorarios"
    assert campos["renta_bruta_min"] == 1_850_000
    assert campos["renta_bruta_max"] == 1_850_000
    assert campos["numero_vacantes"] == 2
    assert campos["email_postulacion"] == "postulaciones@independencia.cl"
    assert campos["email_consultas"] == "consultas@independencia.cl"
    assert campos["url_oferta"] == "https://www.independencia.cl/oferta"
    assert campos.get("region") == "Metropolitana de Santiago"
    # requisitos y descripción capturados desde las secciones
    assert "Abogado" in (campos.get("requisitos") or "")
    # "Independencia" NO debe disparar el patrón "Dependencia:" del perfil
    assert campos.get("lugar_desempenio") != "requiere contratar"


def test_extraer_campos_sin_datos_avisa():
    campos, advertencias = E.extraer_campos_oferta("Página sin contenido laboral alguno.")
    assert "No se detectó fecha de cierre" in advertencias
    assert any("institución" in a for a in advertencias)


def test_extraer_campos_url_pdf_va_a_url_bases():
    campos, _ = E.extraer_campos_oferta(
        TEXTO_OFERTA, url="https://demo.cl/bases_perfil.pdf")
    assert campos["url_oferta"] == "https://demo.cl/bases_perfil.pdf"
    assert campos["url_bases"] == "https://demo.cl/bases_perfil.pdf"


def test_extraer_campos_pdf_links_relevantes():
    campos, _ = E.extraer_campos_oferta(
        TEXTO_OFERTA,
        url="https://demo.cl/empleos",
        pdf_links=["https://demo.cl/otro.pdf", "https://demo.cl/bases_2026.pdf"],
    )
    # El primero no matchea (bases|concurso|convocatoria|perfil|tdr); el segundo sí.
    assert campos["url_bases"] == "https://demo.cl/bases_2026.pdf"


# ══════════════════════════════════════════════════════════════════════════════
# extraer_desde_contenido: rutas PDF (con y sin OCR) y HTML
# ══════════════════════════════════════════════════════════════════════════════
def test_desde_contenido_pdf_con_ocr(monkeypatch):
    from scrapers import bases_pdf
    monkeypatch.setattr(bases_pdf, "leer_pdf",
                        lambda contenido, allow_ocr=True, **kw: (TEXTO_OFERTA, "ocr"))
    res = E.extraer_desde_contenido(b"%PDF-1.4 fake", "application/pdf",
                                    nombre_archivo="perfil_escaneado.pdf")
    assert res["meta"]["tipo"] == "pdf"
    assert res["meta"]["usado_ocr"] is True
    assert any("OCR" in a for a in res["meta"]["advertencias"])
    assert res["campos"]["cargo"] == "Profesional Apoyo Jurídico"
    assert res["texto"].strip().startswith("Municipalidad de Independencia")


def test_desde_contenido_pdf_ilegible(monkeypatch):
    from scrapers import bases_pdf
    monkeypatch.setattr(bases_pdf, "leer_pdf",
                        lambda contenido, allow_ocr=True, **kw: ("", "sin_texto"))
    with pytest.raises(E.ExtraccionError, match="No se pudo extraer texto"):
        E.extraer_desde_contenido(b"%PDF-1.4 fake", "application/pdf")


def test_desde_contenido_html():
    html = f"<html><head><title>Oferta</title></head><body><main>{TEXTO_OFERTA}</main></body></html>"
    res = E.extraer_desde_contenido(html.encode(), "text/html; charset=utf-8",
                                    url="https://www.independencia.cl/oferta")
    assert res["meta"]["tipo"] == "html"
    assert res["meta"]["usado_ocr"] is False
    assert res["campos"]["cargo"] == "Profesional Apoyo Jurídico"
    assert res["campos"]["fecha_cierre"] == "2026-08-25"


def test_desde_contenido_html_vacio_error():
    with pytest.raises(E.ExtraccionError, match="casi no tiene texto"):
        E.extraer_desde_contenido(b"<html><body><p>hola</p></body></html>", "text/html")


# ══════════════════════════════════════════════════════════════════════════════
# descargar_recurso: validaciones locales (sin red)
# ══════════════════════════════════════════════════════════════════════════════
def test_descargar_rechaza_esquema_invalido():
    with pytest.raises(E.ExtraccionError, match="http"):
        E.descargar_recurso("ftp://ejemplo.cl/bases.pdf")
    with pytest.raises(E.ExtraccionError, match="http"):
        E.descargar_recurso("no-es-url")


def test_descargar_rechaza_host_privado(monkeypatch):
    monkeypatch.setattr(E.socket, "getaddrinfo",
                        lambda host, port: [(2, 1, 6, "", ("127.0.0.1", 0))])
    with pytest.raises(E.ExtraccionError, match="interna/privada"):
        E.descargar_recurso("https://interno.ejemplo.cl/oferta")
