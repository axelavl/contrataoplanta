"""Tests de `_html_a_texto` del scraper Trabajando.cl (BancoEstado, ENAP, etc.).

Foco: que los `<br>` (auto-cerrantes) se conviertan a salto de línea, para
que el perfil/funciones no queden pegados y el parser semántico del frontend
pueda separar secciones. Sin red ni BD.
"""

from __future__ import annotations

from scrapers.trabajando import _html_a_texto


def test_br_se_convierte_en_salto():
    html = "-Conocimiento en monitoreo de sistemas.<br>-Ingeniería en Informática.<br/>-Experiencia."
    out = _html_a_texto(html)
    lineas = [l for l in out.split("\n") if l.strip()]
    assert lineas == [
        "-Conocimiento en monitoreo de sistemas.",
        "-Ingeniería en Informática.",
        "-Experiencia.",
    ]
    # No deben quedar viñetas pegadas.
    assert ".-Ingeniería" not in out


def test_br_con_espacios_y_mayusculas():
    assert _html_a_texto("a<BR />b<BR>c") == "a\nb\nc"


def test_bloques_y_li_siguen_funcionando():
    html = "<p>Funciones Principales</p><ul><li>Monitoreo</li><li>Recepción</li></ul>"
    out = _html_a_texto(html)
    assert "Funciones Principales" in out
    assert "- Monitoreo" in out
    assert "- Recepción" in out


def test_vacio_y_none():
    assert _html_a_texto(None) == ""
    assert _html_a_texto("") == ""
