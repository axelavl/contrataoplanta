"""Municipalidad de Viña del Mar: segunda fuente /concursos-publicos/.

La página publica los concursos de planta como bloques de texto planos
("BASES CONCURSO PÚBLICO PARA PROVEER N CARGO(S) PLANTA <ESTAMENTO> GRADO <G>
<TÍTULO> D.A NNNN") y cada bloque enlaza el PDF de bases nombrado por el número
D.A (NNNN.YY.pdf). Modo concursos_da: una oferta por cargo. Ambas fuentes de
Viña comparten institucion_id 345 → cierre por ausencia agrupado.
"""
from __future__ import annotations

import scrapers.municipios as M

_VINA_CP = next(f for f in M.FUENTES if f["clave"] == "vina_cp")

_HTML = """
<html><body><main>
  <h1>CONCURSOS PÚBLICOS</h1>
  <p>LLAMADO A CONCURSO PÚBLICO D.A. 8001 AL 8003 DEL 2 JULIO 2026</p>
  <p>Anexos <a href="/wp-content/uploads/2026/07/ANEXOS.pdf">Descargar Aquí</a></p>

  <p>BASES CONCURSO PÚBLICO PARA PROVEER 1 CARGO PLANTA PROFESIONAL GRADO 11
     DEPARTAMENTO SERVICIOS DEL AMBIENTE D.A 8001
     <a href="/wp-content/uploads/2026/07/8001.26.pdf">Descargar Aquí</a></p>

  <p>BASES CONCURSO PÚBLICO PARA PROVEER 3 CARGOS PLANTA PROFESIONAL GRADO 11
     ABOGADOS D.A 8003
     <a href="/wp-content/uploads/2026/07/8003.26.pdf">Descargar Aquí</a></p>

  <p>BASES CONCURSO PÚBLICO PARA PROVEER 1 CARGO PLANTA TÉCNICO GRADO 15
     TÉCNICO ABASTECIMIENTO D.A 8011
     <a href="/wp-content/uploads/2026/07/8011.26.pdf">Descargar Aquí</a></p>

  <a href="/wp-content/uploads/2026/04/CP_GESTION2025.pdf">Cuenta Pública</a>
</main></body></html>
"""


def test_fuente_concursos_usa_modo_concursos_da():
    assert _VINA_CP["url"] == "https://www.munivina.cl/concursos-publicos/"
    assert _VINA_CP["id"] == 345
    assert _VINA_CP["modo"] == "concursos_da"
    assert _VINA_CP["nombre"] == "Municipalidad de Viña del Mar"


def test_una_oferta_por_cargo_con_su_pdf():
    items = M.extraer_concursos_da(_HTML, _VINA_CP)
    porcargo = {it["cargo"]: it for it in items}
    assert len(items) == 3          # 3 bloques BASES … D.A (anexos/cuenta pública fuera)
    # El PDF de bases es el nombrado por el número D.A.
    assert porcargo["Profesional Departamento Servicios del Ambiente"]["url_bases"].endswith("/8001.26.pdf")
    assert porcargo["Profesional Abogados"]["numero_vacantes"] == 3
    assert porcargo["Técnico Abastecimiento"]["url_bases"].endswith("/8011.26.pdf")
    # No confunde ANEXOS ni la Cuenta Pública con un cargo.
    assert not any("anexo" in c.lower() or "cuenta" in c.lower() for c in porcargo)


def test_construir_oferta_concursos_da():
    items = M.extraer_concursos_da(_HTML, _VINA_CP)
    o = M.construir_oferta(items[0], _VINA_CP)
    assert o["region"] == "Valparaíso"
    assert o["ciudad"] == "Viña del Mar"
    assert o["numero_vacantes"] == 1
    assert o["url_bases"].endswith(".pdf")
    assert "Grado 11" in o["descripcion"]


def test_dos_fuentes_vina_comparten_id_para_cierre_agrupado():
    vinas = [f for f in M.FUENTES if f["id"] == 345]
    assert {f["clave"] for f in vinas} >= {"vina", "vina_cp"}
    src = __import__("pathlib").Path(M.__file__).read_text(encoding="utf-8")
    assert "urls_por_inst" in src


def test_titulo_cargo_respeta_conectores_y_siglas():
    assert M._titulo_cargo("DEPARTAMENTO SERVICIOS DEL AMBIENTE") == "Departamento Servicios del Ambiente"
    # Siglas cortas (≤4) se conservan en mayúsculas; palabras largas se capitalizan.
    assert M._titulo_cargo("TECNICO EN TI") == "Tecnico en TI"
    assert M._titulo_cargo("PROFESIONAL SECPLA") == "Profesional Secpla"
