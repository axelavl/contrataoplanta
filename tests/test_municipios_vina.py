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


def test_url_original_es_la_pagina_de_concursos_no_el_pdf():
    # "Ir al portal de postulación" debe llevar al SITIO donde está el llamado
    # (la página de concursos, con ancla por D.A); el PDF va sólo en url_bases
    # ("Ver bases"). Antes url = PDF y el portal abría el decreto directamente.
    items = M.extraer_concursos_da(_HTML, _VINA_CP)
    for it in items:
        assert it["url"].startswith(_VINA_CP["url"] + "#da-")
        assert not it["url"].lower().split("#")[0].endswith(".pdf")
        assert it["url_bases"].endswith(".pdf")
    # construir_oferta preserva la separación página/PDF.
    o = M.construir_oferta(items[0], _VINA_CP)
    assert o["url_original"].startswith(_VINA_CP["url"] + "#da-")
    assert o["url_bases"].endswith(".pdf")


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


# ── Formato ANTIGUO de Viña (2024): "CARGO A DESEMPEÑAR: X, SEGÚN D.A. NNNN" ──
# La página acumula concursos de varios años; el formato viejo producía títulos
# basura ("…, Según") y el PDF (7738.pdf, sin sufijo de año) no se asociaba.
_HTML_2024 = """
<html><body><main>
  <p>BASES CONCURSO PÚBLICO PARA PROVEER 1 CARGO PLANTA PROFESIONALES GRADO 11°
     E.M., PARA MAESTRANZA MUNICIPAL, CARGO A DESEMPEÑAR: PROFESIONAL
     MAESTRANZA, SEGÚN D.A. 7738.
     <a href="/wp-content/uploads/2024/06/7738.pdf">Descargar Aquí</a></p>
  <p>BASES CONCURSO PÚBLICO PARA PROVEER 4 CARGOS PLANTA AUXILIAR GRADO 15,
     CARGO A DESEMPEÑAR: AUXILIARES ASEO RODOVIARIO, SEGÚN DA 11187.
     <a href="/wp-content/uploads/2025/01/11187.pdf">Descargar Aquí</a></p>
  <p>BASES CONCURSO PÚBLICO PARA PROVEER 2 CARGOS PLANTA TÉCNICO GRADO 15,
     CARGO A DESEMPEÑAR: TÉCNICO SIN PDF, SEGÚN DA 99999.</p>
</main></body></html>
"""


def test_formato_2024_cargo_limpio_y_pdf_asociado():
    items = M.extraer_concursos_da(_HTML_2024, _VINA_CP)
    por = {it["cargo"]: it for it in items}
    # El cargo real sale de "CARGO A DESEMPEÑAR", sin la cola ", Según".
    assert "Profesional Maestranza" in por
    assert por["Profesional Maestranza"]["url_bases"].endswith("/7738.pdf")
    assert por["Profesional Maestranza"]["numero_vacantes"] == 1
    assert "Auxiliares Aseo Rodoviario" in por
    assert por["Auxiliares Aseo Rodoviario"]["url_bases"].endswith("/11187.pdf")
    assert not any("según" in c.lower() for c in por)


def test_bloque_sin_pdf_no_inventa_asociacion():
    items = M.extraer_concursos_da(_HTML_2024, _VINA_CP)
    sin = next(it for it in items if it["url_bases"] is None)
    assert sin["url_bases"] is None          # sin bases → revisar manualmente
    assert sin["url"].endswith("#da-99999")  # url única igual (dedup estable)


# ── Vigencia desde el PDF de bases (bases_enriquecer) ────────────────────────
_DECRETO_2024 = """
VIÑA DEL MAR, 04 JUN. 2024
N° 7738 / VISTOS: ...
1. CARGO A CONCURSAR:
PLANTA GRADO VACANTES CARGO A DESEMPEÑAR
Profesionales 11° 1 Profesional Maestranza
8. CRONOGRAMA CONCURSO
Cierre de recepción de postulaciones 17 de Junio de 2024
Resolución del concurso 30 de Julio de 2024
"""


class _SesionPDF:
    def get(self, url, **kw):
        class R:
            status_code = 200
            content = b"%PDF-fake"
            text = ""
        return R()


def test_filtrar_items_omite_vencidas_por_cronograma(monkeypatch, tmp_path):
    # El PDF dice cierre 2024-06-17 → aunque la tarjeta no traiga plazo, la
    # oferta NO debe publicarse (era el caso "Nueva · hace menos de 1 día").
    import scrapers.bases_pdf as B
    monkeypatch.setattr(M.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(M, "_BASES_CACHE_PATH", tmp_path / "cache.json")
    monkeypatch.setattr(B, "leer_pdf", lambda contenido, **kw: (_DECRETO_2024, "ocr"))
    items = M.extraer_concursos_da(_HTML_2024, _VINA_CP)
    maestranza = [it for it in items if it["cargo"] == "Profesional Maestranza"]
    ofertas = M._filtrar_items(maestranza, _VINA_CP, _SesionPDF(), 0, False)
    assert ofertas == []                     # vencida por cronograma del PDF


def test_filtrar_items_enriquece_vigentes(monkeypatch, tmp_path):
    import scrapers.bases_pdf as B
    decreto_vigente = _DECRETO_2024.replace("17 de Junio de 2024", "17 de Junio de 2099") \
                                   .replace("30 de Julio de 2024", "30 de Julio de 2099")
    monkeypatch.setattr(M.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(M, "_BASES_CACHE_PATH", tmp_path / "cache.json")
    monkeypatch.setattr(B, "leer_pdf", lambda contenido, **kw: (decreto_vigente, "ocr"))
    items = M.extraer_concursos_da(_HTML_2024, _VINA_CP)
    maestranza = [it for it in items if it["cargo"] == "Profesional Maestranza"]
    ofertas = M._filtrar_items(maestranza, _VINA_CP, _SesionPDF(), 0, False)
    assert len(ofertas) == 1
    o = ofertas[0]
    from datetime import date as _d
    assert o["fecha_cierre"] == _d(2099, 6, 17)          # cronograma del PDF
    assert "Grado 11" in o["descripcion"]


# ── Caché persistente del texto OCR (logs/bases_pdf_cache.json) ─────────────
def test_cache_persistente_evita_reocr(monkeypatch, tmp_path):
    # 1ª corrida: OCR una vez y guarda; 2ª corrida: ni descarga ni re-OCR-ea.
    import scrapers.bases_pdf as B
    monkeypatch.setattr(M.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(M, "_BASES_CACHE_PATH", tmp_path / "cache.json")
    llamadas = {"leer": 0, "get": 0}

    def _leer(contenido, **kw):
        llamadas["leer"] += 1
        return _DECRETO_2024, "ocr"
    monkeypatch.setattr(B, "leer_pdf", _leer)

    class _Sesion(_SesionPDF):
        def get(self, url, **kw):
            llamadas["get"] += 1
            return super().get(url, **kw)

    items = M.extraer_concursos_da(_HTML_2024, _VINA_CP)
    maestranza = [it for it in items if it["cargo"] == "Profesional Maestranza"]
    M._filtrar_items(maestranza, _VINA_CP, _Sesion(), 0, False)
    assert llamadas == {"leer": 1, "get": 1}
    M._filtrar_items(maestranza, _VINA_CP, _Sesion(), 0, False)
    assert llamadas == {"leer": 1, "get": 1}     # 2ª corrida: todo desde caché


def test_cache_persistente_no_guarda_sin_texto_y_expira(monkeypatch, tmp_path):
    import json
    from datetime import date, timedelta
    ruta = tmp_path / "cache.json"
    monkeypatch.setattr(M, "_BASES_CACHE_PATH", ruta)
    # sin_texto NO se persiste: apenas se instale tesseract debe reintentarse.
    M._bases_cache_guardar({"https://x/1.pdf": ("", "sin_texto"),
                            "https://x/2.pdf": ("texto útil", "ocr")})
    assert set(json.loads(ruta.read_text(encoding="utf-8"))) == {"https://x/2.pdf"}
    assert M._bases_cache_cargar() == {"https://x/2.pdf": ("texto útil", "ocr")}
    # Entrada vieja (> TTL) expira al cargar.
    vieja = (date.today() - timedelta(days=M._BASES_CACHE_TTL_DIAS + 1)).isoformat()
    ruta.write_text(json.dumps({"https://x/3.pdf": {
        "texto": "t", "metodo": "ocr", "fecha": vieja}}), encoding="utf-8")
    assert M._bases_cache_cargar() == {}


def test_titulo_cargo_respeta_conectores_y_siglas():
    assert M._titulo_cargo("DEPARTAMENTO SERVICIOS DEL AMBIENTE") == "Departamento Servicios del Ambiente"
    # Siglas cortas (≤4) se conservan en mayúsculas; palabras largas se capitalizan.
    assert M._titulo_cargo("TECNICO EN TI") == "Tecnico en TI"
    assert M._titulo_cargo("PROFESIONAL SECPLA") == "Profesional Secpla"
