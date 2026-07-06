"""Municipalidad de Viña del Mar: segunda fuente /concursos-publicos/.

Viña ya se scrapeaba en /ofertas-laborales/ (modo pdf_links). Se agrega
/concursos-publicos/ (misma muni, mismo CMS/host) en el mismo modo: la página
lista los concursos como enlaces a PDF de bases. Ambas fuentes comparten
institucion_id 345 → el cierre por ausencia se agrupa (no se cierran entre sí).
"""
from __future__ import annotations

import scrapers.municipios as M

_VINA_CP = next(f for f in M.FUENTES if f["clave"] == "vina_cp")


class _RespVacia:
    status_code = 404
    content = b""
    text = ""


class _SessionSinPdf:
    """No baja PDFs (para probar la extracción de enlaces sin red)."""
    def get(self, *a, **k):
        return _RespVacia()


def test_fuente_concursos_registrada():
    assert _VINA_CP["url"] == "https://www.munivina.cl/concursos-publicos/"
    assert _VINA_CP["id"] == 345
    assert _VINA_CP["modo"] == "pdf_links"
    assert _VINA_CP["pdf_host"] == "munivina.cl"
    # Mismo nombre canónico que la fuente principal (display sin sufijo).
    assert _VINA_CP["nombre"] == "Municipalidad de Viña del Mar"


def test_dos_fuentes_vina_comparten_id_para_cierre_agrupado():
    vinas = [f for f in M.FUENTES if f["id"] == 345]
    assert {f["clave"] for f in vinas} >= {"vina", "vina_cp"}
    # El cierre agrupado por institución vive en municipios.py.
    src = __import__("pathlib").Path(M.__file__).read_text(encoding="utf-8")
    assert "urls_por_inst" in src


_HTML_CONCURSOS = """
<html><body><main>
  <h1>Concursos Públicos</h1>
  <ul>
    <li><a href="/wp-content/uploads/2026/07/Bases-Concurso-Profesional-Informatica.pdf">Bases</a></li>
    <li><a href="/wp-content/uploads/2026/07/Perfil-Tecnico-en-Enfermeria.pdf">Descargar</a></li>
    <li><a href="/wp-content/uploads/2026/01/Cuenta-Publica-2025.pdf">Cuenta Pública 2025</a></li>
    <li><a href="https://otrositio.cl/algo.pdf">PDF de otro host</a></li>
  </ul>
</main></body></html>
"""


def test_extrae_concursos_como_pdf_de_bases_y_filtra_institucionales():
    items = M.extraer_pdf_links(_HTML_CONCURSOS, _VINA_CP, _SessionSinPdf(), 0)
    urls = {it["url"] for it in items}
    # Los dos PDF de bases/perfil se recogen (host propio, señal de oferta).
    assert any("Profesional-Informatica" in u for u in urls)
    assert any("Tecnico-en-Enfermeria" in u for u in urls)
    # La cuenta pública (documento institucional) se descarta.
    assert not any("Cuenta-Publica" in u for u in urls)
    # PDF de otro host se ignora (pdf_host = munivina.cl).
    assert not any("otrositio.cl" in u for u in urls)
    # El cargo sale del nombre de archivo (cargo_desde_archivo).
    cargos = {it["cargo"] for it in items}
    assert any("Informatica" in c or "Informática" in c for c in cargos)


def test_construir_oferta_vina_concursos():
    items = M.extraer_pdf_links(_HTML_CONCURSOS, _VINA_CP, _SessionSinPdf(), 0)
    o = M.construir_oferta(items[0], _VINA_CP)
    assert o["institucion_nombre"] == "Municipalidad de Viña del Mar"
    assert o["region"] == "Valparaíso"
    assert o["ciudad"] == "Viña del Mar"
    # url_bases apunta al PDF (el .pdf es tanto url_original como bases).
    assert o["url_original"].endswith(".pdf")
