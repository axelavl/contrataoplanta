from __future__ import annotations

from scrapers.plataformas.trabajando_cl import TrabajandoCLScraper


def _make_scraper(url_empleo: str = "https://demo.trabajando.cl/trabajo-empleo") -> TrabajandoCLScraper:
    inst = {
        "id": 9999,
        "nombre": "Institución Demo",
        "url_empleo": url_empleo,
        "sitio_web": "https://www.demo.gob.cl/trabaja-con-nosotros",
        "sector": "Autónomo",
        "region": "Nacional",
    }
    return TrabajandoCLScraper(fuente_id=9999, institucion=inst)


def test_parse_nuxt_state_extracts_offers_and_total_pages():
    scraper = _make_scraper()
    html = """
    <html><body>
      <script>
        window.__NUXT__={cantidadPaginas: 3, data:[
          1234567,\"Analista de Datos\",\"x\",\"y\",\"Santiago, Región Metropolitana\",\"2026-04-21 10:30\",
          2345678,\"Abogado\",\"x\",\"y\",\"Valparaíso, Región de Valparaíso\",\"2026-04-20 09:00\"
        ]};
      </script>
    </body></html>
    """

    offers, total_pages = scraper._parse_nuxt_state(html, "https://demo.trabajando.cl/trabajo-empleo")

    assert total_pages == 3
    assert len(offers) == 2
    assert offers[0].url == "https://demo.trabajando.cl/oferta/1234567"
    assert offers[0].cargo == "Analista de Datos"
    assert offers[1].url == "https://demo.trabajando.cl/oferta/2345678"


def test_extract_trabajando_url_from_html_uses_link_path_if_present():
    scraper = _make_scraper(url_empleo="https://www.demo.gob.cl/trabaja")
    html = """
    <html><body>
      <a href="https://fiscaliadechile.trabajando.cl/trabajo-empleo">Postula aquí</a>
    </body></html>
    """

    discovered = scraper._extract_trabajando_url_from_html(html, "https://www.demo.gob.cl/trabaja")

    assert discovered == "https://fiscaliadechile.trabajando.cl/trabajo-empleo"


def test_extract_trabajando_url_from_html_adds_default_path_when_missing():
    scraper = _make_scraper(url_empleo="https://www.demo.gob.cl/trabaja")
    html = """
    <html><body>
      <a href="https://fiscaliadechile.trabajando.cl">Portal de empleos</a>
    </body></html>
    """

    discovered = scraper._extract_trabajando_url_from_html(html, "https://www.demo.gob.cl/trabaja")

    assert discovered == "https://fiscaliadechile.trabajando.cl/trabajo-empleo"
