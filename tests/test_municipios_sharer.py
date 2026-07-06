"""Los botones de compartir en redes no deben confundirse con la ficha del
concurso: su href lleva la URL de la oferta en un parámetro y el detalle_re la
matcheaba, haciendo que el scraper pidiera el share (HTTP 400) en vez del detalle.
"""
import scrapers.municipios as M


class _Resp:
    status_code = 200
    text = "<main><h1>Analista</h1><p>Requisitos: título.</p></main>"
    content = b""


class _Sess:
    def get(self, url, **kw):
        return _Resp()


_FUENTE = {"url": "https://renca.cl/trabaja/",
           "detalle_re": r"/proceso-de-seleccion[^/\"']*"}


def test_ignora_boton_facebook_sharer():
    html = ('<a href="https://www.facebook.com/sharer/sharer.php?u=https%3A//renca.cl/proceso-de-seleccion-x/">Compartir</a>'
            '<a href="https://renca.cl/proceso-de-seleccion-analista/">Analista</a>')
    items = M.extraer_enlaces_detalle(html, _FUENTE, _Sess(), delay=0)
    urls = [i["url"] for i in items]
    assert all("facebook" not in u for u in urls)
    assert any("proceso-de-seleccion-analista" in u for u in urls)


def test_ignora_whatsapp_y_twitter():
    html = ('<a href="https://api.whatsapp.com/send?text=https://renca.cl/proceso-de-seleccion-y/">WA</a>'
            '<a href="https://twitter.com/intent/tweet?url=https://renca.cl/proceso-de-seleccion-z/">Tw</a>'
            '<a href="https://renca.cl/proceso-de-seleccion-real/">Real</a>')
    items = M.extraer_enlaces_detalle(html, _FUENTE, _Sess(), delay=0)
    assert len(items) == 1
    assert "proceso-de-seleccion-real" in items[0]["url"]
