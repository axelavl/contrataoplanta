"""TVN en Trabajando: empleos.tvn.cl es un wrapper que embebe su bolsa
Trabajando en un <iframe>. El scraper sigue el iframe hasta el portal real y
detecta su idDominio en runtime (auto-detección), sin fijar valores no
observables. TVN también publica en Laborum (id 280): dominios distintos, no
se cierran mutuamente.
"""
from __future__ import annotations

import scrapers.trabajando as T


# ── Registro / filtro ────────────────────────────────────────────────────────
def test_tvn_es_fuente_trabajando_por_flag_plataforma():
    # url_empleo (empleos.tvn.cl) no contiene "trabajando.cl"; se incluye por
    # el flag plataforma="trabajando".
    cands = T._filter_trabajando([], solo_id=280)
    assert [c["id"] for c in cands] == [280]
    tvn = cands[0]
    assert tvn["url_empleo"] == "https://empleos.tvn.cl/"
    assert tvn["plataforma"] == "trabajando"
    assert tvn["sector"] == "Empresa del Estado"
    # No se fija idDominio: se auto-detecta siguiendo el iframe.
    assert "id_dominio" not in tvn


# ── Seguir el iframe del wrapper ─────────────────────────────────────────────
class _Resp:
    def __init__(self, url: str, text: str):
        self.url = url
        self.text = text

    def raise_for_status(self):
        pass


class _SessFake:
    """Session mínima: responde por coincidencia de fragmento en la URL."""
    def __init__(self, rutas):
        self.rutas = rutas          # list[(fragmento, (url_final, html))]
        self.headers: dict = {}

    def get(self, url, **kw):
        for frag, (final, html) in self.rutas:
            if frag in url:
                return _Resp(final, html)
        return _Resp(url, "<html>vacío</html>")


_NUXT = '<html><body><script id="__NUXT_DATA__">[1,2,3]</script></body></html>'
_WRAP_SAME_ORIGIN = '<html><body><iframe src="./jobs.html"></iframe></body></html>'
_WRAP_CROSS = '<html><iframe src="https://tvn.trabajando.cl/trabajo-empleo/"></iframe></html>'


def test_resolver_sigue_iframe_same_origin():
    # empleos.tvn.cl/ (wrapper) → /jobs.html (mismo origen, trae Nuxt).
    sess = _SessFake([
        ("empleos.tvn.cl/jobs.html", ("https://empleos.tvn.cl/jobs.html", _NUXT)),
        ("empleos.tvn.cl", ("https://empleos.tvn.cl/", _WRAP_SAME_ORIGIN)),
    ])
    assert T._resolver_base_efectiva(sess, "https://empleos.tvn.cl") == "https://empleos.tvn.cl"


def test_resolver_sigue_iframe_cross_origin():
    # wrapper apunta a un subdominio *.trabajando.cl con el portal real.
    sess = _SessFake([
        ("tvn.trabajando.cl", ("https://tvn.trabajando.cl/trabajo-empleo/", _NUXT)),
        ("empleos.tvn.cl", ("https://empleos.tvn.cl/", _WRAP_CROSS)),
    ])
    assert T._resolver_base_efectiva(sess, "https://empleos.tvn.cl") == "https://tvn.trabajando.cl"


def test_resolver_portal_directo_sin_wrapper():
    # Portal servido directo (Nuxt en la raíz) → base sin cambios.
    sess = _SessFake([("x.trabajando.cl", ("https://x.trabajando.cl/", _NUXT))])
    assert T._resolver_base_efectiva(sess, "https://x.trabajando.cl") == "https://x.trabajando.cl"


def test_resolver_sin_iframe_ni_nuxt_cae_a_base():
    # Página sin Nuxt y sin iframe → no inventa nada, devuelve base.
    sess = _SessFake([("empleos.tvn.cl", ("https://empleos.tvn.cl/", "<html>nada</html>"))])
    assert T._resolver_base_efectiva(sess, "https://empleos.tvn.cl") == "https://empleos.tvn.cl"


def test_resolver_corta_por_profundidad():
    # Cadena infinita de iframes: corta a los 3 saltos y cae a base (no cuelga).
    bucle = '<html><iframe src="/next"></iframe></html>'
    sess = _SessFake([("empleos.tvn.cl", ("https://empleos.tvn.cl/", bucle))])
    assert T._resolver_base_efectiva(sess, "https://empleos.tvn.cl") == "https://empleos.tvn.cl"


def test_resolver_wrapper_js_sin_iframe_estatico():
    # Wrapper que inyecta el iframe por JavaScript: el HTML servido no trae
    # <iframe>, pero la URL del portal *.trabajando.cl sí aparece en el JS.
    # Antes esto caía a base (sin Nuxt → sin idDominio → fallback HTML que
    # tampoco ve nada) y TVN quedaba en 0 ofertas.
    wrap_js = ('<html><body><div id="app"></div><script>'
               'const config = {portal: "https://tvn.trabajando.cl"};'
               'montarPortalEnIframe(config.portal);'
               '</script></body></html>')
    sess = _SessFake([
        ("tvn.trabajando.cl", ("https://tvn.trabajando.cl/", _NUXT)),
        ("empleos.tvn.cl", ("https://empleos.tvn.cl/", wrap_js)),
    ])
    assert T._resolver_base_efectiva(sess, "https://empleos.tvn.cl") == "https://tvn.trabajando.cl"
