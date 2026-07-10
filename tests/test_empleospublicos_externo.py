"""Regresión del aviso JUNJI atribuido a otra institución y con ficha vacía.

Caso real (jul-2026): el aviso "Encargado(a) de Tecnologías de la Información,
comuna de Punta Arenas" de JUNJI venía con jerarquía terminada en "Dirección
Regional de Magallanes" y postulación en el portal propio de la institución
(junji.myfront.cl). Dos bugs combinados:

1. ``_resolver_institucion`` probaba el segmento más específico primero y el
   matching substring/difuso lo cruzaba con el alias "Dirección Regional de
   Magallanes SNRSJ" del Servicio Nacional de Reinserción Social Juvenil →
   institución equivocada en la tarjeta.
2. La ficha redirigía al portal externo (myfront) y el parser del portal no
   encontraba nada → estamento/jornada/vacantes "Sin datos", y el bloque
   genérico "Cómo postular" mandaba al usuario al "portal de Empleos Públicos"
   cuando la postulación real era en el portal de la institución.
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from scrapers.empleos_publicos import EmpleosPublicosScraper

CATALOGO = [
    {
        "id": 67,
        "nombre": "Junta Nacional de Jardines Infantiles",
        "sigla": "JUNJI",
    },
    {
        "id": 77,
        "nombre": "Servicio Nacional de Reinserción Social Juvenil",
        "sigla": "SENARRSE",
        "aliases": [
            "Dirección Regional de Magallanes SNRSJ",
            "Dirección Regional Metropolitana SNRSJ",
        ],
    },
    {
        "id": 30,
        "nombre": "Servicio de Salud Magallanes",
        "sigla": "SS Magallanes",
    },
]


def _scraper() -> EmpleosPublicosScraper:
    return EmpleosPublicosScraper(instituciones=CATALOGO, dry_run=True)


# ── Resolución de institución con segmentos territoriales genéricos ──────────

def test_direccion_regional_generica_no_cruza_institucion():
    """El segmento "Dirección Regional de Magallanes" (sin sigla) NO debe
    matchear el alias regional de SENARRSE: la institución está en el
    segmento padre (JUNJI)."""
    sc = _scraper()
    inst_id, nombre = sc._resolver_institucion(
        "Ministerio de Educación / Junta Nacional de Jardines Infantiles / "
        "Dirección Regional de Magallanes"
    )
    assert inst_id == 67
    assert nombre == "Junta Nacional de Jardines Infantiles"


def test_direccion_regional_con_sigla_matchea_exacto():
    """Con la sigla incluida, el alias regional sí identifica a la institución."""
    sc = _scraper()
    inst_id, _ = sc._resolver_institucion(
        "Ministerio de Justicia / Servicio Nacional de Reinserción Social Juvenil / "
        "Dirección Regional de Magallanes SNRSJ"
    )
    assert inst_id == 77


def test_direccion_regional_sola_no_inventa_institucion():
    """Una jerarquía que SOLO trae la unidad territorial no debe resolver id
    (es inherentemente ambigua), pero conserva un nombre para mostrar."""
    sc = _scraper()
    inst_id, nombre = sc._resolver_institucion("Dirección Regional de Magallanes")
    assert inst_id is None
    assert nombre == "Dirección Regional de Magallanes"


def test_jerarquia_salud_sigue_resolviendo():
    sc = _scraper()
    inst_id, nombre = sc._resolver_institucion(
        "Ministerio de Salud / Servicio de Salud Magallanes / Hospital Clínico"
    )
    assert inst_id == 30
    assert nombre == "Servicio de Salud Magallanes"


# ── Lugar de desempeño desde la jerarquía ────────────────────────────────────

def test_lugar_desde_jerarquia_rescata_establecimiento():
    """Si la jerarquía baja a un establecimiento más específico que la
    institución resuelta, ese segmento es el lugar de desempeño (aviso real:
    Complejo Hospitalario San José de Maipo bajo el SS Metropolitano Sur
    Oriente, que es lo único que conoce el catálogo)."""
    sc = _scraper()
    lugar = sc._lugar_desde_jerarquia(
        "Ministerio de Salud / Servicio de Salud Metropolitano Sur Oriente / "
        "Complejo Hospitalario San José de Maipo",
        "Servicio de Salud Metropolitano Sur Oriente",
    )
    assert lugar == "Complejo Hospitalario San José de Maipo"


def test_lugar_desde_jerarquia_sin_nivel_extra_devuelve_none():
    """Si la institución resuelta YA es el segmento más específico, no hay
    lugar que rescatar (evita duplicar la institución como lugar)."""
    sc = _scraper()
    assert sc._lugar_desde_jerarquia(
        "Ministerio de Salud / Servicio de Salud Magallanes",
        "Servicio de Salud Magallanes",
    ) is None
    assert sc._lugar_desde_jerarquia("Servicio de Salud Magallanes", None) is None
    assert sc._lugar_desde_jerarquia(None, "Cualquiera") is None


def test_oferta_api_hereda_lugar_desde_jerarquia():
    """El mapeo de la API pobla lugar_desempenio con el establecimiento cuando
    la jerarquía trae un nivel bajo la institución resuelta."""
    sc = _scraper()
    oferta = sc._oferta_desde_api({
        "url": "/pub/convocatorias/convpostularavisoTrabajo.aspx?i=141356",
        "Cargo": "Psicólogo(a) Clínico(a) para Atención Primaria de Salud",
        "Ministerio": "Ministerio de Salud",
        "Institución / Entidad": (
            "Servicio de Salud Magallanes / Hospital Clínico Regional"
        ),
        "Región": "Región de Magallanes",
    })
    assert oferta is not None
    assert oferta["institucion_id"] == 30
    assert oferta["lugar_desempenio"] == "Hospital Clínico Regional"


# ── Ficha en portal externo (myfront) ────────────────────────────────────────

_URL_AVISO = "https://www.empleospublicos.cl/pub/convocatorias/avisopizarronficha.aspx?i=19865"
_URL_MYFRONT = (
    "https://junji.myfront.cl/oferta-de-empleo/19865/"
    "encargadoa-de-tecnologias-de-la-informacion-comuna-de-punta-arenas-r-de-magallanes/"
)

_PAGINA_MYFRONT = """
<html><head>
<script type="application/ld+json">
{
  "@context": "https://schema.org/",
  "@type": "JobPosting",
  "title": "Encargado(a) de Tecnologías de la Información, comuna de Punta Arenas - R. de Magallanes",
  "description": "&lt;p&gt;La Junta Nacional de Jardines Infantiles requiere contratar un Encargado(a) de Tecnologías de la Información, estamento profesional, grado 12° EUS, para la Subdirección de Planificación, Comuna de Punta Arenas, Región de Magallanes.&lt;/p&gt;&lt;p&gt;Vacantes: 01&lt;/p&gt;",
  "employmentType": "FULL_TIME",
  "validThrough": "2026-07-06T23:59:00",
  "hiringOrganization": {"@type": "Organization", "name": "JUNJI"},
  "jobLocation": {"@type": "Place", "address": {"@type": "PostalAddress",
    "streetAddress": "José Menéndez 788", "addressLocality": "Punta Arenas"}}
}
</script></head>
<body>
<h2>Encargado(a) de Tecnologías de la Información</h2>
<p>1 Vacantes</p>
<h3>Descripción</h3>
<p>La Junta Nacional de Jardines Infantiles requiere contratar un Encargado(a)
de Tecnologías de la Información, estamento profesional, grado 12° EUS.</p>
<p><strong>Correo de consultas:</strong> rnahuelquin@junji.cl</p>
</body></html>
"""


def _oferta_api() -> dict:
    return {
        "url_oferta": _URL_AVISO,
        "url_bases": _URL_AVISO,
        "cargo": "Encargado(a) de Tecnologías de la Información, comuna de Punta Arenas - R. de Magallanes",
        "institucion_id": 67,
        "institucion_nombre": "Junta Nacional de Jardines Infantiles",
        "descripcion": None,
        "jornada": None,
        "numero_vacantes": None,
        "estamento": None,
        "grado_eus": None,
        "fecha_cierre": None,
    }


def test_detalle_externo_extrae_jsonld_y_regex():
    sc = _scraper()
    soup = BeautifulSoup(_PAGINA_MYFRONT, "html.parser")
    resultado = sc._parsear_detalle_externo(soup, _oferta_api(), _URL_MYFRONT)

    assert resultado["numero_vacantes"] == 1
    assert resultado["jornada"] == "jornada completa"
    assert resultado["estamento"] == "Profesional"
    assert resultado["grado_eus"] == "EUS-12"
    assert resultado["email_consultas"] == "rnahuelquin@junji.cl"
    assert resultado["fecha_cierre"] is not None
    assert resultado["lugar_desempenio"] == "José Menéndez 788, Punta Arenas"
    assert "requiere contratar" in (resultado["descripcion"] or "")
    assert resultado["url_bases"] == _URL_MYFRONT


def test_detalle_externo_no_pisa_institucion_de_la_api():
    """La página externa no controla la institución: aunque traiga encabezados
    ajenos, se conserva lo que resolvió la API."""
    sc = _scraper()
    html = _PAGINA_MYFRONT.replace(
        "<h2>Encargado(a) de Tecnologías de la Información</h2>",
        "<h3>Institución</h3><p>Servicio Nacional de Reinserción Social Juvenil</p>",
    )
    soup = BeautifulSoup(html, "html.parser")
    resultado = sc._parsear_detalle_externo(soup, _oferta_api(), _URL_MYFRONT)
    assert resultado["institucion_id"] == 67
    assert resultado["institucion_nombre"] == "Junta Nacional de Jardines Infantiles"


def test_detalle_externo_como_postular_no_menciona_portal_ep():
    sc = _scraper()
    soup = BeautifulSoup(_PAGINA_MYFRONT, "html.parser")
    resultado = sc._parsear_detalle_externo(soup, _oferta_api(), _URL_MYFRONT)
    desc = resultado["descripcion"] or ""
    assert "Cómo postular:" in desc
    assert "portal de Empleos Públicos" not in desc
    assert "portal de postulación de la institución" in desc


def test_detalle_externo_sin_jsonld_usa_encabezados_y_regex():
    sc = _scraper()
    html = """
    <html><body>
    <h3>Descripción</h3>
    <p>Se requiere profesional, estamento técnico. Vacantes: 2.
    Jornada completa. Correo de consultas: rrhh@institucion.cl</p>
    </body></html>
    """
    soup = BeautifulSoup(html, "html.parser")
    resultado = sc._parsear_detalle_externo(soup, _oferta_api(), _URL_MYFRONT)
    assert resultado["numero_vacantes"] == 2
    assert resultado["estamento"] == "Técnico"
    assert resultado["jornada"] == "jornada completa"
    assert resultado["email_consultas"] == "rrhh@institucion.cl"
    assert "Se requiere profesional" in resultado["descripcion"]


# ── Clasificación de URLs del portal ─────────────────────────────────────────

def test_es_url_del_portal():
    sc = _scraper()
    assert sc._es_url_del_portal("https://www.empleospublicos.cl/pub/x.aspx")
    assert sc._es_url_del_portal("https://empleospublicos.cl/apiConvocatorias.ashx")
    assert not sc._es_url_del_portal(_URL_MYFRONT)


def test_es_home_del_portal_detecta_redirect_al_spa():
    sc = _scraper()
    assert sc._es_home_del_portal("https://www.empleospublicos.cl/", _URL_AVISO)
    assert not sc._es_home_del_portal(_URL_AVISO, _URL_AVISO)


def test_ficha_portal_no_puede_cambiar_institucion_de_la_api():
    """Si el detalle (p.ej. tras un redirect) resuelve una institución DISTINTA
    a la de la API, se conserva la de la API."""
    sc = _scraper()
    html = """
    <html><body>
    <h3>Institución</h3><p>Servicio Nacional de Reinserción Social Juvenil /</p>
    <h3>Convocatoria</h3><p>Otro cargo cualquiera</p>
    </body></html>
    """
    soup = BeautifulSoup(html, "html.parser")
    resultado = sc._parsear_detalle(soup, _oferta_api())
    assert resultado["institucion_id"] == 67
