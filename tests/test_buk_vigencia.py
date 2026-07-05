"""Tests de vigencia del scraper Buk: procesos cerrados y fechas de cierre.

Buk deja avisos ya finalizados en el listado /trabaja-con-nosotros; el detalle
publica un aviso ("proceso cerrado") o, cuando existe, una fecha límite. Estos
tests fijan que `parsear_detalle` detecte ambos casos para que la recolección
pueda omitir las ofertas vencidas.
"""
from datetime import date

from scrapers.buk import _parse_fecha_cierre, construir_oferta, parsear_detalle


def _html(cuerpo: str) -> str:
    return f"<html><body>{cuerpo}</body></html>"


def test_detecta_proceso_cerrado():
    html = _html(
        "<h1>Analista de Presupuesto</h1>"
        "<p>Este proceso de selección ya se encuentra cerrado.</p>"
        "<p>Descripción Apoyar la gestión financiera de la unidad.</p>"
    )
    det = parsear_detalle(html)
    assert det.get("cerrada") is True


def test_detecta_postulaciones_cerradas():
    html = _html("<div>Las postulaciones cerradas para esta vacante.</div>")
    assert parsear_detalle(html).get("cerrada") is True


def test_oferta_vigente_no_marca_cerrada():
    html = _html(
        "<h2>Contador</h2>"
        "<p>Jornada Completa Vacantes 1</p>"
        "<p>Descripción Llevar la contabilidad general del organismo.</p>"
    )
    det = parsear_detalle(html)
    assert not det.get("cerrada")


def test_no_falso_positivo_por_recinto_cerrado():
    # "recinto cerrado" no debe interpretarse como proceso cerrado.
    html = _html("<p>Descripción Custodia de un recinto cerrado con acceso controlado.</p>")
    det = parsear_detalle(html)
    assert not det.get("cerrada")


def test_parse_fecha_cierre_barra():
    assert _parse_fecha_cierre("Postula hasta el 20/08/2026") == date(2026, 8, 20)


def test_parse_fecha_cierre_texto():
    assert _parse_fecha_cierre("Fecha de cierre: 5 de septiembre de 2026") == date(2026, 9, 5)


def test_parse_fecha_cierre_ausente():
    assert _parse_fecha_cierre("Vacantes 2 Jornada Completa") is None


def test_construir_oferta_propaga_fecha_cierre():
    item = {
        "titulo": "Ingeniero", "url": "https://x.buk.cl/s/abc", "ubicacion": "Santiago, Metropolitana",
        "renta_min": None, "renta_max": None, "renta_texto": None,
        "fecha_publicacion": date(2026, 7, 1),
    }
    det = {"descripcion": "Diseñar y ejecutar proyectos de infraestructura vial regional.",
           "fecha_cierre": date(2026, 8, 15)}
    fuente = {"id": 294, "nombre": "SASIPA", "sector": "Empresa Pública", "region": "Valparaíso"}
    oferta = construir_oferta(item, det, fuente)
    assert oferta["fecha_cierre"] == date(2026, 8, 15)


def test_construir_oferta_sin_cierre_queda_none():
    item = {
        "titulo": "Ingeniero", "url": "https://x.buk.cl/s/abc", "ubicacion": "Santiago, Metropolitana",
        "renta_min": None, "renta_max": None, "renta_texto": None,
        "fecha_publicacion": date(2026, 7, 1),
    }
    det = {"descripcion": "Diseñar y ejecutar proyectos de infraestructura vial regional."}
    fuente = {"id": 294, "nombre": "SASIPA", "sector": "Empresa Pública", "region": "Valparaíso"}
    oferta = construir_oferta(item, det, fuente)
    assert oferta["fecha_cierre"] is None
