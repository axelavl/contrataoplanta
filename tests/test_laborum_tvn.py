"""Laborum (perfiles de empresa): registro paramétrico + extracción de tarjeta.

Incorporación de TVN — Televisión Nacional de Chile (id 280), que publica sus
vacantes en su perfil de Laborum. El scraper laborum.py es paramétrico: basta
registrar el perfil en PERFILES. Se cubre además la extracción de región por
tarjeta (TVN tiene centros regionales) con fallback a la región de la empresa.
"""
from __future__ import annotations

import scrapers.laborum as L


# ── Registro de TVN ──────────────────────────────────────────────────────────
def test_tvn_registrado_en_perfiles():
    assert 280 in L.PERFILES
    perfil = L.PERFILES[280]
    assert perfil["perfil_url"] == (
        "https://www.laborum.cl/perfiles/"
        "empresa_television-nacional-de-chile_13340328.html"
    )
    assert perfil["sector"] == "Empresa del Estado"
    assert perfil["tipo_cargo_default"] == "Código del Trabajo"
    # HQ en Santiago como fallback de región.
    assert perfil["region"] == "Metropolitana de Santiago"


def test_tvn_excluida_del_dispatch_generico():
    # 280 debe estar en _IDS_NUEVO_ESTANDAR para que el genérico no intente
    # scrapear empleos.tvn.cl (sin scraper) además del perfil de Laborum.
    import scrapers.run_all as R
    assert 280 in R._IDS_NUEVO_ESTANDAR
    # Y el orquestador corre laborum si alguna fuente del catálogo es un perfil.
    assert 280 in L.PERFILES


# ── Región por tarjeta ───────────────────────────────────────────────────────
def test_region_desde_tarjeta_reconoce_regiones_canonicas():
    casos = [
        ("Providencia, Región Metropolitana de Santiago Presencial", "Metropolitana de Santiago"),
        ("Concepción, Región del Biobío Presencial", "Biobío"),
        ("Región de Valparaíso Híbrido", "Valparaíso"),
        ("Región de La Araucanía Remoto", "La Araucanía"),
    ]
    for texto, esperado in casos:
        assert L._region_desde_tarjeta(texto) == esperado


def test_region_desde_tarjeta_sin_region_devuelve_none():
    # Sin "Región <X>" reconocible → None (el llamador cae al fallback empresa).
    assert L._region_desde_tarjeta("Analista de Sistemas Presencial") is None
    assert L._region_desde_tarjeta("") is None


# ── Parseo de tarjeta + construcción de oferta ───────────────────────────────
_CARD_REGIONAL = {
    "href": "/empleos/periodista-red-regional-45678901.html",
    "text": (
        "Publicado hace 2 días PERIODISTA RED REGIONAL "
        "TVN — Televisión Nacional de Chile "
        "Cobertura de noticias en el territorio. Requisitos: Título de "
        "Periodista, 2 años de experiencia. RENTA BRUTO: 1.500.000 "
        "Concepción, Región del Biobío Presencial"
    ),
}

_CARD_SANTIAGO = {
    "href": "/empleos/editor-de-contenidos-45678902.html",
    "text": (
        "Actualizado hace 5 días EDITOR DE CONTENIDOS DIGITALES "
        "TVN — Televisión Nacional de Chile "
        "Gestión de la parrilla digital. Requisitos: Periodista o afín. "
        "SUELDO BRUTO: 1.800.000 Presencial"
    ),
}


def test_parsear_tarjeta_extrae_campos_clave():
    item = L.parsear_tarjeta(_CARD_REGIONAL, L.PERFILES[280])
    assert item["id_aviso"] == "45678901"
    assert item["cargo"] == "PERIODISTA RED REGIONAL"
    assert item["renta_bruta"] == 1_500_000
    assert item["modalidad"] == "Presencial"
    assert item["region"] == "Biobío"
    assert "Periodista" in (item["requisitos_texto"] or "")


def test_construir_oferta_usa_region_de_tarjeta():
    item = L.parsear_tarjeta(_CARD_REGIONAL, L.PERFILES[280])
    oferta = L.construir_oferta(item, 280, L.PERFILES[280])
    assert oferta["region"] == "Biobío"                      # de la tarjeta
    assert oferta["institucion_id"] == 280
    assert oferta["sector"] == "Empresa del Estado"
    assert oferta["tipo_cargo"] == "Código del Trabajo"
    assert oferta["url_original"].endswith("/empleos/periodista-red-regional-45678901.html")


def test_construir_oferta_cae_a_region_empresa_sin_region_en_tarjeta():
    item = L.parsear_tarjeta(_CARD_SANTIAGO, L.PERFILES[280])
    assert item["region"] is None
    oferta = L.construir_oferta(item, 280, L.PERFILES[280])
    # Sin región en la tarjeta → fallback a la casa central (HQ).
    assert oferta["region"] == "Metropolitana de Santiago"
    assert oferta["renta_bruta_min"] == 1_800_000
