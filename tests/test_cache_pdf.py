"""Tests de scrapers/cache_pdf.py (caché del texto de PDFs en Postgres).

La persistencia se reemplaza por un dict en memoria: lo que se prueba es la
lógica de cuándo descargar, cuándo reutilizar y cuándo NO cachear.
"""
from __future__ import annotations

import pytest

from scrapers import cache_pdf

PDF = b"%PDF-1.4\n...contenido..."


@pytest.fixture
def memoria(monkeypatch):
    """Sustituye leer/guardar por un dict y reinicia el estado del módulo."""
    almacen: dict[tuple[str, str], str] = {}

    def leer(url, modo="texto"):
        return almacen.get((url, modo))

    def guardar(url, texto, modo="texto", bytes_pdf=None):
        if cache_pdf._estado["escribir"]:
            almacen[(url, modo)] = texto

    monkeypatch.setattr(cache_pdf, "leer", leer)
    monkeypatch.setattr(cache_pdf, "guardar", guardar)
    cache_pdf.configurar(escribir=True)
    return almacen


def _contador(contenido=PDF):
    llamadas = []

    def descargar(url):
        llamadas.append(url)
        return contenido
    return descargar, llamadas


def test_segunda_lectura_no_descarga(memoria):
    descargar, llamadas = _contador()
    t1 = cache_pdf.texto_pdf("https://x.cl/b.pdf", descargar, lambda b: "REQUISITOS")
    t2 = cache_pdf.texto_pdf("https://x.cl/b.pdf", descargar, lambda b: "REQUISITOS")
    assert t1 == t2 == "REQUISITOS"
    assert llamadas == ["https://x.cl/b.pdf"]          # una sola descarga
    assert "1 reutilizados, 1 descargados" in cache_pdf.resumen()


def test_texto_vacio_tambien_se_cachea(memoria):
    # PDF escaneado leído sin OCR: "" es un resultado válido, no hay que
    # volver a bajarlo en cada corrida.
    descargar, llamadas = _contador()
    assert cache_pdf.texto_pdf("u", descargar, lambda b: "") == ""
    assert cache_pdf.texto_pdf("u", descargar, lambda b: "") == ""
    assert len(llamadas) == 1


def test_modo_ocr_es_independiente(memoria):
    descargar, llamadas = _contador()
    cache_pdf.texto_pdf("u", descargar, lambda b: "", modo="texto")
    assert cache_pdf.texto_pdf("u", descargar, lambda b: "OCR", modo="ocr") == "OCR"
    assert len(llamadas) == 2


def test_respuesta_que_no_es_pdf_no_se_cachea(memoria):
    # Página de bloqueo del WAF con status 200: no debe quedar "resuelta".
    descargar, llamadas = _contador(b"<html>Access denied</html>")
    assert cache_pdf.texto_pdf("u", descargar, lambda b: "X") == ""
    assert memoria == {}
    cache_pdf.texto_pdf("u", descargar, lambda b: "X")
    assert len(llamadas) == 2                           # se reintenta


def test_descarga_fallida_devuelve_none_y_no_cachea(memoria):
    assert cache_pdf.texto_pdf("u", lambda u: None, lambda b: "X") is None
    assert memoria == {}


def test_error_al_extraer_no_rompe(memoria):
    def extraer(_b):
        raise ValueError("pdf corrupto")
    descargar, _ = _contador()
    assert cache_pdf.texto_pdf("u", descargar, extraer) == ""


def test_dry_run_lee_pero_no_escribe(memoria):
    memoria[("u", "texto")] = "GUARDADO"
    cache_pdf.configurar(escribir=False)
    descargar, llamadas = _contador()
    assert cache_pdf.texto_pdf("u", descargar, lambda b: "NUEVO") == "GUARDADO"
    assert cache_pdf.texto_pdf("v", descargar, lambda b: "NUEVO") == "NUEVO"
    assert ("v", "texto") not in memoria
    assert llamadas == ["v"]


def test_sin_base_de_datos_funciona_igual(monkeypatch):
    """Si no hay DB (standalone, DB caída, tabla sin migrar) la caché se
    desactiva y se descarga siempre, como antes."""
    monkeypatch.setattr(cache_pdf, "_estado", {
        "conn": None, "deshabilitada": True, "escribir": True,
        "purgada": False, "hits": 0, "misses": 0})
    descargar, llamadas = _contador()
    assert cache_pdf.texto_pdf("u", descargar, lambda b: "T") == "T"
    assert cache_pdf.texto_pdf("u", descargar, lambda b: "T") == "T"
    assert len(llamadas) == 2


def test_descargador_respeta_delay_solo_al_descargar(monkeypatch):
    esperas = []
    monkeypatch.setattr("time.sleep", lambda s: esperas.append(s))

    class R:
        content = PDF
    bajar = cache_pdf.descargador(lambda session, url: R(), session=None, delay=1.5)
    assert bajar("u") == PDF
    assert esperas == [1.5]
