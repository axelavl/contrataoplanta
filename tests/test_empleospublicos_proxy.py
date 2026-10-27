"""Bloqueo por IP del portal empleospublicos.cl y respaldo por proxy chileno.

Contexto (oct-2026): el portal y su API apiConvocatorias.ashx responden 403
a IPs de datacenter fuera de Chile. El scraper parte sin proxy y, ante el
primer 403, activa PROXY_CL para el resto de la corrida. Además solo
descarga la ficha de ofertas nuevas (o las que toca refrescar hoy), para no
pagar tráfico de proxy por fichas que ya están guardadas.
"""
from __future__ import annotations

import asyncio
import hashlib
from datetime import date

import aiohttp
import pytest

from scrapers import empleos_publicos as ep
from scrapers.empleos_publicos import DIAS_REFRESCO_FICHA, EmpleosPublicosScraper, PageRequest

PROXY = "http://usuario:clave@proxy.cl:22225"
_SLEEP_REAL = asyncio.sleep


class _Resp:
    def __init__(self, status: int, texto: str = "", url: str = "https://empleospublicos.cl/x"):
        self.status = status
        self._texto = texto
        self.url = url

    async def text(self, **_):
        return self._texto

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientError(f"HTTP {self.status}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _SesionBloqueada:
    """403 si la request sale directo; 200 si viene por proxy."""

    def __init__(self):
        self.llamadas: list[dict] = []

    def request(self, metodo, url, **kw):
        self.llamadas.append(kw)
        if kw.get("proxy"):
            return _Resp(200, '{"total": 0, "data": []}')
        return _Resp(403, "<h1>403 Forbidden</h1>")


def _scraper(monkeypatch, proxy: str | None) -> EmpleosPublicosScraper:
    monkeypatch.setattr(ep, "_proxy_cl_configurado", lambda: proxy)
    sc = EmpleosPublicosScraper(instituciones=[], dry_run=True, delay=0)
    sc._rate_limiter = type("RL", (), {"wait": staticmethod(lambda: _SLEEP_REAL(0))})()
    return sc


def test_403_activa_el_proxy_y_reintenta(monkeypatch):
    sc = _scraper(monkeypatch, PROXY)
    sesion = _SesionBloqueada()
    texto, _ = asyncio.run(sc._request_text_y_url(sesion, PageRequest(method="GET", url=ep.API_URL)))
    assert texto == '{"total": 0, "data": []}'
    assert sc._usar_proxy is True
    assert "proxy" not in sesion.llamadas[0]                     # primero directo
    assert sesion.llamadas[1] == {**sesion.llamadas[1], "proxy": PROXY, "ssl": False}


def test_sin_proxy_configurado_falla_con_error_claro(monkeypatch):
    sc = _scraper(monkeypatch, None)
    errores: list[str] = []
    monkeypatch.setattr(sc.logger, "error", lambda msg, *a: errores.append(msg % a))
    monkeypatch.setattr(ep.asyncio, "sleep", lambda *_: _SLEEP_REAL(0))
    with pytest.raises(aiohttp.ClientError):
        asyncio.run(sc._request_text_y_url(_SesionBloqueada(), PageRequest(method="GET", url=ep.API_URL)))
    assert sc._usar_proxy is False
    assert len(errores) == 1 and "PROXY_CL no está configurado" in errores[0]   # un solo aviso


def test_sin_bloqueo_no_usa_proxy(monkeypatch):
    sc = _scraper(monkeypatch, PROXY)

    class _SesionLibre:
        def request(self, metodo, url, **kw):
            assert "proxy" not in kw
            return _Resp(200, "ok")

    texto, _ = asyncio.run(sc._request_text_y_url(_SesionLibre(), PageRequest(method="GET", url="https://x")))
    assert texto == "ok" and sc._usar_proxy is False


class _ConexionFalsa:
    def __init__(self, filas):
        self._filas = filas

    def cursor(self):
        filas = self._filas

        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, *_):
                pass

            def fetchall(self):
                return [(u,) for u in filas]
        return _C()

    def close(self):
        pass


def test_fichas_conocidas_se_omiten_salvo_refresco_semanal(monkeypatch):
    sc = _scraper(monkeypatch, PROXY)
    conocidas = [f"https://www.empleospublicos.cl/ficha?i={i}" for i in range(700)]
    monkeypatch.setattr(sc, "get_connection", lambda: _ConexionFalsa(conocidas))
    omitidas = sc._urls_con_ficha_vigente(conocidas + ["https://nueva"])
    hoy = date.today().toordinal() % DIAS_REFRESCO_FICHA
    a_refrescar = {u for u in conocidas
                   if int(hashlib.sha1(u.encode()).hexdigest(), 16) % DIAS_REFRESCO_FICHA == hoy}
    assert omitidas == set(conocidas) - a_refrescar
    assert "https://nueva" not in omitidas
    # ~1/7 de las conocidas se refresca cada día
    assert 50 < len(a_refrescar) < 150


def test_sin_base_de_datos_descarga_todas(monkeypatch):
    sc = _scraper(monkeypatch, PROXY)

    def _falla():
        raise RuntimeError("sin DB")
    monkeypatch.setattr(sc, "get_connection", _falla)
    assert sc._urls_con_ficha_vigente(["https://a"]) == set()
