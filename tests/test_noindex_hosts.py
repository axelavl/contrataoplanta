"""El backend debe pedir noindex fuera del dominio de marca.

Google indexaba el host técnico de Railway como un sitio duplicado de
contrataoplanta.cl. El middleware agrega `X-Robots-Tag: noindex, nofollow`
a toda respuesta que no llegue por el host de marca, salvo que venga
marcada por el proxy de Pages (header X-Canonical-Proxy), que sirve el
mismo contenido bajo el dominio canónico.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_host_tecnico_recibe_noindex():
    resp = client.get(
        "/health",
        headers={"host": "contrataoplanta-production.up.railway.app"},
    )
    assert resp.headers.get("x-robots-tag") == "noindex, nofollow"


def test_host_de_marca_no_recibe_noindex():
    resp = client.get("/health", headers={"host": "contrataoplanta.cl"})
    assert "x-robots-tag" not in resp.headers


def test_www_de_marca_no_recibe_noindex():
    resp = client.get("/health", headers={"host": "www.contrataoplanta.cl"})
    assert "x-robots-tag" not in resp.headers


def test_proxy_de_pages_exime_del_noindex():
    # El proxy le pega al host de Railway pero marca la request; el contenido
    # se sirve bajo contrataoplanta.cl y sí debe indexarse.
    resp = client.get(
        "/health",
        headers={
            "host": "contrataoplanta-production.up.railway.app",
            "x-canonical-proxy": "1",
        },
    )
    assert "x-robots-tag" not in resp.headers
