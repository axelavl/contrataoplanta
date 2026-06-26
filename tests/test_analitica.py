"""Tests de los helpers puros de `api.services.analitica`.

Cubren la clasificación de dispositivo, normalización de path, extracción
del host del referrer, la sesión anónima determinista y el filtrado de
bots/eventos inválidos en `registrar_evento` (que descarta ANTES de tocar
la base de datos, así que no requieren Postgres).
"""
from __future__ import annotations

from api.services import analitica


def test_clasificar_dispositivo():
    assert analitica.clasificar_dispositivo("Mozilla/5.0 (iPhone) Mobile") == "movil"
    assert analitica.clasificar_dispositivo("Mozilla/5.0 (Windows NT 10.0)") == "escritorio"
    assert analitica.clasificar_dispositivo("Mozilla/5.0 (iPad)") == "tablet"
    assert analitica.clasificar_dispositivo("Googlebot/2.1") == "bot"
    assert analitica.clasificar_dispositivo("") == "bot"


def test_es_bot():
    assert analitica.es_bot("python-requests/2.31") is True
    assert analitica.es_bot("curl/8.0") is True
    assert analitica.es_bot("Mozilla/5.0 (Macintosh)") is False


def test_normalizar_path():
    assert analitica.normalizar_path("/oferta/123-analista?utm=x") == "/oferta/:id"
    assert analitica.normalizar_path("/institucion/45") == "/institucion/:id"
    assert analitica.normalizar_path("/curso/mi-curso") == "/curso/:slug"
    assert analitica.normalizar_path("/cursos.html") == "/cursos.html"
    assert analitica.normalizar_path("") is None
    # Sin slash inicial se normaliza igual.
    assert analitica.normalizar_path("faq.html").startswith("/")


def test_host_de_referrer():
    assert analitica.host_de_referrer("https://www.google.com/search?q=x") == "www.google.com"
    assert analitica.host_de_referrer("not-a-url") is None
    assert analitica.host_de_referrer(None) is None


def test_sesion_anonima_determinista():
    a = analitica.sesion_anonima("UA", "1.2.3.4")
    b = analitica.sesion_anonima("UA", "1.2.3.4")
    c = analitica.sesion_anonima("UA", "9.9.9.9")
    assert a == b           # mismo día + mismo UA/IP → mismo hash
    assert a != c           # IP distinta → hash distinto
    assert len(a) == 32     # nunca guarda el original, solo el hash truncado
    assert "1.2.3.4" not in a


def test_registrar_evento_descarta_bots_sin_tocar_db():
    # Un bot se descarta antes de cualquier acceso a la base de datos.
    assert analitica.registrar_evento(
        tipo="pageview", path="/", evento=None, oferta_id=None,
        referrer=None, user_agent="Googlebot/2.1", ip="1.1.1.1",
    ) is False


def test_registrar_evento_descarta_evento_invalido():
    # Evento no reconocido → se descarta antes de tocar la base de datos.
    assert analitica.registrar_evento(
        tipo="evento", path="/", evento="evento_inexistente", oferta_id=None,
        referrer=None, user_agent="Mozilla/5.0 (Macintosh)", ip="1.1.1.1",
    ) is False


def test_umami_no_configurado_por_defecto(monkeypatch):
    for var in ("UMAMI_API_URL", "UMAMI_API_KEY", "UMAMI_USERNAME",
                "UMAMI_PASSWORD", "UMAMI_WEBSITE_ID"):
        monkeypatch.delenv(var, raising=False)
    assert analitica.umami_configurado() is False
    assert analitica.resumen_umami(30) == {"configurado": False}
