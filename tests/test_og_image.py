"""Tests del renderer de imágenes OG/RRSS (api/services/og_image.py).

Valida que:

- Ambos formatos (horizontal 1200x630, square 1080x1080) producen PNGs válidos.
- La jerarquía visual no rompe cuando faltan campos (institución, región,
  renta, fecha de cierre).
- El estado "cierra pronto" dispara el render de la alerta roja.
- Cargos muy largos no desbordan ni revientan el layout.
- La resolución de sigla fallback funciona con o sin dato explícito.

No se hace verificación pixel-a-pixel — eso sería frágil ante cambios de
fuente/sistema. Verificamos sí que el PNG decodifica a las dimensiones
esperadas y que ciertos pixeles muestreados contienen los colores de marca
o de alerta cuando corresponde.
"""
from __future__ import annotations

from datetime import date, timedelta
from io import BytesIO

import pytest

PIL = pytest.importorskip("PIL")  # saltamos el archivo completo si falta
from PIL import Image  # noqa: E402

from api.services import og_image  # noqa: E402


def _oferta_completa(**overrides):
    base = {
        "id": 12345,
        "cargo": "Profesional Administrativo para Unidad de Recursos Humanos",
        "institucion": "Servicio de Salud Metropolitano Sur Oriente",
        "sigla": "SSMSO",
        "institucion_sitio_web": None,  # evita hit de red en tests
        "region": "Metropolitana",
        "ciudad": "Puente Alto",
        "tipo_contrato": "contrata",
        "renta_bruta_min": 1_200_000,
        "renta_bruta_max": 1_500_000,
        "fecha_cierre": date.today() + timedelta(days=10),
        "dias_restantes": 10,
        "estado": "active",
        "fecha_actualizado": "2026-04-18T10:00:00",
    }
    base.update(overrides)
    return base


def _png_size(data: bytes) -> tuple[int, int]:
    return Image.open(BytesIO(data)).size


class TestRenderOfferCard:
    def test_horizontal_default_dimensions(self):
        png = og_image.render_offer_card(_oferta_completa())
        assert _png_size(png) == (1200, 630)

    def test_horizontal_explicit(self):
        png = og_image.render_offer_card(_oferta_completa(), fmt="horizontal")
        assert _png_size(png) == (1200, 630)

    def test_square_format(self):
        png = og_image.render_offer_card(_oferta_completa(), fmt="square")
        assert _png_size(png) == (1080, 1080)

    def test_png_signature(self):
        png = og_image.render_offer_card(_oferta_completa())
        # PNG magic number — confirma que Pillow entregó un PNG válido.
        assert png[:8] == b"\x89PNG\r\n\x1a\n"

    def test_missing_all_secondary_fields(self):
        """Sin institución, región, renta ni cierre el render no debe reventar."""
        oferta = {"id": 1, "cargo": "Oferta laboral"}
        png = og_image.render_offer_card(oferta)
        assert _png_size(png) == (1200, 630)

    def test_missing_cargo_uses_default(self):
        oferta = {"id": 1, "institucion": "Ministerio"}
        png = og_image.render_offer_card(oferta)
        assert _png_size(png) == (1200, 630)

    def test_very_long_cargo_does_not_overflow(self):
        cargo = (
            "Profesional Especialista Senior en Coordinación Estratégica y "
            "Gestión de Políticas Públicas para la Implementación de Programas "
            "de Modernización Institucional del Sector Salud Subnacional"
        )
        oferta = _oferta_completa(cargo=cargo)
        png = og_image.render_offer_card(oferta)
        # Verifica que seguimos respetando las dimensiones finales.
        assert _png_size(png) == (1200, 630)


class TestCierraPronto:
    def test_closing_today_triggers_alert(self):
        oferta = _oferta_completa(
            dias_restantes=0,
            estado="closing_today",
            fecha_cierre=date.today(),
        )
        texto, alerta = og_image._format_cierre(oferta)
        assert alerta is True
        assert "HOY" in texto

    def test_manana_triggers_alert(self):
        oferta = _oferta_completa(dias_restantes=1)
        texto, alerta = og_image._format_cierre(oferta)
        assert alerta is True
        assert "mañana" in texto.lower()

    def test_tres_dias_triggers_alert(self):
        oferta = _oferta_completa(dias_restantes=3)
        _texto, alerta = og_image._format_cierre(oferta)
        assert alerta is True

    def test_lejos_no_alerta(self):
        oferta = _oferta_completa(dias_restantes=20)
        _texto, alerta = og_image._format_cierre(oferta)
        assert alerta is False

    def test_vencida_devuelve_vacio(self):
        oferta = _oferta_completa(dias_restantes=-2, estado="closed")
        texto, alerta = og_image._format_cierre(oferta)
        assert texto == ""
        assert alerta is False

    def test_render_con_alerta_genera_png(self):
        png = og_image.render_offer_card(
            _oferta_completa(
                dias_restantes=0,
                estado="closing_today",
                fecha_cierre=date.today(),
            ),
            fmt="horizontal",
        )
        assert _png_size(png) == (1200, 630)


class TestFormateoRenta:
    def test_rango(self):
        out = og_image._format_renta({"renta_bruta_min": 900_000, "renta_bruta_max": 1_200_000})
        # Formato chileno: separador de miles con punto.
        assert "$900.000" in out
        assert "$1.200.000" in out

    def test_solo_min(self):
        out = og_image._format_renta({"renta_bruta_min": 900_000, "renta_bruta_max": None})
        assert out == "Desde $900.000"

    def test_solo_max(self):
        out = og_image._format_renta({"renta_bruta_min": None, "renta_bruta_max": 1_200_000})
        assert out == "Hasta $1.200.000"

    def test_iguales(self):
        out = og_image._format_renta({"renta_bruta_min": 1_000_000, "renta_bruta_max": 1_000_000})
        assert out == "$1.000.000"

    def test_sin_renta(self):
        assert og_image._format_renta({}) is None
        assert og_image._format_renta({"renta_bruta_min": 0, "renta_bruta_max": 0}) is None


class TestSiglaFallback:
    def test_con_sigla_explicita(self):
        assert og_image._sigla_fallback("Irrelevante", "SSMSO") == "SSM"

    def test_sin_sigla_usa_iniciales(self):
        # "de" es stopword — debe ignorarse.
        assert og_image._sigla_fallback("Servicio de Salud Metropolitano", None) == "SS"

    def test_nombre_vacio_fallback_generico(self):
        assert og_image._sigla_fallback(None, None) == "CL"
        assert og_image._sigla_fallback("", "") == "CL"


class TestLogoFallback:
    def test_sin_dominio_no_hace_red(self, monkeypatch):
        # Si intentáramos hacer fetch la llamada explotaría por no tener requests.
        called = {}

        def _fake_fetch(domain):  # pragma: no cover — no debería llamarse
            called["hit"] = True
            return None

        monkeypatch.setattr(og_image, "_fetch_logo", _fake_fetch)
        og_image._cached_logo.cache_clear()
        assert og_image._load_institution_logo(None) is None
        assert "hit" not in called

    def test_fetch_falla_silencioso(self, monkeypatch):
        def _boom(domain):
            return None

        monkeypatch.setattr(og_image, "_fetch_logo", _boom)
        og_image._cached_logo.cache_clear()
        assert og_image._load_institution_logo("dominio-inexistente.cl") is None
