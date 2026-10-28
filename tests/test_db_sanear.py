"""db/sanear.py: una oferta con un campo opcional inválido se guarda igual.

Regresión (oct-2026): Codelco encontró 10 ofertas y no guardó ninguna,
porque `tipo_cargo = 'Código del Trabajo (Plazo Fijo)'` violaba el CHECK
chk_ofertas_tipo_cargo y Postgres rechazaba la fila completa.
"""
from __future__ import annotations

from datetime import date

import pytest

from db.sanear import canonizar_tipo_cargo, sanear_oferta

HOY = date(2026, 10, 27)


@pytest.mark.parametrize("texto,esperado", [
    ("Código del Trabajo (Plazo Fijo)", "codigo_trabajo"),
    ("Código del Trabajo (Indefinido)", "codigo_trabajo"),
    ("Planta", "planta"), ("CONTRATA", "contrata"), ("Honorarios", "honorarios"),
    ("Reemplazo / suplencia", "reemplazo"), ("codigo_trabajo", "codigo_trabajo"),
    ("ADP", None), ("", None), (None, None),
])
def test_canonizar_tipo_cargo(texto, esperado):
    assert canonizar_tipo_cargo(texto) == esperado


def test_campos_fuera_de_regla_quedan_null_y_el_resto_intacto():
    d = sanear_oferta({
        "cargo": "Analista", "tipo_cargo": "Código del Trabajo (Plazo Fijo)",
        "renta_bruta_min": 150_000, "renta_bruta_max": "1.200.000",   # baja / no numérica
        "horas_semanales": 120,
        "fecha_publicacion": date(2026, 11, 5), "fecha_cierre": date(2026, 11, 3),
    }, hoy=HOY)
    assert d["cargo"] == "Analista"
    assert d["tipo_cargo"] == "codigo_trabajo"
    assert d["renta_bruta_min"] is None and d["renta_bruta_max"] is None
    assert d["horas_semanales"] is None
    assert d["fecha_cierre"] == date(2026, 11, 3) and d["fecha_publicacion"] is None


def test_renta_invertida_se_ordena_y_fechas_validas_se_conservan():
    d = sanear_oferta({"renta_bruta_min": 2_000_000, "renta_bruta_max": 1_500_000,
                       "fecha_publicacion": "2026-10-20", "fecha_cierre": "2026-11-10"}, hoy=HOY)
    assert (d["renta_bruta_min"], d["renta_bruta_max"]) == (1_500_000, 2_000_000)
    assert d["fecha_publicacion"] == "2026-10-20" and d["fecha_cierre"] == "2026-11-10"


def test_fechas_fuera_de_rango():
    d = sanear_oferta({"fecha_publicacion": date(2019, 1, 1),
                       "fecha_cierre": date(2031, 1, 1)}, hoy=HOY)
    assert d["fecha_publicacion"] is None and d["fecha_cierre"] is None


def test_no_agrega_claves_ausentes():
    assert sanear_oferta({"cargo": "X"}, hoy=HOY) == {"cargo": "X"}


def test_fecha_de_hoy_utc_se_lleva_a_hoy_en_chile(monkeypatch):
    """27-oct 21:47 Chile = 28-oct 00:47 UTC: el scraper de Laborum guardó
    fecha_publicacion = 28 y el sitio ocultó 19 ofertas como 'próximas'."""
    import db.sanear as s
    from datetime import datetime as dt, timezone

    class _Reloj(dt):
        @classmethod
        def now(cls, tz=None):
            return dt(2026, 10, 28, 0, 47, tzinfo=timezone.utc).astimezone(tz) if tz else dt(2026, 10, 28, 0, 47)
    monkeypatch.setattr(s, "datetime", _Reloj)
    assert s.hoy_chile() == date(2026, 10, 27)
    assert s.sanear_oferta({"fecha_publicacion": date(2026, 10, 28)})["fecha_publicacion"] == date(2026, 10, 27)
    # Inicio de postulación futuro legítimo (Empleos Públicos): se respeta.
    assert s.sanear_oferta({"fecha_publicacion": date(2026, 11, 2)})["fecha_publicacion"] == date(2026, 11, 2)
