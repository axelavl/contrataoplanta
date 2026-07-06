"""Programador in-app: ventana de silencio (no correr de madrugada).

El scheduler no debe reclamar corridas dentro de la ventana horaria de Chile
configurada (por defecto 00:00–08:00). Durante la ventana la corrida pendiente
espera y se dispara una vez al terminar (no se acumulan).
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import api.services.scheduler as S

_TZ = ZoneInfo("America/Santiago")


def _at(hora: int) -> datetime:
    return datetime(2026, 7, 7, hora, 30, tzinfo=_TZ)


def test_bounds_por_defecto():
    assert S._quiet_bounds() == (0, 8)


def test_silencio_de_madrugada_por_defecto():
    # 00–07 en silencio; 08 en adelante, activo.
    for h in range(0, 8):
        assert S._en_silencio(_at(h)) is True, h
    for h in (8, 9, 12, 18, 22, 23):
        assert S._en_silencio(_at(h)) is False, h


def test_ventana_configurable_por_entorno(monkeypatch):
    monkeypatch.setenv("SCHEDULER_QUIET_START", "1")
    monkeypatch.setenv("SCHEDULER_QUIET_END", "6")
    assert S._quiet_bounds() == (1, 6)
    assert S._en_silencio(_at(0)) is False   # fuera de 1–6
    assert S._en_silencio(_at(1)) is True
    assert S._en_silencio(_at(5)) is True
    assert S._en_silencio(_at(6)) is False


def test_ventana_que_envuelve_medianoche(monkeypatch):
    monkeypatch.setenv("SCHEDULER_QUIET_START", "22")
    monkeypatch.setenv("SCHEDULER_QUIET_END", "6")
    assert S._en_silencio(_at(21)) is False
    assert S._en_silencio(_at(22)) is True
    assert S._en_silencio(_at(0)) is True
    assert S._en_silencio(_at(5)) is True
    assert S._en_silencio(_at(6)) is False


def test_inicio_igual_fin_sin_ventana(monkeypatch):
    monkeypatch.setenv("SCHEDULER_QUIET_START", "0")
    monkeypatch.setenv("SCHEDULER_QUIET_END", "0")
    for h in (0, 3, 8, 15, 23):
        assert S._en_silencio(_at(h)) is False


def test_claim_no_corre_en_silencio(monkeypatch):
    # En silencio, _claim_due_run corta antes de tocar la BD (devuelve None).
    monkeypatch.setattr(S, "_en_silencio", lambda *a, **k: True)
    assert S._claim_due_run() is None
