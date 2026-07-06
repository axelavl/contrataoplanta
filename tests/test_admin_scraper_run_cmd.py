"""/scraper/run del panel: construcción del comando de run_all.

Regresión del "no corrió nada": una corrida MANUAL desde el panel debe pasar
--force-evaluate para saltarse el cooldown de retry-policy. Sin el flag, las
fuentes evaluadas hace poco quedan en cooldown y run_all termina con "No hay
fuentes a evaluar en esta corrida" — un botón que no hace nada.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import api.routers.admin as A


class _FakeProc:
    pid = 4242


class _FakeProceso:
    proc = _FakeProc()
    log_path = Path("/tmp/fake.log")


@pytest.fixture
def capturar_cmd(monkeypatch):
    capturado: dict = {}

    def _fake_lanzar(cmd, tipo):
        capturado["cmd"] = cmd
        capturado["tipo"] = tipo
        return _FakeProceso()

    monkeypatch.setattr(A, "_lanzar_proceso", _fake_lanzar)
    monkeypatch.setattr(A, "_auditar", lambda *a, **k: None)
    return capturado


def _run(payload):
    return asyncio.run(A.admin_scraper_run(payload, _user="tester"))


def test_corrida_manual_fuerza_evaluacion(capturar_cmd):
    _run({"mode": "institucion", "institucion_id": 275})
    cmd = capturar_cmd["cmd"]
    assert "--force-evaluate" in cmd            # salta el cooldown
    assert "--id" in cmd and "275" in cmd
    assert "--skip-empleos-publicos" in cmd


def test_modo_municipios_ids_y_force(capturar_cmd):
    _run({"mode": "municipios"})
    cmd = capturar_cmd["cmd"]
    assert "--force-evaluate" in cmd
    i = cmd.index("--ids")
    ids = cmd[i + 1]
    assert "345" in ids and "388" in ids and "398" in ids   # Viña, La Cisterna, Maipú
    assert "--skip-empleos-publicos" in cmd


def test_modo_universidades_y_puertos(capturar_cmd):
    _run({"mode": "universidades"})
    ids_uni = capturar_cmd["cmd"][capturar_cmd["cmd"].index("--ids") + 1]
    assert "246" in ids_uni and "243" in ids_uni            # UFRO, USACH
    _run({"mode": "puertos_empresas"})
    ids_pue = capturar_cmd["cmd"][capturar_cmd["cmd"].index("--ids") + 1]
    assert "166" in ids_pue and "291" in ids_pue            # ENAER, Pto. Montt


def test_simulacion_agrega_evaluate_only_ademas_del_force(capturar_cmd):
    _run({"mode": "municipios", "dry_run": True})
    cmd = capturar_cmd["cmd"]
    assert "--evaluate-only" in cmd
    assert "--force-evaluate" in cmd
