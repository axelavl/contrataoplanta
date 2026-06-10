from __future__ import annotations

from datetime import date
from pathlib import Path

from scrapers.runtime_inventory import (
    LEGACY_MODULE_PATHS,
    LEGACY_MODULES,
    LEGACY_RETIREMENT_DATE,
    LEGACY_STATUS_DEPRECATED,
    PRODUCTION_RUNTIME_MODULES,
    RUNTIME_STATUS_ACTIVE,
    is_legacy_module,
)


def test_runtime_modules_are_explicitly_active():
    assert PRODUCTION_RUNTIME_MODULES, "Debe existir al menos un módulo runtime activo."
    assert all(module.status == RUNTIME_STATUS_ACTIVE for module in PRODUCTION_RUNTIME_MODULES)


def test_legacy_modules_have_uniform_deprecation_contract():
    # El inventario legacy fue retirado por completo (2026-06-09): los módulos
    # per-institución se eliminaron y su lógica vive en scrapers/plataformas/.
    # Si en el futuro se vuelve a deprecar algún módulo, debe cumplir el contrato.
    for module in LEGACY_MODULES:
        assert module.status == LEGACY_STATUS_DEPRECATED
        assert module.retirement_date == LEGACY_RETIREMENT_DATE
        assert Path(module.module).exists(), f"Módulo legacy no encontrado en repo: {module.module}"
        assert is_legacy_module(module.module) is True


def test_legacy_inventory_is_empty_after_retirement():
    # Inventario legacy cerrado y vacío tras el retiro de los scrapers per-institución.
    assert LEGACY_MODULES == ()
    assert LEGACY_MODULE_PATHS == frozenset()
    assert LEGACY_RETIREMENT_DATE == date(2026, 9, 30)
    assert is_legacy_module("scrapers/trabajando.py") is False
