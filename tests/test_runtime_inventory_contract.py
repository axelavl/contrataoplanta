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
    assert LEGACY_MODULES, "Debe existir al menos un módulo legacy deprecado."
    for module in LEGACY_MODULES:
        assert module.status == LEGACY_STATUS_DEPRECATED
        assert module.retirement_date == LEGACY_RETIREMENT_DATE
        assert Path(module.module).exists(), f"Módulo legacy no encontrado en repo: {module.module}"
        assert is_legacy_module(module.module) is True


def test_legacy_inventory_is_closed_and_explicit():
    expected_legacy_modules = {
        "scrapers/banco_central.py",
        "scrapers/codelco.py",
        "scrapers/externouchile.py",
        "scrapers/gobiernos_regionales.py",
        "scrapers/muni_la_florida.py",
        "scrapers/muni_puente_alto.py",
        "scrapers/muni_san_bernardo.py",
        "scrapers/muni_temuco.py",
        "scrapers/poder_judicial.py",
        "scrapers/trabajando.py",
        "scrapers/tvn.py",
    }
    assert LEGACY_MODULE_PATHS == expected_legacy_modules
    assert LEGACY_RETIREMENT_DATE == date(2026, 9, 30)
