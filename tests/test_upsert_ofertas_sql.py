"""Contrato entre el upsert de ofertas (scrapers/base.py) y el schema.

Regresión (oct-2026): la migración 20260710_0001 reemplazó UNIQUE(url_oferta)
por el índice parcial uq_ofertas_url_oferta_cargo (url_oferta, cargo) WHERE
url_oferta IS NOT NULL, pero `_offer_upsert_sql` seguía con
`ON CONFLICT (url_oferta)`. Postgres rechazaba TODOS los INSERT de Empleos
Públicos ("no unique or exclusion constraint matching the ON CONFLICT
specification") y la web quedaba sin ofertas.
"""
from __future__ import annotations

import inspect
import re

import scrapers.base as base


def _sql() -> str:
    cls = next(c for _, c in inspect.getmembers(base, inspect.isclass)
               if hasattr(c, "_offer_upsert_sql") and c.__module__ == base.__name__)
    return re.sub(r"\s+", " ", cls._offer_upsert_sql(None))


def test_on_conflict_calza_con_indice_url_oferta_cargo():
    assert "ON CONFLICT (url_oferta, cargo) WHERE url_oferta IS NOT NULL" in _sql()


def test_insert_incluye_institucion_nombre():
    # ofertas.institucion_nombre es NOT NULL en db/schema.sql.
    sql = _sql()
    columnas = sql.split("INSERT INTO ofertas (", 1)[1].split(")", 1)[0]
    assert "institucion_nombre" in columnas
