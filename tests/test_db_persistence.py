"""Tests for the DB-writing helpers in db/database.py.

Uses a lightweight fake Session rather than a real Postgres instance. The SQL
these helpers issue is Postgres-specific (ARRAY `!= ALL(...)`, `NOW()`, etc.)
so SQLite would misparse it. Instead, we record every `execute` call and
assert on the SQL text plus the bound parameters.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any

from db.database import (
    marcar_ofertas_cerradas,
    registrar_log,
    upsert_oferta,
    url_a_hash,
)


@dataclass
class _ExecuteCall:
    sql: str
    params: dict
    # What the fake result should return when `.fetchone()` is called.
    fetchone_value: Any = None
    # Simulated affected-row count for UPDATE/DELETE.
    rowcount: int = 0


class _FakeResult:
    def __init__(self, fetchone_value=None, rowcount=0):
        self._fetchone_value = fetchone_value
        self.rowcount = rowcount

    def fetchone(self):
        return self._fetchone_value


@dataclass
class FakeSession:
    """Records executes; optionally scripts responses for each execute call."""

    scripted_results: list = field(default_factory=list)
    calls: list = field(default_factory=list)
    commits: int = 0
    rollbacks: int = 0
    raise_on_execute: Exception | None = None

    def execute(self, clause, params=None):
        if self.raise_on_execute is not None:
            raise self.raise_on_execute
        # SQLAlchemy text() objects expose the raw SQL via str().
        sql = str(clause)
        self.calls.append(_ExecuteCall(sql=sql, params=params or {}))
        if self.scripted_results:
            scripted = self.scripted_results.pop(0)
            return _FakeResult(
                fetchone_value=scripted.get("fetchone"),
                rowcount=scripted.get("rowcount", 0),
            )
        return _FakeResult()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _oferta_minima(**overrides) -> dict:
    base = {
        "id_externo": "X-1",
        "fuente_id": 1,
        "url_original": "https://site.cl/oferta/1",
        "cargo": "Analista",
        "descripcion": "Descripción...",
        "institucion_nombre": "Municipalidad",
        "sector": "Municipal",
        "area_profesional": "Administración",
        "tipo_cargo": "Contrata",
        "nivel": "Profesional",
        "region": "Metropolitana de Santiago",
        "ciudad": "Santiago",
        "renta_bruta_min": 1_000_000,
        "renta_bruta_max": 1_500_000,
        "renta_texto": "$1.000.000 - $1.500.000",
        "fecha_publicacion": None,
        "fecha_cierre": None,
        "requisitos_texto": "Título profesional",
    }
    base.update(overrides)
    return base


class UpsertOfertaTests(unittest.TestCase):
    def test_insert_when_row_missing(self):
        session = FakeSession(scripted_results=[{"fetchone": None}])
        es_nueva, actualizada = upsert_oferta(session, _oferta_minima())

        self.assertTrue(es_nueva)
        self.assertFalse(actualizada)
        self.assertEqual(len(session.calls), 2)  # SELECT + INSERT
        self.assertIn("SELECT", session.calls[0].sql)
        self.assertIn("INSERT INTO ofertas", session.calls[1].sql)
        self.assertEqual(session.commits, 1)
        self.assertEqual(session.rollbacks, 0)

    def test_update_when_row_exists(self):
        session = FakeSession(
            scripted_results=[{"fetchone": (123, None)}]  # existing row
        )
        es_nueva, actualizada = upsert_oferta(session, _oferta_minima())

        self.assertFalse(es_nueva)
        self.assertTrue(actualizada)
        self.assertEqual(len(session.calls), 2)  # SELECT + UPDATE
        self.assertIn("UPDATE ofertas", session.calls[1].sql)
        self.assertEqual(session.commits, 1)

    def test_url_hash_bound_parameter_matches_helper(self):
        session = FakeSession(scripted_results=[{"fetchone": None}])
        datos = _oferta_minima(url_original="https://example.cl/foo")
        upsert_oferta(session, datos)

        expected_hash = url_a_hash("https://example.cl/foo")
        self.assertEqual(session.calls[0].params["h"], expected_hash)
        self.assertEqual(session.calls[1].params["url_hash"], expected_hash)

    def test_insert_params_include_all_columns(self):
        session = FakeSession(scripted_results=[{"fetchone": None}])
        upsert_oferta(session, _oferta_minima())
        insert_params = session.calls[1].params
        # A few representative keys the INSERT binds.
        for key in [
            "id_externo", "fuente_id", "url_original", "url_hash",
            "cargo", "institucion_nombre", "region", "ciudad",
            "renta_bruta_min", "renta_bruta_max", "renta_texto",
            "fecha_cierre", "requisitos_texto",
        ]:
            self.assertIn(key, insert_params)

    def test_long_fields_truncated_before_binding(self):
        session = FakeSession(scripted_results=[{"fetchone": None}])
        datos = _oferta_minima(cargo="A" * 1000, region="R" * 500)
        upsert_oferta(session, datos)
        insert_params = session.calls[1].params
        self.assertEqual(len(insert_params["cargo"]), 500)
        self.assertEqual(len(insert_params["region"]), 80)

    def test_rollback_on_execute_exception(self):
        session = FakeSession(raise_on_execute=RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            upsert_oferta(session, _oferta_minima())
        self.assertEqual(session.rollbacks, 1)
        self.assertEqual(session.commits, 0)


class MarcarOfertasCerradasTests(unittest.TestCase):
    def test_no_urls_returns_zero_without_sql(self):
        session = FakeSession()
        closed = marcar_ofertas_cerradas(session, fuente_id=7, urls_activas=[])
        self.assertEqual(closed, 0)
        self.assertEqual(session.calls, [])
        self.assertEqual(session.commits, 0)

    def test_returns_affected_rowcount(self):
        session = FakeSession(scripted_results=[{"rowcount": 3}])
        closed = marcar_ofertas_cerradas(
            session, fuente_id=7, urls_activas=["https://a", "https://b"]
        )
        self.assertEqual(closed, 3)
        self.assertEqual(session.commits, 1)

    def test_hashes_are_derived_from_urls(self):
        session = FakeSession(scripted_results=[{"rowcount": 0}])
        urls = ["https://a.cl/1", "https://b.cl/2"]
        marcar_ofertas_cerradas(session, fuente_id=5, urls_activas=urls)
        params = session.calls[0].params
        self.assertEqual(params["fid"], 5)
        self.assertEqual(params["hashes"], [url_a_hash(u) for u in urls])

    def test_sql_marks_rows_inactive(self):
        session = FakeSession(scripted_results=[{"rowcount": 1}])
        marcar_ofertas_cerradas(session, 1, ["https://x"])
        sql = session.calls[0].sql
        self.assertIn("UPDATE ofertas", sql)
        self.assertIn("activa", sql)

    def test_rollback_on_exception(self):
        session = FakeSession(raise_on_execute=RuntimeError("db down"))
        with self.assertRaises(RuntimeError):
            marcar_ofertas_cerradas(session, 1, ["https://x"])
        self.assertEqual(session.rollbacks, 1)
        self.assertEqual(session.commits, 0)


class RegistrarLogTests(unittest.TestCase):
    def test_writes_log_and_updates_fuentes(self):
        session = FakeSession(
            scripted_results=[{"rowcount": 1}, {"rowcount": 1}]
        )
        registrar_log(
            session,
            fuente_id=9,
            estado="OK",
            ofertas_nuevas=2,
            ofertas_actualizadas=3,
            ofertas_cerradas=1,
            paginas=10,
            duracion=4.2,
        )
        self.assertEqual(len(session.calls), 2)
        self.assertIn("INSERT INTO logs_scraping", session.calls[0].sql)
        self.assertIn("UPDATE fuentes", session.calls[1].sql)
        self.assertEqual(session.commits, 1)

    def test_duracion_is_rounded_to_2_decimals(self):
        session = FakeSession(scripted_results=[{"rowcount": 1}, {"rowcount": 1}])
        registrar_log(session, fuente_id=1, estado="OK", duracion=3.14159)
        self.assertEqual(session.calls[0].params["dur"], 3.14)

    def test_default_counters(self):
        session = FakeSession(scripted_results=[{"rowcount": 1}, {"rowcount": 1}])
        registrar_log(session, fuente_id=1, estado="OK")
        params = session.calls[0].params
        self.assertEqual(params["nuevas"], 0)
        self.assertEqual(params["actualizadas"], 0)
        self.assertEqual(params["cerradas"], 0)
        self.assertEqual(params["paginas"], 0)
        self.assertIsNone(params["error"])

    def test_error_state_is_passed_through(self):
        session = FakeSession(scripted_results=[{"rowcount": 1}, {"rowcount": 1}])
        registrar_log(
            session, fuente_id=1, estado="ERROR", error="timeout"
        )
        params = session.calls[0].params
        self.assertEqual(params["estado"], "ERROR")
        self.assertEqual(params["error"], "timeout")

    def test_rollback_on_exception(self):
        session = FakeSession(raise_on_execute=RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            registrar_log(session, fuente_id=1, estado="OK")
        self.assertEqual(session.rollbacks, 1)
        self.assertEqual(session.commits, 0)


if __name__ == "__main__":
    unittest.main()
