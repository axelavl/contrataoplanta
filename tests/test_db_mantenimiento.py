"""Retención de datos (db/mantenimiento.py) contra un Postgres real.

Requiere la base de tests (DATABASE_URL / DB_* de conftest); si no hay
conexión, se omite como el resto de tests de base de datos.
"""
from __future__ import annotations

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from db import mantenimiento  # noqa: E402


@pytest.fixture
def conn():
    from db.config import DB_CONFIG
    try:
        c = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
    except Exception:
        pytest.skip("sin Postgres de tests")
    with c.cursor() as cur:
        cur.execute("SELECT to_regclass('public.source_evaluations')")
        if cur.fetchone()[0] is None:
            c.close()
            pytest.skip("schema sin migrar")
        cur.execute("DELETE FROM source_evaluations; DELETE FROM scraper_descartes; DELETE FROM web_eventos")
    c.commit()
    yield c
    c.close()


def _evaluacion(cur, inst, dias):
    cur.execute(
        """INSERT INTO source_evaluations
           (institucion_id, source_url, availability, page_type, job_relevance,
            open_calls_status, validity_status, decision, confidence, retry_policy, evaluated_at)
           VALUES (%s, 'https://x', 'ok', 'p', 'r', 'o', 'v', 'd', 0.5, 'normal',
                   NOW() - make_interval(days => %s)) RETURNING id""",
        (inst, dias),
    )
    return cur.fetchone()[0]


def test_retencion_conserva_ultima_evaluacion_por_fuente(conn):
    with conn.cursor() as cur:
        vieja_1 = _evaluacion(cur, 1, 90)
        ultima_1 = _evaluacion(cur, 1, 60)      # vieja, pero es la última de la fuente 1
        reciente_2 = _evaluacion(cur, 2, 5)
        vieja_2 = _evaluacion(cur, 2, 40)
        sin_inst = _evaluacion(cur, None, 40)
        cur.execute("INSERT INTO scraper_descartes (motivo, fecha_descarte) VALUES "
                    "('vencida', NOW() - interval '31 days'), ('vencida', NOW())")
        cur.execute("INSERT INTO web_eventos (ts) VALUES (NOW() - interval '700 days'), "
                    "(NOW() - interval '731 days')")
    conn.commit()

    a_borrar = mantenimiento.purgar(conn, dry_run=True)
    assert a_borrar["source_evaluations"] == 3
    assert a_borrar["scraper_descartes"] == 1 and a_borrar["web_eventos"] == 1

    borradas = mantenimiento.purgar(conn)
    assert {k: borradas[k] for k in a_borrar} == a_borrar
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM source_evaluations ORDER BY id")
        quedan = {r[0] for r in cur.fetchall()}
    assert quedan == {ultima_1, reciente_2}
    assert not quedan & {vieja_1, vieja_2, sin_inst}
    # idempotente
    assert sum(mantenimiento.purgar(conn).values()) == 0


def test_tabla_inexistente_no_rompe(conn, monkeypatch):
    monkeypatch.setattr(mantenimiento, "RETENCIONES", (
        mantenimiento.Retencion("tabla_que_no_existe", "ts", 1),
        mantenimiento.Retencion("scraper_descartes", "fecha_descarte", 30),
    ))
    res = mantenimiento.purgar(conn)
    assert "tabla_que_no_existe" not in res and "scraper_descartes" in res
