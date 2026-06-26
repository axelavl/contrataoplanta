"""Programador propio de recolecciones (in-app).

Un loop en segundo plano (lanzado en el startup de `api/main.py`) consulta
la tabla singleton `scheduler_state` y, cuando llega la hora, dispara una
corrida de `scrapers/run_all.py` igual que el botón «Ejecutar ahora» del
panel.

Seguridad multi-worker: uvicorn corre con varios workers, así que el loop
vive en cada uno. Para que la corrida NO se dispare N veces, el «claim» es
una única sentencia atómica:

    UPDATE scheduler_state
       SET ultima_ejecucion = NOW(),
           proxima_ejecucion = NOW() + intervalo
     WHERE activo AND proxima_ejecucion <= NOW()
    RETURNING modo, max_offers;

Postgres bloquea la fila durante el UPDATE, así que solo un worker recibe
la fila (y por tanto lanza el proceso); el resto no toca nada.

Convive con el timer de systemd: ambos llaman al mismo `run_all.py`. Está
DESACTIVADO por defecto (`activo = FALSE`); actívalo desde el panel solo
si quieres que la app también dispare recolecciones.
"""
from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("api.scheduler")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_LOG_DIR = _PROJECT_ROOT / "logs" / "admin_runs"

#: Cada cuánto revisa el loop si toca correr (segundos).
_TICK_SEG = 60

_MODOS_VALIDOS = {"all", "empleos_publicos", "wordpress", "generic"}


def get_estado() -> dict[str, Any] | None:
    """Lee la fila singleton de `scheduler_state`. None si no existe la tabla."""
    try:
        from api.services.db import execute_fetch_one
        return execute_fetch_one(
            """SELECT id, activo, intervalo_horas, modo, max_offers,
                      proxima_ejecucion, ultima_ejecucion,
                      actualizado_en, actualizado_por
               FROM scheduler_state WHERE id = 1""",
            [],
        )
    except Exception as exc:  # tabla ausente / DB caída
        logger.warning("[scheduler] estado no disponible: %s", exc)
        return None


def set_estado(
    *,
    activo: bool | None = None,
    intervalo_horas: int | None = None,
    modo: str | None = None,
    max_offers: int | None = None,
    usuario: str = "ops",
) -> dict[str, Any]:
    """Actualiza la configuración del programador y recalcula la próxima corrida.

    Si se activa (o cambia el intervalo), `proxima_ejecucion` se fija a
    `NOW() + intervalo`. Si se desactiva, se deja en NULL.
    """
    from api.services.db import execute_fetch_one, get_cursor

    if modo is not None and modo not in _MODOS_VALIDOS:
        raise ValueError(f"modo inválido: {modo}. Válidos: {sorted(_MODOS_VALIDOS)}")
    if intervalo_horas is not None:
        intervalo_horas = max(1, min(int(intervalo_horas), 168))
    if max_offers is not None:
        max_offers = max(1, min(int(max_offers), 500))

    sets: list[str] = []
    params: list[Any] = []
    if activo is not None:
        sets.append("activo = %s")
        params.append(bool(activo))
    if intervalo_horas is not None:
        sets.append("intervalo_horas = %s")
        params.append(intervalo_horas)
    if modo is not None:
        sets.append("modo = %s")
        params.append(modo)
    if max_offers is not None:
        sets.append("max_offers = %s")
        params.append(max_offers)
    sets.append("actualizado_en = NOW()")
    sets.append("actualizado_por = %s")
    params.append(usuario)

    # Recalcular próxima ejecución según el estado resultante.
    with get_cursor() as (conn, cur):
        cur.execute(
            f"UPDATE scheduler_state SET {', '.join(sets)} WHERE id = 1", params
        )
        # Releer para decidir proxima_ejecucion con los valores ya aplicados.
        cur.execute(
            "SELECT activo, intervalo_horas FROM scheduler_state WHERE id = 1"
        )
        row = cur.fetchone()
        row = dict(row) if row else {}
        if row.get("activo"):
            cur.execute(
                "UPDATE scheduler_state "
                "SET proxima_ejecucion = NOW() + make_interval(hours => intervalo_horas) "
                "WHERE id = 1 AND (proxima_ejecucion IS NULL OR %s)",
                [intervalo_horas is not None],
            )
        else:
            cur.execute("UPDATE scheduler_state SET proxima_ejecucion = NULL WHERE id = 1")
        conn.commit()
    return execute_fetch_one(
        """SELECT id, activo, intervalo_horas, modo, max_offers,
                  proxima_ejecucion, ultima_ejecucion, actualizado_en, actualizado_por
           FROM scheduler_state WHERE id = 1""",
        [],
    ) or {}


def _claim_due_run() -> dict[str, Any] | None:
    """Reclama atómicamente una corrida pendiente. Devuelve {modo,max_offers}
    si este worker ganó el claim, o None."""
    try:
        from api.services.db import get_cursor
        with get_cursor() as (conn, cur):
            cur.execute(
                """
                UPDATE scheduler_state
                   SET ultima_ejecucion = NOW(),
                       proxima_ejecucion = NOW() + make_interval(hours => intervalo_horas)
                 WHERE id = 1 AND activo = TRUE
                   AND proxima_ejecucion IS NOT NULL
                   AND proxima_ejecucion <= NOW()
                RETURNING modo, max_offers
                """,
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
    except Exception as exc:
        logger.warning("[scheduler] no se pudo reclamar corrida: %s", exc)
        return None


def _lanzar_run_all(modo: str, max_offers: int) -> int | None:
    """Lanza `run_all.py` en background (mismo patrón que el panel)."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(tz=timezone.utc)
    log_path = _LOG_DIR / f"scheduler-{modo}_{ahora.strftime('%Y%m%d_%H%M%S')}.log"
    run_all = str(_PROJECT_ROOT / "scrapers" / "run_all.py")
    cmd = [sys.executable, run_all, "--mode", "production", "--max", str(max_offers)]
    if modo == "all":
        pass  # corrida completa
    elif modo in {"empleos_publicos", "wordpress", "generic"}:
        cmd += ["--only-kind", modo]
        if modo != "empleos_publicos":
            cmd.append("--skip-empleos-publicos")
    try:
        with open(log_path, "a", encoding="utf-8") as log_f:
            proc = subprocess.Popen(
                cmd, stdout=log_f, stderr=subprocess.STDOUT,
                cwd=str(_PROJECT_ROOT), text=True,
            )
            log_f.write(
                f"### ADMIN-RUN pid={proc.pid} tipo=scheduler-{modo} "
                f"started={ahora.isoformat()} cmd={' '.join(cmd[1:])}\n"
            )
        # Registrar en scraper_runs para que aparezca en el historial.
        try:
            from api.services.db import get_cursor
            with get_cursor() as (conn, cur):
                cur.execute(
                    "INSERT INTO scraper_runs (started_at, status, run_mode, notas) "
                    "VALUES (NOW(), 'en_curso', %s, %s)",
                    [f"scheduled-{modo}", f"pid={proc.pid} (programador)"],
                )
                conn.commit()
        except Exception:
            pass
        logger.info("[scheduler] corrida lanzada modo=%s pid=%s", modo, proc.pid)
        return proc.pid
    except Exception as exc:
        logger.error("[scheduler] no se pudo lanzar run_all: %s", exc)
        return None


async def scheduler_loop() -> None:
    """Loop infinito: cada `_TICK_SEG` revisa y, si toca, lanza la corrida."""
    logger.info("[scheduler] loop iniciado (tick=%ss)", _TICK_SEG)
    while True:
        try:
            await asyncio.sleep(_TICK_SEG)
            claim = await asyncio.to_thread(_claim_due_run)
            if claim:
                await asyncio.to_thread(
                    _lanzar_run_all, claim.get("modo", "empleos_publicos"),
                    int(claim.get("max_offers") or 50),
                )
        except asyncio.CancelledError:
            logger.info("[scheduler] loop detenido")
            raise
        except Exception as exc:  # nunca dejar morir el loop
            logger.warning("[scheduler] error en tick: %s", exc)
