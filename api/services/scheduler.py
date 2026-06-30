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
    RETURNING modo, limite_fuentes;

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

#: Modos que se traducen a flags REALES de `scrapers/run_all.py`:
#:  - "completa": corrida completa (`--mode production`).
#:  - "sin_portal": excluye el portal central (`--skip-empleos-publicos`).
#: run_all.py no tiene filtro por «kind», así que no ofrecemos eso aquí.
_MODOS_VALIDOS = {"completa", "sin_portal"}

#: Centinela para distinguir «no se pasó el campo» de «se pasó como None/0».
_UNSET: Any = object()


def get_estado() -> dict[str, Any] | None:
    """Lee la fila singleton de `scheduler_state`. None si no existe la tabla."""
    try:
        from api.services.db import execute_fetch_one
        return execute_fetch_one(
            """SELECT id, activo, intervalo_horas, modo, limite_fuentes,
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
    limite_fuentes: Any = _UNSET,
    usuario: str = "ops",
) -> dict[str, Any]:
    """Actualiza la configuración del programador y recalcula la próxima corrida.

    Si se activa (o cambia el intervalo), `proxima_ejecucion` se fija a
    `NOW() + intervalo`. Si se desactiva, se deja en NULL. `limite_fuentes`
    nulo o 0 = sin límite (corre todas las instituciones del modo).
    """
    from api.services.db import execute_fetch_one, get_cursor

    if modo is not None and modo not in _MODOS_VALIDOS:
        raise ValueError(f"modo inválido: {modo}. Válidos: {sorted(_MODOS_VALIDOS)}")
    if intervalo_horas is not None:
        intervalo_horas = max(1, min(int(intervalo_horas), 168))

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
    if limite_fuentes is not _UNSET:
        # 0/None/negativo = sin límite (NULL); si no se proporcionó, no se toca.
        try:
            lf = int(limite_fuentes) if limite_fuentes not in (None, "") else 0
        except (TypeError, ValueError):
            lf = 0
        lf = None if lf <= 0 else min(lf, 2000)
        sets.append("limite_fuentes = %s")
        params.append(lf)
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
        """SELECT id, activo, intervalo_horas, modo, limite_fuentes,
                  proxima_ejecucion, ultima_ejecucion, actualizado_en, actualizado_por
           FROM scheduler_state WHERE id = 1""",
        [],
    ) or {}


def _claim_due_run() -> dict[str, Any] | None:
    """Reclama atómicamente una corrida pendiente. Devuelve {modo,limite_fuentes}
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
                RETURNING modo, limite_fuentes
                """,
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
    except Exception as exc:
        logger.warning("[scheduler] no se pudo reclamar corrida: %s", exc)
        return None


def _lanzar_run_all(modo: str, limite_fuentes: int | None) -> int | None:
    """Lanza `run_all.py` en background usando solo flags que el script soporta.

    `run_all.py` acepta `--mode`, `--limit`, `--ids`, `--skip-empleos-publicos`
    (no existe `--max` ni `--only-kind`). Por eso los modos se traducen a:
      - "completa"   → `--mode production`
      - "sin_portal" → `--mode production --skip-empleos-publicos`
    y `limite_fuentes` (si se fijó) acota la cantidad de instituciones con
    `--limit`.
    """
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(tz=timezone.utc)
    log_path = _LOG_DIR / f"scheduler-{modo}_{ahora.strftime('%Y%m%d_%H%M%S')}.log"
    run_all = str(_PROJECT_ROOT / "scrapers" / "run_all.py")
    cmd = [sys.executable, run_all, "--mode", "production"]
    if modo == "sin_portal":
        cmd.append("--skip-empleos-publicos")
    if limite_fuentes:
        cmd += ["--limit", str(int(limite_fuentes))]
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
                    _lanzar_run_all, claim.get("modo", "completa"),
                    claim.get("limite_fuentes"),
                )
        except asyncio.CancelledError:
            logger.info("[scheduler] loop detenido")
            raise
        except Exception as exc:  # nunca dejar morir el loop
            logger.warning("[scheduler] error en tick: %s", exc)
