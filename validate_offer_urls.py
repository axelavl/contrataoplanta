"""
contrataoplanta — Validador de URLs de ofertas activas
=======================================================
Hace HEAD/GET a `url_oferta` y `url_bases` de las ofertas activas y guarda el
resultado en columnas `url_oferta_valida`, `url_bases_valida` y
`url_valida_chequeada_en` de la tabla `ofertas`.

Reutiliza el patrón asíncrono de `verificar_urls.py`. Pensado para correr al
final de `run_scrapers.py` o de forma manual.

Uso:
    python validate_offer_urls.py                  # valida todas las activas
    python validate_offer_urls.py --limit 100      # primeras N
    python validate_offer_urls.py --workers 20     # concurrencia
    python validate_offer_urls.py --max-edad-h 24  # solo si chequeado_en es viejo (default 24h)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sys
import time
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import aiohttp

# Importamos los helpers del API (conexión a Postgres, get_cursor, etc.).
sys.path.insert(0, str((__import__("pathlib").Path(__file__).resolve().parent)))
from api.main import get_cursor  # type: ignore  # noqa: E402

logger = logging.getLogger("validate_offer_urls")

# ── Configuración ────────────────────────────────────────────────────────
TIMEOUT_SEG = 12
DEFAULT_WORKERS = 20
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
]


def _is_http_url(value: Any) -> bool:
    if not value or not isinstance(value, str):
        return False
    s = value.strip()
    if not s or s == "#" or s.lower().startswith("javascript:"):
        return False
    try:
        parsed = urlparse(s)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


async def _verificar_url(session: aiohttp.ClientSession, url: str,
                         semaforo: asyncio.Semaphore) -> bool | None:
    """
    Devuelve True si la URL responde 2xx/3xx, False si 4xx/5xx o falla,
    None si la URL no es válida en formato (no se chequea, queda en NULL).
    """
    if not _is_http_url(url):
        return None
    headers = {"User-Agent": random.choice(USER_AGENTS),
               "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}
    async with semaforo:
        # Primero HEAD; si devuelve 405/501/403 se reintenta con GET.
        try:
            async with session.head(url, headers=headers, allow_redirects=True,
                                    timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEG)) as resp:
                if resp.status < 400:
                    return True
                if resp.status not in (403, 405, 501):
                    return False
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        # Fallback: GET (algunos servidores rechazan HEAD).
        try:
            async with session.get(url, headers=headers, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEG)) as resp:
                return resp.status < 400
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.debug("URL caída %s — %s", url, exc)
            return False


def _ofertas_a_validar(limit: int | None, max_edad_h: int) -> list[dict[str, Any]]:
    sql = """
        SELECT id, url_oferta, url_bases
        FROM ofertas
        WHERE COALESCE(estado, 'activo') = 'activo'
          AND (url_valida_chequeada_en IS NULL
               OR url_valida_chequeada_en < NOW() - (%s || ' hours')::INTERVAL)
        ORDER BY url_valida_chequeada_en NULLS FIRST, id
    """
    params: list[Any] = [str(int(max_edad_h))]
    if limit:
        sql += " LIMIT %s"
        params.append(int(limit))
    with get_cursor() as (_, cursor):
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def _actualizar_resultado(oferta_id: int,
                          url_oferta_valida: bool | None,
                          url_bases_valida: bool | None) -> None:
    sql = """
        UPDATE ofertas
           SET url_oferta_valida = %s,
               url_bases_valida  = %s,
               url_valida_chequeada_en = NOW()
         WHERE id = %s
    """
    with get_cursor() as (conn, cursor):
        cursor.execute(sql, [url_oferta_valida, url_bases_valida, oferta_id])
        conn.commit()


async def _procesar_oferta(session: aiohttp.ClientSession, oferta: dict[str, Any],
                           semaforo: asyncio.Semaphore) -> tuple[int, bool | None, bool | None]:
    url_oferta = oferta.get("url_oferta")
    url_bases  = oferta.get("url_bases")
    # url_bases idéntica a url_oferta: chequear una sola vez y reutilizar.
    if url_bases and url_oferta and url_bases == url_oferta:
        v = await _verificar_url(session, url_oferta, semaforo)
        return oferta["id"], v, v
    v_of = await _verificar_url(session, url_oferta, semaforo)
    v_ba = await _verificar_url(session, url_bases,  semaforo) if url_bases else None
    return oferta["id"], v_of, v_ba


async def _main_async(limit: int | None, workers: int, max_edad_h: int) -> dict[str, int]:
    ofertas = _ofertas_a_validar(limit, max_edad_h)
    if not ofertas:
        logger.info("No hay ofertas para validar (todas chequeadas en últimas %sh).", max_edad_h)
        return {"total": 0, "ok": 0, "rotas": 0, "sin_url": 0}

    logger.info("Validando %d ofertas con %d workers…", len(ofertas), workers)
    semaforo = asyncio.Semaphore(workers)
    inicio = time.time()
    contadores = {"total": len(ofertas), "ok": 0, "rotas": 0, "sin_url": 0}

    async with aiohttp.ClientSession() as session:
        tareas = [_procesar_oferta(session, o, semaforo) for o in ofertas]
        for completada in asyncio.as_completed(tareas):
            oferta_id, v_of, v_ba = await completada
            _actualizar_resultado(oferta_id, v_of, v_ba)
            estados = [v_of, v_ba]
            if all(v is None for v in estados):
                contadores["sin_url"] += 1
            elif any(v is False for v in estados):
                contadores["rotas"] += 1
            else:
                contadores["ok"] += 1

    duracion = time.time() - inicio
    logger.info("Validación completa en %.1fs — %s", duracion, contadores)
    return contadores


def main(limit: int | None = None, workers: int = DEFAULT_WORKERS,
         max_edad_h: int = 24) -> dict[str, int]:
    """Punto de entrada para uso programático (e.g. desde run_scrapers.py)."""
    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s — %(message)s",
                        datefmt="%H:%M:%S")
    return asyncio.run(_main_async(limit, workers, max_edad_h))


def _cli() -> None:
    p = argparse.ArgumentParser(description="Valida URLs de ofertas activas.")
    p.add_argument("--limit", type=int, default=None,
                   help="Limita a N ofertas (default: todas las elegibles).")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                   help=f"Concurrencia (default: {DEFAULT_WORKERS}).")
    p.add_argument("--max-edad-h", type=int, default=24,
                   help="Sólo re-validar si la última verificación es más vieja que N horas (default: 24).")
    args = p.parse_args()
    resultado = main(limit=args.limit, workers=args.workers, max_edad_h=args.max_edad_h)
    print(f"\nResumen: {resultado}")


if __name__ == "__main__":
    _cli()
