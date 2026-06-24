"""
contrataoplanta — Validador de URLs de ofertas activas
=======================================================
Hace HEAD/GET a `url_oferta` y `url_bases` de las ofertas activas y guarda el
resultado en columnas `url_oferta_valida`, `url_bases_valida` y
`url_valida_chequeada_en` de la tabla `ofertas`.

Pensado para correr al final de `run_scrapers.py` o de forma manual.

Uso:
    python validate_offer_urls.py                  # valida todas las activas
    python validate_offer_urls.py --limit 100      # primeras N
    python validate_offer_urls.py --workers 20     # concurrencia
    python validate_offer_urls.py --max-edad-h 24  # solo si chequeado_en es viejo (default 24h)
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import logging
import random
import socket
import sys
import time
from datetime import datetime
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

import aiohttp

# Helper de cursor a Postgres. Lo importamos del módulo de DB (NO de api.main),
# para no arrastrar la app FastAPI ni la auth de admin (ADMIN_PASSWORD), que no
# existe en el runtime de scrapers y hacía fallar la validación de URLs.
sys.path.insert(0, str((__import__("pathlib").Path(__file__).resolve().parent)))
from api.services.db import get_cursor  # type: ignore  # noqa: E402

logger = logging.getLogger("validate_offer_urls")

# ── Configuración ────────────────────────────────────────────────────────
TIMEOUT_SEG = 12
DEFAULT_WORKERS = 20
#: Máximo de redirects a seguir manualmente. Cada salto se re-valida contra
#: el bloqueo SSRF, así que un 30x hacia una IP interna no puede colarse.
MAX_REDIRECTS = 5
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


def _ip_es_publica(ip_str: str) -> bool:
    """True si la IP es ruteable en internet (no privada/loopback/reservada)."""
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local      # 169.254.0.0/16 (incluye metadata de cloud)
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


async def _host_url_seguro(url: str, resolver=None) -> bool:
    """Resuelve el host de `url` y exige que TODAS sus IPs sean públicas.

    Defensa SSRF: las URLs vienen de scraping (no confiables). Sin esto, una
    `url_oferta` apuntando a `http://169.254.169.254/...` o a `localhost`
    haría que el validador conecte a recursos internos. Se rechaza si alguna
    resolución cae en rango privado/reservado (evita DNS rebinding parcial y
    registros A múltiples mezclando pública+privada).

    `resolver` es inyectable (async, firma `resolver(host) -> list[ip_str]`)
    para tests herméticos; por defecto usa `getaddrinfo` real.

    Nota: queda una ventana TOCTOU teórica (la IP usada al conectar podría
    diferir de la resuelta aquí). Para el modelo de amenaza de este job batch
    —que sólo guarda un booleano vivo/muerto, sin devolver cuerpo— es
    aceptable; cerrarla del todo exigiría un conector que fije la IP validada.
    """
    host = (urlparse(url).hostname or "").strip()
    if not host:
        return False
    # Si ya es una IP literal, validar directo.
    try:
        ipaddress.ip_address(host)
        return _ip_es_publica(host)
    except ValueError:
        pass
    try:
        if resolver is not None:
            ips = set(await resolver(host))
        else:
            loop = asyncio.get_running_loop()
            infos = await loop.getaddrinfo(host, None)
            ips = {info[4][0] for info in infos}
    except (socket.gaierror, OSError):
        return False
    if not ips:
        return False
    return all(_ip_es_publica(ip) for ip in ips)


async def _verificar_url(session: aiohttp.ClientSession, url: str,
                         semaforo: asyncio.Semaphore,
                         cache: dict[str, bool | None] | None = None) -> bool | None:
    """
    Devuelve True si la URL responde 2xx/3xx, False si 4xx/5xx o falla,
    None si la URL no es válida en formato (no se chequea, queda en NULL).

    El fragmento (#ancla) NO viaja al servidor, así que muchas ofertas de un
    mismo listado WordPress comparten la misma URL real (p.ej. `?page_id=89#a`,
    `?page_id=89#b`). Se normaliza quitando el fragmento y se cachea el
    resultado por URL real: así una página de 50 concursos se chequea UNA vez
    en lugar de 50 (evita el rate-limit que marcaba como "muerta" una URL viva).
    """
    if not _is_http_url(url):
        return None
    clave = urldefrag(url).url
    if cache is not None and clave in cache:
        return cache[clave]
    resultado = await _consultar_red(session, clave, semaforo)
    if cache is not None:
        cache[clave] = resultado
    return resultado


async def _consultar_red(session: aiohttp.ClientSession, url: str,
                         semaforo: asyncio.Semaphore) -> bool:
    headers = {"User-Agent": random.choice(USER_AGENTS),
               "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SEG)
    async with semaforo:
        # Seguimos redirects a mano (allow_redirects=False) revalidando cada
        # salto contra el bloqueo SSRF: un 30x hacia una IP interna no debe
        # poder colarse, y con allow_redirects=True aiohttp no da ese hook.
        current = url
        for _ in range(MAX_REDIRECTS + 1):
            if not await _host_url_seguro(current):
                logger.warning("URL bloqueada por destino no público: %s", current)
                return False
            metodo_resp = await _un_salto(session, current, headers, timeout)
            if metodo_resp is None:
                return False
            status, location = metodo_resp
            if status in (301, 302, 303, 307, 308) and location:
                current = urljoin(current, location)
                if not _is_http_url(current):
                    return False
                continue
            return status < 400
        logger.debug("Demasiados redirects para %s", url)
        return False


async def _un_salto(session: aiohttp.ClientSession, url: str,
                    headers: dict[str, str],
                    timeout: aiohttp.ClientTimeout) -> tuple[int, str | None] | None:
    """Hace UN request (HEAD, con fallback a GET) sin seguir redirects.

    Devuelve (status, location) o None si la conexión falla. `location` sólo
    viene en respuestas 30x.
    """
    # HEAD primero; si el server lo rechaza (403/405/501) reintentamos GET.
    try:
        async with session.head(url, headers=headers, allow_redirects=False,
                                timeout=timeout) as resp:
            if resp.status not in (403, 405, 501):
                return resp.status, _location_de(resp)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass
    # Fallback GET, con un reintento ante corte transitorio.
    for intento in (1, 2):
        try:
            async with session.get(url, headers=headers, allow_redirects=False,
                                   timeout=timeout) as resp:
                return resp.status, _location_de(resp)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if intento == 2:
                logger.debug("URL caída %s — %s", url, exc)
                return None
            await asyncio.sleep(1.0)
    return None


def _location_de(resp: Any) -> str | None:
    """Header Location de una respuesta, sólo si es un redirect (30x).

    Defensivo ante respuestas sin `.headers` (p.ej. mocks de tests que sólo
    exponen `.status`): en ese caso no hay redirect que seguir.
    """
    if getattr(resp, "status", 0) not in (301, 302, 303, 307, 308):
        return None
    headers = getattr(resp, "headers", None)
    if headers is None:
        return None
    return headers.get("Location")


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
                           semaforo: asyncio.Semaphore,
                           cache: dict[str, bool | None] | None = None,
                           ) -> tuple[int, bool | None, bool | None]:
    url_oferta = oferta.get("url_oferta")
    url_bases  = oferta.get("url_bases")
    # url_bases idéntica a url_oferta: chequear una sola vez y reutilizar.
    if url_bases and url_oferta and url_bases == url_oferta:
        v = await _verificar_url(session, url_oferta, semaforo, cache)
        return oferta["id"], v, v
    v_of = await _verificar_url(session, url_oferta, semaforo, cache)
    v_ba = await _verificar_url(session, url_bases,  semaforo, cache) if url_bases else None
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
    # Cache de resultados por URL real (sin fragmento), compartido en toda la
    # corrida: ofertas que comparten página se chequean una sola vez.
    cache: dict[str, bool | None] = {}

    async with aiohttp.ClientSession() as session:
        tareas = [_procesar_oferta(session, o, semaforo, cache) for o in ofertas]
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
