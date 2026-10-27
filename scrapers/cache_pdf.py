"""Caché del texto extraído de PDFs de bases/perfiles, guardado en Postgres.

Por qué existe
--------------
Los scrapers que salen por el proxy residencial (Carabineros, PDI, Armada)
volvían a descargar y leer los mismos PDF en cada corrida. Esos PDF son la
mayor parte del tráfico del proxy (≈220 KB por request en Carabineros, que se
paga a $8/GB) y las bases de un concurso no cambian mientras está abierto.

Ahora, antes de descargar se consulta la tabla `cache_textos_pdf`; si el
texto de esa URL ya fue extraído (en el mismo modo: texto u OCR), se reutiliza
sin tocar la red. Se guarda el TEXTO, no el archivo: unos pocos KB por PDF.

Se usa Postgres y no el disco porque en Railway el contenedor de scrapers
parte vacío en cada corrida del cron.

Robustez
--------
La caché es una optimización, nunca un requisito: si la base no responde, la
tabla no existe (migración sin aplicar) o el modo standalone no tiene DB, se
desactiva en silencio para el resto de la corrida y todo funciona como antes.

Uso
---
    from scrapers import cache_pdf

    cache_pdf.configurar(escribir=not dry_run)       # al inicio de ejecutar()
    texto = cache_pdf.texto_pdf(
        url,
        descargar=lambda u: (r.content if (r := _get(session, u)) else None),
        extraer=_pdf_a_texto,
        modo="texto",            # "ocr" si se extrae con OCR
    )
    # texto: str (puede ser "") si se obtuvo; None si la descarga falló.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Callable

logger = logging.getLogger("scrapers.cache_pdf")

#: Días que una entrada se considera vigente. Los concursos duran semanas;
#: pasado este plazo se vuelve a descargar (y la entrada vieja se purga).
TTL_DIAS = 60
#: Tope de texto guardado por PDF, para que un PDF enorme no infle la tabla.
MAX_CHARS = 200_000

_estado: dict = {"conn": None, "deshabilitada": False, "escribir": True,
                 "purgada": False, "hits": 0, "misses": 0}


def configurar(*, escribir: bool = True) -> None:
    """Llamar al inicio de cada corrida. Con escribir=False (dry-run) la
    caché se lee pero no se escribe."""
    _estado["escribir"] = escribir
    _estado["hits"] = 0
    _estado["misses"] = 0


def resumen() -> str:
    return (f"cache_pdf: {_estado['hits']} reutilizados, "
            f"{_estado['misses']} descargados")


def _hash(url: str) -> str:
    return hashlib.sha256(url.strip().encode("utf-8")).hexdigest()


def _deshabilitar(motivo: str) -> None:
    if not _estado["deshabilitada"]:
        logger.info("cache_pdf deshabilitada para esta corrida: %s", motivo)
    _estado["deshabilitada"] = True


def _conexion():
    """Conexión autocommit propia (no comparte transacción con los upserts)."""
    if _estado["deshabilitada"]:
        return None
    if _estado["conn"] is not None and not _estado["conn"].closed:
        return _estado["conn"]
    try:
        import psycopg2
        from db.config import DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
        conn.autocommit = True
        _estado["conn"] = conn
        return conn
    except Exception as exc:  # sin DB (standalone) o DB caída
        _deshabilitar(type(exc).__name__)
        return None


def _purgar_vencidas(cur) -> None:
    if _estado["purgada"] or not _estado["escribir"]:
        return
    cur.execute(
        "DELETE FROM cache_textos_pdf "
        "WHERE creado_en < NOW() - make_interval(days => %s)",
        (TTL_DIAS,),
    )
    _estado["purgada"] = True


def leer(url: str, modo: str = "texto") -> str | None:
    conn = _conexion()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            _purgar_vencidas(cur)
            cur.execute(
                "SELECT texto FROM cache_textos_pdf "
                "WHERE url_hash = %s AND modo = %s "
                "AND creado_en >= NOW() - make_interval(days => %s)",
                (_hash(url), modo, TTL_DIAS),
            )
            fila = cur.fetchone()
            return fila[0] if fila else None
    except Exception as exc:  # p. ej. tabla inexistente
        _deshabilitar(f"{type(exc).__name__}: {exc}".splitlines()[0][:120])
        return None


def guardar(url: str, texto: str, modo: str = "texto",
            bytes_pdf: int | None = None) -> None:
    if not _estado["escribir"]:
        return
    conn = _conexion()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cache_textos_pdf (url_hash, modo, url, texto, bytes_pdf) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (url_hash, modo) DO UPDATE SET "
                "texto = EXCLUDED.texto, bytes_pdf = EXCLUDED.bytes_pdf, "
                "creado_en = NOW()",
                (_hash(url), modo, url[:2000], (texto or "")[:MAX_CHARS], bytes_pdf),
            )
    except Exception as exc:
        _deshabilitar(f"{type(exc).__name__}: {exc}".splitlines()[0][:120])


def descargador(get_fn: Callable, session, delay: float = 0.0
                ) -> Callable[[str], bytes | None]:
    """Adapta el `_get(session, url)` de cada scraper al formato que espera
    `texto_pdf`. El `delay` (cortesía con el sitio) solo se aplica cuando hay
    descarga real: un hit de caché no espera ni toca la red."""
    import time

    def _bajar(url: str) -> bytes | None:
        if delay:
            time.sleep(delay)
        r = get_fn(session, url)
        return r.content if r is not None else None
    return _bajar


def texto_pdf(url: str,
              descargar: Callable[[str], bytes | None],
              extraer: Callable[[bytes], str],
              *, modo: str = "texto") -> str | None:
    """Texto del PDF en `url`, desde la caché o descargándolo.

    - Hit: devuelve el texto guardado sin descargar (puede ser "", p. ej. un
      PDF escaneado leído sin OCR: también vale la pena no volver a bajarlo).
    - Miss: descarga con `descargar`, extrae con `extraer`, guarda y devuelve.
    - Descarga fallida: None, y no se guarda nada (se reintenta la próxima).
    """
    if not url:
        return None
    guardado = leer(url, modo)
    if guardado is not None:
        _estado["hits"] += 1
        return guardado
    contenido = descargar(url)
    if not contenido:
        return None
    _estado["misses"] += 1
    # Si no llegó un PDF (página de bloqueo del WAF, error HTML con status
    # 200, captcha…) no se cachea: quedaría "resuelto" por TTL_DIAS días.
    if not contenido[:1024].lstrip().startswith(b"%PDF"):
        return ""
    try:
        texto = extraer(contenido) or ""
    except Exception as exc:
        logger.debug("cache_pdf: error extrayendo %s: %s", url, exc)
        return ""
    guardar(url, texto, modo, len(contenido))
    return texto
