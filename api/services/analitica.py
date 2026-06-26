"""Analítica de tráfico del sitio — agregación interna + integración con Umami.

Dos fuentes complementarias alimentan la pestaña «Estadísticas del sitio»
del panel:

1. **Analítica propia** (`web_eventos`): el frontend envía un beacon a
   `POST /api/track` en cada vista de página y en eventos clave. Aquí se
   agregan esos registros (visitas, páginas top, referidos, dispositivos,
   ofertas más vistas). No se guarda IP ni datos personales.

2. **Umami** (opcional): si están definidas las variables de entorno
   `UMAMI_API_URL`, credenciales (`UMAMI_API_KEY` o
   `UMAMI_USERNAME`/`UMAMI_PASSWORD`) y `UMAMI_WEBSITE_ID`, se consultan
   sus métricas vía API y se muestran junto a las propias. Si Umami no
   está configurado o no responde, la sección se omite sin romper nada.

Todas las funciones son defensivas: si la tabla `web_eventos` aún no
existe (migración `20260626_0002` sin aplicar) devuelven estructuras
vacías con un `warning`, igual que hace `admin_audit`.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import date
from typing import Any

from api.services.db import execute_fetch_all, execute_fetch_one, get_cursor

logger = logging.getLogger("api.analitica")


# ── Registro de eventos (lado público) ───────────────────────────────────────

_BOT_RE = re.compile(
    r"bot|crawl|spider|slurp|bingpreview|facebookexternalhit|embedly|"
    r"quora|pinterest|vkshare|whatsapp|telegram|preview|monitor|lighthouse|"
    r"headless|python-requests|curl|wget|axios|go-http",
    re.IGNORECASE,
)

#: Eventos personalizados que aceptamos desde el beacon público. Cualquier
#: otro valor se descarta para evitar que se llene la tabla con basura.
EVENTOS_VALIDOS = {
    "ver_oferta", "click_postular", "click_bases", "suscribir_alerta",
    "buscar", "filtrar", "compartir", "ver_curso", "click_curso",
    "descargar_csv", "ver_institucion",
}

#: Secciones/paths que normalizamos para no explotar la cardinalidad
#: (las ofertas concretas se agrupan bajo `/oferta/:id`, etc.).
_PATH_NORMALIZE = [
    (re.compile(r"^/oferta/\d+.*"), "/oferta/:id"),
    (re.compile(r"^/institucion/\d+.*"), "/institucion/:id"),
    (re.compile(r"^/curso/[^/?#]+.*"), "/curso/:slug"),
]


def clasificar_dispositivo(user_agent: str) -> str:
    """Deriva una categoría gruesa de dispositivo del User-Agent (sin PII)."""
    ua = (user_agent or "").lower()
    if not ua or _BOT_RE.search(ua):
        return "bot"
    if "ipad" in ua or "tablet" in ua or ("android" in ua and "mobile" not in ua):
        return "tablet"
    if "mobi" in ua or "iphone" in ua or "android" in ua:
        return "movil"
    return "escritorio"


def es_bot(user_agent: str) -> bool:
    return clasificar_dispositivo(user_agent) == "bot"


def normalizar_path(path: str | None) -> str | None:
    if not path:
        return None
    # Quitar query string / fragmento y limitar longitud.
    limpio = re.split(r"[?#]", str(path).strip())[0][:300] or "/"
    if not limpio.startswith("/"):
        limpio = "/" + limpio
    for patron, reemplazo in _PATH_NORMALIZE:
        if patron.match(limpio):
            return reemplazo
    return limpio


def host_de_referrer(referrer: str | None) -> str | None:
    """Devuelve solo el dominio del referrer (nunca la URL completa)."""
    if not referrer:
        return None
    try:
        from urllib.parse import urlparse
        host = (urlparse(referrer).hostname or "").lower()
    except Exception:
        return None
    if not host:
        return None
    return host[:200]


def sesion_anonima(user_agent: str, ip: str) -> str:
    """Hash anónimo que rota a diario.

    No identifica a la persona: combina UA + IP + fecha del día y descarta
    el original. Solo sirve para aproximar «visitantes únicos por día». La
    IP nunca se guarda en claro.
    """
    semilla = f"{date.today().isoformat()}|{user_agent or ''}|{ip or ''}"
    return hashlib.sha256(semilla.encode("utf-8")).hexdigest()[:32]


def registrar_evento(
    *,
    tipo: str,
    path: str | None,
    evento: str | None,
    oferta_id: int | None,
    referrer: str | None,
    user_agent: str,
    ip: str,
) -> bool:
    """Inserta un registro en `web_eventos`. Best-effort: nunca lanza.

    Devuelve True si se guardó, False si se descartó (bot) o falló el insert.
    """
    if es_bot(user_agent):
        return False
    tipo = "evento" if tipo == "evento" else "pageview"
    if tipo == "evento":
        if evento not in EVENTOS_VALIDOS:
            return False
    else:
        evento = None
    try:
        oid = int(oferta_id) if oferta_id not in (None, "") else None
    except (TypeError, ValueError):
        oid = None
    try:
        with get_cursor() as (conn, cur):
            cur.execute(
                """INSERT INTO web_eventos
                       (tipo, path, evento, oferta_id, referrer_host, dispositivo, sesion)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                [
                    tipo,
                    normalizar_path(path),
                    (evento or None),
                    oid,
                    host_de_referrer(referrer),
                    clasificar_dispositivo(user_agent),
                    sesion_anonima(user_agent, ip),
                ],
            )
            conn.commit()
        return True
    except Exception as exc:  # tabla ausente o error transitorio
        logger.warning(f"[analitica] no se pudo registrar evento: {exc}")
        return False


# ── Agregación (lado admin) ──────────────────────────────────────────────────

def resumen_interno(dias: int = 30) -> dict[str, Any]:
    """Métricas agregadas de `web_eventos` para los últimos `dias` días."""
    dias = max(1, min(int(dias or 30), 365))
    try:
        totales = execute_fetch_one(
            """
            SELECT
                COUNT(*) FILTER (WHERE tipo = 'pageview') AS paginas_vistas,
                COUNT(DISTINCT sesion) FILTER (WHERE tipo = 'pageview') AS visitantes,
                COUNT(*) FILTER (WHERE tipo = 'evento') AS eventos,
                COUNT(*) FILTER (
                    WHERE tipo = 'pageview' AND ts >= CURRENT_DATE
                ) AS vistas_hoy,
                COUNT(DISTINCT sesion) FILTER (
                    WHERE tipo = 'pageview' AND ts >= CURRENT_DATE
                ) AS visitantes_hoy
            FROM web_eventos
            WHERE ts >= NOW() - make_interval(days => %s)
            """,
            [dias],
        ) or {}

        serie = execute_fetch_all(
            """
            SELECT TO_CHAR(DATE_TRUNC('day', ts), 'YYYY-MM-DD') AS dia,
                   COUNT(*) FILTER (WHERE tipo = 'pageview')        AS vistas,
                   COUNT(DISTINCT sesion) FILTER (WHERE tipo='pageview') AS visitantes
            FROM web_eventos
            WHERE ts >= NOW() - make_interval(days => %s)
            GROUP BY 1 ORDER BY 1 ASC
            """,
            [dias],
        )

        top_paginas = execute_fetch_all(
            """
            SELECT COALESCE(path, '(desconocido)') AS path, COUNT(*) AS vistas
            FROM web_eventos
            WHERE tipo = 'pageview' AND ts >= NOW() - make_interval(days => %s)
            GROUP BY 1 ORDER BY 2 DESC LIMIT 12
            """,
            [dias],
        )

        top_referidos = execute_fetch_all(
            """
            SELECT referrer_host AS host, COUNT(*) AS visitas
            FROM web_eventos
            WHERE tipo = 'pageview' AND referrer_host IS NOT NULL
              AND ts >= NOW() - make_interval(days => %s)
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """,
            [dias],
        )

        dispositivos = execute_fetch_all(
            """
            SELECT COALESCE(dispositivo, 'otro') AS dispositivo, COUNT(*) AS vistas
            FROM web_eventos
            WHERE tipo = 'pageview' AND ts >= NOW() - make_interval(days => %s)
            GROUP BY 1 ORDER BY 2 DESC
            """,
            [dias],
        )

        eventos_top = execute_fetch_all(
            """
            SELECT evento, COUNT(*) AS total
            FROM web_eventos
            WHERE tipo = 'evento' AND evento IS NOT NULL
              AND ts >= NOW() - make_interval(days => %s)
            GROUP BY 1 ORDER BY 2 DESC LIMIT 12
            """,
            [dias],
        )

        ofertas_top = execute_fetch_all(
            """
            SELECT w.oferta_id,
                   COALESCE(o.cargo, '(oferta eliminada)') AS cargo,
                   COALESCE(o.institucion_nombre, '') AS institucion,
                   COUNT(*) AS vistas
            FROM web_eventos w
            LEFT JOIN ofertas o ON o.id = w.oferta_id
            WHERE w.evento = 'ver_oferta' AND w.oferta_id IS NOT NULL
              AND w.ts >= NOW() - make_interval(days => %s)
            GROUP BY w.oferta_id, o.cargo, o.institucion_nombre
            ORDER BY vistas DESC LIMIT 10
            """,
            [dias],
        )

        return {
            "disponible": True,
            "dias": dias,
            "totales": totales,
            "serie": serie,
            "top_paginas": top_paginas,
            "top_referidos": top_referidos,
            "dispositivos": dispositivos,
            "eventos_top": eventos_top,
            "ofertas_top": ofertas_top,
        }
    except Exception as exc:
        logger.warning(f"[analitica] resumen interno no disponible: {exc}")
        return {
            "disponible": False,
            "dias": dias,
            "warning": "Tabla web_eventos no disponible — aplicar `alembic upgrade head`.",
        }


# ── Integración con Umami ────────────────────────────────────────────────────

def _umami_config() -> dict[str, str]:
    return {
        "api_url": os.getenv("UMAMI_API_URL", "").rstrip("/"),
        "api_key": os.getenv("UMAMI_API_KEY", "").strip(),
        "username": os.getenv("UMAMI_USERNAME", "").strip(),
        "password": os.getenv("UMAMI_PASSWORD", "").strip(),
        "website_id": os.getenv("UMAMI_WEBSITE_ID", "").strip(),
    }


def umami_configurado() -> bool:
    c = _umami_config()
    tiene_credenciales = bool(c["api_key"] or (c["username"] and c["password"]))
    return bool(c["api_url"] and c["website_id"] and tiene_credenciales)


def _umami_headers() -> dict[str, str] | None:
    """Resuelve los headers de autenticación de Umami (api-key o login)."""
    import requests

    c = _umami_config()
    if c["api_key"]:
        # Umami Cloud usa x-umami-api-key.
        return {"x-umami-api-key": c["api_key"]}
    if c["username"] and c["password"]:
        try:
            r = requests.post(
                f"{c['api_url']}/api/auth/login",
                json={"username": c["username"], "password": c["password"]},
                timeout=8,
            )
            r.raise_for_status()
            token = r.json().get("token")
            if token:
                return {"Authorization": f"Bearer {token}"}
        except Exception as exc:
            logger.warning(f"[analitica] login Umami falló: {exc}")
    return None


def resumen_umami(dias: int = 30) -> dict[str, Any]:
    """Consulta métricas de Umami vía API. Defensivo: nunca lanza."""
    if not umami_configurado():
        return {"configurado": False}
    try:
        import time as _time

        import requests

        c = _umami_config()
        headers = _umami_headers()
        if not headers:
            return {"configurado": True, "error": "No se pudo autenticar con Umami"}

        ahora_ms = int(_time.time() * 1000)
        desde_ms = ahora_ms - max(1, min(int(dias or 30), 365)) * 86_400_000
        base = f"{c['api_url']}/api/websites/{c['website_id']}"
        params = {"startAt": desde_ms, "endAt": ahora_ms}

        stats = requests.get(f"{base}/stats", headers=headers, params=params, timeout=8)
        stats.raise_for_status()
        data = stats.json()

        # Páginas más vistas (metric type=url).
        paginas: list[dict[str, Any]] = []
        try:
            pv = requests.get(
                f"{base}/metrics",
                headers=headers,
                params={**params, "type": "url", "limit": 10},
                timeout=8,
            )
            if pv.ok:
                paginas = pv.json()
        except Exception:
            paginas = []

        return {"configurado": True, "stats": data, "paginas": paginas, "dias": dias}
    except Exception as exc:
        logger.warning(f"[analitica] Umami no respondió: {exc}")
        return {"configurado": True, "error": f"Umami no respondió: {exc}"}
