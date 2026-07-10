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


def _ips_excluidas() -> set[str]:
    """Lee las IPs excluidas de site_config (clave 'ips_excluidas', CSV)."""
    try:
        row = execute_fetch_one(
            "SELECT valor FROM site_config WHERE clave = 'ips_excluidas'", []
        )
        if row and row.get("valor"):
            return {ip.strip() for ip in row["valor"].split(",") if ip.strip()}
    except Exception:
        pass
    return set()


_PAIS_NOMBRES: dict[str, str] = {
    "CL": "Chile", "AR": "Argentina", "PE": "Perú", "CO": "Colombia",
    "MX": "México", "BR": "Brasil", "EC": "Ecuador", "VE": "Venezuela",
    "BO": "Bolivia", "UY": "Uruguay", "PY": "Paraguay", "US": "EE.UU.",
    "ES": "España", "DE": "Alemania", "FR": "Francia", "GB": "Reino Unido",
    "CA": "Canadá", "AU": "Australia", "IT": "Italia", "PT": "Portugal",
    "CR": "Costa Rica", "PA": "Panamá", "DO": "Rep. Dominicana",
    "GT": "Guatemala", "HN": "Honduras", "SV": "El Salvador",
    "NI": "Nicaragua", "CU": "Cuba", "PR": "Puerto Rico",
}


def nombre_pais(codigo: str | None) -> str:
    """Devuelve nombre legible de un código ISO 2 letras."""
    if not codigo:
        return "Desconocido"
    return _PAIS_NOMBRES.get(codigo.upper(), codigo.upper())


def registrar_evento(
    *,
    tipo: str,
    path: str | None,
    evento: str | None,
    oferta_id: int | None,
    referrer: str | None,
    user_agent: str,
    ip: str,
    pais: str | None = None,
    ciudad: str | None = None,
) -> bool:
    """Inserta un registro en `web_eventos`. Best-effort: nunca lanza.

    Devuelve True si se guardó, False si se descartó (bot) o falló el insert.
    ``pais`` y ``ciudad`` vienen de headers de Cloudflare (CF-IPCountry,
    CF-IPCity) o similares del proxy. No se almacena la IP.
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
    excluida = ip in _ips_excluidas() if ip else False
    pais_clean = (pais or "")[:2].upper() or None
    if pais_clean == "XX":
        pais_clean = None
    ciudad_clean = (ciudad or "")[:100].strip() or None
    try:
        with get_cursor() as (conn, cur):
            cur.execute(
                """INSERT INTO web_eventos
                       (tipo, path, evento, oferta_id, referrer_host,
                        dispositivo, sesion, excluida, pais, ciudad)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                [
                    tipo,
                    normalizar_path(path),
                    (evento or None),
                    oid,
                    host_de_referrer(referrer),
                    clasificar_dispositivo(user_agent),
                    sesion_anonima(user_agent, ip),
                    excluida,
                    pais_clean,
                    ciudad_clean,
                ],
            )
            conn.commit()
        return True
    except Exception as exc:  # tabla ausente o error transitorio
        logger.warning(f"[analitica] no se pudo registrar evento: {exc}")
        return False


# ── Agregación (lado admin) ──────────────────────────────────────────────────

def resumen_interno(
    dias: int = 30,
    *,
    excluir_propias: bool = True,
    granularidad: str = "dia",
) -> dict[str, Any]:
    """Métricas agregadas de `web_eventos` para los últimos `dias` días.

    ``excluir_propias``: filtra eventos marcados como ``excluida=TRUE``
    (IPs del admin configuradas en site_config 'ips_excluidas').

    ``granularidad``: 'dia' o 'semana' para la serie temporal.
    """
    dias = max(1, min(int(dias or 30), 365))
    filtro_excl = "AND NOT COALESCE(excluida, FALSE)" if excluir_propias else ""
    trunc = "week" if granularidad == "semana" else "day"
    try:
        totales = execute_fetch_one(
            f"""
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
            WHERE ts >= NOW() - make_interval(days => %s) {filtro_excl}
            """,
            [dias],
        ) or {}

        # Totales del período anterior (misma ventana desplazada) para comparación.
        totales_prev = execute_fetch_one(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE tipo = 'pageview') AS paginas_vistas,
                COUNT(DISTINCT sesion) FILTER (WHERE tipo = 'pageview') AS visitantes,
                COUNT(*) FILTER (WHERE tipo = 'evento') AS eventos
            FROM web_eventos
            WHERE ts >= NOW() - make_interval(days => %s)
              AND ts < NOW() - make_interval(days => %s) {filtro_excl}
            """,
            [dias * 2, dias],
        ) or {}

        serie = execute_fetch_all(
            f"""
            SELECT TO_CHAR(DATE_TRUNC('{trunc}', ts), 'YYYY-MM-DD') AS dia,
                   COUNT(*) FILTER (WHERE tipo = 'pageview')              AS vistas,
                   COUNT(DISTINCT sesion) FILTER (WHERE tipo='pageview')   AS visitantes,
                   COUNT(*) FILTER (WHERE tipo = 'evento')                AS eventos
            FROM web_eventos
            WHERE ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1 ORDER BY 1 ASC
            """,
            [dias],
        )

        top_paginas = execute_fetch_all(
            f"""
            SELECT COALESCE(path, '(desconocido)') AS path, COUNT(*) AS vistas
            FROM web_eventos
            WHERE tipo = 'pageview' AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 12
            """,
            [dias],
        )

        top_referidos = execute_fetch_all(
            f"""
            SELECT referrer_host AS host, COUNT(*) AS visitas
            FROM web_eventos
            WHERE tipo = 'pageview' AND referrer_host IS NOT NULL
              AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """,
            [dias],
        )

        dispositivos = execute_fetch_all(
            f"""
            SELECT COALESCE(dispositivo, 'otro') AS dispositivo, COUNT(*) AS vistas
            FROM web_eventos
            WHERE tipo = 'pageview' AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1 ORDER BY 2 DESC
            """,
            [dias],
        )

        eventos_top = execute_fetch_all(
            f"""
            SELECT evento, COUNT(*) AS total
            FROM web_eventos
            WHERE tipo = 'evento' AND evento IS NOT NULL
              AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 12
            """,
            [dias],
        )

        ofertas_top = execute_fetch_all(
            f"""
            SELECT w.oferta_id,
                   COALESCE(o.cargo, '(oferta eliminada)') AS cargo,
                   COALESCE(o.institucion_nombre, '') AS institucion,
                   COUNT(*) AS vistas
            FROM web_eventos w
            LEFT JOIN ofertas o ON o.id = w.oferta_id
            WHERE w.evento = 'ver_oferta' AND w.oferta_id IS NOT NULL
              AND w.ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY w.oferta_id, o.cargo, o.institucion_nombre
            ORDER BY vistas DESC LIMIT 10
            """,
            [dias],
        )

        embudo_row = execute_fetch_one(
            f"""
            SELECT
                COUNT(DISTINCT sesion)                                            AS visitaron,
                COUNT(DISTINCT sesion) FILTER (WHERE evento = 'ver_oferta')       AS vieron_oferta,
                COUNT(DISTINCT sesion) FILTER (WHERE evento = 'click_postular')   AS postularon,
                COUNT(DISTINCT sesion) FILTER (WHERE evento = 'click_bases')      AS vieron_bases,
                COUNT(DISTINCT sesion) FILTER (WHERE evento = 'suscribir_alerta') AS se_suscribieron
            FROM web_eventos
            WHERE sesion IS NOT NULL AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            """,
            [dias],
        ) or {}
        base = int(embudo_row.get("visitaron") or 0) or 1
        embudo = [
            {"paso": "Visitaron el sitio", "sesiones": int(embudo_row.get("visitaron") or 0)},
            {"paso": "Vieron una oferta", "sesiones": int(embudo_row.get("vieron_oferta") or 0)},
            {"paso": "Revisaron las bases", "sesiones": int(embudo_row.get("vieron_bases") or 0)},
            {"paso": "Clic en Postular", "sesiones": int(embudo_row.get("postularon") or 0)},
            {"paso": "Se suscribieron a alertas", "sesiones": int(embudo_row.get("se_suscribieron") or 0)},
        ]
        for paso in embudo:
            paso["pct"] = round(paso["sesiones"] * 100 / base, 1)

        # Horas pico: distribución de pageviews por hora del día.
        horas = execute_fetch_all(
            f"""
            SELECT EXTRACT(HOUR FROM ts)::int AS hora, COUNT(*) AS vistas
            FROM web_eventos
            WHERE tipo = 'pageview' AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1 ORDER BY 1
            """,
            [dias],
        )

        # Días de la semana más activos.
        dias_semana = execute_fetch_all(
            f"""
            SELECT TO_CHAR(ts, 'Dy') AS dia_semana,
                   EXTRACT(ISODOW FROM ts)::int AS dow,
                   COUNT(*) AS vistas
            FROM web_eventos
            WHERE tipo = 'pageview' AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1, 2 ORDER BY 2
            """,
            [dias],
        )

        # Geolocalización: países y ciudades más frecuentes.
        paises = execute_fetch_all(
            f"""
            SELECT COALESCE(pais, 'XX') AS pais,
                   COUNT(*) AS vistas,
                   COUNT(DISTINCT sesion) AS visitantes
            FROM web_eventos
            WHERE tipo = 'pageview' AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
            """,
            [dias],
        )
        for p in paises:
            p["nombre"] = nombre_pais(p["pais"])

        ciudades = execute_fetch_all(
            f"""
            SELECT COALESCE(ciudad, '(desconocida)') AS ciudad,
                   COALESCE(pais, 'XX') AS pais,
                   COUNT(*) AS vistas,
                   COUNT(DISTINCT sesion) AS visitantes
            FROM web_eventos
            WHERE tipo = 'pageview' AND ciudad IS NOT NULL
              AND ts >= NOW() - make_interval(days => %s) {filtro_excl}
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15
            """,
            [dias],
        )
        for c in ciudades:
            c["pais_nombre"] = nombre_pais(c["pais"])

        return {
            "disponible": True,
            "dias": dias,
            "granularidad": granularidad,
            "excluir_propias": excluir_propias,
            "totales": totales,
            "totales_prev": totales_prev,
            "serie": serie,
            "top_paginas": top_paginas,
            "top_referidos": top_referidos,
            "dispositivos": dispositivos,
            "eventos_top": eventos_top,
            "ofertas_top": ofertas_top,
            "embudo": embudo,
            "horas": horas,
            "dias_semana": dias_semana,
            "paises": paises,
            "ciudades": ciudades,
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
