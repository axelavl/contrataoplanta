"""
Dependencias compartidas entre `api/main.py` y los routers en
`api/routers/`.

Contiene:

- Helper `_requerido_env` para env vars obligatorias.
- Constantes de auth (`ADMIN_PASSWORD`, `ADMIN_JWT_SECRET`, etc.) y
  `ADMIN_PATH` (prefijo secreto de las rutas admin).
- Rate limit en memoria (ventana de 10 min, 5 intentos por IP).
- Helpers JWT: `create_admin_token`, `verify_admin_jwt` (FastAPI
  `Depends` compatible), `revoke_jti`, denylist `_revoked_jti`.

Se movió acá desde `api/main.py` para que varios routers puedan
compartir el contrato sin que `main.py` sea la fuente única del saber.
Los nombres sin `_` son el API público; los con `_` siguen siendo
implementation detail y no deberían importarse desde afuera.

Nota: ``on_event("startup")`` / ``on_shutdown`` no viven acá porque
pertenecen al ciclo de vida de la app; viven en `api/main.py`.
"""
from __future__ import annotations

import os
import secrets
import time as _time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import jwt  # PyJWT
from fastapi import HTTPException, Request


# ── Helpers de entorno ────────────────────────────────────────

def _requerido_env(nombre: str) -> str:
    """Lee una env var obligatoria o levanta ``RuntimeError``.

    Evita fallbacks hardcodeados: si la variable falta en Railway, el
    proceso debe abortar el arranque con traceback claro.
    """
    valor = os.getenv(nombre)
    if not valor:
        raise RuntimeError(
            f"Variable de entorno {nombre!r} no definida. "
            f"Configúrala en Railway/entorno (ver .env.example)."
        )
    return valor


# ── Constantes compartidas (SEO/SSR y endpoints) ──────────────────

#: Raíz del proyecto — útil para resolver paths relativos (index.html,
#: catálogo JSON, etc.) desde cualquier módulo.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: URL canónica del frontend. Cloudflare Pages por default; los dominios
#: de marca previos (contrataoplanta.cl / estadoemplea.cl /
#: empleoestado.cl) ya no resuelven en DNS — si se filtran a un
#: `og:image` o `og:url`, el crawler recibe NXDOMAIN y el unfurl no
#: se renderiza.
SITE_URL = (
    os.getenv("SITE_URL", "https://estadoemplea.pages.dev")
    or "https://estadoemplea.pages.dev"
).rstrip("/")

#: Path absoluto a `web/index.html` — el template que los endpoints
#: SSR leen para inyectar meta tags, JSON-LD y el bloque de contenido.
WEB_INDEX_PATH = _PROJECT_ROOT / "web" / "index.html"

#: Imagen OG por defecto (cuando no hay oferta/landing específica).
DEFAULT_OG_IMAGE = f"{SITE_URL}/og-default.jpg"


# ── Constantes de admin ───────────────────────────────────────

ADMIN_PASSWORD = _requerido_env("ADMIN_PASSWORD")
ADMIN_JWT_SECRET = _requerido_env("ADMIN_JWT_SECRET")
ADMIN_JWT_TTL_SEG = int(os.getenv("ADMIN_JWT_TTL_SEG", "43200"))  # 12h default
ADMIN_JWT_ALG = "HS256"
ADMIN_JWT_USER = "ops"  # usuario lógico único por ahora
# Prefijo secreto de las rutas de administración. Con JWT activo es
# defense-in-depth, no la barrera principal.
ADMIN_PATH = os.getenv("ADMIN_PATH", "_gestion_ops").strip("/")


# ── Rate limiting (Postgres compartido, con fallback en memoria) ──
#
# El estado de auth (intentos fallidos + denylist de jti) vivía en dicts
# de proceso. Con la API corriendo en >1 worker de uvicorn eso fragmenta
# el límite (5 intentos por worker → N×5 efectivos) y un jti revocado en
# /logout seguía válido en los otros workers. Ahora ambos viven en
# Postgres para que el estado sea consistente entre workers/instancias.
#
# Fallback: si la DB no está disponible, caemos al dict en memoria. Es
# degradación intencional — preferible a tumbar el login por un blip de
# Postgres, asumiendo que el outage de DB es transitorio.

_auth_failures: dict[str, list[float]] = defaultdict(list)  # fallback en memoria
RATE_WINDOW_SEG = 600   # 10 minutos
RATE_MAX_INTENTOS = 5   # máx. intentos fallidos por ventana

#: Marca para no spamear el log cuando la DB no responde.
_auth_store_degraded = False


def _auth_store_warn_once(exc: Exception) -> None:
    global _auth_store_degraded
    if not _auth_store_degraded:
        _auth_store_degraded = True
        import logging
        logging.getLogger("api.deps").warning(
            "Auth store en Postgres no disponible (%s). "
            "Rate limit y denylist degradan a memoria por proceso.",
            type(exc).__name__,
        )


def check_rate_limit(ip: str) -> None:
    ahora = _time.time()
    corte = ahora - RATE_WINDOW_SEG
    try:
        from api.services.db import get_cursor
        with get_cursor() as (conn, cur):
            # Purga perezosa de la ventana vencida y conteo en una pasada.
            cur.execute(
                "DELETE FROM admin_auth_failures WHERE ts < to_timestamp(%s)",
                [corte],
            )
            cur.execute(
                "SELECT COUNT(*) AS n FROM admin_auth_failures "
                "WHERE ip = %s AND ts >= to_timestamp(%s)",
                [ip, corte],
            )
            row = cur.fetchone()
            conn.commit()
            n = int((row["n"] if isinstance(row, dict) else row[0]) or 0)
    except Exception as exc:  # noqa: BLE001 — degradación intencional
        _auth_store_warn_once(exc)
        _auth_failures[ip] = [t for t in _auth_failures[ip] if t > corte]
        n = len(_auth_failures[ip])

    if n >= RATE_MAX_INTENTOS:
        raise HTTPException(
            status_code=429,
            detail="Demasiados intentos fallidos. Espere 10 minutos.",
            headers={"Retry-After": str(RATE_WINDOW_SEG)},
        )


def record_failure(ip: str) -> None:
    ahora = _time.time()
    try:
        from api.services.db import get_cursor
        with get_cursor() as (conn, cur):
            cur.execute(
                "INSERT INTO admin_auth_failures (ip, ts) "
                "VALUES (%s, to_timestamp(%s))",
                [ip, ahora],
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        _auth_store_warn_once(exc)
        _auth_failures[ip].append(ahora)


def client_ip(request: Request) -> str:
    """IP real del cliente considerando ``X-Forwarded-For`` del proxy.

    Tomamos el primer valor del header (cliente original). No es
    infalsificable si el proxy no sanea, pero en Railway/Cloudflare
    viene limpio.
    """
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        primera = xff.split(",")[0].strip()
        if primera:
            return primera
    return (request.client.host if request.client else "unknown") or "unknown"


# ── JWT admin ─────────────────────────────────────────────────

#: Denylist de tokens revocados: jti → exp_ts unix. Se purga
#: perezosamente al verificar tokens; un jti vencido sin limpiar igual
#: falla la validación de ``exp`` de PyJWT.
_revoked_jti: dict[str, float] = {}


def create_admin_token(user: str = ADMIN_JWT_USER) -> dict[str, Any]:
    """Emite un JWT de admin. Devuelve ``{token, expires_at, jti}``."""
    ahora = datetime.now(tz=timezone.utc)
    exp_dt = ahora + timedelta(seconds=ADMIN_JWT_TTL_SEG)
    jti = secrets.token_urlsafe(12)
    payload = {
        "sub": user,
        "jti": jti,
        "iat": int(ahora.timestamp()),
        "exp": int(exp_dt.timestamp()),
    }
    token = jwt.encode(payload, ADMIN_JWT_SECRET, algorithm=ADMIN_JWT_ALG)
    return {"token": token, "expires_at": payload["exp"], "jti": jti}


def verify_admin_jwt(request: Request) -> str:
    """FastAPI dependency: valida Authorization Bearer JWT de admin.

    - Espera ``Authorization: Bearer <jwt>``.
    - Valida firma, ``exp``, ``iat`` con PyJWT.
    - Rechaza si el ``jti`` está en el denylist (logout).
    - No aplica rate limit acá: vive en ``/auth/login`` donde se
      presentan credenciales.
    """
    auth_header = request.headers.get("authorization", "")
    scheme, _, raw = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not raw.strip():
        raise HTTPException(
            status_code=401,
            detail="Autenticación requerida",
            # Scheme no estándar para evitar el diálogo nativo del navegador.
            headers={"WWW-Authenticate": 'Bearer realm="Gestion"'},
        )
    try:
        payload = jwt.decode(
            raw.strip(),
            ADMIN_JWT_SECRET,
            algorithms=[ADMIN_JWT_ALG],
            options={"require": ["exp", "iat", "sub", "jti"]},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Sesión expirada") from None
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido") from None

    jti = payload.get("jti") or ""
    if _jti_revocado(jti):
        raise HTTPException(status_code=401, detail="Sesión revocada")

    user = str(payload.get("sub") or "")
    if user != ADMIN_JWT_USER:
        raise HTTPException(status_code=401, detail="Token inválido")
    # Guardamos el jti en el request para que /logout pueda revocarlo sin
    # re-decodificar.
    request.state.admin_jti = jti
    request.state.admin_exp = int(payload.get("exp") or 0)
    return user


def _jti_revocado(jti: str) -> bool:
    """True si el jti está en la denylist (revocado por /logout y aún no
    vencido). Consulta Postgres; si la DB no responde, cae a memoria."""
    if not jti:
        return False
    ahora = _time.time()
    try:
        from api.services.db import get_cursor
        with get_cursor() as (_, cur):
            cur.execute(
                "SELECT 1 FROM admin_jwt_denylist "
                "WHERE jti = %s AND exp_ts > to_timestamp(%s) LIMIT 1",
                [jti, ahora],
            )
            return cur.fetchone() is not None
    except Exception as exc:  # noqa: BLE001 — degradación intencional
        _auth_store_warn_once(exc)
        return _revoked_jti.get(jti, 0.0) > ahora


def revoke_jti(jti: str, exp_ts: int) -> None:
    """Añade un jti a la denylist (Postgres) y purga los ya vencidos.

    Idempotente: un re-logout del mismo token no falla. Fallback a memoria
    si la DB no responde — en ese caso la revocación sólo aplica a este
    worker, igual que antes de la migración a estado compartido.
    """
    ahora = _time.time()
    if exp_ts <= ahora:
        return  # ya vencido: PyJWT lo rechaza por `exp`, no hace falta guardarlo
    try:
        from api.services.db import get_cursor
        with get_cursor() as (conn, cur):
            cur.execute(
                "DELETE FROM admin_jwt_denylist WHERE exp_ts <= to_timestamp(%s)",
                [ahora],
            )
            cur.execute(
                "INSERT INTO admin_jwt_denylist (jti, exp_ts) "
                "VALUES (%s, to_timestamp(%s)) ON CONFLICT (jti) DO NOTHING",
                [jti, float(exp_ts)],
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        _auth_store_warn_once(exc)
        vencidos = [j for j, e in _revoked_jti.items() if e <= ahora]
        for j in vencidos:
            _revoked_jti.pop(j, None)
        _revoked_jti[jti] = float(exp_ts)
