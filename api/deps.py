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


# ── Constantes de admin ───────────────────────────────────────

ADMIN_PASSWORD = _requerido_env("ADMIN_PASSWORD")
ADMIN_JWT_SECRET = _requerido_env("ADMIN_JWT_SECRET")
ADMIN_JWT_TTL_SEG = int(os.getenv("ADMIN_JWT_TTL_SEG", "43200"))  # 12h default
ADMIN_JWT_ALG = "HS256"
ADMIN_JWT_USER = "ops"  # usuario lógico único por ahora
# Prefijo secreto de las rutas de administración. Con JWT activo es
# defense-in-depth, no la barrera principal.
ADMIN_PATH = os.getenv("ADMIN_PATH", "_gestion_ops").strip("/")


# ── Rate limiting (in-memory) ─────────────────────────────────

_auth_failures: dict[str, list[float]] = defaultdict(list)
RATE_WINDOW_SEG = 600   # 10 minutos
RATE_MAX_INTENTOS = 5   # máx. intentos fallidos por ventana


def check_rate_limit(ip: str) -> None:
    ahora = _time.monotonic()
    corte = ahora - RATE_WINDOW_SEG
    _auth_failures[ip] = [t for t in _auth_failures[ip] if t > corte]
    if len(_auth_failures[ip]) >= RATE_MAX_INTENTOS:
        raise HTTPException(
            status_code=429,
            detail="Demasiados intentos fallidos. Espere 10 minutos.",
            headers={"Retry-After": str(RATE_WINDOW_SEG)},
        )


def record_failure(ip: str) -> None:
    _auth_failures[ip].append(_time.monotonic())


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
    if jti in _revoked_jti:
        raise HTTPException(status_code=401, detail="Sesión revocada")

    user = str(payload.get("sub") or "")
    if user != ADMIN_JWT_USER:
        raise HTTPException(status_code=401, detail="Token inválido")
    # Guardamos el jti en el request para que /logout pueda revocarlo sin
    # re-decodificar.
    request.state.admin_jti = jti
    request.state.admin_exp = int(payload.get("exp") or 0)
    return user


def revoke_jti(jti: str, exp_ts: int) -> None:
    """Añade un jti al denylist y purga los ya vencidos."""
    ahora = _time.time()
    vencidos = [j for j, e in _revoked_jti.items() if e <= ahora]
    for j in vencidos:
        _revoked_jti.pop(j, None)
    if exp_ts > ahora:
        _revoked_jti[jti] = float(exp_ts)
