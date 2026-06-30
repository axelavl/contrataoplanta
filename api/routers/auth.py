"""
Endpoints de autenticación del panel admin:

- ``POST /api/{ADMIN_PATH}/auth/login``  — intercambia password por JWT.
- ``POST /api/{ADMIN_PATH}/auth/logout`` — revoca el token actual.
- ``GET  /api/{ADMIN_PATH}/auth/me``     — valida que un token siga vivo.

Usa los helpers de ``api.deps``: rate limit por IP para ``/login``,
denylist de jti para ``/logout``, verificación JWT para ``/me``. Los
otros ~30 endpoints admin siguen en ``api/main.py`` (se moverán a
routers propios en PRs siguientes) pero también importan
``verify_admin_jwt`` desde ``api.deps``.
"""
from __future__ import annotations

import secrets
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from api.deps import (
    ADMIN_PASSWORD,
    ADMIN_PATH,
    autenticar_usuario,
    check_rate_limit,
    client_ip,
    create_admin_token,
    record_failure,
    revoke_jti,
    verify_admin_jwt,
)


router = APIRouter(prefix=f"/api/{ADMIN_PATH}/auth", tags=["admin"])


class _LoginPayload(BaseModel):
    password: str
    usuario: str | None = None   # opcional: cuenta nominal de admin_usuarios


@router.post("/login")
def admin_login(payload: _LoginPayload, request: Request) -> dict[str, Any]:
    """Valida credenciales y emite un JWT firmado.

    Dos caminos:
    - **Usuario nominal**: si se envía `usuario`, se valida contra
      `admin_usuarios` (cuenta + rol propio).
    - **Contraseña maestra**: si no se envía `usuario`, se compara con
      `ADMIN_PASSWORD` y se entra como «ops» con rol admin (respaldo).

    Rate limit por IP sobre intentos fallidos (5 en 10 min). Las sesiones
    válidas no se ven afectadas — el resto de los endpoints admin sólo
    valida el token, no vuelve a pedir credenciales.
    """
    ip = client_ip(request)
    check_rate_limit(ip)

    password = (payload.password or "").strip()
    usuario = (payload.usuario or "").strip()

    if usuario:
        cuenta = autenticar_usuario(usuario, password)
        if not cuenta:
            record_failure(ip)
            raise HTTPException(status_code=401, detail="Credenciales incorrectas")
        emitido = create_admin_token(user=cuenta["usuario"], rol=cuenta["rol"])
        return {
            "token": emitido["token"],
            "expires_at": emitido["expires_at"],
            "token_type": "Bearer",
            "usuario": cuenta["usuario"],
            "rol": emitido["rol"],
            "nombre": cuenta.get("nombre"),
        }

    if not password or not secrets.compare_digest(
        password.encode("utf-8"), ADMIN_PASSWORD.encode("utf-8")
    ):
        record_failure(ip)
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")

    emitido = create_admin_token()
    return {
        "token": emitido["token"],
        "expires_at": emitido["expires_at"],
        "token_type": "Bearer",
        "usuario": "ops",
        "rol": emitido["rol"],
    }


@router.post("/logout")
def admin_logout(
    request: Request,
    _user: str = Depends(verify_admin_jwt),
) -> dict[str, bool]:
    """Revoca el token actual (añade su jti al denylist hasta su ``exp``)."""
    jti = getattr(request.state, "admin_jti", "") or ""
    exp_ts = int(getattr(request.state, "admin_exp", 0) or 0)
    if jti and exp_ts:
        revoke_jti(jti, exp_ts)
    return {"ok": True}


@router.get("/me")
def admin_me(
    request: Request,
    _user: str = Depends(verify_admin_jwt),
) -> dict[str, Any]:
    """Ping autenticado para validar que un token guardado sigue vivo."""
    return {
        "user": _user,
        "rol": getattr(request.state, "admin_rol", "admin"),
        "expires_at": int(getattr(request.state, "admin_exp", 0) or 0),
    }
