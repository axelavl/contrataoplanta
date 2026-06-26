"""Tests del hashing de contraseñas y las dependencias de rol del panel.

Son puros (no tocan la base de datos): cubren `hash_password`/
`verify_password`, que el rol viaje firmado en el token y que
`require_admin` / `require_editor` apliquen la jerarquía de roles.
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException, Request

from api import deps


def _request_con_token(token: str) -> Request:
    scope = {
        "type": "http",
        "headers": [(b"authorization", ("Bearer " + token).encode())],
        "state": {},
    }
    return Request(scope)


def test_hash_password_roundtrip():
    h = deps.hash_password("contraseña-secreta")
    assert h.startswith("pbkdf2_sha256$")
    assert deps.verify_password("contraseña-secreta", h)
    assert not deps.verify_password("otra", h)


def test_verify_password_formato_invalido():
    assert deps.verify_password("x", "") is False
    assert deps.verify_password("x", "basura") is False
    assert deps.verify_password("x", "md5$1$a$b") is False


def test_create_admin_token_lleva_rol():
    assert deps.create_admin_token("ana", "lector")["rol"] == "lector"
    assert deps.create_admin_token("juan", "editor")["rol"] == "editor"
    # Rol inválido cae a 'editor' (nunca escala a admin por accidente).
    assert deps.create_admin_token("x", "superusuario")["rol"] == "editor"


def test_verify_admin_jwt_expone_rol():
    tok = deps.create_admin_token("camila", "admin")["token"]
    req = _request_con_token(tok)
    assert deps.verify_admin_jwt(req) == "camila"
    assert req.state.admin_rol == "admin"


def test_require_admin_rechaza_no_admin():
    for rol in ("lector", "editor"):
        tok = deps.create_admin_token("u", rol)["token"]
        with pytest.raises(HTTPException) as exc:
            deps.require_admin(_request_con_token(tok))
        assert exc.value.status_code == 403
    # admin pasa sin excepción.
    tok = deps.create_admin_token("jefa", "admin")["token"]
    assert deps.require_admin(_request_con_token(tok)) == "jefa"


def test_require_editor_jerarquia():
    # lector no puede; editor y admin sí.
    with pytest.raises(HTTPException):
        deps.require_editor(_request_con_token(deps.create_admin_token("a", "lector")["token"]))
    assert deps.require_editor(_request_con_token(deps.create_admin_token("b", "editor")["token"])) == "b"
    assert deps.require_editor(_request_con_token(deps.create_admin_token("c", "admin")["token"])) == "c"


def test_token_legacy_sin_rol_es_admin():
    # Tokens antiguos no traían 'rol'; deben asumirse admin para no romper
    # sesiones vivas tras el deploy.
    import jwt
    from datetime import datetime, timezone, timedelta
    ahora = datetime.now(tz=timezone.utc)
    payload = {
        "sub": "ops",
        "jti": "x" * 12,
        "iat": int(ahora.timestamp()),
        "exp": int((ahora + timedelta(hours=1)).timestamp()),
    }
    tok = jwt.encode(payload, deps.ADMIN_JWT_SECRET, algorithm=deps.ADMIN_JWT_ALG)
    req = _request_con_token(tok)
    assert deps.verify_admin_jwt(req) == "ops"
    assert req.state.admin_rol == "admin"
