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


def test_cuenta_nominal_revalida_rol_desde_db(monkeypatch):
    # El token dice admin, pero la BD ahora marca rol=lector → manda la BD.
    monkeypatch.setattr(deps, "_estado_usuario_admin", lambda u: (True, "lector"))
    tok = deps.create_admin_token("camila", "admin")["token"]
    req = _request_con_token(tok)
    assert deps.verify_admin_jwt(req) == "camila"
    assert req.state.admin_rol == "lector"   # demotion efectiva al instante
    with pytest.raises(HTTPException):
        deps.require_admin(_request_con_token(tok))


def test_cuenta_nominal_desactivada_rechazada(monkeypatch):
    # activo=False → 401 aunque el token siga vigente.
    monkeypatch.setattr(deps, "_estado_usuario_admin", lambda u: (False, None))
    tok = deps.create_admin_token("juan", "editor")["token"]
    with pytest.raises(HTTPException) as exc:
        deps.verify_admin_jwt(_request_con_token(tok))
    assert exc.value.status_code == 401


def test_cuenta_nominal_db_caida_degrada_al_token(monkeypatch):
    # BD no disponible → se conserva el rol firmado del token (no se bloquea).
    monkeypatch.setattr(deps, "_estado_usuario_admin", lambda u: (None, None))
    tok = deps.create_admin_token("ana", "editor")["token"]
    req = _request_con_token(tok)
    assert deps.verify_admin_jwt(req) == "ana"
    assert req.state.admin_rol == "editor"


def test_usuario_maestro_ops_no_consulta_db(monkeypatch):
    # 'ops' (contraseña maestra) no tiene fila: nunca debe consultar la BD.
    def _boom(_u):
        raise AssertionError("no debería consultarse la BD para 'ops'")
    monkeypatch.setattr(deps, "_estado_usuario_admin", _boom)
    tok = deps.create_admin_token()["token"]   # sub='ops', rol='admin'
    req = _request_con_token(tok)
    assert deps.verify_admin_jwt(req) == "ops"
    assert req.state.admin_rol == "admin"


def test_token_sin_rol_degrada_a_lector():
    # Fail-safe de seguridad: un token sin claim `rol` (o con uno inválido) NO
    # debe asumirse admin — eso sería una escalada silenciosa. Se degrada al
    # rol de menor privilegio (`lector`). Los tokens legítimos actuales siempre
    # incluyen `rol`, así que este default solo cubre el caso anómalo.
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
    assert req.state.admin_rol == "lector"
