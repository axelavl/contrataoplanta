"""Los endpoints admin que exponen PII (correos de suscriptores) deben
exigir rol editor — no basta un JWT de `lector`.

Puro (sin DB): inspecciona la dependencia FastAPI declarada en la firma
de cada endpoint, para que un refactor no los devuelva silenciosamente a
`verify_admin_jwt` (cualquier rol autenticado).
"""
from __future__ import annotations

import inspect

from api import deps


def _dependencia_de(fn):
    return inspect.signature(fn).parameters["_user"].default.dependency


def test_endpoints_pii_exigen_editor():
    from api.routers import admin

    endpoints_pii = (
        admin.admin_suscripciones,        # lista correos de suscriptores
        admin.admin_export_suscripciones,  # CSV con todos los correos
        admin.admin_email_eventos,         # eventos Resend con destinatarios
    )
    for fn in endpoints_pii:
        assert _dependencia_de(fn) is deps.require_editor, (
            f"{fn.__name__} debe exigir rol editor (expone PII)"
        )
