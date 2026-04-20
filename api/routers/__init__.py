"""Routers FastAPI para `api/main.py`.

Cada módulo acá agrupa endpoints por dominio. El entry point
(`api/main.py`) los registra con ``app.include_router(router)`` y
pasa a ser cada vez más delgado.

Primer router extraído: `auth` (login/logout/me del admin). Los 30+
endpoints admin restantes siguen en `main.py` por ahora y se moverán
en PRs siguientes.
"""
