# Estado Emplea — API Setup

## Despliegues vigentes

- **Frontend**: `https://estadoemplea.pages.dev` (Cloudflare Pages, servido desde este repo).
- **Backend**: `https://contrataoplanta-production.up.railway.app` (Railway, FastAPI `api.main:app`).

El frontend llama al backend directo — sin cadena de fallbacks, sin proxy same-origin.
Los dominios de marca históricos (`contrataoplanta.cl`, `estadoemplea.cl`, `empleoestado.cl`) **no resuelven**: no los agregues en listas de CORS, `SITE_URL` ni `fetchApi`.

## Entrada ASGI

- Backend FastAPI: `api/main.py`
- Aplicación ASGI: `api.main:app`
- En Railway basta con apuntar el start command a `uvicorn api.main:app --host 0.0.0.0 --port $PORT`.
- El servicio systemd de `deploy/systemd/contrataoplanta-api.service` queda como alternativa para deploy en VPS con nginx, no se usa en producción hoy.

## Comando correcto de arranque (local)

```bash
uvicorn api.main:app --reload --port 8000
```

El frontend estático en `web/` detecta `localhost` y apunta automáticamente a `http://localhost:8000` para la API.

## Variables de entorno

Definirlas en Railway (o en `.env` para desarrollo):

```bash
# Base de datos (Railway inyecta DATABASE_URL; ver config.py)
DB_HOST=...
DB_PORT=5432
DB_NAME=empleospublicos
DB_USER=...
DB_PASSWORD=...

# Sitio/canonicals/links (URL pública del front)
SITE_URL=https://estadoemplea.pages.dev

# CORS (opcional, CSV). Si no se define, se usan defaults seguros del código.
CORS_ALLOW_ORIGINS=https://estadoemplea.pages.dev

# Integraciones
RESEND_API_KEY=re_xxxxxxxxxxxx
EMAIL_FROM=alertas@estadoemplea.pages.dev
MEILISEARCH_URL=http://localhost:7700
MEILISEARCH_API_KEY=tu_master_key
```

## Endpoints mínimos de verificación

- `GET /health`
- `GET /api/ofertas?pagina=1&por_pagina=50&orden=cierre`
- `GET /api/estadisticas`
- `GET /docs`

## Nota de arquitectura frontend/backend

El frontend principal está en `web/index.html`. La función `fetchApi('/api/...')`
usa la constante `RAILWAY_BACKEND` como base única. Para apuntar a un backend
distinto en tests o staging, setear `window.__API_BASE` antes de cargar el bundle.
