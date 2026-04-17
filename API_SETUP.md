# contrata o planta .cl — API Setup (estado real del repo)

## Entrada ASGI real

- Backend FastAPI: `api/main.py`
- Aplicación ASGI: `api.main:app`
- Servicio systemd de producción: `deploy/systemd/contrataoplanta-api.service`
  - `ExecStart=... uvicorn api.main:app --host 127.0.0.1 --port 8000 --workers 2`

## Comando correcto de arranque

```bash
uvicorn api.main:app --host 127.0.0.1 --port 8000
```

En desarrollo:

```bash
uvicorn api.main:app --reload --port 8000
```

## Variables de entorno (sin hardcode en código)

Definirlas en `.env` (o en el entorno del servicio):

```bash
# Base de datos
DB_HOST=localhost
DB_PORT=5432
DB_NAME=empleospublicos
DB_USER=postgres
DB_PASSWORD=tu_password

# Sitio/canonicals/links
SITE_URL=https://contrataoplanta.cl

# CORS (opcional, CSV)
# Si no se define, se usan defaults seguros del código.
CORS_ALLOW_ORIGINS=https://contrataoplanta.cl,https://www.contrataoplanta.cl

# Integraciones
RESEND_API_KEY=re_xxxxxxxxxxxx
EMAIL_FROM=alertas@contrataoplanta.cl
MEILISEARCH_URL=http://localhost:7700
MEILISEARCH_API_KEY=tu_master_key
```

## Endpoints mínimos de verificación

- `GET /health`
- `GET /api/ofertas?pagina=1&por_pagina=50&orden=cierre`
- `GET /api/estadisticas`
- `GET /docs`

## Nota de arquitectura frontend/backend

El frontend principal está en `web/index.html` y consulta la API por `fetchApi('/api/...')`
con fallback a `https://api.contrataoplanta.cl` si no existe proxy `/api` en el host estático.
