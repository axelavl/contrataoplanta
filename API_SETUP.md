# contrata o planta .cl — API Setup

## Estructura de carpetas

```
empleoestado/
├── api/
│   └── main.py          ← este archivo
├── scrapers/
│   ├── empleos_publicos.py
│   └── ...
├── db/
│   └── schema.sql
└── requirements.txt
```

## Instalación

```
pip install fastapi uvicorn psycopg2-binary python-dotenv
```

O instalar todo desde el archivo:

```
pip install -r requirements.txt
```

## Configurar la contraseña

Abre `api/main.py` y cambia esta línea:

```python
"password": os.getenv("DB_PASSWORD", "admin123"),  # ← tu contraseña PostgreSQL
```

## Ejecutar en desarrollo

Desde la carpeta raíz del proyecto:

```
uvicorn api.main:app --reload --port 8000
```

La API queda disponible en:
- http://localhost:8000/api/ofertas
- http://localhost:8000/api/estadisticas
- http://localhost:8000/docs  ← documentación automática interactiva

## Probar que funciona

Abrir el navegador en:
  http://localhost:8000/health

Debe responder:
  {"status": "ok", "db": "2026-04-14 ..."}

## Variables de entorno

```bash
# Base de datos
DB_HOST=localhost
DB_PORT=5432
DB_NAME=empleospublicos
DB_USER=postgres
DB_PASSWORD=tu_password

# Resend — alertas de email
RESEND_API_KEY=re_xxxxxxxxxxxx
EMAIL_FROM=alertas@contrataoplanta.cl

# Meilisearch — búsqueda rápida
MEILISEARCH_URL=http://localhost:7700
MEILISEARCH_API_KEY=tu_master_key

# Umami — analytics
UMAMI_SCRIPT_URL=https://analytics.contrataoplanta.cl/script.js
UMAMI_WEBSITE_ID=tu_website_id
```

## APIs externas integradas

| API | Propósito | Endpoint |
|-----|-----------|----------|
| DPA (apis.digital.gob.cl) | Regiones y comunas oficiales | `GET /api/regiones`, `GET /api/regiones/{codigo}/comunas` |
| BCN LeyChile | Ley orgánica por institución | `GET /api/instituciones/{id}/ley`, `GET /api/leyes/buscar` |
| Resend | Alertas automáticas por email | `POST /api/alertas/enviar` |
| Mailcheck | Validación de email + typos | `GET /api/validar-email` |
| Meilisearch | Búsqueda rápida con sinónimos | `GET /api/buscar`, `GET /api/autocompletar` |
| Umami | Analytics sin cookies | Script en frontend |

## Conectar el frontend

En el archivo index_contrataoplanta.html, reemplazar los datos
hardcodeados por llamadas a la API:

```javascript
// Al cargar la página
fetch('http://localhost:8000/api/ofertas')
  .then(r => r.json())
  .then(data => console.log(data.ofertas))
```

## Requirements.txt

```
pip install -r requirements.txt
```
