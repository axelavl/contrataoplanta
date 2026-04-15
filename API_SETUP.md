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
fastapi==0.111.0
uvicorn==0.29.0
psycopg2-binary==2.9.9
python-dotenv==1.0.1
```
