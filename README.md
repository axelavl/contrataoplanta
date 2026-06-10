# EmpleoEstado.cl — Sistema de Scraping

Agregador de ofertas laborales del sector público de Chile.  
Extrae, normaliza y centraliza todas las publicaciones de empleo del Estado.

---

## Requisitos

- Python 3.11+
- PostgreSQL 14+
- pip

---

## Instalación en tu máquina local

### 1. Clonar y configurar entorno

```bash
# Crear carpeta del proyecto
mkdir empleoestado && cd empleoestado

# Crear entorno virtual Python (recomendado)
python3 -m venv venv
source venv/bin/activate          # Linux/Mac
# venv\Scripts\activate           # Windows

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Crear base de datos PostgreSQL

```bash
# Conectarse a PostgreSQL
psql -U postgres

# Crear base de datos y usuario
CREATE DATABASE empleoestado;
CREATE USER empleoestado_user WITH PASSWORD 'tu_password_aqui';
GRANT ALL PRIVILEGES ON DATABASE empleoestado TO empleoestado_user;
\q

# Aplicar el schema
psql -U empleoestado_user -d empleoestado -f db/schema.sql
```

### 3. Configurar variables de entorno

Crear archivo `.env` en la raíz del proyecto:

```env
DATABASE_URL=postgresql://empleoestado_user:tu_password_aqui@localhost:5432/empleoestado

# Scraping
DELAY_REQUESTS=1.5
TIMEOUT_REQUEST=20
MAX_REINTENTOS=3

# Email (alertas) — usar Resend.com, plan gratuito: 3.000 emails/mes
EMAIL_PROVIDER=resend
EMAIL_API_KEY=re_xxxxxxxxxxxx
EMAIL_FROM=alertas@empleoestado.cl

# Logs
LOG_DIR=logs
LOG_LEVEL=INFO
```

### 4. Crear carpeta de logs

```bash
mkdir logs
```

---

## Uso

### Ejecutar el scraper principal (empleospublicos.cl)

```bash
# Modo normal — extrae todas las ofertas
python scrapers/empleos_publicos.py

# Solo las primeras 5 páginas (para probar)
python scrapers/empleos_publicos.py --max 5

# Sin escribir en BD (para depurar)
python scrapers/empleos_publicos.py --dry-run --max 3

# Con detalle de cada oferta (más lento, más información)
python scrapers/empleos_publicos.py --con-detalle --max 2
```

### Ejecutar todos los scrapers

```bash
# Solo los que corresponde según su frecuencia
python run_scrapers.py

# Forzar todos
python run_scrapers.py --todos

# Ver estado de fuentes
python run_scrapers.py --listar

# Solo el Poder Judicial
python run_scrapers.py --fuente 3
```

### Verificar datos en PostgreSQL

```bash
psql -U empleoestado_user -d empleoestado

-- ¿Cuántas ofertas hay?
SELECT COUNT(*) FROM ofertas WHERE activa = TRUE;

-- ¿Cuántas por sector?
SELECT sector, COUNT(*) FROM ofertas WHERE activa = TRUE GROUP BY sector ORDER BY 2 DESC;

-- ¿Cuántas por región?
SELECT region, COUNT(*) FROM ofertas WHERE activa = TRUE GROUP BY region ORDER BY 2 DESC;

-- Últimas 10 ofertas nuevas
SELECT cargo, institucion_nombre, region, fecha_cierre FROM ofertas ORDER BY creada_en DESC LIMIT 10;

-- Log de ejecuciones
SELECT * FROM logs_scraping ORDER BY iniciado_en DESC LIMIT 5;
```

### Dashboard de QA del scraper (Streamlit)

```bash
# 1) levantar API en local (otro terminal)
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# 2) levantar dashboard
streamlit run scripts/streamlit_scraper_dashboard.py

# Opcional: apuntar a otra API
export SCRAPER_API_BASE_URL="https://tu-api"
streamlit run scripts/streamlit_scraper_dashboard.py
```

---

## Automatización con cron (Linux/Mac)

Para que los scrapers se ejecuten automáticamente:

```bash
# Editar crontab
crontab -e

# Agregar estas líneas:
# Scraper principal: cada 12 horas (6am y 6pm)
0 6,18 * * * cd /ruta/a/empleoestado && venv/bin/python run_scrapers.py >> logs/cron.log 2>&1

# Limpieza de ofertas expiradas: cada noche a la 1am
0 1 * * * cd /ruta/a/empleoestado && venv/bin/python -c "
from db.database import SessionLocal
from sqlalchemy import text
db = SessionLocal()
db.execute(text(\"UPDATE ofertas SET activa=FALSE, es_nueva=FALSE WHERE fecha_cierre < CURRENT_DATE\"))
db.execute(text(\"UPDATE ofertas SET es_nueva=FALSE WHERE detectada_en < NOW() - INTERVAL '24 hours'\"))
db.commit(); db.close()
print('Limpieza completada')
" >> logs/cron.log 2>&1
```

---

## Estructura del proyecto

```
empleoestado/
├── config.py               # Configuración central
├── run_scrapers.py          # Orquestador principal
├── requirements.txt         # Dependencias Python
├── .env                     # Variables de entorno (NO subir a git)
├── .gitignore
│
├── db/
│   ├── schema.sql           # Schema PostgreSQL
│   └── database.py          # Capa de acceso a datos
│
├── scrapers/
│   ├── empleos_publicos.py  # ★ Scraper principal (empleospublicos.cl)
│   ├── poder_judicial.py    # Poder Judicial
│   ├── adp.py               # Alta Dirección Pública (por implementar)
│   ├── banco_central.py     # Banco Central (por implementar)
│   ├── contraloria.py       # Contraloría (por implementar)
│   └── ...                  # Más scrapers a agregar
│
├── api/
│   └── main.py              # API FastAPI (próxima fase)
│
├── logs/                    # Logs de ejecución (generado automáticamente)
└── data/                    # Datos auxiliares (listas de instituciones, etc.)
```

---

## Agregar un nuevo scraper

1. Crear `scrapers/nombre_institucion.py` siguiendo la estructura de `empleos_publicos.py`
2. Agregar la fuente en `db/schema.sql` (tabla `fuentes`)
3. Registrar el scraper en `run_scrapers.py` (lista `SCRAPERS`)
4. Probar con `--dry-run` antes de ejecutar en producción

---

## Troubleshooting

**Error: `ModuleNotFoundError: No module named 'psycopg2'`**
```bash
pip install psycopg2-binary
```

**Error de conexión a PostgreSQL**
```bash
# Verificar que PostgreSQL esté corriendo
pg_isready -U postgres

# Verificar la URL en .env
echo $DATABASE_URL
```

**El scraper no encuentra ofertas**
El sitio puede haber cambiado su estructura HTML. Inspeccionar con DevTools:
1. Abrir empleospublicos.cl en Chrome
2. F12 → Elements
3. Identificar el selector CSS del contenedor de ofertas
4. Actualizar los selectores en `parsear_listado()` y `parsear_tarjeta()`

---

## Próximos pasos

- [ ] Implementar scrapers pendientes (ADP, Banco Central, Contraloría, DT)
- [ ] API FastAPI para exponer los datos al frontend
- [ ] Conectar con el frontend HTML (ya diseñado)
- [ ] Deploy en Railway.app
- [ ] Sistema de alertas por email
- [ ] Scrapers de municipios grandes
