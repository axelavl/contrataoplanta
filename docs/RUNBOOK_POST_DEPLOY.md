# Runbook — post-deploy de PR #155 (y siguientes)

Esta guía condensa los tres pasos operativos que quedan tras mergear
[#155](https://github.com/axelavl/contrataoplanta/pull/155) (SSR real
de contenido + CHECK constraints de DB). Cada sección es ejecutable
con un solo comando — no hay que pensar, leer y correr.

---

## 1. Aplicar la migración 005 a Postgres

La migración añade CHECK constraints de sanidad sobre `renta_bruta_*`,
`fecha_*` y `horas_semanales`. Usa `NOT VALID` → **no va a fallar** si
hay datos históricos que violan los rangos; sólo los nuevos INSERT /
UPDATE se validarán.

### En Railway (producción)

```bash
railway link <project>                       # si no está linkeado
railway run -- psql "$DATABASE_URL" -f db/migrations/005_data_quality_constraints.sql
```

Alternativa si `railway run` no tiene el binario `psql`:

```bash
export DATABASE_URL="$(railway variables get DATABASE_URL)"
psql "$DATABASE_URL" -f db/migrations/005_data_quality_constraints.sql
```

### En local (staging / QA)

```bash
psql -U "$DB_USER" -h "$DB_HOST" -d "$DB_NAME" \
     -f db/migrations/005_data_quality_constraints.sql
```

La migración es **idempotente** (cada `ADD CONSTRAINT` está envuelto en
`DO $$ BEGIN IF NOT EXISTS ... END $$`). Correrla dos veces no duplica
ni rompe nada.

---

## 2. Auditar violaciones actuales y monitorear scrapers

### Antes de aplicar 005 — estimar el "daño histórico"

```bash
# Desde local, apuntando a la DB de Railway:
export DB_HOST=... DB_PORT=... DB_NAME=... DB_USER=... DB_PASSWORD=...
python scripts/qa/check_data_quality.py --samples 3
```

Salida esperada:

```
Total ofertas: 18,430
------------------------------------------------------------------------
  ✗ renta_bruta_min fuera de rango (300k–20M)          142 ( 0.77%)
      id=2914 cargo='Analista...' renta=(1,999999999) ...
  ✗ fecha_publicacion fuera de rango                    3 ( 0.02%)
      id=7729 cargo='...' renta=(...) fpub=1970-01-01 ...
  ✓ renta_bruta_min > renta_bruta_max                   0 ( 0.00%)
  ...
```

Si aparecen violaciones, son datos que **ya existían** con bugs de
scraping. No bloquean la migración pero sí indican qué hay que limpiar
después.

### Después de aplicar 005 — monitorear que no entren nuevas

Correr el mismo comando periódicamente (daily cron). El número total
debería mantenerse plano o **bajar** (a medida que ofertas vencidas
son UPDATE-eadas por los scrapers, las que violaban quedarán flaggeadas
cuando se intente editarlas).

Si el número **sube**: algún scraper está produciendo basura. Ver logs
de scrapers (`journalctl -u contrataoplanta-scrapers` o Railway logs
equivalente) buscando errores `check_violation` de psycopg2.

### Limpieza y validación de constraint

Una vez las filas violatorias están limpias (UPDATE a NULL o valor
razonable), el constraint puede promoverse a estado `VALIDATED`:

```sql
ALTER TABLE ofertas VALIDATE CONSTRAINT chk_ofertas_renta_min_rango;
ALTER TABLE ofertas VALIDATE CONSTRAINT chk_ofertas_renta_max_rango;
ALTER TABLE ofertas VALIDATE CONSTRAINT chk_ofertas_renta_min_leq_max;
ALTER TABLE ofertas VALIDATE CONSTRAINT chk_ofertas_fecha_publicacion_rango;
ALTER TABLE ofertas VALIDATE CONSTRAINT chk_ofertas_fecha_cierre_rango;
ALTER TABLE ofertas VALIDATE CONSTRAINT chk_ofertas_horas_semanales_rango;
```

Si falla alguno: aún quedan filas inválidas, volver a
`check_data_quality.py` para identificarlas.

---

## 3. Verificar SSR contra producción (y Rich Results Test)

### Script local — pre-check rápido

Elegir una oferta activa cualquiera de la home (`https://estadoemplea.pages.dev/`)
y obtener su id numérico. Luego:

```bash
python scripts/qa/verify_ssr.py \
    https://contrataoplanta-production.up.railway.app/oferta/<ID>
```

El script verifica sin ejecutar JavaScript (como Googlebot-lite):

- Headers `Strict-Transport-Security`, `X-Content-Type-Options`,
  `Permissions-Policy`, `Referrer-Policy`, `X-Frame-Options`, `CSP-RO`.
- `<title>`, `meta[description]`, `og:title`, `og:description`,
  `<link rel=canonical>`.
- `<script type="application/ld+json">` con `@type: JobPosting` parseable
  y los 5 campos mínimos (`title`, `hiringOrganization`, `validThrough`,
  `url`, `jobLocation`).
- `<article class="oferta-ssr">` visible con `<h1>` y `.oferta-ssr-institucion`.

Salida esperada: `Todo OK. SSR + headers + JSON-LD listos para crawlers.`

### Google Rich Results Test — validación oficial

Una vez el pre-check local pasa, pegar la URL de la oferta en:

> <https://search.google.com/test/rich-results>

Debería detectar **1 ítem "JobPosting"** sin errores críticos. Los
warnings opcionales (`employmentType`, `baseSalary`, `description`)
son aceptables si la oferta no tiene esos datos.

### Google Search Console

Tras verificar ambos dominios (`estadoemplea.pages.dev` y
`contrataoplanta-production.up.railway.app`):

1. **Sitemaps → Añadir nuevo** → `https://contrataoplanta-production.up.railway.app/sitemap.xml`
2. **Inspección de URL** → pegar una `/oferta/{id}-{slug}` → "Probar URL
   en vivo" → confirmar que Google ve el `<h1>` y el JobPosting.
3. **Cobertura** (una semana después) → debería ver cientos → miles de
   URLs indexadas de `/oferta/*`.

---

## Resumen de un vistazo

```bash
# Paso 1 — migración (idempotente)
railway run -- psql "$DATABASE_URL" -f db/migrations/005_data_quality_constraints.sql

# Paso 2 — auditoría de datos (no bloquea, sólo reporta)
python scripts/qa/check_data_quality.py --samples 3

# Paso 3 — verificación SSR contra una oferta real
python scripts/qa/verify_ssr.py \
    https://contrataoplanta-production.up.railway.app/oferta/<ID>
```

Tres comandos, tres caras del deploy cubiertas.
