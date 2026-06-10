# Migraciones de schema — Alembic

## Estado actual

Hay **tres mecanismos** de DDL en el repo, cada uno con un rol distinto:

| Mecanismo | Dónde | Cuándo |
|---|---|---|
| `db/schema.sql` | Un archivo monolítico con `CREATE TABLE`, índices, triggers, constraints. | Para bootstrappear una DB vacía. |
| `db/migrations/001..005_*.sql` | SQL suelto, idempotente con `DO $$ IF NOT EXISTS ... END $$`. | Historia pre-Alembic — se aplica una vez detrás del schema base. |
| `db/migrations_alembic/` | Alembic (`versions/*.py` con `upgrade()`/`downgrade()`). | **Todo cambio de schema nuevo va acá.** |

El antipatrón que reemplazamos: `api/main.py::ensure_api_schema()` corría 60+ DDL en cada startup. Ya no se ejecuta automáticamente — la función queda disponible para uso manual, pero Alembic toma el control del schema.

## Flujos

### DB nueva (bootstrap)

```bash
# 1. Schema base + migraciones pre-Alembic
psql "$DATABASE_URL" -f db/schema.sql
for f in db/migrations/*.sql; do
  psql "$DATABASE_URL" -f "$f"
done

# 2. Marcar Alembic como "al día" sin re-aplicar nada
alembic stamp head

# 3. Verificar
alembic current
# Debería imprimir: 20260420_0001_sync_api_schema (head)
```

### DB existente (producción Railway)

La DB de producción ya tiene el schema aplicado (via `schema.sql` + `ensure_api_schema()` de los startups anteriores). Para ponerla bajo control de Alembic:

```bash
# Primera vez nada más — marcar como "al día"
railway run -- alembic stamp head
```

A partir de ahí, cada deploy corre:

```bash
railway run -- alembic upgrade head
```

Si no hay migraciones pendientes, es no-op. Si hay, se aplican en orden de `down_revision`.

### Agregar una migración

```bash
alembic revision -m "agrega columna X a ofertas"
```

Edita el archivo generado en `db/migrations_alembic/versions/`:

```python
def upgrade() -> None:
    op.execute("ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS x VARCHAR(100)")

def downgrade() -> None:
    op.execute("ALTER TABLE ofertas DROP COLUMN IF EXISTS x")
```

Patrón recomendado en este repo: **`op.execute()` con SQL raw idempotente** (`IF NOT EXISTS` en ADD, `IF EXISTS` en DROP). Evita el autogenerate de Alembic por ahora — la DB tiene mucho schema legacy que no está en `Base.metadata`, y confundir Alembic puede generar migraciones que borren cosas que no deben.

### Ver diff sin aplicar

```bash
alembic upgrade head --sql
```

Muestra el SQL que se correría, sin ejecutarlo. Útil para code review.

### Rollback

```bash
alembic downgrade -1
```

Baja una migración. Funciona sólo si la migración implementó `downgrade()` de forma no-destructiva. La mayoría de las migraciones en este repo (incluyendo `20260420_0001_sync_api_schema`) tienen `downgrade()` como `pass` — no se desarman para no perder datos. Si hay que revertir, drop y recrear la DB desde `schema.sql`.

## Cómo aplica el deploy

En Railway, antes de arrancar uvicorn (pre-deploy hook o comando en el `Procfile`):

```bash
alembic upgrade head && uvicorn api.main:app --host 0.0.0.0 --port $PORT
```

Si Alembic falla, uvicorn no arranca → el health check falla → Railway mantiene la versión anterior viva. Failsafe limpio.

## Env vars

Alembic lee la URL de DB vía `db.config.get_database_config()`. Las mismas env vars que usa la API y los scrapers:

- `DATABASE_URL` (Railway la inyecta al agregar Postgres plugin).
- Si no hay `DATABASE_URL`, usa split vars `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD`.

Sin password, el módulo aborta con error explícito.

## Archivos relevantes

```
alembic.ini                                    # config Alembic
db/migrations_alembic/env.py                   # integra Alembic + db.config
db/migrations_alembic/script.py.mako           # template de nuevas migraciones
db/migrations_alembic/versions/
    20260420_0000_baseline.py                  # baseline vacío
    20260420_0001_sync_api_schema.py           # traduce ensure_api_schema
db/schema.sql                                  # schema base para bootstrap
db/migrations/001..005_*.sql                   # historia pre-Alembic
```

## Reglas

1. **No reintroducir `ensure_api_schema()` al startup.** Fue removida por diseño.
2. **Nuevas migraciones van en `db/migrations_alembic/versions/`.** No agregar `*.sql` a `db/migrations/`.
3. **Idempotencia siempre.** `IF NOT EXISTS` en ADD, `IF EXISTS` en DROP, en toda nueva migración.
4. **`downgrade()` honesto.** Si no se puede revertir sin perder datos, `pass` con comentario explicando. No falsear.
