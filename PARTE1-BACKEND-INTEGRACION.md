# Parte 1 — Backend: parches y cómo integrarlos

Estado tras auditar el pipeline real. Lo que **ya estaba** y lo que **entrego nuevo**.

## Estado por sub-ítem (con evidencia)

- **1.1 esquema canónico** — existe. `models/job_posting.py`, tabla `ofertas` en `db/schema.sql`.
- **1.2 / 1.3 / 1.4 / 1.6 (display)** — ya hechos en el frontend (`web/app.js`, `web/rich-text.js`, `web/integracion/ficha-oferta.js`). No se tocan.
- **1.5 tabla de renta multi-región** — FALTABA. Hoy `extraction/salary_extractor.py` solo saca un monto único; `db/schema.sql:88-91` guarda `renta_bruta_min/max/texto/grado_eus`, sin estructura por región. → **Entrego parser + columna `renta_regional` + persistencia. Falta API+UI.**
- **1.7 nulos / "-" / "N/A" → ausente** — parcial. El frontend ya oculta vacíos; faltaba en servidor. → **Entrego `es_valor_nulo`.**
- **1.8 dedup entre portales** — FALTABA. Hoy solo `url_hash` exacto (`db/database.py:131`, `db/schema.sql:126`). → **Entrego `clave_dedup_difusa` + migración + fusión cableada en `upsert_oferta`.**
- **1.9 `fecha_ingesta` + `hash`** — YA CUBIERTO. `ofertas` tiene `detectada_en` (timestamp de ingesta) y `url_hash`. No hace falta columna nueva; la Parte 10 (polling) ya puede usar ambas.
- **1.10 log de ingesta + robustez** — parcial. El intake descarta y loguea por oferta, pero no había resumen de corrida. → **Entrego `IngestSummary`.**

## Lo que entrego (puro y testeado)

| Archivo nuevo | Qué hace | Test |
|---|---|---|
| `extraction/renta_regional.py` | `parse_renta_regional(texto) -> [RentaRegion] \| None`: reconstruye tabla región/sin bono/con bono; `None` si no es parseable (el caller conserva el crudo). | `tests/test_renta_regional.py` |
| `scrapers/ingest_summary.py` | `IngestSummary` (contadores OK / faltantes / no parseable / descartadas), `es_valor_nulo`, `campos_faltantes`. | `tests/test_ingest_summary.py` |
| `db/database.py` (+2 funciones) | `clave_dedup_difusa(institucion, cargo, fecha_cierre, region)`: llave difusa estable entre portales. | `tests/test_dedup_difusa.py` |

Correr los tests (en tu entorno con Python 3.11):

```bash
pytest tests/test_renta_regional.py tests/test_ingest_summary.py tests/test_dedup_difusa.py -q
```

> Nota honesta: **no pude ejecutarlos aquí** (sin DB ni sandbox en esta sesión). Verifiqué la lógica a mano y con una revisión estática independiente: los 17 casos pasan en trazado. Confírmalo corriendo pytest.

## Cableado ya escrito en código (no ejecutado — necesita tu DB)

Estos cambios ya están en el repo; **no pude correrlos** (sin DB en la sesión). Los cubren tests con `FakeSession`, pero el efecto real sobre Postgres tienes que verificarlo tú.

### 1.8 — Fusión difusa en `upsert_oferta` — HECHO

- Migración: `db/migrations_alembic/versions/20260622_0001_dedup_renta.py` (agrega `dedup_hash` y `renta_regional`, idempotente).
- `db/database.py::upsert_oferta`: calcula `clave_dedup_difusa(...)`, lo guarda en `dedup_hash` (INSERT y UPDATE) y, **solo cuando hay `fecha_cierre`**, busca una gemela por `dedup_hash` antes de insertar. Si la encuentra, refresca esa fila (conserva el enlace ya establecido) en vez de duplicar.
- Test: `tests/test_db_persistence.py::test_fusion_difusa_entre_portales` y `::test_insert_incluye_dedup_hash_y_renta_regional`.

Decisión de diseño: la fusión solo actúa con `fecha_cierre` presente (señal fuerte) para no unir avisos sin plazo del mismo cargo. Riesgo asumido (lo dice el propio plan 1.8): dos concursos realmente distintos con misma institución+cargo+cierre+región se fusionarían. Si te preocupa, súbele el umbral añadiendo el grado/estamento a la llave.

### 1.5 — Renta multi-región de punta a punta — HECHO (1.5 + 6.6)

- Backend: `upsert_oferta` llama a `parse_renta_regional(renta_texto o descripcion)` y guarda el resultado en `renta_regional` (JSONB) o NULL.
- API: `api/services/sql.py::ofertas_select_sql()` ahora selecciona `o.renta_regional`; `serialize_offer` hace `dict(row)`, así que fluye al payload sin más cambios (psycopg2 deserializa JSONB a lista de dicts).
- Frontend: `web/app.js::normalizarOferta` expone `rentaRegional`; `web/integracion/ficha-oferta.js` renderiza una tabla compacta región / sin bono / con bono (Parte 6.6) cuando hay filas; si no, queda el campo "Renta bruta" del grid.
- Caveat: solo lo hace la ficha nueva (`FichaOferta`). El modal legacy de fallback (`_abrirModalLegacy`) no muestra la tabla — si querés, se replica después.

### Orden de despliegue (importante)

El nuevo código de `upsert_oferta` referencia `dedup_hash` y `renta_regional`, que **solo existen tras la migración**. Aplica en este orden:

1. `alembic upgrade head` (crea las columnas). En la API el deploy ya lo hace; **los scrapers corren en otra unidad systemd** (`contrataoplanta-scrapers.service`) — asegúrate de que la migración esté aplicada antes de la próxima corrida de scrapers, o el INSERT fallará.
2. Recién entonces desplegar el `db/database.py` nuevo.
3. Como las columnas vienen solo de Alembic (igual que la migración previa `oferta_contacto`, que tampoco está en `schema.sql`), una DB nueva debe correr `alembic upgrade head` (no solo `stamp head`) para tenerlas.

### 1.10 — Resumen de corrida — HECHO (nivel corrida) + opcional (por campo)

Cableado: `scrapers/run_all.py::persistir_corrida` ahora emite al cerrar una línea `log.info("ingesta_resumen ...")` con instituciones / encontradas / ok (nuevas+actualizadas) / descartadas / vencidas / errores / tasa + los top reason codes de descarte (incluyen renta/contenido no parseable). Es aditivo: solo agrega un log, no toca la persistencia.

**Opcional — detalle por campo:** el resumen de corrida usa agregados. Si querés el conteo fino "con campos faltantes" / "renta no parseable" por oferta, instrumentá el bucle de upsert con `scrapers/ingest_summary.py` (que también trae `es_valor_nulo`, cerrando 1.7 en servidor):

```python
from scrapers.ingest_summary import IngestSummary, campos_faltantes
resumen = IngestSummary()
CAMPOS_CLAVE = ["cargo", "institucion_nombre", "region", "fecha_cierre", "renta_texto"]
# por cada oferta aceptada:
resumen.registrar_oferta(
    faltantes=campos_faltantes(oferta, CAMPOS_CLAVE),
    renta_no_parseable=(parse_renta_regional(oferta.get("renta_texto")) is None and bool(oferta.get("renta_texto"))),
)
# por cada descarte del intake: resumen.registrar_descarte()
# al final:
logger.info(resumen.log_line())
```

Esto da el log que pide 1.10 (normalizadas OK / con campos faltantes / renta no parseable / descartadas), sin frenar la corrida si una fuente cambia de formato.
```
