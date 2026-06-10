# Estándar `ejecutar()` para scrapers — guía de migración

Esta guía define el "nuevo estándar" de scrapers (módulos auto-contenidos con
una función `ejecutar()`), derivado de los tres ya migrados y operativos:

- `scrapers/empleos_publicos.py` — portal central (clase + `ejecutar()`).
- `scrapers/carabineros.py` — sitio específico (DOM `div.concurso-card`).
- `scrapers/trabajando.py` — familia `*.trabajando.cl` (API-first).

Sirve para migrar el resto de forma fiable. **No** reemplaza al motor del
gatekeeper (`scrapers/evaluation/` + `GenericSiteScraper`), que sigue
atendiendo la cobertura masiva (~640 instituciones) vía `wordpress`,
`generic_site` y `playwright`.

## Por qué los scrapers restantes necesitan reescritura, no conversión

`hiringroom`, `buk`, `pdi` y `ffaa` (en `scrapers/plataformas/`) **no** son
standalone: heredan de `GenericSiteScraper`/`BaseScraper` y reusan su motor
(fetch async, scoring de ofertas, enriquecimiento, `candidate → OfertaRaw`,
persistencia y quality scoring). Lo "propio" de cada uno es solo el parseo
específico de su plataforma. Portarlos al estándar `ejecutar()` implica
reimplementar ese motor de forma standalone por cada scraper — es desarrollo
de scraping real que **debe probarse contra el sitio vivo y una BD** antes de
producción. No es una conversión mecánica.

Recomendación: desarrollá y probá cada versión standalone (como hiciste con los
tres anteriores) y se integra con el patrón de abajo.

## Anatomía de un módulo del estándar

Un scraper del estándar es un archivo en `scrapers/<nombre>.py` con:

1. **Fallback standalone.** Importa los módulos del proyecto dentro de
   `try/except ImportError`, con shims mínimos para poder correr suelto en
   dry-run/export sin la infraestructura. Define `STANDALONE = False/True`.

   ```python
   try:
       from config import config
       from db.database import (
           SessionLocal, generar_id_estable, limpiar_texto,
           marcar_ofertas_cerradas, normalizar_area, normalizar_region,
           registrar_log, upsert_oferta,
       )
       STANDALONE = False
   except ImportError:
       STANDALONE = True
       # ... shims ...
   ```

   `empleos_publicos.py` usa en cambio `from scrapers.base import LegacyBaseScraper, ...`
   porque reusa la clase base síncrona; ambos caminos son válidos.

2. **Descubrimiento de fuentes propio.** El módulo lee el catálogo
   (`repositorio_instituciones_publicas_chile.json`, en la raíz) y filtra sus
   fuentes. Ej. trabajando: `"trabajando.cl" in url_empleo`. Carabineros:
   `FUENTE` fija (id 161).

3. **Parsers puros y testeables.** Funciones sin red (`parsear_listado`,
   `parsear_detalle`, etc.) que toman HTML/JSON y devuelven estructuras. Esto
   permite tests unitarios con HTML de muestra.

4. **Mapeo al esquema estándar de 18 columnas** (idéntico en los tres):

   ```
   id_externo, fuente_id, institucion_nombre, sector, cargo,
   area_profesional, tipo_cargo, nivel, region, ciudad,
   renta_bruta_min, renta_bruta_max, renta_texto,
   fecha_publicacion, fecha_cierre, url_original,
   descripcion, requisitos_texto
   ```

5. **Persistencia vía `db.database`.** Upsert con `upsert_oferta(db, datos)`,
   cierre de vencidas con `marcar_ofertas_cerradas(db, fuente_id, urls_activas)`,
   log con `registrar_log(...)`. Dedup por `id_externo`/URL.

6. **Entry point `ejecutar(...)`** que orquesta descubrimiento → recolección →
   persistencia y **devuelve un dict de stats** con al menos:
   `encontradas, nuevas, actualizadas, cerradas, errores` (+ `duracion_seg`,
   `status`). Firma de referencia:

   ```python
   def ejecutar(dry_run=False, verbose=False, max_results=None,
                con_detalle=True, delay=..., export=None) -> dict[str, Any]: ...
   ```

7. **CLI `if __name__ == "__main__"`** con flags homologados:
   `--dry-run --verbose --max --delay --export`.

## Cableado en `run_all.py` (ya implementado para los 3)

El orquestador trata estos módulos como **batches sync** (igual que
`empleos_publicos`), fuera del dispatch de clases del gatekeeper, para que **no
corran dos veces**. Pasos para sumar un módulo nuevo:

1. Import del módulo:
   ```python
   from scrapers import <nombre> as <nombre>_scraper
   ```

2. Excluir su perfil del dispatch de clases (evita doble corrida). Agregar el
   `profile_name` del gatekeeper al set:
   ```python
   _PERFILES_NUEVO_ESTANDAR: frozenset[str] = frozenset(
       {"carabineros_pdf_first", "ats_trabajando", "<perfil_nuevo>"}
   )
   ```
   Los perfiles actuales por plataforma: hiringroom=`ats_hiringroom`,
   buk=`ats_buk`, pdi=`pdi_pdf_first`, ffaa=`ffaa_waf`.

3. En `main()`, dentro de `if not args.evaluate_only:`, después del batch de
   empleos_publicos, invocar el batch nuevo gateado por presencia de sus
   fuentes en `catalog_sources` (respeta `--ids`/`--limit`):
   ```python
   hay_<x> = any(<predicado de fuente> for s in catalog_sources)
   if hay_<x>:
       reports.append(await asyncio.to_thread(
           _run_modulo_ejecutar_sync, "<nombre>", <nombre>_scraper.ejecutar))
   ```

El helper `_run_modulo_ejecutar_sync(nombre, ejecutar_fn)` ya existe: corre
`ejecutar_fn(dry_run=False)` y traduce las stats a `PrecisionReport`.

> Mejora futura sugerida: reemplazar las entradas hardcodeadas por un pequeño
> registro (lista de `(perfil, predicado, módulo)`) para que sumar scrapers sea
> una sola línea de datos.

## Retiro de la clase vieja

Al migrar un scraper, su clase en `scrapers/plataformas/` deja de despacharse
(queda excluida por `_PERFILES_NUEVO_ESTANDAR`). **No borrar el archivo sin
verificar dependencias**: por ejemplo, varias clases comparten base
(`GenericSiteScraper`, `PdfFirstScraper`) y algunos tests las importan. Cuando
el módulo nuevo esté probado en producción, recién ahí conviene retirar la
clase y sus referencias en `plataformas/__init__.py`, `runtime_inventory.py` y
los tests.

## Checklist por scraper

- [ ] Versión standalone desarrollada y **probada contra el sitio vivo**.
- [ ] Parsers puros con tests unitarios (HTML/JSON de muestra).
- [ ] `ejecutar()` devuelve stats con las claves esperadas.
- [ ] Persiste vía `db.database` y respeta el esquema de 18 columnas.
- [ ] Cableado en `run_all`: import + perfil en `_PERFILES_NUEVO_ESTANDAR` +
      batch en `main()`.
- [ ] Verificado: la fuente no se despacha dos veces (clase + módulo).
- [ ] Tras validar en producción: retiro de la clase vieja y sus referencias.
