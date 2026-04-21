# Inventario ejecutable de scrapers (runtime único)

Este documento define **una sola fuente de verdad** para saber:

1. Qué módulos se ejecutan en producción (`scrapers/runtime_inventory.py`).
2. Qué módulos legacy quedan marcados como `deprecated`.
3. Cuál es la fecha comprometida de retiro.
4. Cómo migrar cada scraper legacy al runtime nuevo basado en gatekeeper + pipeline.

> Enforcement técnico:
> - `scrapers/run_all.py` registra en logs este inventario al iniciar cada corrida.
> - `tests/test_runtime_inventory_contract.py` falla si aparece deriva entre runtime productivo y legacy deprecados.

## Producción (activo)

El runtime productivo corre exclusivamente los módulos de plataforma declarados en `PRODUCTION_RUNTIME_MODULES`.

| ExtractorKind | profile_name | módulo/clase ejecutada |
|---|---|---|
| `SCRAPER_WORDPRESS_JOBS` | `*` | `scrapers.plataformas.wordpress.WordPressScraper` |
| `SCRAPER_WORDPRESS_NEWS_FILTER` | `*` | `scrapers.plataformas.wordpress.WordPressScraper` |
| `SCRAPER_EXTERNAL_ATS` | `ats_trabajando` | `scrapers.plataformas.trabajando_cl.TrabajandoCLScraper` |
| `SCRAPER_EXTERNAL_ATS` | `ats_hiringroom` | `scrapers.plataformas.hiringroom.HiringRoomScraper` |
| `SCRAPER_EXTERNAL_ATS` | `ats_buk` | `scrapers.plataformas.buk.BukScraper` |
| `SCRAPER_EXTERNAL_ATS` | `*` | `scrapers.plataformas.generic_site.GenericSiteScraper` |
| `SCRAPER_PDF_JOBS` | `carabineros_pdf_first` | `scrapers.plataformas.carabineros.CarabinerosScraper` |
| `SCRAPER_PDF_JOBS` | `pdi_pdf_first` | `scrapers.plataformas.pdi.PdiScraper` |
| `SCRAPER_PDF_JOBS` | `*` | `scrapers.plataformas.generic_site.GenericSiteScraper` |
| `SCRAPER_CUSTOM_DETAIL` | `ffaa_waf` | `scrapers.plataformas.ffaa.FfaaScraper` |
| `SCRAPER_CUSTOM_DETAIL` | `*` | `scrapers.plataformas.generic_site.GenericSiteScraper` |
| `SCRAPER_PLAYWRIGHT` | `*` | `scrapers.plataformas.playwright_scraper.PlaywrightScraper` |
| `SCRAPER_GENERIC_FALLBACK` | `*` | `scrapers.plataformas.generic_site.GenericSiteScraper` |

## Legacy (deprecated)

Fecha de retiro comprometida: **2026-09-30**.

| módulo legacy | estado | retiro | reemplazo runtime |
|---|---|---|---|
| `scrapers/banco_central.py` | `deprecated` | 2026-09-30 | `GenericSiteScraper` |
| `scrapers/codelco.py` | `deprecated` | 2026-09-30 | `GenericSiteScraper` |
| `scrapers/externouchile.py` | `deprecated` | 2026-09-30 | `GenericSiteScraper` |
| `scrapers/gobiernos_regionales.py` | `deprecated` | 2026-09-30 | `GenericSiteScraper` |
| `scrapers/muni_la_florida.py` | `deprecated` | 2026-09-30 | `WordPressScraper` |
| `scrapers/muni_puente_alto.py` | `deprecated` | 2026-09-30 | `WordPressScraper` |
| `scrapers/muni_san_bernardo.py` | `deprecated` | 2026-09-30 | `WordPressScraper` |
| `scrapers/muni_temuco.py` | `deprecated` | 2026-09-30 | `WordPressScraper` |
| `scrapers/poder_judicial.py` | `deprecated` | 2026-09-30 | `GenericSiteScraper` |
| `scrapers/trabajando.py` | `deprecated` | 2026-09-30 | `TrabajandoCLScraper` |
| `scrapers/tvn.py` | `deprecated` | 2026-09-30 | `GenericSiteScraper` |

## Migración por scraper legacy

> Objetivo común: dejar cada scraper legacy solo como referencia histórica y ejecutar la fuente vía `scrapers/run_all.py` + `build_runtime_scraper`.

### `scrapers/banco_central.py`
- Migrar discovery de URLs a una regla de `SourceEvaluator` + `ExtractorKind.SCRAPER_GENERIC_FALLBACK`.
- Reusar extracción común (`JobExtractionPipeline`) sin nuevas heurísticas locales.

### `scrapers/codelco.py`
- Mantener la detección de vacantes como señales en `classification/rule_engine.py`.
- Ejecutar fetch y parsing con `GenericSiteScraper` para evitar forks de lógica.

### `scrapers/externouchile.py`
- Convertir selectores específicos en señales positivas/negativas dentro del pipeline.
- Consolidar persistencia en la capa común (`BaseScraper.run`).

### `scrapers/gobiernos_regionales.py`
- Pasar de listado estático/manual a clasificación por catálogo (`classify_source` + override por ID cuando aplique).
- Usar `GenericSiteScraper` por institución para trazabilidad en audit store.

### `scrapers/muni_la_florida.py`, `muni_puente_alto.py`, `muni_san_bernardo.py`, `muni_temuco.py`
- Eliminar parsing municipal ad-hoc y delegar a `WordPressScraper`.
- Ajustar señales de fecha/cierre en pipeline, no en el scraper municipal.

### `scrapers/poder_judicial.py`
- Mover reglas de rechazo de noticias/boletines a `rule_engine`.
- Resolver cierre y validez con `expiry_validator` común.

### `scrapers/trabajando.py`
- Derivar todo el flujo al adaptador ATS (`TrabajandoCLScraper`).
- Centralizar deduplicación en URL/hash estable de persistencia.

### `scrapers/tvn.py`
- Extraer señales de "oferta laboral" al clasificador compartido.
- Mantener runtime en `GenericSiteScraper` y desactivar ejecución directa legacy.
