# Auditoría: ofertas de empresas del estado (2026-06-11)

Fuente: API de producción (`/api/ofertas?sector=Empresa del Estado`, filtros `tipo` y `renta_min`), ofertas vigentes al 2026-06-11.

## Resumen

| Métrica | Valor |
|---|---|
| Ofertas activas del sector | 89 (10% del sitio) |
| `tipo_contrato` = Código del Trabajo | 73 (82%) |
| `tipo_contrato` = Planta | 7 — **100% erróneas** |
| `tipo_contrato` = Contrata | 4 — **100% erróneas** |
| Sin tipo (null) | 5 |
| Honorarios / Plazo fijo / Indefinido | 0 |
| Con renta estructurada (`renta_bruta_min`) | 1 de 89 (SASIPA vía Buk, $1.100.000–$1.150.000) |

## Detalle de etiquetas erróneas

Las empresas del estado operan bajo Código del Trabajo; ninguno de estos avisos declara calidad contractual. Las etiquetas provienen de matching por substring sobre la descripción.

**"Planta" (7):**

| id | Institución | Cargo | Causa |
|---|---|---|---|
| 15981 | ENAP | Ingeniero(a) de Confiabilidad Eléctrico | "disponibilidad de las **plantas**" |
| 15980 | ENAP | Ingeniero(a) de Confiabilidad Electrónico | ídem |
| 15979 | ENAP | Ingeniero(a) de Confiabilidad Equipos Estáticos | ídem |
| 16233 | ENAP | Ingeniero(a) de Confiabilidad Equipos Rotativos | ídem |
| 14872 | ENAP | Programador/a | "disponibilidad de la **planta**" |
| 5252 | Emp. Portuaria Coquimbo | "Directorio y Personal Planta" | **No es una oferta**: sección de transparencia |
| 5251 | Emp. Portuaria Coquimbo | "Transparencia" | **No es una oferta**: índice de transparencia |

**"Contrata" (4):**

| id | Institución | Cargo | Causa |
|---|---|---|---|
| 14900 | BancoEstado | Encargada/o Comercial | "administrar **contrataciones**" |
| 14906 | BancoEstado | Jefe de Productos Vida | "procesos de **contratación**" |
| 14869 | ENAP | Jefe/a División Estudios y Cumplimiento | "empresas **contratistas**" |
| 14943 | Correos de Chile | Subgerente De Abastecimiento | "**contratación** de proveedores" |

## Hallazgo adicional

La oferta id 15010 figura con `institucion_id` 290 y nombre "SASIPA", pero la sigla (EPTSV) y el sitio web (puertotalcahuano.cl) del payload corresponden a Empresa Portuaria Talcahuano. En el catálogo, 290 es Talcahuano y SASIPA es 294. Revisar el matching institución↔oferta del scraper Buk.

## Correcciones aplicadas (2026-06-11, pendiente deploy)

1. `scrapers/base.py`, `extraction/contract_extractor.py`, `scrapers/trabajando.py`, `scrapers/buk.py`: calidad contractual solo por mención explícita ("a contrata", "calidad jurídica: contrata", "cargo de planta"...), nunca por substring. Default Código del Trabajo por categoría de institución (Empresa del Estado, Empresa FFAA, Empresa, Banco central) vía `normalizar_tipo_cargo(raw, categoria=...)`.
2. `scrapers/trabajando.py`: el campo `sueldo` de la API puebla `renta_bruta_min` (moneda CLP); se reconoce "renta líquida" (solo `renta_texto`) y "renta de $X" sin calificativo.
3. `classification/policy.py` (ruleset 2026.06.11): "contrata"/"planta" dejan de ser keywords positivas a secas; se agregan patrones y rutas negativas para secciones de Ley de Transparencia.
4. `db/limpieza_transparencia_empresas.sql`: soft-delete de los residuos (ids 5251, 5252 y patrón de rutas de transparencia).

## Cómo se corrige lo existente

Los upserts usan `COALESCE` donde el valor nuevo no nulo gana: tras desplegar y correr los scrapers de trabajando.cl, las 9 ofertas de ENAP/BancoEstado/Correos se reescriben solas a "Código del Trabajo". Las 2 páginas de transparencia requieren el script SQL (no se re-scrapean como ofertas con la nueva política).

Tests: `pytest tests/test_extraction.py tests/test_tipo_contrato_empresas.py tests/test_policy.py -q`.
