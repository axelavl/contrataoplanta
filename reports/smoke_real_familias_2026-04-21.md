# Smoke real por familias — 2026-04-21

Se ejecutó `scripts/smoke_real_familias.py` con fuentes conocidas por familia:

- WordPress: id 387 (Municipalidad de Independencia)
- ATS HiringRoom: id 392 (Municipalidad de La Reina)
- Playwright: id 145 (Banco Central de Chile)

## Resultado consolidado

| Familia | Ofertas | Campos mínimos OK | Vigentes | Reason code (gatekeeper) | Profile |
|---|---:|---:|---:|---|---|
| WordPress | 0 | 0 | 0 | `empty_response` | `wordpress` |
| ATS HiringRoom | 0 | 0 | 0 | `empty_response` | `ats_hiringroom` |
| Playwright | 0 | 0 | 0 | `empty_response` | `playwright_js` |

## Observaciones clave

- En esta corrida el entorno no tuvo salida de red hacia los dominios objetivo (`Network is unreachable`), por lo que no fue posible recuperar publicaciones reales.
- En Playwright, además, el runtime indicó `Playwright no disponible en entorno` y ejecutó fallback.
- El JSON detallado de la ejecución quedó en `reports/smoke_real_familias.json`.
