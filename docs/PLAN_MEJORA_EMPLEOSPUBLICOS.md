# Plan de mejora — calidad de ofertas (empleospublicos y municipales)

Estado a 2026-06-10. Trabajo ya aplicado en BD/código marcado con ✅; pendiente con ☐.

## Resumen de lo ya aplicado (este ciclo)

- ✅ **136 ofertas no-laborales desactivadas** (noticias, actas, decretos,
  etiquetas de navegación) vía `scripts/limpiar_ofertas_no_laborales.py --apply`.
  Copiapó quedó en 0 activas; activas totales 1162 → 1026.
- ✅ **89 ofertas de hospitales desatadas de "Superintendencia de Pensiones"**
  (`institucion_id` puesto a NULL donde el nombre venía vacío). id=85 quedó con 1.
- ✅ **192 `url_bases` muertos anulados** (terminaban en `…aspx#contenido`).
- ✅ **Validador `intake.py` reforzado**: fecha desde la URL (`/AAAA/MM/DD/`),
  cap de cierre futuro inverosímil (>150 d), publicación >365 d invalida el
  cierre, rechazo de actas/decretos/etiquetas, y la señal de empleo
  ("concurso público", "cargo de"…) anula el descarte por ruta `/noticias`.
- ✅ **`url_bases` deja de guardar el ancla muerta**: `_extraer_url_bases`
  devuelve `None` cuando no hay enlace real a bases.

## Issue 1 — Misatribución de institución (CRÍTICO)

**Causa raíz real.** No es el matching: `base.py::match_institucion_id` ya
devuelve `None` con nombre vacío. El problema es que la **extracción del
organismo empleador falla** (`institucion_nombre = ''`) en la ficha de detalle
de empleospublicos; ese vacío, en el scraper viejo, terminaba pegado al id 85.

- ✅ Limpieza de las 89 filas históricas.
- ☐ **Arreglar la extracción de `institucion_nombre`** en
  `_extraer_metadata_detalle` (campo "Institución / Entidad" de
  `avisopizarronficha.aspx`). Requiere HTML real (ver Issue 4, comparten parser).
- ☐ **Endurecer el substring bidireccional** del matching
  (`if key in candidate_key or candidate_key in key`): exigir ratio de longitud
  (p. ej. `len(menor)/len(mayor) > 0.6`) para no pegar nombres cortos por azar.
  Cambio acotado en `base.py`, requiere correr la suite de tests.
- ☐ **Regla de validador (defensa en profundidad)**: en `intake.py`, si el
  cargo es claramente de un sector (salud: enfermer/paciente/UTI/anestesia) y la
  institución asignada es de otro sector, marcar `needs_review`. Bajo riesgo.

Esfuerzo: medio. Verificación: re-scrapear 5–10 fichas y confirmar nombre no vacío.

## Issue 2 — `url_bases` inútil (`#contenido`)

- ✅ Datos limpiados (192) y ✅ código corregido (`_extraer_url_bases → None`).
- ☐ Verificar en la próxima corrida que no se guarden nuevos `#contenido`.

Esfuerzo: hecho.

## Issue 3 — El validador de URLs no se ejecuta

**Causa.** `run_all.py` no invoca `validate_offer_urls.py` (lo hacía el viejo
`run_scrapers.py`). Hoy 237/1026 activas tienen `url_oferta_valida = NULL`, así
que el frontend no puede gatear "Ver bases"/"Postular".

- ☐ **Correr una vez** `validate_offer_urls.py` sobre las activas para poblar
  los flags (se corre en Railway/servidor; hace requests HTTP a las URLs).
- ☐ **Cablearlo de forma recurrente**: recomendado como **paso separado** del
  timer/cron de scrapers en Railway (no dentro de `run_all`, para no inflar la
  corrida principal con cientos de requests de red). Alternativa: invocar su
  entry programática al final de `run_all` con un límite de tiempo.

Esfuerzo: bajo. Riesgo: bajo (solo escribe flags de validez).

## Issue 4 — Campos incompletos (jornada 73%, grado 65%, institución vacía)

**Causa.** Los selectores/regex de `_extraer_metadata_detalle`,
`_extraer_jornada` y el parse de grado no matchean el markup real de la ficha
`avisopizarronficha.aspx` en la mayoría de los casos. Comparte parser con Issue 1.

Plan (no se puede hacer a ciegas, necesita HTML real):

1. ☐ Script que baje 10–15 fichas de detalle reales y guarde su HTML.
2. ☐ Inspeccionar el markup de Institución/Entidad, Jornada, Grado/Renta.
3. ☐ Ajustar `_extraer_metadata_detalle` / `_extraer_jornada` / parse de grado.
4. ☐ Tests con las muestras (HTML fijo → campos esperados), sin red.

Esfuerzo: medio-alto. Riesgo: bajo (solo mejora extracción).

## Orden sugerido

1. ✅ Limpiezas de datos + `url_bases` (hecho).
2. ☐ Issue 3: correr `validate_offer_urls` una vez + cablearlo recurrente. Rápido, impacto visible.
3. ☐ Issues 1+4 juntos: capturar fichas reales y arreglar el parser de detalle (institución, jornada, grado). El mayor salto de calidad.
4. ☐ Issue 1: endurecer matching + regla de sector en el validador. Defensa en profundidad.

## Para que lo aplicado persista

Commitear y pushear los cambios de código de este ciclo:
`scrapers/intake.py`, `scrapers/empleos_publicos.py`,
`scripts/limpiar_ofertas_no_laborales.py`, `docs/`.
Sin eso, el deploy no toma el validador reforzado ni el fix de `url_bases`.
