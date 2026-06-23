# Backlog priorizado — contrata o planta .cl

Fundamentado en el código real del repo, no solo en el plan maestro. Lo importante primero: **el repo está bastante más sano de lo que el plan asume.** El plan describe bugs observados en el sitio desplegado; varias "tareas de construcción" del plan ya tienen código y deben re-encuadrarse como **arreglo de comportamiento**, no como features faltantes.

## Desajustes plan ↔ código (léelo antes de planificar)

- **"Comparador faltante" (Parte 7).** Falso. Existe `web/integracion/comparador.js` completo (toggle, máximo 3, persistencia en `localStorage` `cmp_contrataoplanta`, overlay de comparación) y el botón en cada tarjeta (`app.js:1314`) y la bandeja flotante (`app.js:4470`). Si "no aparece", es un bug de render/CSS/posición, no ausencia. → Re-encuadrar como depuración.
- **"Archivo gigante / todo en uno".** Falso. `web/index.html` son 737 líneas con CSS y JS externos. La lógica vive en `web/app.js` (~4.500 líneas) y módulos en `web/integracion/`. El refactor visual no parte de "romper un monolito".
- **Normalización (Parte 1) ya existe parcialmente.** `normalizarOferta()` (`app.js:2222`), `normalizarTituloOferta()` (`app.js:595`), `formatRenta()` (`app.js:747`) y el parser semántico `web/rich-text.js`. La Parte 1 no es greenfield: es **endurecer y completar** lo que hay (tablas de renta, dedup, humanizar snake_case, recortar duplicación).
- **Mapa por región (Parte 5) ya existe.** `_renderMapaConteos()` (`app.js:4406`) + `web/integracion/mapa-chile.js`. De nuevo: arreglo de layout/overflow, no construcción.
- **Header/wordmark ya está centralizado.** `web/partials/header.html` cargado por `shared-shell.js` en todas las páginas. La Parte 2.2 ("estandarizar el wordmark") está en gran parte hecha; queda verificar consistencia por breakpoint y modo oscuro.

**Riesgo central que el plan no nombra:** *código presente ≠ comportamiento correcto.* No se puede saber qué partes están realmente rotas sin QA visual sobre el sitio desplegado (`https://estadoemplea.pages.dev`) en anchos 360/1280/1440, claro y oscuro. **Recomendación fuerte: una pasada de QA visual de 1–2 h ANTES de tocar más código**, para convertir el plan (escrito desde capturas) en una lista de bugs reproducibles. Sin eso, se arriesga "arreglar" cosas que ya funcionan.

## Estado por Parte (mapeo rápido)

| Parte | Tema | Estado en código | Acción |
|---|---|---|---|
| 1 | Datos / normalización | Parcial (existe base) | Endurecer: renta, dedup, humanizar, anti-duplicación |
| 2 | Sistema visual | Centralizado (header/tokens) | Auditar consistencia + a11y (tap-targets, foco) |
| 3 | Navegación drawer/nav | `nav-mobile.js` existe | QA: estado activo, ícono por ítem, badge favoritos |
| 4 | Tarjeta canónica | `renderCard()` existe | Auditar overflow, fallback logo, estado de plazo |
| 5 | Mapa por región | Existe | Layout maestro-detalle móvil, overflow burbujas |
| 6 | Ficha de detalle | Existe (`_abrirModalLegacy`) | Eliminar duplicación "Objetivo del cargo" (dep. P1) |
| 7 | Comparador | **Existe completo** | Depurar visibilidad/posición vs botón ↑ |
| 8 | Favoritos / alertas | Existe | Sincronizar contadores, ícono fecha SVG |
| **9** | **Estadísticas** | **ARREGLADO en esta sesión** | ✅ ver abajo |
| 10 | Aviso nuevas ofertas | No verificado | Requiere `hash`/`fecha_ingesta` (dep. P1) |
| 11 | Layout escritorio | — | Ancho/max-width, footer al fondo |
| 12 | Cifras / taxonomía | — | **Requiere decisión de contenido** (qué mide cada métrica) |
| 13 | Footer / legal | `partials/footer.html` | Overflow disclaimer, verificar enlaces |
| 14 | QA final | — | Matriz de casos dinámicos |

## Orden de ejecución recomendado (con dependencias)

**Hito 0 — QA visual de base (NUEVO, antes de todo).** Recorrer el sitio desplegado y convertir el plan en bugs reproducibles con captura + ancho + modo. Entrega: lista de bugs reales priorizada. Sin esto, el resto es a ciegas.

**Hito 1 — Parte 1 (datos/normalización).** Raíz: habilita 4, 6 y 10, y elimina la mayoría de la duplicación y los muros de texto. Empezar por: (a) tablas de renta parseables → `renta[]` con fallback a crudo+`null`; (b) anti-duplicación en la ficha (extraer una vez, no repetir); (c) humanizar valores máquina; (d) `hash`+`fecha_ingesta` por oferta (lo necesita la Parte 10). Entrega: log de normalizadas OK / con campos faltantes / no parseables.

**Hito 2 — Partes 2 y 3 (sistema visual + navegación).** Base transversal. Tokens, tipografía, a11y (tap-targets ≥44px, foco visible), drawer con estado activo real y badge de favoritos. Entrega: componentes consistentes en claro/oscuro.

**Hito 3 — Parte 4 (tarjeta canónica).** La consumen 5, 8 y 11. Overflow controlado, fallback de logo con iniciales determinista, estado de plazo idéntico en listado/detalle/favoritos. Entrega: una sola plantilla de tarjeta usada en todo el sitio.

**Hito 4 — Parte 6 (detalle).** Depende de 1 y 4. Prioridad: eliminar el volcado crudo de "Objetivo del cargo" que se repite parseado abajo. Resumen primero + acordeones + barra de acciones fija. Entrega: ficha condensada sin datos repetidos.

**Hito 5 — Partes 5 y 7 (mapa + comparador).** Dependen de 4. Mapa: maestro-detalle móvil, burbujas sin cortarse, panel con tarjeta canónica. Comparador: depurar por qué "no aparece" y separar del botón ↑. Entrega: ambos sin chocar con flotantes.

**Hito 6 — Parte 8 (favoritos/alertas).** Contadores sincronizados (header, tarjetas, badge drawer, vacío), ícono de fecha SVG, decidir formulario de alerta canónico.

**Hito 7 — Parte 9 (Estadísticas).** ✅ **Hecho en esta sesión** (independiente, se adelantó por ser bug crítico y autocontenido).

**Hito 8 — Parte 10 (nuevas ofertas).** Depende de 1 (hash/ingesta). Polling liviano, aviso opt-in, inserción sin recargar conservando filtros/scroll.

**Hito 9 — Parte 11 (escritorio).** Depende de 4 y 6. Aprovechar ancho, `max-width` coherente, footer pegado al fondo.

**Hito 10 — Partes 12–13 (pulido).** 12 **requiere decisión tuya de contenido**: definir qué representa "660 fuentes / 709 instituciones" vs "204 instituciones" antes de tocar nada; si una no se calcula con confianza, se oculta.

**Hito 11 — Parte 14 (QA final).** Matriz de casos dinámicos (a–k del plan) en 360/1280/1440, claro/oscuro.

## Parte 9 — Estadísticas: ya corregido

**Causa raíz.** `web/estadisticas.js` hacía un `fetch` en vivo a Railway **sin timeout ni AbortController y sin caché**. Si la petición se cuelga (cold start de Railway, red lenta), la promesa nunca resuelve ni rechaza: los esqueletos y "cargando…" quedan para siempre y el `catch` nunca dispara (por eso se ven esqueletos vacíos en vez del mensaje de error). El ribbon superior **sí** muestra esos números porque los pinta desde `localStorage` (`cop_ribbon_cache`), no del fetch en vivo.

Descartado: no era colisión de `const`/`fmt` globales — `estadisticas.js` es el único script con esos globales en esa página.

**Qué cambié** (`web/estadisticas.js` + estilo en `web/estadisticas.html`):
- `AbortController` con timeout de 10 s.
- Pinta KPIs y "última actualización" al instante desde `cop_ribbon_cache` (reutiliza la fuente que ya funciona, como pide la Parte 9.1).
- Estados explícitos (Parte 9.2): cargando con timeout, OK, sin datos, error/timeout con botón **Reintentar** (tap-target 44px). Si había caché, conserva los KPIs y solo marca con reintento las secciones del fetch en vivo.
- "Última actualización" nunca queda en "cargando…" infinito: muestra fecha real, fecha de caché, o "no disponible".
- Refresca la caché al éxito (sin romper el contrato del ribbon).

**Pendiente de verificación real:** no pude ejecutar el fetch contra Railway desde aquí (sandbox de red caído), así que el diagnóstico del "cuelgue" es la hipótesis más consistente con el síntoma, pero el fix es robusto ante cualquiera de las causas posibles (cuelgue, lentitud o error intermitente). Conviene confirmar en el navegador con la pestaña Network.
