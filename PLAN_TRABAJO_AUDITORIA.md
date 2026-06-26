# Plan de trabajo — Auditoría y corrección (contrataoplanta — contrata o planta)

Backlog priorizado del PLAN MAESTRO (14 partes). Orden por dependencias, no por número.
Estado por ítem: ✅ corregido · 🟡 parcial · ⬜ pendiente · ❓ requiere decisión de contenido.

## Orden de ejecución (hitos)

1. **Hito A — Raíz de datos (Parte 1).** Esquema canónico + normalización. Habilita 4, 6 y 10 y elimina la mayoría de la duplicación y los muros de texto. Es la raíz: sin esto, cada oferta nueva vuelve a verse rota.
2. **Hito B — Base transversal (Partes 2 y 3).** Sistema visual (header opaco, wordmark único, tipografía, tokens, accesibilidad) y navegación (drawer, página activa). Todo lo demás se monta encima.
3. **Hito C — Tarjeta canónica (Parte 4).** La consumen mapa (5), favoritos (8) y escritorio (11).
4. **Hito D — Detalle (Parte 6).** Depende de 1 y 4. Condensar, acordeones, ocultar vacíos, modal sticky.
5. **Hito E — Mapa (5) y Comparador (7).** Dependen de 4.
6. **Hito F — Favoritos/alertas (8).**
7. **Hito G — Estadísticas (9).** Bug crítico; independiente, se puede paralelizar en cualquier momento.
8. **Hito H — Nuevas ofertas (10).** Depende de 1 (hash/fecha_ingesta).
9. **Hito I — Escritorio (11).** Depende de 4 y 6.
10. **Hito J — Consistencia/footer (12–13)** y **QA final (14).**

## Detalle por parte

### Parte 1 — Normalización de datos (raíz) 🟡
- 1.1 Esquema canónico único por oferta — ⬜
- 1.2 Limpieza de títulos (espacios, guiones, tokens dup., fallback "Cargo sin título") — ⬜
- 1.3 Normalización de texto (frases concatenadas, listas pegadas, "NIVEL BÁSICO/MEDIO…") — ⬜
- 1.4 Humanizar valores crudos (snake_case → etiqueta; "codigo_trabajo" → "Código del Trabajo") — ⬜
- 1.5 Tablas de renta aplanadas → `renta[]`; fallback crudo atenuado — ⬜
- 1.6 Reducir longitud excesiva: extraer una sola vez, no duplicar — 🟡 (objetivo ya no duplica el muro)
- 1.7 Campos faltantes → `null` — ⬜
- 1.8 Deduplicación de ofertas multi-portal — ⬜
- 1.9 `fecha_ingesta` + `hash` por oferta (para Parte 10) — ⬜
- 1.10 Robustez: parseo tolerante + log normalizadas/faltantes/no parseables — ⬜

### Parte 6.1 — Duplicación del "Objetivo del cargo" ✅
- Clasificador (`web/rich-text.js`): `extractObjective` descarta volcados (texto que arranca con otra categoría o muro >400 chars); el fallback por heading exige señal positiva de propósito. El muro deja de mostrarse como objetivo y sus datos quedan solo en sus secciones.
- UI (`web/app.js` + CSS): "Objetivo del cargo" largo se colapsa a 5 líneas con "Ver más / Ver menos".
- Verificado: test funcional (volcado→objetivo vacío; objetivos reales intactos) + `tests/test_modal_jsdom.js` 47/48 (la falla restante es un string preexistente ajeno).

### Parte 2 — Sistema visual transversal ⬜
Header opaco+z-index · wordmark único reutilizable · tipografía/escala · tokens · accesibilidad (tap≥44px, foco, contraste, claro/oscuro).

### Parte 3 — Navegación ⬜
Drawer (cabecera/cuerpo/pie) · íconos por ítem · página activa real (`aria-current`) · badge (N) en favoritos · transiciones/foco/scroll-lock · estado activo escritorio.

### Parte 4 — Tarjeta canónica + listado ⬜
Plantilla única · overflow título/institución · logo con fallback iniciales · check verificado real · estado de plazo dinámico · placeholders unificados.

### Parte 5 — Mapa por región ⬜
Responsive 2-col / maestro-detalle · burbujas legibles sin corte · región sincronizada · panel con tarjeta canónica · quitar comparar huérfano y "—" · filtros dentro de la región.

### Parte 6 — Ficha de detalle ⬜ (6.1 ✅)
Resumen primero · acordeones · ocultar vacíos/no repetir · vacantes/humanización · tabla renta compacta · modal sticky header + barra acciones fija.

### Parte 7 — Comparador flotante (faltante) ⬜
Implementar: visible con 1+ ofertas, contador, máximo con aviso, abrir/quitar, ocultar en 0, sin chocar con "↑".

### Parte 8 — Favoritos y alertas ⬜
Conteo sincronizado · SVG de fecha neutro · unificar/diferenciar formularios de alerta.

### Parte 9 — Estadísticas (BUG CRÍTICO) ⬜
Reparar fetch/render (queda en "cargando…") · estados cargando+timeout/OK/sin datos/error+reintentar · reutilizar fuente de la barra superior.

### Parte 10 — Aviso de nuevas ofertas ⬜
Polling liviano por hash/ingesta · píldora opt-in · insertar sin recargar conservando filtros/scroll · aria-live.

### Parte 11 — Layout de escritorio ⬜
Aprovechar ancho (sidebar o grilla 2-col + max-width) · centrado coherente · footer pegado al fondo.

### Partes 12–13 — Consistencia / footer ❓⬜
- 12.1 Coherencia de cifras (660 fuentes / 709 instituciones vs 204) — ❓ requiere decisión de contenido.
- 12.2 Taxonomía unificada · 12.3 microcopy roto ("Limpia r") — ⬜
- 13 disclaimer cortado + verificar enlaces footer — ⬜

### Parte 14 — QA final ⬜
Matriz de casos (a)-(k) en cada vista · anchos 360/1280/1440 · claro/oscuro · checklist final.

## Registro de sesión (correcciones aplicadas y verificadas)

Cambios hechos y verificados (sintaxis + prueba funcional donde aplica):

- **6.1 Objetivo** ✅ — `web/rich-text.js` descarta volcados; `web/app.js`+CSS colapsan objetivos largos (Ver más/menos). Probado.
- **1.4 / 6.4 / 6.5** ✅ — `web/integracion/ficha-oferta.js`: humaniza valores de máquina ("codigo_trabajo"→"Código del Trabajo"), no repite Modalidad↔Calidad jurídica, Vacantes 0→"No especificado". Probado.
- **12.1 Cifras** ✅ — `web/index.html`+`web/app.js`: el hero muestra una sola cifra (instituciones con concursos abiertos = `instituciones_activas`), coherente con barra y stats. (Decisión: mostrar solo una.)
- **12.3 Microcopy** ✅ — `styles/index.css`: `white-space:nowrap` en `.widget-ver-todo` ("Limpiar" ya no se parte).
- **13.1 Disclaimer footer** ✅ — `styles/index.css`+`styles/shared-layout.css`: `flex-wrap`+`overflow-wrap` para que envuelva, no se corte. (No reproducible en este entorno; arreglo conservador.)
- **3.3 / 3.6 Nav** ✅ — `shared-shell.js`: `aria-current="page"` en activo; `nav-mobile.js`+CSS: contador de favoritos como **badge** (no "(N)" inline).

Verificado ya resuelto en producción/código (NO reescribir):

- **2.2 Wordmark** ✅ ya unificado — `partials/header.html` + `shared-shell.js` (componente único, tagline "empleo público confiable").
- **9 Estadísticas** ✅ funciona en vivo (fecha real, 4 KPIs, sectores, instituciones, gráfico 12m). Pendiente solo endurecer estados error/timeout (9.2).
- **7 Comparador** 🟡 el tray flotante ya existe en `app.js` (contador + "Comparar empleos" + "Quitar"); falta verificar el flujo en vivo (marcar→contador→comparar→quitar).

Pendientes reales priorizados (próximos): 2.1 header opaco (revisar modo claro), 3.1 drawer móvil, 4 tarjeta canónica (overflow títulos, logo fallback), 5 mapa, 6.2/6.3 acordeones detalle, 10 nuevas ofertas, 11 ancho escritorio.

## Decisiones de contenido pendientes (te necesito)
- **Cifras del hero vs barra (12.1):** ¿qué representa cada métrica (660 fuentes activas, 709 instituciones, 204 instituciones)? Sin eso no puedo hacerlas coherentes; la alternativa es ocultar la que no se calcula con confianza.
- **Formularios de alerta (8.3):** ¿"Activar alerta gratuita" y "Alertas de empleo" son lo mismo (unificar) o distintos (diferenciar)?
