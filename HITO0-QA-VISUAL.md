# Hito 0 — QA visual sobre el sitio desplegado

QA hecho con navegador real sobre `https://estadoemplea.pages.dev` en escritorio (1280–1568px), claro. Confronté cada hallazgo contra el código actual del repo.

## Conclusión principal (lo que más importa)

**El sitio desplegado corre un build MÁS VIEJO que el repo.** Varios "bugs" del plan maestro que se ven en pantalla **ya están corregidos en el código**, solo que no están desplegados. Evidencia directa:

- **Cifras 660/709 (Parte 12.1):** en el deploy el hero dice "660 fuentes activas de 709 instituciones". En el repo (`web/index.html:100`) eso ya se reemplazó por un solo número en vivo: `<span id="hero-instituciones-activas">` → "N instituciones públicas con concursos abiertos". `web/app.js:1803` lo comenta: "(dos números que confundían)". → **Ya arreglado, falta deploy.**
- **"Limpiar" → "Limpia r" (Parte 12.3):** en el deploy el botón del widget de sectores se parte en dos líneas. En el repo (`web/styles/index.css:1089`) `.widget-ver-todo` ya tiene `white-space: nowrap` con el comentario "12.3". Con eso no puede partirse. → **Ya arreglado, falta deploy.**

Por lo tanto, **la acción de mayor rendimiento ahora mismo es desplegar el repo actual** (Cloudflare Pages). Mucho del plan ya está hecho en código pero invisible para el usuario porque el deploy quedó atrás. Esto también aplica a todo lo que toqué en esta sesión (Partes 1, 2, 9): vive en el repo, no en producción.

## Verificado FUNCIONANDO en el deploy (claims del plan ya obsoletos)

- **2.1 Header opaco** ✓ — fondo navy sólido, sin traslucidez, sticky, z-index correcto.
- **2.2 Wordmark** ✓ — "contrata o planta .cl" + tagline "EMPLEO PÚBLICO CONFIABLE" consistente, mismo lockup en todas las páginas.
- **Parte 7 Comparador** ✓ — al marcar una oferta aparece la bandeja flotante "N para comparar · Comparar empleos · Quitar", con contador; persiste tras recargar (localStorage); no choca con el botón "↑". El claim del plan "no aparece el comparador" está **obsoleto**.
- **Parte 6 Ficha de detalle** ✓ — modal condensado: cabecera con kicker, institución + check verde SEPARADO de la X de cierre, plazo, grilla de metadatos humanizados (sin snake_case), requisitos en dos columnas. Sin el volcado duplicado de "Objetivo del cargo".
- **Parte 9 Estadísticas** — cargó bien (KPIs 1.209/368/42/206 + fecha real). Esto **confirma que el bug es intermitente** (depende del cold-start de Railway): ahora estaba caliente. Es exactamente el modo de fallo que ataca el fix de esta sesión (timeout + caché + reintento), aún sin desplegar.
- **Parte 11.1 Layout escritorio** — el home usa sidebar poblado (filtros + alerta + sectores) junto a las tarjetas; no se ve la "franja blanca muerta".

## Genuinamente abierto (no es solo deploy)

- **1.3 Listas pegadas con guiones (a verificar):** en una ficha, las competencias llegaron como "-Iniciativa-Flexibilidad-Tolerancia a la presión" y "-Respeto…-Trabajo en equipo-Orientación…" sin explotar a lista. El parser `rich-text.js` maneja " - " (guiones con espacios) pero los run-ons "-Palabra-Palabra" sin espacios probablemente no. Como el deploy es viejo, hay que confirmar contra el `rich-text.js` actual antes de tocarlo; si sigue, es una mejora acotada del parser. Menor.
- **Microcopy "1 años":** "Al menos 1 años de experiencia laboral" (debería ser "1 año"). Dato parseado; singular/plural.
- **2.5 Tap-targets 44px:** toggle de tema (28px), paginación (34px), X del modal (28px) pasan WCAG AA (24px) pero no los 44px del plan. Inflarlos toca layouts densos: requiere ajuste visual fino, no a ciegas.

## Móvil — visual imposible, auditoría de código en su lugar

El navegador disponible **no emula viewport móvil**: redimensionar la ventana a 390px no cambia el render (la captura se mantiene en 1568px con el nav de escritorio completo, sin hamburguesa, probado 3 veces). Así que el QA móvil visual no se pudo hacer acá — conviene verificarlo en un teléfono real o en DevTools (device mode).

En su lugar audité el **código móvil** contra los claims del plan, y se repite el patrón: ya están corregidos en el repo.

- **3.1 Drawer "plano y vacío":** falso en el repo. `web/nav-mobile.js` monta un panel real en `body` con botón de cierre, links (`buildPanelLinks()`), contador de favoritos dentro del panel y manejo de foco (`:focus-visible`, foco al primer link, cierre por overlay/esc).
- **13.1 Disclaimer del footer cortado:** ya corregido. `web/styles/index.css:1400` tiene el comentario literal "13.1: permitir que el disclaimer envuelva… flex-wrap + gap evitan el truncado", más `overflow-wrap: anywhere` y una regla móvil que apila el footer en columna.
- **12.3 microcopy y demás:** ya en `@media (max-width:600px)` con reestructuración de los controles del listado en dos filas.

Lo que NO pude confirmar ni por código rápido: overflow real de tarjetas ("…San Anto"), mapa maestro-detalle móvil, footer en páginas cortas pegado al fondo. Requieren viewport móvil real.

## Recomendación

1. **Desplegar el repo actual primero.** Resuelve de un golpe varios ítems del plan que ya están en código (12.1, 12.3, y todo lo de esta sesión). Sin esto, seguir "arreglando" es invisible.
2. Después, re-correr este QA contra el deploy nuevo para separar lo que de verdad queda (1.3, móvil, 2.5 tap-targets) de lo que ya estaba resuelto.
