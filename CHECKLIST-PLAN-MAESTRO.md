# Checklist final — Plan Maestro contrata o planta .cl

Estado por Parte. Leyenda: ✅ corregido · 🟡 parcial (falta verificación en vivo) · 🔵 requiere decisión de contenido.

Última actualización del checklist: 23-06-2026.

---

## Parte 1 — Datos: ingesta y normalización ✅

Esquema canónico de oferta; limpieza de títulos y reparación de run-ons pegados por guiones (1.3); humanización de valores de máquina (`codigo_trabajo` → "Código del Trabajo"); reconstrucción de renta por región (`renta_regional`); deduplicación difusa por institución + título + cierre + ubicación; `hash`/`fecha_ingesta` para novedades; log de ingesta (normalizadas OK / con faltantes / no parseables). Campos ausentes → `null`, nunca inventados. Backend desplegado en Railway.

## Parte 2 — Sistema visual transversal ✅

Header opaco con z-index y padding correctos (2.1); wordmark unificado como partial reutilizable con tagline (2.2); tipografía serif/sans consistente (2.3); tokens de espaciado/radio/sombra (2.4); accesibilidad base: tap-targets, foco visible, contraste, claro/oscuro (2.5).

## Parte 3 — Navegación (drawer + nav escritorio) ✅ 🟡

Drawer móvil con ícono por ítem (lupa/corazón/libro/birrete/ayuda) y pie con toggle Claro/Oscuro + enlaces legales tras divisoria (3.1, 3.2). Estado "you are here" con `aria-current` y el contador de favoritos como badge, no como fondo de fila (3.3). 🟡 Falta verlo en móvil real (~360px) tras desplegar.

## Parte 4 — Tarjeta de oferta canónica ✅

Plantilla única en listado, mapa y favoritos; overflow controlado (institución/título a 2 líneas, sin cortar contra el borde); logo con fallback a iniciales determinista; estado de plazo dinámico idéntico en todas las vistas.

## Parte 5 — Vista "Mapa por región" ✅

Dos columnas en escritorio (mapa sticky + panel) y maestro-detalle con "← Volver al mapa" en móvil. Filas del panel **reestructuradas**: institución completa y cargo a 2 líneas sin recorte, con línea de meta (condición · plazo · renta · acciones) que aprovecha el ancho. Ícono de comparar huérfano y los "—" saneados.

## Parte 6 — Ficha de detalle ✅

Sin duplicación: "Objetivo del cargo" muestra solo el objetivo; requisitos/funciones/condiciones viven una sola vez en sus secciones (6.1). Tabla de renta compacta región/sin bono/con bono (6.6). Modal con header sticky y barra de acciones al pie.

## Parte 7 — Comparador flotante ✅

Bandeja visible solo con 1+ ofertas; texto "N ofertas seleccionadas" (singular/plural correcto); botón "Comparar"; límite con aviso "Puedes comparar hasta 3 ofertas"; quitar y vaciar; se oculta en 0; no choca con el botón ↑.

## Parte 8 — Favoritos y alertas ✅

Conteo sincronizado entre encabezado, tarjetas y badge del drawer; ícono de fecha en SVG neutro (no emoji iOS); un único formulario de alerta canónico (widget "Alertas de empleo" + botón "Activar alerta gratuita"), no duplicado. Sugerencia de cursos según los cargos guardados.

## Parte 9 — Estadísticas ✅

Fetch/render reparado con timeout ~10 s y estados explícitos (cargando / OK / sin datos / error-Reintentar). Verificado en vivo.

## Parte 10 — Aviso de nuevas ofertas ✅

Píldora opt-in "▲ N nuevas ofertas — Actualizar" con polling liviano contra la línea base; nunca inserta solo; conserva filtros y scroll; `aria-live`.

## Parte 11 — Layout de escritorio ✅ 🟡

"Buscar" usa sidebar persistente (alertas + "En este momento"); contenido centrado en ~1100px sobre 1280 de viewport, sin franja blanca muerta a la derecha (11.1). 🟡 Footer al fondo en páginas cortas (11.4): no se reproduce en tamaños comunes (favoritos vacío ya excede el viewport), no se forzó layout sticky para no arriesgar regresiones.

## Parte 12 — Consistencia de cifras, taxonomía y redacción ✅

Taxonomía unificada con "Otros" para lo no mapeado (12.2); "Limpiar" con `nowrap` (12.3); hero coherente con la barra ("cargos abiertos"); voseo argentino → tuteo chileno en todo el sitio (microcopy de comparador, mapa, cursos, gestión, favoritos, etc.).

## Parte 13 — Footer y legal ✅

Disclaimer "No somos el Estado…" no se corta (con `overflow-wrap`); los enlaces Términos, Privacidad, Descargo y Panel de mercado existen y resuelven (13.1, 13.2).

## Parte 14 — QA final ✅ (con 2 correcciones)

Pasada en vivo sobre el build desplegado, claro y oscuro, ancho 1280:

- **Home / Buscar:** sin overflow horizontal, sin "—" huérfanos, header opaco sin solape; hero "cargos abiertos" coherente con la barra.
- **Comparador:** "2 ofertas seleccionadas" + botón "Comparar"; ▲ solo en la renta mayor; descripciones limpias (sin "Funciones del cargo"); X separada del check.
- **Ficha de detalle:** resumen primero (renta, modalidad, estamento, vacantes, ubicación, publicación) sin celdas vacías ni "—"; correo extraído; X separada del check.
- **Estadísticas:** carga OK (sin esqueletos, con números, "última actualización" real).
- **Favoritos:** contador y tarjeta sincronizados; ícono de fecha en SVG (no emoji); sugerencia de cursos visible.
- **Cursos:** tarjeta nueva (banda de categoría + logo institucional) en claro/oscuro.

Correcciones hechas durante la QA (pendientes de push):

1. **Logos de cursos no cargaban** — el favicon directo falla en varios dominios (chilecompra.cl, subdere.gov.cl…). Cambiado a íconos de DuckDuckGo (confiable y sin tracking); verificado: cargan los 16.
2. **"1 ofertas guardadas"** en favoritos → ahora pluraliza ("1 oferta guardada" / "N ofertas guardadas").

Observaciones menores (no bloqueantes): el toggle de tema mide 28px en escritorio (cómodo con mouse; en móvil el área es mayor); la "Jornada" vacía en el comparador muestra "—" (tolerable en grilla comparativa). Sin verificar aún: drawer móvil a ~360px (este navegador no emula viewport móvil) — revisar en teléfono tras el deploy.

Nota de contenido: la sugerencia de cursos infiere bien el área del cargo, pero como el catálogo aún **no tiene cursos de Salud**, para un cargo de salud cae al relleno general. Se resuelve agregando cursos de esa categoría desde el admin.

---

## Pendientes que requieren tu decisión o acción 🔵

- **Logos de cursos en alta**: hoy se usa el favicon institucional del propio sitio que dicta el curso (primera parte, sin terceros), con monograma de respaldo. El campo `logo` por curso ya tiene prioridad si quieres subir logos nítidos más adelante.
- **AdSense**: andamiaje sembrado y seguro (slots ocultos hasta que cargue un anuncio real). Desactivado hasta que pongas tu `ca-pub-…`, los slot id y `enabled:true` en `web/ads-config.js`, y permitas los dominios de Google en el CSP.
- **Base de datos**: las migraciones de cursos/categorías/tipo ya están aplicadas en producción. Recomendación de seguridad: rotar la contraseña de Postgres (apareció en texto plano durante la configuración).

## Para desplegar el frontend

```
git add web/
git commit -m "cierre de pendientes del plan maestro"
git push
```
