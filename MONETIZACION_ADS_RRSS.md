# Propuesta de monetización: publicidad (AdSense) + estrategia de RRSS

Documento de trabajo. Cubre dos cosas que se refuerzan: (1) publicidad display
en el sitio y (2) una estrategia de redes sociales (TikTok/Instagram) que
genera el tráfico que esa publicidad necesita. Al final, cómo encajan con lo ya
construido (calculadora de renta, preparación, cursos, panel B2B).

## Evaluación honesta: ¿cuánto rinde la publicidad?

La publicidad display (AdSense) paga por cada 1.000 páginas vistas (RPM). Chile
tiene de los CPM más altos de Sudamérica, pero el display general sigue siendo
modesto: una estimación conservadora y realista para contenido general en Chile
es del orden de CLP 1.000–2.500 por cada 1.000 páginas vistas (≈ USD 1–3),
variable según nicho, época y proporción de tráfico móvil. Son estimaciones, no
una promesa.

Llevado a números: con 50.000 páginas vistas al mes hablamos de ~CLP
50.000–125.000 mensuales; con 200.000, de ~CLP 200.000–500.000. La conclusión
estratégica es clara: **la publicidad sola no te hace rentable; es una capa base
que escala con el tráfico.** El dinero por visitante es mucho mayor en las otras
vías (avisadores de cursos pagan por audiencia segmentada; el panel B2B cobra
por cliente, no por impresión). Por eso la prioridad es: traer tráfico con RRSS
→ monetizarlo en varias capas a la vez.

El riesgo de la publicidad: si abusas de la densidad de anuncios, dañas la UX,
subes el rebote y Google puede penalizar. Menos anuncios bien ubicados rinden
más que saturar la página.

## Lo que ya quedó construido para activar AdSense

- `web/ads-config.js` — configuración central. Hoy `enabled: false`. Cuando
  tengas tu cuenta, pones tu `client` (ca-pub-…), los `slots` de cada unidad y
  `enabled: true`.
- `web/ads.js` — inyecta las unidades respetando el CSP (sin scripts inline).
  Si está desactivado, no carga nada de terceros (no rompe ni ralentiza).
- `web/index.html` — ya carga ambos scripts y tiene dos espacios:
  `ad-slot-resultados` (banner bajo resultados) y `ad-slot-sidebar` (caja
  lateral).
- `web/_headers` — el CSP ya incluye los dominios de Google Ads en
  `script-src`, `connect-src` y `frame-src`. Mientras los anuncios estén
  desactivados, esos orígenes no se contactan.

### Pasos para activarlo
1. Crea cuenta en Google AdSense y verifica el dominio (`contrataoplanta.cl`
   o el dominio propio que uses).
2. Crea unidades de anuncio y copia el `client` y el `slot id` de cada una.
3. Rellena `web/ads-config.js` y pon `enabled: true`.
4. Publica. Si una unidad no carga, revisa la consola: casi siempre es el CSP.
   Algunas modalidades (Auto Ads) pueden pedir `'unsafe-inline'` en
   `script-src`; evalúa si vale la pena el costo de seguridad o usa solo
   unidades manuales (las que ya soporta `ads.js`).

### Dónde poner anuncios (sin arruinar la experiencia)
- **Index (alto tráfico):** los dos slots actuales bastan para empezar.
- **Páginas de contenido** (preparación, guías, glosario, calculadora): un solo
  bloque dentro del contenido (`<div class="ad-slot ad-slot-contenido"><div class="ad-slot-inner"></div></div>`)
  e incluir `ads-config.js` + `ads.js`. Estas páginas, si crecen con SEO, son
  las que más páginas vistas generan.
- **Evita** anuncios en la ficha de oferta justo sobre el botón “Postular” y en
  el panel B2B (ahí vendes datos, no impresiones).

## Estrategia de redes sociales (TikTok / Instagram)

El objetivo de RRSS no es “tener seguidores”, es **mover gente al sitio** (donde
monetizas) y **hacer crecer la lista de correos** (tu activo más valioso). El
sector público chileno es un nicho con dolor real y búsqueda constante: hay
materia prima infinita en tus propios datos.

### Tu ventaja: ya generas contenido sin escribir
El sitio produce datos que son contenido listo: ofertas nuevas cada día, rentas
por grado, instituciones que más contratan, plazos que cierran. Además ya
generas imágenes OG por oferta (`/api/og/{id}.png`) — esas imágenes sirven como
base visual para publicaciones.

### Pilares de contenido (qué publicar)
1. **Alertas de oportunidad** — “3 concursos que cierran esta semana en [región]”,
   “Nueva vacante en [institución]”. Formato rápido, recurrente, con CTA al sitio.
2. **Plata / transparencia de renta** — “Cuánto paga de verdad el grado 12”,
   “Planta vs. contrata: la diferencia en tu líquido”. Tu calculadora es oro
   para esto; es el contenido que más se comparte.
3. **Cómo postular bien** — errores que te dejan fuera, qué piden las bases,
   prueba psicolaboral. Sale directo de tu hub de preparación.
4. **Datos del mercado** — “Las áreas que más contrata el Estado este mes”,
   “Las 5 instituciones con más vacantes”. Sale del panel de mercado; además es
   el mismo dato que le vendes a las empresas de cursos (doble uso).
5. **Glosario / educación** — “Qué es un grado EUS”, “Qué significa estamento”.
   Capta a quien recién empieza.

### Formato y cadencia (realista para empezar)
- **TikTok / Reels:** 3–5 videos cortos por semana (15–40 s), verticales, con
  texto en pantalla y voz o música. El gancho en los primeros 2 segundos.
- **Instagram:** además de Reels, carruseles (“5 cosas que…”) que rinden mucho
  en este nicho, e Historias para las alertas diarias de cierre.
- **Constancia > perfección.** Mejor 4 videos simples por semana que 1 muy
  producido al mes. Reutiliza el mismo contenido en ambas plataformas.

### Automatización (apóyate en tus datos)
- Una tarea diaria/semanal puede generar el “guion” de las publicaciones de
  alertas a partir de la API (`/api/estadisticas`, `/api/ofertas`,
  `/api/mercado/agregados`): top instituciones, qué cierra pronto, áreas con más
  demanda. Tú solo grabas/armas el visual.
- Las imágenes OG ya existentes sirven como plantilla para posts de oferta.
- A futuro: generación semiautomática de carruseles desde plantillas.

### Embudo (cómo se convierte en plata)
RRSS → clic al sitio → la persona (a) ve anuncios [AdSense], (b) usa la
calculadora/preparación y deja su correo en alertas [activo + retención], (c)
vuelve por las alertas [tráfico recurrente que revende impresiones]. En paralelo,
el volumen y la segmentación que demuestras en RRSS y en el panel B2B es el
argumento para venderle a (d) empresas de cursos y (e) clientes de datos.

### Métricas que importan (no los likes)
- Clics al sitio desde RRSS (usa enlaces con UTM para medir).
- Correos nuevos en alertas por semana (la conversión real).
- Páginas vistas/mes (lo que mueve los ingresos por AdSense).
- Costo: tiempo. Empieza orgánico; pauta pagada solo cuando un formato ya
  funciona orgánicamente.

## Cómo encaja todo (capas de ingreso que se refuerzan)
1. **RRSS** trae tráfico barato y recurrente.
2. **AdSense** monetiza ese tráfico de forma pasiva (capa base).
3. **Alertas por correo** retienen y traen de vuelta (multiplican páginas vistas
   → más ingresos por ads, y son audiencia para avisadores).
4. **Directorio de cursos** cobra a empresas por llegar a esa audiencia
   segmentada (mayor ingreso por visitante que el ad display).
5. **Panel B2B** vende el dato agregado a instituciones/consultoras/prensa
   (ingreso por cliente, el de mayor margen).

No dependas de una sola capa. El error típico de los portales de empleo es vivir
solo de publicidad; aquí la publicidad es el piso, no el techo.

## Próximos pasos sugeridos
- Activar AdSense (cuenta + IDs) y añadir el bloque de anuncio a 2–3 páginas de
  contenido de alto tráfico.
- Definir 1 plantilla visual por pilar y arrancar con 4 publicaciones/semana.
- Montar la tarea programada que arma el guion diario de alertas desde la API.
- Reemplazar `contacto@estadoemplea.cl` por un correo real (lo usan el directorio
  de cursos y el panel B2B).
