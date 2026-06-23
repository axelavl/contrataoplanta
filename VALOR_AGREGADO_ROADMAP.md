# Roadmap de valor agregado y monetización

Documento de trabajo. Base: el sitio ya es un agregador maduro de empleo público
(búsqueda, alertas por email, favoritos, fichas estructuradas, estadísticas, panel
admin). El objetivo es construir capas de valor que (a) diferencien del portal
oficial gratuito del Servicio Civil y (b) generen ingresos.

## Realidad de negocio (leer antes de decidir)

El portal oficial `empleospublicos.cl` cubre solo la administración central y es
gratuito. La ventaja de este sitio es **cobertura** (municipios, FF.AA., judicial,
empresas del Estado, ~640 instituciones) y **experiencia de uso**.

Restricción clave: los empleadores son públicos y publican gratis por ley, así
que el modelo clásico de "empleador paga por aviso destacado" casi no aplica. El
dinero realista viene de tres lados:

1. **Lado candidato** (suscripción / herramientas premium / contenido).
2. **Datos** (analítica vendida a terceros).
3. **Afiliación y publicidad** (cursos, diplomados, display) — ya hay dos slots
   de publicidad vacíos en el front.

Diversificar es obligatorio: depender de una sola fuente es el error más común
de los job boards. Apunta a 3 fuentes que se refuercen entre sí.

---

## Vertical 1 — Inteligencia de renta (EUS) · ENTREGADO (v1)

Estado: construido en este pase.

- `web/escala-eus-data.js` — escala EUS real por estamento y grado (fuente:
  Transparencia Activa, Subsecretaría del Interior), reajuste 2026 (Ley 21.806),
  estimación de líquido.
- `web/calculadora-renta.html` + `web/calculadora-renta.js` — calculadora
  pública con tabla completa, enlazada en el menú.

Por qué primero: es el **moat de datos** más defensible (nadie traduce bien
"grado → renta"), es imán de SEO ("cuánto paga el grado 12", "sueldo grado EUS"),
y alimenta el panel B2B (vertical 3).

### Siguientes pasos para monetizar este vertical
- **Integrar en la ficha de oferta:** cuando una oferta trae grado pero no renta,
  mostrar la estimación con un enlace "ver cómo se calcula". Aumenta tiempo en
  sitio y valor percibido.
- **SEO programático:** generar páginas estáticas por grado/estamento
  (`/renta/profesional-grado-12`) con la calculadora pre-cargada. Cada una captura
  búsquedas de cola larga.
- **Premium (gancho ya visible en la página):** renta real por institución,
  simulador con Isapre/AFP exactas, proyección de carrera, alertas por grado.
- **Esfuerzo:** integración en ficha ~1–2 días; SEO programático ~2–3 días.

---

## Vertical 2 — Centro de preparación de concursos

Concepto: convertir el tráfico de buscadores de empleo en una audiencia que
consume contenido de preparación, y monetizar por **afiliación/lead-gen** (cursos
y diplomados de administración pública) + **contenido premium**, sin cobrarle
directamente al postulante en la v1.

### Qué construir
1. **Guías estructuradas** (ya existen borradores: `guia-*.html`). Consolidar en
   un hub `/preparacion` con ruta clara: CV para sector público → postulación →
   prueba psicolaboral → entrevista por competencias → seguimiento.
2. **Plantillas descargables:** CV formato sector público, carta de presentación,
   checklist de documentos por tipo de concurso. Descarga a cambio de email
   (crece la lista de alertas, que es el activo más valioso).
3. **Banco de preguntas / simulacro** de prueba psicolaboral y de conocimientos
   generales del Estado (probidad, Ley 19.880, Estatuto Administrativo). Versión
   gratis limitada + versión premium completa.
4. **Directorio de cursos afiliados:** diplomados y cursos de preparación, con
   enlaces de afiliado o lead-gen pagado por academia.

### Monetización
- Afiliación/lead-gen de cursos: comisión por clic o por matrícula.
- Contenido premium (simulacros completos, plantillas pro): suscripción baja
  (CLP 3.000–5.000/mes) o pago único por pack.
- Patrocinio de academias en el hub de preparación.

### Requisitos técnicos
- Contenido (el grueso del trabajo es editorial, no código).
- Para descargas-por-email: reutilizar el endpoint de alertas existente
  (`POST /api/alertas`) marcando origen.
- Para premium: requiere el sistema de cuentas del vertical 2 de la lista
  original (login). Sin cuentas, limitar a pago único / contenido abierto +
  afiliación.
- **Esfuerzo:** hub + plantillas ~3–5 días; banco de preguntas ~1–2 semanas
  (depende del volumen de contenido).

### Riesgo
- Es intensivo en contenido y mantención. El diferencial está en la calidad y
  actualización (cambios legales). Empezar acotado (CV + 1 simulacro) y medir
  conversión antes de escalar.

---

## Vertical 3 — Panel de datos B2B

Concepto: ya capturas un dataset único (qué instituciones contratan, qué cargos,
qué rentas, en qué regiones, con qué estacionalidad). Eso tiene valor para
terceros que hoy no tienen una vista consolidada del mercado laboral público.

### Clientes potenciales
- Consultoras de RR.HH. y reclutamiento del sector público.
- Universidades e institutos (orientación vocacional, estudios laborales).
- Sindicatos / asociaciones de funcionarios (ANEF y similares).
- Las propias instituciones (benchmarking de rentas y competencia por talento).
- Prensa y centros de estudio (datos para reportajes/estudios).

### Qué construir
1. **Dashboard interno → externo:** ya existe `estadisticas.html` + endpoints
   `/api/estadisticas`, `/api/instituciones/{id}/estadisticas`. Extender con
   series temporales por sector/región/estamento y por rangos de renta (usando
   `escala-eus-data.js` para normalizar grados a renta).
2. **Reportes exportables** (PDF/Excel) mensuales: "Estado del empleo público
   este mes" — descarga gratis (genera leads) + versión detallada de pago.
3. **API de datos** (de pago) o entregas de dataset bajo suscripción.

### Monetización
- Suscripción B2B mensual/anual al dashboard avanzado + reportes.
- Venta de reportes personalizados / estudios a pedido.
- Acceso a API/dataset.

### Requisitos técnicos
- Buena parte de la infraestructura existe (endpoints + Meilisearch + DB). Falta:
  agregaciones históricas, control de acceso por cliente, y exportación.
- **Esfuerzo:** MVP de dashboard ampliado ~1–2 semanas; reportes exportables
  ~3–5 días; control de acceso/billing ~1–2 semanas.

### Riesgo
- Ciclo de venta B2B lento. Validar con 1–2 clientes ancla (ej. un sindicato o
  una universidad) antes de invertir en billing y self-service.

---

## Secuencia recomendada

1. **Cerrar vertical 1**: integrar la renta en la ficha + SEO programático. Bajo
   esfuerzo, sube tráfico y valor percibido de inmediato.
2. **Activar la publicidad existente** (los dos slots vacíos) y la afiliación de
   cursos del vertical 2 — ingreso temprano con poco desarrollo.
3. **Vertical 2 (preparación)**: hub + plantillas + 1 simulacro. Crece la lista
   de correos (activo central) y abre afiliación.
4. **Cuentas de usuario** (de la lista original): habilita premium real y
   sincronización. Es el desbloqueador de la suscripción del candidato.
5. **Vertical 3 (B2B)** una vez que el dataset histórico esté maduro y haya
   1–2 clientes ancla validados.

## Métrica que importa
La **lista de correos** (alertas) es el activo más monetizable que ya tienes.
Cada herramienta nueva debería, además de su función, hacer crecer esa lista.

---

# Estado de avance (actualizado)

## Vertical 1 — Inteligencia de renta (EUS) · ENTREGADO
`web/calculadora-renta.html` + `escala-eus-data.js` + `calculadora-renta.js`.
Calculadora pública grado→renta con datos reales de la escala EUS, reajuste 2026
y estimación de líquido. Enlazada en el menú.

## Vertical 2 — Centro de preparación de concursos · ENTREGADO (MVP)
`web/preparacion.html` (hub con ruta de 5 etapas), `preparacion.js` (simulacro de
10 preguntas verificadas contra normativa), `plantilla-cv-sector-publico.html`
(CV editable/imprimible). Pendiente: ampliar banco a 40–50 preguntas, captura de
email real en descargas.

## Vertical 3 — Panel de datos B2B · ENTREGADO (MVP)
`web/panel-mercado.html` + `panel-mercado.js`, consumiendo la API en vivo.
Backend: nuevo endpoint `GET /api/mercado/agregados` (`api/routers/public.py`) que
agrega por región, área (normalizada), tipo de vínculo y cobertura de renta sobre
el universo completo. El panel usa ese endpoint con fallback a agregación por
muestra en el navegador. Planes B2B (reporte, estudio a medida, API) presentados.
Pendiente: control de acceso/billing para clientes B2B; exportación PDF/Excel.

## Monetización por empresas de cursos · ENTREGADO (estructura)
`web/cursos.html` + `cursos-data.js` + `cursos.js`. Directorio de cursos y
especializaciones con niveles de aviso (destacado/estándar/por desempeño),
precios referenciales, deep-link por área (`?area=`/`?cat=`) desde el panel, y
sección “Anúnciate”. Pendiente: cargar avisadores reales y precios definitivos;
reemplazar el correo de contacto.

## Publicidad (AdSense) + RRSS · ENTREGADO (infraestructura + estrategia)
`web/ads-config.js` + `ads.js` (desactivado hasta tener ID), slots en el index,
CSP actualizado en `web/_headers`. Estrategia completa en
`MONETIZACION_ADS_RRSS.md` (ads + plan TikTok/Instagram + embudo). Pendiente:
crear cuenta AdSense y poner IDs; arrancar publicación en RRSS; tarea programada
que arme guiones desde la API.

## Modelo de ingresos (capas que se refuerzan)
1. AdSense — piso pasivo, escala con tráfico.
2. Alertas por correo — retención y audiencia.
3. Directorio de cursos — avisadores pagan por audiencia segmentada.
4. Panel B2B — venta de datos por cliente (mayor margen).
RRSS alimenta todo trayendo tráfico recurrente. No depender de una sola capa.

## Próximos pasos priorizados
1. Reemplazar `contacto@estadoemplea.cl` por correo real (cursos + B2B).
2. Desplegar el backend para que `/api/mercado/agregados` quede activo.
3. Activar AdSense y sumar 1 bloque de anuncio a 2–3 páginas de contenido.
4. Integrar la calculadora de renta dentro de la ficha de oferta + SEO por grado.
5. Ampliar el simulacro y montar la captura de email en descargas.
