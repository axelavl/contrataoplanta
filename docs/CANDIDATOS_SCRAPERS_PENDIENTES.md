# Candidatos a scraper dedicado — instituciones con ofertas activas sin cobertura propia

_Generado: 2026-06-25. Fuente: `repositorio_instituciones_publicas_chile.json` (709 instituciones) + `scrapers/source_status.py` + `scrapers/source_overrides.json`. Verificación en vivo de una muestra representativa por tier (junio 2026)._

## Resumen

De las 709 instituciones del catálogo, 587 quedan cubiertas por el portal central `empleospublicos.cl` (no necesitan scraper propio). 122 publican en portal propio. De esas, **54 tienen ofertas activas/permanentes y NO tienen scraper dedicado ni override** en `source_overrides.json` — son la brecha real.

Se agrupan en cuatro tiers por relación valor/dificultad:

| Tier | Qué son | N° | Esfuerzo | Prioridad |
|---|---|---|---|---|
| 1 | Instituciones nacionales (Congreso, Judicial, autónomos) — HTML server-rendered | 10 | Bajo–Medio | **Alta** |
| 2 | Grandes municipios en SPA/ATS JavaScript (requieren Playwright) | 7 | Medio–Alto | **Alta/Media** |
| 3 | Corporaciones municipales WordPress/Trabajando/OMIL (ya tocadas por scrapers genéricos) | 35 | Bajo | Baja |
| 4 | Casos js_required sueltos | 2 | Medio | Baja |

Detalle fila a fila en `candidatos_scrapers_pendientes.csv` (raíz del repo o adjunto).

---

## Tier 1 — Nacionales HTML, ganancia rápida (10)

Hoy están clasificadas `manual_review` o `js_required`, así que el orquestador NO las corre. Son instituciones de alto valor (poderes del Estado, autónomos) y la mayoría publica en HTML plano server-rendered, por lo que un scraper genérico con selectores por sitio las captura sin navegador.

| ID | Institución | Estado verificado en vivo | Plataforma / nota técnica |
|---|---|---|---|
| 135 | Senado de la República | ✅ **3 concursos vigentes** + terminados, HTML plano | `senado.cl/transparencia/.../concursos`. Lista directa, fácil. |
| 138 | Poder Judicial | ✅ **Alto volumen vigente** (jueces, notarios, conservadores, JPL) | `pjud.cl/.../trabaje-con-nosotros` — comunicados + edictos PDF + `postulaciones.pjud.cl`. El de mayor volumen recurrente del tier. |
| 136 | Cámara de Diputados | ⚠️ **URL del catálogo rota (404)** | La correcta es `camara.cl/transparencia/transparencia_activa.aspx`. ASP.NET. Corregir `url_empleo`. |
| 137 | Biblioteca del Congreso (BCN) | ⚠️ "Abiertos" se cargan vía JS | `bcn.cl/concurso_publico` lista histórico en HTML pero los vigentes vienen de `postulaciones.bcn.cl`. Baja frecuencia. |
| 128 | Consejo Nacional de Televisión | ⚠️ Respuesta vacía (probable JS/WAF) | `cntv.cl/trabaja-con-nosotros`. Revisar con navegador. |
| 140 | Tribunal Defensa Libre Competencia | Por confirmar (página pesada) | `tdlc.cl/concursos_publicos/`. HTML. |
| 141 | 1er Tribunal Ambiental (Antofagasta) | Baja frecuencia | `1ta.cl`. HTML, dificultad baja. |
| 142 | 2do Tribunal Ambiental (Santiago) | Baja frecuencia | `tribunalambiental.cl/trabaje-con-nosotros`. |
| 143 | 3er Tribunal Ambiental (Valdivia) | Baja frecuencia | `3ta.cl`. |
| 144 | Tribunal de Contratación Pública | Baja frecuencia | `tcp.gob.cl/trabaje-con-nosotros`. |

**Recomendación Tier 1.** Promover de `manual_review` a `active` con `kind=generic` y un selector específico por sitio (o un perfil dedicado para Senado y Poder Judicial, que son los de más valor). Empezar por **Senado (135)** y **Poder Judicial (138)**: confirmados con ofertas vigentes y son los de mayor volumen. Corregir la URL de Cámara de Diputados (136) en el catálogo antes de intentar scrapearla.

---

## Tier 2 — Grandes municipios en SPA/ATS JavaScript (7)

Comunas de alta población con concursos recurrentes, pero el portal de postulación corre sobre una SPA JavaScript o un ATS de terceros. El scraper HTML genérico no los ve; requieren `playwright_scraper.py` o pegarle a la API JSON de cada plataforma.

| ID | Institución | Plataforma | Notas |
|---|---|---|---|
| 380 | Municipalidad de Santiago | HiringRoom + portal propio + Trabajando | `munistgo.cl/portal-laboral`. Multiplataforma; alto volumen. |
| 398 | Municipalidad de Maipú | SPA JavaScript | `municipalidadmaipu.cl/concursos-publicos`. Dificultad "Muy Alta". |
| 399 | Municipalidad de Ñuñoa | RecruitK (SPA) | Concursos reales viven en `recruitk.com/mnunoa` (la página WP `nunoa.cl/concursos` tiene un post obsoleto de 2022). |
| 632 | Municipalidad de Punta Arenas | Vue.js SPA | `puntaarenas.cl/municipio/ofertas` + APS `apscormupa.cl`. |
| 648 | Corp. Municipal Viña del Mar | Reqlut | `cmvm.cl`. Corporación mixta (educación/salud/deportes), alto volumen potencial. |
| 649 | Corp. Municipal Quilpué | Procit | `cmq.procit.cl/empleos/`. |
| 392 | Municipalidad de La Reina | HiringRoom Campus | ⚠️ Verificado: el portal `mlareina.hiringroomcampus.com` es una **bolsa OMIL de sector privado** con ofertas viejas (2022-23), no concursos municipales. Bajo valor. |

**ROI de plataforma.** Cada plataforma SPA aparece pocas veces en el catálogo: HiringRoom ×4, Reqlut ×1, Procit ×1, RecruitK ×1, Vue ×1. No conviene un scraper por plataforma salvo HiringRoom. Lo eficiente es **un scraper Playwright genérico** que renderice la SPA y extraiga el listado, reutilizable para Maipú, Punta Arenas, Viña, Quilpué y Ñuñoa. Para HiringRoom (4 instituciones) vale un adaptador dedicado que consuma su endpoint `/jobs` — ya existe `scrapers/plataformas/hiringroom.py` que conviene cablear vía override.

**Recomendación Tier 2.** Priorizar **Santiago (380)**, **Maipú (398)** y **Viña del Mar (648)** por población/volumen. La Reina (392) bajarla de prioridad (es OMIL privado).

---

## Tier 3 — Corporaciones municipales WordPress/Trabajando/OMIL (35)

Mayoritariamente corporaciones de salud/educación de comunas chicas (cluster grande en Chiloé) sobre WordPress, más unas OMIL en Trabajando.cl. **Ya las tocan los scrapers genéricos `wordpress.py` y `trabajando.py`** porque están clasificadas `active` — no son un "gap de desarrollo" sino un gap de _rendimiento_: hay que verificar cuánto rinden realmente.

Dos consideraciones de scope:

- Las entradas **"Trabajando.cl/OMIL"** (348 Calle Larga, 370 San Felipe, 581 Corral, 669 Providencia) y las HiringRoom-OMIL (335 Combarbalá) son **bolsas de intermediación laboral de sector privado**, no concursos públicos. Si el producto es exclusivamente empleo público, conviene marcarlas `disabled`/`skip` en vez de invertir en ellas.
- **476 Pelluhue**: sitio "en construcción" — reverificar en 1–3 meses.

**Recomendación Tier 3.** No desarrollar scrapers nuevos. Correr `run_all.py --include-experimental` sobre este cluster, medir ofertas reales capturadas, y decidir por dato: las WordPress que rinden se quedan, las OMIL de sector privado se deshabilitan por scope.

---

## Tier 4 — js_required sueltos (2)

| ID | Institución | Nota |
|---|---|---|
| 67 | JUNJI | Su sitio propio requiere JS, **pero ya publica en `empleospublicos.cl`** → cubierta por el batch central. No necesita scraper propio. |
| 622 | Municipalidad de Coyhaique | Portal de transparencia PHP que requiere JS. Volumen bajo. |

---

## Próximos pasos sugeridos (orden de impacto)

1. **Senado (135) + Poder Judicial (138)** — perfil/selector dedicado, HTML, ofertas vigentes confirmadas. Ganancia inmediata.
2. **Corregir `url_empleo` de Cámara de Diputados (136)** en el catálogo (la actual da 404) y agregar selector.
3. **Resto Tier 1** (Tribunales Ambientales, TDLC, TCP, BCN, CNTV) — promover a `active` con selectores; baja frecuencia pero alto valor institucional.
4. **Scraper Playwright genérico para SPA municipales** — cubrir Santiago, Maipú, Viña, Quilpué, Punta Arenas, Ñuñoa de una vez.
5. **Cablear `hiringroom.py`** vía override para las 4 instituciones HiringRoom con ofertas públicas reales.
6. **Decidir scope OMIL** (sector privado) y deshabilitar las que no correspondan.
7. **Medir rendimiento del cluster WordPress Tier 3** antes de invertir más.

> Nota de método: la verificación en vivo cubrió una muestra por tier (Senado, Poder Judicial, Cámara, BCN, Ñuñoa, La Reina, CNTV, TDLC). Los "por confirmar" del Tier 1 y el rendimiento real del Tier 3 requieren una pasada adicional con navegador/orquestador antes de comprometer desarrollo.

---

## Estado de implementación (2026-06-25)

- ✅ **Senado (135)** — `scrapers/senado.py` creado y validado en vivo. Parsea la sección "Concursos Vigentes", visita cada detalle y extrae la fecha de cierre. Hoy rinde 0 vigentes reales: las 3 entradas del listado tienen cierre pasado (mayo/enero 2026 y junio 2024) — el Senado no limpia su sección "Vigentes". Capturará los nuevos automáticamente.
- ✅ **Poder Judicial (138)** — `scrapers/poder_judicial.py` creado y validado en vivo. Parsea los "Comunicados" (un aviso por párrafo con "CARGO DE …" + edicto PDF), con filtro de recencia (default 35 días) porque el cierre vive en el PDF del edicto. Hoy rinde 2 concursos abiertos recientes (Receptor Judicial de Puente Alto; Juez PL de Alto Bío Bío).
- ✅ Ambos cableados en `scrapers/run_all.py` (imports, `_IDS_NUEVO_ESTANDAR`, bloques de despacho) y registrados en `source_overrides.json` (`active`/`skip`).
- ✅ **OMIL deshabilitadas** en `source_overrides.json` (`disabled`): 335 Combarbalá, 348 Calle Larga, 370 San Felipe, 392 La Reina, 581 Corral, 669 Providencia — bolsas de intermediación de sector privado, fuera de scope.

> Limitación de validación: el entorno de prueba (sandbox) montó el repo con un caché desincronizado, así que la prueba `dry-run` de cada scraper se hizo con scripts equivalentes contra los sitios en vivo y la lógica del módulo, no vía `run_all.py` completo. Conviene una corrida real `python scrapers/senado.py --dry-run -v` y `python scrapers/poder_judicial.py --dry-run -v` en el entorno del proyecto antes de desplegar.

---

## LinkedIn — perfil EGGP (egresados Esc. de Gobierno y Gestión Pública, U. de Chile)

Perfil consultado: `cl.linkedin.com/in/egresados-eggp-9810199b`. La EGGP difunde ofertas de sector público para sus egresados.

**Veredicto: no es viable un scraper automático; sí uno semimanual de bajo valor marginal.**

- El feed/actividad de un perfil de LinkedIn está tras authwall (HTTP 999 sin sesión). Descubrir los posts nuevos automáticamente exige iniciar sesión, lo que **viola los Términos de Servicio de LinkedIn**, arriesga el bloqueo de la cuenta y es frágil. El proyecto ya tomó esta postura: ver `scrapers/linkedin_penalolen.py`.
- Los **posts públicos individuales** sí renderizan con JSON-LD (`articleBody`, `datePublished`) y son parseables — exactamente el patrón de `linkedin_penalolen.py`. Pero el descubrimiento depende de una **lista de URLs mantenida a mano**.
- Mejor ángulo: la EGGP publica las mismas ofertas en la **Bolsa de Trabajo de la U. de Chile** (fuente estructurada y sin problema de ToS), y buena parte de lo que comparte son re-publicaciones de vacantes que ya entran por `empleospublicos.cl` y sitios institucionales que ya scrapeamos. El valor único incremental es probablemente bajo.

**Recomendación:** no construir un scraper dedicado al perfil. Si el contenido resulta único y valioso, dos caminos limpios: (a) generalizar `linkedin_penalolen.py` para aceptar una lista de URLs de este perfil (semimanual), o (b) evaluar la Bolsa de Trabajo de la U. de Chile como fuente estructurada. Antes de invertir, medir cuántas ofertas del perfil NO están ya en nuestra base.
