# Auditoría de calidad de ofertas (estadoemplea.pages.dev) — 2026-06-10

Análisis sobre la API en vivo (`/api/ofertas`, `/api/estadisticas`). Muestra
inspeccionada: 49 ofertas únicas (la API capa `por_pagina`, así que los
porcentajes son indicativos; los problemas estructurales son independientes del
tamaño de muestra). 40 de 49 vienen de `empleospublicos.cl`, donde se concentran
los problemas.

## Hallazgo 1 — Misatribución de institución (CRÍTICO)

Ofertas claramente hospitalarias aparecen bajo **"Superintendencia de Pensiones"**
(id 85), un regulador financiero que no contrata personal clínico. Ejemplos
reales (todas plataforma=empleospublicos.cl):

- `8887` Enfermero/a Clínico para Unidad de Anestesia y Pabellones → Pensiones
- `8815` Psicólogo/a para Servicio de Pediatría → Pensiones
- `8755` Técnico Enfermería UTI 2 SUR, Subdirección Gestión del Cuidado → Pensiones
- `8734` Técnico de Equipos Médicos → Pensiones
- `13796` Médico Cirujano Especialista → Pensiones (oferta reciente: el bug sigue activo)
- `13776` Enfermera/o Clínica/o 4° Turno → Pensiones

Las descripciones mencionan "establecidos en el Hospital", "pacientes
hospitalizados", anestesia, pediatría; las ciudades (Cerro Navia, Independencia,
Providencia) son de hospitales, no de la Superintendencia. En `/api/estadisticas`,
"Superintendencia de Pensiones" figura con **89 activas / 89 nuevas en la semana**:
es un bucket que está absorbiendo ofertas de varios hospitales distintos.

**Causa raíz.** `scrapers/empleos_publicos.py` → `match_institucion_id()` (línea
153) mapea el nombre de la institución por **substring suelto**: devuelve la
primera institución del catálogo cuyo nombre normalizado esté contenido en el
texto de la oferta. Eso (a) colapsa instituciones distintas en una sola, y (b)
cuando el hospital real no está en el catálogo de ~640, en vez de quedar sin id
termina cayendo en un match espurio. El portal empleospublicos.cl lista cientos
de empleadores; si la institución por-oferta no se extrae con precisión, se
bucketiza mal.

**Recomendaciones (scraper):**
1. Reemplazar el match por substring por uno **estricto**: igualdad sobre nombre
   normalizado, o solapamiento de tokens con umbral (p. ej. ≥0.8 Jaccard),
   exigiendo además coincidencia de sigla/dominio cuando exista.
2. Si no hay match confiable, **NO** asignar un id por defecto: dejar
   `institucion_id = NULL` y conservar el `institucion_nombre` extraído del
   detalle (`#lblAvisoTrabajoDatos`, campo "Institución / Entidad"). Es preferible
   "sin institución del catálogo" antes que "institución equivocada".
3. Verificar que el extractor de institución del detalle (`_extraer_metadata_detalle`)
   esté tomando el campo correcto y no un default de la plantilla.

## Hallazgo 2 — `url_bases` inútil en empleospublicos (ALTO)

48 de 49 ofertas tienen `url_bases` terminando en
`…convpostularavisoTrabajo.aspx#contenido`: es el ancla de la propia página de la
oferta, **no** las bases reales del concurso. El frontend habilita el botón
"Ver bases" con este campo, llevando a un enlace que no aporta las bases.

**Recomendación:** en el parser de detalle, si no se encuentra un enlace real a
bases (PDF/documento), dejar `url_bases = NULL` en vez de guardar el ancla
`#contenido`. Así el frontend no muestra un "Ver bases" muerto. (La lógica de
`_extraer_url_bases` ya prioriza hints/PDF; el fallback debe ser NULL, no la URL
de la oferta.)

## Hallazgo 3 — El validador de URLs no se está ejecutando (ALTO)

**49/49** ofertas tienen `url_oferta_valida = null`, `url_bases_valida = null`,
`url_valida_chequeada_en = null`. `validate_offer_urls.py` no corrió sobre estas
ofertas. Según el propio CLAUDE.md, `scrapers/run_all.py` **no** lo invoca (solo
lo hacía el viejo `run_scrapers.py`). Resultado: los flags que el frontend usa
para gatear "Ver bases"/"Postular" están sin poblar para todo el inventario nuevo.

**Recomendación:** cablear `validate_offer_urls.py` al final de `run_all.py`
(después de persistir, antes de cerrar el pool), o agregarlo como paso del
servicio/timer de scrapers. Hoy es un paso manual que no está corriendo.

## Hallazgo 4 — Campos incompletos en empleospublicos (MEDIO)

Sobre la muestra: `jornada` vacío en **73%**, `grado_eus` vacío en **65%**,
`tipo_contrato` vacío en ~8%. Estos datos suelen estar en la ficha de detalle de
empleospublicos.cl, así que es extracción incompleta, no ausencia en la fuente.

**Recomendación:** revisar `_extraer_jornada`, `parse_renta`/grado y
`_extraer_metadata_detalle` contra el HTML real de `avisopizarronficha.aspx`; los
selectores/regex están perdiendo la mayoría de los casos.

## Hallazgo 5 — Sin vencidas activas (POSITIVO)

No se encontraron ofertas con `fecha_cierre` pasada y `estado=active` en la
muestra; el validador de expiración (`validation/expiry_validator.py`) parece
estar cerrando correctamente las vencidas. Conviene confirmarlo sobre el total,
pero la señal es buena.

## Mejoras sugeridas al VALIDADOR de ofertas

Independiente de arreglar la extracción, el validador debería atrapar estos casos
antes de publicar:

1. **Coherencia institución ↔ contenido:** si el cargo/descripción es claramente
   sanitario (enfermer, paciente, hospital, UTI, anestesia, pediatría, médic…) y
   la institución asignada no es del sector Salud, marcar `needs_review=true`.
   Generalizable a otros sectores con un mapa palabra-clave → sector esperado.
2. **Bucket anómalo:** alertar cuando una institución acumula un salto súbito de
   ofertas nuevas muy por encima de su histórico (Pensiones: 0 → 89 en una
   semana). Es señal de misatribución o scraping defectuoso.
3. **URL de bases degenerada:** rechazar/anular `url_bases` que sea igual a
   `url_oferta` o termine en `#contenido`.
4. **Gate por validación de URL:** no marcar una oferta como "postulable" si
   `url_oferta_valida` es null o false.

## Limitaciones

Muestra de 49 ofertas (la API limita `por_pagina` y los fetches grandes fallaron).
Los hallazgos 1–4 son estructurales y visibles en cualquier muestra; los
porcentajes exactos requieren correr el chequeo sobre el total en la BD.
