# Revisión de ofertas sin fecha de cierre

Fecha del análisis: 2026-06-19. Fuente de datos: API de producción (`/api/ofertas`, 1.149 ofertas activas). Inventario completo en `ofertas_sin_fecha_cierre.csv` (527 filas).

## Resumen

527 ofertas activas (45,9% del total) tienen `fecha_cierre = NULL`. La API trata `fecha_cierre IS NULL` como `active` indefinidamente (`api/services/sql.py:35`) y el front les pone badge verde "Disponible", así que ninguna caduca salvo que el scraper deje de verlas. El problema está muy concentrado: una sola fuente (Fundación Integra vía Aira) es el 28%, y las 5 primeras instituciones suman el 51%.

La conclusión central de la verificación contra los sitios de origen: **la mayoría de las ofertas sin fecha SÍ tienen fecha en el sitio de origen; el scraper la descarta.** No es que las fuentes no publiquen plazo — es que varios scrapers no lo extraen (en dos casos lo ponen en `None` por código).

## Distribución (527 ofertas sin fecha)

Por plataforma: WordPress 236 · Sitio propio 222 · Laborum 23 · Sitio propio+EP 16 · Buk 7 · Joomla 7 · Trabajando 3 · resto menor.

Por sector: Municipal 268 · Ejecutivo Central 151 · FF.AA. y Orden 44 · Empresa del Estado 17 · Universidad 16 · Autónomo/Regulador 15 · Judicial 9 · GORE 7.

Top instituciones: Fundación Integra 149 · Mun. Pucón 50 · Mun. Independencia 27 · Hospital Militar de Santiago 23 · Mun. Cobquecura 21 · Armada 10 · Tribunal Constitucional 9 · Mun. Los Álamos 9 · Corp. Mun. Ancud 8 · CODELCO 7 · Mun. Alto Biobío 7 · Mun. Cerro Navia 7 · ENAER 6 · Defensoría Penal 6.

Validez de URL entre las sin-fecha: 211 vivas (`true`), 209 sin chequear (`null`), **107 muertas (`false`)**.

Antigüedad de `fecha_scraped`: 0 con más de 90 días, 320 entre 30-90 días, 207 con menos de 30 días.

## Verificación contra sitios de origen (muestra)

Verifiqué a mano 5 orígenes representativos que cubren los buckets dominantes (vía navegador; `web_fetch` y el sandbox estuvieron caídos durante la sesión). Veredicto por caso:

**Fundación Integra / Aira (149 ofertas — 28%) — LA FECHA EXISTE, el scraper la tira.**
La página de Aira es pública (redirige a `offer_info/<token>`, no requiere login para leer) y trae el plazo en el texto. Ejemplo verificado (id 25403): *"RECEPCIÓN DE POSTULACIONES HASTA EL 26 DE JUNIO 2026"* y *"Periodo: al 28.08.26"*. Sin embargo `scrapers/aira_integra.py` asigna `"fecha_cierre": None` por código (líneas ~268 y ~328). Arreglar este único scraper resolvería ~28% de todas las ofertas sin fecha.

**CODELCO (7 ofertas) — LA FECHA EXISTE, el scraper la pierde.**
Ejemplo verificado (id 25393, empleos.codelco.cl / SuccessFactors): el detalle dice textualmente *"Cierre de Postulaciones: Domingo 28 de Junio 2026"* y *"Hora de cierre: 23:59 hrs"*. `scrapers/codelco.py` (líneas ~341-345) tiene un parseo frágil y además descarta la fecha si ya pasó (`cierre >= date.today()`), dejándola en NULL.

**Municipal WordPress — Mun. Pucón (50 ofertas) — BASURA HISTÓRICA + sin fecha por ítem.**
La URL de origen (`municipalidadpucon.cl/?page_id=89`) es UNA sola página-listado que mezcla 8 concursos "En Curso" con 70+ "Concursos Públicos Anteriores" (hasta 2015-2016). El scraper ingirió todo el archivo histórico como ofertas activas con anchors `#hash`. De las 50, solo ~8 están vigentes; el resto es histórico y debe limpiarse. Además el listado no trae fecha por ítem (estaría dentro de cada PDF/decreto). Este patrón probablemente se repite en los demás municipios WordPress (Independencia, Cobquecura, Los Álamos, etc.).

**Tribunal Constitucional (9 ofertas) — fecha solo en el PDF de bases.**
Verificado (id 25058, "Relator"): la página HTML no muestra ninguna fecha de cierre, solo un enlace "Bases del concurso" (PDF). El plazo, si existe, vive en ese PDF. El scraper no lo parsea. Nota: algunas ofertas más nuevas del mismo sitio sí salieron con fecha, así que es inconsistente.

**Hospital Militar / Laborum (23 ofertas) — sin fecha en origen + campo `cargo` sucio.**
Laborum muestra "Publicado hace X horas" pero no un plazo de cierre, así que aquí la ausencia es real. `scrapers/laborum.py:312` además fija `fecha_cierre=None` por diseño. Bug aparte: el `cargo` viene contaminado con texto de badges, p.ej. *"NuevoPublicado hace 5 horasANALISTA SOPORTA INFORMÁTICO-DIURNO"*.

## Clasificación causa-raíz → acción

1. **El scraper descarta una fecha que sí existe (alto impacto, arreglar extracción):** Aira/Integra (149) y CODELCO (7). ~156 ofertas recuperables arreglando 2 scrapers.
2. **Archivo histórico ingerido como activo (limpiar + filtrar):** municipios WordPress (Pucón 50, y probablemente Independencia 27, Cobquecura 21, Los Álamos 9, Ancud 8, etc.). Cientos de filas, en su mayoría concursos cerrados de años anteriores.
3. **Fecha solo en PDF de bases (mejorar o aceptar sin fecha):** Tribunal Constitucional (9), y parte de los municipales con `url_bases` a PDF.
4. **Genuinamente sin fecha en origen (política de caducidad):** Laborum/Buk/algunos portales ATS que no publican plazo. Aquí la única defensa es caducar por antigüedad.

## Limpieza recomendada (acciones concretas)

- **Cerrar las 107 ofertas con `url_oferta_valida = false` y sin fecha**: URL muerta + sin plazo = no verificable. Candidatas inmediatas (vía `activa = FALSE`).
- **Depurar el archivo histórico municipal**: cerrar las ofertas WordPress cuyo `cargo` referencia años pasados (2016-2024) o que caen en la sección "Anteriores". Empezar por Pucón.
- **Regla de caducidad por antigüedad para `fecha_cierre IS NULL`**: hoy `limpiar_vencidas` (`scrapers/base.py:1674`) excluye explícitamente las NULL, así que nunca expiran. Añadir un cierre por `fecha_scraped`/`fecha_publicacion` > N días (p.ej. 60-90) para las fuentes que no son "cierre por ausencia".

## Mejoras de scraper y filtros (mapeadas al código)

- **`scrapers/aira_integra.py`** (máxima prioridad): dejar de hardcodear `None`; extraer del texto el patrón "RECEPCIÓN DE POSTULACIONES HASTA EL DD DE MES DE AAAA" y "Periodo: al DD.MM.AA". Resuelve el 28% del problema.
- **`scrapers/codelco.py:341-345`**: corregir el parseo de "Cierre de Postulaciones: <fecha>" y NO descartar la fecha por estar en el pasado — persistirla para que `limpiar_vencidas` cierre la oferta formalmente.
- **WordPress municipal (`scrapers/plataformas/wordpress.py` / `generic_site.py`)**: respetar la separación "En Curso" vs "Anteriores" del listado (no ingerir el archivo histórico), o filtrar por una señal de frescura. Hoy se crean ofertas con anchor `#hash` sin fecha de publicación.
- **`scrapers/laborum.py`**: limpiar el `cargo` (quitar prefijos "Nuevo", "Publicado hace X horas").
- **Tribunal Constitucional / municipales con PDF**: pasar el PDF de bases por extracción de fecha (ya existe `fecha_cierre_desde_texto` en `scrapers/intake.py:416`, hoy usado solo por el intake legacy) como fallback antes de persistir.
- **Validación**: para fuentes que no son cierre-por-ausencia, marcar `needs_review=True` de forma consistente cuando no hay fecha ni señal de vigencia (hoy solo `quality_validator.py:152` lo hace; `intake.py:591` y `base.py:451` aceptan sin marcar).
- **Filtro/API**: evaluar un estado separado (p.ej. `active_no_deadline`) o condición de antigüedad en `ACTIVE_OFFER_SQL` para no mostrar como "Disponible" ofertas NULL antiguas.

## Limitaciones de esta pasada

`web_fetch` y el sandbox Linux estuvieron intermitentes/caídos, así que la verificación origen-por-origen se hizo sobre una muestra de 5 fuentes (que cubre ~190 ofertas / 36% de forma directa, y el resto por generalización de plataforma). El inventario completo (527) sí está en el CSV. Puedo profundizar fuente por fuente en lotes cuando quieras, o implementar las correcciones de scraper priorizadas.
