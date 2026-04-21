# Revisión de scrapers existentes

| scraper_name | source | problem_detected | severity | recommended_fix |
|---|---|---|---|---|
| empleos_publicos.py | EmpleosPublicos.cl | Alto volumen; riesgo de aceptar páginas de difusión o fichas incompletas si falla enriquecimiento de detalle. | Media | Enrutar cada detalle por `JobExtractionPipeline` y aplicar rechazo por completitud mínima antes de persistir. |
| plataformas/generic_site.py | Sitios institucionales heterogéneos | Heurística amplia por keywords; falsos positivos en noticias/eventos/listados índice. | Alta | Ya refactorizado para delegar a clasificación + extracción común con trazabilidad y reglas de rechazo. |
| _base_wordpress.py + muni_* | Portales WordPress municipales | Detección por texto suelto con palabras genéricas; puede confundir posts/noticias con concursos. | Alta | Crear adaptador `RawPage` en base WordPress y pasar por RuleEngine + expiry validator. |
| plataformas/wordpress.py | WordPress genérico | Ambigüedad entre entradas tipo blog y vacantes, especialmente en categorías mixtas. | Alta | Penalizar rutas `/noticias|blog` y exigir señales esenciales de vacante. |
| plataformas/ffaa.py | Sitios FF.AA. | Históricos mezclados con vigentes; metadatos de fecha no siempre explícitos. | Alta | Resolver fechas desde adjuntos y descartar años antiguos sin señales actuales. |
| plataformas/policia.py | Policías/seguridad | Publicaciones institucionales + resultados de concurso en el mismo árbol. | Alta | Reglas negativas fuertes para `resultados/nómina/adjudicación`. |
| plataformas/trabajando_cl.py | Trabajando.cl | Menor riesgo de contenido no laboral, pero puede arrastrar duplicados y ofertas vencidas. | Media | Activar dedupe por ID externo + vigencia por fecha de cierre. |
| plataformas/buk.py | Buk careers | Similar a job-board; riesgo principal es expiración y baja completitud de campos. | Media | Completar extracción desde detalle y score de calidad antes de guardar. |
| plataformas/hiringroom.py | HiringRoom | Job board estructurado, pero con datos parciales en listado. | Baja | Priorizar detalle y marcar `needs_review` cuando falten requisitos/fechas. |
| banco_central.py / codelco.py / tvn.py / poder_judicial.py / gobiernos_regionales.py | Fuentes específicas | Lógica ad-hoc repetida por scraper; difícil auditoría y mantenimiento. | Media | Extraer inteligencia común (clasificación, fechas, scoring, normalización), dejar scraper solo para discovery/fetch. |

## Hallazgo transversal
La principal brecha histórica es **acoplar detección + extracción dentro de cada scraper**. Se corrige moviendo la inteligencia a módulos compartidos y auditables.

## Inventario ejecutable único
El inventario productivo/legacy y el plan de retiro/migración quedaron consolidados en `docs/SCRAPERS_RUNTIME_INVENTORY.md` y en la configuración ejecutable `scrapers/runtime_inventory.py`.
