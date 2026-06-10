# EXTRACTION_STRATEGY

## Pipeline objetivo implementado
1. Discover/fetch página candidata (scraper).
2. Construcción de `RawPage`.
3. Clasificación por reglas (`RuleEngine`).
4. Fallback LLM solo en ambiguos (`llm_fallback_classifier`).
5. Rechazo temprano de contenido no laboral.
6. Extracción especializada por campos (`field_extractors`).
7. Parseo jerárquico de adjuntos (`attachment_parser`).
8. Resolución de fechas/vigencia (`date_extractor`, `expiry_validator`).
9. Validación mínima (`job_validator`).
10. Scoring de calidad (`quality_scoring`).
11. Normalización unificada (`job_normalizer`).

## Extracción jerárquica
Prioridad:
- HTML principal.
- Tablas (si hay señales de estructura laboral).
- PDF adjuntos relevantes por nombre.
- OCR solo último recurso y únicamente para adjuntos relevantes sin texto.

## Cobertura de campos
- Título de cargo: headings + patrones explícitos.
- Funciones: secciones de funciones/objetivo/responsabilidades.
- Requisitos: obligatorios y deseables por bloques.
- Documentos requeridos: CV/certificados/títulos/anexos/declaración/cédula.
- Sueldo: monto + moneda + texto original.
- Contrato/jornada/modalidad: diccionario de patrones normalizados.
- Fechas: publicación/inicio/cierre desde texto, tablas y adjuntos.

## Calidad y revisión manual
`overall_quality_score` combina:
- confianza de clasificación,
- completitud de campos,
- confianza de fechas,
- confianza de sueldo,
- confianza de requisitos.

Si score bajo o contradicción fuerte -> `needs_review=true`.
