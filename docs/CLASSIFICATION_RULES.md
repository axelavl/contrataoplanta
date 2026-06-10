# CLASSIFICATION_RULES

## Enfoque
Clasificación determinística por reglas con trazabilidad completa (`rule_trace`, señales positivas/negativas, razones de rechazo).

## Señales positivas principales
- Keywords laborales: cargo, postulación, concurso público, funciones, requisitos, renta, contrata/planta, fecha de cierre, oferta laboral, vacantes, ADP/Alta Dirección Pública, términos de referencia.
- Estructura de URL laboral: `/trabaja-con-nosotros`, `/concursos`, `/postulacion`, `/ofertas-laborales`.
- Tablas con columnas típicas de vacante (`cargo/renta/cierre`).
- Adjuntos PDF relevantes (`bases`, `perfil`, `concurso`, `tdr`, `convocatoria`).

## Señales negativas principales
- Contexto de noticias/comunicados/prensa/blog (incluye "sala de prensa").
- Contexto de eventos/agenda institucional/galería.
- Resultados o cierre (`nómina`, `adjudicación`, `proceso finalizado`, `concurso cerrado`).
- Históricos/archivo/memoria anual.
- URL no laboral (`/noticias/`, `/prensa/`, `/blog/`, `/sala-de-prensa/`, `/multimedia/`).
- Concursos no laborales (cuento, fotografía, escolar, proyectos).
- Convocatoria de becas y beneficios sociales explícitos.

## Detección de antigüedad (relativa al año en curso)
La heurística `_is_historical` deriva los años "antiguos" y "actuales" de
`date.today()` en lugar de listas hardcodeadas:

- **Histórico**: año entre `current_year - 6` y `current_year - 2`, sin
  current_hint en el texto.
- **Current hint**: `current_year - 1`, `current_year`, `current_year + 1`,
  o palabras-señal (`vigente`, `abierto`, `postulaciones hasta`, `en curso`).

Esto evita el efecto rollover anual: un aviso publicado en enero de 2027 que
mencione "2025" deja de considerarse vigente automáticamente sin tocar código.

## Guard noticioso por contexto estructural
`news_without_deadline_guard` ya no se gatilla por mención casual de
"comunicado" o "boletín" en el cuerpo del aviso. Sólo se activa si la pista
noticiosa aparece en la URL, breadcrumbs, `section_hint` o título — es decir,
en metadatos estructurales — Y no se detectó fecha de cierre. Esto reduce
falsos negativos en cargos legítimos como "Director(a) de Comunicaciones".

## Umbrales configurables
- `score >= 0.80`: aceptación automática.
- `0.55 <= score < 0.80`: caso ambiguo, activar fallback LLM.
- `score < 0.55`: rechazo.

## Reglas mínimas de aceptación
Aunque el score sea alto, debe cumplir mínimo de señales esenciales configurables (por defecto 2):
- título/cargo,
- fechas de cierre/inicio,
- funciones o requisitos,
- adjunto de bases/perfil,
- contrato o renta.

## Fallback LLM (acotado)
Se usa solo para ambiguos/conflictivos. El prompt recibe resumen estructurado y evidencia parcial, nunca HTML completo bruto indiscriminado.

Si el LLM contradice una señal estructural fuerte (ej. URL de noticias, página de resultados), se marca `needs_review=true`.

## Auditoría estadística de clasificación

`scrapers/evaluation/classification_quality_audit.py` consume eventos reales
(`offer_quality_events` o JSON/JSONL exportado) y emite un reporte con:

- **Distribución por decisión** (publish/review/reject) y resúmenes
  estadísticos (count/mean/median/p25/p75/min/max) de `quality_score`,
  `policy_score`, `classification_score`.
- **Distribución por reason_code**, segmentada por decisión.
- **Anomalías detectadas**:
  - `publish_with_negative_url` — posible falso positivo.
  - `reject_with_only_positive_signals` — posible falso negativo.
  - `review_above_accept_threshold` — umbral mal calibrado.
  - `publish_below_ambiguity_threshold` — fuente confiable que pasa con score bajo.
  - `reject_with_high_classification_score` — contradicción rule_engine vs quality_validator.
  - `needs_review_without_reason` — trazabilidad débil.
  - `llm_fallback_rejected` — el LLM no logró desambiguar.
- **Sugerencias accionables**: tokens recurrentes en cargos rechazados que
  deberían entrar a `NEGATIVE_PATTERNS`, y segmentos de URL recurrentes en
  rechazos que faltan en `NEGATIVE_URL_PARTS`. Las sugerencias requieren
  soporte mínimo (default 3) y discriminación reject/publish ≥ 3x.

CLI:

```bash
# Desde JSONL exportado:
python -m scrapers.evaluation.classification_quality_audit \
    --input reports/quality_events.jsonl \
    --output reports/classification_audit.json

# Desde la base de datos directamente (requiere DB_PASSWORD):
python -m scrapers.evaluation.classification_quality_audit \
    --from-db --since 2026-04-01 --limit 5000 \
    --output reports/classification_audit.json
```

El reporte resultante alimenta el ciclo de refinamiento de heurísticas:
una vez al mes (o tras correr una campaña de scraping), se revisa el
top de sugerencias y se promueven candidatos a `classification/policy.py`,
bumpeando `RULESET_VERSION`.
