# CLASSIFICATION_RULES

## Enfoque
Clasificación determinística por reglas con trazabilidad completa (`rule_trace`, señales positivas/negativas, razones de rechazo).

## Señales positivas principales
- Keywords laborales: cargo, postulación, concurso público, funciones, requisitos, renta, contrata/planta, fecha de cierre.
- Estructura de URL laboral: `/trabaja-con-nosotros`, `/concursos`, `/postulacion`, `/ofertas-laborales`.
- Tablas con columnas típicas de vacante (`cargo/renta/cierre`).
- Adjuntos PDF relevantes (`bases`, `perfil`, `concurso`, `tdr`, `convocatoria`).

## Señales negativas principales
- Contexto de noticias/comunicados/prensa/blog.
- Contexto de eventos/agenda institucional.
- Resultados o cierre (`nómina`, `adjudicación`, `proceso finalizado`, `concurso cerrado`).
- Históricos/archivo/memoria anual.
- URL no laboral (`/noticias/`, `/prensa/`, `/blog/`, `/novedades/`).

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
