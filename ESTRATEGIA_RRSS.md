# Estrategia RRSS — estadoemplea (Instagram + LinkedIn)

Documento operativo. Define qué publicar, con qué frecuencia, cómo se genera de
forma semi-automática desde los avisos destacados, y cómo se mide. Complementa
`MONETIZACION_ADS_RRSS.md` (que cubre el embudo y la monetización); aquí el foco
es la **operación de publicación** y la **automatización del diseño**.

## El principio

El objetivo de RRSS no es juntar seguidores: es **mover gente al sitio** (donde se
monetiza) y **hacer crecer la lista de correos**. La ventaja de estadoemplea es
que ya produce el contenido sin escribir: cada aviso es materia prima visual, y
el sitio ya genera la imagen por oferta. La automatización solo industrializa eso.

## Qué se automatizó

El paquete `social/` toma los avisos marcados como **destacados**
(`ofertas.nivel = 'destacado'`), y por cada uno genera:

- La **imagen** para Instagram (1080×1080) y LinkedIn (1200×630), con la marca,
  el cargo, la institución, ubicación, remuneración, alerta de cierre y CTA.
  Reutiliza el motor `render_offer_card` que ya alimenta `/api/og/{id}.png`.
- El **copy** por plataforma (Instagram con emojis y hashtags; LinkedIn más
  sobrio, con enlace inline), con **enlace UTM** para medir clics reales.

Todo cae en una **cola de aprobación** (`social/cola/<fecha>/revisar.html`): se
revisa, se edita el texto si hace falta, se aprueba y se exporta. **No publica
solo** — es semi-automático por diseño (ver más abajo).

## Dos caminos de diseño (la decisión que quedó abierta)

**Por código (recomendado para el flujo diario).** Es el que ya está integrado:
gratis, instantáneo, 100% automático, consistente con la marca y sin depender de
terceros. Corre dentro del backend con Pillow. Para el volumen recurrente de
"avisos destacados", es el camino correcto.

**Canva (para piezas especiales).** Mejor para contenido editorial puntual donde
quieres control creativo fino: carruseles educativos ("5 errores al postular"),
campañas, plantillas estacionales. La generación automática **por aviso** en Canva
no escala bien con la API estándar (requiere Brand Templates + Autofill, una
función de plan superior); por eso conviene usar Canva a mano para lo especial y
dejar el flujo diario al motor por código.

Conclusión práctica: **código para el día a día (alertas de avisos), Canva para
las piezas de pilar/campaña**. No es uno u otro; es cada uno donde rinde.

## Pilares de contenido

1. **Alertas de oportunidad** (lo automatizado) — avisos destacados, "cierra esta
   semana", nuevas vacantes por región. Recurrente, rápido, con CTA al sitio.
2. **Plata / transparencia de renta** — cuánto paga de verdad un grado, planta vs.
   contrata. Es lo que más se comparte; sale de la calculadora.
3. **Cómo postular bien** — errores que dejan fuera, qué piden las bases, prueba
   psicolaboral. Sale del hub de preparación.
4. **Datos del mercado** — instituciones que más contratan, áreas con más demanda.
   Sale del panel de mercado (doble uso con el B2B).
5. **Glosario / educación** — qué es un grado EUS, qué es estamento. Capta a quien
   recién empieza.

Los pilares 2–5 son carruseles/Reels que se arman a mano (o en Canva). El pilar 1
es el que el pipeline produce automáticamente.

## Cadencia realista

- **Instagram:** 4–6 posts/semana. La mayoría, alertas de avisos destacados desde
  la cola; 1–2 de pilar (renta, cómo postular). Historias diarias para los cierres
  del día.
- **LinkedIn:** 3–4 posts/semana. Prioriza avisos profesionales/jefaturas y datos
  de mercado; el tono profesional rinde mejor aquí que las alertas masivas.
- **Constancia > perfección.** Mejor 4 piezas simples por semana que una muy
  producida al mes. El mismo aviso sirve para ambas plataformas.

## Flujo de trabajo semanal (≈5 min de operación)

1. Marca como `destacado` los avisos que valen (criterio: institución conocida,
   renta atractiva, cierre próximo, o vacante con alto interés).
2. Corre `python -m social.generar_cola` (o déjalo en un timer 2×/semana).
3. Abre `revisar.html`, revisa imagen + copy, ajusta lo que quieras, aprueba.
4. Exporta los aprobados y publica: en Instagram con link en la bio; en LinkedIn
   con el enlace en el texto. Las Historias diarias salen de los mismos PNG.

## Métricas que importan (no los likes)

- **Clics al sitio desde RRSS** — medibles por los UTM (`utm_source=instagram|
  linkedin`, `utm_campaign=destacados`) en Umami/Analytics.
- **Correos nuevos en alertas/semana** — la conversión real.
- **Páginas vistas/mes** — lo que mueve los ingresos por AdSense.
- Likes y seguidores: señal secundaria, no objetivo.

## Roadmap

- **Fase 1 (lista):** generación de diseño + copy + cola de aprobación para IG y
  LinkedIn desde avisos destacados.
- **Fase 2:** criterio automático de "destacado" (p. ej. top por renta/cierre/
  institución) para no marcar a mano; plantillas de carrusel por pilar.
- **Fase 3 (opcional):** publicación full-auto vía Meta Graph API (Instagram
  Business) y LinkedIn API, leyendo del `manifest.json` los posts en estado
  `aprobado`. Requiere apps aprobadas y tokens; evaluar cuando el orgánico ya
  funcione.

## Riesgos / cuidados

- **Dato mal scrapeado en una imagen pública.** La cola de aprobación existe para
  esto: un humano valida antes de publicar. No saltarse ese paso en full-auto sin
  un umbral de calidad (`overall_quality_score`) alto.
- **Saturar con solo alertas.** Mezcla pilares: si todo es "postula", el alcance
  cae. La regla 80/20 (valor/promoción) aplica.
- **Marca consistente.** El motor por código garantiza consistencia; si se suma
  Canva a mano, respetar paleta (#0A2E6E / #E8A820) y tipografía.
