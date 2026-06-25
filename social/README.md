# social/ — Automatización de RRSS (Instagram + LinkedIn)

Genera, a partir de los avisos marcados **destacados**, las imágenes y el copy
para Instagram y LinkedIn, y los deja en una **cola de aprobación**. Nada se
publica solo: tú revisas, editas el texto y exportas lo aprobado. Semi-automático
a propósito (menos riesgo, sin tokens de publicación de Meta/LinkedIn).

Reutiliza lo que ya existía en el repo:
- Motor de diseño: `api/services/og_image.py` → `render_offer_card(oferta, fmt)`
  (mismo render que `/api/og/{id}.png`). Paleta NAVY/GOLD de la marca.
- SQL canónico de ofertas: `api/services/sql.py` + serializador `api/services/seo.py`.

## Qué es un "aviso destacado"

La columna `ofertas.nivel` con valor `'destacado'` (se marca desde el panel
admin, `api/routers/admin.py`). El pipeline solo toma esos, vigentes y con
cierre dentro de la ventana configurada.

## Uso

```bash
# Probar el visual sin BD (datos de ejemplo) — genera la cola y la UI:
python -m social.generar_cola --demo

# Producción (lee destacados vigentes de la BD):
python -m social.generar_cola

# Opciones:
python -m social.generar_cola --max 5            # tope de avisos
python -m social.generar_cola --solo instagram   # una sola plataforma
python -m social.generar_cola --dry-run          # lista sin escribir
```

Salida en `social/cola/<fecha>/`:
- `oferta-<id>/instagram.png` (1080×1080) y `linkedin.png` (1200×630)
- `manifest.json` — datos + copy de cada post
- `revisar.html` — **ábrelo con doble clic**: muestra cada post, permite editar
  el copy, aprobar/rechazar (se guarda en el navegador) y exportar los aprobados.

## Flujo semanal (5 minutos)

1. Marca avisos como `destacado` en el panel (o automatiza ese criterio).
2. Corre `python -m social.generar_cola`.
3. Abre `social/cola/<fecha>/revisar.html`, revisa, aprueba, ajusta copy.
4. "Exportar aprobados" → descarga `aprobados-<fecha>.json`. Sube cada imagen y
   pega el copy en Instagram (link en bio) y LinkedIn (link en el texto).

## Programarlo (opcional)

Crear un timer systemd análogo al de scrapers
(`deploy/systemd/contrataoplanta-scrapers.*`), p. ej. lunes y jueves 08:00:

```ini
# contrataoplanta-rrss.service  (oneshot)
ExecStart=/opt/contrataoplanta/.venv/bin/python -m social.generar_cola
WorkingDirectory=/opt/contrataoplanta
```

```ini
# contrataoplanta-rrss.timer
OnCalendar=Mon,Thu 08:00
```

## Configuración

Todo en `social/config.py` (o vía env): `RRSS_MAX`, `RRSS_VENTANA_DIAS`,
`RRSS_COLA_DIR`, `RRSS_UTM_CAMPAIGN`, hashtags, emojis (ponlos en `""` para
desactivar), mapa región→hashtag.

## Por qué semi-automático y no publicación directa

Publicar solo en IG/LinkedIn requiere apps aprobadas y tokens (Meta Graph API
con Instagram Business; LinkedIn Marketing API con revisión). Es factible como
fase 2, pero el 90% del valor (diseño + copy listos) está en esta capa, sin ese
costo ni el riesgo de publicar algo con un dato mal scrapeado. Si más adelante
quieres full-auto, el `manifest.json` ya es el contrato: un publicador leería los
posts en estado `aprobado` y los enviaría por API.
