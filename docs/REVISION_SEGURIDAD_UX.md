# Revisión de seguridad y experiencia de uso

Auditoría del sitio (backend FastAPI, frontend estático y panel admin) enfocada
en robustecer la seguridad y mejorar la experiencia. Se separa lo **corregido en
este cambio** de las **recomendaciones pendientes** que implican cambios de
producto/esquema y conviene decidir aparte.

El backend está, en general, bien blindado: SQL siempre parametrizado, `ORDER BY`
por allowlist, autorización por roles consistente (`lector`/`editor`/`admin`),
subprocesos sin `shell`, webhook de Resend con firma HMAC y anti-replay, JSON-LD
escapado contra breakout de `</script>`, y CSP en modo enforce sin
`script-src 'unsafe-inline'`. Los hallazgos son de endurecimiento, no de agujeros
críticos abiertos.

## Corregido en este cambio

| # | Sev. | Área | Problema | Fix |
|---|------|------|----------|-----|
| M2 | Media | `meilisearch_svc.py` | Los filtros `region`/`sector`/`tipo` se interpolaban sin escapar en la expresión de filtro de Meilisearch → inyección en el DSL (anular `activo = true`, exponer inactivas, romper el índice). | Escape de `\` y `"` antes de interpolar. |
| M1 | Media | `admin.py` | Endpoints con PII de suscriptores (lista, export CSV, eventos de email) usaban `verify_admin_jwt`, así que el rol `lector` (solo-lectura) podía descargar el padrón de correos. | Elevados a `require_editor`, alineado con `/usuarios`. |
| M4 | Media | `public.py` | `AlertaPayload` (POST `/api/alertas`) sin límites de longitud → entradas enormes al regex de email y 500 al superar el VARCHAR. | `Field(max_length=…)` por campo. |
| A1-fe | Media | `gestion.js` | Celdas del panel admin inyectaban `cargo`/`institucion`/nombre scrapeados con `trunc()` sin escapar → inyección de HTML en el origin del JWT (hoy contenida por CSP). | Nuevo helper `truncEsc()` + `esc()` en `cmd`/`det`. |
| B1-fe | Media | `historial.html` | `onchange="buscar()"`/`onkeydown` inline los bloquea la CSP enforce → los dropdowns no filtraban. | Handlers movidos a `historial.js` con `addEventListener`. |
| B5 | Baja | `public.py` | `GET /api` divulgaba `db_host`; el 503 de OG devolvía la excepción interna. | Quitado `db_host`; mensaje genérico + detalle a logs. |
| B6 | Baja | `leyes.py`, `regiones.py` | `termino` sin URL-encodear hacia BCN; `codigo_region` interpolado sin validar en la URL de la DPA. | `quote()` en el término; validación numérica del código. |
| B8 | Baja | `public.py` | `pagina` sin tope superior → OFFSET profundo costoso (DoS de bajo grado). | `le=10000` en `pagina`. |
| A2-fe | Baja | `app.js` | `grado_eus` scrapeado sin escapar en `formatRenta`/`formatRentaRow` (innerHTML de la tarjeta). | `escHtml(String(grado))`. |
| A4-fe | Baja | `app.js` | El botón "Postular" abría `window.open(url)` confiando solo en el flag backend, sin re-validar esquema. | Gate `isValidHttpUrl(url)` antes de abrir. |
| I5 | Info | `deps.py` | Un JWT sin claim `rol` defaulteaba a `admin` (fail-open). | Default a `lector` (fail-safe). |
| A3-fe | Info | `app.js` | `footer_extra` inyectado como HTML sin documentar el contrato de confianza. | Comentario: es HTML de operador (solo `PUT /config`, rol admin). |
| B2 | Baja | `admin.py` | `admin_set_override` no validaba `kind` contra `ScraperKind`. | Validación de `kind` (mismo patrón que `scraper_run`). |
| A1 | **Alta** | alertas | `POST /api/alertas` activaba la suscripción sin verificar la propiedad del correo → email bombing a terceros. | **Doble opt-in** (ver detalle abajo). |

### A1 — Doble opt-in en suscripción a alertas (implementado)
`POST /api/alertas` ya no activa envíos sin confirmar el correo. Cambios:
1. Migración `20260701_0002_alertas_doble_optin`: columnas `verificada`,
   `token_verificacion`, `verificada_en` en `alertas_suscripciones`. Las filas
   **preexistentes** se marcan `verificada = TRUE` (no se cortan sus envíos).
2. `crear_alerta`: inserta con `verificada = FALSE` + token
   (`secrets.token_urlsafe(32)`) y envía `enviar_verificacion`. Si Resend NO
   está configurado, degrada a activación inmediata (dev/staging).
3. `GET /api/alertas/confirmar?token=…`: marca `verificada = TRUE`, consume el
   token (un solo uso) y responde. Con rate limit público.
4. El envío (`api_enviar_alertas_pendientes`) filtra `verificada = TRUE`.
5. Frontend: maneja `?verificar=` (banner de confirmación) y el mensaje de alta
   distingue "revisa tu correo" de "activada". El panel admin muestra
   verificadas / sin verificar.

### M3 — Rate limiting de endpoints públicos compartido (implementado)
El límite de `/api/alertas`, `/api/alertas/confirmar` y `/api/validar-email`
vivía en un `dict` en memoria por worker (≈N× el nominal, se reiniciaba en cada
deploy, y las claves de IP nunca se purgaban). Cambios:
1. Migración `20260701_0003_public_rate_limit`: tabla `public_rate_hits`
   (`ip`, `bucket`, `ts`) con índices, análoga a `admin_auth_failures`.
2. `deps.check_public_rate_limit(ip, bucket, window, max)`: cuenta en Postgres
   (compartido entre workers), purga la ventana vencida y cae a memoria si la DB
   no responde. El fallback en memoria descarta claves vencidas (no crece sin
   límite).
3. `bucket` separa el presupuesto por endpoint (`alertas`, `alertas_confirmar`,
   `validar_email`) para que el abuso de uno no consuma la cuota de otro.
4. El beacon `/api/track` se mantiene en memoria **a propósito** (alto volumen,
   valor de seguridad bajo: un INSERT por hit sería desproporcionado).

Nota abierta: `client_ip` toma el último `X-Forwarded-For`. Detrás de
Cloudflare+Railway conviene fijar el número de proxies de confianza para extraer
la IP real; no se cambió aquí porque depende de la topología exacta del proxy y
un valor incorrecto agruparía a todos los clientes bajo una IP.

### Protección de "último admin" (implementado)
`admin_editar_usuario` y `admin_borrar_usuario` rechazan (409) la operación si
degradaría, desactivaría o eliminaría la **única** cuenta `admin` activa. Helper
`_es_ultimo_admin_activo`. La contraseña maestra `ops` sigue siendo respaldo,
pero ya no se puede quedar sin admins nominales por accidente.

## Recomendaciones pendientes (trade-offs deliberados — no cambiar a ciegas)

- **`img-src https:` — se mantiene a propósito.** Revisado: la cadena de fallback
  de logos institucionales carga imágenes desde DuckDuckGo, Google s2 y los
  dominios de cada institución (`app.js` ~1414-1417). Restringir `img-src` a
  orígenes propios rompería los logos en todo el sitio. El riesgo que habilita
  (exfiltración vía beacon de imagen) requiere una inyección de HTML previa, ya
  mitigada por el escapado + `script-src 'self'`. Es un trade-off aceptado.
- **CSP `style-src 'unsafe-inline'`**: trade-off ya documentado; migrar los
  ~263 `style=` inline a clases lo cerraría (refactor grande y frágil, bajo
  beneficio — el vector es CSS injection, que exige inyección de HTML previa).
- **Bundles sin minificar** (`app.js` 238 KB, `gestion.js` 115 KB) con
  `max-age=0, must-revalidate`: versionar el nombre (hash) permitiría cachear
  `immutable` y mejorar la carga percibida. Requiere pipeline de build (hoy los
  archivos se sirven tal cual); es perf, no seguridad.
- **`client_ip` / `X-Forwarded-For`**: fijar el número de proxies de confianza al
  extraer la IP (depende de la topología exacta Cloudflare+Railway).
