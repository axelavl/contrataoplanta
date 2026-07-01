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

## Recomendaciones pendientes (requieren decisión de producto/esquema)

### A1 — Doble opt-in en suscripción a alertas (severidad ALTA)
`POST /api/alertas` activa la suscripción (`activa = TRUE`) **sin verificar la
propiedad del correo**. Cualquiera puede suscribir a un tercero, que recibirá
alertas no solicitadas (acoso/spam y daño a la reputación del dominio remitente).
La infraestructura ya existe parcialmente: `enviar_verificacion(email, token)` en
`email_alerts.py` construye el enlace `?verificar=<token>`, pero **no se invoca**,
no hay endpoint de confirmación, y la tabla `alertas_suscripciones` no tiene
columna de token (la que existe, `token_verificacion`, vive en la tabla `usuarios`,
que la API no usa).

Plan de implementación:
1. Migración Alembic: agregar `token_verificacion VARCHAR(100)` y
   `verificada BOOLEAN DEFAULT FALSE` a `alertas_suscripciones`.
2. `crear_alerta`: insertar con `verificada = FALSE` + token
   (`secrets.token_urlsafe`), llamar `enviar_verificacion`, responder
   "revisa tu correo".
3. Nuevo `GET /api/alertas/confirmar?token=…` que marca `verificada = TRUE`.
4. El cron de envío debe filtrar `verificada = TRUE`.
5. Frontend: manejar `?verificar=` y cambiar el mensaje de éxito.

No se implementó aquí porque cambia el comportamiento visible (el usuario debe
confirmar) y toca el flujo de email en vivo + esquema; conviene probarlo
end-to-end contra Resend y una DB real antes de desplegar.

### M3 — Rate limiting de endpoints públicos frágil (severidad MEDIA)
El límite de `/api/alertas` y `/api/track` vive en un `dict` en memoria: con
`--workers 2` es por-worker (≈2×) y se reinicia en cada deploy; las claves de IP
no se purgan (crecimiento no acotado); y `client_ip` toma el último `X-Forwarded-For`,
que detrás de Cloudflare+Railway puede ser una IP de infraestructura compartida.
El estado de auth admin ya se migró a Postgres (`admin_auth_failures`); conviene
llevar también el rate limit público a un backend compartido (Postgres/Redis),
purgar claves vacías y fijar el número de proxies de confianza al extraer la IP.

### Otros (defensa en profundidad)
- **CSP `style-src 'unsafe-inline'`**: trade-off ya documentado; migrar los
  `style=` inline a clases lo cerraría (refactor grande, bajo beneficio).
- **`img-src https:`** amplio: si ocurriera inyección de HTML, permitiría
  exfiltración vía beacon de imagen a cualquier host. Acotar a orígenes propios.
- **Bundles sin minificar** (`app.js` 238 KB, `gestion.js` 115 KB) con
  `max-age=0, must-revalidate`: versionar el nombre (hash) permitiría cachear
  `immutable` y mejorar la carga percibida.
- **Protección de "último admin"**: hoy un admin puede degradarse/borrar a todos
  los admins (recuperable vía contraseña maestra `ops`). Opcional: impedir dejar
  cero admins activos.
