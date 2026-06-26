/* ═══════════════════════════════════════════════════════════════
   web/analytics.js — bootstrap de Umami + analítica propia + window.track().

   Dos canales complementarios, ambos opcionales y sin datos personales:

   1. Umami (externo). Se activa con dos <meta> en el <head> de las
      páginas:
          <meta name="umami-url" content="">
          <meta name="umami-website-id" content="">
      Si alguno está vacío, el script de Umami no carga.

   2. Analítica propia. Envía un beacon liviano a `POST /api/track`
      del backend en cada vista de página y cuando se llama
      `window.track('evento', …)`. Alimenta la pestaña «Estadísticas
      del sitio» del panel de gestión. No guarda IP; el visitante único
      se aproxima con un hash anónimo que rota a diario en el servidor.

   `window.track(evento, props)` dispara AMBOS canales y es un no-op
   seguro si ninguno está disponible — el resto del código sigue igual.

   Externalizado del <script> inline para habilitar `script-src 'self'`
   en CSP (web/_headers) sin `'unsafe-inline'`.
   =================================================================== */
(function () {
  // Guard anti-doble-arranque: algunas páginas cargan el tag directo y, vía
  // shared-shell.js, podrían intentar inyectarlo otra vez. Solo el primero
  // dispara la vista de página y el bootstrap de Umami.
  if (window.__analyticsBooted) return;
  window.__analyticsBooted = true;

  function meta(name) {
    var el = document.querySelector('meta[name="' + name + '"]');
    return el ? (el.getAttribute('content') || '').trim() : '';
  }

  // Nombres de evento que ya usa app.js → eventos canónicos del backend.
  // Así no hay que renombrar las decenas de llamadas existentes a track().
  var ALIAS = {
    'offer-view': 'ver_oferta',
    'apply-click': 'click_postular',
    'ver-bases': 'click_bases',
    'search': 'buscar',
    'search-autocomplete': 'buscar',
    'whatsapp': 'compartir', 'linkedin': 'compartir',
    'instagram': 'compartir', 'copiar': 'compartir'
  };

  // ── Base del backend propio (mismo criterio que app.js / gestion.js) ──
  var RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
  var API_BASE = (typeof window.__API_BASE === 'string' && window.__API_BASE.trim())
    ? window.__API_BASE.trim()
    : (location.hostname === 'localhost' || location.hostname === '127.0.0.1') ? '' : RAILWAY_BACKEND;

  // Eventos personalizados que el backend acepta (debe calzar con
  // EVENTOS_VALIDOS en api/services/analitica.py). Si no calza, se descarta.
  var EVENTOS_VALIDOS = {
    ver_oferta: 1, click_postular: 1, click_bases: 1, suscribir_alerta: 1,
    buscar: 1, filtrar: 1, compartir: 1, ver_curso: 1, click_curso: 1,
    descargar_csv: 1, ver_institucion: 1
  };

  function enviarBeacon(body) {
    try {
      var url = API_BASE + '/api/track';
      var json = JSON.stringify(body);
      // text/plain es un content-type "CORS-safelisted": evita el preflight
      // que sendBeacon no puede hacer cuando el beacon va cross-origin
      // (pages.dev → Railway). El backend parsea el cuerpo a mano.
      if (navigator.sendBeacon) {
        navigator.sendBeacon(url, new Blob([json], { type: 'text/plain;charset=UTF-8' }));
      } else {
        fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
          body: json,
          keepalive: true
        }).catch(function () { /* silencio */ });
      }
    } catch (e) { /* silencio */ }
  }

  function trackInterno(evento, props) {
    props = props || {};
    var body = {
      tipo: evento ? 'evento' : 'pageview',
      path: location.pathname,
      ref: document.referrer || null
    };
    if (evento) {
      var canonico = ALIAS[evento] || evento;
      if (!EVENTOS_VALIDOS[canonico]) return; // descartar eventos no reconocidos
      body.evento = canonico;
      var oid = (props.oferta_id != null ? props.oferta_id : props.id);
      if (oid != null) body.oferta_id = parseInt(oid, 10) || null;
    }
    enviarBeacon(body);
  }

  // Helper único de trackeo: dispara Umami (si está) + analítica propia.
  window.track = function (evento, props) {
    try {
      if (window.umami && typeof window.umami.track === 'function') {
        window.umami.track(evento, props || {});
      }
    } catch (e) { /* silencio */ }
    trackInterno(evento, props);
  };

  // ── Vista de página inicial (analítica propia, siempre) ──
  // Respeta Do Not Track: si el usuario lo activó, no registramos nada.
  var dnt = (navigator.doNotTrack === '1' || window.doNotTrack === '1');
  if (!dnt) trackInterno(null, null);

  // ── Umami (solo si está configurado) ──
  var url = meta('umami-url');
  var websiteId = meta('umami-website-id');
  if (!url || !websiteId) return;

  var s = document.createElement('script');
  s.defer = true;
  s.src = url;
  s.setAttribute('data-website-id', websiteId);
  document.head.appendChild(s);
})();
