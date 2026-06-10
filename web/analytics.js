/* ═══════════════════════════════════════════════════════════════
   web/analytics.js — bootstrap de Umami + helper window.track().

   La config se lee de dos `<meta>` tags en el `<head>` de
   `index.html`:

       <meta name="umami-url" content="">
       <meta name="umami-website-id" content="">

   Para activar Umami, editar esos dos valores. Si alguno está vacío,
   el script no carga y `window.track()` queda como no-op — el resto
   del código sigue funcionando sin cambios.

   Antes esto vivía como `<script>` inline con `window.__UMAMI_URL` /
   `window.__UMAMI_WEBSITE_ID`. Al externalizarlo se habilita
   `script-src 'self'` sin `'unsafe-inline'` en CSP.
   =================================================================== */
(function () {
  function meta(name) {
    var el = document.querySelector('meta[name="' + name + '"]');
    return el ? (el.getAttribute('content') || '').trim() : '';
  }

  var url = meta('umami-url');
  var websiteId = meta('umami-website-id');

  // Helper único de trackeo. No-op si Umami no carga.
  window.track = function (evento, props) {
    try {
      if (window.umami && typeof window.umami.track === 'function') {
        window.umami.track(evento, props || {});
      }
    } catch (e) { /* silencio */ }
  };

  if (!url || !websiteId) return;

  var s = document.createElement('script');
  s.defer = true;
  s.src = url;
  s.setAttribute('data-website-id', websiteId);
  document.head.appendChild(s);
})();
