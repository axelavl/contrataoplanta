/* ──────────────────────────────────────────────────────────────────────────
   ads.js · Inyector de anuncios AdSense, respetando CSP (sin scripts inline).
   Depende de ads-config.js (window.ADS_CONFIG).

   - Si ADS_CONFIG.enabled === false → no carga nada de terceros.
   - Si está activo → carga el loader de AdSense una sola vez y reemplaza los
     `.ad-slot .ad-slot-inner` por unidades <ins class="adsbygoogle">.

   Ubicaciones soportadas (clase del contenedor → clave en ADS_CONFIG.slots):
     .ad-slot-resultados → resultados
     .ad-slot-sidebar    → sidebar
     .ad-slot-contenido  → contenido
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var cfg = window.ADS_CONFIG || {};

  function placeholders() {
    if (!cfg.mostrarPlaceholder) return;
    var els = document.querySelectorAll('.ad-slot .ad-slot-inner');
    Array.prototype.forEach.call(els, function (el) {
      el.textContent = 'Espacio publicitario';
      el.style.opacity = '.5';
    });
  }

  if (!cfg.enabled || !cfg.client) { placeholders(); return; }

  // 1) Cargar el loader de AdSense una sola vez.
  if (!document.getElementById('adsbygoogle-js')) {
    var s = document.createElement('script');
    s.id = 'adsbygoogle-js';
    s.async = true;
    s.src = 'https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=' + encodeURIComponent(cfg.client);
    s.crossOrigin = 'anonymous';
    document.head.appendChild(s);
  }

  // 2) Reemplazar cada slot por una unidad de anuncio.
  var mapa = [
    ['ad-slot-resultados', 'resultados'],
    ['ad-slot-sidebar', 'sidebar'],
    ['ad-slot-contenido', 'contenido']
  ];

  mapa.forEach(function (par) {
    var slotId = cfg.slots && cfg.slots[par[1]];
    if (!slotId) return;
    var inners = document.querySelectorAll('.' + par[0] + ' .ad-slot-inner');
    Array.prototype.forEach.call(inners, function (inner) {
      var ins = document.createElement('ins');
      ins.className = 'adsbygoogle';
      ins.style.display = 'block';
      ins.setAttribute('data-ad-client', cfg.client);
      ins.setAttribute('data-ad-slot', slotId);
      ins.setAttribute('data-ad-format', 'auto');
      ins.setAttribute('data-full-width-responsive', 'true');
      inner.replaceWith(ins);
      try { (window.adsbygoogle = window.adsbygoogle || []).push({}); } catch (e) {}
    });
  });
})();
