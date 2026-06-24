/* ──────────────────────────────────────────────────────────────────────────
   ads.js · Inyector de anuncios AdSense, respetando CSP (sin scripts inline).

   Config en dos capas:
     1. `ads-config.js` (window.ADS_CONFIG) → valores por defecto del repo.
     2. `GET /api/site-config` → lo que se edita EN CALIENTE desde el panel
        admin (claves ads_enabled, ads_client, ads_slot_*). Si la API responde,
        sus valores pisan a los del archivo. Así se activa/desactiva y se
        cambian los IDs sin tocar código ni redeploy.

   - Si el resultado tiene enabled=false o sin client → no carga nada de
     terceros (solo, opcionalmente, un placeholder tenue).
   - Si está activo → carga el loader de AdSense una vez y reemplaza los
     `.ad-slot .ad-slot-inner` por unidades <ins class="adsbygoogle">.

   Ubicaciones (clase del contenedor → clave en slots):
     .ad-slot-resultados → resultados
     .ad-slot-sidebar    → sidebar
     .ad-slot-contenido  → contenido
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var RAILWAY = 'https://contrataoplanta-production.up.railway.app';
  var _h = location.hostname;
  var API = window.__API_BASE ||
    ((_h === 'localhost' || _h === '127.0.0.1' || _h === '') ? 'http://localhost:8000' : RAILWAY);

  // Combina los defaults del archivo con lo que venga de la API (admin).
  function combinar(base, conf) {
    var c = { enabled: false, client: '', slots: {}, mostrarPlaceholder: false };
    base = base || {};
    c.enabled = !!base.enabled;
    c.client = base.client || '';
    c.mostrarPlaceholder = !!base.mostrarPlaceholder;
    c.slots = {
      resultados: (base.slots && base.slots.resultados) || '',
      sidebar: (base.slots && base.slots.sidebar) || '',
      contenido: (base.slots && base.slots.contenido) || ''
    };
    if (conf) {
      if (conf.ads_enabled != null) {
        c.enabled = (conf.ads_enabled === '1' || conf.ads_enabled === 'true' || conf.ads_enabled === true);
      }
      if (conf.ads_client) c.client = String(conf.ads_client).trim();
      if (conf.ads_slot_resultados) c.slots.resultados = String(conf.ads_slot_resultados).trim();
      if (conf.ads_slot_sidebar) c.slots.sidebar = String(conf.ads_slot_sidebar).trim();
      if (conf.ads_slot_contenido) c.slots.contenido = String(conf.ads_slot_contenido).trim();
    }
    return c;
  }

  function placeholders(cfg) {
    if (!cfg.mostrarPlaceholder) return;
    var els = document.querySelectorAll('.ad-slot .ad-slot-inner');
    Array.prototype.forEach.call(els, function (el) {
      el.textContent = 'Espacio publicitario';
      el.style.opacity = '.5';
    });
  }

  function run(cfg) {
    if (!cfg.enabled || !cfg.client) { placeholders(cfg); return; }

    // 1) Loader de AdSense, una sola vez.
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
  }

  var base = window.ADS_CONFIG || {};
  // Consulta la config del admin; si falla, usa solo los defaults del archivo.
  fetch(API + '/api/site-config')
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (d) { run(combinar(base, d && d.config)); })
    .catch(function () { run(combinar(base, null)); });
})();
