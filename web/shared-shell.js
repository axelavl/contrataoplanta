(function () {
  'use strict';

  var page = (document.body && document.body.dataset && document.body.dataset.page) || 'none';

  function applyActiveNav(root) {
    var map = {
      home: 'home', favoritos: 'favoritos', estadisticas: 'estadisticas', faq: 'faq',
      historial: 'home', terminos: 'none', privacidad: 'none', descargo: 'none', ruta: 'ruta'
    };
    var target = map[page] || 'none';
    if (target === 'none') return;
    var el = root.querySelector('[data-nav="' + target + '"]');
    if (el) el.classList.add('active');
  }

  function updateFavCount(root) {
    try {
      var favs = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]');
      if (!favs.length) return;
      var navFav = root.querySelector('#nav-favoritos');
      if (navFav) navFav.textContent = '♥ Favoritos (' + favs.length + ')';
    } catch (e) {}
  }

  function ensureMobileNavScript() {
    if (window.__mobileNavInitialized || document.getElementById('nav-mobile-script')) return;
    var alreadyLoaded = Array.prototype.some.call(document.scripts, function (s) {
      return /(^|\/)nav-mobile\.js($|[?#])/.test(s.getAttribute('src') || '');
    });
    if (alreadyLoaded) return;
    var script = document.createElement('script');
    script.src = 'nav-mobile.js';
    script.id = 'nav-mobile-script';
    document.body.appendChild(script);
  }

  function ensureThemeToggleScript() {
    if (document.getElementById('theme-toggle-script')) return;
    var alreadyLoaded = Array.prototype.some.call(document.scripts, function (s) {
      return /(^|\/)theme-toggle\.js($|[?#])/.test(s.getAttribute('src') || '');
    });
    if (alreadyLoaded) return;
    var script = document.createElement('script');
    script.src = 'theme-toggle.js';
    script.id = 'theme-toggle-script';
    document.body.appendChild(script);
  }

  function loadPartial(id, path) {
    var mount = document.getElementById(id);
    if (!mount) return Promise.resolve(false);
    return fetch(path)
      .then(function (res) { return res.ok ? res.text() : ''; })
      .then(function (html) {
        if (!html) return false;
        mount.outerHTML = html;
        return true;
      })
      .catch(function () { return false; });
  }

  // Cargar ribbon (freshness bar) primero, después header, después footer
  loadPartial('site-ribbon', 'partials/ribbon.html')
    .then(function () {
      return loadPartial('site-header', 'partials/header.html');
    })
    .then(function (headerOk) {
      if (headerOk) {
        applyActiveNav(document);
        updateFavCount(document);
      }
      ensureMobileNavScript();
      ensureThemeToggleScript();
      return loadPartial('site-footer', 'partials/footer.html');
    })
    .then(function () {
      document.dispatchEvent(new CustomEvent('shell:ready', { detail: { page: page } }));
    });
})();
