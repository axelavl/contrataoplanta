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
      if (navFav) navFav.textContent = '♥ Mis favoritos (' + favs.length + ')';
    } catch (e) {}
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

  loadPartial('site-header', 'partials/header.html')
    .then(function (ok) {
      if (ok) {
        applyActiveNav(document);
        updateFavCount(document);
      }
      return loadPartial('site-footer', 'partials/footer.html');
    })
    .then(function () {
      document.dispatchEvent(new CustomEvent('shell:ready', { detail: { page: page } }));
    });
})();
