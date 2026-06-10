(function () {
  'use strict';

  /* ── Logo fallback robusto · fuente única de verdad ──
     Se registra ANTES de que se carguen los partials y las cards, para
     que el override de `window.imgFavFallback` y `window.imgFavCheckQuality`
     esté activo desde el primer image error.

     Cadena de resolución (primero a último):
       1. Clearbit logo @ 256 px
       2. apple-touch-icon del dominio (suele ser 180 px+)
       3. apple-touch-icon-precomposed
       4. Google s2 favicons @ sz=128
       5. favicon del dominio raíz (suele ser 32 px o menos)
     Si ninguna supera el chequeo de calidad (naturalWidth >= 40 px), se
     reemplaza la <img> por un ícono SVG según sector institucional — nunca
     quedamos con recuadros vacíos ni con emojis. */
  (function registerLogoFallback() {
    var MIN_LOGO_PX = 40; // Bajo esto el logo se ve pixelado al subir a 44×44.

    var SECTOR_SVGS = {
      municipal:  '<path d="M12 2 L3 7 h18 Z M4 9 v10 M8 9 v10 M12 9 v10 M16 9 v10 M20 9 v10 M3 21 h18"/>',
      salud:      '<path d="M9 3 h6 v6 h6 v6 h-6 v6 h-6 v-6 H3 V9 h6 Z"/>',
      educacion:  '<path d="M2 9 L12 4 L22 9 L12 14 Z M6 11 V17 c0 1 3 3 6 3 s6-2 6-3 V11"/>',
      ejecutivo:  '<path d="M4 20 V8 l8-4 l8 4 V20 Z M9 20 V14 h6 v6 M9 9 h2 M13 9 h2 M9 12 h2 M13 12 h2"/>',
      judicial:   '<path d="M12 3 V21 M6 6 h12 M5 9 h4 M15 9 h4 M4 14 c0-2 2-3 3-3 s3 1 3 3 M14 14 c0-2 2-3 3-3 s3 1 3 3"/>',
      ffaa:       '<path d="M12 2 L4 6 V12 c0 5 4 8 8 10 c4-2 8-5 8-10 V6 Z"/>',
      empresa:    '<path d="M3 7 h18 v13 H3 Z M8 7 V4 h8 v3 M3 11 h18 M10 15 h4"/>',
      regional:   '<path d="M3 6 L9 3 L15 6 L21 3 V18 L15 21 L9 18 L3 21 Z M9 3 V18 M15 6 V21"/>',
      universidad:'<path d="M2 9 L12 4 L22 9 L12 14 Z M6 11 V17 c0 1 3 3 6 3 s6-2 6-3 V11 M19 9 V14"/>',
      default:    '<path d="M4 20 V9 l8-5 l8 5 V20 Z M9 20 V14 h6 v6"/>'
    };

    function iconSvg(paths) {
      return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="100%" height="100%" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + paths + '</svg>';
    }

    function sectorFor(name) {
      var n = (name || '').toLowerCase();
      if (/municipalidad|municipal|muni\./.test(n)) return 'municipal';
      if (/hospital|salud|clinic|consultori|cesfam|servicio\s+de\s+salud/.test(n)) return 'salud';
      if (/universidad|instituto\s+profesional|centro\s+de\s+formaci/.test(n)) return 'universidad';
      if (/colegio|escuela|liceo|educaci/.test(n)) return 'educacion';
      if (/poder\s+judicial|juzgado|corte|fiscal|tribunal|ministerio\s+p[úu]blico|fiscal[íi]a/.test(n)) return 'judicial';
      if (/fuerzas|armada|ej[ée]rcito|carabineros|pdi|gendarmer|bomberos/.test(n)) return 'ffaa';
      if (/gobierno\s+regional|intendencia|gore/.test(n)) return 'regional';
      if (/empresa|banco|metro|tvn|codelco|enap|enami|correos/.test(n)) return 'empresa';
      if (/ministerio|subsecretar|superintendencia|servicio\s+de|direcci[óo]n\s+general/.test(n)) return 'ejecutivo';
      return 'default';
    }

    function domainFromImg(img) {
      if (img.dataset.domain) return img.dataset.domain;
      var src = img.src || '';
      var m =
        src.match(/logo\.clearbit\.com\/([^?#/]+)/) ||
        src.match(/duckduckgo\.com\/ip3\/([^.]+\.[^/]+)\.ico/) ||
        src.match(/google\.com\/s2\/favicons.*domain=([^&]+)/) ||
        src.match(/^https?:\/\/([^/]+)\/apple-touch-icon(?:-precomposed)?\.png/) ||
        src.match(/^https?:\/\/([^/]+)\/favicon\.ico/);
      var domain = m ? decodeURIComponent(m[1]) : '';
      if (domain) img.dataset.domain = domain;
      return domain;
    }

    function sourcesFor(domain) {
      var enc = encodeURIComponent(domain);
      return [
        'https://logo.clearbit.com/' + enc + '?size=256',
        'https://' + domain + '/apple-touch-icon.png',
        'https://' + domain + '/apple-touch-icon-precomposed.png',
        'https://www.google.com/s2/favicons?domain=' + enc + '&sz=128',
        'https://' + domain + '/favicon.ico'
      ];
    }

    function replaceWithSectorIcon(img) {
      var alt = img.getAttribute('alt') || '';
      var nameMatch = alt.match(/Logo de (.+)/i);
      var institutionName = nameMatch ? nameMatch[1] : alt;
      var sec = sectorFor(institutionName);
      var svg = iconSvg(SECTOR_SVGS[sec] || SECTOR_SVGS.default);
      var wrapper = document.createElement('div');
      wrapper.className = 'sector-icon-fallback sector-icon-fallback--' + sec;
      wrapper.setAttribute('data-sector', sec);
      wrapper.innerHTML = svg;
      if (img.parentNode) img.replaceWith(wrapper);
    }

    function advance(img) {
      var domain = domainFromImg(img);
      if (!domain) { replaceWithSectorIcon(img); return; }
      var sources = sourcesFor(domain);
      var tried = parseInt(img.dataset.attempt || '0', 10);
      // Compat: si venía con la vieja flag fallbackTried=1, salta al tercer source.
      if (img.dataset.fallbackTried === '1' && tried < 2) tried = 2;
      var nextIdx = tried + 1;
      if (nextIdx >= sources.length) { replaceWithSectorIcon(img); return; }
      img.dataset.attempt = String(nextIdx);
      img.src = sources[nextIdx];
    }

    // Error de carga (404, CORS, DNS) → siguiente fuente.
    window.imgFavFallback = advance;

    // Chequeo de calidad al cargar: si el bitmap resulta ser demasiado chico
    // (favicon 16×16/32×32 sobre un recuadro de 44×44+) saltamos. También
    // cubre el caso Clearbit "200 OK + imagen vacía" que no dispara onerror.
    window.imgFavCheckQuality = function (img) {
      if (!img || img.dataset.qualityChecked === '1') return;
      img.dataset.qualityChecked = '1';
      var w = img.naturalWidth || 0;
      var h = img.naturalHeight || 0;
      // 0×0 = imagen rota → que onerror se encargue.
      if (w === 0 && h === 0) return;
      if (Math.min(w, h) < MIN_LOGO_PX) {
        // Permitimos reintentar con el siguiente source.
        img.dataset.qualityChecked = '';
        advance(img);
      }
    };

    // ── Delegación para CSP-safe fallback ──
    // CSP `script-src 'self'` (sin 'unsafe-inline') bloquea `onerror=` y
    // `onload=` inline. Los <img> con `data-attempt` (logos institucionales)
    // se marcaban con esos atributos pero el browser rechazaba ejecutarlos.
    // Resultado visible: recuadro vacío con el placeholder "Lo Sup" de alt.
    //
    // Fix: capturing listeners en document. `error` y `load` no bubbean,
    // pero sí se propagan en la fase de captura → llegan a document.
    document.addEventListener('error', function (e) {
      var img = e.target;
      if (!img || img.tagName !== 'IMG') return;
      if (!img.hasAttribute('data-attempt')) return;
      advance(img);
    }, true);

    document.addEventListener('load', function (e) {
      var img = e.target;
      if (!img || img.tagName !== 'IMG') return;
      if (!img.hasAttribute('data-attempt')) return;
      window.imgFavCheckQuality(img);
    }, true);
  })();

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

  function ensureScript(src, id) {
    if (document.getElementById(id)) return;
    var alreadyLoaded = Array.prototype.some.call(document.scripts, function (s) {
      var currentSrc = s.getAttribute('src') || '';
      var pattern = src.replace('.', '\\.');
      return new RegExp('(^|/)' + pattern + '($|[?#])').test(currentSrc);
    });
    if (alreadyLoaded) return;
    var script = document.createElement('script');
    script.src = src;
    script.id = id;
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

  loadPartial('site-ribbon', 'partials/ribbon.html')
    .then(function () {
      return loadPartial('site-header', 'partials/header.html');
    })
    .then(function (ok) {
      if (ok) {
        applyActiveNav(document);
        updateFavCount(document);
      }
      ensureMobileNavScript();
      ensureScript('theme-toggle.js', 'theme-toggle-script');
      ensureScript('ribbon-data.js', 'ribbon-data-script');
      ensureScript('logo-fallback.js', 'logo-fallback-script');
      ensureScript('search-toggle.js', 'search-toggle-script');
      ensureScript('plazo-colors.js', 'plazo-colors-script');
      ensureScript('title-truncate.js', 'title-truncate-script');
      ensureScript('share-mejoras.js', 'share-mejoras-script');
      return loadPartial('site-footer', 'partials/footer.html');
    })
    .then(function () {
      document.dispatchEvent(new CustomEvent('shell:ready', { detail: { page: page } }));
    });
})();
