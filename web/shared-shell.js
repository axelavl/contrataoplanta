(function () {
  'use strict';

  /* ── Analítica del sitio · carga única ──
     shared-shell.js lo cargan ~todas las páginas públicas, así que es el
     punto ideal para activar la analítica (Umami + beacon propio) en todo
     el sitio sin tocar 28 HTML. analytics.js trae su propio guard
     anti-doble-arranque; aquí solo evitamos duplicar el <script> cuando la
     página ya lo incluye directo (p. ej. index.html). */
  (function cargarAnalitica() {
    if (document.querySelector('script[src="/analytics.js"]')) return;
    var s = document.createElement('script');
    s.src = '/analytics.js';
    s.defer = true;
    document.head.appendChild(s);
  })();

  /* ── Ruta canónica de una oferta · fuente única de verdad ──
     /oferta/{id}-{slug-del-cargo}. El slug DEBE coincidir con `_slugify` del
     backend (api/services/formatters.py) para que el enlace que compartimos ya
     sea el CANÓNICO y no dispare el redirect 301 del SSR. Lo usan app.js,
     favoritos.js e historial.js (todos cargan shared-shell.js antes). */
  window.slugificarCargo = function (texto) {
    if (!texto) return '';
    return String(texto)
      .normalize('NFKD').replace(/[^\x00-\x7F]/g, '')   // NFKD + descarta no-ASCII (= encode('ascii','ignore'))
      .replace(/[^a-zA-Z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .toLowerCase()
      .slice(0, 80)
      .replace(/-+$/, '');
  };
  window.rutaOferta = function (id, cargo) {
    var slug = window.slugificarCargo(cargo);
    return '/oferta/' + id + (slug ? '-' + slug : '');
  };

  /* ── Logo fallback robusto · fuente única de verdad ──
     Se registra ANTES de que se carguen los partials y las cards, para
     que el override de `window.imgFavFallback` y `window.imgFavCheckQuality`
     esté activo desde el primer image error.

     Cadena de resolución (primero a último):
       0. Logo local pre-cacheado en /logos/{dominio}.png (CDN, instantáneo)
       1. DuckDuckGo ip3 (mejor icono del sitio, 60-180 px)
       2. Google s2 favicons @ sz=128
       3. apple-touch-icon del dominio (suele ser 180 px+)
       4. apple-touch-icon-precomposed
       5. favicon del dominio raíz (suele ser 32 px o menos)
     Si ninguna supera el chequeo de calidad (naturalWidth >= 40 px), se
     reemplaza la <img> por un ícono SVG según sector institucional — nunca
     quedamos con recuadros vacíos ni con emojis.
     Logos locales se generan con scripts/descargar_logos.py. */
  (function registerLogoFallback() {
    var MIN_LOGO_PX = 16; // Bajamos umbral: 32×32 se ve bien a 44px; 16×16 es mejor que nada.

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
      if (/municipalidad|municipal|muni\.|corporaci[óo]n\s+(?:de\s+(?:desarrollo|educaci|salud)|municipal)/.test(n)) return 'municipal';
      if (/hospital|salud|clinic|consultori|cesfam|servicio\s+de\s+salud|cenabast|seremi\s+de\s+salud/.test(n)) return 'salud';
      if (/universidad|instituto\s+profesional|centro\s+de\s+formaci|cft\s+estatal/.test(n)) return 'universidad';
      if (/colegio|escuela|liceo|educaci|servicio\s+local\s+de\s+educaci/.test(n)) return 'educacion';
      if (/poder\s+judicial|juzgado|corte|fiscal|tribunal|ministerio\s+p[úu]blico|fiscal[íi]a|defensor[íi]a|registro\s+civil/.test(n)) return 'judicial';
      if (/fuerzas|armada|ej[ée]rcito|carabineros|pdi|gendarmer|bomberos|fuerza\s+a[ée]rea|famae|asmar|enaer|emco/.test(n)) return 'ffaa';
      if (/gobierno\s+regional|intendencia|gore|delegaci[óo]n\s+presidencial/.test(n)) return 'regional';
      if (/empresa|banco|metro|tvn|codelco|enap|enami|correos|portuari|polla|zofri/.test(n)) return 'empresa';
      if (/ministerio|subsecretar|superintendencia|servicio\s+(?:de|nacional|civil)|direcci[óo]n|agencia|comisi[óo]n|consejo|contralor|secretar[íi]a\s+general/.test(n)) return 'ejecutivo';
      return 'default';
    }

    function domainFromImg(img) {
      if (img.dataset.domain) return img.dataset.domain;
      var src = img.src || '';
      var m =
        src.match(/\/logos\/([^/]+)\.png/) ||
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
        '/logos/' + domain + '.png',
        'https://icons.duckduckgo.com/ip3/' + domain + '.ico',
        'https://www.google.com/s2/favicons?domain=' + enc + '&sz=128',
        'https://t1.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&fallback_opts=TYPE,SIZE,URL&url=https://' + enc + '&size=128',
        'https://' + domain + '/apple-touch-icon.png',
        'https://www.' + domain + '/apple-touch-icon.png',
        'https://' + domain + '/apple-touch-icon-precomposed.png',
        'https://' + domain + '/favicon.ico',
        'https://www.' + domain + '/favicon.ico'
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
      if (Math.max(w, h) < MIN_LOGO_PX) {
        // Permitimos reintentar con el siguiente source.
        img.dataset.qualityChecked = '';
        advance(img);
      } else {
        // Logo bueno: lo memorizamos para esta institución. Así, en la
        // próxima carga, lo usamos directo (sin reintentar la cadena
        // Clearbit→apple→Google) y evitamos el parpadeo "aparece y
        // desaparece". Es un repositorio propio incremental en el cliente.
        cacheSet(domainFromImg(img), img.currentSrc || img.src);
      }
    };

    // ── Caché de logos resueltos (repositorio propio incremental) ──────
    // Persistimos en localStorage el último source que SÍ pasó el chequeo de
    // calidad por dominio. `window.__logoCacheGet` lo consume app.js para
    // pintar el logo correcto desde el primer frame.
    // v3: invalida cachés previos — umbral de calidad reducido + nuevas fuentes.
    var LOGO_CACHE_KEY = 'cop_logo_cache_v3';
    function cacheRead() {
      try { return JSON.parse(localStorage.getItem(LOGO_CACHE_KEY) || '{}') || {}; }
      catch (e) { return {}; }
    }
    function cacheGet(domain) {
      if (!domain) return null;
      var c = cacheRead();
      return c[domain] || null;
    }
    function cacheSet(domain, src) {
      if (!domain || !src) return;
      // No cacheamos el ícono genérico (data-URI / svg inline) ni vacíos.
      if (src.indexOf('data:') === 0) return;
      try {
        var c = cacheRead();
        if (c[domain] === src) return;
        c[domain] = src;
        localStorage.setItem(LOGO_CACHE_KEY, JSON.stringify(c));
      } catch (e) { /* storage lleno/bloqueado: seguimos sin caché */ }
    }
    window.__logoCacheGet = cacheGet;

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
      calculadora: 'calculadora', preparacion: 'preparacion', cursos: 'cursos', panel: 'none',
      historial: 'home', terminos: 'none', privacidad: 'none', descargo: 'none', ruta: 'preparacion'
    };
    var target = map[page] || 'none';
    if (target === 'none') return;
    var el = root.querySelector('[data-nav="' + target + '"]');
    if (el) {
      el.classList.add('active');
      el.setAttribute('aria-current', 'page'); // 3.3/3.6: "you are here" accesible
    }
  }

  function updateFavCount(root) {
    try {
      var favs = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]');
      var navFav = root.querySelector('#nav-favoritos');
      if (!navFav) return;
      // El texto base ("♡ Mis favoritos") siempre se conserva; el conteo va en
      // un badge separado, igual que en app.js/nav-mobile.js. Antes esto sólo
      // corría cuando había favoritos y reescribía todo el texto con formato
      // "(N)", dejando inconsistencia entre páginas y rompiendo cuando el
      // contador estaba en cero.
      var n = Array.isArray(favs) ? favs.length : 0;
      navFav.innerHTML = '♡ Mis favoritos' +
        (n > 0 ? ' <span class="nav-fav-badge">' + n + '</span>' : '');
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

  // Inserta una hoja de estilo si no está ya presente (por id o por href).
  // Idempotente: en el home, donde el <link> ya está en el HTML, no duplica.
  function ensureStylesheet(href, id) {
    if (document.getElementById(id)) return;
    var exists = Array.prototype.some.call(
      document.querySelectorAll('link[rel="stylesheet"]'),
      function (l) { return (l.getAttribute('href') || '').indexOf(href) !== -1; }
    );
    if (exists) return;
    var link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = href;
    link.id = id;
    document.head.appendChild(link);
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

  // Banner de aviso / modo mantención / footer extra, editables desde el panel
  // admin (site_config). El home ya lo resuelve en app.js; acá cubrimos TODAS
  // las demás páginas (que no cargan app.js). Idempotente vía ids.
  function renderSiteConfig() {
    var hasApp = Array.prototype.some.call(document.scripts, function (s) {
      return /(^|\/)app\.js($|[?#])/.test(s.getAttribute('src') || '');
    });
    if (hasApp) return; // en el home lo maneja app.js, no duplicamos
    var RAILWAY = 'https://contrataoplanta-production.up.railway.app';
    var _h = location.hostname;
    var API = window.__API_BASE ||
      ((_h === 'localhost' || _h === '127.0.0.1' || _h === '') ? 'http://localhost:8000' : RAILWAY);
    fetch(API + '/api/site-config')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        var conf = (d && d.config) || {};
        if (conf.banner_activo === 'true' && (conf.banner_mensaje || '').trim() &&
            !document.getElementById('site-config-banner')) {
          var b = document.createElement('div');
          b.id = 'site-config-banner';
          b.setAttribute('role', 'status');
          b.style.cssText = 'background:#1d4ed8;color:#fff;text-align:center;padding:8px 14px;font-size:14px;line-height:1.4';
          b.textContent = conf.banner_mensaje.trim();
          document.body.insertBefore(b, document.body.firstChild);
        }
        if (conf.mantenimiento === 'true' && !document.getElementById('site-config-mantenimiento')) {
          var a = document.createElement('div');
          a.id = 'site-config-mantenimiento';
          a.setAttribute('role', 'alert');
          a.style.cssText = 'background:#b45309;color:#fff;text-align:center;padding:8px 14px;font-size:14px;font-weight:600';
          a.textContent = '⚠️ Sitio en mantención — algunos datos pueden estar desactualizados.';
          document.body.insertBefore(a, document.body.firstChild);
        }
        if ((conf.footer_extra || '').trim() && !document.getElementById('site-config-footer-extra')) {
          var foot = document.querySelector('footer');
          if (foot) {
            var ex = document.createElement('div');
            ex.id = 'site-config-footer-extra';
            ex.style.cssText = 'text-align:center;padding:10px 14px;font-size:13px;color:#64748b';
            ex.innerHTML = conf.footer_extra;
            foot.parentNode.insertBefore(ex, foot.nextSibling);
          }
        }
      })
      .catch(function () { /* sin conexión: el sitio sigue normal */ });
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
      // Ajustes de UX móvil + modo oscuro (costura cinta/header, contraste de
      // títulos de la ficha, acordeón). En el home ya van en el HTML; acá los
      // propagamos a TODAS las demás páginas que usan el shell. Idempotente.
      ensureStylesheet('styles/mobile-ux-fixes.css', 'mobile-ux-fixes-style');
      ensureScript('mobile-ux.js', 'mobile-ux-script');
      return loadPartial('site-footer', 'partials/footer.html');
    })
    .then(function () {
      renderSiteConfig();
      document.dispatchEvent(new CustomEvent('shell:ready', { detail: { page: page } }));
    });
})();
