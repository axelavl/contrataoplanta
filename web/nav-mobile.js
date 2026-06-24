/**
 * nav-mobile.js — Navegación móvil unificada para todas las páginas.
 *
 * Responsabilidades:
 *   - Inyectar CSS base del menú móvil (hamburguesa, overlay, panel).
 *   - Montar el panel y overlay DIRECTAMENTE en `document.body`, fuera del
 *     stacking context del <header> sticky. Esto garantiza que
 *     `position: fixed` del panel y del overlay se ancle al viewport sin
 *     interferencia de `transform`, `filter`, `backdrop-filter` o
 *     `z-index` del header.
 *   - Un SOLO listener delegado en `document`. No depende de que el
 *     botón exista al momento de cargar el script — funciona aunque el
 *     partial del header se inyecte después (el caso real en este sitio,
 *     donde `shared-shell.js` lo trae vía fetch).
 *   - Cierre por Escape, tap en overlay, tap en link.
 *   - Fija href del logo → index.html y repinta el contador de favoritos.
 *
 * Carga:
 *   <script defer src="nav-mobile.js"></script>
 * El timing `defer` es suficiente — el script sólo necesita `document.body`
 * listo para montar el panel.
 */
(function () {
  'use strict';

  if (window.__mobileNavBooted) return;
  window.__mobileNavBooted = true;

  // `js-nav` se agrega siempre al <html> para que las reglas CSS que lo
  // usan como gate funcionen (histórico del codebase). No bloquea nada.
  document.documentElement.classList.add('js-nav');

  // ── 1. CSS baseline ───────────────────────────────────────────────
  // Inyectamos el CSS del menú móvil como medida de seguridad (index.css
  // ya lo define, pero si por algún motivo no carga, el menú sigue
  // usable). z-index alto (10050 overlay, 10060 panel) para escapar de
  // cualquier stacking context del resto del layout.
  var css = [
    'html.menu-abierto, body.menu-abierto { overflow: hidden; }',

    /* `display: block` explícito gana al `display: none` baseline que
       existe en index.css para el mismo selector. Sin esto, el overlay
       nunca se pinta aunque pongamos `.visible`. */
    '.nav-mobile-overlay {',
    '  display: block;',
    '  position: fixed; inset: 0;',
    '  background: var(--overlay, rgba(4, 10, 20, 0.6));',
    '  backdrop-filter: blur(2px);',
    '  -webkit-backdrop-filter: blur(2px);',
    '  z-index: 10050;',
    '  opacity: 0; pointer-events: none;',
    '  transition: opacity .24s ease;',
    '}',
    '.nav-mobile-overlay.visible { opacity: 1; pointer-events: auto; }',

    '.nav-mobile-panel {',
    '  position: fixed; top: 0; right: 0;',
    '  width: 280px; max-width: 85vw;',
    '  height: 100vh; height: 100dvh;',
    '  background: linear-gradient(180deg, var(--surface-elevated, #193156) 0%, var(--surface-base, #12233F) 100%);',
    '  color: var(--text-primary, #F3F0E6);',
    '  border-left: 1px solid var(--border-subtle, #27436D);',
    '  box-shadow: -22px 0 48px rgba(0,0,0,0.45), inset 0 1px 0 rgba(255,255,255,0.03);',
    '  z-index: 10060;',
    '  display: flex; flex-direction: column;',
    '  padding: 76px 0 20px;',
    '  overflow-y: auto;',
    '  transform: translateX(100%);',
    '  pointer-events: none;',
    '  transition: transform .28s cubic-bezier(.4, 0, .2, 1), box-shadow .2s ease;',
    '  -webkit-overflow-scrolling: touch;',
    '  overscroll-behavior: contain;',
    '}',
    '.nav-mobile-panel.visible { transform: translateX(0); pointer-events: auto; }',

    '.nav-mobile-panel a {',
    '  display: flex; align-items: center; gap: 12px; padding: 12px 22px;',
    '  font-size: 14.5px; color: var(--text-secondary, #C2CADE);',
    '  text-decoration: none; font-weight: 500;',
    '  border-bottom: 1px solid rgba(255,255,255,0.06);',
    '  transition: background .15s, color .15s, transform .15s;',
    '}',
    '.nav-mobile-panel a svg { width: 18px; height: 18px; flex-shrink: 0; opacity: .85; }',
    '.nav-mobile-panel a:hover,',
    '.nav-mobile-panel a:focus,',
    '.nav-mobile-panel a.active {',
    '  background: rgba(255,255,255,0.08); color: var(--text-primary, #fff);',
    '  transform: translateX(-1px);',
    '}',
    '.nav-mobile-panel a:active { background: rgba(255,255,255,0.13); transform: translateX(-2px); }',
    '.nav-mobile-panel a:focus-visible {',
    '  outline: 2px solid var(--accent, #EAB858);',
    '  outline-offset: -2px;',
    '}',
    '.nav-mobile-panel .nav-link-favs { color: #E8A820 !important; }',

    '.nm-foot { margin-top: auto; padding: 16px 22px 6px; border-top: 1px solid rgba(255,255,255,0.08); }',
    '.nm-theme { display: flex; gap: 8px; margin-bottom: 14px; }',
    '.nm-theme button { flex: 1; display: flex; align-items: center; justify-content: center; gap: 6px;',
    '  padding: 9px; border-radius: 10px; background: rgba(255,255,255,0.06);',
    '  border: 1px solid rgba(255,255,255,0.12); color: var(--text-secondary, #C2CADE);',
    '  font-size: 12.5px; font-weight: 600; cursor: pointer; font-family: inherit; }',
    '.nm-theme button svg { width: 16px; height: 16px; }',
    '.nm-theme button.on { background: rgba(234,184,88,0.16); border-color: rgba(234,184,88,0.4); color: #EAB858; }',
    '.nm-legal { display: flex; flex-wrap: wrap; gap: 4px 14px; }',
    '.nav-mobile-panel .nm-legal a { display: inline; padding: 2px 0; border-bottom: none; font-size: 12px; color: var(--text-secondary, #9AA3B8); }',
    '.nav-mobile-panel .nm-legal a:hover { color: #fff; transform: none; background: none; }',

    '.nav-mobile-panel-close {',
    '  position: absolute; top: 12px; right: 12px;',
    '  width: 36px; height: 36px;',
    '  background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.12);',
    '  border-radius: 10px; color: var(--text-primary, #fff); cursor: pointer;',
    '  font-size: 20px; line-height: 1;',
    '  display: flex; align-items: center; justify-content: center;',
    '  transition: background .15s ease, border-color .15s ease, color .15s ease;',
    '}',
    '.nav-mobile-panel-close:hover { background: rgba(255,255,255,0.12); border-color: rgba(255,255,255,0.24); }',
    '.nav-mobile-panel-close:active { background: rgba(255,255,255,0.18); }',
    '.nav-mobile-panel-close:focus-visible { outline: 2px solid var(--accent, #EAB858); outline-offset: 1px; }',

    /* Baseline: en desktop la hamburguesa SIEMPRE oculta. Necesario porque
       las páginas que cargan shared-layout.css (favoritos, faq, estadísticas,
       ruta, historial) no definen `.hamburger { display:none }` como sí hace
       index.css — sin esto el <button> aparecía en desktop. */
    '.hamburger { display: none; }',

    /* Las 3 barras del ícono. SOLO index.css las definía (.hamburger span),
       así que en las páginas internas el botón salía como un cuadro vacío.
       Las inyectamos acá para que el ícono se dibuje en todas las páginas. */
    '.hamburger { background: none; border: none; cursor: pointer; width: 36px; height: 36px; flex-direction: column; align-items: center; justify-content: center; gap: 5px; padding: 0; margin-left: auto; }',
    '.hamburger span { display: block; width: 22px; height: 2px; background: #fff; border-radius: 2px; transition: transform .3s, opacity .3s; }',
    '.hamburger.abierto span:nth-child(1) { transform: translateY(7px) rotate(45deg); }',
    '.hamburger.abierto span:nth-child(2) { opacity: 0; }',
    '.hamburger.abierto span:nth-child(3) { transform: translateY(-7px) rotate(-45deg); }',

    /* En móvil forzamos hamburguesa visible y nav-links oculto. Esto
       duplica parte de redesign-overrides pero garantiza que el menú
       opere aunque ese stylesheet no haya cargado. */
    '@media (max-width: 600px) {',
    '  .hamburger { display: flex !important; }',
    '  .nav-links { display: none !important; }',
    '}'
  ].join('\n');

  var style = document.createElement('style');
  style.setAttribute('data-source', 'nav-mobile');
  style.textContent = css;
  document.head.appendChild(style);

  // ── 2. Estado + helpers ───────────────────────────────────────────
  var panel = null;
  var overlay = null;
  var mounted = false;

  function currentBtn() {
    return document.getElementById('hamburger-btn') ||
           document.querySelector('.hamburger');
  }

  function normalizeHref(href) {
    if (!href) return '';
    var a = document.createElement('a');
    a.href = href;
    var p = (a.pathname.split('/').pop() || 'index.html');
    return p + (a.hash || '');
  }

  function buildPanelLinks() {
    // Replica los links del menú desktop. Si todavía no hay `.nav-links`
    // (partial no cargado), cae a un set mínimo hardcoded.
    var desktop = document.querySelectorAll('.nav-links a');
    var data;
    if (desktop.length) {
      data = Array.prototype.map.call(desktop, function (a) {
        return {
          href: a.getAttribute('href') || '',
          text: (a.textContent || '').trim(),
          cls: a.className || '',
          id: a.id ? ('mobile-' + a.id) : ''
        };
      }).filter(function (l) { return !!l.href; });
    } else {
      data = [
        { href: 'index.html', text: 'Buscar', cls: '', id: '' },
        { href: 'favoritos.html', text: '♡ Mis favoritos', cls: 'nav-link-favs', id: 'mobile-nav-favoritos' },
        { href: 'preparacion.html', text: 'Preparación', cls: '', id: '' },
        { href: 'cursos.html', text: 'Capacítate', cls: '', id: '' },
        { href: 'faq.html', text: 'Preguntas frecuentes', cls: '', id: '' }
      ];
    }

    var currentPath = location.pathname.split('/').pop() || 'index.html';
    var currentHash = location.hash || '';
    return data.map(function (l) {
      var target = normalizeHref(l.href);
      var isActive = target === currentPath || target === (currentPath + currentHash);
      return {
        href: l.href,
        text: l.text,
        cls: (l.cls || '') + (isActive ? ' active' : ''),
        id: l.id
      };
    });
  }

  // Íconos por ítem (Parte 3.2). SVG inline para no depender de fuentes/CSP.
  var _SVG_P = 'fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"';
  function _svg(inner) { return '<svg viewBox="0 0 24 24" ' + _SVG_P + ' aria-hidden="true">' + inner + '</svg>'; }
  var _ICON_SUN = _svg('<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M6.3 17.7l-1.4 1.4M19.1 4.9l-1.4 1.4"/>');
  var _ICON_MOON = _svg('<path d="M21 12.8A9 9 0 1 1 11.2 3 7 7 0 0 0 21 12.8z"/>');
  function iconoFor(href) {
    var f = String(href || '').split('/').pop().split('#')[0].split('?')[0];
    if (f === '' || f === 'index.html') return _svg('<circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/>');
    if (f === 'favoritos.html') return _svg('<path d="M20.8 4.6a5.5 5.5 0 0 0-7.8 0L12 5.6l-1-1a5.5 5.5 0 1 0-7.8 7.8L12 21l8.8-8.6a5.5 5.5 0 0 0 0-7.8z"/>');
    if (f === 'preparacion.html') return _svg('<path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/>');
    if (f === 'cursos.html') return _svg('<path d="M22 10 12 5 2 10l10 5 10-5z"/><path d="M6 12v5c0 1 2.7 2 6 2s6-1 6-2v-5"/>');
    if (f === 'faq.html') return _svg('<circle cx="12" cy="12" r="9"/><path d="M9.1 9a3 3 0 0 1 5.8 1c0 2-3 2.5-3 4"/><path d="M12 17h.01"/>');
    if (f === 'calculadora-renta.html') return _svg('<rect x="4" y="2" width="16" height="20" rx="2"/><path d="M8 6h8M8 18h8M8 11h.01M12 11h.01M16 11h.01M8 14.5h.01M12 14.5h.01"/>');
    return _svg('<circle cx="12" cy="12" r="2.5"/>');
  }

  function ensureMounted() {
    if (mounted && panel && panel.isConnected && overlay && overlay.isConnected) {
      return;
    }

    overlay = document.getElementById('nav-mobile-overlay');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'nav-mobile-overlay';
      overlay.className = 'nav-mobile-overlay';
      document.body.appendChild(overlay);
    }

    panel = document.getElementById('nav-mobile-panel');
    if (!panel) {
      panel = document.createElement('nav');
      panel.id = 'nav-mobile-panel';
      panel.className = 'nav-mobile-panel';
      panel.setAttribute('aria-label', 'Menú principal');
      document.body.appendChild(panel);
    }

    // Botón de cierre explícito en el panel (redundante con overlay/esc
    // pero mejora descubribilidad en pantallas muy angostas).
    panel.innerHTML = '';
    var closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'nav-mobile-panel-close';
    closeBtn.setAttribute('aria-label', 'Cerrar menú');
    closeBtn.textContent = '✕';
    panel.appendChild(closeBtn);

    buildPanelLinks().forEach(function (l) {
      var a = document.createElement('a');
      a.href = l.href;
      if (l.cls) a.className = l.cls.trim();
      if (l.id) a.id = l.id;
      // Limpia el ♡/emoji inicial que arrastra el texto del nav desktop: ahora
      // el ícono lo aporta el SVG, uniforme para todos los ítems (Parte 3.2).
      var txt = (l.text || '').replace(/^[^0-9A-Za-zÁÉÍÓÚÑáéíóúñ]+/, '').trim();
      a.innerHTML = iconoFor(l.href) + '<span class="nm-txt"></span>';
      a.querySelector('.nm-txt').textContent = txt;
      panel.appendChild(a);
    });

    // Contador de favoritos dentro del panel.
    try {
      var favCount = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]').length;
      if (favCount > 0) {
        var mobileFav = panel.querySelector('#mobile-nav-favoritos .nm-txt');
        if (mobileFav) mobileFav.textContent = 'Mis favoritos (' + favCount + ')';
      }
    } catch (e) { /* localStorage bloqueado */ }

    // Pie del drawer (Parte 3.1): toggle claro/oscuro + enlaces legales tras
    // divisoria, aprovechando el espacio inferior.
    var foot = document.createElement('div');
    foot.className = 'nm-foot';
    foot.innerHTML =
      '<div class="nm-theme">' +
        '<button type="button" data-tema="light" aria-label="Tema claro">' + _ICON_SUN + 'Claro</button>' +
        '<button type="button" data-tema="dark" aria-label="Tema oscuro">' + _ICON_MOON + 'Oscuro</button>' +
      '</div>' +
      '<div class="nm-legal">' +
        '<a href="terminos.html">Términos</a>' +
        '<a href="privacidad.html">Privacidad</a>' +
        '<a href="descargo.html">Descargo</a>' +
      '</div>';
    panel.appendChild(foot);

    var _html = document.documentElement;
    function _marcaTema() {
      var cur = _html.getAttribute('data-theme') || 'light';
      Array.prototype.forEach.call(foot.querySelectorAll('[data-tema]'), function (b) {
        b.classList.toggle('on', b.getAttribute('data-tema') === cur);
      });
    }
    Array.prototype.forEach.call(foot.querySelectorAll('[data-tema]'), function (b) {
      b.addEventListener('click', function () {
        var t = b.getAttribute('data-tema');
        // Reusa los botones del header (theme-toggle.js) si están; si no, aplica directo.
        var hb = document.getElementById(t === 'light' ? 'light-btn' : 'dark-btn');
        if (hb) { hb.click(); }
        else { _html.setAttribute('data-theme', t); try { localStorage.setItem('theme', t); } catch (e) {} }
        _marcaTema();
      });
    });
    _marcaTema();

    mounted = true;
  }

  function abrirMenu() {
    ensureMounted();
    var btn = currentBtn();
    if (btn) {
      btn.classList.add('abierto');
      btn.setAttribute('aria-expanded', 'true');
      btn.setAttribute('aria-label', 'Cerrar menú');
    }
    panel.classList.add('visible');
    overlay.classList.add('visible');
    document.documentElement.classList.add('menu-abierto');
    document.body.classList.add('menu-abierto');

    // Foco al primer link para keyboard users.
    var firstLink = panel.querySelector('a');
    if (firstLink) {
      try { firstLink.focus({ preventScroll: true }); }
      catch (e) { firstLink.focus(); }
    }
  }

  function cerrarMenu(devolverFoco) {
    var btn = currentBtn();
    if (btn) {
      btn.classList.remove('abierto');
      btn.setAttribute('aria-expanded', 'false');
      btn.setAttribute('aria-label', 'Abrir menú');
    }
    if (panel) panel.classList.remove('visible');
    if (overlay) overlay.classList.remove('visible');
    document.documentElement.classList.remove('menu-abierto');
    document.body.classList.remove('menu-abierto');
    if (devolverFoco && btn) {
      try { btn.focus({ preventScroll: true }); }
      catch (e) { btn.focus(); }
    }
  }

  function toggleMenu() {
    var btn = currentBtn();
    var abierto = !!(btn && btn.classList.contains('abierto')) ||
                  !!(panel && panel.classList.contains('visible'));
    abierto ? cerrarMenu() : abrirMenu();
  }

  // ── 3. Listeners delegados en document ────────────────────────────
  // Delegamos en document para que funcione aunque el botón se haya
  // inyectado vía partial DESPUÉS de que corriera este script.
  document.addEventListener('click', function (e) {
    var t = e.target;
    if (!t || !t.closest) return;

    // Tap en hamburguesa → toggle.
    if (t.closest('.hamburger')) {
      e.preventDefault();
      e.stopPropagation();
      toggleMenu();
      return;
    }
    // Tap en overlay → cerrar.
    if (t.closest('#nav-mobile-overlay')) {
      e.preventDefault();
      cerrarMenu(true);
      return;
    }
    // Tap en botón cerrar del panel → cerrar.
    if (t.closest('.nav-mobile-panel-close')) {
      e.preventDefault();
      cerrarMenu(true);
      return;
    }
    // Tap en un link dentro del panel → cerrar (dejamos navegar).
    if (t.closest('#nav-mobile-panel a')) {
      cerrarMenu(false);
      return;
    }
  }, true);

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && panel && panel.classList.contains('visible')) {
      cerrarMenu(true);
    }
  });

  // ── 4. Logo → index.html ──────────────────────────────────────────
  function fixLogo() {
    var logos = document.querySelectorAll('a.logo');
    Array.prototype.forEach.call(logos, function (a) {
      if (a.getAttribute('href') !== 'index.html') {
        a.setAttribute('href', 'index.html');
      }
    });
  }

  // ── 5. Contador de favoritos en nav desktop ──────────────────────
  function fixFavCount() {
    try {
      var favCount = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]').length;
      if (favCount > 0) {
        var navFav = document.getElementById('nav-favoritos');
        // 3.3: el contador va como badge, no como "(N)" inline ni fondo de fila.
        if (navFav) navFav.innerHTML = '♡ Mis favoritos <span class="nav-fav-badge">' + favCount + '</span>';
      }
    } catch (e) { /* noop */ }
  }

  // ── 6. Boot: monta panel y fija logo tan pronto como haya body ───
  // Primer intento: si el partial del header ya está en el DOM lo usamos.
  // Segundo intento: escuchar `shell:ready` que emite shared-shell.js
  // cuando termina de inyectar ribbon/header/footer. Tercer fallback: el
  // listener delegado arriba — si el usuario hace click antes de que
  // montemos, `ensureMounted()` se llama dentro de `abrirMenu()`.
  function boot() {
    ensureMounted();
    fixLogo();
    fixFavCount();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
  document.addEventListener('shell:ready', function () {
    // Repintamos links por si el header recién apareció y cambia el set
    // de destinos (ej. homepage vs. secundaria).
    mounted = false;
    boot();
  });
})();
