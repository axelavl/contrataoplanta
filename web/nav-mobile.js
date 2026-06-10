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
    '  display: block; padding: 12px 22px;',
    '  font-size: 14.5px; color: var(--text-secondary, #C2CADE);',
    '  text-decoration: none; font-weight: 500;',
    '  border-bottom: 1px solid rgba(255,255,255,0.06);',
    '  transition: background .15s, color .15s, transform .15s;',
    '}',
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
        { href: 'index.html', text: 'Buscar empleos', cls: '', id: '' },
        { href: 'favoritos.html', text: '♡ Mis favoritos', cls: 'nav-link-favs', id: 'mobile-nav-favoritos' },
        { href: 'estadisticas.html', text: 'Estadísticas', cls: '', id: '' },
        { href: 'faq.html', text: 'Preguntas frecuentes', cls: '', id: '' },
        { href: 'ruta-ingreso-empleo-publico.html', text: 'Ruta de ingreso', cls: '', id: '' }
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
      a.textContent = l.text;
      if (l.cls) a.className = l.cls.trim();
      if (l.id) a.id = l.id;
      panel.appendChild(a);
    });

    // Contador de favoritos dentro del panel.
    try {
      var favCount = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]').length;
      if (favCount > 0) {
        var mobileFav = panel.querySelector('#mobile-nav-favoritos');
        if (mobileFav) mobileFav.textContent = '♡ Mis favoritos (' + favCount + ')';
      }
    } catch (e) { /* localStorage bloqueado */ }

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
        if (navFav) navFav.textContent = '♡ Mis favoritos (' + favCount + ')';
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
