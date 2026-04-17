/**
 * nav-mobile.js — Shared mobile navigation for secondary pages
 * Include before </body>: <script src="nav-mobile.js"></script>
 *
 * Provides:
 *  - Hamburger menu button with smooth slide-in panel
 *  - Close on Escape key, overlay tap, or link click
 *  - Logo href fix → index.html
 *  - Favorites counter in mobile menu
 *  - Mobile CSS fixes (overflow, grids)
 */
(function () {
  'use strict';

  if (window.__mobileNavInitialized) return;

  document.documentElement.classList.add('js-nav');

  function initMobileNav() {

  // ── 1. Inject mobile CSS ──────────────────────────────────────────
  var css = [
    'html, body { overflow-x: hidden; }',
    '.skip-link { position:absolute; left:12px; top:-48px; z-index:1000; background:#0A2E6E; color:#FAFAF8; padding:10px 14px; border-radius:8px; text-decoration:none; font-size:13px; font-weight:600; transition:top .2s; }',
    '.skip-link:focus { top:10px; }',

    /* Hamburger button */
    '.hamburger { display:none; background:none; border:none; cursor:pointer;',
    '  width:36px; height:36px; flex-direction:column; align-items:center;',
    '  justify-content:center; gap:5px; padding:0; margin-left:auto; }',
    '.hamburger span { display:block; width:22px; height:2px; background:white;',
    '  border-radius:2px; transition:transform .3s, opacity .3s; }',
    '.hamburger.abierto span:nth-child(1) { transform:translateY(7px) rotate(45deg); }',
    '.hamburger.abierto span:nth-child(2) { opacity:0; }',
    '.hamburger.abierto span:nth-child(3) { transform:translateY(-7px) rotate(-45deg); }',

    /* Overlay */
    '.nav-mobile-overlay { display:none; position:fixed; inset:0; top:56px;',
    '  background:rgba(0,0,0,0.5); z-index:98; opacity:0; transition:opacity .3s; }',
    '.nav-mobile-overlay.visible { opacity:1; }',

    /* Panel */
    '.nav-mobile-panel { display:none; position:fixed; top:56px; right:0;',
    '  width:280px; max-width:85vw; height:calc(100vh - 56px); height:calc(100dvh - 56px);',
    '  background:#0A2E6E; z-index:99; flex-direction:column; padding:16px 0;',
    '  overflow-y:auto; transform:translateX(100%);',
    '  transition:transform .3s cubic-bezier(.4,0,.2,1); }',
    '.nav-mobile-panel.visible { transform:translateX(0); }',
    '.nav-mobile-panel a { display:block; padding:14px 24px; font-size:15px;',
    '  color:rgba(255,255,255,0.7); text-decoration:none; font-weight:500;',
    '  transition:background .15s,color .15s;',
    '  border-bottom:1px solid rgba(255,255,255,0.06); }',
    '.nav-mobile-panel a:hover, .nav-mobile-panel a.active {',
    '  background:rgba(255,255,255,0.1); color:white; }',
    '.nav-mobile-panel .nav-link-favs { color:#E8A820 !important; }',

    /* Show only on mobile */
    '@media (max-width:600px) {',
    '  .js-nav .hamburger { display:flex; }',
    '  .js-nav .nav-mobile-overlay, .js-nav .nav-mobile-panel { display:flex; }',
    '  .js-nav .nav-links { display:none !important; }',
    '  .footer-inner { grid-template-columns:1fr !important; gap:24px !important; }',
    '  .footer-bottom { flex-direction:column; gap:4px; text-align:center; }',
    '}',
    '@media (max-width:900px) {',
    '  .footer-inner { grid-template-columns:1fr 1fr; }',
    '}'
  ].join('\n');

  var style = document.createElement('style');
  style.textContent = css;
  document.head.appendChild(style);

  // ── 2. Fix logo links → index.html ────────────────────────────────
  var logos = document.querySelectorAll('a.logo');
  logos.forEach(function (a) {
    if (a.getAttribute('href') !== 'index.html') {
      a.setAttribute('href', 'index.html');
    }
  });

  // ── 3. Detect current page for active state ───────────────────────
  var path = location.pathname.split('/').pop() || 'index.html';

  var links = [
    { href: 'index.html',          text: 'Buscar empleos' },
    { href: 'favoritos.html',      text: '\u2661 Mis favoritos', cls: 'nav-link-favs', id: 'nav-mobile-favoritos' },
    { href: 'index.html#fuentes-activas',  text: 'Fuentes activas' },
    { href: 'estadisticas.html',   text: 'Estad\u00edsticas' },
    { href: 'faq.html',            text: 'Preguntas frecuentes' }
  ];

  // ── 4. Build hamburger button ─────────────────────────────────────
  var navInner = document.querySelector('.nav-inner');
  if (!navInner) return false;

  var btn = document.createElement('button');
  btn.className = 'hamburger';
  btn.id = 'hamburger-btn';
  btn.setAttribute('aria-label', 'Abrir men\u00fa');
  btn.setAttribute('aria-expanded', 'false');
  btn.setAttribute('aria-controls', 'nav-mobile-panel');
  btn.innerHTML = '<span></span><span></span><span></span>';
  navInner.appendChild(btn);

  // ── 5. Build overlay & panel ──────────────────────────────────────
  var overlay = document.createElement('div');
  overlay.className = 'nav-mobile-overlay';
  overlay.id = 'nav-mobile-overlay';

  var panel = document.createElement('div');
  panel.className = 'nav-mobile-panel';
  panel.id = 'nav-mobile-panel';

  links.forEach(function (l) {
    var a = document.createElement('a');
    a.href = l.href;
    a.textContent = l.text;
    if (l.cls) a.className = l.cls;
    if (l.id) a.id = l.id;
    if (l.href === path) a.classList.add('active');
    panel.appendChild(a);
  });

  // Insert after nav
  var nav = document.querySelector('nav');
  if (nav && nav.parentNode) {
    nav.parentNode.insertBefore(overlay, nav.nextSibling);
    nav.parentNode.insertBefore(panel, overlay.nextSibling);
  }

  // ── 6. Open / close logic ─────────────────────────────────────────
  function abrirMenu() {
    btn.classList.add('abierto');
    btn.setAttribute('aria-expanded', 'true');
    btn.setAttribute('aria-label', 'Cerrar menú');
    panel.classList.add('visible');
    overlay.classList.add('visible');
    document.body.style.overflow = 'hidden';
    var firstLink = panel.querySelector('a');
    if (firstLink) firstLink.focus();
  }
  function cerrarMenu(devolverFoco) {
    btn.classList.remove('abierto');
    btn.setAttribute('aria-expanded', 'false');
    btn.setAttribute('aria-label', 'Abrir menú');
    panel.classList.remove('visible');
    overlay.classList.remove('visible');
    document.body.style.overflow = '';
    if (devolverFoco) btn.focus();
  }

  btn.addEventListener('click', function () {
    btn.classList.contains('abierto') ? cerrarMenu() : abrirMenu();
  });
  overlay.addEventListener('click', function () { cerrarMenu(true); });
  panel.querySelectorAll('a').forEach(function (a) {
    a.addEventListener('click', function () { cerrarMenu(false); });
  });
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && btn.classList.contains('abierto')) cerrarMenu(true);
  });

  // ── 7. Favorites counter ──────────────────────────────────────────
  try {
    var favCount = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]').length;
    if (favCount > 0) {
      var mobileFav = document.getElementById('nav-mobile-favoritos');
      if (mobileFav) mobileFav.textContent = '\u2661 Mis favoritos (' + favCount + ')';
      var navFav = document.getElementById('nav-favoritos');
      if (navFav) navFav.textContent = '\u2661 Mis favoritos (' + favCount + ')';
    }
  } catch (e) { /* localStorage unavailable */ }
  window.__mobileNavInitialized = true;
  return true;
}

if (!initMobileNav()) {
  document.addEventListener('shell:ready', function(){ initMobileNav(); }, { once: true });
}
})();
