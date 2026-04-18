/* ══════════════════════════════════════════════════════════════════════
   theme-toggle.js
   Gestiona el toggle light/dark en el header. Se ejecuta después de que
   shared-shell.js haya cargado el partial del header (evento shell:ready).
   El tema inicial se aplica con el script inline en el <head> de cada
   página (ver anti-flash snippet en APPLY.md).
   ══════════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  var html = document.documentElement;

  function syncButtons() {
    var light = document.getElementById('light-btn');
    var dark = document.getElementById('dark-btn');
    if (!light || !dark) return;
    var t = html.getAttribute('data-theme') || 'light';
    light.classList.toggle('is-active', t === 'light');
    dark.classList.toggle('is-active', t === 'dark');
    light.setAttribute('aria-pressed', t === 'light');
    dark.setAttribute('aria-pressed', t === 'dark');
  }

  function setTheme(theme) {
    html.setAttribute('data-theme', theme);
    try { localStorage.setItem('theme', theme); } catch (e) {}
    syncButtons();
  }

  function init() {
    var light = document.getElementById('light-btn');
    var dark = document.getElementById('dark-btn');
    if (!light || !dark) return;
    light.addEventListener('click', function () { setTheme('light'); });
    dark.addEventListener('click', function () { setTheme('dark'); });
    syncButtons();
  }

  // Seguir el sistema si el usuario no ha fijado preferencia
  try {
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function (e) {
      if (!localStorage.getItem('theme')) {
        html.setAttribute('data-theme', e.matches ? 'dark' : 'light');
        syncButtons();
      }
    });
  } catch (e) {}

  // Integración con shared-shell.js — init cuando el header ya está en DOM
  document.addEventListener('shell:ready', init);

  // Fallback: también intentar en DOMContentLoaded por si shared-shell no existe
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
