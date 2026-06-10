/* theme-toggle.js — switch light/dark con persistencia */
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

  try {
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function (e) {
      if (!localStorage.getItem('theme')) {
        html.setAttribute('data-theme', e.matches ? 'dark' : 'light');
        syncButtons();
      }
    });
  } catch (e) {}

  document.addEventListener('shell:ready', init);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
