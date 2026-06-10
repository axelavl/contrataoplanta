/* logo-fallback.js — legacy shim.
   La lógica real (sources de logos, chequeo de calidad y fallback al
   ícono SVG por sector) vive en `shared-shell.js`, que la registra
   sincrónicamente antes del primer render para evitar races con los
   eventos `onerror` de los <img>. Este archivo permanece como shim
   para no romper referencias antiguas vía `ensureScript`.
*/
/* eslint-disable no-empty */
(function () {
  'use strict';
  // Defensiva: si por alguna razón shared-shell.js no corrió, proveemos
  // una versión mínima que al menos reemplaza la img por un div neutro.
  if (typeof window.imgFavFallback !== 'function') {
    window.imgFavFallback = function (img) {
      try { if (img && img.parentNode) img.replaceWith(document.createElement('div')); } catch {}
    };
  }
  if (typeof window.imgFavCheckQuality !== 'function') {
    window.imgFavCheckQuality = function () {};
  }
})();
