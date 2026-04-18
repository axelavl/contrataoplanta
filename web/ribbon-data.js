/* ribbon-data.js — llena el ribbon con datos del API cuando está visible */
(function () {
  'use strict';

  var RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
  // Reusa la base detectada por el sitio si está en window (permite staging).
  function apiBase() {
    if (window.__API_BASE) return window.__API_BASE;
    if (window.API_BASE) return window.API_BASE;
    return RAILWAY_BACKEND;
  }

  function fmt(n) {
    try { return Number(n || 0).toLocaleString('es-CL'); } catch (e) { return String(n); }
  }

  function timeAgo(isoDate) {
    if (!isoDate) return null;
    var fecha = new Date(isoDate);
    if (isNaN(fecha.getTime())) return null;
    var minutos = Math.max(0, Math.floor((Date.now() - fecha.getTime()) / 60000));
    if (minutos < 1) return 'ahora';
    if (minutos < 60) return minutos + ' min';
    if (minutos < 1440) return Math.floor(minutos / 60) + ' h';
    return Math.floor(minutos / 1440) + ' d';
  }

  function setText(id, value) {
    var el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  async function cargar() {
    // Solo corre si el ribbon existe en la página
    if (!document.querySelector('.ribbon')) return;

    try {
      var resp = await fetch(apiBase() + '/api/estadisticas', { signal: AbortSignal.timeout ? AbortSignal.timeout(8000) : undefined });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      var data = await resp.json();

      var ago = timeAgo(data.ultima_actualizacion);
      if (ago) setText('ribbon-actualizado', ago);

      if (data.instituciones_activas != null) setText('ribbon-instituciones', fmt(data.instituciones_activas));
      if (data.activas_hoy != null) setText('ribbon-vigentes', fmt(data.activas_hoy));
      if (data.cierran_hoy != null) setText('ribbon-cierran', fmt(data.cierran_hoy));
    } catch (err) {
      // Falla silenciosa: los "—" de placeholder quedan visibles
      if (window.console && console.warn) console.warn('ribbon-data: no se pudo cargar', err);
    }
  }

  // Correr cuando el ribbon esté en el DOM
  document.addEventListener('shell:ready', cargar);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', cargar);
  } else {
    // Ya puede estar listo
    setTimeout(cargar, 100);
  }
})();
