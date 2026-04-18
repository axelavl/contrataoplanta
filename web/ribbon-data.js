/* ribbon-data.js — llena el ribbon con datos del API.
   Estrategia robusta con polling para evitar timing issues:
   1. Cada 500ms chequea si los elementos de la home (#hs-activas, etc.)
      ya tienen valores. Si sí, los copia al ribbon.
   2. En paralelo, hace fetch propio al API para obtener "cierran hoy"
      y como fallback por si el DOM copy no funciona.
   3. Se detiene cuando todos los slots están poblados o a los 30 seg.
*/
(function () {
  'use strict';

  var RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
  var DBG = true; // poner false cuando esté estable

  function log() {
    if (!DBG || !window.console) return;
    var args = Array.prototype.slice.call(arguments);
    console.log.apply(console, ['[ribbon-data]'].concat(args));
  }

  function apiBase() {
    if (window.__API_BASE) return window.__API_BASE;
    if (window.API_BASE) return window.API_BASE;
    return RAILWAY_BACKEND;
  }

  function fmt(n) {
    try { return Number(n || 0).toLocaleString('es-CL'); } catch (e) { return String(n); }
  }

  function timeAgoFromIso(isoDate) {
    if (!isoDate) return null;
    var fecha = new Date(isoDate);
    if (isNaN(fecha.getTime())) return null;
    var minutos = Math.max(0, Math.floor((Date.now() - fecha.getTime()) / 60000));
    if (minutos < 1) return 'ahora';
    if (minutos < 60) return minutos + ' min';
    if (minutos < 1440) return Math.floor(minutos / 60) + ' h';
    return Math.floor(minutos / 1440) + ' d';
  }

  function getRibbonSlot(id) { return document.getElementById(id); }

  function isFilled(el) {
    if (!el) return false;
    var t = (el.textContent || '').trim();
    return t && t !== '—' && t !== '-';
  }

  function setText(id, value) {
    var el = getRibbonSlot(id);
    if (el && value != null && value !== '') {
      el.textContent = value;
    }
  }

  function copyFromDom() {
    var hsA = document.getElementById('hs-activas');
    var hsI = document.getElementById('hs-instituciones');
    var countSub = document.getElementById('count-sub');

    if (isFilled(hsA)) {
      setText('ribbon-vigentes', hsA.textContent.trim());
      log('copied vigentes from #hs-activas:', hsA.textContent.trim());
    }
    if (isFilled(hsI)) {
      setText('ribbon-instituciones', hsI.textContent.trim());
      log('copied instituciones from #hs-instituciones:', hsI.textContent.trim());
    }
    if (countSub && countSub.textContent) {
      var m = countSub.textContent.match(/hace\s+([^·]+?)(?:\s*$|\s*·)/i);
      if (m && m[1]) {
        setText('ribbon-actualizado', m[1].trim());
        log('copied actualizado from #count-sub:', m[1].trim());
      }
    }
  }

  async function fetchAndFill() {
    try {
      log('fetching', apiBase() + '/api/estadisticas');
      var resp = await fetch(apiBase() + '/api/estadisticas');
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      var data = await resp.json();
      log('fetched data:', {
        activas_hoy: data.activas_hoy,
        instituciones_activas: data.instituciones_activas,
        cierran_hoy: data.cierran_hoy,
        ultima_actualizacion: data.ultima_actualizacion
      });

      if (!isFilled(getRibbonSlot('ribbon-actualizado'))) {
        var ago = timeAgoFromIso(data.ultima_actualizacion);
        if (ago) setText('ribbon-actualizado', ago);
      }
      if (!isFilled(getRibbonSlot('ribbon-instituciones')) && data.instituciones_activas != null) {
        setText('ribbon-instituciones', fmt(data.instituciones_activas));
      }
      if (!isFilled(getRibbonSlot('ribbon-vigentes')) && data.activas_hoy != null) {
        setText('ribbon-vigentes', fmt(data.activas_hoy));
      }
      if (data.cierran_hoy != null) {
        setText('ribbon-cierran', fmt(data.cierran_hoy));
      }

      // También llenar #data-last-update si está en "no disponible"
      var lu = document.getElementById('data-last-update');
      if (lu && /no disponible|cargando/i.test(lu.textContent) && data.ultima_actualizacion) {
        var fecha = new Date(data.ultima_actualizacion);
        if (!isNaN(fecha.getTime())) {
          lu.textContent = fecha.toLocaleString('es-CL', { dateStyle: 'medium', timeStyle: 'short' });
        }
      }
    } catch (err) {
      log('fetch failed:', err && err.message);
    }
  }

  function allRibbonFilled() {
    var ids = ['ribbon-actualizado', 'ribbon-instituciones', 'ribbon-vigentes', 'ribbon-cierran'];
    return ids.every(function (id) { return isFilled(getRibbonSlot(id)); });
  }

  function init() {
    if (!document.querySelector('.ribbon')) {
      log('no .ribbon on page, skipping');
      return;
    }
    log('init — poll + fetch');

    // Intento inmediato (por si ya está todo cargado)
    copyFromDom();

    // Polling cada 500ms durante 30s para copiar valores de la home
    var tries = 0;
    var maxTries = 60;
    var poller = setInterval(function () {
      copyFromDom();
      tries++;
      if (allRibbonFilled() || tries >= maxTries) {
        log('polling stopped, tries:', tries, 'allFilled:', allRibbonFilled());
        clearInterval(poller);
      }
    }, 500);

    // Fetch paralelo al API para cierran_hoy y fallback
    fetchAndFill();
  }

  // Múltiples triggers para máxima robustez
  document.addEventListener('shell:ready', function () { setTimeout(init, 50); });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(init, 50); });
  } else {
    setTimeout(init, 100);
  }
})();
