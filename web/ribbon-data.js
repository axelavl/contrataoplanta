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

  // Caché en localStorage: pinta los últimos valores conocidos al instante para
  // que la cinta no muestre "—" durante los ~2-3s que tarda el fetch a Railway.
  var CACHE_KEY = 'cop_ribbon_cache';
  function saveCache(d) {
    try { localStorage.setItem(CACHE_KEY, JSON.stringify(d)); } catch (e) {}
  }
  function fillFromCache() {
    try {
      var d = JSON.parse(localStorage.getItem(CACHE_KEY) || 'null');
      if (!d) return;
      if (d.ultima_actualizacion) { var a = timeAgoFromIso(d.ultima_actualizacion); if (a) setText('ribbon-actualizado', a); }
      if (d.instituciones_activas != null) setText('ribbon-instituciones', fmt(d.instituciones_activas));
      if (d.activas_hoy != null) setText('ribbon-vigentes', fmt(d.activas_hoy));
      if (d.cierran_hoy != null) {
        setText('ribbon-cierran', fmt(d.cierran_hoy));
        var n = Number(d.cierran_hoy);
        setText('ribbon-cierran-label', (Number.isFinite(n) && n === 1) ? 'cierra hoy' : 'cierran hoy');
      }
      log('filled from cache');
    } catch (e) {}
  }

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
    // El "actualizado hace X" del DOM (#count-sub) es un fallback para pintar
    // rápido; una vez que /api/estadisticas respondió (fuente fresca y
    // autoritativa), NO lo sobreescribimos con el valor del DOM, que puede
    // venir de un SSR/listado más viejo.
    if (!window.__copRibbonApiOk && countSub && countSub.textContent) {
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
      // cache:'no-store' evita que el navegador sirva una copia HTTP vieja del
      // endpoint: queremos el estado real del servidor en cada visita.
      var resp = await fetch(apiBase() + '/api/estadisticas', { cache: 'no-store' });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      var data = await resp.json();
      log('fetched data:', {
        activas_hoy: data.activas_hoy,
        instituciones_activas: data.instituciones_activas,
        cierran_hoy: data.cierran_hoy,
        ultima_actualizacion: data.ultima_actualizacion
      });
      saveCache({
        activas_hoy: data.activas_hoy,
        instituciones_activas: data.instituciones_activas,
        cierran_hoy: data.cierran_hoy,
        ultima_actualizacion: data.ultima_actualizacion
      });

      // El dato del fetch es la fuente de verdad: SIEMPRE sobreescribe lo que
      // pintó el caché. Antes esto sólo corría si el slot estaba vacío
      // (`!isFilled`), de modo que el valor cacheado viejo se quedaba fijo y
      // "actualizado hace X" no se refrescaba aunque los scrapers ya hubieran
      // corrido. El caché es sólo para el primer pintado sin parpadeo.
      var ago = timeAgoFromIso(data.ultima_actualizacion);
      if (ago) setText('ribbon-actualizado', ago);
      // A partir de aquí el API manda: copyFromDom dejará de tocar el timestamp.
      window.__copRibbonApiOk = true;
      if (data.instituciones_activas != null) {
        setText('ribbon-instituciones', fmt(data.instituciones_activas));
      }
      if (data.activas_hoy != null) {
        setText('ribbon-vigentes', fmt(data.activas_hoy));
      }
      if (data.cierran_hoy != null) {
        setText('ribbon-cierran', fmt(data.cierran_hoy));
        // Ajustar label a singular cuando sea exactamente 1.
        var n = Number(data.cierran_hoy);
        var label = (Number.isFinite(n) && n === 1) ? 'cierra hoy' : 'cierran hoy';
        setText('ribbon-cierran-label', label);
      }

      // También llenar #data-last-update (siempre, con el valor fresco).
      var lu = document.getElementById('data-last-update');
      if (lu && data.ultima_actualizacion) {
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
    // 'ribbon-instituciones' se quitó de la cinta; no se incluye para que el
    // polling pueda darse por completo cuando los 3 slots restantes están llenos.
    var ids = ['ribbon-actualizado', 'ribbon-vigentes', 'ribbon-cierran'];
    return ids.every(function (id) { return isFilled(getRibbonSlot(id)); });
  }

  function hideActualizadoIfEmpty() {
    // Si a los 10 segundos no tenemos fecha, ocultamos el item.
    var el = getRibbonSlot('ribbon-actualizado');
    if (!el) return;
    if (!isFilled(el)) {
      var item = el.closest('.ribbon-item');
      if (item) {
        item.style.display = 'none';
        log('ocultando item actualizado (sin fecha disponible)');
      }
    }
  }

  function init() {
    if (!document.querySelector('.ribbon')) {
      log('no .ribbon on page, skipping');
      return;
    }
    if (window.__copRibbonInit) return;  // evita doble init (shell:ready + DOMContentLoaded disparaban 2 fetch)
    window.__copRibbonInit = true;
    log('init — cache + poll + fetch');

    // Pinta de inmediato los últimos valores conocidos (sin "—" mientras carga)
    fillFromCache();

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

    // Botón "Actualizar": fuerza datos frescos sin recargar la página.
    var btnRefresh = document.getElementById('ribbon-refresh');
    if (btnRefresh && !btnRefresh.__wired) {
      btnRefresh.__wired = true;
      btnRefresh.addEventListener('click', function () {
        if (btnRefresh.classList.contains('is-loading')) return;
        btnRefresh.classList.add('is-loading');
        btnRefresh.disabled = true;
        // Limpia el caché para que el próximo pintado no parta de un valor viejo.
        try { localStorage.removeItem(CACHE_KEY); } catch (e) {}
        var tareas = [fetchAndFill()];
        // Recarga el listado de ofertas si la home expone la función.
        try {
          if (typeof window.cargarOfertas === 'function') {
            var p = window.cargarOfertas();
            if (p && typeof p.then === 'function') tareas.push(p);
          }
        } catch (e) { /* noop */ }
        Promise.all(tareas).finally(function () {
          // Pequeño mínimo visible para que el giro no parpadee.
          setTimeout(function () {
            btnRefresh.classList.remove('is-loading');
            btnRefresh.disabled = false;
          }, 400);
        });
      });
    }

    // Si tras 10 seg no se llenó "Actualizado hace", ocultar ese item
    setTimeout(hideActualizadoIfEmpty, 10000);
  }

  // Múltiples triggers para máxima robustez
  document.addEventListener('shell:ready', function () { setTimeout(init, 50); });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(init, 50); });
  } else {
    setTimeout(init, 100);
  }
})();
