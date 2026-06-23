/* ──────────────────────────────────────────────────────────────────────────
   panel-mercado.js · Panel B2B de mercado laboral público
   Consume la API en vivo (igual base que estadisticas.js) y agrega la demanda
   por región, área profesional, tipo de vínculo y cobertura de renta.
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
  var hostLocal = (location.hostname === 'localhost' || location.hostname === '127.0.0.1' || location.hostname === '');
  var forzada = (typeof window.__API_BASE === 'string' ? window.__API_BASE.trim() : '');
  var API_BASE = forzada ? forzada.replace(/\/+$/, '') : (hostLocal ? 'http://localhost:8000' : RAILWAY_BACKEND);

  var CAP_PAGINAS = 12;       // tope de páginas a traer (muestra)
  var POR_PAGINA = 100;

  var fmt = function (n) { return (n == null ? 0 : n).toLocaleString('es-CL'); };
  function $(id) { return document.getElementById(id); }

  function cap(s) {
    if (!s) return 'Sin clasificar';
    s = String(s).trim();
    if (!s) return 'Sin clasificar';
    return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
  }

  /* Barras horizontales: data = [{k, v}] ordenado desc */
  function barras(elId, data, opts) {
    opts = opts || {};
    var el = $(elId);
    if (!el) return;
    if (!data.length) { el.innerHTML = '<p class="pm-vacio">Sin datos.</p>'; return; }
    var top = data.slice(0, opts.top || 10);
    var max = Math.max.apply(null, top.map(function (d) { return d.v; }));
    el.innerHTML = top.map(function (d) {
      var pct = max > 0 ? Math.round(d.v / max * 100) : 0;
      return '<div class="pm-row"><span class="pm-row-k">' + d.k + '</span>' +
        '<div class="pm-bar-wrap"><div class="pm-bar" style="width:' + pct + '%"></div></div>' +
        '<span class="pm-row-v">' + fmt(d.v) + '</span></div>';
    }).join('');
  }

  function topDeConteo(map, normaliza) {
    var arr = Object.keys(map).map(function (k) { return { k: normaliza ? normaliza(k) : k, v: map[k] }; });
    arr.sort(function (a, b) { return b.v - a.v; });
    return arr;
  }

  /* ── KPIs + estadísticas oficiales ── */
  function cargarEstadisticas() {
    return fetch(API_BASE + '/api/estadisticas')
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (d) {
        $('pm-kpis').innerHTML =
          kpi('Ofertas activas', fmt(d.activas_hoy)) +
          kpi('Instituciones activas', fmt(d.instituciones_activas)) +
          kpi('Nuevas (48 h)', fmt(d.nuevas_48h)) +
          kpi('Cierran hoy', fmt(d.cierran_hoy));
        var fecha = d.ultima_actualizacion ? new Date(d.ultima_actualizacion).toLocaleString('es-CL', { dateStyle: 'medium', timeStyle: 'short' }) : '—';
        $('pm-actualizado').textContent = fecha;
        barras('pm-sectores', (d.por_sector || []).map(function (s) { return { k: s.sector || 'Sin sector', v: s.total }; }));
        var inst = (d.mas_activas || []).map(function (i) { return { k: i.nombre, v: i.activas }; });
        barras('pm-instituciones', inst, { top: 10 });
      });
  }

  function kpi(label, val) {
    return '<div class="pm-kpi"><div class="pm-kpi-label">' + label + '</div><div class="pm-kpi-valor">' + val + '</div></div>';
  }

  /* ── Agregados del backend (universo completo, rápido) ── */
  function cargarAgregados() {
    return fetch(API_BASE + '/api/mercado/agregados')
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (d) {
        barras('pm-regiones', (d.por_region || []).map(function (x) { return { k: x.region, v: x.total }; }), { top: 14 });
        barras('pm-areas', (d.por_area || []).map(function (x) { return { k: x.area, v: x.total }; }), { top: 10 });
        barras('pm-tipos', (d.por_tipo || []).map(function (x) { return { k: x.tipo, v: x.total }; }), { top: 6 });

        var rt = d.renta || {};
        $('pm-renta').innerHTML =
          '<div class="pm-renta-num">' + (rt.pct_con_renta || 0) + '%</div>' +
          '<div class="pm-renta-txt">de las ofertas activas publican renta en un campo estructurado (' +
          fmt(rt.con_renta || 0) + ' de ' + fmt(rt.total || 0) + '). El resto requiere abrir las bases — ' +
          'una brecha que la inteligencia de renta por grado ayuda a cerrar.</div>';

        var nota = document.querySelector('.pm-muestra-nota');
        if (nota) nota.innerHTML = 'Vistas calculadas sobre el <strong>universo completo</strong> de ' +
          fmt(d.total_activas || 0) + ' ofertas activas.';

        var topAreas = (d.por_area || []).slice(0, 4).map(function (x) { return x.area; });
        if (topAreas.length) $('pm-bridge-areas').textContent = topAreas.join(' · ');
      });
  }

  /* ── Fallback: muestra de ofertas agregada en el navegador ── */
  function cargarMuestra() {
    return fetch(API_BASE + '/api/ofertas?pagina=1&por_pagina=' + POR_PAGINA)
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (d) {
        var total = d.total || 0;
        var paginas = Math.min(d.paginas || 1, CAP_PAGINAS);
        var ofertas = (d.ofertas || []).slice();
        var pend = [];
        for (var p = 2; p <= paginas; p++) {
          pend.push(fetch(API_BASE + '/api/ofertas?pagina=' + p + '&por_pagina=' + POR_PAGINA)
            .then(function (r) { return r.ok ? r.json() : { ofertas: [] }; })
            .then(function (j) { return j.ofertas || []; })
            .catch(function () { return []; }));
        }
        return Promise.all(pend).then(function (resto) {
          resto.forEach(function (arr) { ofertas = ofertas.concat(arr); });
          agregar(ofertas, total);
        });
      });
  }

  function agregar(ofertas, totalReal) {
    var n = ofertas.length;
    $('pm-muestra-n').textContent = fmt(n);
    $('pm-total-real').textContent = fmt(totalReal);

    var regiones = {}, areas = {}, tipos = {};
    var conRenta = 0;
    ofertas.forEach(function (o) {
      var reg = o.region || 'Sin región';
      regiones[reg] = (regiones[reg] || 0) + 1;
      var ar = (o.area_profesional || '').trim() || 'Sin clasificar';
      ar = ar.toLowerCase();
      areas[ar] = (areas[ar] || 0) + 1;
      var t = (o.tipo_contrato || o.calidad_juridica || 'Sin especificar');
      t = String(t).trim() || 'Sin especificar';
      tipos[t.toLowerCase()] = (tipos[t.toLowerCase()] || 0) + 1;
      if (o.renta_bruta_min != null || o.renta_bruta_max != null) conRenta++;
    });

    barras('pm-regiones', topDeConteo(regiones), { top: 12 });
    barras('pm-areas', topDeConteo(areas, cap), { top: 10 });
    barras('pm-tipos', topDeConteo(tipos, cap), { top: 6 });

    var pctRenta = n ? Math.round(conRenta / n * 100) : 0;
    $('pm-renta').innerHTML =
      '<div class="pm-renta-num">' + pctRenta + '%</div>' +
      '<div class="pm-renta-txt">de las ofertas de la muestra publican renta en un campo estructurado (' +
      fmt(conRenta) + ' de ' + fmt(n) + '). El resto requiere abrir las bases — ' +
      'una brecha que la inteligencia de renta por grado ayuda a cerrar.</div>';

    // Puente a cursos: top áreas como demanda de especialización.
    var topAreas = topDeConteo(areas, cap).slice(0, 4).map(function (d) { return d.k; });
    if (topAreas.length) {
      $('pm-bridge-areas').textContent = topAreas.join(' · ');
    }
  }

  function errorGlobal() {
    var msg = '<p class="pm-vacio">No pudimos cargar los datos en vivo ahora. Reintenta en unos minutos.</p>';
    ['pm-kpis','pm-sectores','pm-instituciones','pm-regiones','pm-areas','pm-tipos','pm-renta'].forEach(function (id) {
      var el = $(id); if (el) el.innerHTML = msg;
    });
  }

  // Prioriza el endpoint de agregados (universo completo); si no está
  // disponible, cae a la agregación por muestra en el navegador.
  var agregaciones = cargarAgregados().catch(function () { return cargarMuestra(); });
  Promise.all([cargarEstadisticas(), agregaciones]).catch(errorGlobal);
})();
