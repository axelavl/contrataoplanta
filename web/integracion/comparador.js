/* ============================================================================
 * comparador.js — Comparador de ofertas lado a lado (contrata o planta)
 * ----------------------------------------------------------------------------
 * El usuario va marcando ofertas "para comparar" desde cualquier vista
 * (tarjeta, fila, mapa) y acá las ve enfrentadas: institución, modalidad,
 * región, renta, jornada y plazo. Resalta en dorado la mayor renta.
 * La selección persiste en localStorage ('cmp_contrataoplanta', máx. 3).
 *
 * Vanilla JS. Expone window.Comparador. Requiere estilos.css. Reutiliza tu
 * fetch de detalle + tu normalizador (los pasás por opciones, como en la ficha).
 *
 * API:
 *   Comparador.toggle(id)        → marca/desmarca una oferta (devuelve bool)
 *   Comparador.has(id)           → ¿está seleccionada?
 *   Comparador.ids()             → array de ids seleccionados
 *   Comparador.count()           → cantidad seleccionada
 *   Comparador.limpiar()
 *   Comparador.onChange(fn)      → te avisa cuando cambia la selección
 *   Comparador.abrir(opts)       → abre el overlay de comparación
 *
 * Para abrir necesita saber cómo traer y normalizar cada oferta:
 *   Comparador.abrir({
 *     fetchOferta: async (id) => rawOfertaDeTuAPI,
 *     normalizar:  (raw) => ({ cargo, institucion, sector, tipo, region,
 *                              comuna, jornada, renta, diasRestantes,
 *                              fechaCierre, portalUrl }),   // = esquema ficha
 *     onVerDetalles: (id) => abrirModal(id)
 *   });
 *
 * También podés configurar una sola vez:
 *   Comparador.config({ fetchOferta, normalizar, onVerDetalles });
 *   …y luego Comparador.abrir() sin argumentos.
 * ========================================================================== */
(function (global) {
  'use strict';

  const KEY = 'cmp_contrataoplanta';
  const MAX = 3;
  let cfg = {};
  const listeners = [];

  const load = () => { try { return JSON.parse(localStorage.getItem(KEY) || '[]'); } catch (e) { return []; } };
  const save = (a) => { try { localStorage.setItem(KEY, JSON.stringify(a)); } catch (e) { /* noop */ } };
  let sel = load();

  const escHtml = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const fmtRenta = (n) => {
    if (n == null) return null;
    if (typeof n === 'string') { const s = n.trim(); return s || null; }
    return isNaN(n) ? null : '$' + Number(n).toLocaleString('es-CL');
  };
  const rentaNum = (n) => {
    if (typeof n === 'number') return n;
    if (typeof n === 'string') { const m = n.replace(/[^\d]/g, ''); return m ? Number(m) : null; }
    return null;
  };

  function notify() { listeners.forEach((fn) => { try { fn(sel.slice()); } catch (e) { /* noop */ } }); }

  function toggle(id) {
    id = Number(id);
    const i = sel.indexOf(id);
    if (i >= 0) { sel.splice(i, 1); }
    else { if (sel.length >= MAX) return false; sel.push(id); }
    save(sel); notify();
    return sel.indexOf(id) >= 0;
  }
  const has = (id) => sel.indexOf(Number(id)) >= 0;
  const ids = () => sel.slice();
  const count = () => sel.length;
  function limpiar() { sel = []; save(sel); notify(); }
  function onChange(fn) { if (typeof fn === 'function') { listeners.push(fn); fn(sel.slice()); } }
  function config(o) { cfg = Object.assign(cfg, o || {}); }

  // ---------- overlay ----------
  let overlay = null;
  function montar() {
    if (overlay) return;
    overlay = document.createElement('div');
    overlay.className = 'cop-cmp-overlay';
    overlay.innerHTML = `
      <div class="cop-cmp" role="dialog" aria-modal="true" aria-label="Comparador de ofertas">
        <div class="cop-cmp-head">
          <h3>Comparador de ofertas</h3>
          <button class="cop-cmp-x" type="button" aria-label="Cerrar">✕</button>
        </div>
        <div class="cop-cmp-body" id="cop-cmp-body"></div>
      </div>`;
    document.body.appendChild(overlay);
    overlay.querySelector('.cop-cmp-x').addEventListener('click', cerrar);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) cerrar(); });
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && overlay.classList.contains('is-open')) cerrar(); });
  }
  function cerrar() { if (overlay) { overlay.classList.remove('is-open'); document.body.style.overflow = ''; } }

  async function abrir(opts) {
    opts = Object.assign({}, cfg, opts || {});
    montar();
    const body = overlay.querySelector('#cop-cmp-body');
    overlay.classList.add('is-open');
    document.body.style.overflow = 'hidden';

    if (!sel.length) {
      body.innerHTML = `<div class="cop-cmp-empty">Todavía no marcaste ofertas para comparar.<br>Tocá <b>“⇆ Comparar”</b> en las tarjetas o filas (hasta ${MAX}).</div>`;
      return;
    }
    if (typeof opts.fetchOferta !== 'function' || typeof opts.normalizar !== 'function') {
      body.innerHTML = `<div class="cop-cmp-empty">Falta configurar <code>fetchOferta</code> y <code>normalizar</code>.</div>`;
      return;
    }

    body.innerHTML = `<div class="cop-cmp-loading"><div class="cop-spin"></div></div>`;
    let ofertas;
    try {
      ofertas = await Promise.all(sel.map(async (id) => opts.normalizar(await opts.fetchOferta(id))));
    } catch (e) {
      body.innerHTML = `<div class="cop-cmp-empty">No pudimos cargar las ofertas. Reintentá.</div>`;
      return;
    }

    const maxRenta = Math.max.apply(null, ofertas.map((o) => rentaNum(o.renta) || 0));
    const filas = [
      ['Institución', (o) => escHtml(o.institucion)],
      ['Modalidad', (o) => o.tipo ? `<span class="cop-cmp-badge">${escHtml(o.tipo)}</span>` : '—'],
      ['Región', (o) => escHtml([o.region, o.comuna].filter(Boolean).join(' · ')) || '—'],
      ['Renta bruta', (o) => { const v = fmtRenta(o.renta); const best = rentaNum(o.renta) && rentaNum(o.renta) === maxRenta; return v ? `<span class="cop-cmp-renta${best ? ' is-best' : ''}">${escHtml(v)}</span>` : '—'; }],
      ['Jornada', (o) => escHtml(o.jornada) || '—'],
      ['Plazo de cierre', (o) => plazoTxt(o)]
    ];

    const labels = `<div class="cop-cmp-col cop-cmp-col--labels"><div class="cop-cmp-cellhead"></div>${filas.map((f) => `<div class="cop-cmp-cell cop-cmp-cell--label">${f[0]}</div>`).join('')}<div class="cop-cmp-cell"></div></div>`;
    const cols = ofertas.map((o, i) => `
      <div class="cop-cmp-col">
        <div class="cop-cmp-cellhead">
          <button class="cop-cmp-rm" type="button" data-rm="${sel[i]}" title="Quitar">✕</button>
          <div class="cop-cmp-cargo">${escHtml(o.cargo)}</div>
        </div>
        ${filas.map((f) => `<div class="cop-cmp-cell">${f[1](o)}</div>`).join('')}
        <div class="cop-cmp-cell cop-cmp-cell--act"><button class="cop-cmp-ver" type="button" data-ver="${sel[i]}">Ver detalles</button></div>
      </div>`).join('');

    body.innerHTML =
      `<div class="cop-cmp-grid" style="grid-template-columns:120px repeat(${ofertas.length}, minmax(150px,1fr))">${labels}${cols}</div>
       <div class="cop-cmp-foot"><button class="cop-cmp-clear" type="button">Vaciar comparador</button></div>`;

    body.querySelectorAll('[data-rm]').forEach((b) => b.addEventListener('click', () => { toggle(b.dataset.rm); abrir(opts); }));
    body.querySelectorAll('[data-ver]').forEach((b) => b.addEventListener('click', () => { cerrar(); opts.onVerDetalles && opts.onVerDetalles(Number(b.dataset.ver)); }));
    body.querySelector('.cop-cmp-clear').addEventListener('click', () => { limpiar(); abrir(opts); });

    function plazoTxt(o) {
      const d = o.diasRestantes;
      let cls = 'ok', t;
      if (d == null) { cls = 'unknown'; t = 'No informado'; }
      else if (d < 0) { cls = 'urg'; t = 'Cerrada'; }
      else if (d === 0) { cls = 'urg'; t = 'Cierra hoy'; }
      else if (d === 1) { cls = 'urg'; t = 'Cierra mañana'; }
      else if (d <= 5) { cls = 'mid'; t = d + ' días'; }
      else { cls = 'ok'; t = d + ' días'; }
      return `<span class="cop-cmp-plazo is-${cls}">${t}</span>`;
    }
  }

  global.Comparador = { toggle, has, ids, count, limpiar, onChange, config, abrir, cerrar, MAX };
})(window);
