/* ============================================================================
 * mapa-chile.js — Mapa real de Chile (16 regiones) coloreado por densidad
 * ----------------------------------------------------------------------------
 * Aprovecha que el país es vertical: layout en 2 columnas → MAPA | OFERTAS.
 * Al tocar una región, sus ofertas aparecen al costado (sin tener que bajar).
 * Color por CUARTILES (no lineal) para que las diferencias se noten aunque
 * unas pocas regiones concentren casi todo.
 * Vanilla JS. Requiere chile-geo.js (window.CHILE_GEO) y estilos.css.
 *
 * Uso:
 *   const api = MapaChile.render(host, {
 *     counts: { 'Metropolitana': 40, 'Valparaíso': 19, ... },
 *     selected: 'Metropolitana',
 *     onSelect: (region) => {            // el host pide las ofertas y las pinta
 *       api.cargando();                  // spinner en el panel derecho
 *       fetchOfertas(region).then(html => api.setPanel(html));
 *     }
 *   });
 *   // api.setPanel(htmlString)  → inyecta contenido en la columna derecha
 *   // api.cargando()            → estado "cargando…" en la columna derecha
 *   // api.regiones()            → lista de nombres canónicos
 *
 * Contorno: amCharts geodata (uso libre). Proyección Mercator.
 * ========================================================================== */
(function (global) {
  'use strict';

  // Rampa discreta de 5 pasos (claro → azul profundo). Pasos = saltos visibles.
  const RAMPA = ['#D7E0F2', '#A6BEE8', '#6E8FD6', '#3E63BD', '#1B3A82'];
  const VACIO = '#ECE7DA';

  // Asigna a cada región un nivel 0..4 por CUARTIL de su recuento (>0).
  function escala(counts, regiones) {
    const vals = regiones.map((r) => counts[r.name] || 0).filter((v) => v > 0).sort((a, b) => a - b);
    if (!vals.length) return () => -1; // todas vacías
    // umbrales en los percentiles 20/40/60/80 de los valores no-cero
    const q = [0.2, 0.4, 0.6, 0.8].map((p) => vals[Math.min(vals.length - 1, Math.floor(p * vals.length))]);
    return (c) => {
      if (c <= 0) return -1;
      let lvl = 0;
      for (let i = 0; i < q.length; i++) if (c >= q[i]) lvl = i + 1;
      return lvl;
    };
  }

  function render(host, opts) {
    opts = opts || {};
    const G = global.CHILE_GEO;
    if (!G) { host.innerHTML = '<div class="cop-map-empty">No se cargó chile-geo.js</div>'; return { setPanel() {}, cargando() {}, regiones: () => [] }; }
    const counts = opts.counts || {};
    let selected = opts.selected || null;
    const onSelect = opts.onSelect || function () {};

    const nivel = escala(counts, G.regions);

    const paths = G.regions.map((r) => {
      const c = counts[r.name] || 0;
      const lvl = nivel(c);
      const fill = lvl < 0 ? VACIO : RAMPA[lvl];
      const sel = selected === r.name;
      return `<path d="${r.d}" data-region="${r.name}" tabindex="0" role="button"
        aria-label="${r.name}: ${c} ${c === 1 ? 'vacante' : 'vacantes'}"
        style="fill:${fill};stroke:${sel ? '#0E2148' : '#fff'};stroke-width:${sel ? '2.4' : '0.7'};cursor:pointer">
        <title>${r.name}: ${c} ${c === 1 ? 'vacante' : 'vacantes'}</title></path>`;
    }).join('');

    const dots = G.regions.filter((r) => (counts[r.name] || 0) > 0).map((r) => {
      const sel = selected === r.name;
      const c = counts[r.name];
      const rad = sel ? 13 : 11;
      return `<circle cx="${r.cx}" cy="${r.cy}" r="${rad}" fill="${sel ? '#C8881F' : '#0E2148'}" stroke="#fff" stroke-width="1.5" pointer-events="none"/>
       <text x="${r.cx}" y="${r.cy + 3.7}" text-anchor="middle" font-size="11.5"
         font-family="Inter,system-ui,sans-serif" font-weight="700" fill="#fff" pointer-events="none">${c}</text>`;
    }).join('');

    const legend = `<div class="cop-map-legend"><span>Menos</span>${RAMPA.map((c) => `<i style="background:${c}"></i>`).join('')}<span>Más</span></div>`;

    host.innerHTML =
      `<div class="cop-mapx">
        <div class="cop-mapx-map">
          <svg viewBox="${G.viewBox}" class="cop-map-svg" role="img" aria-label="Mapa de Chile con vacantes por región">${paths}${dots}</svg>
          ${legend}
        </div>
        <div class="cop-mapx-panel" id="cop-mapx-panel"></div>
      </div>`;

    const panel = host.querySelector('#cop-mapx-panel');
    const placeholder = () => { panel.innerHTML = `<div class="cop-mapx-hint"><svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 21s-7-5.5-7-11a7 7 0 0 1 14 0c0 5.5-7 11-7 11Z"/><circle cx="12" cy="10" r="2.5"/></svg><p>Tocá una región del mapa para ver sus ofertas aquí, sin perder el mapa de vista.</p></div>`; };
    if (selected && (counts[selected] || 0) >= 0) {/* el host inyectará */} else placeholder();

    function fire(el) {
      if (!el || !el.dataset.region) return;
      selected = el.dataset.region;
      // re-pintar selección (stroke + dot) sin re-render completo
      host.querySelectorAll('path[data-region]').forEach((p) => {
        const on = p.dataset.region === selected;
        p.style.stroke = on ? '#0E2148' : '#fff';
        p.style.strokeWidth = on ? '2.4' : '0.7';
      });
      onSelect(selected);
    }
    host.querySelectorAll('path[data-region]').forEach((p) => {
      p.addEventListener('click', () => fire(p));
      p.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fire(p); } });
    });

    const api = {
      setPanel: (html) => { panel.innerHTML = html; },
      cargando: () => { panel.innerHTML = `<div class="cop-mapx-hint"><div class="cop-spin"></div><p>Cargando ofertas…</p></div>`; },
      placeholder,
      regiones: () => G.regions.map((r) => r.name)
    };
    return api;
  }

  global.MapaChile = { render };
})(window);
