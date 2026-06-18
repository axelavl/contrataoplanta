/* ============================================================================
 * mapa-chile.js — Mapa real de Chile (16 regiones) coloreado por densidad
 * ----------------------------------------------------------------------------
 * Dibuja el contorno geográfico real (datos en chile-geo.js, proyección
 * Mercator) y pinta cada región según la cantidad de ofertas. Click en una
 * región dispara un callback con el nombre canónico de la región.
 * Vanilla JS. Requiere chile-geo.js (define window.CHILE_GEO) y estilos.css.
 *
 * Nombres canónicos de región (usá estos como claves de `counts`):
 *   'Arica y Parinacota','Tarapacá','Antofagasta','Atacama','Coquimbo',
 *   'Valparaíso','Metropolitana',"O'Higgins",'Maule','Ñuble','Biobío',
 *   'La Araucanía','Los Ríos','Los Lagos','Aysén','Magallanes'
 *
 * Uso:
 *   MapaChile.render(document.getElementById('mapa'), {
 *     counts: { 'Metropolitana': 7, 'Valparaíso': 1, ... },
 *     selected: 'Metropolitana',          // opcional
 *     onSelect: (region) => { ...filtra y vuelve a render... }
 *   });
 *
 * Contorno: amCharts geodata (uso libre, atribución apreciada).
 * ========================================================================== */
(function (global) {
  'use strict';

  function render(container, opts) {
    opts = opts || {};
    const G = global.CHILE_GEO;
    if (!G) { container.innerHTML = '<div class="cop-map-empty">No se cargó chile-geo.js</div>'; return; }
    const counts = opts.counts || {};
    const selected = opts.selected || null;
    const onSelect = opts.onSelect || function () {};
    const azul = opts.color || '37,75,160'; // rgb de --cielo/--azul-m

    const max = Math.max(1, ...G.regions.map((r) => counts[r.name] || 0));

    const paths = G.regions.map((r) => {
      const c = counts[r.name] || 0;
      const empty = c === 0;
      const fill = empty ? '#ECE7DA' : `rgba(${azul},${(0.20 + 0.62 * (c / max)).toFixed(2)})`;
      const sel = selected === r.name;
      return `<path d="${r.d}" data-region="${r.name}" tabindex="0" role="button"
        aria-label="${r.name}: ${c} ${c === 1 ? 'vacante' : 'vacantes'}"
        style="fill:${fill};stroke:${sel ? '#142E6B' : '#fff'};stroke-width:${sel ? '2.2' : '0.7'};cursor:pointer">
        <title>${r.name}: ${c} ${c === 1 ? 'vacante' : 'vacantes'}</title></path>`;
    }).join('');

    const dots = G.regions.filter((r) => (counts[r.name] || 0) > 0).map((r) =>
      `<circle cx="${r.cx}" cy="${r.cy}" r="11" fill="#142E6B" pointer-events="none"/>
       <text x="${r.cx}" y="${r.cy + 3.7}" text-anchor="middle" font-size="11.5"
         font-family="Inter,system-ui,sans-serif" font-weight="700" fill="#fff"
         pointer-events="none">${counts[r.name]}</text>`).join('');

    container.innerHTML =
      `<svg viewBox="${G.viewBox}" class="cop-map-svg" role="img" aria-label="Mapa de Chile con vacantes por región">${paths}${dots}</svg>
       <div class="cop-map-legend"><span>Menos</span>
         <i style="background:#ECE7DA"></i>
         <i style="background:rgba(${azul},.32)"></i>
         <i style="background:rgba(${azul},.58)"></i>
         <i style="background:rgba(${azul},.82)"></i>
         <span>Más vacantes</span></div>`;

    function fire(el) { if (el && el.dataset.region) onSelect(el.dataset.region); }
    container.querySelectorAll('path[data-region]').forEach((p) => {
      p.addEventListener('click', () => fire(p));
      p.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fire(p); } });
    });
  }

  global.MapaChile = { render };
})(window);
