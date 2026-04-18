/* search-toggle.js — colapsa el buscador a lo esencial y expande on demand.
   Por defecto muestra: Cargo + Región + Buscar.
   El botón "+ Más filtros" expande el resto (Ciudad/Comuna, Sector,
   Institución, Renta mínima, Atajos).
*/
(function () {
  'use strict';

  function init() {
    var wrap = document.querySelector('.buscador-wrap');
    if (!wrap) return;
    if (wrap.dataset.searchToggleReady === '1') return;
    wrap.dataset.searchToggleReady = '1';

    // 1) Clonar el botón "Buscar" de fila2 y ponerlo al final de la fila1
    var firstRow = wrap.querySelector('.buscador-form');
    var origBtn = wrap.querySelector('.buscador-fila2 .btn-buscar');
    if (firstRow && origBtn) {
      var compactBtn = origBtn.cloneNode(true);
      compactBtn.classList.add('btn-buscar-compact');
      compactBtn.removeAttribute('id');
      compactBtn.addEventListener('click', function (e) {
        e.preventDefault();
        origBtn.click();
      });
      firstRow.appendChild(compactBtn);
    }

    // 2) Añadir el botón de toggle "+ Más filtros"
    var toggleWrap = document.createElement('div');
    toggleWrap.className = 'filtros-toggle-wrap';
    var toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'btn-filtros-mas';
    toggle.setAttribute('aria-expanded', 'false');
    toggle.innerHTML = '<span class="icon">+</span> Más filtros';
    toggle.addEventListener('click', function () {
      var expanded = wrap.classList.toggle('is-expanded');
      toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
      toggle.innerHTML = expanded
        ? '<span class="icon">−</span> Menos filtros'
        : '<span class="icon">+</span> Más filtros';
    });
    toggleWrap.appendChild(toggle);

    // Insertarlo entre buscador-form y fila2
    var fila2 = wrap.querySelector('.buscador-fila2');
    if (fila2) {
      fila2.parentNode.insertBefore(toggleWrap, fila2);
    } else {
      wrap.appendChild(toggleWrap);
    }
  }

  document.addEventListener('DOMContentLoaded', init);
  document.addEventListener('shell:ready', init);
  if (document.readyState !== 'loading') setTimeout(init, 200);
})();
