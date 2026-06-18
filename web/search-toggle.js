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
    toggleWrap.appendChild(toggle);

    var fila2 = wrap.querySelector('.buscador-fila2');

    // Reubica el toggle según el estado:
    //  - colapsado → antes de fila2 (queda al pie del buscador compacto).
    //  - expandido → al final del buscador, DEBAJO de todos los filtros
    //    (fila Institución/Renta + Profesión + Atajos), no entre medio.
    function ubicarToggle(expanded) {
      if (expanded) {
        wrap.appendChild(toggleWrap);
      } else if (fila2 && fila2.parentNode) {
        fila2.parentNode.insertBefore(toggleWrap, fila2);
      } else {
        wrap.appendChild(toggleWrap);
      }
    }

    toggle.addEventListener('click', function () {
      var expanded = wrap.classList.toggle('is-expanded');
      toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
      toggle.innerHTML = expanded
        ? '<span class="icon">−</span> Menos filtros'
        : '<span class="icon">+</span> Más filtros';
      ubicarToggle(expanded);
    });

    // Posición inicial según el estado actual del buscador.
    ubicarToggle(wrap.classList.contains('is-expanded'));
  }

  document.addEventListener('DOMContentLoaded', init);
  document.addEventListener('shell:ready', init);
  if (document.readyState !== 'loading') setTimeout(init, 200);
})();
