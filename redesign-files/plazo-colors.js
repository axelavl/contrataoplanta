/* plazo-colors.js — colorea cards según días al cierre
   0-2 días: rojo terra (crítico)
   3-5 días: ámbar (warning)
   6+ días: verde sage (seguro)
   Sin fecha: neutro (sin cambio)

   También oculta "Renta no informada" cuando no hay data.
*/
(function () {
  'use strict';

  function parseDias(plazoText) {
    if (!plazoText) return null;
    var t = plazoText.toLowerCase();
    if (t.includes('hoy')) return 0;
    if (t.includes('mañana')) return 1;
    var m = t.match(/(\d+)\s*(día|dias)/);
    if (m) return parseInt(m[1], 10);
    if (t.includes('disponible')) return 999;
    if (t.includes('próximamente') || t.includes('proximamente')) return -1;
    if (t.includes('finalizada') || t.includes('cerrad')) return -2;
    return null;
  }

  function patchCard(card) {
    // Solo primer span de plazo (puede haber variantes con plazo-dot + span)
    var plazoSpan = card.querySelector('.plazo-dot + span, .plazo-dot ~ span');
    if (!plazoSpan) return;
    var dias = parseDias(plazoSpan.textContent);
    if (dias === null) return;

    card.classList.remove('plazo-critico', 'plazo-warning', 'plazo-safe', 'plazo-cerrado');

    if (dias === -2) {
      card.classList.add('plazo-cerrado');
    } else if (dias < 0) {
      // upcoming → neutro
    } else if (dias <= 2) {
      card.classList.add('plazo-critico');
    } else if (dias <= 5) {
      card.classList.add('plazo-warning');
    } else {
      card.classList.add('plazo-safe');
    }
  }

  function hideRentaMuted(card) {
    // Ocultar cualquier "Renta no informada" del card
    var rentas = card.querySelectorAll('.oferta-renta--muted');
    rentas.forEach(function (el) { el.style.display = 'none'; });
  }

  function patchAllCards() {
    document.querySelectorAll('.oferta-card').forEach(function (card) {
      patchCard(card);
      hideRentaMuted(card);
    });
  }

  function init() {
    patchAllCards();
    // Observar la lista para re-aplicar cuando el filtro cambia
    var lista = document.getElementById('lista-ofertas');
    if (lista) {
      var obs = new MutationObserver(function () {
        // Debounce
        clearTimeout(window.__plazoColorsTimer);
        window.__plazoColorsTimer = setTimeout(patchAllCards, 50);
      });
      obs.observe(lista, { childList: true, subtree: false });
    }
  }

  document.addEventListener('shell:ready', init);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    setTimeout(init, 300);
  }
})();
