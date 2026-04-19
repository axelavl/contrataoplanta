/* title-truncate.js — acorta títulos largos en el modal de oferta.
   Si detecta un título muy largo, lo parte en un punto "inteligente":
     ' - ', ' — ', '(', primer '.', ',', ';' o ':'.
   El título completo queda disponible en el body de la descripción.
*/
(function () {
  'use strict';

  var MAX_LEN = 90;

  function truncateSmart(text) {
    if (!text || text.length <= MAX_LEN) return text;
    // Separadores ordenados por preferencia
    var seps = [
      { re: /\s[-–—]\s/, offset: 0 },  // " - ", " – ", " — "
      { re: /\s\(/, offset: 0 },        // " ("
      { re: /\.(?=\s|$)/, offset: 0 },  // ". "
      { re: /[,;:](?=\s|$)/, offset: 0 },
    ];
    var best = null;
    for (var i = 0; i < seps.length; i++) {
      var m = text.match(seps[i].re);
      if (m && m.index > 20 && m.index < 120) {
        if (!best || m.index < best) best = m.index;
      }
    }
    if (best !== null) {
      return text.substring(0, best).trim();
    }
    // Fallback: cortar en espacio cerca del máximo
    var truncated = text.substring(0, MAX_LEN);
    var lastSpace = truncated.lastIndexOf(' ');
    if (lastSpace > 50) truncated = truncated.substring(0, lastSpace);
    return truncated.trim() + '…';
  }

  function patchModalTitle() {
    var el = document.getElementById('modal-cargo');
    if (!el) return;
    var original = el.textContent;
    if (!original || el.dataset.truncated === '1') return;
    var short = truncateSmart(original);
    if (short !== original && short.length > 0) {
      el.dataset.fullTitle = original;
      el.textContent = short;
      el.setAttribute('title', original);
      el.dataset.truncated = '1';
    }
  }

  function patchCardTitles() {
    document.querySelectorAll('.oferta-cargo, .oferta-cargo-link').forEach(function (el) {
      if (el.dataset.truncated === '1') return;
      var original = el.textContent.trim();
      if (!original || original.length <= MAX_LEN) {
        el.dataset.truncated = '1';
        return;
      }
      var short = truncateSmart(original);
      if (short && short !== original) {
        el.dataset.fullTitle = original;
        el.textContent = short;
        el.setAttribute('title', original);
        el.dataset.truncated = '1';
      }
    });
  }

  function init() {
    patchCardTitles();
    // Observar el modal-cargo para cuando se abre una oferta
    var modalCargo = document.getElementById('modal-cargo');
    if (modalCargo) {
      var obs = new MutationObserver(function () {
        delete modalCargo.dataset.truncated;
        patchModalTitle();
      });
      obs.observe(modalCargo, { childList: true, characterData: true, subtree: true });
    }
    // Observar la lista de ofertas para nuevas cards
    var lista = document.getElementById('lista-ofertas');
    if (lista) {
      var obs2 = new MutationObserver(function () {
        clearTimeout(window.__titleTruncateTimer);
        window.__titleTruncateTimer = setTimeout(patchCardTitles, 80);
      });
      obs2.observe(lista, { childList: true, subtree: true });
    }
  }

  document.addEventListener('shell:ready', init);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    setTimeout(init, 300);
  }
})();
