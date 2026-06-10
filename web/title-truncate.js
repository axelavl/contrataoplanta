/* title-truncate.js — acorta títulos largos en el modal de oferta.
   Si detecta un título muy largo, lo parte en un punto "inteligente":
     ' - ', ' — ', '(', primer '.', ',', ';' o ':'.
   El título completo queda disponible en el body de la descripción.

   Notas:
   - Evitamos cortar en puntos de abreviatura (Dr., Sr., U., E.U.S., etc.)
     o en iniciales de una sola letra. El corte por punto sólo se dispara
     cuando el token anterior es una palabra normal (no sigla/abreviatura).
*/
(function () {
  'use strict';

  var MAX_LEN = 90;

  // Abreviaturas frecuentes en títulos de cargos (sector público chileno).
  // Se comparan en minúsculas contra la palabra anterior al punto.
  var ABBR_SET = new Set([
    'dr','dra','drs','sr','sra','srta','srs','sres','sras',
    'prof','ing','lic','arq','téc','tec','mag','mg','mgr','ph','phd',
    'univ','u','ud','uds','dn','don','doña','dona',
    'art','n','no','núm','num','cap','pág','pag',
    'av','avda','cía','cia','ltda','spa','depto','dpto','fac',
    'adm','admin','asist','aux','eus','esu','apu','apr','apt',
    'slep','cmsc','sercotec','minsal','mineduc','minjus'
  ]);

  function wordBefore(text, periodIdx) {
    var start = periodIdx;
    while (start > 0 && /[A-Za-zÑñÁÉÍÓÚÜáéíóúü]/.test(text.charAt(start - 1))) start--;
    return text.substring(start, periodIdx);
  }

  function isAbbreviationPeriod(text, periodIdx) {
    var w = wordBefore(text, periodIdx);
    if (!w) return false;
    // Inicial (1 sola letra mayúscula): "J. M. González" → J., M. son iniciales.
    if (w.length === 1 && /[A-ZÑ]/.test(w)) return true;
    // Patrón de sigla con puntos intercalados (E.U.S., S.A., N.N.), detectado
    // si el token anterior termina con un punto (ya consumido) o si hay otro
    // punto en los 3 caracteres previos.
    if (periodIdx >= 2 && text.charAt(periodIdx - 2) === '.') return true;
    return ABBR_SET.has(w.toLowerCase());
  }

  function findPeriodBreak(text) {
    var re = /\.(?=\s|$)/g;
    var m;
    while ((m = re.exec(text)) !== null) {
      if (m.index <= 20 || m.index >= 120) continue;
      if (isAbbreviationPeriod(text, m.index)) continue;
      return m.index;
    }
    return -1;
  }

  function findFirstMatch(text, re) {
    var m = text.match(re);
    if (!m || m.index == null) return -1;
    if (m.index <= 20 || m.index >= 120) return -1;
    return m.index;
  }

  function truncateSmart(text) {
    if (!text || text.length <= MAX_LEN) return text;
    var candidates = [
      findFirstMatch(text, /\s[-–—]\s/),  // " - ", " – ", " — "
      findFirstMatch(text, /\s\(/),         // " ("
      findPeriodBreak(text),                 // ". " (ignorando abreviaturas)
      findFirstMatch(text, /[,;:](?=\s|$)/)  // ", " ";" ":"
    ].filter(function (idx) { return idx > 0; });

    if (candidates.length) {
      var best = Math.min.apply(null, candidates);
      return text.substring(0, best).trim();
    }
    // Fallback: cortar en espacio cerca del máximo.
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
