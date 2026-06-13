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

/* Issue #241 — overrides livianos cargados desde un script ya incluido. */
(function () {
  'use strict';

  function okUrl(url) {
    if (!url || typeof url !== 'string') return false;
    try {
      var u = new URL(url.trim());
      return u.protocol === 'http:' || u.protocol === 'https:';
    } catch (e) {
      return false;
    }
  }

  function hideBadBases(root) {
    (root || document).querySelectorAll('.btn-ver-off,[data-action="noop"]').forEach(function (el) {
      if (/bases/i.test(el.textContent || el.title || '')) el.remove();
    });
  }

  // Sobrescribe el renderer si app.js ya lo definió; si aún no, queda disponible
  // como función global para el render posterior.
  window.renderBtnBases = function renderBtnBasesIssue241(oferta) {
    if (!oferta || oferta.url_bases_valida !== true) return '';
    if (!okUrl(oferta.url_bases)) return '';
    if (oferta.url_oferta && oferta.url_bases === oferta.url_oferta) return '';
    return '<button class="btn-ver" type="button" data-action="abrir-visor-bases" data-stop-propagation="true" data-oferta-id="' +
      (Number(oferta.id) || 0) + '">Bases oficiales</button>';
  };

  function norm(s) {
    return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
  }

  function mergeInstitucionesEnBusqueda() {
    var cargo = document.getElementById('input-cargo');
    var inst = document.getElementById('input-institucion');
    if (!cargo || !inst || !inst.value || inst.value.indexOf(',') < 0) return;
    var current = cargo.value || '';
    var currentNorm = norm(current);
    inst.value.split(',').map(function (p) { return p.trim(); }).filter(Boolean).forEach(function (p) {
      if (!currentNorm.includes(norm(p))) current += (current ? ' ' : '') + p;
    });
    cargo.value = current.trim();
  }

  function injectBorderPolish() {
    if (document.getElementById('issue-241-border-polish')) return;
    var st = document.createElement('style');
    st.id = 'issue-241-border-polish';
    st.textContent = [
      '.oferta-card,.oferta-row,.widget,.resultados-shell,.modal-seccion,.modal-info-item,.bases-viewer-panel{border-color:rgba(15,45,90,.14)!important;box-shadow:0 10px 28px rgba(11,31,68,.07)}',
      '.oferta-card,.oferta-row,.widget,.resultados-shell{border-radius:18px}',
      '.oferta-card:hover,.oferta-row:hover{border-color:rgba(15,45,90,.24)!important;box-shadow:0 16px 36px rgba(11,31,68,.11)}',
      '.btn-ver-off{display:none!important}',
      ':root[data-theme="dark"] .oferta-card,:root[data-theme="dark"] .oferta-row,:root[data-theme="dark"] .widget,:root[data-theme="dark"] .resultados-shell,:root[data-theme="dark"] .modal-seccion{border-color:rgba(160,185,225,.18)!important;box-shadow:0 14px 32px rgba(0,0,0,.26)}'
    ].join('\n');
    document.head.appendChild(st);
  }

  function initIssue241() {
    document.querySelectorAll('[data-filtro="nuevos"]').forEach(function (btn) {
      btn.textContent = '✨ Últimas 24 h';
      btn.title = 'Ofertas añadidas durante las últimas 24 horas';
    });
    document.querySelectorAll('[data-filtro="cierra-hoy"]').forEach(function (btn) {
      btn.title = 'Ofertas que cierran hoy o mañana';
    });
    var inst = document.getElementById('input-institucion');
    if (inst) inst.placeholder = 'Institución: Contraloría, Codelco, SII...';

    if (!window.__issue241SearchHooks) {
      window.__issue241SearchHooks = true;
      document.addEventListener('click', function (e) {
        if (e.target.closest('[data-action="buscar"]')) mergeInstitucionesEnBusqueda();
      }, true);
      document.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' && e.target && e.target.id === 'input-institucion') mergeInstitucionesEnBusqueda();
      }, true);
    }

    hideBadBases(document);
    if (!window.__issue241BasesObserver && document.body) {
      window.__issue241BasesObserver = new MutationObserver(function (ms) {
        ms.forEach(function (m) {
          Array.prototype.forEach.call(m.addedNodes || [], function (n) {
            if (n.nodeType === 1) hideBadBases(n);
          });
        });
      });
      window.__issue241BasesObserver.observe(document.body, { childList: true, subtree: true });
    }
    injectBorderPolish();
  }

  document.addEventListener('shell:ready', initIssue241);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initIssue241);
  } else {
    setTimeout(initIssue241, 300);
  }
})();
