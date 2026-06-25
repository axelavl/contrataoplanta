/* ══════════════════════════════════════════════════════════════════════
 * mobile-ux.js — Mejoras de experiencia móvil (cinta + acordeón de ficha)
 * ----------------------------------------------------------------------
 * Vanilla JS, CSP-safe (sin inline). Dos responsabilidades:
 *
 *   1. --ribbon-h: mide el alto REAL de la cinta (.ribbon) y lo publica en
 *      :root. El CSS lo usa para pegar el header justo debajo, sin el hueco
 *      de 1–3px que dejaba el `top` fijo. Se recalcula al cargar, al
 *      redimensionar y cuando cambia el alto de la cinta.
 *
 *   2. Acordeón de la ficha de oferta: en móvil, las secciones largas
 *      (requisitos, funciones, condiciones, etc.) arrancan plegadas y se
 *      abren al tocar el título. Reduce mucho el scroll del detalle.
 * ══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  /* ── 1 · Alto de la cinta ───────────────────────────────────────────── */
  function medirRibbon() {
    var ribbon = document.querySelector('.ribbon');
    if (!ribbon) return;
    var h = Math.round(ribbon.getBoundingClientRect().height);
    if (h > 0) {
      document.documentElement.style.setProperty('--ribbon-h', h + 'px');
    }
  }

  function initRibbon() {
    medirRibbon();
    // La cinta se inyecta de forma asíncrona (shared-shell): reintenta unas
    // veces hasta que exista y tenga alto.
    var intentos = 0;
    var t = setInterval(function () {
      medirRibbon();
      if (++intentos >= 10 || document.querySelector('.ribbon')) {
        // un par de mediciones extra tras aparecer, luego corta.
        if (intentos >= 12) clearInterval(t);
      }
    }, 250);

    window.addEventListener('resize', medirRibbon, { passive: true });
    window.addEventListener('load', medirRibbon, { passive: true });

    // Si el navegador soporta ResizeObserver, seguimos el alto de la cinta
    // (cambia si el texto envuelve a 2 líneas en pantallas muy angostas).
    if (window.ResizeObserver) {
      var ro = new ResizeObserver(medirRibbon);
      var ribbon = document.querySelector('.ribbon');
      if (ribbon) {
        ro.observe(ribbon);
      } else {
        // Espera a que aparezca para observarla.
        var espera = setInterval(function () {
          var r = document.querySelector('.ribbon');
          if (r) { ro.observe(r); clearInterval(espera); }
        }, 250);
        setTimeout(function () { clearInterval(espera); }, 4000);
      }
    }
  }

  /* ── 2 · Acordeón de la ficha de oferta ─────────────────────────────── */
  var mq = window.matchMedia('(max-width: 600px)');

  // Secciones que se mantienen ABIERTAS por defecto (el resto se pliega).
  // El grid de datos clave no es .cop-sec, así que siempre queda visible.
  function debeQuedarAbierta(sec) {
    return sec.classList.contains('cop-contacto');
  }

  function aplicarAcordeon(overlay) {
    var body = overlay.querySelector('.cop-body');
    if (!body) return;
    var secs = body.querySelectorAll('.cop-sec');

    if (mq.matches) {
      overlay.classList.add('cop-accordion');
      for (var i = 0; i < secs.length; i++) {
        var t = secs[i].querySelector('.cop-sec-t');
        if (t) {
          t.setAttribute('role', 'button');
          t.setAttribute('tabindex', '0');
        }
        if (debeQuedarAbierta(secs[i])) {
          secs[i].classList.remove('cop-colapsada');
          if (t) t.setAttribute('aria-expanded', 'true');
        } else {
          secs[i].classList.add('cop-colapsada');
          if (t) t.setAttribute('aria-expanded', 'false');
        }
      }
    } else {
      // Desktop: sin acordeón, todo desplegado.
      overlay.classList.remove('cop-accordion');
      for (var j = 0; j < secs.length; j++) {
        secs[j].classList.remove('cop-colapsada');
      }
    }
  }

  function toggleSeccion(titulo) {
    var sec = titulo.closest('.cop-sec');
    if (!sec) return;
    var plegada = sec.classList.toggle('cop-colapsada');
    titulo.setAttribute('aria-expanded', plegada ? 'false' : 'true');
  }

  function initAcordeon() {
    // Click/teclado delegado sobre el título de sección (sólo en modo
    // acordeón, que sólo se activa en móvil).
    document.addEventListener('click', function (e) {
      var t = e.target.closest && e.target.closest('.cop-accordion .cop-sec > .cop-sec-t');
      if (t) toggleSeccion(t);
    });
    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      var t = e.target.closest && e.target.closest('.cop-accordion .cop-sec > .cop-sec-t');
      if (t) { e.preventDefault(); toggleSeccion(t); }
    });

    // La ficha (.cop-modal-overlay) la crea ficha-oferta.js bajo demanda.
    // Observamos el body para detectarla, y luego su atributo class para
    // saber cuándo se abre (gana .is-open) y re-aplicar el acordeón —el
    // contenido se reconstruye en cada apertura—.
    function engancharOverlay(overlay) {
      if (overlay.__accObs) return;
      overlay.__accObs = new MutationObserver(function () {
        if (overlay.classList.contains('is-open')) {
          // tras el reflow del innerHTML recién escrito.
          requestAnimationFrame(function () { aplicarAcordeon(overlay); });
        }
      });
      overlay.__accObs.observe(overlay, { attributes: true, attributeFilter: ['class'] });
      if (overlay.classList.contains('is-open')) aplicarAcordeon(overlay);
    }

    var existente = document.querySelector('.cop-modal-overlay');
    if (existente) engancharOverlay(existente);

    var bodyObs = new MutationObserver(function (muts) {
      for (var i = 0; i < muts.length; i++) {
        var nodos = muts[i].addedNodes;
        for (var j = 0; j < nodos.length; j++) {
          var n = nodos[j];
          if (n.nodeType === 1 && n.classList && n.classList.contains('cop-modal-overlay')) {
            engancharOverlay(n);
          }
        }
      }
    });
    bodyObs.observe(document.body, { childList: true });

    // Si el usuario rota o cambia el tamaño con la ficha abierta, re-evalúa.
    var onMq = function () {
      var ov = document.querySelector('.cop-modal-overlay.is-open');
      if (ov) aplicarAcordeon(ov);
    };
    if (mq.addEventListener) mq.addEventListener('change', onMq);
    else if (mq.addListener) mq.addListener(onMq);
  }

  function init() {
    initRibbon();
    initAcordeon();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
