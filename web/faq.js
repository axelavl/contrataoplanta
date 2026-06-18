/* faq.js — acordeón de preguntas frecuentes.
   Externo (no inline) para cumplir la CSP `script-src 'self'`; antes la lógica
   vivía inline y el navegador la bloqueaba, dejando las respuestas sin abrir.
   El contador de favoritos del nav lo gestiona shared-shell.js. */
(function () {
  'use strict';
  function init() {
    var preguntas = document.querySelectorAll('.faq-pregunta');
    if (!preguntas.length) return;
    preguntas.forEach(function (btn) {
      btn.addEventListener('click', function () {
        var open = btn.getAttribute('aria-expanded') === 'true';
        preguntas.forEach(function (x) {
          x.setAttribute('aria-expanded', 'false');
          if (x.nextElementSibling) x.nextElementSibling.style.display = 'none';
          var ic = x.querySelector('.faq-icono');
          if (ic) ic.textContent = '＋';
        });
        if (!open) {
          btn.setAttribute('aria-expanded', 'true');
          if (btn.nextElementSibling) btn.nextElementSibling.style.display = 'block';
          var icon = btn.querySelector('.faq-icono');
          if (icon) icon.textContent = '−';
        }
      });
    });
  }
  if (document.readyState !== 'loading') init();
  else document.addEventListener('DOMContentLoaded', init);
})();
