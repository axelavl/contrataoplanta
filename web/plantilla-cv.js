/* plantilla-cv.js · Acción de impresión/PDF para la plantilla de CV.
   JS externo para cumplir el CSP (script-src 'self', sin inline). */
(function () {
  'use strict';
  var b = document.getElementById('btn-pdf');
  if (b) b.addEventListener('click', function () { window.print(); });
})();
