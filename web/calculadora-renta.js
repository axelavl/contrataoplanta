/* ──────────────────────────────────────────────────────────────────────────
   calculadora-renta.js · Lógica de la calculadora de renta por grado (EUS)
   Depende de escala-eus-data.js (window.ESCALA_EUS).
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var EUS = window.ESCALA_EUS;
  if (!EUS) return;

  var fmt = new Intl.NumberFormat('es-CL', {
    style: 'currency', currency: 'CLP', maximumFractionDigits: 0
  });

  function $(id) { return document.getElementById(id); }

  var selEst = $('calc-estamento');
  var selGrado = $('calc-grado');
  var selVinculo = $('calc-vinculo');
  var chkReajuste = $('calc-reajuste');
  var resultado = $('calc-resultado');
  var tablaBody = $('calc-tabla-body');
  var tablaEstLabel = $('calc-tabla-estamento');

  /* ── Poblar grados según estamento ── */
  function poblarGrados() {
    var grados = EUS.gradosDe(selEst.value);
    selGrado.innerHTML = '';
    grados.forEach(function (g) {
      var opt = document.createElement('option');
      opt.value = g;
      opt.textContent = 'Grado ' + g;
      selGrado.appendChild(opt);
    });
  }

  /* ── Calcular y pintar resultado principal ── */
  function calcular() {
    var est = selEst.value;
    var grado = parseInt(selGrado.value, 10);
    var r = EUS.calcular(est, grado, {
      vinculo: selVinculo.value,
      aplicarReajuste: chkReajuste.checked
    });
    if (!r) { resultado.innerHTML = ''; return; }

    var brutoMostrar = r.reajusteAplicado ? r.brutoReajustado : r.bruto;
    var descPct = Math.round(r.tasaDescuento * 100);

    resultado.innerHTML =
      '<div class="calc-cards">' +
        '<div class="calc-card calc-card--bruto">' +
          '<div class="calc-card-label">Renta bruta mensual</div>' +
          '<div class="calc-card-valor">' + fmt.format(brutoMostrar) + '</div>' +
          '<div class="calc-card-desc">' +
            (r.reajusteAplicado ? 'Incluye reajuste 2026 (+3,4%)' : 'Sin reajuste 2026') +
          '</div>' +
        '</div>' +
        '<div class="calc-card calc-card--liquido">' +
          '<div class="calc-card-label">Líquido estimado</div>' +
          '<div class="calc-card-valor">' + fmt.format(r.liquidoEstimado) + '</div>' +
          '<div class="calc-card-desc">Tras ~' + descPct + '% de descuentos previsionales</div>' +
        '</div>' +
      '</div>' +
      '<div class="calc-desglose">' +
        '<div class="calc-desglose-fila"><span>Sueldo base (grado ' + grado + ')</span><span>' + (r.base ? fmt.format(r.base) : '—') + '</span></div>' +
        '<div class="calc-desglose-fila"><span>Asignaciones e incrementos</span><span>' + fmt.format(brutoMostrar - (r.base || 0)) + '</span></div>' +
        '<div class="calc-desglose-fila calc-desglose-total"><span>Total haberes (bruto)</span><span>' + fmt.format(brutoMostrar) + '</span></div>' +
      '</div>' +
      '<p class="calc-aviso">Valor referencial. La liquidación real depende del servicio, asignaciones específicas (zona, modernización, bienios), sistema de salud y antigüedad. ' +
      'Si la oferta indica una renta distinta, prevalece la de la oferta.</p>';
  }

  /* ── Tabla completa del estamento ── */
  function pintarTabla() {
    var est = selEst.value;
    var info = EUS.estamentos[est];
    if (!info) return;
    tablaEstLabel.textContent = info.etiqueta;
    var grados = EUS.gradosDe(est);
    var rows = grados.map(function (g) {
      var r = EUS.calcular(est, g, {
        vinculo: selVinculo.value,
        aplicarReajuste: chkReajuste.checked
      });
      var bruto = r.reajusteAplicado ? r.brutoReajustado : r.bruto;
      return '<tr><td>Grado ' + g + '</td><td>' + fmt.format(bruto) +
             '</td><td>' + fmt.format(r.liquidoEstimado) + '</td></tr>';
    });
    tablaBody.innerHTML = rows.join('');
  }

  function actualizarTodo() { calcular(); pintarTabla(); }

  /* ── Init ── */
  poblarGrados();
  actualizarTodo();

  selEst.addEventListener('change', function () { poblarGrados(); actualizarTodo(); });
  selGrado.addEventListener('change', calcular);
  selVinculo.addEventListener('change', actualizarTodo);
  chkReajuste.addEventListener('change', actualizarTodo);
})();
