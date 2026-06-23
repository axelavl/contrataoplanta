/* ──────────────────────────────────────────────────────────────────────────
   preparacion.js · Hub de preparación + mini-simulacro de conocimientos del Estado
   Banco de preguntas verificado contra normativa vigente (referencial, no asesoría
   legal). Fuentes: Ley 19.880 (Bases de los Procedimientos Administrativos),
   Ley 18.834 (Estatuto Administrativo), Ley 20.880 (Probidad).
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  // banco: cada pregunta tiene texto, opciones, índice correcto y explicación.
  var BANCO = [
    {
      q: 'Como regla general, ¿cuál es el plazo máximo de un procedimiento administrativo?',
      o: ['30 días', '3 meses', '6 meses', '1 año'],
      c: 2,
      e: 'La Ley 19.880 (art. 27) fija que, salvo caso fortuito o fuerza mayor, el procedimiento no podrá exceder de 6 meses desde su inicio hasta la resolución final.'
    },
    {
      q: 'En el cómputo de plazos de un procedimiento administrativo, ¿qué días son inhábiles?',
      o: ['Solo los domingos', 'Sábados, domingos y festivos', 'Solo los festivos', 'Ninguno: corren todos los días'],
      c: 1,
      e: 'La Ley 19.880 (art. 25) establece que los plazos se computan en días hábiles, entendiéndose inhábiles los sábados, domingos y festivos.'
    },
    {
      q: 'Si la Administración no responde a tiempo una solicitud del interesado, la regla general es que el silencio sea:',
      o: ['Negativo (rechazada)', 'Positivo (aceptada)', 'Sin efecto', 'Prórroga automática'],
      c: 1,
      e: 'La Ley 19.880 (art. 64) establece el silencio positivo como regla general en solicitudes del interesado, salvo excepciones del art. 65 (p. ej. cuando afecta el patrimonio fiscal o la Administración actúa de oficio).'
    },
    {
      q: '¿Hasta cuándo dura, como máximo, un empleo a contrata?',
      o: ['6 meses', 'Hasta el 31 de diciembre de cada año', '2 años', 'Indefinido tras 1 año'],
      c: 1,
      e: 'En el Estatuto Administrativo, los empleos a contrata son transitorios y duran, como máximo, hasta el 31 de diciembre de cada año, salvo prórroga.'
    },
    {
      q: '¿Cuál de estos NO es funcionario público regido por el Estatuto Administrativo?',
      o: ['Personal de planta', 'Personal a contrata', 'Personal a honorarios', 'Suplente'],
      c: 2,
      e: 'El personal a honorarios no es funcionario público: se rige por su contrato (no por el Estatuto Administrativo) y, por regla general, no tiene los derechos estatutarios.'
    },
    {
      q: 'La jornada ordinaria de trabajo en la Administración del Estado es de:',
      o: ['45 horas semanales', '44 horas semanales', '40 horas semanales', '48 horas semanales'],
      c: 1,
      e: 'El Estatuto Administrativo fija una jornada ordinaria de 44 horas semanales, distribuidas de lunes a viernes, sin exceder de 9 horas diarias.'
    },
    {
      q: 'El feriado legal de un funcionario con menos de 15 años de servicio es de:',
      o: ['10 días hábiles', '15 días hábiles', '20 días hábiles', '25 días hábiles'],
      c: 1,
      e: 'Son 15 días hábiles con menos de 15 años de servicio; 20 días entre 15 y 20 años; y 25 días con 20 o más años de servicio.'
    },
    {
      q: 'El principio de probidad administrativa implica:',
      o: [
        'Preeminencia del interés general sobre el particular',
        'Reserva total de los actos del Estado',
        'Libertad para contratar con familiares',
        'Prioridad del interés del servicio jefe'
      ],
      c: 0,
      e: 'La probidad (Ley 20.880) exige una conducta funcionaria intachable y un desempeño honesto y leal, con preeminencia del interés general sobre el particular.'
    },
    {
      q: '¿Cuál es un requisito de ingreso a la Administración del Estado?',
      o: [
        'Tener menos de 30 años',
        'Salud compatible con el desempeño del cargo',
        'Residir en la región del servicio',
        'Experiencia previa en el sector público'
      ],
      c: 1,
      e: 'Entre los requisitos de ingreso (Estatuto Administrativo, art. 12) están: ser ciudadano, situación militar al día, salud compatible, nivel educacional o título exigido, y no estar inhabilitado para cargos públicos.'
    },
    {
      q: 'El ingreso a los cargos de planta de carrera se efectúa, por regla general, mediante:',
      o: ['Designación directa', 'Concurso público', 'Sorteo', 'Antigüedad'],
      c: 1,
      e: 'El ingreso a la planta de carrera se realiza por concurso público, en el grado de ingreso del respectivo estamento, conforme al principio de igualdad y mérito.'
    }
  ];

  var GRATIS = BANCO.length; // v1: todas gratis. El premium ofrece el simulacro extendido.

  function $(id) { return document.getElementById(id); }

  var cont = $('quiz-cont');
  if (!cont) return;

  var estado = { idx: 0, aciertos: 0, respondida: false };

  function pintarPregunta() {
    var p = BANCO[estado.idx];
    estado.respondida = false;
    var opciones = p.o.map(function (op, i) {
      return '<button class="quiz-op" data-i="' + i + '">' +
        '<span class="quiz-op-letra">' + String.fromCharCode(65 + i) + '</span>' +
        '<span>' + op + '</span></button>';
    }).join('');
    cont.innerHTML =
      '<div class="quiz-progreso">Pregunta ' + (estado.idx + 1) + ' de ' + GRATIS +
        ' · Aciertos: ' + estado.aciertos + '</div>' +
      '<div class="quiz-pregunta">' + p.q + '</div>' +
      '<div class="quiz-opciones">' + opciones + '</div>' +
      '<div class="quiz-feedback" id="quiz-feedback"></div>' +
      '<div class="quiz-nav"><button class="quiz-next" id="quiz-next" disabled>Siguiente →</button></div>';

    Array.prototype.forEach.call(cont.querySelectorAll('.quiz-op'), function (btn) {
      btn.addEventListener('click', function () { responder(parseInt(btn.dataset.i, 10)); });
    });
    $('quiz-next').addEventListener('click', siguiente);
  }

  function responder(i) {
    if (estado.respondida) return;
    estado.respondida = true;
    var p = BANCO[estado.idx];
    var correcta = i === p.c;
    if (correcta) estado.aciertos++;
    Array.prototype.forEach.call(cont.querySelectorAll('.quiz-op'), function (btn) {
      var bi = parseInt(btn.dataset.i, 10);
      btn.disabled = true;
      if (bi === p.c) btn.classList.add('quiz-op--ok');
      else if (bi === i) btn.classList.add('quiz-op--mal');
    });
    $('quiz-feedback').innerHTML =
      '<div class="quiz-fb ' + (correcta ? 'quiz-fb--ok' : 'quiz-fb--mal') + '">' +
      (correcta ? '✓ Correcto. ' : '✗ Incorrecto. ') + p.e + '</div>';
    $('quiz-next').disabled = false;
  }

  function siguiente() {
    estado.idx++;
    if (estado.idx >= GRATIS) { pintarResultado(); return; }
    pintarPregunta();
  }

  function pintarResultado() {
    var pct = Math.round((estado.aciertos / GRATIS) * 100);
    var msg = pct >= 80 ? '¡Excelente base!' : pct >= 50 ? 'Vas bien, sigue repasando.' : 'A reforzar la normativa.';
    cont.innerHTML =
      '<div class="quiz-resultado">' +
        '<div class="quiz-resultado-pct">' + pct + '%</div>' +
        '<div class="quiz-resultado-msg">' + estado.aciertos + ' de ' + GRATIS + ' correctas — ' + msg + '</div>' +
        '<button class="quiz-next" id="quiz-reiniciar">Reintentar</button>' +
        '<div class="quiz-premium">' +
          '<strong>¿Quieres el simulacro completo?</strong> Estamos preparando 50+ preguntas con cronómetro, ' +
          'explicaciones ampliadas y modo por institución. Activa una alerta y te avisamos primero.' +
          '<a class="quiz-premium-cta" href="index.html#alertas">Activar alerta →</a>' +
        '</div>' +
      '</div>';
    $('quiz-reiniciar').addEventListener('click', function () {
      estado = { idx: 0, aciertos: 0, respondida: false };
      pintarPregunta();
    });
  }

  pintarPregunta();
})();
