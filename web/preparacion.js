/* ──────────────────────────────────────────────────────────────────────────
   preparacion.js · Hub de preparación + mini-simulacro de conocimientos del Estado
   Banco verificado contra normativa vigente (referencial, no asesoría legal).
   Cada pregunta lleva su fuente directa (ley + artículo) en el campo `f`.
   Fuentes consultadas (BCN / leychile.cl):
     · Ley 19.880  — Bases de los Procedimientos Administrativos
     · DFL 29 / Ley 18.834 — Estatuto Administrativo
     · Ley 18.575 — Bases Generales de la Administración del Estado
     · Ley 20.880 — Probidad en la Función Pública
     · Ley 19.886 — Compras Públicas (texto vigente, incl. Ley 21.634)
     · Ley 20.285 — Transparencia y Acceso a la Información Pública
     · Ley 20.730 — Lobby
     · Ley 10.336 y Constitución Política — Contraloría y organización del Estado

   Funciones:
     · Rotación: en cada ingreso elige 5 preguntas al azar evitando las
       recientemente mostradas (memoria en localStorage) hasta agotar el banco.
     · Filtro por tema/ley: el usuario puede practicar todo o una sola materia.
     · Modo contrarreloj: cuenta atrás por pregunta (se revela la respuesta al
       agotarse el tiempo).
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  // Banco: q=pregunta, o=opciones, c=índice correcto, e=explicación, f=fuente.
  var BANCO = [
    /* ── Ley 19.880 · Procedimiento administrativo ───────────────────────── */
    {
      q: 'Como regla general, ¿cuál es el plazo máximo de un procedimiento administrativo?',
      o: ['30 días', '3 meses', '6 meses', '1 año'],
      c: 2,
      e: 'Salvo caso fortuito o fuerza mayor, el procedimiento no puede exceder de 6 meses desde su inicio hasta la resolución final.',
      f: 'Ley 19.880, art. 27'
    },
    {
      q: 'En el cómputo de plazos de un procedimiento administrativo, ¿qué días son inhábiles?',
      o: ['Solo los domingos', 'Sábados, domingos y festivos', 'Solo los festivos', 'Ninguno: corren todos los días'],
      c: 1,
      e: 'Los plazos se computan en días hábiles, entendiéndose inhábiles los sábados, domingos y festivos.',
      f: 'Ley 19.880, art. 25'
    },
    {
      q: 'Si la Administración no responde a tiempo una solicitud del interesado, la regla general es que el silencio sea:',
      o: ['Negativo (rechazada)', 'Positivo (aceptada)', 'Sin efecto', 'Prórroga automática'],
      c: 1,
      e: 'La regla general es el silencio positivo: denunciado el incumplimiento, si la autoridad no resuelve en 5 días la solicitud se entiende aceptada. El silencio negativo es la excepción del art. 65.',
      f: 'Ley 19.880, art. 64'
    },
    {
      q: 'Un acto administrativo se expresa, por regla general, a través de:',
      o: ['Oficios y memos', 'Decretos supremos y resoluciones', 'Circulares internas', 'Actas de reunión'],
      c: 1,
      e: 'Las decisiones formales de los órganos de la Administración toman la forma de decretos supremos y resoluciones.',
      f: 'Ley 19.880, art. 3'
    },
    {
      q: 'Desde su entrada en vigencia, los actos administrativos gozan de:',
      o: ['Presunción de legalidad, imperio y exigibilidad', 'Inmunidad ante los tribunales', 'Reserva absoluta', 'Carácter consultivo'],
      c: 0,
      e: 'Gozan de una presunción de legalidad, de imperio y exigibilidad frente a sus destinatarios, lo que autoriza su ejecución de oficio salvo orden de suspensión.',
      f: 'Ley 19.880, art. 3'
    },
    {
      q: 'El recurso de reposición contra un acto administrativo se interpone, ante el mismo órgano, dentro de:',
      o: ['5 días', '10 días', '30 días', '60 días'],
      c: 0,
      e: 'Se interpone dentro de 5 días ante el mismo órgano que dictó el acto; en subsidio puede deducirse el recurso jerárquico.',
      f: 'Ley 19.880, art. 59'
    },
    {
      q: 'Frente a un procedimiento administrativo especial regulado por otra ley, la Ley 19.880 se aplica:',
      o: ['No se aplica', 'Como norma supletoria', 'Con preferencia sobre la ley especial', 'Solo si las partes lo acuerdan'],
      c: 1,
      e: 'La Ley 19.880 fija las bases del procedimiento y, ante procedimientos especiales, rige con carácter supletorio.',
      f: 'Ley 19.880, art. 1'
    },
    {
      q: 'La ampliación de un plazo dentro del procedimiento administrativo no puede exceder de:',
      o: ['La mitad del plazo', 'El doble del plazo', 'Un plazo igual', '90 días'],
      c: 0,
      e: 'La Administración puede ampliar los plazos, antes de su vencimiento y por una sola vez, sin que la ampliación exceda de la mitad de los mismos. Nunca un plazo ya vencido.',
      f: 'Ley 19.880, art. 26'
    },
    {
      q: 'Un procedimiento administrativo puede iniciarse:',
      o: ['Solo de oficio', 'Solo a solicitud de parte', 'De oficio o a solicitud de parte', 'Solo por orden judicial'],
      c: 2,
      e: 'El procedimiento puede iniciarse de oficio por la propia Administración o a solicitud de persona interesada.',
      f: 'Ley 19.880, art. 28'
    },
    {
      q: 'La autoridad puede invalidar de oficio o a petición de parte un acto contrario a derecho dentro de:',
      o: ['6 meses', '1 año', '2 años', '5 años'],
      c: 2,
      e: 'La invalidación procede, previa audiencia del interesado, dentro de los 2 años contados desde la notificación o publicación del acto.',
      f: 'Ley 19.880, art. 53'
    },
    {
      q: 'El recurso extraordinario de revisión contra un acto administrativo firme se interpone dentro del plazo de:',
      o: ['5 días', '30 días', '6 meses', '1 año'],
      c: 3,
      e: 'Procede ante el superior jerárquico por causales tasadas (error de hecho manifiesto, documentos falsos, cohecho, etc.), dentro del plazo de 1 año.',
      f: 'Ley 19.880, art. 60'
    },
    {
      q: '¿En cuál de estos casos el silencio de la Administración es negativo (se entiende rechazada la solicitud)?',
      o: ['Cuando afecta el patrimonio fiscal', 'En cualquier solicitud', 'Cuando el interesado lo pide', 'Nunca'],
      c: 0,
      e: 'El silencio negativo opera cuando la solicitud afecta el patrimonio fiscal, cuando la Administración actúa de oficio, cuando debe pronunciarse sobre impugnaciones o cuando se ejerce el derecho de petición.',
      f: 'Ley 19.880, art. 65'
    },
    {
      q: 'Una notificación por carta certificada se entiende practicada:',
      o: ['El día del envío', 'Al tercer día siguiente a su recepción en la oficina de Correos', 'A los 5 días', 'Cuando el interesado firma'],
      c: 1,
      e: 'La notificación por carta certificada se entiende practicada a contar del tercer día siguiente a su recepción en la oficina de Correos.',
      f: 'Ley 19.880, art. 46'
    },
    {
      q: '¿Cuál de los siguientes NO es un principio del procedimiento administrativo?',
      o: ['Celeridad', 'Economía procedimental', 'Onerosidad', 'Imparcialidad'],
      c: 2,
      e: 'El procedimiento se rige, entre otros, por los principios de escrituración, gratuidad (no onerosidad), celeridad, economía procedimental, contradictoriedad e imparcialidad.',
      f: 'Ley 19.880, art. 4'
    },

    /* ── DFL 29 / Ley 18.834 · Estatuto Administrativo ───────────────────── */
    {
      q: '¿Hasta cuándo dura, como máximo, un empleo a contrata?',
      o: ['6 meses', 'Hasta el 31 de diciembre de cada año', '2 años', 'Indefinido tras 1 año'],
      c: 1,
      e: 'Los empleos a contrata son transitorios y duran, como máximo, hasta el 31 de diciembre de cada año, expirando por el solo ministerio de la ley salvo prórroga.',
      f: 'Estatuto Administrativo (DFL 29), art. 10'
    },
    {
      q: '¿Cuál de estos NO es funcionario público regido por el Estatuto Administrativo?',
      o: ['Personal de planta', 'Personal a contrata', 'Personal a honorarios', 'Suplente'],
      c: 2,
      e: 'El personal a honorarios se rige por su respectivo contrato y no le son aplicables las disposiciones del Estatuto Administrativo: no tiene la calidad de funcionario público.',
      f: 'Estatuto Administrativo (DFL 29), art. 11'
    },
    {
      q: 'La jornada ordinaria de trabajo en la Administración del Estado es de:',
      o: ['45 horas semanales', '44 horas semanales', '40 horas semanales', '48 horas semanales'],
      c: 1,
      e: 'La jornada ordinaria es de 44 horas semanales, distribuidas de lunes a viernes, sin exceder de 9 horas diarias.',
      f: 'Estatuto Administrativo (DFL 29), art. 65'
    },
    {
      q: 'El feriado legal de un funcionario con menos de 15 años de servicio es de:',
      o: ['10 días hábiles', '15 días hábiles', '20 días hábiles', '25 días hábiles'],
      c: 1,
      e: 'Son 15 días hábiles con menos de 15 años de servicio; 20 días hábiles entre 15 y 20 años; y 25 días hábiles con 20 o más años de servicio.',
      f: 'Estatuto Administrativo (DFL 29), art. 103'
    },
    {
      q: '¿Cuál es un requisito de ingreso a la Administración del Estado?',
      o: ['Tener menos de 30 años', 'Salud compatible con el desempeño del cargo', 'Residir en la región del servicio', 'Experiencia previa en el sector público'],
      c: 1,
      e: 'Entre los requisitos de ingreso están ser ciudadano, tener la situación militar al día, salud compatible con el cargo, el nivel educacional o título exigido, y no estar inhabilitado para cargos públicos.',
      f: 'Estatuto Administrativo (DFL 29), art. 12'
    },
    {
      q: 'El ingreso a los cargos de planta de carrera se efectúa, por regla general, mediante:',
      o: ['Designación directa', 'Concurso público', 'Sorteo', 'Antigüedad'],
      c: 1,
      e: 'El ingreso a la planta de carrera en calidad de titular se realiza por concurso público, en el último grado de la planta respectiva.',
      f: 'Estatuto Administrativo (DFL 29), art. 17'
    },
    {
      q: 'El permiso administrativo por motivos particulares, con goce de remuneraciones, es de hasta:',
      o: ['3 días al año', '6 días hábiles al año', '10 días al año', '15 días al año'],
      c: 1,
      e: 'El funcionario puede solicitar permiso por motivos particulares hasta por 6 días hábiles al año, con goce de remuneraciones, fraccionables por días o medios días.',
      f: 'Estatuto Administrativo (DFL 29), art. 109'
    },
    {
      q: 'Las listas del sistema de calificación de los funcionarios son:',
      o: ['A, B, C y D', 'Distinción, Buena, Condicional y Eliminación', 'Excelente, Aprobado y Reprobado', 'Titular, Suplente y Subrogante'],
      c: 1,
      e: 'La calificación anual ubica al funcionario en Lista Nº 1 de Distinción, Lista Nº 2 Buena, Lista Nº 3 Condicional o Lista Nº 4 de Eliminación.',
      f: 'Estatuto Administrativo (DFL 29), art. 33'
    },
    {
      q: '¿Cuál de las siguientes es una medida disciplinaria del Estatuto Administrativo?',
      o: ['Amonestación verbal', 'Destitución', 'Rebaja de grado', 'Traslado forzoso'],
      c: 1,
      e: 'Las medidas disciplinarias son censura, multa, suspensión del empleo y destitución, aplicadas según la gravedad de la falta.',
      f: 'Estatuto Administrativo (DFL 29), art. 121'
    },
    {
      q: 'La multa como medida disciplinaria consiste en privar de un porcentaje de la remuneración mensual:',
      o: ['No inferior al 5% ni superior al 20%', 'Hasta el 50%', 'Un monto fijo en UTM', 'Equivalente a un mes de sueldo'],
      c: 0,
      e: 'La multa priva de un porcentaje de la remuneración mensual, no inferior al 5% ni superior al 20%.',
      f: 'Estatuto Administrativo (DFL 29), art. 123'
    },
    {
      q: 'El número de funcionarios a contrata no puede exceder de un porcentaje de los cargos de la planta. ¿Cuál?',
      o: ['10%', '20%', '50%', 'No hay límite'],
      c: 1,
      e: 'Como regla general, el personal a contrata no puede exceder del 20% del total de los cargos de la planta de la institución.',
      f: 'Estatuto Administrativo (DFL 29), art. 10'
    },
    {
      q: 'En una investigación sumaria, la etapa de investigación no puede exceder de:',
      o: ['5 días', '15 días', '30 días', '60 días'],
      c: 0,
      e: 'La investigación sumaria es un procedimiento simplificado y mayormente verbal cuya etapa de investigación no excede de 5 días; por regla general no permite aplicar destitución.',
      f: 'Estatuto Administrativo (DFL 29), art. 126'
    },
    {
      q: 'Se entiende por trabajo nocturno el realizado entre:',
      o: ['Las 20:00 y las 6:00', 'Las 21:00 de un día y las 7:00 del día siguiente', 'Las 22:00 y las 8:00', 'Las 00:00 y las 6:00'],
      c: 1,
      e: 'Para efectos de los trabajos extraordinarios, es nocturno el realizado entre las 21:00 horas de un día y las 7:00 horas del día siguiente.',
      f: 'Estatuto Administrativo (DFL 29), art. 67'
    },
    {
      q: '¿Cuál de las siguientes es una causal de cesación de funciones?',
      o: ['Cambio de jefatura', 'Destitución', 'Reorganización del servicio', 'Vencimiento del feriado'],
      c: 1,
      e: 'Son causales de cesación, entre otras: aceptación de renuncia, jubilación, declaración de vacancia, destitución, supresión del empleo, término del período legal y fallecimiento.',
      f: 'Estatuto Administrativo (DFL 29), art. 146'
    },
    {
      q: 'La destitución procede, entre otras causales, cuando el funcionario:',
      o: ['Llega tarde una vez', 'Se ausenta más de 3 días consecutivos sin causa justificada', 'Pide feriado', 'Solicita permiso sin goce'],
      c: 1,
      e: 'La destitución procede cuando los hechos vulneran gravemente la probidad, o en causales tasadas como ausentarse por más de 3 días consecutivos sin justificación o ser condenado por crimen o simple delito.',
      f: 'Estatuto Administrativo (DFL 29), art. 125'
    },
    {
      q: 'El feriado legal de un funcionario con 20 o más años de servicio es de:',
      o: ['15 días hábiles', '20 días hábiles', '25 días hábiles', '30 días hábiles'],
      c: 2,
      e: 'Con 20 o más años de servicio el feriado es de 25 días hábiles. Para el feriado, el día sábado se considera inhábil.',
      f: 'Estatuto Administrativo (DFL 29), art. 103'
    },
    {
      q: 'El permiso sin goce de remuneraciones por motivos particulares puede otorgarse hasta por:',
      o: ['1 mes al año', '6 meses al año', '1 año', '2 años'],
      c: 1,
      e: 'Por motivos particulares puede concederse permiso sin goce de remuneraciones hasta por 6 meses en cada año calendario (y hasta 2 años para permanecer en el extranjero).',
      f: 'Estatuto Administrativo (DFL 29), art. 110'
    },
    {
      q: 'Durante una licencia médica, el funcionario:',
      o: ['Pierde sus remuneraciones', 'Conserva el total de sus remuneraciones', 'Recibe la mitad', 'Pasa a honorarios'],
      c: 1,
      e: 'Mientras dure la licencia médica, el funcionario conserva el total de sus remuneraciones.',
      f: 'Estatuto Administrativo (DFL 29), art. 111'
    },
    {
      q: 'Es un deber del funcionario respecto de hechos con caracteres de delito de los que tome conocimiento:',
      o: ['Guardar silencio', 'Denunciarlos al Ministerio Público', 'Resolverlos por sí mismo', 'Informar solo a su jefatura'],
      c: 1,
      e: 'El funcionario debe denunciar al Ministerio Público (o a la policía) los crímenes o simples delitos, y a la autoridad competente los hechos de carácter irregular que conozca.',
      f: 'Estatuto Administrativo (DFL 29), art. 61'
    },

    /* ── Ley 18.575 · Bases Generales de la Administración del Estado ─────── */
    {
      q: 'Según la Ley de Bases, la Administración del Estado está al servicio de:',
      o: ['El gobierno de turno', 'La persona humana', 'Los funcionarios', 'Los partidos políticos'],
      c: 1,
      e: 'La Administración del Estado está al servicio de la persona humana; su finalidad es promover el bien común atendiendo las necesidades públicas en forma continua y permanente.',
      f: 'Ley 18.575, art. 3'
    },
    {
      q: 'El principio de probidad administrativa exige:',
      o: ['Preeminencia del interés general sobre el particular', 'Reserva total de los actos del Estado', 'Libertad para contratar con familiares', 'Prioridad del interés del jefe del servicio'],
      c: 0,
      e: 'La probidad consiste en observar una conducta funcionaria intachable y un desempeño honesto y leal de la función, con preeminencia del interés general sobre el particular.',
      f: 'Ley 18.575, art. 52'
    },
    {
      q: 'Cuando un órgano de la Administración causa daño por falta de servicio:',
      o: ['No hay responsabilidad', 'El Estado es responsable', 'Responde solo el ministro', 'Responde el particular afectado'],
      c: 1,
      e: 'Los órganos de la Administración son responsables del daño que causen por falta de servicio; el Estado tiene derecho a repetir contra el funcionario que incurrió en falta personal.',
      f: 'Ley 18.575, art. 42'
    },
    {
      q: 'No puede ingresar a la Administración quien tenga contratos o cauciones con el respectivo organismo por un monto igual o superior a:',
      o: ['50 UTM', '100 UTM', '200 UTM', '500 UTM'],
      c: 2,
      e: 'Constituye inhabilidad de ingreso tener contratos o cauciones ascendentes a 200 UTM o más, o litigios pendientes, con el organismo de que se trata.',
      f: 'Ley 18.575, art. 54'
    },
    {
      q: 'La delegación de atribuciones de una autoridad administrativa debe ser:',
      o: ['Total e irrevocable', 'Parcial, en materias específicas y revocable', 'Permanente', 'Verbal'],
      c: 1,
      e: 'La delegación debe ser parcial y recaer en materias específicas; la responsabilidad recae en el delegado y es esencialmente revocable.',
      f: 'Ley 18.575, art. 41'
    },
    {
      q: '¿Cuál de los siguientes integra la Administración del Estado?',
      o: ['El Congreso Nacional', 'Las municipalidades', 'Los tribunales de justicia', 'El Tribunal Constitucional'],
      c: 1,
      e: 'La Administración del Estado la integran, entre otros, los ministerios, los gobiernos regionales, las municipalidades, la Contraloría, el Banco Central, las Fuerzas Armadas y de Orden, y los servicios públicos.',
      f: 'Ley 18.575, art. 1'
    },
    {
      q: 'El control jerárquico permanente que ejercen las jefaturas se extiende a:',
      o: ['Solo la legalidad', 'Solo la eficiencia', 'La legalidad, oportunidad, eficiencia y eficacia de las actuaciones', 'Solo el cumplimiento horario'],
      c: 2,
      e: 'Las autoridades y jefaturas ejercen un control jerárquico permanente del funcionamiento de los órganos y de la actuación del personal, extendido tanto a la eficiencia y eficacia como a la legalidad y oportunidad.',
      f: 'Ley 18.575, art. 11'
    },

    /* ── Ley 20.880 · Probidad y conflictos de intereses ─────────────────── */
    {
      q: 'La declaración de intereses y patrimonio debe presentarse dentro de cuántos días desde la asunción del cargo?',
      o: ['10 días', '30 días', '60 días', '90 días'],
      c: 1,
      e: 'Debe efectuarse dentro de los 30 días siguientes a la asunción del cargo, actualizarse anualmente en marzo y presentarse nuevamente dentro de los 30 días posteriores al cese.',
      f: 'Ley 20.880, art. 5'
    },
    {
      q: 'La declaración de intereses y patrimonio se actualiza, como regla general:',
      o: ['Cada cinco años', 'Anualmente durante marzo', 'Solo al dejar el cargo', 'Nunca'],
      c: 1,
      e: 'La declaración se actualiza anualmente durante el mes de marzo, y además dentro de los 30 días siguientes a dejar el cargo.',
      f: 'Ley 20.880, art. 5'
    },
    {
      q: 'El llamado "fideicomiso ciego" se denomina técnicamente, en la ley chilena:',
      o: ['Fideicomiso ciego', 'Mandato especial de administración de cartera de valores', 'Sociedad de inversión', 'Cuenta vista'],
      c: 1,
      e: 'La ley no usa la expresión "fideicomiso ciego": el instrumento es el mandato especial de administración de cartera de valores, en que el mandatario administra a nombre propio y a riesgo de la autoridad.',
      f: 'Ley 20.880, arts. 23 y 24'
    },
    {
      q: 'Las autoridades obligadas deben constituir mandato o enajenar cuando su cartera de valores de oferta pública supera:',
      o: ['1.000 UF', '5.000 UF', '25.000 UF', '100.000 UF'],
      c: 2,
      e: 'Si el valor total de las acciones, bonos y demás valores de oferta pública supera las 25.000 UF, la autoridad debe optar, dentro de 90 días, entre constituir el mandato especial o vender el exceso.',
      f: 'Ley 20.880, art. 26'
    },
    {
      q: '¿Qué órgano fiscaliza la oportunidad, integridad y veracidad de la declaración de intereses y patrimonio?',
      o: ['El Servicio de Impuestos Internos', 'La Contraloría General de la República', 'El Consejo para la Transparencia', 'El Ministerio Público'],
      c: 1,
      e: 'La Contraloría General fiscaliza la oportunidad, integridad y veracidad del contenido de las declaraciones de los sujetos del Capítulo 1°.',
      f: 'Ley 20.880, art. 10'
    },
    {
      q: 'Existe conflicto de intereses cuando concurren, a la vez:',
      o: ['Dos cargos públicos', 'El interés general de la función con un interés particular de quien la ejerce', 'Dos funcionarios del mismo grado', 'Un feriado y un permiso'],
      c: 1,
      e: 'Hay conflicto de intereses cuando concurre el interés general propio del ejercicio de la función con un interés particular, económico o no, del funcionario o de terceros vinculados, o circunstancias que le resten imparcialidad.',
      f: 'Ley 20.880, art. 1'
    },

    /* ── Ley 19.886 · Compras públicas ───────────────────────────────────── */
    {
      q: 'Como regla general, los órganos del Estado celebran sus contratos de suministro y servicios mediante:',
      o: ['Trato directo', 'Licitación pública', 'Licitación privada', 'Sorteo entre proveedores'],
      c: 1,
      e: 'La regla general es la licitación pública; la licitación privada y el trato directo son excepcionales y requieren acto fundado.',
      f: 'Ley 19.886, art. 5'
    },
    {
      q: 'La modalidad de "Compra Ágil" permite adquirir bienes o servicios por un monto igual o inferior a:',
      o: ['30 UTM', '100 UTM', '500 UTM', '1.000 UTM'],
      c: 1,
      e: 'La Compra Ágil permite adquirir por monto igual o inferior a 100 UTM, previa solicitud de al menos tres cotizaciones por el sistema (con la reforma de la Ley 21.634 el umbral subió de 30 a 100 UTM).',
      f: 'Ley 19.886, art. 7'
    },
    {
      q: 'El Convenio Marco gestionado por ChileCompra se traduce, para los organismos compradores, en:',
      o: ['Un catálogo electrónico de bienes y servicios', 'Una licitación por cada compra', 'Un trato directo automático', 'Un registro de reclamos'],
      c: 0,
      e: 'Los convenios marco vigentes se traducen en un catálogo electrónico publicado en el sistema (Mercado Público), donde los organismos compran directamente.',
      f: 'Ley 19.886, art. 7'
    },
    {
      q: 'La licitación privada procede, por regla general, solo cuando:',
      o: ['El jefe del servicio lo prefiere', 'En la licitación pública previa no hubo interesados o las ofertas fueron inadmisibles', 'El monto es bajo', 'Hay urgencia política'],
      c: 1,
      e: 'Procede la licitación privada (y luego, si tampoco hay interesados, el trato directo) cuando en la licitación pública no se presentaron oferentes o las ofertas fueron declaradas inadmisibles.',
      f: 'Ley 19.886, art. 8'
    },
    {
      q: 'Para contratar con el Estado bajo la Ley 19.886, el proveedor debe, por regla general:',
      o: ['Ser empresa grande', 'Estar inscrito en el Registro de Proveedores (ChileProveedores)', 'Tener oficina en Santiago', 'Pagar una cuota anual'],
      c: 1,
      e: 'Solo pueden contratar quienes acrediten idoneidad e inscripción en el Registro de Proveedores del Estado, operado como ChileProveedores.',
      f: 'Ley 19.886, art. 4'
    },
    {
      q: 'El órgano jurisdiccional especial que conoce de impugnaciones en los procesos de compras públicas es:',
      o: ['La Contraloría', 'El Tribunal de Contratación Pública', 'El Consejo para la Transparencia', 'El Tribunal Constitucional'],
      c: 1,
      e: 'El Tribunal de Contratación Pública, con asiento en Santiago y competencia nacional, conoce de la acción de impugnación contra actos ilegales o arbitrarios ocurridos entre la aprobación de las bases y la adjudicación.',
      f: 'Ley 19.886, arts. 22 y 24'
    },
    {
      q: 'En una licitación pública, pueden presentar ofertas:',
      o: ['Solo proveedores invitados', 'Cualquier persona que cumpla las bases', 'Solo empresas estatales', 'Solo el proveedor único'],
      c: 1,
      e: 'La licitación pública es un procedimiento concursal con llamado público en que cualquier persona puede presentar ofertas; la privada, en cambio, es por invitación.',
      f: 'Ley 19.886, art. 7'
    },

    /* ── Ley 20.285 · Transparencia y acceso a la información ─────────────── */
    {
      q: 'El plazo general que tiene un órgano del Estado para responder una solicitud de acceso a la información es de:',
      o: ['10 días hábiles', '20 días hábiles', '30 días hábiles', '48 horas'],
      c: 1,
      e: 'El órgano debe pronunciarse, entregando o negando, dentro de 20 días hábiles desde la recepción de la solicitud.',
      f: 'Ley 20.285 (Transparencia), art. 14'
    },
    {
      q: 'El plazo para responder una solicitud de acceso puede prorrogarse excepcionalmente por:',
      o: ['Otros 10 días hábiles', 'Otros 20 días hábiles', 'Un mes', 'No es prorrogable'],
      c: 0,
      e: 'El plazo de 20 días hábiles puede prorrogarse excepcionalmente por otros 10 días hábiles, comunicando la prórroga y sus fundamentos antes del vencimiento.',
      f: 'Ley 20.285 (Transparencia), art. 14'
    },
    {
      q: 'La "transparencia activa" consiste en:',
      o: ['Responder solicitudes ciudadanas', 'Publicar información de oficio y permanentemente en el sitio web', 'Reservar la información', 'Entregar datos solo a periodistas'],
      c: 1,
      e: 'La transparencia activa es la obligación del órgano de mantener a disposición del público, en su sitio web y actualizada al menos una vez al mes, información como estructura, remuneraciones, contrataciones y transferencias. La transparencia pasiva opera a solicitud.',
      f: 'Ley 20.285 (Transparencia), art. 7'
    },
    {
      q: 'Para solicitar información a un órgano del Estado, el ciudadano:',
      o: ['Debe expresar la causa o motivo', 'No necesita expresar causa ni motivo', 'Debe ser periodista', 'Debe pagar una tasa fija'],
      c: 1,
      e: 'Toda persona tiene derecho a solicitar y recibir información; por el principio de no discriminación no se le puede exigir que exprese la causa o motivo de su solicitud.',
      f: 'Ley 20.285 (Transparencia), arts. 10 y 11'
    },
    {
      q: 'Si se deniega el acceso a la información, el solicitante puede presentar un amparo ante el Consejo para la Transparencia dentro de:',
      o: ['5 días', '15 días', '30 días', '60 días'],
      c: 1,
      e: 'El amparo al derecho de acceso se presenta ante el Consejo para la Transparencia dentro de 15 días contados desde la notificación de la denegación o desde que expiró el plazo para responder.',
      f: 'Ley 20.285 (Transparencia), art. 24'
    },
    {
      q: 'El Consejo para la Transparencia es:',
      o: ['Un servicio dependiente del Presidente', 'Una corporación autónoma de derecho público', 'Una comisión del Congreso', 'Un tribunal'],
      c: 1,
      e: 'El Consejo para la Transparencia es una corporación autónoma de derecho público, con personalidad jurídica y patrimonio propio, encargada de promover la transparencia y garantizar el derecho de acceso.',
      f: 'Ley 20.285 (Transparencia), art. 31'
    },
    {
      q: 'El principio de divisibilidad en el acceso a la información implica que:',
      o: ['Toda la información es secreta si una parte lo es', 'Se entrega la parte pública y se reserva la parte secreta', 'Se divide el costo entre solicitantes', 'Se responde en partes por correo'],
      c: 1,
      e: 'Por el principio de divisibilidad, si un acto contiene información pública y otra reservada, se da acceso a la primera y no a la segunda.',
      f: 'Ley 20.285 (Transparencia), art. 11'
    },
    {
      q: 'El acceso a la información pública es gratuito, salvo:',
      o: ['Una tasa fija por solicitud', 'Los costos directos de reproducción', 'Un arancel del Consejo', 'El pago de honorarios al funcionario'],
      c: 1,
      e: 'El acceso es gratuito; solo puede exigirse el pago de los costos directos de reproducción y los demás valores que una ley expresamente autorice cobrar.',
      f: 'Ley 20.285 (Transparencia), art. 18'
    },
    {
      q: 'Contra la resolución del Consejo para la Transparencia que deniega el acceso procede un reclamo de ilegalidad ante:',
      o: ['La Corte Suprema', 'La Corte de Apelaciones del domicilio del reclamante', 'El Tribunal Constitucional', 'La Contraloría'],
      c: 1,
      e: 'Procede el reclamo de ilegalidad ante la Corte de Apelaciones del domicilio del reclamante, dentro de 15 días corridos desde la notificación.',
      f: 'Ley 20.285 (Transparencia), art. 28'
    },
    {
      q: 'Las causales por las que un órgano puede denegar el acceso a la información:',
      o: ['Las define cada servicio', 'Son taxativas: solo las que establece la ley', 'Dependen del Consejo', 'No existen'],
      c: 1,
      e: 'La reserva o secreto solo procede por las causales taxativas de la ley (afectación de funciones, derechos de las personas, seguridad de la Nación, interés nacional, o ley de quórum calificado).',
      f: 'Ley 20.285 (Transparencia), art. 21'
    },

    /* ── Ley 20.730 · Lobby ──────────────────────────────────────────────── */
    {
      q: 'Según la Ley del Lobby, el "lobby" es la gestión o actividad destinada a influir en decisiones públicas que es:',
      o: ['Siempre gratuita', 'Remunerada', 'Solo de empresas extranjeras', 'Reservada'],
      c: 1,
      e: 'El lobby es la gestión remunerada para promover, defender o representar intereses particulares ante los sujetos pasivos. Si no media remuneración, se llama gestión de interés particular.',
      f: 'Ley 20.730, art. 2'
    },
    {
      q: 'Quien realiza, sin remuneración, gestiones para influir en una decisión pública se denomina:',
      o: ['Lobbista', 'Gestor de intereses particulares', 'Asesor externo', 'Mandatario'],
      c: 1,
      e: 'Si no media remuneración, la persona se denomina gestor de intereses particulares; el lobbista, en cambio, actúa de forma remunerada.',
      f: 'Ley 20.730, art. 2'
    },
    {
      q: '¿Cuál de los siguientes es un sujeto pasivo del lobby?',
      o: ['Un funcionario administrativo cualquiera', 'Un ministro, subsecretario o jefe de servicio', 'Un proveedor del Estado', 'Un ciudadano que solicita audiencia'],
      c: 1,
      e: 'Son sujetos pasivos, entre otros, los ministros, subsecretarios, jefes de servicio, seremis, delegados presidenciales, alcaldes y concejales, que deben registrar sus audiencias.',
      f: 'Ley 20.730, arts. 3 y 4'
    },
    {
      q: 'Los registros de agenda pública que deben llevar los sujetos pasivos son los de:',
      o: ['Audiencias, viajes y donativos', 'Sueldos, viáticos y horas extra', 'Compras, ventas y arriendos', 'Licencias, permisos y feriados'],
      c: 0,
      e: 'La ley crea los registros de audiencias y reuniones, de viajes y de donativos, que deben mantenerse públicos.',
      f: 'Ley 20.730, arts. 7 y 8'
    },
    {
      q: 'La información de los registros de agenda pública debe publicarse y actualizarse:',
      o: ['Una vez al año', 'Al menos una vez al mes', 'Cada vez que se solicite', 'Solo al término del cargo'],
      c: 1,
      e: 'La información de los registros se publica y actualiza al menos una vez al mes en los sitios de transparencia; el Consejo para la Transparencia la pone a disposición del público.',
      f: 'Ley 20.730, art. 9'
    },
    {
      q: 'Frente a quienes solicitan audiencia sobre una misma materia, la autoridad debe:',
      o: ['Atender solo a empresas', 'Mantener igualdad de trato', 'Priorizar a quien pague', 'Negar todas las audiencias'],
      c: 1,
      e: 'Las autoridades y funcionarios deben mantener igualdad de trato respecto de las personas, organizaciones y entidades que soliciten audiencia sobre una misma materia.',
      f: 'Ley 20.730, art. 11'
    },

    /* ── Contraloría y organización del Estado ───────────────────────────── */
    {
      q: 'La Contraloría General de la República es:',
      o: ['Un ministerio', 'Un organismo autónomo de control de legalidad', 'Un tribunal de justicia', 'Una comisión del Congreso'],
      c: 1,
      e: 'La Contraloría es un organismo autónomo que ejerce el control de legalidad de los actos de la Administración, fiscaliza el ingreso e inversión de fondos públicos y lleva la contabilidad general de la Nación.',
      f: 'Constitución Política, art. 98'
    },
    {
      q: 'La "toma de razón" es el procedimiento por el cual la Contraloría:',
      o: ['Audita las cuentas', 'Controla la legalidad de decretos y resoluciones', 'Nombra funcionarios', 'Fija las remuneraciones'],
      c: 1,
      e: 'Mediante la toma de razón, el Contralor controla la legalidad de los decretos y resoluciones que deben tramitarse por la Contraloría, dándoles curso o representando su ilegalidad.',
      f: 'Constitución Política, art. 99'
    },
    {
      q: 'Los dictámenes de la Contraloría General de la República son:',
      o: ['Meras recomendaciones', 'Obligatorios para la Administración (jurisprudencia administrativa)', 'Solo válidos para quien consultó', 'Apelables ante el Presidente'],
      c: 1,
      e: 'Las decisiones y dictámenes de la Contraloría constituyen la jurisprudencia administrativa y son obligatorios para los servicios sometidos a su fiscalización.',
      f: 'Ley 10.336, art. 6'
    },
    {
      q: 'El Contralor General de la República dura en su cargo:',
      o: ['4 años', '6 años', '8 años, sin reelección para el período siguiente', 'De forma indefinida'],
      c: 2,
      e: 'El Contralor es designado por el Presidente con acuerdo del Senado por 3/5 de sus miembros en ejercicio, y dura 8 años en el cargo sin poder ser designado para el período siguiente (cesa al cumplir 75 años).',
      f: 'Constitución Política, art. 98'
    },
    {
      q: 'Según la Constitución, el Estado de Chile es:',
      o: ['Federal', 'Unitario, con administración descentralizada o desconcentrada', 'Confederado', 'Regionalizado autónomo'],
      c: 1,
      e: 'El Estado de Chile es unitario; su administración será funcional y territorialmente descentralizada, o desconcentrada en su caso, conforme a la ley.',
      f: 'Constitución Política, art. 3'
    },
    {
      q: 'Las municipalidades son:',
      o: ['Servicios dependientes del gobierno regional', 'Corporaciones autónomas de derecho público, con personalidad jurídica y patrimonio propio', 'Ministerios locales', 'Empresas del Estado'],
      c: 1,
      e: 'La municipalidad —alcalde más concejo— es una corporación autónoma de derecho público, con personalidad jurídica y patrimonio propio, cuya finalidad es satisfacer las necesidades de la comunidad local.',
      f: 'Constitución Política, art. 118'
    }
  ];

  // ── Clasificación por tema (derivada de la fuente, sin duplicar datos) ────
  function temaDe(f) {
    if (f.indexOf('Ley 19.880') === 0) return 'Procedimiento administrativo';
    if (f.indexOf('Estatuto') === 0) return 'Estatuto Administrativo';
    if (f.indexOf('Ley 18.575') === 0) return 'Bases de la Administración';
    if (f.indexOf('Ley 20.880') === 0) return 'Probidad';
    if (f.indexOf('Ley 19.886') === 0) return 'Compras públicas';
    if (f.indexOf('Ley 20.285') === 0) return 'Transparencia';
    if (f.indexOf('Ley 20.730') === 0) return 'Lobby';
    return 'Contraloría y Estado';
  }

  // Etiqueta cada pregunta con su tema una sola vez.
  BANCO.forEach(function (p) { p.t = temaDe(p.f); });

  // Lista ordenada de temas disponibles (para los filtros).
  var TEMAS = [];
  BANCO.forEach(function (p) { if (TEMAS.indexOf(p.t) === -1) TEMAS.push(p.t); });

  // ── Configuración y estado ───────────────────────────────────────────────
  var POR_SESION = 5;
  var SEGUNDOS = 30;                       // tiempo por pregunta en contrarreloj
  var STORAGE_KEY = 'preparacion_quiz_vistas_v1';
  var CONFIG = { tema: '', contrarreloj: false }; // tema '' = todas

  function leerVistas() {
    try {
      var v = JSON.parse(localStorage.getItem(STORAGE_KEY));
      if (!Array.isArray(v)) return [];
      return v.filter(function (i) { return typeof i === 'number' && i >= 0 && i < BANCO.length; });
    } catch (e) { return []; }
  }

  function guardarVistas(v) {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(v)); } catch (e) {}
  }

  function barajar(arr) {
    for (var i = arr.length - 1; i > 0; i--) {
      var j = Math.floor(Math.random() * (i + 1));
      var tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp;
    }
    return arr;
  }

  // Elige preguntas del banco (o de un tema), evitando las recientes.
  function elegirPreguntas(tema) {
    var idxs = [];
    for (var i = 0; i < BANCO.length; i++) {
      if (!tema || BANCO[i].t === tema) idxs.push(i);
    }
    var n = Math.min(POR_SESION, idxs.length);
    var vistas = leerVistas();

    var disponibles = idxs.filter(function (i) { return vistas.indexOf(i) === -1; });
    // Si no quedan suficientes sin repetir, se reinicia el ciclo de este subconjunto.
    if (disponibles.length < n) {
      vistas = vistas.filter(function (i) { return idxs.indexOf(i) === -1; });
      disponibles = idxs.slice();
    }

    barajar(disponibles);
    var elegidas = disponibles.slice(0, n);
    guardarVistas(vistas.concat(elegidas));

    return elegidas.map(function (idx) { return BANCO[idx]; });
  }

  function $(id) { return document.getElementById(id); }
  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

  var cont = $('quiz-cont');
  if (!cont) return;

  var PREGUNTAS = [];
  var TOTAL = 0;
  var estado = { idx: 0, aciertos: 0, respondida: false };
  var timer = null;
  var restante = 0;

  // ── Pantalla de configuración (filtro + contrarreloj) ────────────────────
  function pintarConfig() {
    detenerTimer();
    var chips = '<button class="quiz-tema-chip' + (CONFIG.tema === '' ? ' is-sel' : '') +
      '" data-tema="">Todas</button>';
    TEMAS.forEach(function (t) {
      chips += '<button class="quiz-tema-chip' + (CONFIG.tema === t ? ' is-sel' : '') +
        '" data-tema="' + esc(t) + '">' + esc(t) + '</button>';
    });
    cont.innerHTML =
      '<div class="quiz-config">' +
        '<h3 class="quiz-config-tit">Arma tu simulacro</h3>' +
        '<p class="quiz-config-lbl">Elige una materia (o todas):</p>' +
        '<div class="quiz-temas">' + chips + '</div>' +
        '<label class="quiz-toggle">' +
          '<input type="checkbox" id="quiz-cr"' + (CONFIG.contrarreloj ? ' checked' : '') + '>' +
          '<span>Modo contrarreloj · ' + SEGUNDOS + ' s por pregunta</span>' +
        '</label>' +
        '<button class="quiz-next quiz-config-start" id="quiz-start">Comenzar (5 preguntas) →</button>' +
      '</div>';

    Array.prototype.forEach.call(cont.querySelectorAll('.quiz-tema-chip'), function (btn) {
      btn.addEventListener('click', function () {
        CONFIG.tema = btn.dataset.tema;
        Array.prototype.forEach.call(cont.querySelectorAll('.quiz-tema-chip'), function (b) {
          b.classList.toggle('is-sel', b === btn);
        });
      });
    });
    $('quiz-start').addEventListener('click', function () {
      CONFIG.contrarreloj = $('quiz-cr').checked;
      iniciar();
    });
  }

  function iniciar() {
    PREGUNTAS = elegirPreguntas(CONFIG.tema);
    TOTAL = PREGUNTAS.length;
    estado = { idx: 0, aciertos: 0, respondida: false };
    pintarPregunta();
  }

  // ── Cronómetro ───────────────────────────────────────────────────────────
  function detenerTimer() {
    if (timer) { clearInterval(timer); timer = null; }
  }

  function actualizarTimer() {
    var el = $('quiz-timer');
    if (el) el.textContent = '⏱ ' + restante + 's';
  }

  function arrancarTimer() {
    if (!CONFIG.contrarreloj) return;
    restante = SEGUNDOS;
    actualizarTimer();
    timer = setInterval(function () {
      restante--;
      actualizarTimer();
      if (restante <= 0) {
        detenerTimer();
        responder(-1); // tiempo agotado: revela sin acierto
      }
    }, 1000);
  }

  // ── Render de pregunta ───────────────────────────────────────────────────
  function pintarPregunta() {
    var p = PREGUNTAS[estado.idx];
    estado.respondida = false;
    var opciones = p.o.map(function (op, i) {
      return '<button class="quiz-op" data-i="' + i + '">' +
        '<span class="quiz-op-letra">' + String.fromCharCode(65 + i) + '</span>' +
        '<span>' + esc(op) + '</span></button>';
    }).join('');
    var timerHtml = CONFIG.contrarreloj ? '<span class="quiz-timer" id="quiz-timer">⏱ ' + SEGUNDOS + 's</span>' : '';
    var temaHtml = '<span class="quiz-tema-badge">' + esc(p.t) + '</span>';
    cont.innerHTML =
      '<div class="quiz-progreso">' +
        '<span>Pregunta ' + (estado.idx + 1) + ' de ' + TOTAL + ' · Aciertos: ' + estado.aciertos + '</span>' +
        timerHtml +
      '</div>' +
      temaHtml +
      '<div class="quiz-pregunta">' + esc(p.q) + '</div>' +
      '<div class="quiz-opciones">' + opciones + '</div>' +
      '<div class="quiz-feedback" id="quiz-feedback"></div>' +
      '<div class="quiz-nav">' +
        '<button class="quiz-link" id="quiz-cambiar">← Cambiar materia</button>' +
        '<button class="quiz-next" id="quiz-next" disabled>Siguiente →</button>' +
      '</div>';

    Array.prototype.forEach.call(cont.querySelectorAll('.quiz-op'), function (btn) {
      btn.addEventListener('click', function () { responder(parseInt(btn.dataset.i, 10)); });
    });
    $('quiz-next').addEventListener('click', siguiente);
    $('quiz-cambiar').addEventListener('click', function () { detenerTimer(); pintarConfig(); });
    arrancarTimer();
  }

  function responder(i) {
    if (estado.respondida) return;
    estado.respondida = true;
    detenerTimer();
    var p = PREGUNTAS[estado.idx];
    var correcta = i === p.c;
    if (correcta) estado.aciertos++;
    Array.prototype.forEach.call(cont.querySelectorAll('.quiz-op'), function (btn) {
      var bi = parseInt(btn.dataset.i, 10);
      btn.disabled = true;
      if (bi === p.c) btn.classList.add('quiz-op--ok');
      else if (bi === i) btn.classList.add('quiz-op--mal');
    });
    var encabezado = correcta ? '✓ Correcto. ' : (i === -1 ? '⏱ Tiempo agotado. ' : '✗ Incorrecto. ');
    var fuente = p.f ? '<span class="quiz-fb-fuente">Fuente: ' + esc(p.f) + '</span>' : '';
    $('quiz-feedback').innerHTML =
      '<div class="quiz-fb ' + (correcta ? 'quiz-fb--ok' : 'quiz-fb--mal') + '">' +
      encabezado + esc(p.e) + fuente + '</div>';
    $('quiz-next').disabled = false;
  }

  function siguiente() {
    estado.idx++;
    if (estado.idx >= TOTAL) { pintarResultado(); return; }
    pintarPregunta();
  }

  function pintarResultado() {
    detenerTimer();
    var pct = Math.round((estado.aciertos / TOTAL) * 100);
    var msg = pct >= 80 ? '¡Excelente base!' : pct >= 50 ? 'Vas bien, sigue repasando.' : 'A reforzar la normativa.';
    var ambito = CONFIG.tema ? esc(CONFIG.tema) : 'todas las materias';
    cont.innerHTML =
      '<div class="quiz-resultado">' +
        '<div class="quiz-resultado-pct">' + pct + '%</div>' +
        '<div class="quiz-resultado-msg">' + estado.aciertos + ' de ' + TOTAL + ' correctas (' + ambito + ') — ' + msg + '</div>' +
        '<div class="quiz-nav" style="justify-content:center;gap:10px">' +
          '<button class="quiz-next" id="quiz-otras">Otras 5 preguntas</button>' +
          '<button class="quiz-link" id="quiz-cambiar2">Cambiar materia</button>' +
        '</div>' +
        '<div class="quiz-premium">' +
          '<strong>El banco rota:</strong> cada vez que vuelves, el simulacro te muestra preguntas distintas ' +
          'de un catálogo de más de 70, sobre procedimiento administrativo, Estatuto Administrativo, probidad, ' +
          'compras públicas, transparencia, lobby y organización del Estado. Puedes filtrar por ley y activar el ' +
          'modo contrarreloj. Activa una alerta y te avisamos de nuevos concursos.' +
          '<a class="quiz-premium-cta" href="index.html#alertas">Activar alerta →</a>' +
        '</div>' +
      '</div>';
    $('quiz-otras').addEventListener('click', iniciar);
    $('quiz-cambiar2').addEventListener('click', pintarConfig);
  }

  pintarConfig();
})();
