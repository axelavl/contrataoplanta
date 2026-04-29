/**
 * rich-text.js — Parser + renderer de texto libre para detalle de ofertas.
 *
 * Uso:
 *   const html = window.richText.format(rawText, { truncate: true });
 *   // o, compatibilidad con el helper antiguo:
 *   const html = window.formatRichText(rawText);
 *
 * Tubería:
 *   normalize → dedupe → explodeInlineListAfterHeader
 *   → explodeInlineEnumerations → liftInlineHeaders
 *   → splitIntoStructuredBlocks → renderStructuredContent
 *
 * No muta el texto fuente: sólo reestructura visualmente.
 */
(function () {
  'use strict';

  // ─────────────────────────────────────────────────────────────
  // 1. Encabezados conocidos del sector público chileno
  // ─────────────────────────────────────────────────────────────
  const KNOWN_HEADERS = [
    'requisitos principales', 'requisitos del cargo', 'requisitos excluyentes',
    'requisitos deseables', 'requisitos generales', 'requisitos específicos',
    'requisitos especificos', 'requisitos exigibles', 'requisitos legales',
    'requisitos obligatorios', 'requisitos mínimos', 'requisitos minimos',
    'funciones del cargo', 'funciones principales', 'funciones específicas',
    'funciones especificas', 'funciones', 'principales funciones',
    'descripción del cargo', 'descripcion del cargo',
    'descripción de funciones', 'descripcion de funciones',
    'descripción', 'descripcion',
    'competencias requeridas', 'competencias específicas', 'competencias especificas',
    'competencias conductuales', 'competencias técnicas', 'competencias tecnicas',
    'competencias', 'habilidades interpersonales', 'habilidades blandas',
    'habilidades técnicas', 'habilidades tecnicas', 'habilidades',
    'especialización y capacitación', 'especializacion y capacitacion',
    'especialización', 'especializacion',
    'capacitación', 'capacitacion',
    'capacitaciones deseables',
    'formación educacional', 'formacion educacional',
    'formación académica', 'formacion academica',
    'formación', 'formacion',
    'conocimientos claves', 'conocimientos específicos', 'conocimientos especificos',
    'conocimientos', 'experiencia laboral', 'experiencia profesional',
    'experiencia deseable', 'experiencia específica', 'experiencia especifica',
    'experiencia', 'probidad y conducta ética', 'probidad y conducta etica',
    'probidad', 'perfil del cargo', 'perfil profesional', 'perfil',
    'objetivo del cargo', 'objetivo', 'misión del cargo', 'mision del cargo',
    'misión', 'mision',
    'documentos requeridos', 'documentos a presentar',
    'documentación requerida', 'documentacion requerida',
    'antecedentes requeridos', 'antecedentes',
    'beneficios', 'condiciones del contrato', 'condiciones',
    'renta', 'remuneración', 'remuneracion',
    'jornada laboral', 'jornada',
    'lugar de trabajo', 'ubicación', 'ubicacion',
    'vacantes', 'etapas del proceso', 'proceso de selección',
    'proceso de seleccion', 'dependencia', 'supervisa a'
  ];

  const HEADER_TOKENS = new Set(KNOWN_HEADERS.map(function (h) { return h.toLowerCase(); }));
  const HEADING_CANONICAL_MAP = {
    'resumen ejecutivo': 'Resumen ejecutivo',
    'requisitos principales': 'Requisitos principales',
    'requisitos del cargo': 'Requisitos principales',
    'descripcion': 'Descripción',
    'descripción': 'Descripción',
    'detalles del cargo': 'Detalles del cargo',
    'formacion educacional': 'Formación educacional',
    'formación educacional': 'Formación educacional',
    'especializacion y capacitacion': 'Especialización y capacitación',
    'especialización y capacitación': 'Especialización y capacitación',
    'competencias requeridas': 'Competencias requeridas',
    'objetivo del cargo': 'Objetivo del cargo',
    'funciones del cargo': 'Funciones del cargo',
    'perfil del cargo': 'Perfil del cargo',
    'conocimientos tecnicos': 'Conocimientos técnicos',
    'conocimientos técnicos': 'Conocimientos técnicos',
    'habilidades': 'Habilidades',
    'requisitos especificos': 'Requisitos específicos',
    'requisitos específicos': 'Requisitos específicos',
    'requisitos deseables': 'Requisitos deseables',
    'experiencia': 'Experiencia'
  };
  const PRIMARY_SECTION_HEADERS = new Set([
    'resumen ejecutivo', 'requisitos principales', 'descripcion', 'descripción', 'detalles del cargo'
  ]);

  const EXCLUYENTE_NEEDLES = [
    'excluyente', 'excluyentes', 'obligatori', 'legales', 'mínimo', 'minimo'
  ];
  const DESEABLE_NEEDLES = [
    'deseable', 'deseables', 'opcional'
  ];

  // Palabras que sugieren que un ítem separado por coma es en realidad
  // prosa/redacción (verbos conjugados, conectores) → no se divide.
  const PROSE_TOKENS = /\b(es|son|debe|deberá|debera|tendrá|tendra|tiene|requiere|corresponde|corresponderá|correspondera|realizar|realizará|realizara|evaluar|evaluará|evaluara|coordinar|coordinará|coordinara|gestionar|supervisar|elaborar|elaborará|elaborara|apoyar|colaborar|desarrollar|mantener|velar|asegurar|implementar|participar|liderar|además|ademas|sin embargo|por lo tanto|de acuerdo|así como|asi como|entre otros|entre otras)\b/i;

  // Verbos de acción en infinitivo: señal de inicio de función/responsabilidad.
  // Cada función típicamente empieza con uno de estos. Cuando aparecen ≥2 en
  // un mismo párrafo, podemos partirlo en viñetas con alta certeza.
  const ACTION_VERBS = [
    'apoyar', 'realizar', 'registrar', 'elaborar', 'coordinar', 'brindar',
    'ejecutar', 'supervisar', 'gestionar', 'desempeñar', 'desempenar',
    'asistir', 'participar', 'colaborar', 'controlar', 'velar', 'monitorear',
    'monitorizar', 'analizar', 'redactar', 'mantener', 'asegurar',
    'implementar', 'liderar', 'desarrollar', 'revisar', 'programar',
    'atender', 'identificar', 'organizar', 'planificar', 'proponer',
    'generar', 'preparar', 'entregar', 'evaluar', 'tramitar', 'documentar',
    'proveer', 'acompañar', 'acompanar', 'archivar', 'tomar', 'verificar',
    'proyectar', 'estudiar', 'diseñar', 'disenar', 'formular', 'canalizar',
    'comunicar', 'informar', 'reportar', 'facilitar', 'sistematizar',
    'consolidar', 'resolver', 'derivar', 'gestionar', 'fiscalizar',
    'instruir', 'capacitar', 'orientar', 'promover', 'administrar',
    'digitar', 'notificar', 'entregar', 'recibir', 'solicitar', 'emitir',
    'custodiar'
  ];
  const ACTION_VERB_SET = new Set(ACTION_VERBS);
  const ACTION_VERBS_PATTERN = Array.from(ACTION_VERB_SET).sort(function (a, b) {
    return b.length - a.length;
  }).map(function (v) {
    // Mayúscula inicial opcional + acepta tildes en primera letra.
    return v.replace(/^(\w)/, function (c) {
      var map = { a: '[AaÁá]', e: '[EeÉé]', i: '[IiÍí]', o: '[OoÓó]', u: '[UuÚú]' };
      return map[c.toLowerCase()] || ('[' + c.toUpperCase() + c.toLowerCase() + ']');
    });
  }).join('|');
  // Ej: \b(Apoyar|Realizar|...)\b
  const ACTION_VERB_LEAD_RE = new RegExp('\\b(' + ACTION_VERBS_PATTERN + ')\\b', 'g');

  // Conectores que pueden aparecer dentro de un ítem Title-Case sin romperlo.
  const TITLE_CASE_CONNECTORS = new Set([
    'de', 'del', 'en', 'con', 'para', 'y', 'e', 'o', 'a', 'la', 'el',
    'los', 'las', 'al', 'por', 'según', 'segun', 'sobre', 'entre',
    'u', 'vs', 'e/'
  ]);

  // ─────────────────────────────────────────────────────────────
  // 2. Normalización
  // ─────────────────────────────────────────────────────────────
  function normalizeText(raw) {
    if (!raw) return '';
    var t = String(raw);
    t = t.replace(/\r\n?/g, '\n');
    t = t.replace(/[\u200B-\u200D\uFEFF]/g, '');
    t = t.replace(/\u00A0/g, ' ');
    // Colapsa espacios y tabs sin tocar saltos de línea
    t = t.replace(/[ \t]+/g, ' ');
    // Trim por línea
    t = t.split('\n').map(function (l) { return l.trim(); }).join('\n');
    // Más de 2 saltos seguidos → 2
    t = t.replace(/\n{3,}/g, '\n\n');
    // Puntuación repetida artificialmente (...,,,  ..  ;; )
    t = t.replace(/([,;:!?]){2,}/g, '$1');
    t = t.replace(/\.{4,}/g, '...');
    // Espacio antes de puntuación (error ortográfico común en fuentes
    // originales: "palabra ,otra" o "palabra , otra"). El punto final queda
    // incluido porque NO debe llevar espacio antes en español.
    t = t.replace(/[ \t]+([,.;:!?])/g, '$1');
    // Espacio después de puntuación cuando falta (excepto antes de dígitos,
    // para no romper "1,5" o "3.2"). Aplica a coma/punto/punto y coma/dos
    // puntos dentro de la misma línea.
    t = t.replace(/([,;:])(?=[^\s\d\n])/g, '$1 ');
    // Segunda pasada por si el colapso de espacios juntó dos signos distintos
    t = t.replace(/([,;:!?]){2,}/g, '$1');
    // Secuencias largas de guiones/underscores usadas como separadores
    t = t.replace(/[-_]{4,}/g, '');
    return t.trim();
  }

  function dedupeConsecutiveLines(text) {
    var out = [];
    var prev = null;
    var arr = text.split('\n');
    for (var i = 0; i < arr.length; i++) {
      var line = arr[i];
      var key = line.trim().toLowerCase();
      if (key && key === prev) continue;
      out.push(line);
      prev = key;
    }
    return out.join('\n');
  }

  function collapseSplitKnownHeaders(lines) {
    var out = [];
    for (var i = 0; i < lines.length; i++) {
      var current = trim(lines[i]);
      var next = trim(lines[i + 1] || '');
      if (current && next) {
        var merged = (current + ' ' + next).replace(/\s+/g, ' ').trim();
        if (isKnownHeader(merged)) {
          out.push(merged + ':');
          i++;
          continue;
        }
      }
      out.push(lines[i]);
    }
    return out;
  }

  // ─────────────────────────────────────────────────────────────
  // 3. Detección / extracción de ítems
  // ─────────────────────────────────────────────────────────────
  function stripMarker(line) {
    var m;
    // Doble marcador "- 1.- foo" / "• 1) foo": el número manda
    m = line.match(/^[\-\*\+•·●◦▪▫■□–—⇒→➜➝➤]\s+(\d{1,2})\.\-\s+(.+)$/);
    if (m) return { style: 'number', text: m[2] };
    m = line.match(/^[\-\*\+•·●◦▪▫■□–—⇒→➜➝➤]\s+(\d{1,2})[.\)]\s+(.+)$/);
    if (m) return { style: 'number', text: m[2] };
    // "1.- item"
    m = line.match(/^(\d{1,2})\.\-\s+(.+)$/);
    if (m) return { style: 'number', text: m[2] };
    // "(1) item"
    m = line.match(/^\((\d{1,2})\)\s+(.+)$/);
    if (m) return { style: 'number', text: m[2] };
    // "1. item"  /  "1) item"
    m = line.match(/^(\d{1,2})[.\)]\s+(.+)$/);
    if (m) return { style: 'number', text: m[2] };
    // "a) item"
    m = line.match(/^([a-zA-Z])\)\s+(.+)$/);
    if (m) return { style: 'letter', text: m[2] };
    // "i) item" / "IV) item"
    m = line.match(/^[ivxIVX]{1,4}\)\s+(.+)$/);
    if (m) return { style: 'letter', text: m[1] };
    // Viñetas
    m = line.match(/^[\-\*\+•·●◦▪▫■□]\s+(.+)$/);
    if (m) return { style: 'bullet', text: m[1] };
    // Guiones largos
    m = line.match(/^[–—]\s+(.+)$/);
    if (m) return { style: 'bullet', text: m[1] };
    // Flechas
    m = line.match(/^[⇒→➜➝➤]\s+(.+)$/);
    if (m) return { style: 'bullet', text: m[1] };
    return null;
  }

  function explodeInlineEnumerations(text) {
    var t = text;
    // Viñetas unicode → nueva línea "- "
    t = t.replace(/\s*[•·●◦▪▫■□]\s+/g, '\n- ');
    t = t.replace(/\s*[⇒→➜➝➤]\s+/g, '\n- ');
    // Guiones largos como bullet
    t = t.replace(/(^|\n)\s*[–—]\s+/g, '$1- ');
    // Enumeraciones inline: exigir mayúscula después del número para evitar
    // romper referencias legales como "Art. 1. del decreto 22." donde el
    // dígito es parte de la prosa. Las listas reales en español empiezan
    // cada ítem con mayúscula ("1. Atender", "2. Reportar").
    // "texto. 1.- item" → "texto.\n1.- item"
    t = t.replace(/([.;:!?\)\]])\s+(\d{1,2})\.\-\s+(?=[A-ZÁÉÍÓÚÑ])/g, '$1\n$2.- ');
    // "texto. 1. item" / "texto. 1) item"
    t = t.replace(/([.;:!?\)\]])\s+(\d{1,2})[.\)]\s+(?=[A-ZÁÉÍÓÚÑ])/g, '$1\n$2. ');
    // "texto. (1) item"
    t = t.replace(/([.;:!?\)\]])\s+\((\d{1,2})\)\s+(?=[A-ZÁÉÍÓÚÑ])/g, '$1\n$2. ');
    // Secuencia "1. X 2. Y 3. Z" sin puntuación previa fuerte: si hay ≥2 números
    // seguidos de mayúscula, partir cada ocurrencia "<espacio>N. " en nueva línea.
    var seqHits = (t.match(/(^|[\s])(\d{1,2})[.\)]\s+[A-ZÁÉÍÓÚÑ]/g) || []).length;
    if (seqHits >= 2) {
      t = t.replace(/([^\n])\s+(\d{1,2})[.\)]\s+(?=[A-ZÁÉÍÓÚÑ])/g, '$1\n$2. ');
    }
    // "texto. - item"
    t = t.replace(/([.;:!?\)\]])\s+-\s+/g, '$1\n- ');
    // 2+ separadores " - " en prosa → convertir todos.
    // Lookahead para que "A - B - C" cuente 2 (matches solapados), no 1.
    var dashHits = (t.match(/\S\s+-\s+(?=\S)/g) || []).length;
    if (dashHits >= 2) {
      t = t.replace(/(\S)\s+-\s+(?=\S)/g, '$1\n- ');
    }
    return t;
  }

  // Para "Header: item1, item2, item3" genera "Header:\n- item1\n- item2\n- item3"
  // cuando el encabezado es conocido o el contenido se ve claramente listable.
  function explodeInlineListAfterHeader(text) {
    var re = /(^|\n)([A-ZÁÉÍÓÚÑ][^\n:]{2,70}):[ \t]+([^\n]{10,})/g;
    return text.replace(re, function (m, pre, header, rest) {
      var h = header.trim();
      // Si el "header" contiene un punto seguido de letra, es prosa, no encabezado.
      if (/\.\s+\S/.test(h)) return m;
      if (h.length > 60) return m;
      var headerLower = h.toLowerCase();
      var known = HEADER_TOKENS.has(headerLower) || looksLikeListHeader(headerLower);
      if (!known) return m;
      var parts = splitItemsConservatively(rest, true);
      if (!parts || parts.length < 2) return m;
      var lines = parts.map(function (p) { return '- ' + p; }).join('\n');
      return pre + h + ':\n' + lines;
    });
  }

  // Cuando un encabezado conocido aparece a media frase tras punto o punto y coma,
  // lo promovemos a línea propia: "… usuarios. Funciones del cargo:" →
  // "… usuarios.\nFunciones del cargo:".
  var HEADER_INLINE_RE = null;
  function getInlineHeaderRegex() {
    if (HEADER_INLINE_RE) return HEADER_INLINE_RE;
    var tokens = KNOWN_HEADERS.slice()
      .sort(function (a, b) { return b.length - a.length; })
      .map(function (h) { return h.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); });
    HEADER_INLINE_RE = new RegExp('([.;])[ \\t]+((?:' + tokens.join('|') + ')):(?=[ \\t\\n]|$)', 'gi');
    return HEADER_INLINE_RE;
  }

  function splitOnInlineKnownHeaders(text) {
    return text.replace(getInlineHeaderRegex(), function (m, punct, header) {
      return punct + '\n' + header + ':';
    });
  }

  // En textos scrapeados es común ver encabezados conocidos pegados tras coma/punto
  // o incluso sin ":" final. Este paso los separa a una línea propia para dar
  // jerarquía visual consistente.
  function splitOnKnownHeadersAnyContext(text) {
    var tokens = KNOWN_HEADERS.slice()
      .sort(function (a, b) { return b.length - a.length; })
      .map(function (h) { return h.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); });
    var re = new RegExp('([.;])\\s+((?:' + tokens.join('|') + '))(?:\\s*:)?(?=\\s+[A-ZÁÉÍÓÚÑ]|\\n|$)', 'gi');
    return text.replace(re, function (m, punct, header) {
      return punct + '\n' + header + ':';
    });
  }

  function splitStackedKnownHeaders(text) {
    var tokens = KNOWN_HEADERS.slice()
      .sort(function (a, b) { return b.length - a.length; })
      .map(function (h) { return h.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); });
    var re = new RegExp('\\b((?:' + tokens.join('|') + ')):\\s+(.+?)\\s+((?:' + tokens.join('|') + ')):', 'gi');
    return text.split('\n').map(function (line) {
      if (!line || line.indexOf(':') === -1) return line;
      return line.replace(re, function (m, h1, body, h2) {
        return h1 + ':\n' + trim(body) + '\n' + h2 + ':';
      });
    }).join('\n');
  }

  function looksLikeListHeader(s) {
    return /(competencia|conocimiento|habilidad|requisito|funci[oó]n|especializaci[oó]n|capacitaci[oó]n|experiencia|formaci[oó]n|documento|antecedente|beneficio|perfil)/i.test(s);
  }

  function splitItemsConservatively(rest, forceListy) {
    // Intento 1: punto y coma (señal fuerte)
    var semis = rest.split(/\s*;\s*/).map(trim).filter(Boolean);
    if (semis.length >= 2 && semis.every(function (it) { return it.length <= 140; })) {
      return semis.map(cleanItem);
    }
    // Intento 2: " / " con ≥3 ítems cortos
    var slashes = rest.split(/\s+\/\s+/).map(trim).filter(Boolean);
    if (slashes.length >= 3 && slashes.every(function (it) {
      return it.length <= 80 && it.split(/\s+/).length <= 10;
    })) {
      return slashes.map(cleanItem);
    }
    // Intento 3: comas, con criterio estricto
    var commas = smartCommaSplit(rest);
    if (commas.length >= 3) {
      var allShort = commas.every(function (it) {
        return it.length <= 90 && it.split(/\s+/).length <= 12;
      });
      var noSentences = commas.every(function (it) { return !/\.\s+[A-ZÁÉÍÓÚÑ]/.test(it); });
      var noProse = commas.every(function (it) { return !PROSE_TOKENS.test(it); });
      if (forceListy && allShort && noSentences) return commas.map(cleanItem);
      if (allShort && noSentences && noProse) return commas.map(cleanItem);
    }
    return null;
  }

  // Split por coma que respeta paréntesis: no divide "Curso RCP (ALS, BLS)"
  function smartCommaSplit(s) {
    var out = [];
    var depth = 0;
    var buf = '';
    for (var i = 0; i < s.length; i++) {
      var c = s.charAt(i);
      if (c === '(' || c === '[') depth++;
      else if (c === ')' || c === ']') depth = Math.max(0, depth - 1);
      if (c === ',' && depth === 0) {
        out.push(buf.trim());
        buf = '';
      } else {
        buf += c;
      }
    }
    if (buf.trim()) out.push(buf.trim());
    return out.filter(Boolean);
  }

  function cleanItem(s) {
    return s.replace(/\s*\.\s*$/, '').replace(/^y\s+/i, '').trim();
  }

  function trim(s) { return (s || '').trim(); }

  // ─────────────────────────────────────────────────────────────
  // 4. Detección de encabezados a nivel de línea
  // ─────────────────────────────────────────────────────────────
  function isAllCaps(s) {
    var letters = s.replace(/[^A-Za-zÁÉÍÓÚÑáéíóúñ]/g, '');
    if (letters.length < 3) return false;
    var upper = letters.replace(/[^A-ZÁÉÍÓÚÑ]/g, '').length;
    return (upper / letters.length) >= 0.75;
  }

  function isHeadingColon(s) {
    return /^[^:]{2,80}:\s*$/.test(s) && !/[.!?]$/.test(s);
  }

  function isKnownHeader(s) {
    var clean = s.replace(/[:.\s]+$/, '').trim().toLowerCase();
    return HEADER_TOKENS.has(clean);
  }

  var KNOWN_HEADERS_BY_LENGTH = KNOWN_HEADERS.slice().sort(function (a, b) {
    return b.length - a.length;
  });

  function splitLeadingKnownHeader(line) {
    var lower = line.toLowerCase();
    for (var i = 0; i < KNOWN_HEADERS_BY_LENGTH.length; i++) {
      var token = KNOWN_HEADERS_BY_LENGTH[i];
      if (lower.indexOf(token) !== 0) continue;
      var rest = line.slice(token.length);
      if (!rest) return null;
      // Separadores típicos entre subtítulo y contenido.
      if (/^\s*[:\-–—]\s+/.test(rest)) {
        rest = rest.replace(/^\s*[:\-–—]\s+/, '');
      } else if (/^\s{2,}/.test(rest)) {
        rest = rest.replace(/^\s+/, '');
      } else {
        return null;
      }
      if (rest.length < 4) return null;
      return {
        header: line.slice(0, token.length),
        content: rest
      };
    }
    return null;
  }

  function classifyHeaderTone(text) {
    var low = text.toLowerCase();
    for (var i = 0; i < EXCLUYENTE_NEEDLES.length; i++) {
      if (low.indexOf(EXCLUYENTE_NEEDLES[i]) !== -1) return 'excluyente';
    }
    for (var j = 0; j < DESEABLE_NEEDLES.length; j++) {
      if (low.indexOf(DESEABLE_NEEDLES[j]) !== -1) return 'deseable';
    }
    return 'neutral';
  }

  function normalizeHeadingText(text) {
    var cleaned = String(text || '').replace(/[:.\s]+$/, '').trim();
    if (!cleaned) return '';
    var key = headingKey(cleaned);
    if (HEADING_CANONICAL_MAP[key]) return HEADING_CANONICAL_MAP[key];
    return cleaned.charAt(0).toUpperCase() + cleaned.slice(1).toLowerCase();
  }

  function headingLevel(text) {
    var key = headingKey(text);
    return PRIMARY_SECTION_HEADERS.has(key) ? 'section' : 'subsection';
  }

  // "Header: contenido corto" (sin ser un ítem) → separa en dos líneas
  // para que el header pueda clasificarse como heading.
  function liftInlineHeaders(text) {
    var lines = text.split('\n');
    var out = [];
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      // No tocar ítems de lista
      if (/^[\-\*\+•·●◦▪▫■□–—⇒→➜➝➤]\s+/.test(line)) { out.push(line); continue; }
      if (/^\d{1,2}[.\)]\s+/.test(line) || /^\d{1,2}\.\-\s+/.test(line)) { out.push(line); continue; }
      var m = line.match(/^([A-ZÁÉÍÓÚÑ][^:]{2,70}):\s+(.{4,})$/);
      if (m && isKnownHeader(m[1] + ':')) {
        out.push(m[1] + ':');
        out.push(m[2]);
        continue;
      }
      var splitKnown = splitLeadingKnownHeader(line);
      if (splitKnown) {
        out.push(splitKnown.header + ':');
        out.push(splitKnown.content);
      } else {
        out.push(line);
      }
    }
    return out.join('\n');
  }

  // Convierte énfasis markdown o líneas con "encabezado + contenido" en
  // bloques más claros:
  //   "**Formación educacional** Ingeniero..." -> "Formación educacional:\nIngeniero..."
  function liftEmphasizedHeaders(text) {
    var lines = text.split('\n');
    var out = [];
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (!line) continue;

      var mdInline = line.match(/^(?:\*\*|__)\s*([^*_:\n]{2,80}?)\s*(?:\*\*|__)\s*:?\s+(.+)$/);
      if (mdInline && (isKnownHeader(mdInline[1]) || looksLikeListHeader(mdInline[1]))) {
        out.push(mdInline[1].trim() + ':');
        out.push(mdInline[2].trim());
        continue;
      }

      var mdOnly = line.match(/^(?:\*\*|__)\s*([^*_:\n]{2,80}?)\s*(?:\*\*|__)\s*:?\s*$/);
      if (mdOnly && (isKnownHeader(mdOnly[1]) || looksLikeListHeader(mdOnly[1]))) {
        out.push(mdOnly[1].trim() + ':');
        continue;
      }

      var plainInline = line.match(/^([A-ZÁÉÍÓÚÑ][^:]{2,70}?)(?:\s*:)?\s{2,}(.+)$/);
      if (plainInline && (isKnownHeader(plainInline[1]) || looksLikeListHeader(plainInline[1]))) {
        out.push(plainInline[1].trim() + ':');
        out.push(plainInline[2].trim());
        continue;
      }

      out.push(line);
    }
    return out.join('\n');
  }

  // ─────────────────────────────────────────────────────────────
  // 5. Splitter: texto normalizado → bloques tipados
  // ─────────────────────────────────────────────────────────────
  function splitIntoStructuredBlocks(text) {
    var blocks = [];
    var lines = text.split('\n').map(trim).filter(Boolean);

    var listBuf = [];
    var listStyle = null;
    var currentTone = 'neutral';

    function flush() {
      if (listBuf.length) {
        blocks.push({
          type: 'list',
          style: listStyle || 'bullet',
          items: listBuf.slice(),
          tone: currentTone
        });
        listBuf = [];
        listStyle = null;
      }
    }

    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      var marker = stripMarker(line);
      if (marker) {
        if (listStyle && listStyle !== marker.style && listBuf.length) flush();
        listStyle = marker.style;
        listBuf.push(marker.text);
        continue;
      }
      if (isKnownHeader(line) || isHeadingColon(line) || (isAllCaps(line) && line.length <= 80)) {
        flush();
        var clean = normalizeHeadingText(line);
        var tone = classifyHeaderTone(clean);
        currentTone = tone;
        blocks.push({ type: 'heading', text: clean, tone: tone, level: headingLevel(clean) });
        continue;
      }
      flush();
      currentTone = 'neutral';
      blocks.push({ type: 'paragraph', text: line });
    }
    flush();
    return blocks;
  }

  function foldText(s) {
    return String(s || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase();
  }

  function headingKey(s) {
    return foldText(s)
      .replace(/[^\w\s]/g, ' ')
      .replace(/\b(el|la|los|las|de|del|y|e|en|para|por|con)\b/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function areSimilarHeadings(a, b) {
    var ka = headingKey(a);
    var kb = headingKey(b);
    if (!ka || !kb) return false;
    if (ka === kb) return true;
    if (ka.length >= 8 && kb.length >= 8 && (ka.indexOf(kb) !== -1 || kb.indexOf(ka) !== -1)) return true;
    var ta = ka.split(' ').filter(Boolean);
    var tb = kb.split(' ').filter(Boolean);
    if (!ta.length || !tb.length) return false;
    var common = 0;
    for (var i = 0; i < ta.length; i++) if (tb.indexOf(ta[i]) !== -1) common++;
    var ratio = common / Math.max(ta.length, tb.length);
    return ratio >= 0.72;
  }

  function dedupeHeadings(blocks, options) {
    options = options || {};
    var out = [];
    var suppress = (options.suppressHeadings || []).map(headingKey).filter(Boolean);
    var seen = [];
    for (var i = 0; i < blocks.length; i++) {
      var b = blocks[i];
      if (b.type !== 'heading') {
        out.push(b);
        continue;
      }
      var skip = suppress.some(function (h) { return areSimilarHeadings(h, b.text); });
      if (!skip) {
        for (var j = 0; j < seen.length; j++) {
          if (areSimilarHeadings(seen[j], b.text)) {
            skip = true;
            break;
          }
        }
      }
      if (skip) continue;
      seen.push(b.text);
      out.push(b);
    }
    return out;
  }

  // Remueve headings que no tienen contenido real después (antes del próximo
  // heading o fin de bloques). Evita que un origen mal parseado produzca
  // subtítulos sin cuerpo como "Funciones principales:" seguido por otro
  // subtítulo o nada. Se preserva el heading si hay al menos un bloque de
  // tipo list o paragraph entre él y el próximo heading.
  function dropEmptyHeadings(blocks) {
    if (!blocks || !blocks.length) return blocks;
    var out = [];
    for (var i = 0; i < blocks.length; i++) {
      var b = blocks[i];
      if (b.type !== 'heading') {
        out.push(b);
        continue;
      }
      var hasContent = false;
      for (var j = i + 1; j < blocks.length; j++) {
        if (blocks[j].type === 'heading') break;
        if (blocks[j].type === 'list' || blocks[j].type === 'paragraph') {
          hasContent = true;
          break;
        }
      }
      if (hasContent) out.push(b);
    }
    return out;
  }

  // ¿El resultado final tiene contenido real (algún list/paragraph)?
  // Si sólo quedaron headings (o nada), el caller debe usar su fallback.
  function hasRenderableContent(blocks) {
    if (!blocks || !blocks.length) return false;
    for (var i = 0; i < blocks.length; i++) {
      if (blocks[i].type === 'list' || blocks[i].type === 'paragraph') return true;
    }
    return false;
  }

  // ─────────────────────────────────────────────────────────────
  // 6. Escape + negrita para "Rótulo:" inline
  // ─────────────────────────────────────────────────────────────
  function escHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function renderInline(s) {
    var esc = escHtml(s);
    // Conserva énfasis inline pero evita negritas heredadas largas e invasivas.
    esc = esc.replace(/(?:\*\*|__)([^*_]{2,80})(?:\*\*|__)/g, function (m, inner) {
      var t = trim(inner);
      if (t.length > 44) return t;
      if (/[.!?]/.test(t)) return t;
      return '<strong>' + t + '</strong>';
    });
    return esc;
  }

  // ─────────────────────────────────────────────────────────────
  // 7. Renderer de bloques
  // ─────────────────────────────────────────────────────────────
  function renderStructuredContent(blocks, options) {
    options = options || {};
    blocks = dedupeHeadings(blocks, options);
    blocks = dropEmptyHeadings(blocks);
    if (!hasRenderableContent(blocks)) return '';
    var out = [];
    for (var i = 0; i < blocks.length; i++) {
      var b = blocks[i];
      if (b.type === 'heading') {
        var levelClass = b.level === 'section' ? ' rt-heading--section' : ' rt-heading--subsection';
        var toneH = b.tone && b.tone !== 'neutral' ? ' rt-heading--' + b.tone : '';
        out.push('<h4 class="rt-heading' + levelClass + toneH + '">' + escHtml(b.text) + '</h4>');
      } else if (b.type === 'list') {
        var tag = b.style === 'number' ? 'ol' : 'ul';
        var toneL = b.tone && b.tone !== 'neutral' ? ' rt-list--' + b.tone : '';
        var items = b.items.map(function (it) { return '<li>' + renderInline(it) + '</li>'; }).join('');
        out.push('<' + tag + ' class="rt-list' + toneL + '">' + items + '</' + tag + '>');
      } else {
        out.push('<p>' + renderInline(b.text) + '</p>');
      }
    }
    var inner = out.join('');
    var truncAt = options.truncateAt || 900;
    if (options.truncate && estimateLength(blocks) > truncAt) {
      return (
        '<div class="rt-truncate" data-rt-collapsed="true">' +
          '<div class="rt-truncate-inner">' + inner + '</div>' +
          '<button type="button" class="rt-toggle" data-rt-toggle="1" aria-expanded="false">Ver más</button>' +
        '</div>'
      );
    }
    return inner;
  }

  function estimateLength(blocks) {
    var n = 0;
    for (var i = 0; i < blocks.length; i++) {
      var b = blocks[i];
      if (b.text) n += b.text.length;
      if (b.items) for (var j = 0; j < b.items.length; j++) n += b.items[j].length;
    }
    return n;
  }

  // ─────────────────────────────────────────────────────────────
  // 7.b Heurísticas de salvataje: texto corrido → lista con ítems
  //
  // Objetivo: cuando un párrafo llega mal formateado desde el origen y el
  // encabezado previo sugiere que debería ser una lista (funciones,
  // especialización, competencias, etc.), intentamos separarlo con reglas
  // conservadoras. Si la certeza es baja, devolvemos null y el párrafo
  // se renderiza tal cual.
  // ─────────────────────────────────────────────────────────────

  // Nombre normalizado del encabezado previo → qué tipo de splitting aplicar.
  function sectionSplitHint(headingText) {
    if (!headingText) return null;
    var key = headingKey(headingText);
    if (/funci[oó]n|responsabilidad|actividad|tarea/.test(key)) return 'verbs';
    if (/especializaci[oó]n|capacitaci[oó]n|curso|competencia|habilidad|conocimiento|formaci[oó]n/.test(key)) return 'titlecase';
    if (/requisito|perfil|experiencia|documento/.test(key)) return 'flexible';
    return null;
  }

  // Separa un párrafo por verbos de acción cuando hay ≥2 y se detectan
  // fronteras claras. Regla principal: "frase. Verbo" se convierte en ítem.
  // Fallback: si ≥3 verbos están presentes y no hay puntuación fuerte entre
  // ellos, partimos en cada ocurrencia.
  function splitByActionVerbs(text) {
    if (!text) return null;
    var matches = text.match(ACTION_VERB_LEAD_RE);
    if (!matches || matches.length < 2) return null;
    // Limitación: si hay prosa clara (primera persona conjugada, etc.) abortar.
    if (/\b(soy|somos|fui|fuimos|seré|serán|será|tendré|tendrás)\b/i.test(text)) return null;

    // 1) Split por "puntuación + Verbo" (señal fuerte).
    var working = text.replace(
      new RegExp('([.;])\\s+(?=(?:' + ACTION_VERBS_PATTERN + ')\\b)', 'g'),
      '$1\n'
    );

    // 2) Si lo anterior no generó suficientes saltos y detectamos ≥3 verbos
    //    en una sola línea sin puntuación intermedia, partimos antes de cada
    //    verbo (evitando el primero).
    var lines = working.split('\n').map(trim).filter(Boolean);
    if (lines.length < 2) {
      // Sin puntuación entre verbos: intento agresivo pero validado.
      var parts = [];
      var lastIdx = 0;
      var re = new RegExp('(^|\\s)(' + ACTION_VERBS_PATTERN + ')\\b', 'g');
      var m;
      var hits = [];
      while ((m = re.exec(text)) !== null) {
        hits.push({ idx: m.index + (m[1] ? m[1].length : 0), verb: m[2] });
      }
      if (hits.length < 3) return null;
      for (var i = 0; i < hits.length; i++) {
        var start = hits[i].idx;
        var end = (i + 1 < hits.length) ? hits[i + 1].idx : text.length;
        parts.push(text.slice(start, end).trim().replace(/[.;,\s]+$/, ''));
        lastIdx = end;
      }
      lines = parts.filter(Boolean);
    }

    // Validaciones conservadoras
    if (lines.length < 2) return null;
    if (lines.some(function (l) { return l.length > 320; })) return null;
    // Cada ítem debe empezar (o casi) con un verbo de acción reconocido.
    var startsWithVerb = function (l) {
      var first = (l.match(/^[A-Za-zÁÉÍÓÚÑáéíóúñ]+/) || [''])[0].toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '');
      return ACTION_VERB_SET.has(first) ||
        ACTION_VERB_SET.has(first.replace(/n$/, 'ñ')); // "acompanar" → "acompañar"
    };
    var verbItems = lines.filter(startsWithVerb).length;
    if (verbItems < Math.max(2, Math.ceil(lines.length * 0.7))) return null;
    return lines.map(cleanItem);
  }

  // Separa un párrafo que es concatenación de ítems en Title Case sin
  // puntuación entre ellos (ej.: "Atención de Público Gestión Documental
  // Cursos técnicos ..."). Cada ítem comienza con mayúscula y puede contener
  // conectores en minúscula.
  function splitTitleCaseRunOn(text) {
    if (!text) return null;
    // Si ya tiene viñetas, saltos de línea o abundante puntuación, no tocar.
    if (/\n/.test(text)) return null;
    if ((text.match(/[.!?;]/g) || []).length >= 2) return null;
    if (text.length < 60) return null;

    // Tokenización preservando paréntesis/corchetes.
    var rawTokens = text.match(/\([^)]*\)|\[[^\]]*\]|[^\s]+/g) || [];
    if (rawTokens.length < 8) return null;

    // Debe empezar con mayúscula.
    if (!/^[A-ZÁÉÍÓÚÑ]/.test(rawTokens[0])) return null;

    var items = [];
    var current = [];
    var isCap = function (tok) { return /^[A-ZÁÉÍÓÚÑ]/.test(tok); };
    var isConnector = function (tok) {
      return TITLE_CASE_CONNECTORS.has(tok.toLowerCase());
    };
    var isParen = function (tok) { return /^[(\[]/.test(tok); };

    for (var i = 0; i < rawTokens.length; i++) {
      var tok = rawTokens[i];
      // Paréntesis siempre se adhieren al ítem actual.
      if (isParen(tok)) {
        current.push(tok);
        continue;
      }
      if (isCap(tok) && current.length >= 2) {
        var last = current[current.length - 1];
        // Si el token previo es un conector, NO cortamos (ej: "Manejo de ERP").
        if (isConnector(last)) {
          current.push(tok);
          continue;
        }
        items.push(current.join(' '));
        current = [tok];
        continue;
      }
      current.push(tok);
    }
    if (current.length) items.push(current.join(' '));

    // Fusiona ítems finales de una sola palabra (ej. "ChileCompra") con el
    // ítem previo. Esto evita cortar marcas/compuestos que quedaron sueltos
    // al final.
    while (items.length >= 2) {
      var tail = items[items.length - 1];
      if (/\s/.test(tail)) break;
      if (tail.length < 4) break;
      items[items.length - 2] = items[items.length - 2] + ' ' + tail;
      items.pop();
    }

    // Corrige el patrón "PalabraMayus OtraMayus resto..." al final: suele
    // significar que la primera mayúscula era cola del ítem anterior (marca,
    // acrónimo) y el resto es un ítem nuevo independiente.
    //   "ChileCompra Probidad administrativa y transparencia"
    //   → cola "ChileCompra" pega en el ítem previo; nuevo ítem
    //   "Probidad administrativa y transparencia".
    if (items.length >= 2) {
      var lastItem = items[items.length - 1];
      var lastMatch = lastItem.match(/^([A-ZÁÉÍÓÚÑ][\wáéíóúñ\-]{2,19})\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ][^]*)$/);
      if (lastMatch) {
        items[items.length - 2] = items[items.length - 2] + ' ' + lastMatch[1];
        items[items.length - 1] = lastMatch[2].trim();
      }
    }

    // Limpia puntuación final de cada ítem antes de validar (la puntuación
    // terminal del párrafo completo NO debe invalidar el splitting).
    items = items.map(function (it) { return it.trim().replace(/[.,;:]+$/, ''); });

    // Validaciones conservadoras
    if (items.length < 4) return null;
    var hasBadItem = items.some(function (it) {
      var words = it.trim().split(/\s+/);
      if (words.length > 10) return true;
      if (it.length > 90) return true;
      if (/[.!?]/.test(it)) return true;
      return false;
    });
    if (hasBadItem) return null;

    // Si >15% de ítems resultantes son de 1 palabra, baja certeza → abortar.
    var singles = items.filter(function (it) { return it.split(/\s+/).length === 1; }).length;
    if (singles / items.length > 0.15) return null;

    return items.map(cleanItem);
  }

  // Fallback tolerante para encabezados de "requisito/perfil": si existe un
  // patrón claro de texto corrido con muchos verbos, tratarlo como funciones;
  // si parece listado Title Case, tratarlo como tal; si no, dejar como está.
  function splitFlexibleParagraph(text) {
    var byVerbs = splitByActionVerbs(text);
    if (byVerbs) return byVerbs;
    var byTitleCase = splitTitleCaseRunOn(text);
    if (byTitleCase) return byTitleCase;
    return null;
  }

  // Recorre los bloques estructurados y convierte párrafos sueltos en listas
  // cuando el encabezado previo sugiere contenido listable.
  function postProcessListyParagraphs(blocks) {
    if (!blocks || !blocks.length) return blocks;
    var out = [];
    var lastHeading = '';
    for (var i = 0; i < blocks.length; i++) {
      var b = blocks[i];
      if (b.type === 'heading') {
        lastHeading = b.text;
        out.push(b);
        continue;
      }
      if (b.type !== 'paragraph') {
        out.push(b);
        continue;
      }
      var hint = sectionSplitHint(lastHeading);
      var items = null;
      if (hint === 'verbs') items = splitByActionVerbs(b.text);
      else if (hint === 'titlecase') items = splitTitleCaseRunOn(b.text);
      else if (hint === 'flexible') items = splitFlexibleParagraph(b.text);
      // Sin pista de contexto: sólo intentamos verbos cuando hay muy alta certeza
      // (≥4 verbos). Protege frente a falsos positivos en párrafos normales.
      else {
        var verbMatches = (b.text.match(ACTION_VERB_LEAD_RE) || []).length;
        if (verbMatches >= 4 && b.text.length > 160) {
          items = splitByActionVerbs(b.text);
        }
      }
      if (items && items.length >= 2) {
        out.push({ type: 'list', style: 'bullet', items: items, tone: 'neutral' });
      } else {
        out.push(b);
      }
    }
    return out;
  }

  // ─────────────────────────────────────────────────────────────
  // 7.c Parsing semántico para ficha (funciones, requisitos, etc.)
  //
  // Estrategia: clasificación en CASCADA (first-match-wins). Cada
  // sentencia se evalúa contra una lista ordenada de reglas; la
  // primera que matchea se queda con el ítem y el resto no lo ve.
  // Cada regla retorna un score (0-1) — si es < 0.4, el ítem va al
  // pool residual y NO se inyecta como obligatorio por defecto.
  //
  // Patrones secundarios (`CONDITIONS_RE`, `POSTULATION_RE`) se
  // consultan antes que requisitos porque son más acotados y evitan
  // falsos positivos (ej: "realizar jornada..." no es requisito).
  // ─────────────────────────────────────────────────────────────
  var CONDITIONS_RE = /\b(renta|remuneraci[oó]n|jornada|honorarios|horario|turno|vacante|lugar\s+de\s+desempe[nñ]o|duraci[oó]n|modalidad\s+(presencial|remot|h[ií]brid))\b/i;
  var POSTULATION_RE = /\b(postulaci[oó]n|postular|portal|enviar|adjuntar|plazo|cronograma|etapa|comisi[oó]n|entrevista)\b/i;
  var OBJECTIVE_HEADER_RE = /\b(objetivo\s+del\s+cargo|misi[oó]n\s+del\s+cargo)\b/i;
  var BROKEN_FUNCTION_RE = /\b(funciones?\s+de\s+la\s+especialidad\s+tales\s+como|tales\s+como|para)\s*$/i;

  // Prefijos redundantes que aparecen como cola de encabezado embebido
  // dentro de un bullet ya clasificado. Al aplicar `stripRedundantPrefix`
  // con el bloque correcto, se retira "Formación educacional:" del bullet
  // cuando ese bullet ya vive bajo el bloque "Formación".
  var BLOCK_PREFIXES = {
    obligatorios: [
      /^requisitos?(\s+obligatorios?|\s+m[ií]nimos?|\s+excluyentes?|\s+del\s+cargo|\s+generales?|\s+legales?)?\s*[:\-–—]\s*/i,
    ],
    deseables: [
      /^requisitos?\s+deseables?\s*[:\-–—]\s*/i,
      /^(se\s+valorar[aá]|idealmente|preferentemente|deseables?)\s*[:\-–—]\s*/i,
    ],
    formacion: [
      /^formaci[oó]n(\s+educacional|\s+acad[eé]mica|\s+profesional)?\s*[:\-–—]\s*/i,
      /^nivel\s+educacional\s*[:\-–—]\s*/i,
      /^estudios?\s*[:\-–—]\s*/i,
      /^t[ií]tulo\s+(profesional|t[eé]cnico|acad[eé]mico|requerido|exigido)\s*[:\-–—]\s*/i,
    ],
    experiencia: [
      /^experiencia(\s+laboral|\s+profesional|\s+previa|\s+requerida|\s+deseable|\s+espec[ií]fica)?\s*[:\-–—]\s*/i,
      /^trayectoria\s*[:\-–—]\s*/i,
    ],
    especialidades: [
      /^(especializaci[oó]n|especialidad)(\s+y\s+capacitaci[oó]n)?\s*[:\-–—]\s*/i,
      /^capacitaci[oó]n\s*[:\-–—]\s*/i,
      /^cursos?\s*[:\-–—]\s*/i,
      /^licencias?(\s+requeridas?|\s+exigidas?)?\s*[:\-–—]\s*/i,
      /^certificaciones?\s*[:\-–—]\s*/i,
      /^acreditaciones?\s*[:\-–—]\s*/i,
    ],
    competencias: [
      /^competencias?(\s+(requeridas?|conductuales|t[eé]cnicas?|espec[ií]ficas?))?\s*[:\-–—]\s*/i,
      /^habilidades?(\s+(blandas?|t[eé]cnicas?|interpersonales?))?\s*[:\-–—]\s*/i,
      /^conocimientos?(\s+(requeridos?|claves?|espec[ií]ficos?|t[eé]cnicos?))?\s*[:\-–—]\s*/i,
    ],
    documentos: [
      /^documentos?(\s+(requeridos?|exigidos?|a\s+presentar|a\s+adjuntar))?\s*[:\-–—]\s*/i,
      /^documentaci[oó]n\s+(requerida|exigida|solicitada)\s*[:\-–—]\s*/i,
      /^antecedentes?(\s+(requeridos?|exigidos?|a\s+presentar))?\s*[:\-–—]\s*/i,
    ],
    condiciones: [
      /^condiciones?(\s+(del\s+cargo|del\s+contrato|operativas?|laborales?|contractuales?))?\s*[:\-–—]\s*/i,
    ],
    funciones: [
      /^funciones?(\s+(principales|espec[ií]ficas?|del\s+cargo|generales))?\s*[:\-–—]\s*/i,
      /^tareas?(\s+principales|\s+del\s+cargo)?\s*[:\-–—]\s*/i,
      /^responsabilidades?\s*[:\-–—]\s*/i,
    ],
  };

  function stripRedundantPrefix(text, blockName) {
    var patterns = BLOCK_PREFIXES[blockName] || [];
    if (!patterns.length) return text;
    var cleaned = String(text || '');
    // Hasta 2 niveles: cubre el caso "Formación educacional: Estudios: Ing. Civil"
    for (var level = 0; level < 2; level++) {
      var changed = false;
      for (var i = 0; i < patterns.length; i++) {
        if (patterns[i].test(cleaned)) {
          cleaned = cleaned.replace(patterns[i], '').trim();
          changed = true;
          break;
        }
      }
      if (!changed) break;
    }
    // Capitalizar primera letra tras el strip para consistencia visual.
    if (cleaned && /^[a-záéíóúñ]/.test(cleaned)) {
      cleaned = cleaned.charAt(0).toUpperCase() + cleaned.slice(1);
    }
    return cleaned;
  }

  // Reglas de clasificación en CASCADA. El orden importa: la primera
  // regla que matchea se queda con el ítem. Los tests están pensados
  // como un embudo de lo más específico (docs que exigen verbo
  // documental) a lo más permisivo (obligatorios con adjetivo). Cada
  // regla incluye un score de confianza.
  //
  // Principios:
  // - Documentos exige verbo de acción documental (presentar/adjuntar/
  //   remitir/acompañar) para evitar capturar "Certificado en X" como
  //   documento cuando es en realidad una certificación académica.
  // - Licencias/cert va DESPUÉS de documentos y pide sustantivo
  //   educacional (licencia|curso|acreditación|...) sin verbo documental.
  // - Experiencia exige cuantificación ("2 años", "mínimo 3 años") O
  //   un sustantivo fuerte (trayectoria, experiencia previa/laboral/en).
  // - Formación pide nivel educacional formal (título, licenciatura,
  //   ingeniero, bachiller) sin verbo documental.
  // - Competencias sólo acepta "conocimientos" si el ítem explícitamente
  //   acota el dominio ("conocimientos de/en X"), no la palabra suelta.
  // - Obligatorios/deseables aplican SOLO con adjetivos calificadores.
  // Orden de la cascada (primer match gana):
  //   1. Documentos — más específico (verbo documental + sustantivo).
  //   2. Deseables  — qualifier que acá decidimos tratar como bucket
  //                    propio antes que dominios (experiencia, etc.)
  //                    porque el usuario espera ver "deseables" como
  //                    sección separada en el render.
  //   3. Experiencia — cuantificación de años o trayectoria.
  //   4. Formación  — niveles educacionales formales.
  //   5. Especialidades/licencias — cursos, licencias, diplomados, etc.
  //   6. Competencias — liderazgo, habilidades técnicas acotadas.
  //   7. Obligatorios — sólo con adjetivo explícito. Es el último para
  //                    no absorber sentencias que caerían mejor en
  //                    una categoría de dominio.
  // Si nada matchea con confianza ≥ 0.4, cae a residual.
  var CLASSIFICATION_RULES = [
    {
      name: 'documentos',
      confidence: 0.85,
      test: function (t) {
        return /\b(presentar|adjuntar|acompa[nñ]ar|remitir|aportar)\b/i.test(t)
            && /\b(certificad[oa]\s+(de\s+antecedentes|de\s+t[ií]tulo|de\s+nacimiento|m[eé]dico)|fotocopia|curriculum|cv\b|c[eé]dula|declaraci[oó]n\s+jurada|antecedentes\s+(personales|penales|acad[eé]micos)|expediente)\b/i.test(t);
      },
    },
    {
      name: 'deseables',
      confidence: 0.75,
      test: function (t) {
        return /\b(deseable|preferentemente|idealmente|valorable|se\s+valorar[aá]|ser[aá]\s+un\s+plus)\b/i.test(t);
      },
    },
    {
      name: 'experiencia',
      confidence: 0.8,
      test: function (t) {
        // Acepta tanto "N años de experiencia" como "Experiencia de N años",
        // "Experiencia previa/laboral/profesional/en X/demostrable/acreditada"
        // y "trayectoria". "Mínimo N años" también cae acá.
        return /\b(\d+\s+a[nñ]os?\s+de\s+experiencia|experiencia\s+de\s+\d+\s+a[nñ]os?|experiencia\s+m[ií]nima\s+de\s+\d+\s+a[nñ]os?|m[ií]nimo\s+\d+\s+a[nñ]os?|experiencia\s+(previa|laboral|profesional|en\s+\w+|demostrable|comprobable|acreditada)|trayectoria)\b/i.test(t);
      },
    },
    {
      name: 'formacion',
      confidence: 0.8,
      test: function (t) {
        if (/\b(presentar|adjuntar|remitir)\b/i.test(t)) return false;
        return /\b(licencia\s+de\s+ense[nñ]anza\s+media|ense[nñ]anza\s+media\s+(completa|rendida)|t[ií]tulo\s+(profesional|t[eé]cnico|universitario|acad[eé]mico|de\s+\w+)|grado\s+acad[eé]mico|ingenier[oa]|abogad[oa]|licenciatura|bachiller\s+en|magister|m[aá]ster|doctorado|egresad[oa]\s+de)\b/i.test(t);
      },
    },
    {
      name: 'especialidades',
      confidence: 0.8,
      test: function (t) {
        if (/\b(presentar|adjuntar|remitir|aportar)\b/i.test(t)) return false;
        return /\b(licencia\s+(de\s+conducir|clase\s+[a-z]|tipo\s+[a-z])|acreditaci[oó]n|curso\s+(de|en|para)|especializaci[oó]n\s+(en|de)|certificaci[oó]n\s+(en|de)|diplomado\s+(en|de)|registro\s+(sis|superintendencia|nacional|de\s+prestador)|colegiatura)\b/i.test(t);
      },
    },
    {
      name: 'competencias',
      confidence: 0.75,
      test: function (t) {
        return /\b(liderazgo|trabajo\s+en\s+equipo|comunicaci[oó]n\s+efectiva|proactividad|capacidad\s+(de|para)\s+\w+|habilidad\s+(de|para)\s+\w+|manejo\s+de\s+(office|excel|word|sistemas?|herramientas?|\w+)|dominio\s+de\s+\w+|conocimientos?\s+(de|en)\s+\w+)\b/i.test(t);
      },
    },
    {
      name: 'obligatorios',
      confidence: 0.7,
      test: function (t) {
        return /\b(obligatori[oa]s?|excluyente|indispensable|requerid[oa]s?|requisito\s+m[ií]nimo|debe\s+contar|debe\s+poseer|debe\s+acreditar)\b/i.test(t);
      },
    },
  ];

  // Clasifica una oración individual. Devuelve `null` si la confianza
  // no supera el umbral; en ese caso el ítem cae al pool residual.
  // Umbral bajo (0.4) porque preferimos mostrar poco que ensuciar un
  // bloque con contenido ambiguo — la sección residual se conserva
  // y baja al bloque "Texto completo del aviso".
  function classifyRequirementItem(text) {
    for (var i = 0; i < CLASSIFICATION_RULES.length; i++) {
      var rule = CLASSIFICATION_RULES[i];
      if (rule.test(text)) {
        return { category: rule.name, confidence: rule.confidence };
      }
    }
    return null;
  }

  function splitSemanticSentences(text) {
    if (!text) return [];
    var t = normalizeText(text);
    t = t.replace(/\n+/g, '. ');
    t = t.replace(/\s*[•·●◦▪▫■□]\s*/g, '. ');
    t = t.replace(/\s*[;]\s*/g, '. ');
    return t.split(/(?<=[.!?])\s+/).map(trim).filter(function (s) { return s.length >= 18; });
  }

  function normalizeCompareKey(value) {
    return foldText(value).replace(/[^\w\s]/g, ' ').replace(/\s+/g, ' ').trim();
  }

  function dedupeSentenceList(items, maxItems) {
    var out = [];
    var seen = new Set();
    for (var i = 0; i < items.length; i++) {
      var raw = trim(items[i]);
      if (!raw) continue;
      var cleaned = raw.replace(/\s+/g, ' ').replace(/[;,\s]+$/, '');
      var key = normalizeCompareKey(cleaned)
        .replace(/\b(de|la|el|los|las|para|con|en|y|o|del)\b/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
      if (!key || key.length < 12 || seen.has(key)) continue;
      seen.add(key);
      out.push(cleaned.charAt(0).toUpperCase() + cleaned.slice(1));
      if (maxItems && out.length >= maxItems) break;
    }
    return out;
  }

  // Señales fuertes de que una oración es un requisito (no una función).
  // Usado como veto al inicio de `looksLikeFunctionSentence`.
  var REQ_SIGNAL_RE = /\b(obligatori|excluyente|indispensable|requerid[oa]s?|debe\s+(contar|poseer|acreditar)|a[nñ]os?\s+de\s+experiencia|experiencia\s+(de|en|previa|laboral|profesional)|trayectoria|t[ií]tulo\s+(profesional|t[eé]cnico)|grado\s+acad[eé]mico|licencia\s+de\s+ense[nñ]anza)\b/i;

  function looksLikeFunctionSentence(sentence) {
    if (!sentence) return false;
    if (REQ_SIGNAL_RE.test(sentence)) return false;
    var head = sentence.split(/\s+/).slice(0, 4).join(' ');
    var verbMatch = head.match(new RegExp('^(' + ACTION_VERBS_PATTERN + ')\\b', 'i'));
    if (!verbMatch) return false;
    if (BROKEN_FUNCTION_RE.test(sentence)) return false;
    var words = sentence.split(/\s+/).length;
    if (words < 5) return false;
    if (!/[a-záéíóúñ]{3,}\s+[a-záéíóúñ]{3,}/i.test(sentence)) return false;
    return true;
  }

  function extractObjective(text) {
    if (!text) return '';
    var lines = String(text).split('\n').map(trim).filter(Boolean);
    for (var i = 0; i < lines.length; i++) {
      if (OBJECTIVE_HEADER_RE.test(lines[i])) {
        var next = trim(lines[i + 1] || '');
        if (next && next.length >= 18) return next.replace(/\.$/, '') + '.';
      }
    }
    return '';
  }

  // Heurísticas de penalización por ítem: ítems cortos, fragmentarios
  // o muy breves bajan la confianza agregada de la sección. La idea es
  // permitir que el caller muestre un badge "~ aproximado" cuando la
  // clasificación, aunque haya matcheado una regla, no es del todo
  // confiable porque los ítems detrás son ruido.
  function _itemConfidence(text, ruleConfidence) {
    var conf = Number(ruleConfidence) || 0.7;
    var trimmed = String(text || '').trim();
    var words = trimmed.split(/\s+/).filter(Boolean);
    if (trimmed.length < 30) conf -= 0.1;
    if (words.length < 4) conf -= 0.15;
    // ALL CAPS suele ser cabecera / ruido sobreviviente, no contenido real.
    if (trimmed.length >= 6 && trimmed === trimmed.toUpperCase()
        && /[A-ZÁÉÍÓÚÑ]/.test(trimmed)) {
      conf -= 0.1;
    }
    if (conf < 0) conf = 0;
    if (conf > 1) conf = 1;
    return conf;
  }

  function buildSemanticSections(payload) {
    payload = payload || {};
    var descripcion = String(payload.descripcion || '');
    var requisitos = String(payload.requisitos || '');
    var reqSentences = splitSemanticSentences(requisitos);
    var descSentences = splitSemanticSentences(descripcion);
    var all = reqSentences.concat(descSentences);

    var out = {
      objetivo: extractObjective(descripcion + '\n' + requisitos),
      funciones: [],
      condiciones: [],
      postulacion: [],
      residual: [],
      requisitos: {
        obligatorios: [],
        deseables: [],
        experiencia: [],
        formacion: [],
        especialidades: [],
        competencias: [],
        documentos: [],
      },
      // Confianza media por categoría de requisitos (0-1). Sólo aplica a
      // las 7 sub-categorías clasificadas vía cascade. Funciones y
      // condiciones no usan reglas de confianza (asumimos confianza 1
      // si pasaron el filtro). El caller puede usar este objeto para
      // decidir si renderizar un badge "~ aproximado" cuando la media
      // de la categoría queda por debajo de 0.7.
      requisitosConfidence: {
        obligatorios: 0,
        deseables: 0,
        experiencia: 0,
        formacion: 0,
        especialidades: 0,
        competencias: 0,
        documentos: 0,
      },
    };
    var requisitosCount = {
      obligatorios: 0, deseables: 0, experiencia: 0, formacion: 0,
      especialidades: 0, competencias: 0, documentos: 0,
    };

    // Cascada por sentencia. Cada sentencia cae en UNA sola categoría.
    // El orden prioriza clasificación de requisitos sobre postulacion
    // para evitar que "presentar certificado al postular" caiga en
    // postulacion en vez de en documentos.
    //
    //   1. Funciones — verbo de acción al inicio + ≥5 palabras + no
    //      matchea señales de requisitos (REQ_SIGNAL_RE).
    //   2. Requisitos.* — cascade de reglas en CLASSIFICATION_RULES.
    //      Si matchea con confianza ≥ 0.4, gana y aplicamos
    //      stripRedundantPrefix al bullet. Acumulamos la confianza por
    //      ítem para promediarla en `requisitosConfidence[cat]`.
    //   3. Postulación — URL/portal/plazo/etapa (sentencias que hablan
    //      de cómo/cuándo postular, no de qué documentos presentar).
    //   4. Condiciones — keyword contextual (renta/jornada/horario/
    //      modalidad) sin verbo de acción líder.
    //   5. Residual — se expone para el bloque "Texto completo del
    //      aviso"; NO se inyecta en obligatorios como fallback.
    for (var i = 0; i < all.length; i++) {
      var s = all[i];
      if (!s) continue;

      if (looksLikeFunctionSentence(s)) {
        out.funciones.push(s);
        continue;
      }

      var classification = classifyRequirementItem(s);
      if (classification) {
        var cleaned = stripRedundantPrefix(s, classification.category);
        // Si tras el prefix strip el ítem queda muy corto, es ruido
        // (probablemente era sólo un header embebido). Va a residual.
        if (cleaned.length < 10) {
          out.residual.push(s);
        } else {
          var cat = classification.category;
          out.requisitos[cat].push(cleaned);
          var itemConf = _itemConfidence(cleaned, classification.confidence);
          // Promedio incremental: nuevo_promedio = (prev*n + valor) / (n+1)
          var prev = out.requisitosConfidence[cat];
          var n = requisitosCount[cat];
          out.requisitosConfidence[cat] = (prev * n + itemConf) / (n + 1);
          requisitosCount[cat] = n + 1;
        }
        continue;
      }

      if (POSTULATION_RE.test(s)) {
        out.postulacion.push(s);
        continue;
      }

      if (CONDITIONS_RE.test(s)) {
        out.condiciones.push(s);
        continue;
      }

      out.residual.push(s);
    }

    out.funciones = dedupeSentenceList(out.funciones, 10)
      .filter(function (t) { return !BROKEN_FUNCTION_RE.test(t); });
    out.condiciones = dedupeSentenceList(out.condiciones, 8)
      .map(function (t) { return stripRedundantPrefix(t, 'condiciones'); });
    out.postulacion = dedupeSentenceList(out.postulacion, 5);
    out.requisitos.obligatorios = dedupeSentenceList(out.requisitos.obligatorios, 6);
    out.requisitos.deseables = dedupeSentenceList(out.requisitos.deseables, 6);
    out.requisitos.experiencia = dedupeSentenceList(out.requisitos.experiencia, 6);
    out.requisitos.formacion = dedupeSentenceList(out.requisitos.formacion, 6);
    out.requisitos.especialidades = dedupeSentenceList(out.requisitos.especialidades, 6);
    out.requisitos.competencias = dedupeSentenceList(out.requisitos.competencias, 6);
    out.requisitos.documentos = dedupeSentenceList(out.requisitos.documentos, 6);
    out.residual = dedupeSentenceList(out.residual, 20);

    return out;
  }

  // ─────────────────────────────────────────────────────────────
  // 8. API pública
  // ─────────────────────────────────────────────────────────────
  function format(rawText, options) {
    if (rawText == null) return '';
    var t = normalizeText(rawText);
    if (!t) return '';
    t = dedupeConsecutiveLines(t);
    t = splitOnInlineKnownHeaders(t);
    t = splitOnKnownHeadersAnyContext(t);
    t = splitStackedKnownHeaders(t);
    t = explodeInlineListAfterHeader(t);
    t = explodeInlineEnumerations(t);
    t = liftEmphasizedHeaders(t);
    t = liftInlineHeaders(t);
    t = collapseSplitKnownHeaders(t.split('\n')).join('\n');
    t = t.split('\n').map(trim).filter(Boolean).join('\n');
    var blocks = splitIntoStructuredBlocks(t);
    blocks = postProcessListyParagraphs(blocks);
    return renderStructuredContent(blocks, options || {});
  }

  // Delegación de eventos para "Ver más / Ver menos"
  function installToggleHandler(root) {
    root = root || document;
    if (root.__rtToggleInstalled) return;
    root.__rtToggleInstalled = true;
    root.addEventListener('click', function (e) {
      var btn = e.target.closest && e.target.closest('[data-rt-toggle]');
      if (!btn) return;
      var wrap = btn.closest('.rt-truncate');
      if (!wrap) return;
      var collapsed = wrap.getAttribute('data-rt-collapsed') === 'true';
      wrap.setAttribute('data-rt-collapsed', collapsed ? 'false' : 'true');
      btn.setAttribute('aria-expanded', collapsed ? 'true' : 'false');
      btn.textContent = collapsed ? 'Ver menos' : 'Ver más';
    });
  }

  window.richText = {
    format: format,
    normalize: normalizeText,
    splitBlocks: splitIntoStructuredBlocks,
    installToggleHandler: installToggleHandler,
    // Helpers de parsing expuestos para pruebas/reúso.
    splitByActionVerbs: splitByActionVerbs,
    splitTitleCaseRunOn: splitTitleCaseRunOn,
    splitFlexibleParagraph: splitFlexibleParagraph,
    buildSemanticSections: buildSemanticSections,
    _itemConfidence: _itemConfidence,
    classifyRequirementItem: classifyRequirementItem,
    stripRedundantPrefix: stripRedundantPrefix
  };

  // Compatibilidad con llamadas existentes: truncado por defecto activado.
  window.formatRichText = function (raw, options) {
    var defaults = { truncate: true, truncateAt: 900 };
    var cfg = Object.assign({}, defaults, options || {});
    return format(raw, cfg);
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { installToggleHandler(document); });
  } else {
    installToggleHandler(document);
  }
})();
