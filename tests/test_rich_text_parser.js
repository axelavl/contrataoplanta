// tests/test_rich_text_parser.js
// Smoke tests del parser semántico (buildSemanticSections + cascade).
// Se ejecuta con `node tests/test_rich_text_parser.js` — no requiere
// pytest ni módulos externos. Cargamos rich-text.js en un vm sandbox
// con `window` simulado y verificamos los casos del spec.

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'web', 'rich-text.js'), 'utf8');
const sandbox = { window: {}, document: { readyState: 'complete', addEventListener: () => {} } };
vm.createContext(sandbox);
vm.runInContext(source, sandbox);

const rt = sandbox.window.richText;
if (!rt || !rt.buildSemanticSections || !rt.classifyRequirementItem || !rt.stripRedundantPrefix) {
  console.error('FAIL: rich-text public API incompleta');
  process.exit(1);
}

let passed = 0;
let failed = 0;

function assert(actual, expected, label) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) {
    passed++;
    console.log(`  ok ${label}`);
  } else {
    failed++;
    console.log(`  FAIL ${label}`);
    console.log(`       expected: ${JSON.stringify(expected)}`);
    console.log(`       actual:   ${JSON.stringify(actual)}`);
  }
}

function assertInBlock(sections, block, needle, label) {
  const list = block === 'funciones' || block === 'condiciones' || block === 'postulacion' || block === 'residual'
    ? sections[block]
    : sections.requisitos[block];
  const found = list.some(item => item.toLowerCase().includes(needle.toLowerCase()));
  if (found) {
    passed++;
    console.log(`  ok ${label}`);
  } else {
    failed++;
    console.log(`  FAIL ${label}`);
    console.log(`       buscado "${needle}" en [${block}]`);
    console.log(`       contenido: ${JSON.stringify(list)}`);
  }
}

function assertNotInBlock(sections, block, needle, label) {
  const list = block === 'funciones' || block === 'condiciones' || block === 'postulacion' || block === 'residual'
    ? sections[block]
    : sections.requisitos[block];
  const found = list.some(item => item.toLowerCase().includes(needle.toLowerCase()));
  if (!found) {
    passed++;
    console.log(`  ok ${label}`);
  } else {
    failed++;
    console.log(`  FAIL ${label}`);
    console.log(`       NO debería estar "${needle}" en [${block}] pero apareció`);
    console.log(`       contenido: ${JSON.stringify(list)}`);
  }
}

// ============================================================
// stripRedundantPrefix
// ============================================================
console.log('\n## stripRedundantPrefix');
assert(
  rt.stripRedundantPrefix('Formación educacional: Licencia de Enseñanza Media', 'formacion'),
  'Licencia de Enseñanza Media',
  'Elimina "Formación educacional:" del bloque formación'
);
assert(
  rt.stripRedundantPrefix('Experiencia laboral: 3 años en sector público', 'experiencia'),
  '3 años en sector público',
  'Elimina "Experiencia laboral:" del bloque experiencia'
);
assert(
  rt.stripRedundantPrefix('Competencias requeridas: liderazgo y trabajo en equipo', 'competencias'),
  'Liderazgo y trabajo en equipo',
  'Elimina "Competencias requeridas:" y capitaliza'
);
assert(
  rt.stripRedundantPrefix('Documentos requeridos: certificado de antecedentes', 'documentos'),
  'Certificado de antecedentes',
  'Elimina "Documentos requeridos:" del bloque documentos'
);
assert(
  rt.stripRedundantPrefix('Licencia de conducir clase A2', 'especialidades'),
  'Licencia de conducir clase A2',
  'No toca un ítem sin prefijo redundante'
);
assert(
  rt.stripRedundantPrefix('Formación educacional: Estudios: Ingeniero Civil', 'formacion'),
  'Ingeniero Civil',
  'Aplica strip en dos niveles'
);

// ============================================================
// classifyRequirementItem — casos del spec
// ============================================================
console.log('\n## classifyRequirementItem');

function classifyCategory(text) {
  const r = rt.classifyRequirementItem(text);
  return r ? r.category : 'residual';
}

assert(classifyCategory('Debe presentar certificado de antecedentes al momento de postular'),
  'documentos',
  'Oración con "presentar" + "certificado de antecedentes" → documentos');

assert(classifyCategory('Certificado en Gestión de Proyectos'),
  'residual',
  'Bullet "Certificado en X" (sin verbo documental) NO es documento');

assert(classifyCategory('Licencia de conducir clase A2 antigua o A3 nueva'),
  'especialidades',
  '"Licencia de conducir clase X" → especialidades');

assert(classifyCategory('Curso de conducción de vehículos de emergencia'),
  'especialidades',
  '"Curso de X" → especialidades');

assert(classifyCategory('Experiencia de 2 años en sector público'),
  'experiencia',
  '"Experiencia de N años en X" → experiencia');

assert(classifyCategory('Mínimo 3 años en puestos similares'),
  'experiencia',
  '"Mínimo N años" → experiencia');

assert(classifyCategory('Trayectoria comprobable en gestión pública'),
  'experiencia',
  '"Trayectoria comprobable" → experiencia');

assert(classifyCategory('Título profesional de Ingeniero Civil'),
  'formacion',
  '"Título profesional de X" → formación');

assert(classifyCategory('Licencia de Enseñanza Media completa'),
  'formacion',
  '"Licencia de Enseñanza Media" → formación (NO especialidades)');

assert(classifyCategory('Conocimientos de SAP y Office avanzado'),
  'competencias',
  '"Conocimientos de X" (dominio acotado) → competencias');

assert(classifyCategory('Liderazgo y trabajo en equipo'),
  'competencias',
  '"Liderazgo + trabajo en equipo" → competencias');

assert(classifyCategory('Manejo de Office y sistemas contables'),
  'competencias',
  '"Manejo de X" → competencias');

assert(classifyCategory('Ser deseable contar con diplomado en finanzas'),
  'deseables',
  '"Deseable ..." → deseables (gana sobre especialidades por cascada)');

assert(classifyCategory('Requisito mínimo excluyente: ciudadanía chilena'),
  'obligatorios',
  '"Excluyente" → obligatorios');

// Caso problemático histórico: oración muy larga sin keyword → residual
assert(classifyCategory('Otros aspectos relevantes del puesto y entorno'),
  'residual',
  'Oración genérica sin keyword → residual (no se inyecta como obligatorio)');

// ============================================================
// buildSemanticSections — integración
// ============================================================
console.log('\n## buildSemanticSections integración');

// Test 1: separación funciones vs condiciones
const s1 = rt.buildSemanticSections({
  descripcion: 'Realizar gestión de jornada laboral del equipo. Jornada completa 44 horas semanales presencial.',
  requisitos: '',
});
assertInBlock(s1, 'funciones', 'Realizar gestión de jornada', 'Verbo de acción "Realizar" → función');
// Tras E5 (heading-aware split), "Jornada" se promueve a heading
// y el bullet de condiciones queda como "completa 44 horas..." sin
// repetir el rótulo. El cascade ahora usa el hint del heading anterior
// para mandar el bullet a `condiciones` aunque pierda la palabra clave.
assertInBlock(s1, 'condiciones', '44 horas semanales',
  'Bullet bajo heading "Jornada" → condiciones (vía hint)');
assertNotInBlock(s1, 'condiciones', 'Realizar gestión', 'Función NO aparece en condiciones (no doble match)');

// Test 2: strip prefix en bullets clasificados
const s2 = rt.buildSemanticSections({
  descripcion: '',
  requisitos: 'Formación educacional: Licencia de Enseñanza Media. Experiencia laboral: 2 años en conducción de ambulancias en recintos de salud. Competencias requeridas: Manejo de equipos de telefonía móvil.',
});
assertInBlock(s2, 'formacion', 'Licencia de Enseñanza Media',
  'Formación bullet sin prefijo redundante "Formación educacional:"');
assertNotInBlock(s2, 'formacion', 'Formación educacional:',
  'Prefijo "Formación educacional:" eliminado del bullet');
assertInBlock(s2, 'experiencia', '2 años',
  'Experiencia bullet con años cuantificados');

// Test 3: "Certificado de X" sin verbo documental NO va a documentos
const s3 = rt.buildSemanticSections({
  descripcion: '',
  requisitos: 'Certificado en Gestión de Proyectos. Debe presentar certificado de antecedentes al postular.',
});
assertInBlock(s3, 'documentos', 'certificado de antecedentes',
  '"presentar certificado de antecedentes" → documentos');
assertNotInBlock(s3, 'documentos', 'Gestión de Proyectos',
  '"Certificado en X" NO cae en documentos');

// Test 4: oraciones ambiguas/genéricas caen a residual, no a obligatorios
const s4 = rt.buildSemanticSections({
  descripcion: '',
  requisitos: 'Otros aspectos a considerar del entorno laboral institucional.',
});
assert(s4.requisitos.obligatorios.length, 0,
  'Oración genérica NO se inyecta como obligatorio (antes caía al fallback)');

// ============================================================
// Confidence per-category (E3)
// ============================================================
console.log('\n## Confidence per-category');

// _itemConfidence está expuesto como helper interno para testing.
function ic(text, ruleConf) {
  return rt._itemConfidence(text, ruleConf);
}

assert(
  ic('Conocimientos de SAP, Office, Excel y SQL avanzado', 0.75) >= 0.7,
  true,
  'Item largo (>30 chars, >=4 palabras) conserva confianza alta'
);
assert(
  Math.round(ic('Conocimientos en C++', 0.75) * 100) / 100,
  0.5,
  'Item corto (<30 chars, <4 palabras) baja confianza a 0.5'
);
assert(
  ic('LICENCIA CONDUCIR CLASE A2 PROFESIONAL VEHICULOS PESADOS EMERGENCIA', 0.8) < 0.75,
  true,
  'Item ALL CAPS baja confianza por penalización de mayúsculas'
);

// Integración: una sección con un item de baja calidad → confidence agregada baja
const sLowConf = rt.buildSemanticSections({
  descripcion: '',
  requisitos: 'Conocimientos en C++.',
});
const conf = sLowConf.requisitosConfidence?.competencias;
assert(
  typeof conf === 'number' && conf < 0.7,
  true,
  `requisitosConfidence.competencias < 0.7 con sólo 1 item corto (medido: ${conf?.toFixed(2)})`
);

// Sección con items de alta calidad → confidence agregada alta
const sHighConf = rt.buildSemanticSections({
  descripcion: '',
  requisitos: 'Experiencia laboral previa demostrable de 3 años en el sector público o privado equivalente. Mínimo 5 años en cargos de jefatura comprobable con referencias formales.',
});
const confExp = sHighConf.requisitosConfidence?.experiencia;
assert(
  typeof confExp === 'number' && confExp >= 0.7,
  true,
  `requisitosConfidence.experiencia >= 0.7 con items largos y bien formados (medido: ${confExp?.toFixed(2)})`
);

// Categoría sin items → confidence = 0 (no hay nada que medir)
assert(
  sLowConf.requisitosConfidence.documentos,
  0,
  'Categoría sin items: requisitosConfidence === 0 (no inicializado)'
);

// ============================================================
// splitSemanticSentences — abreviaturas (E4)
// ============================================================
console.log('\n## splitSemanticSentences — abreviaturas');

// El split no se exporta directamente, validamos comportamiento via
// buildSemanticSections: ninguna oración debe quedar huérfana como
// "Dr.", "Avda.", "art." cuando son parte de una oración mayor.
function buildAll(text) {
  const out = rt.buildSemanticSections({ descripcion: text, requisitos: '' });
  return out.funciones.concat(
    out.residual || [],
    out.condiciones || [],
    out.postulacion || [],
    ...Object.values(out.requisitos || {})
  );
}

// "Dr. Pedro Soto" no debe quedar como oración huérfana
const sentencesDr = buildAll(
  'El Dr. Pedro Soto fue contratado para liderar el área de salud preventiva durante 2024.'
);
assert(
  sentencesDr.every(s => !/^Dr\.?$/i.test(s.trim())),
  true,
  '"Dr." no genera oración huérfana'
);
assert(
  sentencesDr.some(s => s.includes('Dr.') && s.includes('Pedro Soto')),
  true,
  '"Dr. Pedro Soto" se mantiene en una sola oración'
);

// "Avda. Libertador" no rompe
const sentencesAv = buildAll(
  'La oficina queda en Avda. Libertador 1234, Las Condes, fácil acceso al transporte público.'
);
assert(
  sentencesAv.some(s => s.includes('Avda. Libertador')),
  true,
  '"Avda. Libertador" se mantiene unido'
);

// "art. 5°" administrativo
const sentencesArt = buildAll(
  'Según el art. 5° del decreto del Ministerio de Hacienda, el sueldo será grado 12 de la escala única.'
);
assert(
  sentencesArt.some(s => /art\.\s*5/.test(s)),
  true,
  '"art. 5°" se mantiene unido'
);

// Iniciales: "J. M. Pérez"
const sentencesIni = buildAll(
  'El cargo lo ejerce J. M. Pérez, jefe de departamento, con experiencia en gestión pública desde 2010.'
);
assert(
  sentencesIni.some(s => s.includes('J. M. Pérez') || s.includes('J. M.')),
  true,
  'Iniciales "J. M. Pérez" se mantienen en una sola oración'
);

// Sigla con punto interno: "S.A."
const sentencesSA = buildAll(
  'La empresa BancoEstado S.A. ofrece beneficios complementarios a la renta principal del cargo.'
);
assert(
  sentencesSA.every(s => !/^S\.?A\.?$/i.test(s.trim())),
  true,
  '"S.A." no genera oración huérfana'
);

// Múltiples oraciones reales SÍ deben separarse
const sentencesMulti = buildAll(
  'La oficina queda en Avda. Libertador 1234. Postular antes del 30 de marzo del 2026 al portal oficial.'
);
assert(
  sentencesMulti.length >= 2,
  true,
  `Múltiples oraciones reales se separan correctamente (got ${sentencesMulti.length})`
);

// ============================================================
// E5 — splitSemanticSegments + heading-aware classification
// ============================================================
console.log('\n## E5 — splitSemanticSegments y heading-aware');

assert(
  typeof rt.splitSemanticSegments === 'function',
  true,
  'splitSemanticSegments expuesto en window.richText'
);
assert(
  typeof rt._categoryFromHeading === 'function',
  true,
  '_categoryFromHeading expuesto en window.richText'
);

// _categoryFromHeading mapea headings comunes a categorías
assert(rt._categoryFromHeading('Funciones del cargo'), 'funciones',
  '"Funciones del cargo" → funciones');
assert(rt._categoryFromHeading('Condiciones del contrato'), 'condiciones',
  '"Condiciones del contrato" → condiciones');
assert(rt._categoryFromHeading('Formación educacional'), 'formacion',
  '"Formación educacional" → formacion');
assert(rt._categoryFromHeading('Experiencia laboral'), 'experiencia',
  '"Experiencia laboral" → experiencia');
assert(rt._categoryFromHeading('Documentos requeridos'), 'documentos',
  '"Documentos requeridos" → documentos');
assert(rt._categoryFromHeading('Competencias requeridas'), 'competencias',
  '"Competencias requeridas" → competencias');
assert(rt._categoryFromHeading('Especialización y capacitación'), 'especialidades',
  '"Especialización y capacitación" → especialidades');
assert(rt._categoryFromHeading('Algo random'), null,
  'Heading desconocido → null (no fuerza categoría)');
assert(rt._categoryFromHeading(null), null,
  'Heading null → null');

// splitSemanticSegments preserva el contexto de heading
const segs1 = rt.splitSemanticSegments(
  'Funciones del cargo:\nCoordinar el equipo de trabajo y supervisar tareas.\nRevisar reportes mensuales del área.'
);
const segsConHeading = segs1.filter(s => s.prevHeading);
assert(
  segsConHeading.length >= 1,
  true,
  `Segments bajo "Funciones del cargo" tienen prevHeading (got ${segsConHeading.length})`
);
assert(
  segsConHeading.every(s => /funciones/i.test(s.prevHeading || '')),
  true,
  'prevHeading apunta a "Funciones..." para los segments bajo ese heading'
);

// Heading-aware: bullet sin verbo de acción cae en funciones por hint
const sHint1 = rt.buildSemanticSections({
  descripcion: 'Funciones principales:\n- Atención al público.\n- Gestión de archivo.\n- Apoyo administrativo general.',
  requisitos: '',
});
assert(
  sHint1.funciones.length >= 2,
  true,
  `Bullets bajo "Funciones principales:" caen en funciones aunque no empiecen con verbo (got ${sHint1.funciones.length})`
);

// Heading-aware: bullet sin keyword obvia cae en formación por hint
const sHint2 = rt.buildSemanticSections({
  descripcion: '',
  requisitos: 'Formación educacional:\nEnseñanza media completa rendida en establecimiento reconocido.',
});
assert(
  sHint2.requisitos.formacion.length >= 1,
  true,
  '"Enseñanza media completa rendida" cae en formacion (heading explícito + regla de formación)'
);

// Sin heading explícito: comportamiento del cascade no cambia
const sHint3 = rt.buildSemanticSections({
  descripcion: '',
  requisitos: 'Excluyente: tener ciudadanía chilena vigente y comprobable.',
});
assert(
  sHint3.requisitos.obligatorios.length >= 1,
  true,
  'Sin heading: cascade clasifica por adjetivo "Excluyente" → obligatorios (sin regresión)'
);

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
