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
assertInBlock(s1, 'condiciones', 'Jornada completa 44 horas', 'Keyword jornada sin verbo de acción → condiciones');
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

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
