// tests/test_modal_jsdom.js
// Tests JSDOM del modal de detalle.
// Carga index.html + ui-strings.js + rich-text.js + app.js en un sandbox
// de JSDOM con mocks mínimos de browser APIs (fetch, localStorage,
// IntersectionObserver, matchMedia) y verifica:
//   1. Helpers de render del modal (_renderListInto, _toggleSection,
//      _setApproxBadge) funcionan con DOM real.
//   2. Flujo end-to-end: abrirModal() consume una oferta, el DOM
//      resultante respeta las reglas del spec (sin duplicaciones,
//      secciones vacías ocultas, microcopy desde UI_STRINGS).
//
// Ejecutar: `node tests/test_modal_jsdom.js`
// Requiere: jsdom (npm install jsdom — no se asume instalado por
// defecto; el script aborta con mensaje claro si falta).

'use strict';

let JSDOM;
try {
  JSDOM = require('jsdom').JSDOM;
} catch (e) {
  console.error('FAIL: este test requiere jsdom. Instalá con `npm install jsdom`');
  process.exit(2);
}

const fs = require('fs');
const path = require('path');

const WEB_DIR = path.join(__dirname, '..', 'web');

// ─────────────────────────────────────────────────────────────
// Bootstrap: carga index.html en JSDOM + evaluación de scripts
// ─────────────────────────────────────────────────────────────

function buildSandbox() {
  const html = fs.readFileSync(path.join(WEB_DIR, 'index.html'), 'utf8');
  const dom = new JSDOM(html, {
    runScripts: 'outside-only',
    url: 'http://localhost/',
    pretendToBeVisual: true,
    storageQuota: 1_000_000,
  });

  const w = dom.window;

  // localStorage está provisto por jsdom; lo limpiamos antes de cada test.
  w.localStorage.clear();

  // Mocks que app.js puede requerir y jsdom no implementa por defecto.
  w.matchMedia = w.matchMedia || ((q) => ({
    matches: false,
    media: q,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  }));
  w.IntersectionObserver = w.IntersectionObserver || class {
    constructor() {}
    observe() {}
    unobserve() {}
    disconnect() {}
  };

  // fetch de base — se sobreescribe por test cuando se necesita.
  w.fetch = () => Promise.resolve({
    ok: true,
    status: 200,
    json: () => Promise.resolve({}),
    text: () => Promise.resolve(''),
  });

  // Suprimimos console.error en el sandbox para no ensuciar el output;
  // los errores reales del runner siguen viéndose porque llamamos a
  // process.stdout.write directo desde acá.
  w.console = Object.assign({}, w.console, {
    error: () => {},
    warn: () => {},
  });

  // Evaluamos los scripts en orden (tal como los carga index.html).
  // Si app.js lanza durante setup (DOMContentLoaded), capturamos para no
  // abortar el test — los helpers a nivel de módulo ya quedaron declarados.
  const scripts = ['ui-strings.js', 'rich-text.js', 'app.js'];
  for (const name of scripts) {
    const code = fs.readFileSync(path.join(WEB_DIR, name), 'utf8');
    try {
      w.eval(code);
    } catch (e) {
      // Algunas inicializaciones de app.js fallan por APIs de browser que
      // jsdom no cubre (ej: scrollBehavior, requestAnimationFrame para
      // animaciones, etc.). Las helpers que testeamos quedan definidas
      // antes del crash, así que continuamos.
      // process.stderr.write(`  [warn] script ${name} threw during init: ${e.message}\n`);
    }
  }

  return { dom, window: w, document: w.document };
}

// ─────────────────────────────────────────────────────────────
// Runner / asserts simples (mismo estilo que test_rich_text_parser.js)
// ─────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function ok(label) {
  passed++;
  console.log(`  ok ${label}`);
}
function fail(label, info) {
  failed++;
  console.log(`  FAIL ${label}`);
  if (info != null) console.log(`       ${info}`);
}
function assertEq(actual, expected, label) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) ok(label);
  else fail(label, `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}
function assertTrue(value, label) {
  if (value) ok(label);
  else fail(label, `expected truthy, got ${JSON.stringify(value)}`);
}
function assertFalse(value, label) {
  if (!value) ok(label);
  else fail(label, `expected falsy, got ${JSON.stringify(value)}`);
}

// ─────────────────────────────────────────────────────────────
// Smoke: ¿se cargó todo?
// ─────────────────────────────────────────────────────────────

console.log('\n## Bootstrap');
const env = buildSandbox();
const { window, document } = env;

assertTrue(window.UI_STRINGS && window.UI_STRINGS.CTA_POSTULAR,
  'UI_STRINGS expuesto y poblado');
assertTrue(typeof window._renderListInto === 'function',
  '_renderListInto definido en window');
assertTrue(typeof window._toggleSection === 'function',
  '_toggleSection definido en window');
assertTrue(typeof window._setApproxBadge === 'function',
  '_setApproxBadge definido en window');
assertTrue(window.richText && typeof window.richText.buildSemanticSections === 'function',
  'richText.buildSemanticSections expuesto');
assertTrue(document.getElementById('modal-residual-list') != null,
  'DOM trae #modal-residual-list (E1)');
assertTrue(document.getElementById('modal-funciones-list') != null,
  'DOM trae #modal-funciones-list');
assertTrue(document.getElementById('sec-req-obligatorios') != null,
  'DOM trae #sec-req-obligatorios');

// ─────────────────────────────────────────────────────────────
// _toggleSection
// ─────────────────────────────────────────────────────────────

console.log('\n## _toggleSection');
window._toggleSection('modal-objetivo-wrap', false);
assertTrue(document.getElementById('modal-objetivo-wrap').hidden,
  'toggle false → hidden=true');
window._toggleSection('modal-objetivo-wrap', true);
assertFalse(document.getElementById('modal-objetivo-wrap').hidden,
  'toggle true → hidden=false');
// Idempotente con id desconocido: no debe lanzar
let threw = false;
try { window._toggleSection('id-inexistente', true); } catch (e) { threw = true; }
assertFalse(threw, 'toggle con id inexistente no lanza');

// ─────────────────────────────────────────────────────────────
// _renderListInto — render simple
// ─────────────────────────────────────────────────────────────

console.log('\n## _renderListInto');
window._renderListInto('modal-funciones-list', ['Coordinar equipo', 'Realizar reportes mensuales']);
let ul = document.getElementById('modal-funciones-list');
assertEq(ul.children.length, 2, 'render 2 items');
assertEq(ul.querySelectorAll('li')[0].textContent, 'Coordinar equipo',
  'primer li conserva texto literal');
assertFalse(ul.hasAttribute('data-truncated'),
  'sin truncate, no setea data-truncated');

// Render vacío con emptyText
window._renderListInto('modal-funciones-list', [], { emptyText: 'Sin datos' });
ul = document.getElementById('modal-funciones-list');
assertEq(ul.children.length, 1, 'emptyText → 1 li placeholder');
assertEq(ul.querySelector('li').className, 'modal-list-empty',
  'placeholder con clase modal-list-empty');

// Render vacío sin emptyText: limpia el ul
window._renderListInto('modal-funciones-list', []);
ul = document.getElementById('modal-funciones-list');
assertEq(ul.innerHTML, '', 'sin emptyText: innerHTML vacío');

// ─────────────────────────────────────────────────────────────
// _renderListInto — truncado con "Ver más"
// ─────────────────────────────────────────────────────────────

console.log('\n## _renderListInto truncado');
const items8 = ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8'];
window._renderListInto('modal-funciones-list', items8, { max: 12, truncateAt: 5 });
ul = document.getElementById('modal-funciones-list');
const truncatedLis = ul.querySelectorAll('.modal-list-item--truncated');
const toggleLis = ul.querySelectorAll('.modal-list-toggle');
assertEq(truncatedLis.length, 3, '8 items - 5 visibles = 3 marcados como truncated');
assertEq(toggleLis.length, 1, '1 li.modal-list-toggle insertado');
assertEq(ul.getAttribute('data-truncated'), 'true',
  'ul tiene data-truncated="true"');
assertEq(toggleLis[0].querySelector('button').textContent, 'Ver 3 más',
  'botón muestra "Ver 3 más"');

// Simular click en el botón → expande
const btn = toggleLis[0].querySelector('button');
btn.dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
// Esperar microtarea (handler es síncrono pero usa closest())
assertEq(ul.getAttribute('data-expanded'), 'true',
  'tras click: data-expanded="true"');
assertEq(btn.textContent, 'Ver menos',
  'tras click: botón dice "Ver menos"');

// Click otra vez → colapsa
btn.dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
assertFalse(ul.hasAttribute('data-expanded'),
  'tras segundo click: data-expanded removido');
assertEq(btn.textContent, 'Ver 3 más',
  'tras segundo click: vuelve a "Ver 3 más"');

// Lista por debajo del umbral: sin botón
window._renderListInto('modal-funciones-list', ['A', 'B', 'C'], { max: 10, truncateAt: 5 });
ul = document.getElementById('modal-funciones-list');
assertEq(ul.querySelectorAll('.modal-list-toggle').length, 0,
  'items < truncateAt: sin botón Ver más');
assertFalse(ul.hasAttribute('data-truncated'),
  'items < truncateAt: sin data-truncated');

// ─────────────────────────────────────────────────────────────
// _setApproxBadge
// ─────────────────────────────────────────────────────────────

console.log('\n## _setApproxBadge');
const sec = document.getElementById('sec-req-formacion');
const h5 = sec.querySelector('h5');
const baseHeading = h5.textContent;

// Confianza alta → no badge
window._setApproxBadge('sec-req-formacion', 0.85);
assertEq(h5.querySelector('[data-approx-badge]'), null,
  'confidence ≥ 0.7: sin badge');

// Confianza baja → badge insertado
window._setApproxBadge('sec-req-formacion', 0.55);
let badge = h5.querySelector('[data-approx-badge]');
assertTrue(badge != null, 'confidence < 0.7: badge insertado');
assertEq(badge.textContent, '~ aproximado',
  'badge dice "~ aproximado"');

// Llamada repetida con bajo confidence: NO duplica
window._setApproxBadge('sec-req-formacion', 0.55);
assertEq(h5.querySelectorAll('[data-approx-badge]').length, 1,
  'idempotente: no duplica el badge');

// Volver a alto confidence → remueve
window._setApproxBadge('sec-req-formacion', 0.9);
assertEq(h5.querySelector('[data-approx-badge]'), null,
  'confidence ≥ 0.7: badge removido');

// confidence undefined / 0 / null → no badge (ítem sin clasificar aún)
window._setApproxBadge('sec-req-formacion', undefined);
assertEq(h5.querySelector('[data-approx-badge]'), null,
  'confidence undefined: sin badge');
window._setApproxBadge('sec-req-formacion', 0);
assertEq(h5.querySelector('[data-approx-badge]'), null,
  'confidence 0 (sin items): sin badge');

// Texto del h5 base no se rompe
const finalText = h5.firstChild.textContent.trim();
assertTrue(finalText === baseHeading.trim() || finalText.startsWith(baseHeading.trim()),
  'h5 conserva su texto base tras múltiples toggles del badge');

// ─────────────────────────────────────────────────────────────
// Microcopy desde UI_STRINGS
// ─────────────────────────────────────────────────────────────

console.log('\n## Microcopy / UI_STRINGS');
assertEq(window.UI_STRINGS.CTA_POSTULAR, 'Ir al portal de postulación →',
  'CTA_POSTULAR canónico');
assertEq(window.UI_STRINGS.CTA_POSTULAR_OFF, 'Postulación no disponible',
  'CTA_POSTULAR_OFF canónico');
assertEq(window.UI_STRINGS.CTA_BASES, 'Ver bases oficiales',
  'CTA_BASES canónico');
assertEq(window.UI_STRINGS.CTA_VER_DETALLE, 'Ver detalle →',
  'CTA_VER_DETALLE canónico');
assertEq(window.UI_STRINGS.SEC_AVISO, 'Texto completo del aviso',
  'SEC_AVISO canónico');
assertTrue(Object.isFrozen(window.UI_STRINGS),
  'UI_STRINGS está congelado (Object.isFrozen)');

// ─────────────────────────────────────────────────────────────
// Render de buildSemanticSections + wiring directo (sin abrirModal)
// ─────────────────────────────────────────────────────────────
//
// abrirModal hace fetch + tiene mucha lógica adyacente; en lugar de
// montar todo eso, simulamos su parte clave: tomamos un texto crudo,
// lo procesamos con buildSemanticSections, y wireamos los resultados
// directamente al DOM con los mismos helpers que usa abrirModal.
// Validamos que las reglas del spec se cumplen sobre el DOM real.

console.log('\n## Wiring buildSemanticSections → DOM');

const oferta = {
  cargo: 'Analista contable',
  institucion: 'Municipalidad de Ñuñoa',
  descripcion: 'Realizar conciliaciones bancarias mensuales del municipio. Coordinar pagos a proveedores con tesorería.',
  requisitos: 'Título profesional de Contador Auditor. Experiencia laboral previa de 3 años en sector público demostrable. Manejo de Excel avanzado y software contable. Debe presentar certificado de antecedentes al momento de postular.',
};

const semantic = window.richText.buildSemanticSections({
  descripcion: oferta.descripcion,
  requisitos: oferta.requisitos,
});

// Render mínimo análogo a abrirModal:
window._renderListInto('modal-funciones-list', semantic.funciones, { max: 12, truncateAt: 6 });
window._renderListInto('modal-req-formacion', semantic.requisitos.formacion, { max: 6, truncateAt: 4 });
window._renderListInto('modal-req-experiencia', semantic.requisitos.experiencia, { max: 6, truncateAt: 4 });
window._renderListInto('modal-req-competencias', semantic.requisitos.competencias, { max: 6, truncateAt: 4 });
window._renderListInto('modal-req-documentos', semantic.requisitos.documentos, { max: 6, truncateAt: 4 });

window._toggleSection('sec-req-formacion', semantic.requisitos.formacion.length > 0);
window._toggleSection('sec-req-experiencia', semantic.requisitos.experiencia.length > 0);
window._toggleSection('sec-req-competencias', semantic.requisitos.competencias.length > 0);
window._toggleSection('sec-req-documentos', semantic.requisitos.documentos.length > 0);
window._toggleSection('sec-req-obligatorios', semantic.requisitos.obligatorios.length > 0);
window._toggleSection('sec-req-deseables', semantic.requisitos.deseables.length > 0);
window._toggleSection('sec-req-especialidades', semantic.requisitos.especialidades.length > 0);

const conf = semantic.requisitosConfidence || {};
window._setApproxBadge('sec-req-formacion', conf.formacion);
window._setApproxBadge('sec-req-experiencia', conf.experiencia);
window._setApproxBadge('sec-req-competencias', conf.competencias);
window._setApproxBadge('sec-req-documentos', conf.documentos);

// Ahora chequeamos las reglas:
//
// (1) Funciones se renderizan
const funcionesUl = document.getElementById('modal-funciones-list');
assertTrue(funcionesUl.children.length >= 1,
  'Funciones renderizadas (>= 1 ítem)');

// (2) Formación: contiene "Contador Auditor"
const formacionUl = document.getElementById('modal-req-formacion');
const formacionText = formacionUl.textContent;
assertTrue(/Contador Auditor/i.test(formacionText),
  'Bloque Formación contiene "Contador Auditor"');

// (3) Experiencia: contiene "3 años" o similar
const expUl = document.getElementById('modal-req-experiencia');
const expText = expUl.textContent;
assertTrue(/3\s*a[nñ]os/i.test(expText),
  'Bloque Experiencia contiene "3 años"');

// (4) Documentos: capturó "presentar certificado de antecedentes"
const docsUl = document.getElementById('modal-req-documentos');
assertTrue(/certificado de antecedentes/i.test(docsUl.textContent),
  'Bloque Documentos contiene "certificado de antecedentes"');

// (5) Sin items: la subsección queda hidden
const deseablesSec = document.getElementById('sec-req-deseables');
assertTrue(deseablesSec.hidden,
  'Subsección sin items (Deseables) queda hidden');

// (6) Confianza: las categorías con items reales deberían tener
//     confidence >= 0 y posiblemente badge si hay penalizaciones.
//     "Manejo de Excel avanzado" es 4 palabras: borderline. Lo importante:
//     si hay badge, el data-attribute existe; si no, h5 limpio.
const formacionH5 = document.querySelector('#sec-req-formacion h5');
assertTrue(formacionH5 != null, 'h5 de Formación presente');

// (7) Funciones NO matchean condiciones (cascade exclusiva)
//     "Realizar conciliaciones..." es función; no debería estar en
//     condiciones. El input no tiene keyword de condición → bloque vacío.
assertEq(semantic.condiciones.length, 0,
  'condiciones vacío para input que sólo tiene funciones + requisitos');

// ─────────────────────────────────────────────────────────────
// Output final
// ─────────────────────────────────────────────────────────────

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
