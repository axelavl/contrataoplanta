// tests/test_gestion_modal_guard.js
// Test JSDOM de la protección contra cierre accidental del modal de oferta
// (gestion.html/js): con cambios sin guardar, el clic en el fondo, Escape y
// el botón Cancelar piden confirmación antes de descartar; sin cambios,
// cierran directo. Tras guardar, el cierre es directo (closeModal).
//
// Ejecutar: `node tests/test_gestion_modal_guard.js`  (requiere jsdom)

'use strict';

let JSDOM;
try { JSDOM = require('jsdom').JSDOM; }
catch (e) { console.error('FAIL: requiere jsdom (`npm install jsdom`)'); process.exit(2); }

const fs = require('fs');
const path = require('path');
const WEB = path.join(__dirname, '..', 'web');

function buildSandbox() {
  const html = fs.readFileSync(path.join(WEB, 'gestion.html'), 'utf8');
  const dom = new JSDOM(html, {
    runScripts: 'dangerously',
    url: 'http://localhost/?k=testpath',
    pretendToBeVisual: true,
  });
  const w = dom.window;
  w.localStorage.clear && w.localStorage.clear();
  w.matchMedia = w.matchMedia || (() => ({ matches: false, addEventListener(){}, removeEventListener(){} }));
  w.alert = () => {};
  // confirm espiable: el test controla la respuesta y cuenta invocaciones.
  w.__confirmRespuesta = true;
  w.__confirmLlamados = 0;
  w.confirm = () => { w.__confirmLlamados++; return w.__confirmRespuesta; };
  w.fetch = () => Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) });
  const code = fs.readFileSync(path.join(WEB, 'gestion.js'), 'utf8');
  const s = w.document.createElement('script');
  s.textContent = code;
  w.document.body.appendChild(s);
  w._saveSession('tok-test', Math.floor(Date.now() / 1000) + 3600, 'admin', 'test');
  return w;
}

let passed = 0, failed = 0;
function ok(l) { passed++; console.log('  ok ' + l); }
function bad(l, d) { failed++; console.log('  FAIL ' + l + (d ? ' — ' + d : '')); }
function assert(c, l, d) { c ? ok(l) : bad(l, d); }

(async () => {
  const w = buildSandbox();
  const doc = w.document;
  const modal = () => doc.getElementById('edit-modal');
  const abierto = () => modal().classList.contains('open');
  const clickFondo = () => modal().dispatchEvent(new w.MouseEvent('click', { bubbles: true }));
  const escape = () => doc.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));

  console.log('## Sin cambios: cierra directo, sin preguntar');
  w.openCrearOferta();
  assert(abierto(), 'el modal abre');
  clickFondo();
  assert(!abierto(), 'clic en el fondo cierra sin cambios');
  assert(w.__confirmLlamados === 0, 'no pidió confirmación (formulario intacto)');

  console.log('\n## Con cambios: el clic en el fondo pide confirmación');
  w.openCrearOferta();
  doc.getElementById('edit-cargo').value = 'Psicólogo(a) Clínico(a) APS';
  w.__confirmRespuesta = false;   // el usuario se arrepiente
  clickFondo();
  assert(abierto(), 'con confirm=No el modal SIGUE abierto (no se pierde el avance)');
  assert(w.__confirmLlamados === 1, 'pidió confirmación exactamente una vez');
  assert(doc.getElementById('edit-cargo').value.includes('Psicólogo'), 'lo escrito se conserva');

  console.log('\n## Con cambios: Escape también pide confirmación');
  escape();
  assert(abierto(), 'Escape con confirm=No no cierra');

  console.log('\n## Botón Cancelar (data-action=close-modal) también protege');
  doc.querySelector('[data-action="close-modal"]').click();
  assert(abierto(), 'Cancelar con confirm=No no cierra');

  console.log('\n## Aceptar el descarte sí cierra');
  w.__confirmRespuesta = true;
  clickFondo();
  assert(!abierto(), 'con confirm=Sí el modal cierra');

  console.log('\n## Cierre duro post-guardado no pregunta');
  w.openCrearOferta();
  doc.getElementById('edit-cargo').value = 'Otro cargo';
  const antes = w.__confirmLlamados;
  w.closeModal();   // camino que usa saveEdit() tras guardar OK
  assert(!abierto(), 'closeModal() cierra directo');
  assert(w.__confirmLlamados === antes, 'sin confirmación en el cierre post-guardado');

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('FAIL (excepción):', e); process.exit(1); });
