// tests/test_gestion_run_picker.js
// Test JSDOM del selector "Recolectar ofertas ahora" (gestion.html/js):
// elegir tipo de scraper → completar el listado de instituciones de ese tipo →
// correr todas (mode=kind) o una (mode=institucion), o corrida completa (all).
//
// Ejecutar: `node tests/test_gestion_run_picker.js`  (requiere jsdom)

'use strict';

let JSDOM;
try { JSDOM = require('jsdom').JSDOM; }
catch (e) { console.error('FAIL: requiere jsdom (`npm install jsdom`)'); process.exit(2); }

const fs = require('fs');
const path = require('path');
const WEB = path.join(__dirname, '..', 'web');

// Catálogo simulado que devuelve /scraper/catalog.
const CATALOGO = [
  { id: 345, nombre: 'Municipalidad de Viña del Mar', kind: 'wordpress', status: 'active' },
  { id: 382, nombre: 'Municipalidad de Cerro Navia',  kind: 'wordpress', status: 'active' },
  { id: 280, nombre: 'TVN',                           kind: 'custom_trabajando', status: 'active' },
  { id: 999, nombre: 'Institución Rota',              kind: 'skip', status: 'broken' },
  { id: 275, nombre: 'CODELCO',                       kind: 'custom_playwright', status: 'active' },
];

let lastRunBody = null;   // captura el body del POST /scraper/run

function buildSandbox() {
  const html = fs.readFileSync(path.join(WEB, 'gestion.html'), 'utf8');
  const dom = new JSDOM(html, {
    runScripts: 'dangerously',   // permite ejecutar gestion.js como <script> real
    url: 'http://localhost/?k=testpath',
    pretendToBeVisual: true,
  });
  const w = dom.window;
  w.localStorage.clear && w.localStorage.clear();
  w.matchMedia = w.matchMedia || (() => ({ matches: false, addEventListener(){}, removeEventListener(){} }));
  w.confirm = () => true;
  w.alert = () => {};
  // fetch mock: /scraper/catalog → catálogo; /scraper/run → captura y responde.
  w.fetch = (url, opts) => {
    const u = String(url);
    if (u.includes('/scraper/catalog')) {
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(CATALOGO) });
    }
    if (u.includes('/scraper/run')) {
      lastRunBody = JSON.parse((opts && opts.body) || '{}');
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ pid: 1, run_id: 1, dry_run: !!lastRunBody.dry_run, cmd: ['run_all'] }) });
    }
    if (u.includes('/procesos')) {
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ procesos: [] }) });
    }
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) });
  };
  const code = fs.readFileSync(path.join(WEB, 'gestion.js'), 'utf8');
  const s = w.document.createElement('script');
  s.textContent = code;   // script clásico → las funciones top-level quedan en window
  w.document.body.appendChild(s);
  // Autenticar (setea _creds) para que api() adjunte el header y no lance.
  w._saveSession('tok-test', Math.floor(Date.now() / 1000) + 3600, 'admin', 'test');
  return w;
}

let passed = 0, failed = 0;
function ok(l) { passed++; console.log('  ok ' + l); }
function bad(l, d) { failed++; console.log('  FAIL ' + l + (d ? ' — ' + d : '')); }
function assert(c, l, d) { c ? ok(l) : bad(l, d); }
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

(async () => {
  const w = buildSandbox();
  const doc = w.document;

  console.log('## Carga del catálogo y tipos');
  await w._cargarCatalogoRun();
  await sleep(0);
  const tipoSel = doc.getElementById('run-tipo');
  const valores = Array.from(tipoSel.options).map(o => o.value);
  assert(valores[0] === 'all', 'primera opción es "all" (corrida completa)');
  assert(valores.includes('wordpress'), 'aparece el tipo wordpress');
  assert(valores.includes('custom_trabajando'), 'aparece custom_trabajando');
  assert(valores.includes('custom_playwright'), 'aparece custom_playwright (antes faltaba)');
  assert(!valores.includes('skip'), 'los kinds no corribles (skip) no aparecen');

  console.log('\n## Listado de instituciones por tipo');
  tipoSel.value = 'wordpress';
  tipoSel.dispatchEvent(new w.Event('change'));
  await sleep(0);
  let opts = Array.from(doc.getElementById('run-inst-lista').options).map(o => o.value);
  assert(opts.includes('Municipalidad de Viña del Mar'), 'wordpress lista Viña del Mar');
  assert(opts.includes('Municipalidad de Cerro Navia'), 'wordpress lista Cerro Navia');
  assert(!opts.includes('TVN'), 'wordpress NO lista TVN (es de otro tipo)');
  assert(!opts.includes('Institución Rota'), 'no lista instituciones no corribles');

  console.log('\n## Correr TODAS las de un tipo (mode=kind)');
  doc.getElementById('run-inst').value = '';          // sin institución → todas
  await w.runScraper();
  assert(lastRunBody && lastRunBody.mode === 'kind', 'sin institución → mode=kind', JSON.stringify(lastRunBody));
  assert(lastRunBody.kind === 'wordpress', 'kind = wordpress');

  console.log('\n## Correr UNA institución (mode=institucion)');
  lastRunBody = null;
  doc.getElementById('run-inst').value = 'Municipalidad de Cerro Navia';
  await w.runScraper();
  assert(lastRunBody && lastRunBody.mode === 'institucion', 'con institución → mode=institucion');
  assert(lastRunBody.institucion_id === 382, 'institucion_id resuelto = 382');

  console.log('\n## Institución escrita que no existe → no dispara');
  lastRunBody = null;
  doc.getElementById('run-inst').value = 'No Existe SA';
  await w.runScraper();
  assert(lastRunBody === null, 'institución inválida → no se envía el run');

  console.log('\n## Corrida completa (mode=all) ignora institución');
  lastRunBody = null;
  tipoSel.value = 'all';
  tipoSel.dispatchEvent(new w.Event('change'));
  await sleep(0);
  await w.runScraper();
  assert(lastRunBody && lastRunBody.mode === 'all', 'tipo=all → mode=all');

  console.log('\n## EmpleosPublicos sin institución → mode=empleos_publicos');
  lastRunBody = null;
  // Inyectamos el tipo empleos_publicos como si el catálogo lo tuviera.
  tipoSel.add(new w.Option('EmpleosPublicos', 'empleos_publicos'));
  tipoSel.value = 'empleos_publicos';
  tipoSel.dispatchEvent(new w.Event('change'));
  await sleep(0);
  doc.getElementById('run-inst').value = '';
  await w.runScraper();
  assert(lastRunBody && lastRunBody.mode === 'empleos_publicos', 'empleos_publicos sin inst → mode=empleos_publicos');

  // ── Robustez: catálogo SIN kind/status (backend sin source_status) ──
  console.log('\n## Catálogo sin kind/status → respaldo de tipos');
  {
    const w2 = buildSandbox();
    const doc2 = w2.document;
    // Sobrescribir el catálogo por uno sin kind/status.
    w2.fetch = (url, opts) => {
      const u = String(url);
      if (u.includes('/scraper/catalog')) return Promise.resolve({ ok: true, status: 200,
        json: () => Promise.resolve([{ id: 1, nombre: 'Inst A' }, { id: 2, nombre: 'Inst B' }]) });
      if (u.includes('/scraper/run')) { lastRunBody = JSON.parse((opts && opts.body) || '{}');
        return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ pid: 1, cmd: [] }) }); }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) });
    };
    w2._RUN_CAT = undefined; // forzar recarga no aplica (var interna); usamos función directa
    await w2._cargarCatalogoRun();
    await sleep(0);
    const vals = Array.from(doc2.getElementById('run-tipo').options).map(o => o.value);
    assert(vals.length > 1, 'sin kind del backend → igual se muestran tipos (respaldo)');
    assert(vals.includes('wordpress') && vals.includes('custom_ffaa'), 'respaldo incluye todos los tipos');
    // Sin kinds, el listado de instituciones muestra todas para cualquier tipo.
    doc2.getElementById('run-tipo').value = 'wordpress';
    doc2.getElementById('run-tipo').dispatchEvent(new w2.Event('change'));
    await sleep(0);
    const insts2 = Array.from(doc2.getElementById('run-inst-lista').options).map(o => o.value);
    assert(insts2.includes('Inst A') && insts2.includes('Inst B'), 'sin kinds → lista todas las instituciones');
  }

  // ── Robustez: la API del catálogo falla → igual hay tipos para correr ──
  console.log('\n## API de catálogo falla → respaldo de tipos');
  {
    const w3 = buildSandbox();
    const doc3 = w3.document;
    w3.fetch = (url) => {
      if (String(url).includes('/scraper/catalog')) return Promise.reject(new Error('boom'));
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) });
    };
    await w3._cargarCatalogoRun();
    await sleep(0);
    const vals = Array.from(doc3.getElementById('run-tipo').options).map(o => o.value);
    assert(vals.length > 1, 'API caída → tipos igual disponibles (respaldo)');
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
})();
