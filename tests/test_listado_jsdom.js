// tests/test_listado_jsdom.js
// Tests JSDOM del listado de ofertas (renderCard).
// Valida que la estructura del card respeta el spec §7:
//   1. Institución → 2. Cargo → 3. Chips clave (contrato + región)
//   4. Meta secundaria (renta + comuna + publicación)
//   5. Estado + fecha cierre → 6. CTA "Ver detalle" → 7. Favorito ♡
// + asserts de ruido removido (sin badge-sector, sin "Renta no informada")
// + microcopy desde UI_STRINGS
// + toggle de favorito.
//
// Ejecutar: `node tests/test_listado_jsdom.js`
// Requiere: jsdom (`npm install jsdom`).

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
// Bootstrap (idéntico al de test_modal_jsdom.js)
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
  w.localStorage.clear();
  w.matchMedia = w.matchMedia || ((q) => ({
    matches: false, media: q, onchange: null,
    addListener: () => {}, removeListener: () => {},
    addEventListener: () => {}, removeEventListener: () => {},
    dispatchEvent: () => false,
  }));
  w.IntersectionObserver = w.IntersectionObserver || class {
    constructor() {} observe() {} unobserve() {} disconnect() {}
  };
  w.fetch = () => Promise.resolve({
    ok: true, status: 200,
    json: () => Promise.resolve({}),
    text: () => Promise.resolve(''),
  });
  w.console = Object.assign({}, w.console, { error: () => {}, warn: () => {}, debug: () => {} });

  for (const name of ['ui-strings.js', 'rich-text.js', 'app.js']) {
    const code = fs.readFileSync(path.join(WEB_DIR, name), 'utf8');
    try { w.eval(code); } catch (e) { /* startup may throw */ }
  }
  return { dom, window: w, document: w.document };
}

// ─────────────────────────────────────────────────────────────
// Asserts
// ─────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;
function ok(label) { passed++; console.log(`  ok ${label}`); }
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
  if (value) ok(label); else fail(label, `expected truthy, got ${JSON.stringify(value)}`);
}
function assertFalse(value, label) {
  if (!value) ok(label); else fail(label, `expected falsy, got ${JSON.stringify(value)}`);
}

// ─────────────────────────────────────────────────────────────
// Helper: parsear el HTML que retorna renderCard a un Element real
// ─────────────────────────────────────────────────────────────

function renderToElement(window, oferta) {
  const html = window.renderCard(oferta);
  const div = window.document.createElement('div');
  div.innerHTML = html;
  return div.querySelector('.oferta-card');
}

// ─────────────────────────────────────────────────────────────
// Bootstrap
// ─────────────────────────────────────────────────────────────

console.log('\n## Bootstrap renderCard');
const env = buildSandbox();
const { window, document } = env;

assertTrue(typeof window.renderCard === 'function',
  'renderCard expuesto en window');
assertTrue(typeof window.toggleFavCard === 'function',
  'toggleFavCard expuesto en window');
assertTrue(window.UI_STRINGS && window.UI_STRINGS.CTA_VER_DETALLE,
  'UI_STRINGS.CTA_VER_DETALLE definido');

// ─────────────────────────────────────────────────────────────
// Estructura básica + secuencia §7
// ─────────────────────────────────────────────────────────────

console.log('\n## Estructura spec §7');

const ofertaPlena = {
  id: 1001,
  cargo: 'Analista contable',
  institucion: 'Municipalidad de Ñuñoa',
  tipo_contrato: 'Contrata',
  region: 'Metropolitana de Santiago',
  ciudad: 'Ñuñoa',
  sector: 'Municipal',
  renta_bruta_min: 1500000,
  renta_bruta_max: 1800000,
  fecha_cierre: '2099-12-31',  // fecha lejana → siempre activa
  fecha_publicacion: '2099-01-01',
  jornada: '44 horas',
  url_oferta: 'https://ejemplo.cl/postular/1001',
};

const card = renderToElement(window, ofertaPlena);
assertTrue(card != null, 'renderCard produce un .oferta-card');
assertEq(card.dataset.ofertaId, '1001', 'data-oferta-id presente');

// 1. Institución
const inst = card.querySelector('.oferta-institucion');
assertTrue(inst && inst.textContent.includes('Municipalidad de Ñuñoa'),
  'Institución renderizada');

// 2. Cargo
const cargo = card.querySelector('.oferta-cargo');
assertTrue(cargo && cargo.textContent.includes('Analista contable'),
  'Cargo renderizado');

// 3. Chips: máximo 2 (contrato + región). NO badge-sector.
const chips = card.querySelectorAll('.oferta-tipo-wrap .badge');
assertTrue(chips.length === 2, `2 chips esperados (contrato + región), got ${chips.length}`);
assertTrue(card.querySelector('.badge-region') != null, 'Chip región presente');
assertEq(card.querySelector('.badge-sector'), null,
  'badge-sector NO se renderiza (spec §7: máx 2 chips)');

// Verificar orden DOM: institución antes de cargo, cargo antes de chips
const meta = card.querySelector('.oferta-meta');
const children = Array.from(meta.children).map(c => c.className);
assertTrue(
  children.indexOf('oferta-institucion') < children.indexOf('oferta-cargo'),
  'DOM order: institución antes que cargo'
);
assertTrue(
  children.indexOf('oferta-cargo') < children.indexOf('oferta-tipo-wrap'),
  'DOM order: cargo antes que chips'
);

// 4. Meta secundaria
const detalles = card.querySelector('.oferta-detalles');
const renta = detalles.querySelector('.oferta-renta');
assertTrue(renta && renta.textContent.length > 0, 'Renta renderizada');

// 5. Estado + fecha cierre
const plazoText = card.querySelector('.plazo-text');
assertTrue(plazoText && plazoText.textContent.length > 0,
  '.plazo-text renderizado con texto');
assertTrue(plazoText.className.includes('plazo-text--status-'),
  'plazo-text usa modificador --status-X (no inline color)');
const plazoFecha = card.querySelector('.oferta-plazo-fecha');
assertTrue(plazoFecha != null, '.oferta-plazo-fecha presente cuando hay fecha_cierre');

// Sin inline styles en el footer (los antiguos style="color:..." y style="margin-left")
const footerInlineColors = card.querySelectorAll('.oferta-footer [style*="color:"]');
assertEq(footerInlineColors.length, 0,
  'Footer SIN style="color:..." inline (todo via CSS classes)');
const footerInlineMargin = card.querySelectorAll('.oferta-footer [style*="margin-left:"]');
assertEq(footerInlineMargin.length, 0,
  'Footer SIN style="margin-left:..." inline');

// 6. CTA "Ver detalle" usa UI_STRINGS
const btnDetalle = card.querySelector('.btn-detalle');
assertTrue(btnDetalle != null, 'btn-detalle presente');
assertEq(btnDetalle.textContent.trim(), window.UI_STRINGS.CTA_VER_DETALLE,
  'btn-detalle usa UI_STRINGS.CTA_VER_DETALLE');
assertTrue(btnDetalle.textContent.includes('→'),
  'CTA termina en flecha → consistente con CTA primario del modal');

// 7. Favorito secundario (corner top-right)
const favBtn = card.querySelector('.btn-fav-card');
assertTrue(favBtn != null, 'btn-fav-card presente');
assertEq(favBtn.textContent.trim(), '♡',
  'Sin favoritar: ícono ♡');
assertFalse(favBtn.className.includes('activo'),
  'Sin favoritar: clase btn-fav-card NO incluye activo');

// ─────────────────────────────────────────────────────────────
// Render: oferta sin renta no muestra placeholder
// ─────────────────────────────────────────────────────────────

console.log('\n## Sin renta → sin placeholder "Renta no informada"');

const ofertaSinRenta = Object.assign({}, ofertaPlena, {
  id: 1002,
  renta_bruta_min: null,
  renta_bruta_max: null,
});
const cardSinRenta = renderToElement(window, ofertaSinRenta);
const rentaSpan = cardSinRenta.querySelector('.oferta-renta');
assertTrue(rentaSpan == null || rentaSpan.textContent.trim() === '',
  'Sin renta: span .oferta-renta no se renderiza');
assertEq(cardSinRenta.querySelector('.oferta-renta--muted'), null,
  'Sin renta: clase .oferta-renta--muted NO existe (eliminada en PR D)');
const rentaText = cardSinRenta.textContent;
assertFalse(/Renta no informada/i.test(rentaText),
  'Sin renta: NO aparece el texto "Renta no informada"');

// ─────────────────────────────────────────────────────────────
// Render: oferta sin región/ciudad
// ─────────────────────────────────────────────────────────────

console.log('\n## Sin región/ciudad → sin chip ni icono de ubicación');

const ofertaSinRegion = Object.assign({}, ofertaPlena, {
  id: 1003,
  region: null,
  ciudad: null,
});
const cardSinRegion = renderToElement(window, ofertaSinRegion);
assertEq(cardSinRegion.querySelector('.badge-region'), null,
  'Sin región: chip region no se renderiza');
const chipsSinRegion = cardSinRegion.querySelectorAll('.oferta-tipo-wrap .badge');
assertEq(chipsSinRegion.length, 1,
  'Sólo 1 chip (contrato) cuando no hay región');

// ─────────────────────────────────────────────────────────────
// Render: oferta sin tipo_contrato
// ─────────────────────────────────────────────────────────────

console.log('\n## Sin tipo_contrato → sin chip de contrato');
const ofertaSinTipo = Object.assign({}, ofertaPlena, {
  id: 1004,
  tipo_contrato: null,
});
const cardSinTipo = renderToElement(window, ofertaSinTipo);
const chipsSinTipo = Array.from(cardSinTipo.querySelectorAll('.oferta-tipo-wrap .badge'))
  .filter(b => !b.className.includes('badge-region'));
assertEq(chipsSinTipo.length, 0, 'Sin tipo: NO chip de contrato (sólo región sigue)');

// ─────────────────────────────────────────────────────────────
// Toggle favorito vía toggleFavCard
// ─────────────────────────────────────────────────────────────

console.log('\n## toggleFavCard ♡↔♥ + .favorita');

// Inyectar el card al DOM real para que toggleFavCard pueda usar closest()
const lista = document.getElementById('lista-ofertas') || document.body;
const cardLive = renderToElement(window, ofertaPlena);
lista.appendChild(cardLive);

const liveBtn = cardLive.querySelector('.btn-fav-card');
assertEq(liveBtn.textContent.trim(), '♡', 'Estado inicial: ♡');
assertFalse(cardLive.classList.contains('favorita'), 'Card sin clase .favorita inicial');

// Primer toggle: agrega favorito
window.toggleFavCard(liveBtn);
assertEq(liveBtn.textContent.trim(), '♥', 'Tras toggle: ♥');
assertTrue(liveBtn.classList.contains('activo'), 'Botón con clase .activo');
assertTrue(cardLive.classList.contains('favorita'),
  'Card recibe clase .favorita');
const favsAfterAdd = JSON.parse(window.localStorage.getItem('fav_contrataoplanta') || '[]');
assertEq(favsAfterAdd.length, 1, 'localStorage tiene 1 favorito');
assertEq(favsAfterAdd[0].id, 1001, 'localStorage guarda id correcto');

// Segundo toggle: quita favorito
window.toggleFavCard(liveBtn);
assertEq(liveBtn.textContent.trim(), '♡', 'Tras segundo toggle: ♡');
assertFalse(liveBtn.classList.contains('activo'), 'Botón sin clase .activo');
assertFalse(cardLive.classList.contains('favorita'),
  'Card pierde clase .favorita');
const favsAfterRemove = JSON.parse(window.localStorage.getItem('fav_contrataoplanta') || '[]');
assertEq(favsAfterRemove.length, 0, 'localStorage queda vacío');

// ─────────────────────────────────────────────────────────────
// Estado "Cierra hoy" / activa según fecha
// ─────────────────────────────────────────────────────────────

console.log('\n## plazo classes según fecha_cierre');

const ofertaCerrada = Object.assign({}, ofertaPlena, {
  id: 1005,
  fecha_cierre: '2000-01-01',  // pasado
});
const cardCerrada = renderToElement(window, ofertaCerrada);
const plazoCerrada = cardCerrada.querySelector('.plazo-text');
assertTrue(plazoCerrada.className.includes('status-'),
  'plazo-text con status-X siempre presente');

// Una oferta sin fecha_cierre no muestra .oferta-plazo-fecha
const ofertaSinCierre = Object.assign({}, ofertaPlena, {
  id: 1006,
  fecha_cierre: null,
});
const cardSinCierre = renderToElement(window, ofertaSinCierre);
assertEq(cardSinCierre.querySelector('.oferta-plazo-fecha'), null,
  'Sin fecha_cierre: NO se renderiza .oferta-plazo-fecha');

// ─────────────────────────────────────────────────────────────
// Aria + accesibilidad
// ─────────────────────────────────────────────────────────────

console.log('\n## Accesibilidad (aria-label, role, tabindex)');
assertEq(card.getAttribute('role'), 'button', 'card tiene role="button"');
assertEq(card.getAttribute('tabindex'), '0', 'card tabindex=0 (focuseable)');
assertTrue(card.getAttribute('aria-label').includes('Analista contable'),
  'aria-label incluye el cargo');

// ─────────────────────────────────────────────────────────────
// Output final
// ─────────────────────────────────────────────────────────────

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
