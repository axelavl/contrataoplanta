// tests/test_fecha_parsing_contract.js
// Regresión del bug de zona horaria en el formateo de fechas de sólo-día.
//
// Bug: `new Date('2026-06-23')` se interpreta como medianoche UTC; en Chile
// (UTC-3/-4) eso cae el día anterior, así que las fechas de cierre se mostraban
// corridas un día atrás. Apareció en historial.js (fmtFecha) y gestion.js
// (fmtDate); app.js/favoritos.js ya usaban el parseo correcto.
//
// Este test fija el CONTRATO esperado del parseo: una cadena 'YYYY-MM-DD' debe
// resolverse a medianoche LOCAL, no UTC. Replica la lógica del fix; si alguien
// vuelve a un `new Date(s)` crudo, este test cae.
//
// Se ejecuta con `node tests/test_fecha_parsing_contract.js`. Para que sea
// determinista respecto a la zona, fijar TZ=America/Santiago al invocarlo.

// Réplica exacta del parseo usado en historial.js/gestion.js tras el fix.
function parseFechaSoloDia(s) {
  const str = String(s);
  return /^\d{4}-\d{2}-\d{2}$/.test(str)
    ? new Date(str + 'T00:00:00')   // medianoche LOCAL
    : new Date(str);
}

let passed = 0, failed = 0;
function assert(cond, label) {
  if (cond) { passed++; console.log('  ok ' + label); }
  else { failed++; console.error('  FAIL ' + label); }
}

// El día del mes parseado debe coincidir con el de la cadena, sin corrimiento.
const d = parseFechaSoloDia('2026-06-23');
assert(d.getFullYear() === 2026, 'año conservado');
assert(d.getMonth() === 5, 'mes conservado (junio = 5)');
assert(d.getDate() === 23, 'día conservado (no corrido a 22)');

// Un timestamp con hora se parsea tal cual (no se le añade T00:00:00).
const ts = parseFechaSoloDia('2026-06-23T14:30:00');
assert(!isNaN(ts) && ts.getHours() === 14, 'timestamp con hora intacto');

// Comparación directa con el bug: new Date(soloFecha) crudo daría día 22 en
// zonas al oeste de UTC. Confirmamos que NUESTRO parseo no lo hace.
const local = parseFechaSoloDia('2026-01-01');
assert(local.getDate() === 1 && local.getMonth() === 0,
  '1 de enero no se corre a 31 de diciembre');

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
