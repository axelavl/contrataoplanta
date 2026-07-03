'use strict';
/*
  JS del panel de gestión interna (gestion.html).


  Externalizado desde el <script> inline para cumplir el CSP
  `script-src 'self'` de web/_headers (mismo patrón que PR #164 aplicó
  a index.html). Nada de handlers inline: los elementos estáticos usan
  `data-action` / `data-change` / `data-input` y dos listeners
  delegados a nivel de documento; las filas generadas dinámicamente
  usan los mismos atributos.
*/

// ── Configuración ──────────────────────────────────────────────
const RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
const API_BASE = (typeof window.__API_BASE === 'string' && window.__API_BASE.trim())
  ? window.__API_BASE.trim()
  : (location.hostname === 'localhost' || location.hostname === '127.0.0.1') ? '' : RAILWAY_BACKEND;

// El path secreto de la API viene en el parámetro ?k= de la URL.
// Sin él, la página permanece en blanco (el body CSS lo oculta todo).
const _urlParams = new URLSearchParams(location.search);
const ADMIN_PATH  = (_urlParams.get('k') || '').trim();

// Si no hay ?k=, no revelar nada — redirigir al inicio
if (!ADMIN_PATH) {
  document.documentElement.innerHTML = '';
  location.replace('/');
}

// ── Estado ─────────────────────────────────────────────────────
let _creds = null;
let _rol = 'admin';   // rol del usuario en sesión (admin|editor|lector)
let _searchTimer = null;
let _ofertasPagina = 1;
let _editingId = null;
let _fuenteEditId = null;
let _procPollTimer = null;
let _logPollTimer = null;
let _logArchivo = null;
let _catalogData = [];
// Cache de objetos oferta por id para el modal de edición (reemplaza el
// JSON serializado en atributos onclick, incompatible con CSP).
const _itemCache = {};

// ── Auth ───────────────────────────────────────────────────────
// Autenticación por JWT: el frontend pide `POST /auth/login` con la
// contraseña, guarda el token (sessionStorage) y lo adjunta como
// `Authorization: Bearer <jwt>` en cada request hasta que expire o el
// usuario haga logout.
function buildAuthHeaderFromToken(token) {
  return 'Bearer ' + token;
}

function _saveSession(token, expiresAt, rol, usuario) {
  _creds = { header: buildAuthHeaderFromToken(token), token, expiresAt };
  _rol = rol || 'admin';
  sessionStorage.setItem('_gc', JSON.stringify({
    token, expiresAt, k: ADMIN_PATH, rol: _rol, usuario: usuario || '',
  }));
}

function _tokenSigueVivo(expiresAt) {
  // expiresAt viene del backend en segundos UNIX.
  return typeof expiresAt === 'number' && (expiresAt - 30) > (Date.now() / 1000);
}

async function doLogin() {
  const pass = document.getElementById('auth-pass').value;
  if (!pass) return;
  const usuario = (document.getElementById('auth-user')?.value || '').trim();
  const btn = document.getElementById('auth-btn');
  btn.textContent = 'Verificando…'; btn.disabled = true;
  document.getElementById('auth-error').style.display = 'none';
  try {
    const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(usuario ? { usuario, password: pass } : { password: pass }),
    });
    if (r.status === 429) {
      document.getElementById('auth-error').textContent = 'Demasiados intentos. Espera 10 minutos.';
      document.getElementById('auth-error').style.display = 'block';
      btn.textContent = 'Entrar'; btn.disabled = false;
      return;
    }
    if (!r.ok) {
      document.getElementById('auth-error').style.display = 'block';
      btn.textContent = 'Entrar'; btn.disabled = false;
      return;
    }
    const body = await r.json();
    _saveSession(body.token, body.expires_at, body.rol, body.usuario);
    showApp();
  } catch(e) {
    document.getElementById('auth-error').textContent = 'Error de red: ' + e.message;
    document.getElementById('auth-error').style.display = 'block';
    btn.textContent = 'Entrar'; btn.disabled = false;
  }
}

async function logout() {
  // Intento de revocación en el servidor; si falla, igual limpiamos local.
  if (_creds && _creds.header) {
    try {
      await fetch(`${API_BASE}/api/${ADMIN_PATH}/auth/logout`, {
        method: 'POST',
        headers: { Authorization: _creds.header },
      });
    } catch (_) { /* best-effort */ }
  }
  sessionStorage.removeItem('_gc');
  _creds = null;
  _rol = 'admin';
  document.body.classList.remove('rol-lector');
  const _av = document.getElementById('rol-aviso'); if (_av) _av.remove();
  document.getElementById('app').style.display = 'none';
  document.getElementById('auth-screen').style.display = 'flex';
  document.getElementById('auth-pass').value = '';
  const _au = document.getElementById('auth-user'); if (_au) _au.value = '';
  document.getElementById('auth-btn').textContent = 'Entrar';
  document.getElementById('auth-btn').disabled = false;
}

function showApp() {
  document.getElementById('auth-screen').style.display = 'none';
  document.getElementById('app').style.display = 'block';
  aplicarRol();
  loadDashboard();
}

// Ajusta la UI según el rol: oculta pestañas solo-admin y, para lectores,
// muestra un aviso de solo lectura. La seguridad real la impone el backend.
function aplicarRol() {
  const esAdmin = _rol === 'admin';
  const tabsAdmin = ['programacion', 'usuarios'];
  document.querySelectorAll('.sidebar button[data-rol="admin"]').forEach(b => {
    b.style.display = esAdmin ? '' : 'none';
  });
  if (!esAdmin) {
    // Si un admin cerró sesión con una pestaña solo-admin activa y entra un
    // editor/lector sin recargar, hay que sacarlo de esa pestaña y limpiar lo
    // ya renderizado (datos de usuarios/programación no deben quedar a la vista).
    const usuariosTb = document.getElementById('usuarios-tbody');
    if (usuariosTb) usuariosTb.innerHTML = '';
    const schedEstado = document.getElementById('sched-estado');
    if (schedEstado) schedEstado.innerHTML = '';
    const activa = document.querySelector('.tab-panel.active');
    if (activa && tabsAdmin.some(t => activa.id === 'tab-' + t)) {
      const nav = document.querySelector('.sidebar button[data-tab="dashboard"]');
      if (nav) nav.click();
    }
  }
  document.body.classList.toggle('rol-lector', _rol === 'lector');
  let aviso = document.getElementById('rol-aviso');
  if (_rol === 'lector') {
    if (!aviso) {
      aviso = document.createElement('div');
      aviso.id = 'rol-aviso';
      aviso.style.cssText = 'background:#1d4ed822;color:#60a5fa;border-bottom:1px solid #1d4ed844;padding:6px 24px;font-size:12px;text-align:center';
      aviso.textContent = '👁️ Modo solo lectura — tu rol no permite hacer cambios.';
      document.getElementById('app').prepend(aviso);
    }
  } else if (aviso) {
    aviso.remove();
  }
}

// ── API helper ─────────────────────────────────────────────────
async function api(endpoint, options = {}) {
  const opts = {
    ...options,
    headers: { Authorization: _creds.header, 'Content-Type': 'application/json', ...(options.headers||{}) }
  };
  const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}${endpoint}`, opts);
  if (r.status === 401) { logout(); throw new Error('Sesión expirada'); }
  if (r.status === 429) throw new Error('Rate limit: demasiados intentos. Espera 10 min.');
  if (!r.ok) {
    const txt = await r.text().catch(() => '');
    throw new Error(`HTTP ${r.status}: ${txt.slice(0,120)}`);
  }
  return r.json();
}

// ── Toast ──────────────────────────────────────────────────────
function toast(msg, type = 'success') {
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = 'show ' + type;
  clearTimeout(t._timer);
  t._timer = setTimeout(() => { t.className = ''; }, 3000);
}

// ── Tab navigation helpers ────────────────────────────────────
function _switchToTab(tabName) {
  document.querySelectorAll('.sidebar button').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  var btn = document.querySelector('[data-tab=' + tabName + ']');
  if (btn) btn.classList.add('active');
  var panel = document.getElementById('tab-' + tabName);
  if (panel) panel.classList.add('active');
}

// ── Sidebar toggle (responsive) ──────────────────────────────
var _sidebarEl = document.getElementById('sidebar');
var _sidebarOverlay = document.getElementById('sidebar-overlay');
var _sidebarToggle = document.getElementById('sidebar-toggle');
function _closeSidebar() {
  if (_sidebarEl) _sidebarEl.classList.remove('open');
  if (_sidebarOverlay) _sidebarOverlay.classList.remove('open');
}
if (_sidebarToggle) _sidebarToggle.addEventListener('click', function() {
  _sidebarEl.classList.toggle('open');
  _sidebarOverlay.classList.toggle('open');
});
if (_sidebarOverlay) _sidebarOverlay.addEventListener('click', _closeSidebar);

// ── Tabs ───────────────────────────────────────────────────────
document.querySelectorAll('.sidebar button[data-tab]').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.sidebar button').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + tab).classList.add('active');
    _closeSidebar();
    if (tab === 'ofertas')  loadOfertas(1);
    else if (tab === 'estadisticas') loadAnalitica();
    else if (tab === 'scrapers') loadScrapers();
    else if (tab === 'fuentes')  loadFuentes();
    else if (tab === 'revision') loadRevision();
    else if (tab === 'destacadas') loadDestacadas();
    else if (tab === 'alertas')  { loadAlertasTab(); }
    else if (tab === 'config')   loadConfig();
    else if (tab === 'acciones') loadProcesos();
    else if (tab === 'programacion') loadScheduler();
    else if (tab === 'usuarios') loadUsuarios();
    else if (tab === 'cursos')   loadCursos();
  });
});

// ── Cursos (directorio gestionable) ────────────────────────────
const _CURSO_CATS = [
  ['admin-publica', 'Administración y gestión pública'],
  ['finanzas', 'Finanzas y presupuesto público'],
  ['compras', 'Compras públicas'],
  ['derecho', 'Derecho administrativo y probidad'],
  ['rrhh', 'Recursos humanos del Estado'],
  ['salud', 'Salud pública'],
  ['educacion', 'Educación y párvulos'],
  ['ti', 'TI y transformación digital'],
  ['prevencion', 'Prevención de riesgos'],
  ['atencion', 'Atención ciudadana'],
  ['otros', 'Otros'],
];
let _cursoEditId = null;
function _escCurso(s) {
  return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function _cfEl(id) { return document.getElementById(id); }
function _cfSet(v) {
  _cfEl('cf-titulo').value = v.titulo || '';
  _cfEl('cf-proveedor').value = v.proveedor || '';
  _cfEl('cf-categoria').value = v.categoria || 'admin-publica';
  _cfEl('cf-modalidad').value = v.modalidad || '';
  _cfEl('cf-duracion').value = v.duracion || '';
  _cfEl('cf-tipo').value = v.tipo || 'curso';
  _cfEl('cf-url').value = v.url || '';
  var _logoEl = _cfEl('cf-logo'); if (_logoEl) _logoEl.value = v.logo || '';
  _cfEl('cf-descripcion').value = v.descripcion || '';
  _cfEl('cf-orden').value = (v.orden != null ? v.orden : '');
  _cfEl('cf-gratuito').checked = v.gratuito !== false;
  _cfEl('cf-activo').checked = v.activo !== false;
  _cfEl('cf-destacado').checked = (v.nivel === 'destacado');
}
function _cfReset() {
  _cursoEditId = null;
  _cfSet({ gratuito: true, activo: true });
  _cfEl('cf-guardar').textContent = 'Guardar curso';
  _cfEl('cf-cancelar').style.display = 'none';
}
async function _guardarCurso() {
  const titulo = _cfEl('cf-titulo').value.trim();
  if (!titulo) { toast('El título es obligatorio', 'error'); return; }
  const body = {
    titulo,
    proveedor: _cfEl('cf-proveedor').value.trim(),
    categoria: _cfEl('cf-categoria').value,
    modalidad: _cfEl('cf-modalidad').value.trim(),
    duracion: _cfEl('cf-duracion').value.trim(),
    tipo: _cfEl('cf-tipo').value,
    url: _cfEl('cf-url').value.trim(),
    logo: (_cfEl('cf-logo') ? _cfEl('cf-logo').value.trim() : ''),
    descripcion: _cfEl('cf-descripcion').value.trim(),
    orden: parseInt(_cfEl('cf-orden').value, 10) || 100,
    gratuito: _cfEl('cf-gratuito').checked,
    activo: _cfEl('cf-activo').checked,
    nivel: _cfEl('cf-destacado').checked ? 'destacado' : 'estandar',
  };
  try {
    if (_cursoEditId) await api('/cursos/' + _cursoEditId, { method: 'PUT', body: JSON.stringify(body) });
    else await api('/cursos', { method: 'POST', body: JSON.stringify(body) });
    toast('Curso guardado');
    _cfReset();
    loadCursos();
  } catch (e) { toast(e.message, 'error'); }
}
async function loadCursos() {
  if (!window._cursosWired) {
    window._cursosWired = true;
    const g = _cfEl('cf-guardar'); if (g) g.addEventListener('click', _guardarCurso);
    const c = _cfEl('cf-cancelar'); if (c) c.addEventListener('click', _cfReset);
    const ca = document.getElementById('cc-agregar'); if (ca) ca.addEventListener('click', _agregarCategoria);
  }
  loadCategoriasCursos(); // puebla el <select> de categorías y la tabla de gestión
  const cont = document.getElementById('cursos-tabla');
  cont.innerHTML = '<p class="text-muted">Cargando…</p>';
  try {
    const d = await api('/cursos');
    const cursos = d.cursos || [];
    if (!cursos.length) { cont.innerHTML = '<p class="text-muted">Sin cursos. Agrega el primero arriba.</p>'; return; }
    cont.innerHTML = '<table class="data-table" style="width:100%;border-collapse:collapse"><thead><tr>'
      + '<th style="text-align:left;padding:6px">Orden</th><th style="text-align:left;padding:6px">Título</th>'
      + '<th style="text-align:left;padding:6px">Proveedor</th><th style="text-align:left;padding:6px">Categoría</th>'
      + '<th style="text-align:left;padding:6px">Estado</th><th></th></tr></thead><tbody>'
      + cursos.map(c => '<tr style="border-top:1px solid var(--borde,#e5e5e5)">'
        + '<td style="padding:6px">' + (c.orden != null ? c.orden : '') + '</td>'
        + '<td style="padding:6px">' + _escCurso(c.titulo)
          + (c.nivel === 'destacado' ? ' <span class="pill" style="background:#F2C26A;color:#3a2a00">★ Destacado</span>' : '')
          + (c.gratuito ? ' <span class="pill green">Gratis</span>' : '') + '</td>'
        + '<td style="padding:6px">' + _escCurso(c.proveedor || '') + '</td>'
        + '<td style="padding:6px">' + _escCurso(c.categoria || '') + '</td>'
        + '<td style="padding:6px">' + (c.activo ? '<span class="pill green">Activo</span>' : '<span class="pill gray">Oculto</span>') + '</td>'
        + '<td style="padding:6px;white-space:nowrap"><button class="btn btn-ghost btn-sm" data-curso-edit="' + c.id + '">Editar</button> '
        + '<button class="btn btn-ghost btn-sm" data-curso-del="' + c.id + '">Borrar</button></td></tr>').join('')
      + '</tbody></table>';
    cont.querySelectorAll('[data-curso-edit]').forEach(b => b.addEventListener('click', () => {
      const c = cursos.find(x => String(x.id) === b.dataset.cursoEdit);
      if (!c) return;
      _cursoEditId = c.id;
      _cfSet(c);
      _cfEl('cf-guardar').textContent = 'Actualizar curso';
      _cfEl('cf-cancelar').style.display = '';
      document.getElementById('tab-cursos').scrollIntoView({ behavior: 'smooth', block: 'start' });
    }));
    cont.querySelectorAll('[data-curso-del]').forEach(b => b.addEventListener('click', async () => {
      if (!confirm('¿Borrar este curso del directorio?')) return;
      try { await api('/cursos/' + b.dataset.cursoDel, { method: 'DELETE' }); toast('Curso borrado'); loadCursos(); }
      catch (e) { toast(e.message, 'error'); }
    }));
  } catch (e) { cont.innerHTML = '<p class="text-muted">Error: ' + _escCurso(e.message) + '</p>'; }
}

function _escAttrC(s) { return _escCurso(s).replace(/"/g, '&quot;'); }

async function _agregarCategoria() {
  const et = document.getElementById('cc-etiqueta').value.trim();
  if (!et) { toast('Escribe una etiqueta', 'error'); return; }
  const ord = parseInt(document.getElementById('cc-orden').value, 10) || 100;
  try {
    await api('/cursos/categorias', { method: 'POST', body: JSON.stringify({ etiqueta: et, orden: ord }) });
    toast('Categoría agregada');
    document.getElementById('cc-etiqueta').value = '';
    document.getElementById('cc-orden').value = '';
    loadCategoriasCursos();
  } catch (e) { toast(e.message, 'error'); }
}

async function loadCategoriasCursos() {
  let cats = [];
  try { const d = await api('/cursos/categorias'); cats = d.categorias || []; }
  catch (e) { cats = _CURSO_CATS.map(function (c) { return { slug: c[0], etiqueta: c[1] }; }); }
  // Poblar el <select> de categoría del formulario de cursos.
  const sel = _cfEl('cf-categoria');
  if (sel) {
    const actual = sel.value;
    sel.innerHTML = cats.map(function (c) { return '<option value="' + c.slug + '">' + _escCurso(c.etiqueta) + '</option>'; }).join('');
    if (actual) sel.value = actual;
  }
  const cont = document.getElementById('cursos-cats-tabla');
  if (!cont) return;
  if (!cats.length) { cont.innerHTML = '<p class="text-muted">Sin categorías.</p>'; return; }
  cont.innerHTML = '<table class="data-table" style="width:100%;border-collapse:collapse"><thead><tr>'
    + '<th style="text-align:left;padding:6px;width:74px">Orden</th><th style="text-align:left;padding:6px">Etiqueta</th>'
    + '<th style="text-align:left;padding:6px">Slug</th><th></th></tr></thead><tbody>'
    + cats.map(function (c) {
      return '<tr style="border-top:1px solid var(--borde,#e5e5e5)">'
        + '<td style="padding:6px"><input type="number" data-cat-ord="' + c.id + '" value="' + (c.orden != null ? c.orden : 100) + '" style="padding:4px;width:64px"></td>'
        + '<td style="padding:6px"><input data-cat-et="' + c.id + '" value="' + _escAttrC(c.etiqueta) + '" style="padding:5px;width:100%;max-width:300px"></td>'
        + '<td style="padding:6px"><code class="text-small">' + _escCurso(c.slug) + '</code></td>'
        + '<td style="padding:6px;white-space:nowrap"><button class="btn btn-ghost btn-sm" data-cat-save="' + c.id + '">Guardar</button> '
        + (c.slug === 'otros' ? '' : '<button class="btn btn-ghost btn-sm" data-cat-del="' + c.id + '">Borrar</button>')
        + '</td></tr>';
    }).join('') + '</tbody></table>';
  cont.querySelectorAll('[data-cat-save]').forEach(function (b) {
    b.addEventListener('click', async function () {
      const id = b.dataset.catSave;
      const et = cont.querySelector('[data-cat-et="' + id + '"]').value.trim();
      const ord = parseInt(cont.querySelector('[data-cat-ord="' + id + '"]').value, 10) || 100;
      try { await api('/cursos/categorias/' + id, { method: 'PUT', body: JSON.stringify({ etiqueta: et, orden: ord }) }); toast('Categoría actualizada'); loadCategoriasCursos(); }
      catch (e) { toast(e.message, 'error'); }
    });
  });
  cont.querySelectorAll('[data-cat-del]').forEach(function (b) {
    b.addEventListener('click', async function () {
      if (!confirm('¿Borrar esta categoría? Sus cursos pasan a "Otros".')) return;
      try { await api('/cursos/categorias/' + b.dataset.catDel, { method: 'DELETE' }); toast('Categoría borrada'); loadCategoriasCursos(); }
      catch (e) { toast(e.message, 'error'); }
    });
  });
}

// ── Utils ──────────────────────────────────────────────────────
function fmtDate(v) {
  if (!v) return '<span class="text-muted">—</span>';
  // 'YYYY-MM-DD' → medianoche local (no UTC), para que la fecha no se muestre
  // corrida un día atrás en hora de Chile. Ver nota en app.js/historial.js.
  const str = String(v);
  const d = /^\d{4}-\d{2}-\d{2}$/.test(str) ? new Date(str + 'T00:00:00') : new Date(str);
  return isNaN(d) ? v : d.toLocaleDateString('es-CL',{day:'2-digit',month:'short',year:'numeric'});
}
function fmtDt(v) {
  if (!v) return '<span class="text-muted">—</span>';
  const d = new Date(v);
  if (isNaN(d)) return v;
  return d.toLocaleDateString('es-CL',{day:'2-digit',month:'short'}) + ' ' +
         d.toLocaleTimeString('es-CL',{hour:'2-digit',minute:'2-digit'});
}
function fmtDur(s) {
  if (s == null) return '—';
  return s < 60 ? `${Math.round(s)}s` : `${Math.floor(s/60)}m ${Math.round(s%60)}s`;
}
function pill(t, c) {
  if (!t) return '<span class="text-muted">—</span>';
  return `<span class="pill ${c}">${t}</span>`;
}
function decisionPill(d) {
  const m = { EXTRACT:'green', SKIP:'gray', NO_DATA:'yellow', ERROR:'red', sin_evaluar:'gray' };
  return pill(d, m[d]||'gray');
}
function runPill(s) {
  const m = { completado:'green', en_curso:'blue', error:'red', cancelado:'yellow' };
  return pill(s, m[s]||'gray');
}
function trunc(s, n=38) {
  if (!s) return '<span class="text-muted">—</span>';
  const safe = esc(s);
  return safe.length > n ? safe.slice(0,n) + '…' : safe;
}
// Igual que trunc() pero escapa el contenido para insertarlo en el CUERPO de
// una celda. Datos como cargo/institución vienen scrapeados (no confiables):
// sin escapar, markup como <img src=x onerror=...> se inyecta en el mismo
// origin donde vive el JWT admin (la CSP hoy bloquea la ejecución de scripts,
// pero la inyección de HTML es un sink real). El placeholder "—" es HTML propio
// y se conserva sin escapar.
function truncEsc(s, n=38) {
  if (!s) return '<span class="text-muted">—</span>';
  const t = s.length > n ? s.slice(0,n) + '…' : s;
  return esc(t);
}
// Escapa un valor para insertarlo en un atributo HTML de las filas
// generadas (data-nombre, data-email, title=, …).
function escAttr(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function esc(s) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ── DASHBOARD ─────────────────────────────────────────────────
async function loadDashboard() {
  try {
    const d = await api('/stats');
    renderStatCards(d);
    renderRunsTable(d.scraper_runs||[], 'runs-tbody-dash', 9);
    renderUrlStats(d.url_validez||{});
    populateSectors(d.por_sector||[]);
  } catch(e) {
    toast('Error stats: ' + e.message, 'error');
  }
  loadDestacadasStats();
  loadDiagnostico();
}

async function loadDestacadasStats() {
  const g = document.getElementById('destacadas-stats-grid');
  if (!g) return;
  try {
    const d = await api('/destacadas/stats');
    g.innerHTML = `
      <div class="stat-card yellow"><div class="label">⭐ Marcadas manualmente</div><div class="value">${(d.manual||0).toLocaleString()}</div></div>
      <div class="stat-card"><div class="label">Auto (con renta publicada)</div><div class="value">${d.auto_activo?(d.auto||0).toLocaleString():'Desactivado'}</div><div class="sub">${d.auto_activo?'Se suman en la pestaña pública':'Solo manuales en pestaña pública'}</div></div>
      <div class="stat-card blue"><div class="label">Total ofertas activas</div><div class="value">${(d.total_activas||0).toLocaleString()}</div></div>
    `;
  } catch(e) {
    g.innerHTML = `<div class="stat-card"><div class="label">Error</div><div class="sub">${esc(e.message)}</div></div>`;
  }
}

// ── DESTACADAS (tab dedicado) ─────────────────────────────────
function loadDestacadas() {
  _loadDestacadasStats();
  _loadDestacadasTable();
}

async function _loadDestacadasStats() {
  var stats = document.getElementById('dest-stats');
  if (!stats) return;
  try {
    var d = await api('/destacadas/stats');
    stats.innerHTML =
      '<div class="stat-card yellow"><div class="label">⭐ Marcadas manualmente</div><div class="value">' + (d.manual||0).toLocaleString() + '</div></div>'
      + '<div class="stat-card"><div class="label">Auto (con renta)</div><div class="value">' + (d.auto_activo ? (d.auto||0).toLocaleString() : 'Desactivado') + '</div></div>'
      + '<div class="stat-card blue"><div class="label">Total activas</div><div class="value">' + (d.total_activas||0).toLocaleString() + '</div></div>';
  } catch(e) {
    stats.innerHTML = '<div class="stat-card"><div class="label">Error</div><div class="sub">' + esc(e.message) + '</div></div>';
  }
}

async function _loadDestacadasTable() {
  var tbody = document.getElementById('dest-tbody');
  if (!tbody) return;
  tbody.innerHTML = '<tr class="loading-row"><td colspan="7"><span class="spinner"></span></td></tr>';
  try {
    var r = await api('/ofertas?pagina=1&por_pagina=200&destacada=true');
    var ofertas = r.ofertas || r.data || [];
    if (!ofertas.length) {
      tbody.innerHTML = '<tr class="empty-row"><td colspan="7">No hay ofertas destacadas aún. Usa el buscador de arriba para agregar.</td></tr>';
      return;
    }
    tbody.innerHTML = ofertas.map(function(o) {
      var activa = o.activa || o.estado === 'activa';
      return '<tr>'
        + '<td>' + o.id + '</td>'
        + '<td>' + esc(o.cargo||'') + '</td>'
        + '<td class="text-small">' + esc(o.institucion_nombre||o.institucion||'') + '</td>'
        + '<td class="text-small">' + esc(o.region||'') + '</td>'
        + '<td class="text-small">' + (o.fecha_cierre ? fmtDate(o.fecha_cierre) : '—') + '</td>'
        + '<td>' + (activa ? pill('activa','green') : pill(o.estado||'inactiva','red')) + '</td>'
        + '<td><button class="btn btn-danger btn-sm" data-action="dest-quitar" data-id="' + o.id + '">☆ Quitar</button>'
        + (o.url_oferta ? ' <a href="' + escAttr(o.url_oferta) + '" target="_blank" rel="noopener" class="btn btn-ghost btn-sm">🔗</a>' : '')
        + '</td></tr>';
    }).join('');
  } catch(e) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="7" style="color:var(--red)">Error: ' + esc(e.message) + '</td></tr>';
  }
}

async function destSearch() {
  var q = (document.getElementById('dest-buscar')?.value || '').trim();
  var cont = document.getElementById('dest-search-results');
  if (!cont) return;
  if (!q) { cont.innerHTML = '<p class="text-muted" style="padding:8px">Escribe un cargo o institución para buscar.</p>'; return; }
  cont.innerHTML = '<span class="spinner"></span>';
  try {
    var r = await api('/ofertas?pagina=1&por_pagina=20&q=' + encodeURIComponent(q) + '&destacada=false&activa=true');
    var ofertas = r.ofertas || r.data || [];
    if (!ofertas.length) { cont.innerHTML = '<p class="text-muted" style="padding:8px">Sin resultados para «' + esc(q) + '».</p>'; return; }
    cont.innerHTML = '<table style="width:100%"><thead><tr><th>ID</th><th>Cargo</th><th>Institución</th><th>Región</th><th>Acción</th></tr></thead><tbody>'
      + ofertas.map(function(o) {
        return '<tr><td>' + o.id + '</td><td>' + esc(o.cargo||'') + '</td>'
          + '<td class="text-small">' + esc(o.institucion_nombre||o.institucion||'') + '</td>'
          + '<td class="text-small">' + esc(o.region||'') + '</td>'
          + '<td><button class="btn btn-success btn-sm" data-action="dest-agregar" data-id="' + o.id + '">⭐ Destacar</button></td></tr>';
      }).join('')
      + '</tbody></table>';
  } catch(e) { cont.innerHTML = '<p style="color:var(--red);padding:8px">Error: ' + esc(e.message) + '</p>'; }
}

async function destAgregar(id) {
  try {
    await api('/ofertas/' + id + '/toggle-destacada', { method:'POST' });
    toast('Oferta ' + id + ' destacada ⭐');
    loadDestacadas();
    destSearch();
  } catch(e) { toast('Error: ' + e.message, 'error'); }
}

async function destQuitar(id) {
  try {
    await api('/ofertas/' + id + '/toggle-destacada', { method:'POST' });
    toast('Oferta ' + id + ' quitada de destacadas');
    loadDestacadas();
  } catch(e) { toast('Error: ' + e.message, 'error'); }
}

// ── ALERTAS TAB (con toggle master) ───────────────────────────
async function loadAlertasTab() {
  loadAlertas();
  loadEventos();
  _loadAlertasToggleState();
}

async function _loadAlertasToggleState() {
  var toggle = document.getElementById('alertas-master-toggle');
  var status = document.getElementById('alertas-toggle-status');
  if (!toggle || !status) return;
  try {
    var d = await api('/config');
    var c = d.config || d || {};
    var activas = c.alertas_activas === 'true' || c.alertas_activas === true;
    toggle.checked = activas;
    status.textContent = activas
      ? 'Activas — los usuarios pueden suscribirse y reciben alertas'
      : 'Desactivadas — el widget muestra «Próximamente» y no acepta suscripciones';
    status.style.color = activas ? 'var(--green)' : 'var(--muted)';
  } catch(e) {
    status.textContent = 'Error al cargar estado';
  }
}

async function _toggleAlertasMaster() {
  var toggle = document.getElementById('alertas-master-toggle');
  var status = document.getElementById('alertas-toggle-status');
  if (!toggle) return;
  var nuevoEstado = toggle.checked;
  status.textContent = 'Guardando…';
  try {
    var d = await api('/config');
    var c = d.config || d || {};
    c.alertas_activas = nuevoEstado ? 'true' : 'false';
    await api('/config', { method: 'PUT', body: JSON.stringify(c) });
    status.textContent = nuevoEstado
      ? 'Activas — los usuarios pueden suscribirse y reciben alertas'
      : 'Desactivadas — el widget muestra «Próximamente» y no acepta suscripciones';
    status.style.color = nuevoEstado ? 'var(--green)' : 'var(--muted)';
    toast(nuevoEstado ? 'Alertas activadas ✓' : 'Alertas desactivadas');
  } catch(e) {
    toggle.checked = !nuevoEstado;
    status.textContent = 'Error al guardar';
    toast('Error: ' + e.message, 'error');
  }
}

function renderStatCards(d) {
  const t = d.totales||{};
  document.getElementById('stats-grid').innerHTML = `
    <div class="stat-card blue"><div class="label">Ofertas publicadas</div><div class="value">${(t.activas||0).toLocaleString()}</div><div class="sub">de ${(t.total||0).toLocaleString()} en total</div></div>
    <div class="stat-card"><div class="label">Instituciones</div><div class="value">${(t.instituciones||0).toLocaleString()}</div></div>
    <div class="stat-card green"><div class="label">Enlaces que funcionan</div><div class="value">${(t.urls_validas||0).toLocaleString()}</div></div>
    <div class="stat-card red"><div class="label">Enlaces caídos</div><div class="value">${(t.urls_rotas||0).toLocaleString()}</div></div>
    <div class="stat-card yellow"><div class="label">Enlaces sin revisar</div><div class="value">${(t.urls_sin_validar||0).toLocaleString()}</div></div>
    <div class="stat-card"><div class="label">Pendientes de revisión</div><div class="value">${(t.needs_review||0).toLocaleString()}</div></div>
  `;
}

function renderUrlStats(uv) {
  const g = document.getElementById('url-stats-grid');
  if (!Object.keys(uv).length) { g.innerHTML=''; return; }
  g.innerHTML = `
    <div class="stat-card green"><div class="label">Enlace a la oferta OK</div><div class="value">${(uv.url_oferta_validas||0).toLocaleString()}</div></div>
    <div class="stat-card red"><div class="label">Enlace a la oferta caído</div><div class="value">${(uv.url_oferta_rotas||0).toLocaleString()}</div></div>
    <div class="stat-card green"><div class="label">Enlace a las bases OK</div><div class="value">${(uv.url_bases_validas||0).toLocaleString()}</div></div>
    <div class="stat-card red"><div class="label">Enlace a las bases caído</div><div class="value">${(uv.url_bases_rotas||0).toLocaleString()}</div></div>
    <div class="stat-card yellow"><div class="label">Sin revisar hoy</div><div class="value">${(uv.sin_chequear_hoy||0).toLocaleString()}</div></div>
  `;
}

function renderRunsTable(runs, tbodyId, cols) {
  const tbody = document.getElementById(tbodyId);
  if (!runs.length) { tbody.innerHTML=`<tr class="empty-row"><td colspan="${cols}">Sin datos</td></tr>`; return; }
  tbody.innerHTML = runs.map(r => `<tr>
    <td class="text-small">${fmtDt(r.started_at)}</td>
    <td>${runPill(r.status)}</td>
    <td><span class="pill gray">${r.run_mode||'?'}</span></td>
    <td class="text-muted">${r.total_evaluadas??'—'}</td>
    <td style="color:var(--green)">${r.total_nuevas??'—'}</td>
    <td style="color:var(--accent)">${r.total_actualizadas??'—'}</td>
    <td style="color:${r.total_errores?'var(--red)':'var(--muted)'}">${r.total_errores??'—'}</td>
    <td class="text-muted">${r.tasa_precision!=null?r.tasa_precision+'%':'—'}</td>
    <td class="text-muted text-small">${fmtDur(r.duracion_segundos)}</td>
  </tr>`).join('');
}

function populateSectors(ps) {
  const ss = [...new Set(ps.map(s=>s.sector).filter(Boolean))].sort();
  ['f-sector','f-fuente-sector'].forEach(id => {
    const el = document.getElementById(id); if (!el) return;
    const cur = el.value;
    el.innerHTML = `<option value="">Todos los sectores</option>` +
      ss.map(s=>`<option value="${escAttr(s)}"${s===cur?' selected':''}>${s}</option>`).join('');
  });
}

// ── ESTADÍSTICAS DEL SITIO (analítica) ────────────────────────
const _AN_EVENTOS_LABEL = {
  ver_oferta: 'Vieron una oferta', click_postular: 'Clic en “Postular”',
  click_bases: 'Clic en “Ver bases”', suscribir_alerta: 'Se suscribieron a alertas',
  buscar: 'Hicieron una búsqueda', filtrar: 'Aplicaron filtros',
  compartir: 'Compartieron una oferta', ver_curso: 'Vieron un curso',
  click_curso: 'Clic en un curso', descargar_csv: 'Descargaron CSV',
  ver_institucion: 'Vieron una institución',
};
const _AN_DISPOSITIVO = {
  movil: '📱 Móvil', escritorio: '💻 Escritorio', tablet: '📲 Tablet', otro: '❓ Otro',
};

async function loadAnalitica() {
  const dias = parseInt(document.getElementById('an-dias').value) || 30;
  const cards = document.getElementById('an-cards');
  cards.innerHTML = '<div class="stat-card"><div class="label">Cargando…</div><div class="value"><span class="spinner"></span></div></div>';
  try {
    const d = await api(`/analitica?dias=${dias}`);
    const interno = d.interno || {};
    if (interno.disponible === false) {
      cards.innerHTML = `<div class="stat-card" style="grid-column:1/-1"><div class="label" style="color:var(--yellow)">Analítica no disponible</div><div class="sub">${esc(interno.warning || 'Aplica las migraciones de la base de datos para empezar a medir el tráfico.')}</div></div>`;
      ['an-paginas','an-referidos','an-eventos','an-ofertas'].forEach(id => {
        const el = document.getElementById(id); if (el) el.innerHTML = '<tr class="empty-row"><td colspan="4">Sin datos todavía</td></tr>';
      });
      document.getElementById('an-chart').innerHTML = '<p class="text-muted">Aún no hay visitas registradas.</p>';
      document.getElementById('an-dispositivos').innerHTML = '<p class="text-muted">—</p>';
      document.getElementById('an-embudo').innerHTML = '<p class="text-muted">Aún no hay datos del embudo.</p>';
      renderUmami(d.umami || {});
      return;
    }
    renderAnaliticaCards(interno.totales || {});
    renderAnaliticaChart(interno.serie || []);
    renderEmbudo(interno.embudo || []);
    renderAnaliticaLista('an-paginas', interno.top_paginas || [], 'path', 'vistas');
    renderAnaliticaLista('an-referidos', interno.top_referidos || [], 'host', 'visitas', 'Tráfico directo (sin referido)');
    renderDispositivos(interno.dispositivos || []);
    renderEventos(interno.eventos_top || []);
    renderOfertasVistas(interno.ofertas_top || []);
    renderUmami(d.umami || {});
  } catch (e) {
    cards.innerHTML = `<div class="stat-card" style="grid-column:1/-1"><div class="label" style="color:var(--red)">Error</div><div class="sub">${escAttr(e.message)}</div></div>`;
  }
}

function renderAnaliticaCards(t) {
  document.getElementById('an-cards').innerHTML = `
    <div class="stat-card blue"><div class="label">Páginas vistas</div><div class="value">${(t.paginas_vistas||0).toLocaleString()}</div><div class="sub">en el período</div></div>
    <div class="stat-card green"><div class="label">Visitantes</div><div class="value">${(t.visitantes||0).toLocaleString()}</div><div class="sub">aproximado, anónimo</div></div>
    <div class="stat-card"><div class="label">Vistas hoy</div><div class="value">${(t.vistas_hoy||0).toLocaleString()}</div></div>
    <div class="stat-card"><div class="label">Visitantes hoy</div><div class="value">${(t.visitantes_hoy||0).toLocaleString()}</div></div>
    <div class="stat-card yellow"><div class="label">Acciones</div><div class="value">${(t.eventos||0).toLocaleString()}</div><div class="sub">clics y búsquedas</div></div>
  `;
}

// Gráfico de líneas en SVG puro (sin librerías: respeta el CSP script-src 'self').
function renderAnaliticaChart(serie) {
  const cont = document.getElementById('an-chart');
  if (!serie.length) { cont.innerHTML = '<p class="text-muted">Aún no hay visitas registradas en el período.</p>'; return; }
  const W = 760, H = 160, pad = 28;
  const max = Math.max(1, ...serie.map(p => Math.max(p.vistas||0, p.visitantes||0)));
  const n = serie.length;
  const x = i => pad + (n === 1 ? (W-2*pad)/2 : (i*(W-2*pad))/(n-1));
  const y = v => H - pad - ((v||0)/max)*(H-2*pad);
  const linea = key => serie.map((p,i) => `${x(i).toFixed(1)},${y(p[key]).toFixed(1)}`).join(' ');
  const area = `${pad},${H-pad} ${linea('vistas')} ${x(n-1).toFixed(1)},${H-pad}`;
  // Etiquetas: primera, media y última fecha.
  const idxs = n <= 1 ? [0] : [0, Math.floor((n-1)/2), n-1];
  const labels = idxs.map(i => `<text x="${x(i).toFixed(1)}" y="${H-8}" fill="var(--muted)" font-size="10" text-anchor="middle">${(serie[i].dia||'').slice(5)}</text>`).join('');
  cont.innerHTML = `
    <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" preserveAspectRatio="none" role="img" aria-label="Tendencia de visitas">
      <polygon points="${area}" fill="#3b82f618"/>
      <polyline points="${linea('vistas')}" fill="none" stroke="var(--accent)" stroke-width="2"/>
      <polyline points="${linea('visitantes')}" fill="none" stroke="var(--green)" stroke-width="2" stroke-dasharray="4 3"/>
      <line x1="${pad}" y1="${H-pad}" x2="${W-pad}" y2="${H-pad}" stroke="var(--border)"/>
      ${labels}
    </svg>
    <div style="display:flex;gap:18px;justify-content:center;margin-top:8px;font-size:12px;color:var(--muted)">
      <span><span style="display:inline-block;width:14px;height:2px;background:var(--accent);vertical-align:middle"></span> Páginas vistas</span>
      <span><span style="display:inline-block;width:14px;height:0;border-top:2px dashed var(--green);vertical-align:middle"></span> Visitantes</span>
      <span>Máximo diario: <strong>${max.toLocaleString()}</strong></span>
    </div>`;
}

function renderAnaliticaLista(tbodyId, rows, keyLabel, keyVal, vacioLabel) {
  const tbody = document.getElementById(tbodyId);
  if (!rows.length) { tbody.innerHTML = '<tr class="empty-row"><td colspan="2">Sin datos en el período</td></tr>'; return; }
  const max = Math.max(1, ...rows.map(r => r[keyVal]||0));
  tbody.innerHTML = rows.map(r => {
    const label = r[keyLabel] || vacioLabel || '—';
    const val = r[keyVal] || 0;
    const pct = Math.round((val/max)*100);
    return `<tr>
      <td style="max-width:280px">
        <div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(label)}">${escAttr(trunc(label,40))}</div>
        <div style="height:4px;background:var(--surface2);border-radius:3px;margin-top:5px;overflow:hidden"><div style="height:100%;width:${pct}%;background:var(--accent)"></div></div>
      </td>
      <td style="text-align:right;font-weight:600;white-space:nowrap">${val.toLocaleString()}</td>
    </tr>`;
  }).join('');
}

function renderDispositivos(rows) {
  const cont = document.getElementById('an-dispositivos');
  if (!rows.length) { cont.innerHTML = '<p class="text-muted">Sin datos en el período</p>'; return; }
  const total = rows.reduce((a,r) => a + (r.vistas||0), 0) || 1;
  cont.innerHTML = rows.map(r => {
    const pct = Math.round(((r.vistas||0)/total)*100);
    return `<div style="margin-bottom:12px">
      <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:4px">
        <span>${_AN_DISPOSITIVO[r.dispositivo] || escAttr(r.dispositivo||'—')}</span>
        <span class="text-muted">${pct}% · ${(r.vistas||0).toLocaleString()}</span>
      </div>
      <div style="height:8px;background:var(--surface2);border-radius:4px;overflow:hidden"><div style="height:100%;width:${pct}%;background:var(--accent)"></div></div>
    </div>`;
  }).join('');
}

function renderEventos(rows) {
  const tbody = document.getElementById('an-eventos');
  if (!rows.length) { tbody.innerHTML = '<tr class="empty-row"><td colspan="2">Sin acciones registradas</td></tr>'; return; }
  tbody.innerHTML = rows.map(r => `<tr>
    <td>${escAttr(_AN_EVENTOS_LABEL[r.evento] || r.evento || '—')}</td>
    <td style="text-align:right;font-weight:600">${(r.total||0).toLocaleString()}</td>
  </tr>`).join('');
}

function renderOfertasVistas(rows) {
  const tbody = document.getElementById('an-ofertas');
  if (!rows.length) { tbody.innerHTML = '<tr class="empty-row"><td colspan="4">Sin vistas de ofertas en el período</td></tr>'; return; }
  tbody.innerHTML = rows.map(r => `<tr>
    <td class="text-muted text-small">${r.oferta_id}</td>
    <td style="max-width:280px"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(r.cargo)}">${escAttr(trunc(r.cargo,42))}</div></td>
    <td class="text-small text-muted">${escAttr(trunc(r.institucion||'',26))}</td>
    <td style="text-align:right;font-weight:600">${(r.vistas||0).toLocaleString()}</td>
  </tr>`).join('');
}

function renderUmami(umami) {
  const wrap = document.getElementById('an-umami-wrap');
  const cards = document.getElementById('an-umami-cards');
  if (!umami || !umami.configurado) { wrap.style.display = 'none'; return; }
  wrap.style.display = 'block';
  if (umami.error) {
    cards.innerHTML = `<div class="stat-card" style="grid-column:1/-1"><div class="label" style="color:var(--yellow)">Umami no respondió</div><div class="sub">${escAttr(umami.error)}</div></div>`;
    return;
  }
  const s = umami.stats || {};
  // Umami v2 devuelve {pageviews:{value,prev}, visitors:{…}, visits:{…}, bounces:{…}, totaltime:{…}}
  const val = x => (x && typeof x === 'object') ? (x.value ?? 0) : (x ?? 0);
  cards.innerHTML = `
    <div class="stat-card blue"><div class="label">Páginas vistas</div><div class="value">${val(s.pageviews).toLocaleString()}</div></div>
    <div class="stat-card green"><div class="label">Visitantes</div><div class="value">${val(s.visitors).toLocaleString()}</div></div>
    <div class="stat-card"><div class="label">Visitas</div><div class="value">${val(s.visits).toLocaleString()}</div></div>
    <div class="stat-card yellow"><div class="label">Rebotes</div><div class="value">${val(s.bounces).toLocaleString()}</div></div>
  `;
}

function renderEmbudo(pasos) {
  const cont = document.getElementById('an-embudo');
  if (!pasos.length || (pasos[0] && !pasos[0].sesiones)) {
    cont.innerHTML = '<p class="text-muted">Aún no hay suficientes datos para el embudo.</p>';
    return;
  }
  cont.innerHTML = pasos.map((p, i) => {
    const prev = i > 0 ? pasos[i-1].sesiones : null;
    const conv = (prev && prev > 0) ? Math.round((p.sesiones/prev)*100) : null;
    return `<div style="margin-bottom:14px">
      <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:5px">
        <span>${escAttr(p.paso)}</span>
        <span class="text-muted">${(p.sesiones||0).toLocaleString()} · ${p.pct}%${conv!=null?` <span style="color:var(--green)">(${conv}% del paso anterior)</span>`:''}</span>
      </div>
      <div style="height:22px;background:var(--surface2);border-radius:5px;overflow:hidden">
        <div style="height:100%;width:${Math.max(p.pct||0,1)}%;background:linear-gradient(90deg,var(--accent),var(--accent-hover));display:flex;align-items:center;padding-left:8px;color:#fff;font-size:11px;font-weight:600">${p.pct}%</div>
      </div>
    </div>`;
  }).join('');
}

async function exportAnalitica() {
  const dias = parseInt(document.getElementById('an-dias').value) || 30;
  try {
    const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}/analitica/export?dias=${dias}`, {
      headers: { Authorization: _creds.header }
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `estadisticas_${dias}d.csv`; a.click();
    URL.revokeObjectURL(url);
    toast('CSV descargado ✓');
  } catch (e) { toast('Error: ' + e.message, 'error'); }
}

// ── USUARIOS DEL PANEL ─────────────────────────────────────────
let _usuarioEditId = null;
const _ROL_LABEL = { lector: 'Lector', editor: 'Editor', admin: 'Administrador' };

async function loadUsuarios() {
  const tbody = document.getElementById('usuarios-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="7"><span class="spinner"></span></td></tr>`;
  try {
    const d = await api('/usuarios');
    if (d.warning) { tbody.innerHTML = '<tr class="empty-row"><td colspan="7" style="color:var(--yellow)">' + esc(d.warning) + '</td></tr>'; return; }
    const us = d.usuarios || [];
    if (!us.length) { tbody.innerHTML = '<tr class="empty-row"><td colspan="7">Sin usuarios. Crea el primero arriba.</td></tr>'; return; }
    const rolPill = { admin:'blue', editor:'green', lector:'gray' };
    tbody.innerHTML = us.map(u => `<tr>
      <td class="text-muted text-small">${u.id}</td>
      <td><strong>${escAttr(u.usuario)}</strong></td>
      <td class="text-small text-muted">${escAttr(u.nombre||'—')}</td>
      <td>${pill(_ROL_LABEL[u.rol]||u.rol, rolPill[u.rol]||'gray')}</td>
      <td>${u.activo?pill('activo','green'):pill('inactivo','gray')}</td>
      <td class="text-small text-muted">${u.ultimo_login?fmtDt(u.ultimo_login):'nunca'}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-ghost btn-sm" data-action="editar-usuario" data-id="${u.id}" data-usuario="${escAttr(u.usuario)}" data-nombre="${escAttr(u.nombre||'')}" data-rol="${u.rol}" data-activo="${u.activo?1:0}">✏️</button>
        <button class="btn btn-danger btn-sm" style="margin-left:4px" data-action="borrar-usuario" data-id="${u.id}" data-usuario="${escAttr(u.usuario)}">🗑️</button>
      </td>
    </tr>`).join('');
  } catch(e) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="7" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

function editarUsuario(d) {
  _usuarioEditId = parseInt(d.id);
  document.getElementById('us-usuario').value = d.usuario || '';
  document.getElementById('us-nombre').value = d.nombre || '';
  document.getElementById('us-password').value = '';
  document.getElementById('us-rol').value = d.rol || 'editor';
  document.getElementById('us-usuario').disabled = true;  // el usuario no se renombra
  document.getElementById('us-cancelar').style.display = '';
  document.getElementById('tab-usuarios').scrollIntoView({ behavior:'smooth', block:'start' });
}

function cancelarUsuario() {
  _usuarioEditId = null;
  ['us-usuario','us-nombre','us-password'].forEach(id => document.getElementById(id).value='');
  document.getElementById('us-rol').value = 'editor';
  document.getElementById('us-usuario').disabled = false;
  document.getElementById('us-cancelar').style.display = 'none';
}

async function saveUsuario() {
  const usuario = document.getElementById('us-usuario').value.trim();
  const nombre = document.getElementById('us-nombre').value.trim();
  const password = document.getElementById('us-password').value;
  const rol = document.getElementById('us-rol').value;
  try {
    if (_usuarioEditId) {
      const body = { nombre, rol };
      if (password) body.password = password;
      await api(`/usuarios/${_usuarioEditId}`, { method:'PUT', body:JSON.stringify(body) });
      toast('Usuario actualizado ✓');
    } else {
      if (!usuario || usuario.length < 3) { toast('Usuario muy corto (mín. 3)', 'error'); return; }
      if (password.length < 8) { toast('Contraseña muy corta (mín. 8)', 'error'); return; }
      const r = await api('/usuarios', { method:'POST', body:JSON.stringify({ usuario, nombre, password, rol }) });
      toast(`Usuario ${r.usuario} creado ✓`);
    }
    cancelarUsuario();
    loadUsuarios();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function borrarUsuario(id, usuario) {
  if (!confirm(`¿Eliminar la cuenta "${usuario}"?`)) return;
  try { await api(`/usuarios/${id}`, { method:'DELETE' }); toast('Usuario eliminado ✓'); loadUsuarios(); }
  catch(e) { toast('Error: '+e.message,'error'); }
}

// ── PROGRAMACIÓN DE RECOLECCIONES ──────────────────────────────
async function loadScheduler() {
  const cont = document.getElementById('sched-estado');
  cont.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await api('/scheduler');
    if (!d.disponible) { cont.innerHTML = `<div style="color:var(--yellow)">${esc(d.warning||'No disponible')}</div>`; return; }
    const e = d.estado || {};
    document.getElementById('sched-activo').checked = e.activo === true;
    document.getElementById('sched-intervalo').value = e.intervalo_horas || 24;
    document.getElementById('sched-modo').value = e.modo || 'completa';
    document.getElementById('sched-limite').value = e.limite_fuentes != null ? e.limite_fuentes : '';
    const estadoTxt = e.activo
      ? `<span style="color:var(--green)">● Activa</span> — próxima corrida: <strong>${e.proxima_ejecucion?fmtDt(e.proxima_ejecucion):'—'}</strong>`
      : '<span class="text-muted">○ Desactivada</span>';
    cont.innerHTML = `<div style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;font-size:13px">
      ${estadoTxt}<br>
      <span class="text-muted">Última corrida: ${e.ultima_ejecucion?fmtDt(e.ultima_ejecucion):'nunca'}</span>
    </div>`;
  } catch(err) { cont.innerHTML = `<div style="color:var(--red)">Error: ${err.message}</div>`; }
}

async function saveScheduler() {
  const _lim = parseInt(document.getElementById('sched-limite').value);
  const body = {
    activo: document.getElementById('sched-activo').checked,
    intervalo_horas: parseInt(document.getElementById('sched-intervalo').value) || 24,
    modo: document.getElementById('sched-modo').value,
    limite_fuentes: Number.isFinite(_lim) ? _lim : 0,
  };
  try {
    await api('/scheduler', { method:'PUT', body:JSON.stringify(body) });
    toast('Programación guardada ✓');
    loadScheduler();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

// ── OFERTAS ───────────────────────────────────────────────────
function debounceSearch() {
  clearTimeout(_searchTimer);
  _searchTimer = setTimeout(() => loadOfertas(1), 350);
}

async function loadOfertas(pag=1) {
  _ofertasPagina = pag;
  const p = new URLSearchParams({
    pagina: pag, por_pagina: 25,
    orden: document.getElementById('f-orden').value,
  });
  const q = document.getElementById('f-q').value.trim();
  const a = document.getElementById('f-activa').value;
  const u = document.getElementById('f-url-rota').value;
  const s = document.getElementById('f-sector').value;
  if (q) p.set('q', q);
  if (a) p.set('activa', a);
  if (u) p.set('url_rota', u);
  if (s) p.set('sector', s);
  // Filtros avanzados (segunda fila)
  const instId = document.getElementById('f-inst-id').value.trim();
  const estado = document.getElementById('f-estado').value;
  const cDesde = document.getElementById('f-cierre-desde').value;
  const cHasta = document.getElementById('f-cierre-hasta').value;
  const nrev   = document.getElementById('f-needs-review').value;
  const sinRenta = document.getElementById('f-sin-renta').checked;
  const origen = document.getElementById('f-origen')?.value;
  const dest = document.getElementById('f-destacada').value;
  if (instId) p.set('institucion_id', instId);
  if (estado) p.set('estado', estado);
  if (cDesde) p.set('cierre_desde', cDesde);
  if (cHasta) p.set('cierre_hasta', cHasta);
  if (nrev)   p.set('needs_review', nrev);
  if (sinRenta) p.set('sin_renta', 'true');
  if (origen) p.set('origen', origen);
  if (dest) p.set('destacada', dest);

  _limpiarSeleccion();
  const tbody = document.getElementById('ofertas-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="9"><span class="spinner"></span></td></tr>`;
  try {
    const d = await api(`/ofertas?${p}`);
    renderOfertasTable(d.ofertas||[]);
    renderPaginacion(d, 'ofertas-paginacion');
    document.getElementById('ofertas-badge').textContent = `${(d.total||0).toLocaleString()} ofertas`;
  } catch(e) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="9" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

// ── Selección múltiple de ofertas ──────────────────────────────
function _seleccionadas() {
  return [...document.querySelectorAll('.sel-oferta:checked')].map(c => parseInt(c.dataset.id));
}

function _limpiarSeleccion() {
  document.querySelectorAll('.sel-oferta:checked').forEach(c => { c.checked = false; });
  const selAll = document.getElementById('sel-all-ofertas');
  if (selAll) selAll.checked = false;
  _actualizarBulkBar();
}

function _actualizarBulkBar() {
  const n = _seleccionadas().length;
  const bar = document.getElementById('ofertas-bulkbar');
  bar.style.display = n > 0 ? 'flex' : 'none';
  document.getElementById('sel-count').textContent = `${n} seleccionada${n!==1?'s':''}`;
}

async function bulkSeleccionadas(accion) {
  const ids = _seleccionadas();
  if (!ids.length) return;
  if (accion === 'desactivar') {
    if (!confirm(`¿Desactivar ${ids.length} oferta(s) seleccionada(s)?`)) return;
    try {
      const r = await api('/ofertas/bulk-desactivar', { method:'POST', body: JSON.stringify({ ids }) });
      toast(`${r.desactivadas} ofertas desactivadas ✓`);
      loadOfertas(_ofertasPagina);
    } catch(e) { toast('Error: '+e.message,'error'); }
  } else if (accion === 'revisadas') {
    try {
      const r = await api('/ofertas/bulk-marcar-revisadas', { method:'POST', body: JSON.stringify({ ids }) });
      toast(`${r.marcadas} ofertas marcadas como revisadas ✓`);
      loadOfertas(_ofertasPagina);
    } catch(e) { toast('Error: '+e.message,'error'); }
  } else if (accion === 'destacar' || accion === 'quitar-destacada') {
    const dest = accion === 'destacar';
    try {
      const r = await api('/ofertas/bulk-destacar', { method:'POST', body: JSON.stringify({ ids, destacada: dest }) });
      toast(`${r.afectadas} oferta(s) ${dest ? 'destacadas ⭐' : 'quitadas de destacadas'}`);
      loadOfertas(_ofertasPagina);
    } catch(e) { toast('Error: '+e.message,'error'); }
  }
}

function renderOfertasTable(ofertas) {
  const tbody = document.getElementById('ofertas-tbody');
  if (!ofertas.length) { tbody.innerHTML='<tr class="empty-row"><td colspan="9">Sin resultados</td></tr>'; return; }
  tbody.innerHTML = ofertas.map(o => {
    _itemCache[o.id] = o;
    const activa = o.activa !== false;
    const destacada = o.destacada === true;
    const urlOk  = o.url_oferta_valida;
    const urlIcon = o.url_oferta ? (urlOk===false?'🔴':urlOk===true?'🟢':'⚪') : '<span class="text-muted">—</span>';
    const inst = o.institucion_display || o.institucion_nombre || '<span class="text-muted">—</span>';
    return `<tr>
      <td><input type="checkbox" class="sel-oferta" data-id="${o.id}"></td>
      <td class="text-muted text-small">${o.id}</td>
      <td style="max-width:220px">
        <div title="${escAttr(o.cargo)}" style="font-weight:500">${destacada?'<span title="Destacada en redes sociales">⭐ </span>':''}${truncEsc(o.cargo,36)}</div>
        <div class="text-small text-muted" title="${escAttr(inst)}">${truncEsc(inst,34)}</div>
      </td>
      <td class="text-small text-muted">${trunc(o.sector_real||'',18)}</td>
      <td class="text-small text-muted">${trunc(o.region||'',14)}</td>
      <td class="text-small">${o.fecha_cierre?fmtDate(o.fecha_cierre):'<span class="text-muted">—</span>'}</td>
      <td>${activa?pill('activa','green'):pill(o.estado||'inactiva','red')}</td>
      <td style="text-align:center" title="${escAttr(o.url_oferta)}">${urlIcon}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm ${activa?'btn-danger':'btn-success'}" data-action="toggle-activa" data-id="${o.id}">${activa?'Pausar':'Activar'}</button>
        <button class="btn btn-sm" style="margin-left:4px${destacada?';background:var(--naran,#f59e0b);color:#fff':''}" data-action="toggle-destacada" data-id="${o.id}" title="${destacada?'Quitar de Destacadas (redes sociales)':'Destacar en redes sociales'}">${destacada?'⭐':'☆'}</button>
        <button class="btn btn-ghost btn-sm" style="margin-left:4px" data-action="open-edit" data-id="${o.id}">✏️</button>
        ${o.url_oferta?`<a href="${escAttr(o.url_oferta)}" target="_blank" rel="noopener" class="btn btn-ghost btn-sm" style="margin-left:4px">🔗</a>`:''}
      </td>
    </tr>`;
  }).join('');
}

function renderPaginacion(d, id) {
  const el = document.getElementById(id);
  if (!d.paginas || d.paginas <= 1) { el.innerHTML=''; return; }
  const cur=d.pagina, tot=d.paginas;
  const pages=[];
  const s=Math.max(1,cur-2), e=Math.min(tot,cur+2);
  if (s>1) pages.push(1,'…');
  for(let i=s;i<=e;i++) pages.push(i);
  if (e<tot) pages.push('…',tot);
  el.innerHTML = `
    <span>${d.total.toLocaleString()} resultados · pág. ${cur}/${tot}</span>
    <div class="pages">
      <button ${cur<=1?'disabled':''} data-action="pagina" data-page="${cur-1}">‹</button>
      ${pages.map(p=>p==='…'?`<button disabled>…</button>`:`<button class="${p===cur?'active':''}" data-action="pagina" data-page="${p}">${p}</button>`).join('')}
      <button ${cur>=tot?'disabled':''} data-action="pagina" data-page="${cur+1}">›</button>
    </div>`;
}

async function toggleActiva(id) {
  try {
    const r = await api(`/ofertas/${id}/toggle-activa`, { method:'POST' });
    toast(`Oferta ${id} → ${r.activa?'activada ✓':'desactivada'}`);
    loadOfertas(_ofertasPagina);
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function toggleDestacada(id) {
  try {
    const r = await api(`/ofertas/${id}/toggle-destacada`, { method:'POST' });
    toast(`Oferta ${id} → ${r.destacada?'destacada en redes ⭐':'quitada de Destacadas'}`);
    loadOfertas(_ofertasPagina);
  } catch(e) { toast('Error: '+e.message,'error'); }
}

// ── EDIT MODAL ────────────────────────────────────────────────
let _creandoOferta = false;

// Valores estandarizados para el editor de ofertas. Selects en vez de texto
// libre → datos consistentes (evita "R.M." vs "Metropolitana", "cod. trabajo"
// vs "Código del Trabajo", etc.).
const EDIT_REGIONES = [
  'Arica y Parinacota','Tarapacá','Antofagasta','Atacama','Coquimbo','Valparaíso',
  'Metropolitana de Santiago',"O'Higgins",'Maule','Ñuble','Biobío','La Araucanía',
  'Los Ríos','Los Lagos','Aysén','Magallanes',
];
const EDIT_TIPOS_CONTRATO = [
  'Planta','Contrata','Honorarios','Código del Trabajo','Reemplazo','Suplencia','Otro',
];
const EDIT_SECTORES = [
  'Salud','Educación','Municipal','Justicia','Seguridad','Defensa','Interior',
  'Obras Públicas','Vivienda y Urbanismo','Transportes','Agricultura',
  'Medio Ambiente','Economía','Hacienda','Trabajo y Previsión Social',
  'Desarrollo Social','Cultura','Deporte','Ciencia y Tecnología','Energía',
  'Minería','Relaciones Exteriores','Gobierno','Otro',
];

// Llena un <select> con las opciones estándar y selecciona el valor actual. Si
// el valor guardado no está en la lista (dato heredado no estandarizado), se
// antepone como opción para NO perderlo al editar otro campo.
function poblarSelectEdit(id, opciones, valorActual) {
  const el = document.getElementById(id);
  if (!el) return;
  const actual = (valorActual == null ? '' : String(valorActual)).trim();
  const extra = (actual && !opciones.includes(actual))
    ? `<option value="${escAttr(actual)}" selected>${escAttr(actual)} (actual)</option>`
    : '';
  el.innerHTML =
    `<option value="">— Sin especificar —</option>` + extra +
    opciones.map(o => `<option value="${escAttr(o)}"${o === actual ? ' selected' : ''}>${escAttr(o)}</option>`).join('');
  el.value = actual || '';
}

function openCrearOferta() {
  _creandoOferta = true;
  _editingId = null;
  document.getElementById('edit-modal-title').childNodes[0].textContent = 'Nueva oferta ';
  document.getElementById('edit-id').textContent = '';
  document.getElementById('edit-institucion-group').style.display = '';
  document.getElementById('edit-save-btn').textContent = 'Crear';
  ['edit-cargo','edit-institucion','edit-descripcion','edit-requisitos','edit-fecha-cierre',
   'edit-renta-min','edit-renta-max','edit-url-oferta','edit-url-bases']
    .forEach(id => { document.getElementById(id).value = ''; });
  poblarSelectEdit('edit-region', EDIT_REGIONES, '');
  poblarSelectEdit('edit-tipo-contrato', EDIT_TIPOS_CONTRATO, '');
  poblarSelectEdit('edit-sector', EDIT_SECTORES, '');
  document.getElementById('edit-estado').value = 'activa';
  document.getElementById('edit-modal').classList.add('open');
}

function openEdit(id, o) {
  _creandoOferta = false;
  _editingId = id;
  o = o || _itemCache[id] || {};
  document.getElementById('edit-modal-title').childNodes[0].textContent = 'Editar oferta ';
  document.getElementById('edit-institucion-group').style.display = 'none';
  document.getElementById('edit-save-btn').textContent = 'Guardar';
  document.getElementById('edit-id').textContent = `#${id}`;
  document.getElementById('edit-cargo').value = o.cargo||'';
  document.getElementById('edit-descripcion').value = o.descripcion||'';
  document.getElementById('edit-requisitos').value = o.requisitos||o.requisitos_texto||'';
  document.getElementById('edit-fecha-cierre').value = o.fecha_cierre ? o.fecha_cierre.slice(0,10) : '';
  document.getElementById('edit-estado').value = o.estado||'activa';
  poblarSelectEdit('edit-region', EDIT_REGIONES, o.region);
  poblarSelectEdit('edit-tipo-contrato', EDIT_TIPOS_CONTRATO, o.tipo_contrato);
  poblarSelectEdit('edit-sector', EDIT_SECTORES, o.sector);
  document.getElementById('edit-renta-min').value = o.renta_bruta_min??'';
  document.getElementById('edit-renta-max').value = o.renta_bruta_max??'';
  document.getElementById('edit-url-oferta').value = o.url_oferta||'';
  document.getElementById('edit-url-bases').value = o.url_bases||'';
  document.getElementById('edit-modal').classList.add('open');
}
function closeModal() {
  document.getElementById('edit-modal').classList.remove('open');
  _editingId = null;
  _creandoOferta = false;
}
async function saveEdit() {
  if (!_editingId && !_creandoOferta) return;
  const raw = {
    cargo:         document.getElementById('edit-cargo').value,
    descripcion:   document.getElementById('edit-descripcion').value||null,
    requisitos:    document.getElementById('edit-requisitos').value||null,
    fecha_cierre:  document.getElementById('edit-fecha-cierre').value||null,
    estado:        document.getElementById('edit-estado').value,
    region:        document.getElementById('edit-region').value||null,
    tipo_contrato: document.getElementById('edit-tipo-contrato').value||null,
    sector:        document.getElementById('edit-sector').value||null,
    renta_bruta_min: parseInt(document.getElementById('edit-renta-min').value)||null,
    renta_bruta_max: parseInt(document.getElementById('edit-renta-max').value)||null,
    url_oferta:    document.getElementById('edit-url-oferta').value.trim()||null,
    url_bases:     document.getElementById('edit-url-bases').value.trim()||null,
  };
  const payload = Object.fromEntries(Object.entries(raw).filter(([,v])=>v!=null&&v!==''));
  try {
    if (_creandoOferta) {
      payload.institucion_nombre = document.getElementById('edit-institucion').value.trim();
      if (!payload.cargo || !payload.institucion_nombre) {
        toast('Cargo e institución son requeridos', 'error');
        return;
      }
      const r = await api('/ofertas', { method:'POST', body:JSON.stringify(payload) });
      toast(`Oferta creada — ID ${r.id} ✓`);
    } else {
      await api(`/ofertas/${_editingId}`, { method:'PUT', body:JSON.stringify(payload) });
      toast('Oferta actualizada ✓');
    }
    closeModal();
    loadOfertas(_ofertasPagina);
  } catch(e) { toast('Error: '+e.message,'error'); }
}
document.getElementById('edit-modal').addEventListener('click', e => { if(e.target===e.currentTarget) closeModal(); });

// ── SCRAPERS ──────────────────────────────────────────────────
async function loadScrapers() {
  const tbody = document.getElementById('scrapers-tbody');
  const conDetalle = document.getElementById('scrapers-con-detalle').checked;
  tbody.innerHTML = `<tr class="loading-row"><td colspan="11"><span class="spinner"></span></td></tr>`;
  try {
    const runs = await api(`/scraper-runs?limit=50&con_detalle=${conDetalle}`);
    if (!runs.length) { tbody.innerHTML='<tr class="empty-row"><td colspan="11">Sin corridas</td></tr>'; return; }
    tbody.innerHTML = runs.map(r => {
      // Celda de notas / instituciones
      let notasCell = `<span class="text-muted">${trunc(r.notas,28)||'—'}</span>`;
      if (conDetalle && r.instituciones?.length) {
        const insts = r.instituciones;
        const conNuevas = insts.filter(i=>i.nuevas>0);
        const conErrores = insts.filter(i=>i.errores>0);
        notasCell = `
          <details>
            <summary style="cursor:pointer;color:var(--accent);font-size:12px">
              ${insts.length} instituciones · ${conNuevas.length} con nuevas${conErrores.length?` · <span style="color:var(--red)">${conErrores.length} errores</span>`:''}
            </summary>
            <div style="margin-top:8px;max-height:200px;overflow-y:auto;font-size:11px">
              <table style="width:100%;border-collapse:collapse">
                <thead><tr style="color:var(--muted);border-bottom:1px solid var(--border)">
                  <th style="text-align:left;padding:2px 6px">Institución</th>
                  <th style="text-align:right;padding:2px 4px">Enc.</th>
                  <th style="text-align:right;padding:2px 4px;color:var(--green)">Nuevas</th>
                  <th style="text-align:right;padding:2px 4px">Exist.</th>
                  <th style="text-align:right;padding:2px 4px;color:var(--red)">Err.</th>
                </tr></thead>
                <tbody>
                  ${insts.map(i=>`<tr style="border-bottom:1px solid #ffffff08">
                    <td style="padding:2px 6px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(i.nombre)}">${truncEsc(i.nombre,32)}</td>
                    <td style="text-align:right;padding:2px 4px;color:var(--muted)">${i.encontradas}</td>
                    <td style="text-align:right;padding:2px 4px;color:${i.nuevas>0?'var(--green)':'var(--muted)'};font-weight:${i.nuevas>0?600:400}">${i.nuevas}</td>
                    <td style="text-align:right;padding:2px 4px;color:var(--muted)">${i.existian}</td>
                    <td style="text-align:right;padding:2px 4px;color:${i.errores>0?'var(--red)':'var(--muted)'}">${i.errores||'—'}</td>
                  </tr>`).join('')}
                </tbody>
              </table>
            </div>
          </details>`;
      }
      return `<tr>
        <td class="text-small">${fmtDt(r.started_at)}</td>
        <td class="text-small">${fmtDt(r.finished_at)}</td>
        <td>${runPill(r.status)}</td>
        <td><span class="pill gray">${r.run_mode||'batch'}</span></td>
        <td class="text-muted">${r.total_instituciones??'—'}</td>
        <td style="color:var(--green);font-weight:${r.total_nuevas?600:400}">${r.total_nuevas??'—'}</td>
        <td style="color:var(--accent)">${r.total_actualizadas??'—'}</td>
        <td style="color:${r.total_errores?'var(--red)':'var(--muted)'}">${r.total_errores??'—'}</td>
        <td class="text-muted">${r.tasa_precision!=null?r.tasa_precision+'%':'—'}</td>
        <td class="text-small text-muted">${fmtDur(r.duracion_segundos)}</td>
        <td class="text-small">${notasCell}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    tbody.innerHTML=`<tr class="empty-row"><td colspan="11" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

// ── FUENTES ───────────────────────────────────────────────────
async function loadFuentes() {
  const tbody = document.getElementById('fuentes-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="11"><span class="spinner"></span></td></tr>`;
  const p = new URLSearchParams();
  const s = document.getElementById('f-fuente-sector').value;
  const c = document.getElementById('f-con-ofertas').value;
  const dv = document.getElementById('f-decision').value;
  if (s) p.set('sector', s);
  if (c) p.set('con_ofertas', c);
  try {
    let fs = await api(`/fuentes?${p}`);
    if (dv) fs = fs.filter(f=>(f.ultima_decision||'sin_evaluar')===dv);
    document.getElementById('fuentes-badge').textContent = `${fs.length.toLocaleString()} instituciones`;
    if (!fs.length) { tbody.innerHTML='<tr class="empty-row"><td colspan="11">Sin resultados</td></tr>'; return; }
    const em = {wordpress:'blue',generic:'gray',custom_trabajando:'orange',custom_hiringroom:'orange',
                custom_buk:'orange',empleos_publicos:'blue',custom_playwright:'yellow'};
    tbody.innerHTML = fs.map(f=>`<tr>
      <td class="text-muted text-small">${f.id}</td>
      <td style="max-width:220px"><div title="${escAttr(f.nombre)}">${truncEsc(f.nombre||'—',32)}</div></td>
      <td class="text-small text-muted">${trunc(f.sector||'—',20)}</td>
      <td>${f.recommended_extractor?`<span class="pill ${em[f.recommended_extractor]||'gray'}">${f.recommended_extractor}</span>`:'<span class="text-muted">—</span>'}</td>
      <td>${decisionPill(f.ultima_decision||'sin_evaluar')}</td>
      <td class="text-muted text-small">${f.confidence!=null?Math.round(f.confidence*100)+'%':'—'}</td>
      <td class="text-muted text-small">${f.availability!=null?Math.round(f.availability*100)+'%':'—'}</td>
      <td class="text-small">${f.http_status?`<span class="${f.http_status<400?'text-muted':'pill red'}">${f.http_status}</span>`:'—'}</td>
      <td style="color:${f.oferta_count>0?'var(--green)':'var(--muted)'};font-weight:${f.oferta_count>0?600:400}">${f.oferta_count??0}</td>
      <td class="text-small text-muted">${f.ultima_evaluacion?fmtDate(f.ultima_evaluacion):'—'}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-ghost btn-sm" data-action="editar-fuente" data-id="${f.id}">✏️</button>
        <button class="btn btn-ghost btn-sm" style="margin-left:4px" title="Override de clasificación" data-action="override-fuente" data-id="${f.id}" data-nombre="${escAttr((f.nombre||'').slice(0,25))}">⚙️</button>
        <button class="btn btn-danger btn-sm" style="margin-left:4px" data-action="desactivar-fuente" data-id="${f.id}" data-nombre="${escAttr((f.nombre||'').slice(0,25))}">🗑️</button>
      </td>
    </tr>`).join('');
  } catch(e) {
    tbody.innerHTML=`<tr class="empty-row"><td colspan="11" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

// ── Export CSV ─────────────────────────────────────────────────
async function exportOfertas() {
  const q = document.getElementById('f-q').value.trim();
  const a = document.getElementById('f-activa').value;
  const s = document.getElementById('f-sector').value;
  const p = new URLSearchParams();
  if (q) p.set('q', q);
  if (a) p.set('activa', a);
  if (s) p.set('sector', s);
  // Descarga directa con auth header (fetch + Blob)
  try {
    const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}/ofertas/export?${p}`, {
      headers: { Authorization: _creds.header }
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a2 = document.createElement('a');
    a2.href = url; a2.download = 'ofertas.csv'; a2.click();
    URL.revokeObjectURL(url);
    toast('CSV descargado ✓');
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function exportSubs() {
  try {
    const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}/suscripciones/export`, {
      headers: { Authorization: _creds.header }
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a2 = document.createElement('a');
    a2.href = url; a2.download = 'suscripciones.csv'; a2.click();
    URL.revokeObjectURL(url);
    toast('CSV descargado ✓');
  } catch(e) { toast('Error: '+e.message,'error'); }
}

// ── ALERTAS / SUSCRIPCIONES ────────────────────────────────────
async function loadAlertas() {
  const activa = document.getElementById('f-sub-activa').value;
  const tbody = document.getElementById('subs-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="10"><span class="spinner"></span></td></tr>`;
  document.getElementById('alertas-stats').innerHTML = '';
  try {
    const p = new URLSearchParams();
    if (activa) p.set('activa', activa);
    const d = await api(`/suscripciones?${p}`);
    const r = d.resumen||{};
    document.getElementById('alertas-stats').innerHTML = `
      <div class="stat-card blue"><div class="label">Total suscriptores</div><div class="value">${r.total??0}</div></div>
      <div class="stat-card green"><div class="label">Activos</div><div class="value">${r.activas??0}</div></div>
      <div class="stat-card"><div class="label">Emails únicos</div><div class="value">${r.emails_unicos??0}</div></div>
      <div class="stat-card"><div class="label">Con región</div><div class="value">${r.con_region??0}</div></div>
      <div class="stat-card"><div class="label">Con término</div><div class="value">${r.con_termino??0}</div></div>
      <div class="stat-card"><div class="label">Con sector</div><div class="value">${r.con_sector??0}</div></div>
    `;
    const subs = d.suscripciones||[];
    if (!subs.length) {
      tbody.innerHTML='<tr class="empty-row"><td colspan="10">Sin suscriptores</td></tr>';
      return;
    }
    tbody.innerHTML = subs.map(s=>`<tr>
      <td class="text-muted text-small">${s.id}</td>
      <td>${esc(s.email)}</td>
      <td class="text-small text-muted">${esc(s.region||'—')}</td>
      <td class="text-small text-muted">${esc(s.termino||'—')}</td>
      <td class="text-small text-muted">${esc(s.sector||'—')}</td>
      <td class="text-small text-muted">${esc(s.tipo_contrato||'—')}</td>
      <td><span class="pill gray">${s.frecuencia||'diaria'}</span></td>
      <td>${s.activa?pill('activo','green'):pill('inactivo','gray')}</td>
      <td class="text-small text-muted">${fmtDate(s.creada_en)}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-ghost btn-sm" data-action="test-email-sub" data-email="${escAttr(s.email)}">✉️</button>
        ${s.activa?`<button class="btn btn-danger btn-sm" style="margin-left:4px" data-action="desactivar-sub" data-id="${s.id}" data-email="${escAttr(s.email)}">✕</button>`:''}
      </td>
    </tr>`).join('');
  } catch(e) {
    tbody.innerHTML=`<tr class="empty-row"><td colspan="10" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

async function enviarAlertas() {
  const emailFiltro = document.getElementById('al-email-filtro').value.trim();
  const horas = parseInt(document.getElementById('al-horas').value)||24;
  const dryRun = document.getElementById('al-dry-run').checked;
  const res = document.getElementById('al-resultado');
  res.style.display='block';
  res.innerHTML='<span class="spinner"></span> Procesando…';
  try {
    const r = await api('/alertas/enviar', {
      method:'POST',
      body: JSON.stringify({ email: emailFiltro||undefined, horas, dry_run: dryRun }),
    });
    const icon = dryRun ? '🧪' : '✅';
    res.innerHTML = `${icon} ${dryRun?'Simulación':'Envío'} completado ·
      <strong>${r.enviados}</strong> enviados ·
      <strong>${r.errores}</strong> errores ·
      <strong>${r.sin_coincidencias??'—'}</strong> sin coincidencias
      ${r.detalles?.length?`<details style="margin-top:8px"><summary style="cursor:pointer;color:var(--accent)">Ver detalle (${r.detalles.length})</summary>
        <div style="margin-top:8px;display:grid;gap:4px">
          ${r.detalles.map(d=>`<div style="display:flex;justify-content:space-between;font-size:12px;padding:3px 0;border-bottom:1px solid var(--border)">
            <span>${esc(d.email)}</span>
            <span style="color:${d.resultado==='enviado'?'var(--green)':d.resultado==='error'?'var(--red)':'var(--muted)'}">
              ${esc(d.resultado)} · ${d.coincidencias} oferta${d.coincidencias!==1?'s':''}
            </span>
          </div>`).join('')}
        </div></details>`:''}`;
    if (!dryRun && r.enviados > 0) toast(`${r.enviados} alertas enviadas ✓`);
    else if (dryRun) toast(`Simulación: ${r.enviados} se enviarían`);
  } catch(e) {
    res.innerHTML=`<span style="color:var(--red)">Error: ${esc(e.message)}</span>`;
    toast('Error: '+e.message,'error');
  }
}

function abrirTestEmail() {
  const email = prompt('Email de prueba:');
  if (!email) return;
  testEmailSub(email);
}

async function testEmailSub(email) {
  if (!confirm(`Enviar email de prueba a ${email}?`)) return;
  try {
    const r = await api('/alertas/test-email', { method:'POST', body:JSON.stringify({email}) });
    if (r.ok) toast(`Email de prueba enviado a ${email} ✓`);
    else toast(`Error: ${r.error}`, 'error');
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function desactivarSub(id, email) {
  if (!confirm(`¿Desactivar suscripción de ${email}?`)) return;
  try {
    await api(`/suscripciones/${id}`, { method:'DELETE' });
    toast(`Suscripción de ${email} desactivada ✓`);
    loadAlertas();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

// ── Métricas de entrega (webhooks Resend) ─────────────────────
async function loadEventos() {
  const tbody = document.getElementById('email-eventos-tbody');
  const statsEl = document.getElementById('email-eventos-stats');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="4"><span class="spinner"></span></td></tr>`;
  try {
    const filtro = document.getElementById('ev-email-filtro').value.trim();
    const p = new URLSearchParams({ limit: '50' });
    if (filtro) p.set('email', filtro);
    const d = await api(`/alertas/eventos?${p}`);
    if (d.warning) {
      statsEl.innerHTML = '';
      tbody.innerHTML = `<tr class="empty-row"><td colspan="4" style="color:var(--yellow)">${esc(d.warning)}</td></tr>`;
      return;
    }
    const r = d.resumen || {};
    statsEl.innerHTML = `
      <div class="stat-card blue"><div class="label">Enviados</div><div class="value">${r.enviados??0}</div></div>
      <div class="stat-card green"><div class="label">Entregados</div><div class="value">${r.entregados??0}</div></div>
      <div class="stat-card red"><div class="label">Rebotes</div><div class="value">${r.rebotes??0}</div></div>
      <div class="stat-card"><div class="label">Aperturas</div><div class="value">${r.aperturas??0}</div></div>
      <div class="stat-card"><div class="label">Clics</div><div class="value">${r.clics??0}</div></div>
      <div class="stat-card yellow"><div class="label">Quejas</div><div class="value">${r.quejas??0}</div></div>
    `;
    const evs = d.eventos || [];
    if (!evs.length) {
      const hint = d.webhook_configurado
        ? 'Sin eventos aún'
        : 'Sin eventos — configura el webhook en Resend y la env var RESEND_WEBHOOK_SECRET';
      tbody.innerHTML = `<tr class="empty-row"><td colspan="4">${hint}</td></tr>`;
      return;
    }
    const evPill = { delivered:'green', bounced:'red', opened:'blue', clicked:'blue', complained:'yellow', sent:'gray' };
    tbody.innerHTML = evs.map(ev => {
      const tipo = (ev.evento||'').replace('email.','');
      return `<tr>
        <td class="text-small">${fmtDt(ev.ts)}</td>
        <td>${pill(tipo, evPill[tipo]||'gray')}</td>
        <td class="text-small">${esc(ev.email||'—')}</td>
        <td class="text-small text-muted" style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(ev.asunto)}">${trunc(ev.asunto,46)}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    statsEl.innerHTML = '';
    tbody.innerHTML = `<tr class="empty-row"><td colspan="4" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

// ── Override de clasificación de fuentes ──────────────────────
async function overrideFuente(id, nombre) {
  const valido = 'active, experimental, manual_review, js_required, blocked, broken, no_data, disabled';
  const status = prompt(
    `Override de status para "${nombre}" (ID ${id}).\nValores: ${valido}\nDeja vacío y acepta para QUITAR el override.`
  );
  if (status === null) return; // canceló
  try {
    if (!status.trim()) {
      const r = await api(`/fuentes/${id}/override`, { method:'DELETE' });
      toast(r.eliminado ? `Override de ${id} eliminado ✓` : `Fuente ${id} no tenía override`);
    } else {
      await api(`/fuentes/${id}/override`, {
        method:'PUT',
        body: JSON.stringify({ status: status.trim(), reason: `manual desde panel` }),
      });
      toast(`Override de ${id} → ${status.trim()} ✓`);
    }
    loadFuentes();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

// ── DIAGNÓSTICO ───────────────────────────────────────────────
async function loadDiagnostico() {
  const el = document.getElementById('diag-alertas');
  el.innerHTML = '<span class="spinner"></span>';
  try {
    const d = await api('/diagnostico');
    const nivel = d.nivel_global;
    const nivelColor = {ok:'var(--green)', warning:'var(--yellow)', error:'var(--red)'}[nivel]||'var(--muted)';
    const alertas = d.alertas||[];

    // Fila de estado global
    let html = `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px;display:flex;align-items:center;gap:12px">
      <span style="font-size:20px">${{ok:'✅',warning:'⚠️',error:'🚨'}[nivel]||'ℹ️'}</span>
      <span style="font-weight:600;color:${nivelColor}">Sistema ${nivel==='ok'?'en orden':`con ${alertas.length} alerta(s)`}</span>
    </div>`;

    // Acciones directas disponibles para algunas alertas (un clic resuelve).
    const accionDirecta = {
      'bulk-desactivar': { action: 'diag-desactivar-vencidas', label: '✓ Desactivar vencidas' },
      'revalidar-urls':  { action: 'diag-revalidar', label: '↻ Revalidar ahora' },
    };

    // Alertas individuales
    alertas.forEach(a => {
      const bg = {error:'#dc262218',warning:'#d9770618',info:'#1d4ed818'}[a.nivel]||'var(--surface)';
      const co = {error:'var(--red)',warning:'var(--yellow)',info:'#60a5fa'}[a.nivel]||'var(--muted)';
      const dir = accionDirecta[a.accion];
      html += `<div style="background:${bg};border:1px solid ${co}44;border-radius:var(--radius);padding:12px 16px;display:flex;align-items:center;justify-content:space-between;gap:12px">
        <span style="color:${co}">${esc(a.mensaje)}</span>
        <span style="display:flex;gap:6px;white-space:nowrap">
          ${dir?`<button class="btn btn-primary btn-sm" data-action="${dir.action}">${dir.label}</button>`:''}
          <button class="btn btn-ghost btn-sm" data-action="ir-a" data-target="${escAttr(a.accion)}">Ver →</button>
        </span>
      </div>`;
    });

    // Detalle numérico
    const o = d.ofertas||{};
    html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px">
      <div style="font-weight:600;margin-bottom:10px;font-size:13px">Detalles</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px;font-size:12px;color:var(--muted)">
        ${[
          ['Ofertas vencidas activas', o.vencidas_activas, 'var(--yellow)'],
          ['URLs oferta rotas', o.url_oferta_rota, 'var(--red)'],
          ['URLs bases rotas', o.url_bases_rota, 'var(--red)'],
          ['Pendientes revisión', o.needs_review, 'var(--yellow)'],
          ['Sin fecha de cierre', o.sin_fecha_cierre, null],
          ['Descripción corta', o.descripcion_corta, null],
          ['URLs sin chequear 48h', o.url_no_chequeada_48h, 'var(--yellow)'],
        ].map(([label,val,color])=>`
          <div style="display:flex;justify-content:space-between;border-bottom:1px solid var(--border);padding:3px 0">
            <span>${label}</span>
            <strong style="${color&&val>0?`color:${color}`:''}">${val??0}</strong>
          </div>
        `).join('')}
        ${d.horas_desde_ultima_corrida!=null?`
          <div style="display:flex;justify-content:space-between;border-bottom:1px solid var(--border);padding:3px 0">
            <span>Horas desde última corrida</span>
            <strong style="${d.horas_desde_ultima_corrida>24?'color:var(--yellow)':''}">${d.horas_desde_ultima_corrida}h</strong>
          </div>
        `:''}
      </div>
    </div>`;

    el.innerHTML = html;
  } catch(e) {
    el.innerHTML = `<div style="color:var(--red)">Error: ${esc(e.message)}</div>`;
  }
}

function verDestacadas() {
  _switchToTab('destacadas');
  loadDestacadas();
}

function irA(accion) {
  const tabMap = {
    'bulk-desactivar':'acciones','revalidar-urls':'acciones','revision':'revision',
    'scraper-runs':'scrapers','evaluaciones':'fuentes','alertas':'alertas',
    'nueva-oferta':'ofertas','export':'ofertas',
  };
  const tab = tabMap[accion]||'dashboard';
  _switchToTab(tab);
  if (tab==='revision') loadRevision();
  else if (tab==='scrapers') loadScrapers();
  else if (tab==='alertas') loadAlertasTab();
  else if (tab==='ofertas') {
    loadOfertas(1);
    if (accion==='nueva-oferta') setTimeout(openCrearOferta, 300);
    else if (accion==='export') setTimeout(exportOfertas, 300);
  }
}

// ── REVISIÓN ───────────────────────────────────────────────────
async function loadRevision() {
  const tipo = document.getElementById('rev-tipo').value;
  const tbody = document.getElementById('rev-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="7"><span class="spinner"></span></td></tr>`;
  document.getElementById('rev-resumen').innerHTML = '';
  try {
    const qs = new URLSearchParams({ limit: '100' });
    if (tipo) qs.set('tipo', tipo);
    const d = await api(`/revision?${qs}`);
    // Resumen de contadores
    const resumen = d.resumen||{};
    const etiquetas = {url_rota:'🔴 URL rota', sin_sector:'🏷️ Sin sector',
      calidad_baja:'⚠️ Calidad baja', sin_fecha:'📅 Sin fecha', texto_corto:'📄 Desc. corta',
      duplicado_posible:'👥 Duplicados'};
    document.getElementById('rev-resumen').innerHTML = Object.entries(resumen).map(([k,v])=>`
      <div class="stat-card ${v>0?'yellow':''}">
        <div class="label">${etiquetas[k]||k}</div>
        <div class="value">${v}</div>
      </div>`).join('');

    // Unir todos los items con su tipo
    const items = [];
    const cats = d.categorias||{};
    Object.entries(cats).forEach(([tipo_k, cat]) => {
      (cat.items||[]).forEach(item => items.push({...item, _tipo: tipo_k}));
    });

    if (!items.length) {
      tbody.innerHTML='<tr class="empty-row"><td colspan="7">✅ Sin ofertas pendientes de revisión</td></tr>';
      return;
    }

    const probPill = {
      url_rota: '<span class="pill red">URL rota</span>',
      sin_sector: '<span class="pill yellow">Sin sector</span>',
      calidad_baja: '<span class="pill yellow">Calidad baja</span>',
      sin_fecha: '<span class="pill gray">Sin fecha</span>',
      texto_corto: '<span class="pill gray">Desc. corta</span>',
      duplicado_posible: '<span class="pill orange">Duplicado</span>',
    };

    tbody.innerHTML = items.map(item => {
      _itemCache[item.id] = {..._itemCache[item.id], ...item};
      let detalle = '—';
      if (item._tipo==='url_rota') detalle = `of:${item.url_oferta_valida===false?'❌':'✓'} base:${item.url_bases_valida===false?'❌':'✓'}`;
      else if (item._tipo==='calidad_baja') detalle = item.overall_quality_score!=null?`score: ${(item.overall_quality_score*100).toFixed(0)}%`:'needs_review';
      else if (item._tipo==='texto_corto') detalle = `${item.desc_len||0} chars`;
      else if (item._tipo==='sin_sector') detalle = esc(item.sector||'(vacío)');
      else if (item._tipo==='duplicado_posible') detalle = `grupo #${item.dup_grupo} · ${item.copias} copias`;
      return `<tr>
        <td class="text-muted text-small">${item.id}</td>
        <td style="max-width:220px">${trunc(item.cargo,34)}</td>
        <td class="text-small text-muted">${trunc(item.institucion_nombre,28)}</td>
        <td>${probPill[item._tipo]||item._tipo}</td>
        <td class="text-small text-muted">${detalle}</td>
        <td class="text-small">${item.fecha_cierre?fmtDate(item.fecha_cierre):'<span class="text-muted">—</span>'}</td>
        <td style="white-space:nowrap">
          <button class="btn btn-ghost btn-sm" data-action="open-edit" data-id="${item.id}">✏️ Editar</button>
          <button class="btn btn-success btn-sm" style="margin-left:4px" data-action="marcar-revisada" data-id="${item.id}">✓ OK</button>
          <button class="btn btn-danger btn-sm" style="margin-left:4px" data-action="toggle-activa" data-id="${item.id}">🗑️</button>
          ${item.url_oferta?`<a href="${escAttr(item.url_oferta)}" target="_blank" rel="noopener" class="btn btn-ghost btn-sm" style="margin-left:4px">🔗</a>`:''}
        </td>
      </tr>`;
    }).join('');
  } catch(e) {
    tbody.innerHTML=`<tr class="empty-row"><td colspan="7" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

async function marcarRevisada(id) {
  try {
    await api(`/revision/${id}/marcar-revisada`, { method:'POST', body:'{}' });
    toast(`Oferta ${id} marcada como revisada ✓`);
    loadRevision();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

// ── FUENTES (edit/create/delete) ───────────────────────────────
function openCrearFuente() {
  _fuenteEditId = null;
  document.getElementById('fuente-modal-title').textContent = 'Nueva institución';
  document.getElementById('fm-save-btn').textContent = 'Crear';
  ['fm-nombre','fm-sigla','fm-sector','fm-region','fm-plataforma','fm-url','fm-notas'].forEach(id => {
    document.getElementById(id).value = '';
  });
  document.getElementById('fuente-modal').classList.add('open');
}

async function openEditarFuente(id) {
  _fuenteEditId = id;
  document.getElementById('fuente-modal-title').textContent = `Editar institución #${id}`;
  document.getElementById('fm-save-btn').textContent = 'Guardar';
  try {
    const d = await api(`/fuentes/${id}`);
    const i = d.institucion||{};
    document.getElementById('fm-nombre').value    = i.nombre||'';
    document.getElementById('fm-sigla').value     = i.sigla||'';
    document.getElementById('fm-sector').value    = i.sector||'';
    document.getElementById('fm-region').value    = i.region||'';
    document.getElementById('fm-plataforma').value= i.plataforma_empleo||'';
    document.getElementById('fm-url').value       = i.url_empleo||'';
    document.getElementById('fm-notas').value     = i.notas_admin||'';
    document.getElementById('fuente-modal').classList.add('open');
  } catch(e) { toast('Error: '+e.message,'error'); }
}

function closeFuenteModal() {
  document.getElementById('fuente-modal').classList.remove('open');
  _fuenteEditId = null;
}

async function saveFuente() {
  const payload = {
    nombre:            document.getElementById('fm-nombre').value,
    sigla:             document.getElementById('fm-sigla').value||null,
    sector:            document.getElementById('fm-sector').value||null,
    region:            document.getElementById('fm-region').value||null,
    plataforma_empleo: document.getElementById('fm-plataforma').value||null,
    url_empleo:        document.getElementById('fm-url').value||null,
    notas_admin:       document.getElementById('fm-notas').value||null,
  };
  try {
    if (_fuenteEditId) {
      await api(`/fuentes/${_fuenteEditId}`, { method:'PUT', body:JSON.stringify(payload) });
      toast('Institución actualizada ✓');
    } else {
      const r = await api('/fuentes', { method:'POST', body:JSON.stringify(payload) });
      toast(`Institución creada — ID ${r.id} ✓`);
    }
    closeFuenteModal();
    loadFuentes();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function desactivarFuente(id, nombre) {
  if (!confirm(`¿Desactivar "${nombre}" (ID ${id})?\nSe cerrarán todas sus ofertas activas y se añadirá a la lista de fuentes deshabilitadas.`)) return;
  try {
    const r = await api(`/fuentes/${id}`, { method:'DELETE' });
    toast(`Fuente ${id} desactivada · ${r.ofertas_cerradas} ofertas cerradas ✓`);
    loadFuentes();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

document.getElementById('fuente-modal').addEventListener('click', e => { if(e.target===e.currentTarget) closeFuenteModal(); });

// ── ACCIONES ──────────────────────────────────────────────────
document.getElementById('run-mode').addEventListener('change', function() {
  const v = this.value;
  document.getElementById('run-kind').style.display    = v==='kind'       ? '' : 'none';
  document.getElementById('run-inst-id').style.display = v==='institucion'? '' : 'none';
});

async function runScraper() {
  const mode    = document.getElementById('run-mode').value;
  const kind    = document.getElementById('run-kind').value;
  const instId  = document.getElementById('run-inst-id').value;
  const _lim    = parseInt(document.getElementById('run-limite').value);
  const dryRun  = document.getElementById('run-dry').checked;
  const res     = document.getElementById('run-result');

  const payload = { mode, dry_run: dryRun };
  if (Number.isFinite(_lim) && _lim > 0) payload.limite_fuentes = _lim;
  if (mode==='kind')        payload.kind = kind;
  if (mode==='institucion') payload.institucion_id = parseInt(instId);
  if (mode==='all' && !dryRun) {
    if (!confirm('¿Lanzar corrida completa de todos los scrapers activos? Puede tardar varios minutos.')) return;
  }

  res.style.display='block';
  res.innerHTML='<span class="spinner"></span> Lanzando…';
  try {
    const r = await api('/scraper/run', { method:'POST', body:JSON.stringify(payload) });
    res.innerHTML = `✅ Proceso lanzado — PID: <strong>${r.pid}</strong> · run_id: ${r.run_id??'—'} · dry_run: ${r.dry_run} · cmd: <code>${esc(r.cmd.join(' '))}</code>`;
    toast('Scraper iniciado ✓');
    loadProcesos();
    if (r.log) verLog(r.log);
  } catch(e) {
    res.innerHTML = `<span style="color:var(--red)">Error: ${esc(e.message)}</span>`;
    toast('Error: '+e.message, 'error');
  }
}

// ── Procesos lanzados (seguimiento en vivo) ───────────────────
function _tabAccionesActivo() {
  return document.getElementById('tab-acciones').classList.contains('active');
}

async function loadProcesos() {
  const tbody = document.getElementById('procesos-tbody');
  clearTimeout(_procPollTimer);
  try {
    const d = await api('/procesos?limit=15');
    const ps = d.procesos || [];
    if (!ps.length) {
      tbody.innerHTML = '<tr class="empty-row"><td colspan="6">Sin procesos lanzados desde el panel</td></tr>';
    } else {
      const ep = { en_curso:'blue', completado:'green', error:'red', finalizado:'gray' };
      tbody.innerHTML = ps.map(p => `<tr>
        <td class="text-small">${fmtDt(p.started_at)}</td>
        <td><span class="pill gray">${p.tipo}</span></td>
        <td class="text-muted text-small">${p.pid}</td>
        <td>${pill(p.estado, ep[p.estado]||'gray')}${p.returncode!=null&&p.returncode!==0?` <span class="text-small" style="color:var(--red)">rc=${p.returncode}</span>`:''}</td>
        <td class="text-small text-muted" style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(p.cmd)}">${esc(p.cmd)}</td>
        <td><button class="btn btn-ghost btn-sm" data-action="ver-log" data-log="${escAttr(p.log)}">📜 Ver</button></td>
      </tr>`).join('');
    }
    // Sigue refrescando mientras haya procesos en curso y el tab esté visible
    if (ps.some(p => p.estado === 'en_curso') && _tabAccionesActivo()) {
      _procPollTimer = setTimeout(loadProcesos, 4000);
    }
  } catch(e) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="6" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

function verLog(archivo) {
  _logArchivo = archivo;
  document.getElementById('proceso-log-wrap').style.display = 'block';
  document.getElementById('proceso-log').textContent = 'Cargando…';
  refreshLog();
}

function cerrarLog() {
  _logArchivo = null;
  clearTimeout(_logPollTimer);
  document.getElementById('proceso-log-wrap').style.display = 'none';
}

async function refreshLog() {
  if (!_logArchivo) return;
  clearTimeout(_logPollTimer);
  const pre = document.getElementById('proceso-log');
  try {
    const d = await api(`/procesos/log?archivo=${encodeURIComponent(_logArchivo)}&tail=300`);
    if (!_logArchivo) return; // cerrado mientras cargaba
    document.getElementById('proceso-log-title').textContent =
      `${d.archivo} · ${d.estado}${d.returncode!=null?` (rc=${d.returncode})`:''} · ${d.total_lineas} líneas (últimas 300)`;
    pre.textContent = d.lineas.join('\n') || '(log vacío)';
    pre.scrollTop = pre.scrollHeight;
    if (d.estado === 'en_curso' && _tabAccionesActivo()) {
      _logPollTimer = setTimeout(refreshLog, 4000);
    }
  } catch(e) {
    pre.textContent = 'Error: ' + e.message;
  }
}

async function bulkDesactivar(tipo) {
  const conf = tipo==='url_rota'
    ? '¿Desactivar todas las ofertas con URL rota?'
    : '¿Desactivar todas las ofertas con fecha de cierre vencida?';
  if (!confirm(conf)) return;
  try {
    const r = await api('/ofertas/bulk-desactivar', {
      method:'POST',
      body: JSON.stringify({ [tipo]: true }),
    });
    toast(`${r.desactivadas} ofertas desactivadas ✓`);
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function revalidarUrls() {
  if (!confirm('¿Relanzar validación de URLs en background?')) return;
  try {
    const r = await api('/urls/revalidar', { method:'POST', body: JSON.stringify({ workers:20, max_edad_h:0, limit:2000 }) });
    toast(`Revalidación iniciada — PID ${r.pid} ✓`);
    loadProcesos();
    if (r.log) verLog(r.log);
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function limpiarNoLaborales() {
  if (!confirm('¿Desactivar las ofertas activas que el filtro actual clasifica como no laborales (noticias municipales, actas, licitaciones)?')) return;
  try {
    const r = await api('/ofertas/limpiar-no-laborales', { method:'POST', body: JSON.stringify({ apply:true }) });
    toast(`Limpieza iniciada — PID ${r.pid} ✓`);
    loadProcesos();
    if (r.log) verLog(r.log);
  } catch(e) { toast('Error: '+e.message,'error'); }
}

async function reindexarMeili() {
  var res = document.getElementById('meili-result');
  if (!confirm('¿Reconstruir el índice de búsqueda completo?')) return;
  if (res) { res.style.display = 'block'; res.innerHTML = '<span class="spinner"></span> Reindexando…'; }
  try {
    var r = await api('/meilisearch/reindexar', { method: 'POST' });
    var msg = '✅ Reindexación completada';
    if (r.indexados != null) msg += ' — ' + r.indexados + ' ofertas indexadas';
    if (res) res.innerHTML = msg;
    toast('Índice de búsqueda actualizado ✓');
  } catch(e) {
    if (res) res.innerHTML = '<span style="color:var(--red)">Error: ' + esc(e.message) + '</span>';
    toast('Error: ' + e.message, 'error');
  }
}

async function loadCatalog() {
  const tbody = document.getElementById('catalog-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="6"><span class="spinner"></span> Cargando catálogo…</td></tr>`;
  try {
    _catalogData = await api('/scraper/catalog');
    renderCatalog();
  } catch(e) {
    tbody.innerHTML=`<tr class="empty-row"><td colspan="6" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

function renderCatalog() {
  const tbody = document.getElementById('catalog-tbody');
  const countEl = document.getElementById('catalog-count');
  const search = (document.getElementById('catalog-search')?.value || '').trim().toLowerCase();
  const statusF = (document.getElementById('catalog-status-filter')?.value || '');
  const kindF = (document.getElementById('catalog-kind-filter')?.value || '');
  let items = _catalogData;
  if (!items.length) { tbody.innerHTML='<tr class="empty-row"><td colspan="6">Sin datos</td></tr>'; return; }
  if (search) items = items.filter(i => (i.nombre||'').toLowerCase().includes(search) || (i.sector||'').toLowerCase().includes(search));
  if (statusF) items = items.filter(i => i.status === statusF);
  if (kindF) items = items.filter(i => i.kind === kindF);
  if (countEl) countEl.textContent = items.length + ' de ' + _catalogData.length;
  if (!items.length) { tbody.innerHTML='<tr class="empty-row"><td colspan="6">Sin resultados con estos filtros</td></tr>'; return; }
  const km = {wordpress:'blue',generic:'gray',custom_trabajando:'orange',custom_hiringroom:'orange',
              custom_buk:'orange',empleos_publicos:'blue',custom_playwright:'yellow',skip:'gray'};
  const sm = {active:'green',experimental:'yellow',blocked:'red',broken:'red',
              disabled:'gray',manual_review:'orange',no_data:'gray',js_required:'yellow'};
  tbody.innerHTML = items.map(i=>`<tr>
    <td class="text-muted text-small">${i.id??'—'}</td>
    <td style="max-width:240px">${trunc(i.nombre||'—',36)}</td>
    <td class="text-small text-muted">${trunc(i.sector||'—',22)}</td>
    <td>${i.kind?`<span class="pill ${km[i.kind]||'gray'}">${i.kind}</span>`:'—'}</td>
    <td>${i.status?`<span class="pill ${sm[i.status]||'gray'}">${i.status}</span>`:'—'}</td>
    <td>${i.id && i.kind && i.kind!=='skip'
      ?`<button class="btn btn-ghost btn-sm" data-action="run-instancia" data-id="${i.id}" data-nombre="${escAttr((i.nombre||'').slice(0,20))}">▶ Run</button>`
      :'—'
    }</td>
  </tr>`).join('');
}

async function runInstancia(id, nombre) {
  if (!confirm(`¿Ejecutar scraper para "${nombre}" (ID ${id})?`)) return;
  const res = document.getElementById('run-result');
  res.style.display='block';
  res.innerHTML='<span class="spinner"></span> Lanzando…';
  // Cambiar al tab acciones si no está ahí
  _switchToTab('acciones');
  try {
    const r = await api('/scraper/run', {
      method:'POST',
      body: JSON.stringify({ mode:'institucion', institucion_id:id }),
    });
    res.innerHTML=`✅ PID: <strong>${r.pid}</strong> · run_id: ${r.run_id??'—'} · inst: ${id}`;
    toast(`Scraper inst. ${id} iniciado ✓`);
    loadProcesos();
    if (r.log) verLog(r.log);
  } catch(e) {
    res.innerHTML=`<span style="color:var(--red)">Error: ${esc(e.message)}</span>`;
    toast('Error: '+e.message,'error');
  }
}

// ── CONFIG ─────────────────────────────────────────────────────
// ── Editor de criterios automáticos de Destacadas ─────────────────
let _criteriosCatalogo = [];
function _catEntry(tipo){ return _criteriosCatalogo.find(c => c.tipo === tipo) || null; }
function renderCriterioRow(crit) {
  crit = crit || {};
  const row = document.createElement('div');
  row.className = 'criterio-row';
  row.style.cssText = 'display:flex;gap:8px;align-items:center;margin-bottom:6px';
  const sel = document.createElement('select');
  sel.className = 'crit-tipo';
  sel.innerHTML = _criteriosCatalogo.map(c => `<option value="${esc(c.tipo)}">${esc(c.label)}</option>`).join('');
  if (crit.tipo) sel.value = crit.tipo;
  const val = document.createElement('input');
  val.className = 'crit-valor'; val.style.maxWidth = '160px';
  if (crit.valor != null) val.value = crit.valor;
  const del = document.createElement('button');
  del.type = 'button'; del.className = 'btn btn-ghost btn-sm';
  del.textContent = '✕'; del.title = 'Eliminar criterio';
  del.setAttribute('data-action', 'del-criterio-destacada');
  function syncValor() {
    const e = _catEntry(sel.value);
    const needs = e && e.valor;               // 'numero' | 'texto' | null
    val.style.display = needs ? '' : 'none';
    val.type = needs === 'numero' ? 'number' : 'text';
    val.placeholder = needs === 'numero' ? 'valor' : 'texto';
  }
  sel.addEventListener('change', syncValor);
  syncValor();
  row.appendChild(sel); row.appendChild(val); row.appendChild(del);
  return row;
}
function addCriterioRow(crit) {
  const cont = document.getElementById('cfg-destacadas-criterios');
  if (cont) cont.appendChild(renderCriterioRow(crit));
}

async function loadConfig() {
  try {
    const d = await api('/config');
    // Env info
    const env = d.env||{};
    document.getElementById('env-info').innerHTML = Object.entries(env).map(([k,v])=>
      `<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--border)">
        <span>${k}</span>
        <span style="color:${v===true?'var(--green)':v===false?'var(--red)':'var(--text)'}">${
          v===true?'✓ configurada':v===false?'✗ no configurada':v
        }</span>
       </div>`
    ).join('');
    // Config values
    const c = d.config||{};
    document.getElementById('cfg-banner-mensaje').value  = c.banner_mensaje||'';
    document.getElementById('cfg-banner-activo').checked = c.banner_activo==='true';
    document.getElementById('cfg-mantenimiento').checked = c.mantenimiento==='true';
    document.getElementById('cfg-max-pagina').value      = c.max_resultados_pagina||50;
    // Por defecto OFF: las alertas se muestran como "Próximamente" en el
    // sitio público hasta que aquí se activen explícitamente.
    document.getElementById('cfg-alertas').checked       = c.alertas_activas==='true';
    document.getElementById('cfg-footer-extra').value    = c.footer_extra||'';
    // AdSense (gestionable desde aquí)
    const _ae=document.getElementById('cfg-ads-enabled'); if(_ae) _ae.checked = (c.ads_enabled==='1'||c.ads_enabled==='true');
    const _ac=document.getElementById('cfg-ads-client'); if(_ac) _ac.value = c.ads_client||'';
    const _ar=document.getElementById('cfg-ads-resultados'); if(_ar) _ar.value = c.ads_slot_resultados||'';
    const _as=document.getElementById('cfg-ads-sidebar'); if(_as) _as.value = c.ads_slot_sidebar||'';
    const _aco=document.getElementById('cfg-ads-contenido'); if(_aco) _aco.value = c.ads_slot_contenido||'';
    // Criterios automáticos de destacadas: toggle + modo + lista editable
    const _da=document.getElementById('cfg-destacadas-auto'); if(_da) _da.checked = (c.destacadas_auto==='1'||c.destacadas_auto==='true');
    _criteriosCatalogo = d.criterios_catalogo || [];
    const _dm=document.getElementById('cfg-destacadas-modo'); if(_dm) _dm.value = (c.destacadas_criterios_modo==='all')?'all':'any';
    const _cc=document.getElementById('cfg-destacadas-criterios');
    if(_cc){
      _cc.innerHTML='';
      let list=[]; try{ list=JSON.parse(c.destacadas_criterios||'[]'); }catch(_){ list=[]; }
      if(Array.isArray(list)) list.forEach(x=>addCriterioRow(x));
    }
    // Recuadro "Anúnciate" en la página de Cursos: visible por defecto, se
    // apaga solo si el admin lo puso explícitamente en 'false'.
    const _ca=document.getElementById('cfg-cursos-anunciate'); if(_ca) _ca.checked = (c.cursos_anunciate_activo!=='false');
  } catch(e) {
    document.getElementById('env-info').textContent = 'Error: '+e.message;
  }
  loadAudit();
}

async function loadAudit() {
  const tbody = document.getElementById('audit-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="5"><span class="spinner"></span></td></tr>`;
  try {
    const accion = document.getElementById('audit-accion').value;
    const p = new URLSearchParams({ limit: '100' });
    if (accion) p.set('accion', accion);
    const d = await api(`/audit?${p}`);
    if (d.warning) {
      tbody.innerHTML = `<tr class="empty-row"><td colspan="5" style="color:var(--yellow)">${esc(d.warning)}</td></tr>`;
      return;
    }
    const items = d.items||[];
    if (!items.length) {
      tbody.innerHTML = '<tr class="empty-row"><td colspan="5">Sin acciones registradas</td></tr>';
      return;
    }
    tbody.innerHTML = items.map(a => {
      let det = '—';
      if (a.detalle) {
        const obj = typeof a.detalle === 'string' ? JSON.parse(a.detalle) : a.detalle;
        det = Object.entries(obj).map(([k,v]) => `${k}=${Array.isArray(v)?v.join(','):v}`).join(' · ');
      }
      return `<tr>
        <td class="text-small">${fmtDt(a.ts)}</td>
        <td class="text-small text-muted">${a.usuario}</td>
        <td><span class="pill blue">${a.accion}</span></td>
        <td class="text-small text-muted">${a.entidad?`${a.entidad}${a.entidad_id?` #${a.entidad_id}`:''}`:'—'}</td>
        <td class="text-small text-muted" style="max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(det)}">${esc(det)}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="5" style="color:var(--red)">Error: ${esc(e.message)}</td></tr>`;
  }
}

async function saveConfig() {
  const payload = {
    banner_mensaje:       document.getElementById('cfg-banner-mensaje').value,
    banner_activo:        document.getElementById('cfg-banner-activo').checked ? 'true':'false',
    mantenimiento:        document.getElementById('cfg-mantenimiento').checked ? 'true':'false',
    max_resultados_pagina: document.getElementById('cfg-max-pagina').value,
    alertas_activas:      document.getElementById('cfg-alertas').checked ? 'true':'false',
    footer_extra:         document.getElementById('cfg-footer-extra').value,
  };
  const _gv = id => { const e=document.getElementById(id); return e?e.value.trim():''; };
  const _ae = document.getElementById('cfg-ads-enabled');
  if (_ae) {
    payload.ads_enabled = _ae.checked ? '1':'0';
    payload.ads_client = _gv('cfg-ads-client');
    payload.ads_slot_resultados = _gv('cfg-ads-resultados');
    payload.ads_slot_sidebar = _gv('cfg-ads-sidebar');
    payload.ads_slot_contenido = _gv('cfg-ads-contenido');
  }
  // Criterios automáticos de destacadas: toggle + modo + lista JSON
  const _da = document.getElementById('cfg-destacadas-auto');
  if (_da) payload.destacadas_auto = _da.checked ? 'true':'false';
  const _dm = document.getElementById('cfg-destacadas-modo');
  if (_dm) payload.destacadas_criterios_modo = (_dm.value === 'all') ? 'all':'any';
  const _cc = document.getElementById('cfg-destacadas-criterios');
  if (_cc) {
    const crits = [...(_cc.querySelectorAll('.criterio-row'))].map(r => {
      const tipo = r.querySelector('.crit-tipo').value;
      const entry = _criteriosCatalogo.find(c => c.tipo === tipo);
      const o = { tipo };
      if (entry && entry.valor) o.valor = r.querySelector('.crit-valor').value.trim();
      return o;
    }).filter(o => o.tipo);
    payload.destacadas_criterios = JSON.stringify(crits);
  }
  // Recuadro "Anúnciate" en la página de Cursos
  const _ca = document.getElementById('cfg-cursos-anunciate');
  if (_ca) payload.cursos_anunciate_activo = _ca.checked ? 'true':'false';
  try {
    const r = await api('/config', { method:'PUT', body:JSON.stringify(payload) });
    toast(`Config guardada: ${r.updated.join(', ')} ✓`);
  } catch(e) { toast('Error: '+e.message,'error'); }
}

// ── Delegación de eventos (reemplaza onclick/onchange inline) ──
document.addEventListener('click', e => {
  const el = e.target.closest('[data-action]');
  if (!el || el.disabled) return;
  const d = el.dataset;
  switch (d.action) {
    case 'logout':             logout(); break;
    case 'load-dashboard':     loadDashboard(); break;
    case 'load-analitica':     loadAnalitica(); break;
    case 'export-analitica':   exportAnalitica(); break;
    case 'load-diagnostico':   loadDiagnostico(); break;
    case 'export-ofertas':     exportOfertas(); break;
    case 'load-scrapers':      loadScrapers(); break;
    case 'load-revision':      loadRevision(); break;
    case 'load-alertas':       loadAlertas(); break;
    case 'abrir-test-email':   abrirTestEmail(); break;
    case 'enviar-alertas':     enviarAlertas(); break;
    case 'export-subs':        e.preventDefault(); exportSubs(); break;
    case 'run-scraper':        runScraper(); break;
    case 'bulk-desactivar':    bulkDesactivar(d.tipo); break;
    case 'revalidar-urls':     revalidarUrls(); break;
    case 'limpiar-no-laborales': limpiarNoLaborales(); break;
    case 'reindexar-meili':    reindexarMeili(); break;
    case 'load-catalog':       loadCatalog(); break;
    case 'load-procesos':      loadProcesos(); break;
    case 'cerrar-log':         cerrarLog(); break;
    case 'load-config':        loadConfig(); break;
    case 'save-config':        saveConfig(); break;
    case 'add-criterio-destacada': addCriterioRow({}); break;
    case 'del-criterio-destacada': { const _r=el.closest('.criterio-row'); if(_r) _r.remove(); break; }
    case 'load-audit':         loadAudit(); break;
    case 'crear-fuente':       openCrearFuente(); break;
    case 'nueva-oferta':       openCrearOferta(); break;
    case 'load-eventos':       loadEventos(); break;
    case 'bulk-sel-desactivar':      bulkSeleccionadas('desactivar'); break;
    case 'bulk-sel-revisadas':       bulkSeleccionadas('revisadas'); break;
    case 'bulk-sel-destacar':        bulkSeleccionadas('destacar'); break;
    case 'bulk-sel-quitar-destacada': bulkSeleccionadas('quitar-destacada'); break;
    case 'bulk-sel-limpiar':         _limpiarSeleccion(); break;
    case 'close-modal':        closeModal(); break;
    case 'save-edit':          saveEdit(); break;
    case 'close-fuente-modal': closeFuenteModal(); break;
    case 'save-fuente':        saveFuente(); break;
    // Acciones por fila (data-id / data-* del elemento)
    case 'pagina':             loadOfertas(parseInt(d.page)); break;
    case 'toggle-activa':      toggleActiva(parseInt(d.id)); break;
    case 'toggle-destacada':   toggleDestacada(parseInt(d.id)); break;
    case 'open-edit':          openEdit(parseInt(d.id)); break;
    case 'marcar-revisada':    marcarRevisada(parseInt(d.id)); break;
    case 'editar-fuente':      openEditarFuente(parseInt(d.id)); break;
    case 'override-fuente':    overrideFuente(parseInt(d.id), d.nombre||''); break;
    case 'desactivar-fuente':  desactivarFuente(parseInt(d.id), d.nombre||''); break;
    case 'test-email-sub':     testEmailSub(d.email); break;
    case 'desactivar-sub':     desactivarSub(parseInt(d.id), d.email); break;
    case 'run-instancia':      runInstancia(parseInt(d.id), d.nombre||''); break;
    case 'ver-log':            verLog(d.log); break;
    case 'ver-destacadas':     verDestacadas(); break;
    case 'load-destacadas':    loadDestacadas(); break;
    case 'dest-search':        destSearch(); break;
    case 'dest-agregar':       destAgregar(parseInt(d.id)); break;
    case 'dest-quitar':        destQuitar(parseInt(d.id)); break;
    case 'ir-a':               irA(d.target); break;
    case 'diag-desactivar-vencidas': diagDesactivarVencidas(); break;
    case 'diag-revalidar':     revalidarUrls(); break;
    case 'load-usuarios':      loadUsuarios(); break;
    case 'save-usuario':       saveUsuario(); break;
    case 'cancelar-usuario':   cancelarUsuario(); break;
    case 'editar-usuario':     editarUsuario(d); break;
    case 'borrar-usuario':     borrarUsuario(parseInt(d.id), d.usuario||''); break;
    case 'load-scheduler':     loadScheduler(); break;
    case 'save-scheduler':     saveScheduler(); break;
  }
});

async function diagDesactivarVencidas() {
  if (!confirm('¿Desactivar todas las ofertas activas con fecha de cierre ya vencida?')) return;
  try {
    const r = await api('/ofertas/bulk-desactivar', { method:'POST', body: JSON.stringify({ fecha_cierre_vencida: true }) });
    toast(`${r.desactivadas} ofertas vencidas desactivadas ✓`);
    loadDashboard();
  } catch(e) { toast('Error: '+e.message,'error'); }
}

document.addEventListener('change', e => {
  // Selección múltiple de ofertas (checkboxes sin data-change)
  if (e.target.id === 'sel-all-ofertas') {
    document.querySelectorAll('.sel-oferta').forEach(c => { c.checked = e.target.checked; });
    _actualizarBulkBar();
    return;
  }
  if (e.target.classList && e.target.classList.contains('sel-oferta')) {
    _actualizarBulkBar();
    return;
  }
  const el = e.target.closest('[data-change]');
  if (!el) return;
  switch (el.dataset.change) {
    case 'ofertas':  loadOfertas(1); break;
    case 'analitica': loadAnalitica(); break;
    case 'scrapers': loadScrapers(); break;
    case 'revision': loadRevision(); break;
    case 'alertas':  loadAlertas(); break;
    case 'alertas-toggle': _toggleAlertasMaster(); break;
    case 'fuentes':  loadFuentes(); break;
    case 'audit':    loadAudit(); break;
    case 'eventos':  loadEventos(); break;
    case 'catalog-filter': if (_catalogData.length) renderCatalog(); break;
  }
});

document.addEventListener('input', e => {
  if (e.target.matches('[data-input="ofertas-q"]')) debounceSearch();
  if (e.target.matches('[data-input="catalog-filter"]') && _catalogData.length) renderCatalog();
});

// ── Keyboard ───────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.key==='Escape') closeModal();
  if (e.key==='Enter' && document.getElementById('auth-screen').style.display!=='none') doLogin();
  if (e.key==='Enter' && e.target.id==='dest-buscar') destSearch();
});
document.getElementById('auth-btn').addEventListener('click', doLogin);

// ── Auto-login ─────────────────────────────────────────────────
// Restaura la sesión desde sessionStorage si el token sigue vigente
// (con margen de 30s) y el ADMIN_PATH del URL coincide con el guardado.
(async function() {
  const saved = sessionStorage.getItem('_gc');
  if (!saved) return;
  try {
    const { token, expiresAt, k, rol } = JSON.parse(saved);
    if (k !== ADMIN_PATH || !token || !_tokenSigueVivo(expiresAt)) {
      sessionStorage.removeItem('_gc');
      return;
    }
    _creds = { header: buildAuthHeaderFromToken(token), token, expiresAt };
    _rol = rol || 'admin';
    // Validación rápida contra el servidor: si el token fue revocado o
    // el secreto rotó, /auth/me devuelve 401 y limpiamos.
    try {
      const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}/auth/me`, {
        headers: { Authorization: _creds.header },
      });
      if (!r.ok) { sessionStorage.removeItem('_gc'); _creds = null; return; }
      const me = await r.json().catch(() => ({}));
      if (me.rol) _rol = me.rol;   // fuente de verdad: el rol firmado del token
    } catch (_) { /* sin red: intentamos usar el token igual */ }
    showApp();
  } catch(e) { sessionStorage.removeItem('_gc'); }
})();
