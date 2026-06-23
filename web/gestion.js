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
let _searchTimer = null;
let _ofertasPagina = 1;
let _editingId = null;
let _fuenteEditId = null;
let _procPollTimer = null;
let _logPollTimer = null;
let _logArchivo = null;
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

function _saveSession(token, expiresAt) {
  _creds = { header: buildAuthHeaderFromToken(token), token, expiresAt };
  sessionStorage.setItem('_gc', JSON.stringify({
    token, expiresAt, k: ADMIN_PATH,
  }));
}

function _tokenSigueVivo(expiresAt) {
  // expiresAt viene del backend en segundos UNIX.
  return typeof expiresAt === 'number' && (expiresAt - 30) > (Date.now() / 1000);
}

async function doLogin() {
  const pass = document.getElementById('auth-pass').value;
  if (!pass) return;
  const btn = document.getElementById('auth-btn');
  btn.textContent = 'Verificando…'; btn.disabled = true;
  document.getElementById('auth-error').style.display = 'none';
  try {
    const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: pass }),
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
    _saveSession(body.token, body.expires_at);
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
  document.getElementById('app').style.display = 'none';
  document.getElementById('auth-screen').style.display = 'flex';
  document.getElementById('auth-pass').value = '';
  document.getElementById('auth-btn').textContent = 'Entrar';
  document.getElementById('auth-btn').disabled = false;
}

function showApp() {
  document.getElementById('auth-screen').style.display = 'none';
  document.getElementById('app').style.display = 'block';
  loadDashboard();
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

// ── Tabs ───────────────────────────────────────────────────────
document.querySelectorAll('nav button').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + tab).classList.add('active');
    if (tab === 'ofertas')  loadOfertas(1);
    else if (tab === 'scrapers') loadScrapers();
    else if (tab === 'fuentes')  loadFuentes();
    else if (tab === 'revision') loadRevision();
    else if (tab === 'alertas')  { loadAlertas(); loadEventos(); }
    else if (tab === 'config')   loadConfig();
    else if (tab === 'acciones') loadProcesos();
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
  _cfEl('cf-url').value = v.url || '';
  _cfEl('cf-descripcion').value = v.descripcion || '';
  _cfEl('cf-orden').value = (v.orden != null ? v.orden : '');
  _cfEl('cf-gratuito').checked = v.gratuito !== false;
  _cfEl('cf-activo').checked = v.activo !== false;
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
    url: _cfEl('cf-url').value.trim(),
    descripcion: _cfEl('cf-descripcion').value.trim(),
    orden: parseInt(_cfEl('cf-orden').value, 10) || 100,
    gratuito: _cfEl('cf-gratuito').checked,
    activo: _cfEl('cf-activo').checked,
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
  }
  const sel = _cfEl('cf-categoria');
  if (sel && !sel.options.length) {
    sel.innerHTML = _CURSO_CATS.map(c => `<option value="${c[0]}">${c[1]}</option>`).join('');
  }
  const cont = document.getElementById('cursos-tabla');
  cont.innerHTML = '<p class="text-muted">Cargando…</p>';
  try {
    const d = await api('/cursos');
    const cursos = d.cursos || [];
    if (!cursos.length) { cont.innerHTML = '<p class="text-muted">Sin cursos. Agregá el primero arriba.</p>'; return; }
    cont.innerHTML = '<table class="data-table" style="width:100%;border-collapse:collapse"><thead><tr>'
      + '<th style="text-align:left;padding:6px">Orden</th><th style="text-align:left;padding:6px">Título</th>'
      + '<th style="text-align:left;padding:6px">Proveedor</th><th style="text-align:left;padding:6px">Categoría</th>'
      + '<th style="text-align:left;padding:6px">Estado</th><th></th></tr></thead><tbody>'
      + cursos.map(c => '<tr style="border-top:1px solid var(--borde,#e5e5e5)">'
        + '<td style="padding:6px">' + (c.orden != null ? c.orden : '') + '</td>'
        + '<td style="padding:6px">' + _escCurso(c.titulo) + (c.gratuito ? ' <span class="pill green">Gratis</span>' : '') + '</td>'
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

// ── Utils ──────────────────────────────────────────────────────
function fmtDate(v) {
  if (!v) return '<span class="text-muted">—</span>';
  const d = new Date(v);
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
  return s.length > n ? s.slice(0,n) + '…' : s;
}
// Escapa un valor para insertarlo en un atributo HTML de las filas
// generadas (data-nombre, data-email, title=, …).
function escAttr(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
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
  loadDiagnostico();
}

function renderStatCards(d) {
  const t = d.totales||{};
  document.getElementById('stats-grid').innerHTML = `
    <div class="stat-card blue"><div class="label">Ofertas activas</div><div class="value">${(t.activas||0).toLocaleString()}</div><div class="sub">de ${(t.total||0).toLocaleString()} total</div></div>
    <div class="stat-card"><div class="label">Instituciones</div><div class="value">${(t.instituciones||0).toLocaleString()}</div></div>
    <div class="stat-card green"><div class="label">URLs válidas</div><div class="value">${(t.urls_validas||0).toLocaleString()}</div></div>
    <div class="stat-card red"><div class="label">URLs rotas</div><div class="value">${(t.urls_rotas||0).toLocaleString()}</div></div>
    <div class="stat-card yellow"><div class="label">Sin validar</div><div class="value">${(t.urls_sin_validar||0).toLocaleString()}</div></div>
    <div class="stat-card"><div class="label">Revisión pendiente</div><div class="value">${(t.needs_review||0).toLocaleString()}</div></div>
  `;
}

function renderUrlStats(uv) {
  const g = document.getElementById('url-stats-grid');
  if (!Object.keys(uv).length) { g.innerHTML=''; return; }
  g.innerHTML = `
    <div class="stat-card green"><div class="label">url_oferta OK</div><div class="value">${(uv.url_oferta_validas||0).toLocaleString()}</div></div>
    <div class="stat-card red"><div class="label">url_oferta rota</div><div class="value">${(uv.url_oferta_rotas||0).toLocaleString()}</div></div>
    <div class="stat-card green"><div class="label">url_bases OK</div><div class="value">${(uv.url_bases_validas||0).toLocaleString()}</div></div>
    <div class="stat-card red"><div class="label">url_bases rota</div><div class="value">${(uv.url_bases_rotas||0).toLocaleString()}</div></div>
    <div class="stat-card yellow"><div class="label">Sin chequear hoy</div><div class="value">${(uv.sin_chequear_hoy||0).toLocaleString()}</div></div>
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
  if (instId) p.set('institucion_id', instId);
  if (estado) p.set('estado', estado);
  if (cDesde) p.set('cierre_desde', cDesde);
  if (cHasta) p.set('cierre_hasta', cHasta);
  if (nrev)   p.set('needs_review', nrev);
  if (sinRenta) p.set('sin_renta', 'true');

  _limpiarSeleccion();
  const tbody = document.getElementById('ofertas-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="9"><span class="spinner"></span></td></tr>`;
  try {
    const d = await api(`/ofertas?${p}`);
    renderOfertasTable(d.ofertas||[]);
    renderPaginacion(d, 'ofertas-paginacion');
    document.getElementById('ofertas-badge').textContent = `${(d.total||0).toLocaleString()} ofertas`;
  } catch(e) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="9" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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
  }
}

function renderOfertasTable(ofertas) {
  const tbody = document.getElementById('ofertas-tbody');
  if (!ofertas.length) { tbody.innerHTML='<tr class="empty-row"><td colspan="9">Sin resultados</td></tr>'; return; }
  tbody.innerHTML = ofertas.map(o => {
    _itemCache[o.id] = o;
    const activa = o.activa !== false;
    const urlOk  = o.url_oferta_valida;
    const urlIcon = o.url_oferta ? (urlOk===false?'🔴':urlOk===true?'🟢':'⚪') : '<span class="text-muted">—</span>';
    const inst = o.institucion_display || o.institucion_nombre || '<span class="text-muted">—</span>';
    return `<tr>
      <td><input type="checkbox" class="sel-oferta" data-id="${o.id}"></td>
      <td class="text-muted text-small">${o.id}</td>
      <td style="max-width:220px">
        <div title="${escAttr(o.cargo)}" style="font-weight:500">${trunc(o.cargo,36)}</div>
        <div class="text-small text-muted" title="${escAttr(inst)}">${trunc(inst,34)}</div>
      </td>
      <td class="text-small text-muted">${trunc(o.sector_real||'',18)}</td>
      <td class="text-small text-muted">${trunc(o.region||'',14)}</td>
      <td class="text-small">${o.fecha_cierre?fmtDate(o.fecha_cierre):'<span class="text-muted">—</span>'}</td>
      <td>${activa?pill('activa','green'):pill(o.estado||'inactiva','red')}</td>
      <td style="text-align:center" title="${escAttr(o.url_oferta)}">${urlIcon}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm ${activa?'btn-danger':'btn-success'}" data-action="toggle-activa" data-id="${o.id}">${activa?'Pausar':'Activar'}</button>
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

// ── EDIT MODAL ────────────────────────────────────────────────
let _creandoOferta = false;

function openCrearOferta() {
  _creandoOferta = true;
  _editingId = null;
  document.getElementById('edit-modal-title').childNodes[0].textContent = 'Nueva oferta ';
  document.getElementById('edit-id').textContent = '';
  document.getElementById('edit-institucion-group').style.display = '';
  document.getElementById('edit-save-btn').textContent = 'Crear';
  ['edit-cargo','edit-institucion','edit-descripcion','edit-fecha-cierre','edit-region',
   'edit-tipo-contrato','edit-renta-min','edit-renta-max','edit-url-oferta','edit-url-bases']
    .forEach(id => { document.getElementById(id).value = ''; });
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
  document.getElementById('edit-fecha-cierre').value = o.fecha_cierre ? o.fecha_cierre.slice(0,10) : '';
  document.getElementById('edit-estado').value = o.estado||'activa';
  document.getElementById('edit-region').value = o.region||'';
  document.getElementById('edit-tipo-contrato').value = o.tipo_contrato||'';
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
    fecha_cierre:  document.getElementById('edit-fecha-cierre').value||null,
    estado:        document.getElementById('edit-estado').value,
    region:        document.getElementById('edit-region').value||null,
    tipo_contrato: document.getElementById('edit-tipo-contrato').value||null,
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
                    <td style="padding:2px 6px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(i.nombre)}">${trunc(i.nombre,32)}</td>
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
    tbody.innerHTML=`<tr class="empty-row"><td colspan="11" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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
      <td style="max-width:220px"><div title="${escAttr(f.nombre)}">${trunc(f.nombre||'—',32)}</div></td>
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
    tbody.innerHTML=`<tr class="empty-row"><td colspan="11" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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
      <td>${s.email}</td>
      <td class="text-small text-muted">${s.region||'—'}</td>
      <td class="text-small text-muted">${s.termino||'—'}</td>
      <td class="text-small text-muted">${s.sector||'—'}</td>
      <td class="text-small text-muted">${s.tipo_contrato||'—'}</td>
      <td><span class="pill gray">${s.frecuencia||'diaria'}</span></td>
      <td>${s.activa?pill('activo','green'):pill('inactivo','gray')}</td>
      <td class="text-small text-muted">${fmtDate(s.creada_en)}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-ghost btn-sm" data-action="test-email-sub" data-email="${escAttr(s.email)}">✉️</button>
        ${s.activa?`<button class="btn btn-danger btn-sm" style="margin-left:4px" data-action="desactivar-sub" data-id="${s.id}" data-email="${escAttr(s.email)}">✕</button>`:''}
      </td>
    </tr>`).join('');
  } catch(e) {
    tbody.innerHTML=`<tr class="empty-row"><td colspan="10" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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
            <span>${d.email}</span>
            <span style="color:${d.resultado==='enviado'?'var(--green)':d.resultado==='error'?'var(--red)':'var(--muted)'}">
              ${d.resultado} · ${d.coincidencias} oferta${d.coincidencias!==1?'s':''}
            </span>
          </div>`).join('')}
        </div></details>`:''}`;
    if (!dryRun && r.enviados > 0) toast(`${r.enviados} alertas enviadas ✓`);
    else if (dryRun) toast(`Simulación: ${r.enviados} se enviarían`);
  } catch(e) {
    res.innerHTML=`<span style="color:var(--red)">Error: ${e.message}</span>`;
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
      tbody.innerHTML = `<tr class="empty-row"><td colspan="4" style="color:var(--yellow)">${d.warning}</td></tr>`;
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
        <td class="text-small">${ev.email||'—'}</td>
        <td class="text-small text-muted" style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(ev.asunto)}">${trunc(ev.asunto,46)}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    statsEl.innerHTML = '';
    tbody.innerHTML = `<tr class="empty-row"><td colspan="4" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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

    // Alertas individuales
    alertas.forEach(a => {
      const bg = {error:'#dc262218',warning:'#d9770618',info:'#1d4ed818'}[a.nivel]||'var(--surface)';
      const co = {error:'var(--red)',warning:'var(--yellow)',info:'#60a5fa'}[a.nivel]||'var(--muted)';
      html += `<div style="background:${bg};border:1px solid ${co}44;border-radius:var(--radius);padding:12px 16px;display:flex;align-items:center;justify-content:space-between;gap:12px">
        <span style="color:${co}">${a.mensaje}</span>
        <button class="btn btn-ghost btn-sm" data-action="ir-a" data-target="${escAttr(a.accion)}">Ver →</button>
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
    el.innerHTML = `<div style="color:var(--red)">Error: ${e.message}</div>`;
  }
}

function irA(accion) {
  const tabMap = {
    'bulk-desactivar':'acciones','revalidar-urls':'acciones','revision':'revision',
    'scraper-runs':'scrapers','evaluaciones':'fuentes',
  };
  const tab = tabMap[accion]||'dashboard';
  document.querySelectorAll('nav button').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  const btn = document.querySelector(`[data-tab=${tab}]`);
  if (btn) btn.classList.add('active');
  const panel = document.getElementById('tab-'+tab);
  if (panel) panel.classList.add('active');
  if (tab==='revision') loadRevision();
  else if (tab==='scrapers') loadScrapers();
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
      else if (item._tipo==='sin_sector') detalle = item.sector||'(vacío)';
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
    tbody.innerHTML=`<tr class="empty-row"><td colspan="7" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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
  document.getElementById('run-experimental-wrap').style.display = v==='all' ? 'flex' : 'none';
});

async function runScraper() {
  const mode    = document.getElementById('run-mode').value;
  const kind    = document.getElementById('run-kind').value;
  const instId  = document.getElementById('run-inst-id').value;
  const max     = parseInt(document.getElementById('run-max').value) || 50;
  const dryRun  = document.getElementById('run-dry').checked;
  const res     = document.getElementById('run-result');

  const payload = { mode, dry_run: dryRun, max };
  if (mode==='kind')        payload.kind = kind;
  if (mode==='institucion') payload.institucion_id = parseInt(instId);
  if (mode==='all') {
    payload.include_experimental = document.getElementById('run-experimental').checked;
    const aviso = payload.include_experimental
      ? '¿Lanzar corrida COMPLETA incluyendo fuentes experimentales? Puede tardar bastante.'
      : '¿Lanzar corrida completa de todos los scrapers activos? Puede tardar varios minutos.';
    if (!dryRun && !confirm(aviso)) return;
  }

  res.style.display='block';
  res.innerHTML='<span class="spinner"></span> Lanzando…';
  try {
    const r = await api('/scraper/run', { method:'POST', body:JSON.stringify(payload) });
    res.innerHTML = `✅ Proceso lanzado — PID: <strong>${r.pid}</strong> · run_id: ${r.run_id??'—'} · dry_run: ${r.dry_run} · cmd: <code>${r.cmd.join(' ')}</code>`;
    toast('Scraper iniciado ✓');
    loadProcesos();
    if (r.log) verLog(r.log);
  } catch(e) {
    res.innerHTML = `<span style="color:var(--red)">Error: ${e.message}</span>`;
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
        <td class="text-small text-muted" style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(p.cmd)}">${p.cmd}</td>
        <td><button class="btn btn-ghost btn-sm" data-action="ver-log" data-log="${escAttr(p.log)}">📜 Ver</button></td>
      </tr>`).join('');
    }
    // Sigue refrescando mientras haya procesos en curso y el tab esté visible
    if (ps.some(p => p.estado === 'en_curso') && _tabAccionesActivo()) {
      _procPollTimer = setTimeout(loadProcesos, 4000);
    }
  } catch(e) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="6" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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

async function loadCatalog() {
  const tbody = document.getElementById('catalog-tbody');
  tbody.innerHTML = `<tr class="loading-row"><td colspan="6"><span class="spinner"></span> Cargando catálogo…</td></tr>`;
  try {
    const items = await api('/scraper/catalog');
    if (!items.length) { tbody.innerHTML='<tr class="empty-row"><td colspan="6">Sin datos</td></tr>'; return; }
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
  } catch(e) {
    tbody.innerHTML=`<tr class="empty-row"><td colspan="6" style="color:var(--red)">Error: ${e.message}</td></tr>`;
  }
}

async function runInstancia(id, nombre) {
  if (!confirm(`¿Ejecutar scraper para "${nombre}" (ID ${id})?`)) return;
  const res = document.getElementById('run-result');
  res.style.display='block';
  res.innerHTML='<span class="spinner"></span> Lanzando…';
  // Cambiar al tab acciones si no está ahí
  document.querySelectorAll('nav button').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  document.querySelector('[data-tab=acciones]').classList.add('active');
  document.getElementById('tab-acciones').classList.add('active');
  try {
    const r = await api('/scraper/run', {
      method:'POST',
      body: JSON.stringify({ mode:'institucion', institucion_id:id, max:100 }),
    });
    res.innerHTML=`✅ PID: <strong>${r.pid}</strong> · run_id: ${r.run_id??'—'} · inst: ${id}`;
    toast(`Scraper inst. ${id} iniciado ✓`);
    loadProcesos();
    if (r.log) verLog(r.log);
  } catch(e) {
    res.innerHTML=`<span style="color:var(--red)">Error: ${e.message}</span>`;
    toast('Error: '+e.message,'error');
  }
}

// ── CONFIG ─────────────────────────────────────────────────────
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
    document.getElementById('cfg-alertas').checked       = c.alertas_activas!=='false';
    document.getElementById('cfg-footer-extra').value    = c.footer_extra||'';
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
      tbody.innerHTML = `<tr class="empty-row"><td colspan="5" style="color:var(--yellow)">${d.warning}</td></tr>`;
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
        <td class="text-small text-muted" style="max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(det)}">${det}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="5" style="color:var(--red)">Error: ${e.message}</td></tr>`;
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
    case 'load-catalog':       loadCatalog(); break;
    case 'load-procesos':      loadProcesos(); break;
    case 'cerrar-log':         cerrarLog(); break;
    case 'load-config':        loadConfig(); break;
    case 'save-config':        saveConfig(); break;
    case 'load-audit':         loadAudit(); break;
    case 'crear-fuente':       openCrearFuente(); break;
    case 'nueva-oferta':       openCrearOferta(); break;
    case 'load-eventos':       loadEventos(); break;
    case 'bulk-sel-desactivar': bulkSeleccionadas('desactivar'); break;
    case 'bulk-sel-revisadas':  bulkSeleccionadas('revisadas'); break;
    case 'bulk-sel-limpiar':    _limpiarSeleccion(); break;
    case 'close-modal':        closeModal(); break;
    case 'save-edit':          saveEdit(); break;
    case 'close-fuente-modal': closeFuenteModal(); break;
    case 'save-fuente':        saveFuente(); break;
    // Acciones por fila (data-id / data-* del elemento)
    case 'pagina':             loadOfertas(parseInt(d.page)); break;
    case 'toggle-activa':      toggleActiva(parseInt(d.id)); break;
    case 'open-edit':          openEdit(parseInt(d.id)); break;
    case 'marcar-revisada':    marcarRevisada(parseInt(d.id)); break;
    case 'editar-fuente':      openEditarFuente(parseInt(d.id)); break;
    case 'override-fuente':    overrideFuente(parseInt(d.id), d.nombre||''); break;
    case 'desactivar-fuente':  desactivarFuente(parseInt(d.id), d.nombre||''); break;
    case 'test-email-sub':     testEmailSub(d.email); break;
    case 'desactivar-sub':     desactivarSub(parseInt(d.id), d.email); break;
    case 'run-instancia':      runInstancia(parseInt(d.id), d.nombre||''); break;
    case 'ver-log':            verLog(d.log); break;
    case 'ir-a':               irA(d.target); break;
  }
});

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
    case 'scrapers': loadScrapers(); break;
    case 'revision': loadRevision(); break;
    case 'alertas':  loadAlertas(); break;
    case 'fuentes':  loadFuentes(); break;
    case 'audit':    loadAudit(); break;
    case 'eventos':  loadEventos(); break;
  }
});

document.addEventListener('input', e => {
  if (e.target.matches('[data-input="ofertas-q"]')) debounceSearch();
});

// ── Keyboard ───────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.key==='Escape') closeModal();
  if (e.key==='Enter' && document.getElementById('auth-screen').style.display!=='none') doLogin();
});
document.getElementById('auth-btn').addEventListener('click', doLogin);

// ── Auto-login ─────────────────────────────────────────────────
// Restaura la sesión desde sessionStorage si el token sigue vigente
// (con margen de 30s) y el ADMIN_PATH del URL coincide con el guardado.
(async function() {
  const saved = sessionStorage.getItem('_gc');
  if (!saved) return;
  try {
    const { token, expiresAt, k } = JSON.parse(saved);
    if (k !== ADMIN_PATH || !token || !_tokenSigueVivo(expiresAt)) {
      sessionStorage.removeItem('_gc');
      return;
    }
    _creds = { header: buildAuthHeaderFromToken(token), token, expiresAt };
    // Validación rápida contra el servidor: si el token fue revocado o
    // el secreto rotó, /auth/me devuelve 401 y limpiamos.
    try {
      const r = await fetch(`${API_BASE}/api/${ADMIN_PATH}/auth/me`, {
        headers: { Authorization: _creds.header },
      });
      if (!r.ok) { sessionStorage.removeItem('_gc'); _creds = null; return; }
    } catch (_) { /* sin red: intentamos usar el token igual */ }
    showApp();
  } catch(e) { sessionStorage.removeItem('_gc'); }
})();
