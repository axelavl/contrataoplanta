// El backend vive en Railway. Cloudflare Pages NO proxea /api/* a un origen
// externo (las reglas 200 de _redirects solo aplican a rutas internas), así
// que apuntamos directo a Railway en producción — igual que app.js. En local
// usamos el uvicorn de :8000. Override con window.__API_BASE para staging/tests.
const RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
const _hostLocal = (
  window.location.hostname === 'localhost' ||
  window.location.hostname === '127.0.0.1' ||
  window.location.hostname === ''
);
const _apiForzada = (typeof window.__API_BASE === 'string' ? window.__API_BASE.trim() : '');
const API_BASE = _apiForzada
  ? _apiForzada.replace(/\/+$/, '')
  : (_hostLocal ? 'http://localhost:8000' : RAILWAY_BACKEND);

// Mismo caché que usa el ribbon (web/ribbon-data.js). Lo reutilizamos para
// pintar al instante los KPIs que ya conocemos —activas, cierran hoy,
// instituciones, última actualización— sin esperar el fetch en vivo a Railway,
// que en frío puede tardar o colgarse. Así la página nunca queda en blanco.
const RIBBON_CACHE_KEY = 'cop_ribbon_cache';
const FETCH_TIMEOUT_MS = 10000;

function fmt(n) {
  return (n ?? 0).toLocaleString('es-CL');
}

function leerCache() {
  try { return JSON.parse(localStorage.getItem(RIBBON_CACHE_KEY) || 'null'); }
  catch (e) { return null; }
}

function guardarCache(d) {
  if (!d) return;
  try {
    const prev = leerCache() || {};
    // Conservamos las claves que el ribbon necesita y enriquecemos con lo demás.
    localStorage.setItem(RIBBON_CACHE_KEY, JSON.stringify({
      activas_hoy: d.activas_hoy ?? prev.activas_hoy,
      instituciones_activas: d.instituciones_activas ?? prev.instituciones_activas,
      cierran_hoy: d.cierran_hoy ?? prev.cierran_hoy,
      nuevas_48h: d.nuevas_48h ?? prev.nuevas_48h,
      ultima_actualizacion: d.ultima_actualizacion ?? prev.ultima_actualizacion
    }));
  } catch (e) {}
}

function renderUltimaActualizacion(ts) {
  const el = document.getElementById('data-last-update');
  if (!el || !ts) return;
  const fecha = new Date(ts);
  if (isNaN(fecha.getTime())) return;
  el.textContent = fecha.toLocaleString('es-CL', { dateStyle: 'medium', timeStyle: 'short' });
}

function renderKpis(d) {
  // Cada KPI cae a "—" si su dato puntual no vino, sin romper la grilla.
  const valor = (v) => (v == null ? '—' : fmt(v));
  document.getElementById('kpi-grid').innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Ofertas activas</div>
      <div class="kpi-valor">${valor(d.activas_hoy)}</div>
      <div class="kpi-desc">En este momento</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Nuevas (48 h)</div>
      <div class="kpi-valor">${valor(d.nuevas_48h)}</div>
      <div class="kpi-desc">Detectadas recientemente</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Cierran hoy</div>
      <div class="kpi-valor">${valor(d.cierran_hoy)}</div>
      <div class="kpi-desc">Plazo vence hoy</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Instituciones activas</div>
      <div class="kpi-valor">${valor(d.instituciones_activas)}</div>
      <div class="kpi-desc">Con al menos un concurso</div>
    </div>
  `;
}

function renderSectores(sectores) {
  if (!sectores || !sectores.length) {
    document.getElementById('sector-contenido').innerHTML = '<p class="error-estado">Sin datos disponibles.</p>';
    return;
  }
  const max = Math.max(...sectores.map(s => s.total));
  const html = sectores.map(s => `
    <div class="sector-row">
      <span class="sector-nombre">${s.sector || 'Sin sector'}</span>
      <div class="sector-bar-wrap">
        <div class="sector-bar" style="width:${max > 0 ? Math.round(s.total / max * 100) : 0}%"></div>
      </div>
      <span class="sector-total">${fmt(s.total)}</span>
    </div>
  `).join('');
  document.getElementById('sector-contenido').innerHTML = html;
}

function renderHistorico(meses) {
  if (!meses || !meses.length) {
    document.getElementById('historico-contenido').innerHTML = '<p class="historico-vacio">Sin datos históricos disponibles.</p>';
    return;
  }
  const max = Math.max(...meses.map(m => m.total));
  const barras = meses.map(m => {
    const pct = max > 0 ? Math.round(m.total / max * 100) : 0;
    const label = m.mes ? m.mes.slice(0, 7) : '';
    const mesCorto = label ? label.slice(5) : '';
    return `
      <div class="historico-barra-wrap" title="${label}: ${fmt(m.total)} ofertas">
        <div class="historico-barra" style="height:${pct}%"></div>
        <span class="historico-mes">${mesCorto}</span>
      </div>
    `;
  }).join('');
  document.getElementById('historico-contenido').innerHTML = `<div class="historico-wrap">${barras}</div>`;
}

function renderMasActivas(lista) {
  if (!lista || !lista.length) {
    document.getElementById('mas-activas-contenido').innerHTML = '<p class="error-estado">Sin datos disponibles.</p>';
    return;
  }
  const filas = lista.map(inst => `
    <tr>
      <td><a href="index.html?q=${encodeURIComponent(inst.nombre)}" class="inst-nombre">${inst.nombre}</a></td>
      <td><span class="badge-activas">${fmt(inst.activas)}</span></td>
      <td>${inst.nuevas_semana > 0 ? `<span class="badge-nuevas">+${inst.nuevas_semana}</span>` : '<span style="color:var(--texto3);font-size:12px">—</span>'}</td>
    </tr>
  `).join('');
  document.getElementById('mas-activas-contenido').innerHTML = `
    <table class="inst-tabla">
      <thead>
        <tr>
          <th>Institución</th>
          <th>Activas</th>
          <th>Esta semana</th>
        </tr>
      </thead>
      <tbody>${filas}</tbody>
    </table>
  `;
}

// Estado de error con botón "Reintentar". Si ya teníamos KPIs desde el caché,
// no los pisamos: solo marcamos las secciones que dependen del fetch en vivo.
function renderError(huboCache) {
  const btn = '<button type="button" class="stats-reintentar">Reintentar</button>';
  const msgSeccion = '<p class="error-estado">No pudimos cargar esta sección ahora. ' + btn + '</p>';

  if (!huboCache) {
    const el = document.getElementById('data-last-update');
    if (el && /cargando/i.test(el.textContent)) el.textContent = 'no disponible';
    document.getElementById('kpi-grid').innerHTML =
      '<p class="error-estado" style="grid-column:1/-1">No pudimos cargar el panel ahora. ' + btn + '</p>';
  }
  document.getElementById('sector-contenido').innerHTML = msgSeccion;
  document.getElementById('mas-activas-contenido').innerHTML = msgSeccion;
  document.getElementById('historico-contenido').innerHTML = msgSeccion;
  wireReintentar();
}

function wireReintentar() {
  document.querySelectorAll('.stats-reintentar').forEach(function (b) {
    b.addEventListener('click', function () { cargar(); }, { once: true });
  });
}

// Pinta de inmediato lo que sepamos por el caché del ribbon, para no mostrar
// esqueletos vacíos mientras llega (o no) el fetch en vivo.
function pintarDesdeCache() {
  const c = leerCache();
  if (!c) return false;
  renderKpis({
    activas_hoy: c.activas_hoy,
    nuevas_48h: c.nuevas_48h,        // puede faltar -> "—" hasta el fetch
    cierran_hoy: c.cierran_hoy,
    instituciones_activas: c.instituciones_activas
  });
  renderUltimaActualizacion(c.ultima_actualizacion);
  return true;
}

async function cargar() {
  const huboCache = pintarDesdeCache();

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(`${API_BASE}/api/estadisticas`, { signal: ctrl.signal });
    clearTimeout(timer);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();
    guardarCache(d);
    renderUltimaActualizacion(d.ultima_actualizacion);
    renderKpis(d);
    renderSectores(d.por_sector);
    renderHistorico(d.historico_mensual);
    renderMasActivas(d.mas_activas);
  } catch (e) {
    clearTimeout(timer);
    if (window.console) console.warn('[estadisticas] fetch falló:', e && e.message);
    renderError(huboCache);
  }
}

cargar();
