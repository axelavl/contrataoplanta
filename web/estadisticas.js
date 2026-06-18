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

function fmt(n) {
  return (n ?? 0).toLocaleString('es-CL');
}

function renderUltimaActualizacion(ts) {
  const el = document.getElementById('data-last-update');
  if (!el || !ts) return;
  el.textContent = new Date(ts).toLocaleString('es-CL', { dateStyle: 'medium', timeStyle: 'short' });
}

function renderKpis(d) {
  document.getElementById('kpi-grid').innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Ofertas activas</div>
      <div class="kpi-valor">${fmt(d.activas_hoy)}</div>
      <div class="kpi-desc">En este momento</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Nuevas (48 h)</div>
      <div class="kpi-valor">${fmt(d.nuevas_48h)}</div>
      <div class="kpi-desc">Detectadas recientemente</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Cierran hoy</div>
      <div class="kpi-valor">${fmt(d.cierran_hoy)}</div>
      <div class="kpi-desc">Plazo vence hoy</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Instituciones activas</div>
      <div class="kpi-valor">${fmt(d.instituciones_activas)}</div>
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
        <div class="sector-bar" style="width:${Math.round(s.total / max * 100)}%"></div>
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

async function cargar() {
  try {
    const res = await fetch(`${API_BASE}/api/estadisticas`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();
    renderUltimaActualizacion(d.ultima_actualizacion);
    renderKpis(d);
    renderSectores(d.por_sector);
    renderHistorico(d.historico_mensual);
    renderMasActivas(d.mas_activas);
  } catch (e) {
    const msg = '<p class="error-estado">No pudimos cargar el panel ahora. Inténtalo nuevamente en unos minutos.</p>';
    document.getElementById('kpi-grid').innerHTML = msg;
    document.getElementById('sector-contenido').innerHTML = msg;
    document.getElementById('historico-contenido').innerHTML = msg;
    document.getElementById('mas-activas-contenido').innerHTML = msg;
  }
}

cargar();
