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

const state = { pagina: 1, q: '', region: '', tipo: '' };

function fmt(n) { return (n ?? 0).toLocaleString('es-CL'); }

function fmtFecha(s) {
  if (!s) return null;
  const d = new Date(s);
  if (isNaN(d)) return s;
  return d.toLocaleDateString('es-CL', { day: 'numeric', month: 'short', year: 'numeric' });
}

function renderCard(o) {
  const tipo = o.tipo_contrato || o.tipo_cargo;
  const estado = o.estado === 'cerrado' ? 'badge-cerrado' : 'badge-vencido';
  const estadoLabel = o.estado === 'cerrado' ? 'Cerrado' : 'Vencido';
  const urlOferta = o.url_oferta || o.url_bases;

  return `
    <div class="hist-card">
      <div>
        <div class="hist-cargo">${o.cargo || 'Sin cargo'}</div>
        <div class="hist-inst">${o.institucion || 'Sin institución'}</div>
        <div class="hist-meta">
          <span class="badge ${estado}">${estadoLabel}</span>
          ${tipo ? `<span class="badge badge-tipo">${tipo}</span>` : ''}
          ${o.region ? `<span class="badge badge-region">${o.region}</span>` : ''}
        </div>
      </div>
      <div class="hist-derecho">
        ${o.fecha_cierre ? `<span class="hist-fecha">Cerró: ${fmtFecha(o.fecha_cierre)}</span>` : ''}
        <button type="button" class="hist-link hist-detalle" data-id="${o.id}">Ver detalles</button>
        ${urlOferta ? `<a href="${urlOferta}" target="_blank" rel="noopener" class="hist-link">Ver oferta ↗</a>` : ''}
      </div>
    </div>
  `;
}

function renderEsqueleto() {
  return Array(6).fill(0).map(() => `
    <div class="hist-card">
      <div>
        <span class="skeleton" style="height:16px;width:55%;margin-bottom:6px"></span>
        <span class="skeleton" style="height:12px;width:35%;margin-bottom:10px"></span>
        <span class="skeleton" style="height:10px;width:20%"></span>
      </div>
    </div>
  `).join('');
}

function renderPaginacion(total, paginas) {
  const el = document.getElementById('paginacion');
  if (paginas <= 1) { el.innerHTML = ''; return; }
  let html = '';
  const desde = Math.max(1, state.pagina - 2);
  const hasta = Math.min(paginas, state.pagina + 2);
  if (desde > 1) html += `<button class="pag-btn" type="button" data-pagina="1">1</button>`;
  if (desde > 2) html += `<span style="color:var(--texto3);padding:0 4px">…</span>`;
  for (let i = desde; i <= hasta; i++) {
    html += `<button class="pag-btn ${i === state.pagina ? 'activa' : ''}" type="button" data-pagina="${i}">${i}</button>`;
  }
  if (hasta < paginas - 1) html += `<span style="color:var(--texto3);padding:0 4px">…</span>`;
  if (hasta < paginas) html += `<button class="pag-btn" type="button" data-pagina="${paginas}">${paginas}</button>`;
  el.innerHTML = html;
}

function irPagina(p) {
  state.pagina = p;
  cargar();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function buscar() {
  state.q = document.getElementById('f-q').value.trim();
  state.region = document.getElementById('f-region').value;
  state.tipo = document.getElementById('f-tipo').value;
  state.pagina = 1;
  cargar();
}

async function cargar() {
  document.getElementById('lista').innerHTML = renderEsqueleto();
  document.getElementById('total-label').textContent = 'Cargando…';
  document.getElementById('paginacion').innerHTML = '';

  const params = new URLSearchParams({ pagina: state.pagina, por_pagina: 50 });
  if (state.region) params.set('region', state.region);
  if (state.tipo) params.set('tipo', state.tipo);

  try {
    const res = await fetch(`${API_BASE}/api/historial?${params}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();

    // Filtro de búsqueda local si se ingresó texto (historial no tiene param q)
    let lista = d.ofertas || [];
    if (state.q) {
      const q = state.q.toLowerCase();
      lista = lista.filter(o =>
        (o.cargo || '').toLowerCase().includes(q) ||
        (o.institucion || '').toLowerCase().includes(q)
      );
    }

    if (lista.length === 0) {
      document.getElementById('lista').innerHTML = `<div class="estado-vacio">No se encontraron concursos cerrados con los filtros seleccionados.</div>`;
      document.getElementById('total-label').textContent = '0 resultados';
      return;
    }

    lista.forEach((o) => { if (o && o.id != null) _histPorId[o.id] = o; });
    document.getElementById('lista').innerHTML = lista.map(renderCard).join('');
    document.getElementById('total-label').textContent = `${fmt(d.total)} concurso${d.total !== 1 ? 's' : ''} en el historial`;
    renderPaginacion(d.total, d.paginas);
  } catch (e) {
    document.getElementById('lista').innerHTML = `<div class="estado-vacio">No se pudo conectar con el servidor. Verifica que la API esté corriendo.</div>`;
    document.getElementById('total-label').textContent = '';
  }
}

// Cargar regiones dinámicamente desde API DPA
(async function() {
  try {
    const resp = await fetch(`${API_BASE}/api/regiones`);
    if (!resp.ok) return;
    const regiones = await resp.json();
    const sel = document.getElementById('f-region');
    if (!sel) return;
    while (sel.options.length > 1) sel.remove(1);
    regiones.forEach(r => {
      const opt = document.createElement('option');
      opt.value = r.nombre;
      opt.textContent = r.nombre;
      sel.appendChild(opt);
    });
  } catch {}
})();

// ── Integración: ficha de detalle + handlers CSP-safe ──────────────
// La CSP (script-src 'self', sin 'unsafe-inline') bloquea los onclick= inline,
// así que Filtrar, paginación y "Ver detalles" se enganchan por delegación.
const _histPorId = {};
function _capTipo(t) { t = (t || '').toString().trim(); return t ? t.charAt(0).toUpperCase() + t.slice(1) : ''; }

function normalizarOfertaHist(o) {
  const sem = window.richText && window.richText.buildSemanticSections
    ? window.richText.buildSemanticSections({ descripcion: o.descripcion || '', requisitos: o.requisitos_texto || o.requisitos || '' })
    : null;
  const req = (sem && sem.requisitos) || {};
  const fmtRenta = (min, max, grado) => {
    if (min) { const f = (n) => '$' + Number(n).toLocaleString('es-CL'); return (max && max !== min) ? `${f(min)} – ${f(max)}` : f(min); }
    if (grado) return 'Grado ' + grado + ' EUS';
    return null;
  };
  let portal = null;
  try { if (o.url_oferta) portal = new URL(o.url_oferta).hostname.replace(/^www\./, ''); } catch (e) { /* noop */ }
  return {
    cargo: o.cargo || '', institucion: o.institucion || '', verificada: false,
    sector: o.sector || '', tipo: _capTipo(o.tipo_contrato || o.tipo_cargo),
    region: o.region || '', comuna: o.ciudad || '', jornada: o.jornada || '',
    renta: fmtRenta(o.renta_bruta_min, o.renta_bruta_max, o.grado_eus),
    fechaPublicacion: o.fecha_publicacion ? fmtFecha(o.fecha_publicacion) : null,
    fechaCierre: o.fecha_cierre ? fmtFecha(o.fecha_cierre) : null,
    diasRestantes: (typeof o.dias_restantes === 'number') ? o.dias_restantes : null,
    portal,
    portalUrl: o.url_oferta || null,
    basesUrl: (o.url_bases_valida === false) ? null : (o.url_bases || null),
    objetivo: (sem && sem.objetivo) || null,
    funciones: (sem && sem.funciones) || [],
    requisitos: {
      obligatorios: req.obligatorios || [], formacion: req.formacion || [], experiencia: req.experiencia || [],
      especialidades: req.especialidades || [], competencias: req.competencias || [], documentos: req.documentos || [], deseables: req.deseables || [],
    },
    condiciones: (sem && sem.condiciones) || [],
    comoPostular: (sem && sem.postulacion) || [],
  };
}

function _favToggleHist(o) {
  try {
    const KEY = 'fav_contrataoplanta';
    let favs = JSON.parse(localStorage.getItem(KEY) || '[]');
    const i = favs.findIndex((f) => f.id === o.id);
    if (i >= 0) favs.splice(i, 1);
    else favs.push({ id: o.id, cargo: o.cargo, institucion: o.institucion, region: o.region, fecha_cierre: o.fecha_cierre, url_oferta: o.url_oferta, guardado_en: new Date().toISOString() });
    localStorage.setItem(KEY, JSON.stringify(favs));
  } catch (e) { /* noop */ }
}
function _favGuardadaHist(id) {
  try { return JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]').some((f) => f.id === id); }
  catch (e) { return false; }
}

async function abrirFichaHist(id) {
  let o = _histPorId[id];
  if (!o) {
    try { const res = await fetch(`${API_BASE}/api/ofertas/${id}`); if (res.ok) o = await res.json(); } catch (e) { /* noop */ }
  }
  if (!o || !window.FichaOferta) {
    const u = o && (o.url_oferta || o.url_bases);
    if (u) window.open(u, '_blank', 'noopener');
    return;
  }
  FichaOferta.abrir(normalizarOfertaHist(o), {
    guardada: _favGuardadaHist(o.id),
    onPostular: () => { const u = o.url_oferta || o.url_bases; if (u) window.open(u, '_blank', 'noopener'); },
    onBases: () => { if (o.url_bases) window.open(o.url_bases, '_blank', 'noopener'); },
    onGuardar: () => _favToggleHist(o),
  });
}

document.getElementById('btn-filtrar')?.addEventListener('click', buscar);
document.getElementById('lista')?.addEventListener('click', (e) => {
  const b = e.target.closest('.hist-detalle');
  if (!b) return;
  abrirFichaHist(parseInt(b.dataset.id, 10));
});
document.getElementById('paginacion')?.addEventListener('click', (e) => {
  const b = e.target.closest('[data-pagina]');
  if (!b) return;
  irPagina(parseInt(b.dataset.pagina, 10));
});

cargar();
