// El backend vive en Railway. Cloudflare Pages NO proxea /api/* a un origen
// externo (las reglas 200 de _redirects solo aplican a rutas internas), así
// que apuntamos directo a Railway en producción — igual que app.js. En local
// usamos el uvicorn de :8000. Override con window.__API_BASE para staging/tests.
const RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
const _hostLocal = (
  window.location.hostname === 'localhost' ||
  window.location.hostname === '' ||
  window.location.hostname === '127.0.0.1'
);
const _apiForzada = (typeof window.__API_BASE === 'string' ? window.__API_BASE.trim() : '');
const API_BASE = _apiForzada
  ? _apiForzada.replace(/\/+$/, '')
  : (_hostLocal ? 'http://localhost:8000' : RAILWAY_BACKEND);

const FAV_KEY = 'fav_contrataoplanta';

// ── Normalización de tipo de vínculo (catálogo unificado) ─────────
const TIPO_LABEL = {
  planta:'Planta', contrata:'Contrata', honorarios:'Honorarios',
  codigo_trabajo:'Código del Trabajo', reemplazo:'Reemplazo',
  otro:'Otro', no_informa:'No informa', sin_datos:'Sin datos',
};
const TIPO_CSS = {
  planta:'badge-planta', contrata:'badge-contrata', honorarios:'badge-honorarios',
  codigo_trabajo:'badge-codigo', reemplazo:'badge-otro', otro:'badge-otro',
  no_informa:'badge-sin-dato', sin_datos:'badge-sin-dato',
};
function tipoClaveNormalizada(v){
  if(v==null) return 'sin_datos';
  const s=String(v).trim(); if(!s) return 'sin_datos';
  const k=s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/[^a-z0-9\s]/g,' ').replace(/\s+/g,' ').trim();
  if(!k) return 'sin_datos';
  if(k.includes('planta')) return 'planta';
  if(k.includes('contrata')) return 'contrata';
  if(k.includes('honorario')) return 'honorarios';
  if(k.includes('codigo') && k.includes('trabajo')) return 'codigo_trabajo';
  if(k.includes('reemplazo')||k.includes('suplencia')) return 'reemplazo';
  if(k.includes('no informa')||k.includes('no especifica')) return 'no_informa';
  if(k.includes('otro')) return 'otro';
  return 'otro';
}
function tipoEtiqueta(v){ return TIPO_LABEL[tipoClaveNormalizada(v)] || (v||'Sin datos'); }
function tipoClase(v){ return TIPO_CSS[tipoClaveNormalizada(v)] || 'badge-otro'; }

// ── Helpers ────────────────────────────────────────────────────────
function getFavs() {
  return JSON.parse(localStorage.getItem(FAV_KEY) || '[]');
}
function setFavs(favs) {
  localStorage.setItem(FAV_KEY, JSON.stringify(favs));
}

function diasRestantes(fechaStr) {
  if (!fechaStr) return null;
  const hoy = new Date(); hoy.setHours(0,0,0,0);
  const cierre = new Date(fechaStr + 'T00:00:00');
  return Math.round((cierre - hoy) / 86400000);
}

function formatFecha(fechaStr) {
  if (!fechaStr) return null;
  const [y, m, d] = fechaStr.split('-');
  const meses = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
  return `${parseInt(d)} de ${meses[parseInt(m)-1]} de ${y}`;
}

function formatGuardado(isoStr) {
  const d = new Date(isoStr);
  return `Guardado el ${d.getDate()} de ${['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'][d.getMonth()]}`;
}

function escHtml(s) {
  return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// formatRichText vive en rich-text.js (window.formatRichText).

// ── Renderizar grid ────────────────────────────────────────────────
function renderFavs() {
  const favs = getFavs();
  document.getElementById('hero-count').textContent = favs.length;

  if (favs.length === 0) {
    document.getElementById('empty-state').style.display = 'block';
    document.getElementById('favs-grid').innerHTML = '';
    return;
  }
  document.getElementById('empty-state').style.display = 'none';

  document.getElementById('favs-grid').innerHTML = favs.map(f => {
    const dias = diasRestantes(f.fecha_cierre);
    let cierreHtml = '';
    if (dias === null) {
      cierreHtml = `<span class="fav-cierre fav-cierre--sin">Sin fecha de cierre</span>`;
    } else if (dias < 0) {
      cierreHtml = `<span class="fav-cierre fav-cierre--urgente">✕ Proceso cerrado</span>`;
    } else if (dias <= 2) {
      cierreHtml = `<span class="fav-cierre fav-cierre--urgente">⚡ Cierra en ${dias === 0 ? 'hoy' : dias + (dias===1?' día':' días')}</span>`;
    } else if (dias <= 7) {
      cierreHtml = `<span class="fav-cierre fav-cierre--pronto">⏱ Cierra en ${dias} ${dias === 1 ? 'día' : 'días'}</span>`;
    } else {
      cierreHtml = `<span class="fav-cierre fav-cierre--ok">📅 ${formatFecha(f.fecha_cierre)}</span>`;
    }

    return `
    <div class="fav-card" id="fav-card-${f.id}">
      <div class="fav-card-body">
        <div class="fav-card-kicker">${f.institucion || 'Institución pública'}</div>
        <div class="fav-card-cargo">${f.cargo || 'Cargo'}</div>
        <div class="fav-badges">
          ${f.region ? `<span class="fav-badge fav-badge-region">🗺 ${f.region}</span>` : ''}
        </div>
        ${cierreHtml}
        <div class="fav-guardado">${f.guardado_en ? formatGuardado(f.guardado_en) : ''}</div>
      </div>
      <div class="fav-card-actions">
        <button class="btn-fav-detail" type="button" data-fav-detalle="${f.id}">Ver detalle</button>
        <button class="cop-cmp-btn" type="button" data-fav-comparar="${f.id}" title="Comparar oferta">⇆ Comparar oferta</button>
        <button class="btn-fav-remove" type="button" title="Eliminar de favoritos" data-fav-remove="${f.id}">✕</button>
      </div>
    </div>`;
  }).join('');

  if (window.repintarComparar) window.repintarComparar();
}

function eliminarFav(id) {
  let favs = getFavs().filter(f => f.id !== id);
  setFavs(favs);
  renderFavs();
}

// ── Modal ──────────────────────────────────────────────────────────
// Punto de entrada: usa la ficha compartida (FichaOferta) si está cargada;
// si no (o si falla), cae al modal legacy de esta página.
async function abrirModal(ofertaId) {
  if (window.FichaOferta) {
    try { return await abrirFichaFav(ofertaId); }
    catch (e) { console.warn('[ficha fav] fallo, uso modal legacy', e); }
  }
  return _abrirModalLegacy(ofertaId);
}

// Adapta el objeto de la API al esquema de FichaOferta (versión autónoma de
// esta página: solo usa helpers locales + window.richText).
function normalizarOfertaFav(o) {
  const sem = window.richText && window.richText.buildSemanticSections
    ? window.richText.buildSemanticSections({ descripcion: o.descripcion || '', requisitos: o.requisitos_texto || o.requisitos || '' })
    : null;
  const req = (sem && sem.requisitos) || {};
  const _correoOferta = window.extraerCorreoOferta
    ? window.extraerCorreoOferta((o.descripcion || '') + '\n' + (o.requisitos_texto || o.requisitos || ''))
    : null;
  const fmtRenta = (min, max, grado) => {
    if (min) { const f = (n) => '$' + Number(n).toLocaleString('es-CL'); return (max && max !== min) ? `${f(min)} – ${f(max)}` : f(min); }
    if (grado) return 'Grado ' + grado + ' EUS';
    return null;
  };
  let portal = null;
  try { if (o.url_oferta) portal = new URL(o.url_oferta).hostname.replace(/^www\./, ''); } catch (e) { /* noop */ }
  const dias = (typeof o.dias_restantes === 'number') ? o.dias_restantes : diasRestantes(o.fecha_cierre);
  return {
    cargo: o.cargo || '', institucion: o.institucion || '', verificada: false,
    sector: o.sector || '', tipo: tipoEtiqueta(o.tipo_contrato) || '',
    region: o.region || '', comuna: o.ciudad || '', jornada: o.jornada || '',
    renta: fmtRenta(o.renta_bruta_min, o.renta_bruta_max, o.grado_eus),
    fechaPublicacion: o.fecha_publicacion ? formatFecha(o.fecha_publicacion) : null,
    fechaCierre: o.fecha_cierre ? formatFecha(o.fecha_cierre) : null,
    diasRestantes: dias,
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
    email: _correoOferta ? _correoOferta.email : null,
    emailLabel: _correoOferta ? _correoOferta.label : null,
  };
}

async function abrirFichaFav(ofertaId) {
  const res = await fetch(`${API_BASE}/api/ofertas/${ofertaId}`);
  if (!res.ok) throw new Error(res.status);
  const o = await res.json();
  FichaOferta.abrir(normalizarOfertaFav(o), {
    guardada: getFavs().some((f) => f.id === o.id),
    onPostular: () => { const u = o.url_oferta || o.url_bases; if (u) window.open(u, '_blank', 'noopener'); },
    onBases: () => { if (o.url_bases) window.open(o.url_bases, '_blank', 'noopener'); },
    onGuardar: () => {
      toggleFavorito(o);
      if (!getFavs().some((f) => f.id === o.id)) {
        renderFavs();
        if (FichaOferta.cerrar) FichaOferta.cerrar();
      }
    },
    comparando: window.Comparador ? Comparador.has(o.id) : false,
    onComparar: () => {
      if (!window.Comparador) return false;
      const antes = Comparador.has(o.id);
      const ahora = Comparador.toggle(o.id);
      if (!antes && ahora === false) alert('Podés comparar hasta ' + Comparador.MAX + ' ofertas');
      if (window.repintarComparar) window.repintarComparar();
      return ahora;
    },
  });
}

async function _abrirModalLegacy(ofertaId) {
  const modal = document.getElementById('modal');
  modal.classList.add('open');
  document.body.style.overflow = 'hidden';

  // Reset
  document.getElementById('modal-kicker').textContent = '…';
  document.getElementById('modal-cargo').textContent = 'Cargando…';
  document.getElementById('modal-badges').innerHTML = '';
  document.getElementById('modal-renta').textContent = '…';
  document.getElementById('modal-tipo-contrato').textContent = '…';
  document.getElementById('modal-jornada').textContent = '…';
  document.getElementById('modal-cierre').textContent = '…';
  document.getElementById('modal-requisitos').innerHTML = '';
  document.getElementById('modal-descripcion').innerHTML = '';

  try {
    const res = await fetch(`${API_BASE}/api/ofertas/${ofertaId}`);
    if (!res.ok) throw new Error(res.status);
    const o = await res.json();

    document.getElementById('modal-kicker').textContent =
      [o.institucion, o.sector].filter(Boolean).join(' · ').toUpperCase();
    document.getElementById('modal-cargo').textContent = o.cargo || '—';

    // Badges (reutiliza normalización centralizada)
    const tipoCls = tipoClase(o.tipo_contrato);
    const tipoLbl = tipoEtiqueta(o.tipo_contrato);
    const badgesHTML = [
      `<span class="badge ${tipoCls}">${tipoLbl}</span>`,
      o.region        ? `<span class="badge badge-region">🗺 ${o.region}${o.ciudad ? ' · ' + o.ciudad : ''}</span>` : '',
    ].filter(Boolean).join('');
    document.getElementById('modal-badges').innerHTML = badgesHTML;

    // Info grid
    const fmtRenta = (min, max) => {
      if (!min && !max) return 'Ver bases';
      const fmt = n => '$' + Number(n).toLocaleString('es-CL');
      return max && max !== min ? `${fmt(min)} – ${fmt(max)}` : fmt(min || max);
    };
    document.getElementById('modal-renta').textContent = fmtRenta(o.renta_bruta_min, o.renta_bruta_max);
    document.getElementById('modal-tipo-contrato').textContent = tipoLbl;
    document.getElementById('modal-jornada').textContent = o.jornada || '—';

    const diasR = diasRestantes(o.fecha_cierre);
    const cierreEl = document.getElementById('modal-cierre');
    if (!o.fecha_cierre) {
      cierreEl.textContent = 'Sin fecha'; cierreEl.style.color = '';
    } else if (diasR < 0) {
      cierreEl.textContent = 'Proceso cerrado'; cierreEl.style.color = 'var(--rojo)';
    } else if (diasR <= 2) {
      cierreEl.textContent = `${formatFecha(o.fecha_cierre)} (¡${diasR === 0 ? 'hoy' : diasR + (diasR === 1 ? ' día' : ' días')}!)`;
      cierreEl.style.color = 'var(--rojo)';
    } else if (diasR <= 7) {
      cierreEl.textContent = formatFecha(o.fecha_cierre);
      cierreEl.style.color = 'var(--naran)';
    } else {
      cierreEl.textContent = formatFecha(o.fecha_cierre);
      cierreEl.style.color = 'var(--verde)';
    }

    // Requisitos y descripción con formato enriquecido
    const reqHtml = formatRichText(o.requisitos_texto || o.requisitos || '');
    document.getElementById('modal-requisitos').innerHTML = reqHtml ||
      '<p>Ver bases del concurso para requisitos completos.</p>';

    const descHtml = formatRichText(o.descripcion || '');
    document.getElementById('modal-descripcion').innerHTML = descHtml || '<p>—</p>';

    // Botones
    const urlPostular = o.url_oferta || o.url_bases || '#';
    document.getElementById('modal-btn-postular').onclick = () => window.open(urlPostular, '_blank');

    // Favorito — en esta página siempre está guardado, se puede eliminar
    const btnFav = document.getElementById('modal-btn-favorito');
    const favs = getFavs();
    const yaGuardado = favs.some(f => f.id === o.id);
    btnFav.textContent = yaGuardado ? '♥ Guardado' : '♡ Guardar';
    btnFav.classList.toggle('btn-modal-sec--activo', yaGuardado);
    btnFav.onclick = () => {
      toggleFavorito(o);
      // Actualizar la tarjeta si fue eliminada
      if (!getFavs().some(f => f.id === o.id)) {
        cerrarModal(null, true);
        renderFavs();
      }
    };

    document.getElementById('modal-btn-compartir').onclick = () => compartirOferta(o);

  } catch (err) {
    document.getElementById('modal-cargo').textContent = 'No se pudo cargar el detalle.';
  }
}

function cerrarModal(e, force) {
  if (force || e?.target === document.getElementById('modal')) {
    document.getElementById('modal').classList.remove('open');
    document.body.style.overflow = '';
  }
}

function compartirOferta(oferta) {
  const url  = oferta.url_oferta || window.location.href;
  const titulo = oferta.cargo || 'Oferta laboral';
  const texto  = `${titulo} — ${oferta.institucion || 'Institución pública'}`;
  if (navigator.share) {
    navigator.share({ title: titulo, text: texto, url }).catch(() => {});
  } else {
    navigator.clipboard.writeText(url).then(() => {
      const btn = document.getElementById('modal-btn-compartir');
      const orig = btn.textContent;
      btn.textContent = '✓ Enlace copiado';
      setTimeout(() => btn.textContent = orig, 2000);
    }).catch(() => prompt('Copia este enlace:', url));
  }
}

function toggleFavorito(oferta) {
  let favs = getFavs();
  const idx = favs.findIndex(f => f.id === oferta.id);
  const btn = document.getElementById('modal-btn-favorito');
  if (idx >= 0) {
    favs.splice(idx, 1);
    btn.textContent = '♡ Guardar';
    btn.classList.remove('btn-modal-sec--activo');
  } else {
    favs.push({
      id: oferta.id,
      cargo: oferta.cargo,
      institucion: oferta.institucion,
      region: oferta.region,
      fecha_cierre: oferta.fecha_cierre,
      url_oferta: oferta.url_oferta,
      guardado_en: new Date().toISOString(),
    });
    btn.textContent = '♥ Guardado';
    btn.classList.add('btn-modal-sec--activo');
  }
  setFavs(favs);
}

// ── Init ───────────────────────────────────────────────────────────
document.addEventListener('keydown', e => { if (e.key === 'Escape') cerrarModal(null, true); });

// Handlers CSP-safe (la CSP bloquea onclick= inline): cerrar modal legacy.
document.getElementById('modal')?.addEventListener('click', (e) => cerrarModal(e));
document.getElementById('modal-close-btn')?.addEventListener('click', () => cerrarModal(null, true));

// Delegación de las tarjetas de favoritos (Ver detalle / comparar / eliminar).
document.getElementById('favs-grid')?.addEventListener('click', (e) => {
  const det = e.target.closest('[data-fav-detalle]');
  if (det) { abrirModal(parseInt(det.dataset.favDetalle, 10)); return; }
  const rem = e.target.closest('[data-fav-remove]');
  if (rem) { eliminarFav(parseInt(rem.dataset.favRemove, 10)); return; }
  const cmp = e.target.closest('[data-fav-comparar]');
  if (cmp && window.Comparador) {
    const id = parseInt(cmp.dataset.favComparar, 10);
    if (!Comparador.toggle(id) && Comparador.count() >= Comparador.MAX) {
      alert('Podés comparar hasta ' + Comparador.MAX + ' ofertas');
    }
  }
});

// ── Comparador en favoritos ────────────────────────────────────────
if (window.Comparador) {
  const tray = document.createElement('div');
  tray.className = 'cop-cmp-tray';
  tray.innerHTML = '<span class="cop-cmp-tray-txt"><b id="cmp-tray-n">0</b> para comparar</span>'
    + '<button class="cop-cmp-tray-go" type="button">Ver comparador →</button>'
    + '<button class="cop-cmp-tray-clear" type="button">Quitar</button>';
  document.body.appendChild(tray);
  tray.querySelector('.cop-cmp-tray-go').onclick = () => Comparador.abrir();
  tray.querySelector('.cop-cmp-tray-clear').onclick = () => Comparador.limpiar();

  Comparador.config({
    fetchOferta: async (id) => (await fetch(`${API_BASE}/api/ofertas/${id}`)).json(),
    normalizar: (raw) => normalizarOfertaFav(raw),
    onVerDetalles: (id) => abrirModal(id),
  });

  window.repintarComparar = () => {
    const n = Comparador.count();
    tray.classList.toggle('is-on', n > 0);
    const nEl = tray.querySelector('#cmp-tray-n');
    if (nEl) nEl.textContent = n;
    document.querySelectorAll('.cop-cmp-btn[data-fav-comparar]').forEach((b) => {
      const on = Comparador.has(b.getAttribute('data-fav-comparar'));
      b.classList.toggle('is-on', on);
      b.textContent = on ? '✓ Comparar oferta' : '⇆ Comparar oferta';
    });
  };
  Comparador.onChange(window.repintarComparar);
}

renderFavs();
