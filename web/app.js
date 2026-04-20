// ═══════════════════════════════════════════════════════════════
// web/app.js — JavaScript principal del frontend de estadoemplea
//
// Movido desde el <script> inline de web/index.html (que tenía ~3595
// líneas) por los siguientes motivos:
//
// 1. Caching: el HTML cambia a menudo (meta tags SSR, nuevos links),
//    mientras que este JS cambia rara vez. Archivo externo → cada uno
//    con su propio ETag y ciclo de cache.
// 2. Lectura y diffs: index.html baja de 4 297 a 703 líneas y los
//    cambios en la capa de presentación dejan de mezclarse con los
//    cambios en la lógica del cliente.
// 3. Paso previo a promover CSP de Report-Only a enforce. Sin este
//    corte, `script-src 'self'` rechazaría todo lo que había inline.
//    Pendiente para ese siguiente paso: sacar los 2 scripts chicos
//    del <head> (theme boot + js-nav), el <script> de Umami, el
//    bootstrap `_inject_offer_path_bootstrap` del backend, y los 31
//    `onclick=` inline del HTML.
//
// El archivo se carga con `<script src="app.js"></script>` SIN defer,
// para replicar el timing del <script> inline original (sync, antes
// de nav-mobile.js, sin esperar DOMContentLoaded).
// ═══════════════════════════════════════════════════════════════

// ── Base URL de la API: una sola URL, sin cadena de fallbacks ─────────────
// Backend en Railway. Los dominios de marca (contrataoplanta.cl,
// estadoemplea.cl, etc.) no existen en DNS — intentarlos sólo producía
// ERR_NAME_NOT_RESOLVED y status 530.
//
// Overrides:
//   - window.__API_BASE: fuerza otra base (útil para staging/tests).
//   - localhost/file://: apunta al uvicorn local en :8000.
const RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';

function _normalizarHostname(hostname) {
  const limpio = String(hostname || '').trim().toLowerCase();
  if (limpio.startsWith('[') && limpio.endsWith(']')) return limpio.slice(1, -1);
  return limpio;
}

function _esIPv4Loopback(hostname) {
  const m = /^127(?:\.(\d{1,3})){3}$/.exec(hostname);
  if (!m) return false;
  return hostname.split('.').slice(1).every((n) => Number(n) >= 0 && Number(n) <= 255);
}

function _esIPv6Loopback(hostname) {
  return hostname === '::1' || hostname === '0:0:0:0:0:0:0:1';
}

function _esEntornoLocal(hostname) {
  return (
    hostname === '' ||
    hostname === 'localhost' ||
    hostname.endsWith('.localhost') ||
    _esIPv4Loopback(hostname) ||
    _esIPv6Loopback(hostname)
  );
}

function _hostnameParaURL(hostname) {
  return hostname.includes(':') ? `[${hostname}]` : hostname;
}

const _hostnameNormalizado = _normalizarHostname(window.location.hostname || '');
const _esLocalhost = _esEntornoLocal(_hostnameNormalizado);

const API_BASE = (() => {
  const forzada = (typeof window.__API_BASE === 'string' ? window.__API_BASE : '').trim();
  if (forzada) return forzada.replace(/\/+$/, '');
  if (_esLocalhost) {
    const host = _hostnameParaURL(_hostnameNormalizado || 'localhost');
    const proto = window.location.protocol === 'https:' ? 'https:' : 'http:';
    return `${proto}//${host}:8000`;
  }
  return RAILWAY_BACKEND;
})();

// Alias de compatibilidad: otras partes del código (share a Instagram con
// OG image) leen `API_BASE_ACTIVA`. Ya no hay "activa vs candidatas", pero
// mantenemos el símbolo para no romper referentes.
let API_BASE_ACTIVA = API_BASE;
const API_TIMEOUT_MS = 12000;
const MAX_REINTENTOS_AUTOMATICOS = 3;
let _reintentosConsecutivos = 0;

console.info('[API] Base activa', { API_BASE, esLocalhost: _esLocalhost });

async function fetchApi(path, options = {}) {
  const url = `${API_BASE}${path}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), API_TIMEOUT_MS);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(timeoutId);
    return resp;
  } catch (err) {
    clearTimeout(timeoutId);
    const isTimeout = err?.name === 'AbortError';
    const detalle = isTimeout
      ? `Timeout (${API_TIMEOUT_MS}ms)`
      : (err && err.message ? err.message : String(err));
    const wrapped = isTimeout ? new Error(`Timeout (${API_TIMEOUT_MS}ms) en ${url}`) : err;
    // Mantener el shape `detalles` que consume `_resumenErrorConexion`.
    wrapped.detalles = [{ url, base: API_BASE, detalle }];
    console.warn('[API] Error de conexión', { url, detalle });
    throw wrapped;
  }
}

// ── Estado global de filtros y paginación ──────────────────────────────────
const PREFS_KEY = 'prefs_contrataoplanta';
function cargarPrefs() {
  try { return JSON.parse(localStorage.getItem(PREFS_KEY) || '{}'); } catch { return {}; }
}
function guardarPrefs(prefs) {
  const actual = cargarPrefs();
  localStorage.setItem(PREFS_KEY, JSON.stringify({ ...actual, ...prefs }));
}

const _prefs = cargarPrefs();
const POR_PAGINA_CONFIG = {
  // Mantener paginación clásica: en un agregador con filtros intensivos es más
  // predecible para comparar ofertas entre páginas y no romper el contexto.
  compacta: { opciones: [20, 30, 50, 100], porDefecto: 30 },
  cards:    { opciones: [12, 20, 30], porDefecto: 20 },
  grid:     { opciones: [9, 12, 18], porDefecto: 12 },
};

// Contador de visitas + auto-cambio silencioso a "cierre" en la 3ª visita
// cuando el usuario aún no ha personalizado el orden.
_prefs.visitas = (_prefs.visitas || 0) + 1;
guardarPrefs({ visitas: _prefs.visitas });

function _ordenInicial(prefs) {
  if (prefs.orden_personalizado && prefs.orden) return prefs.orden;
  if ((prefs.visitas || 0) >= 3) return 'cierre';
  return prefs.orden || 'recientes';
}

const TIPOS_POR_DEFECTO = ['planta','contrata','honorarios','codigo_trabajo','otro','no_informa'];
const ORDEN_POR_DEFECTO = _ordenInicial(_prefs);

const estado = {
  pagina: 1,
  q: '',
  region: '',
  sector: '',
  tipos: [...TIPOS_POR_DEFECTO],
  cierra_pronto: false,
  nuevas: false,
  orden:        ORDEN_POR_DEFECTO,
  por_pagina:   _prefs.por_pagina || POR_PAGINA_CONFIG[_prefs.vista || 'cards']?.porDefecto || 20,
  vista:        _prefs.vista      || 'cards',
  institucion_id: null,
  renta_min: null,
  ciudad: '',
  comunas: [],
  vista_listado: 'vigentes',
};
const ORDEN_SHARE_DEFAULT = 'recientes';
let _ofertasPorId = new Map();
let _comunasCatalogo = [];
let _comunasPanelAbierto = false;
let _comunasDraft = [];

function getConfigPorPagina(vista = estado.vista) {
  return POR_PAGINA_CONFIG[vista] || POR_PAGINA_CONFIG.cards;
}

function syncSelectPorPagina(opts = {}) {
  const { resetPagina = false, recargar = false } = opts;
  const sel = document.getElementById('ctrl-por-pagina');
  if (!sel) return;
  const cfg = getConfigPorPagina(estado.vista);
  const guardadoPorVista = (cargarPrefs().por_pagina_por_vista || {})[estado.vista];
  const fallback = cfg.porDefecto;
  const anterior = Number(estado.por_pagina) || fallback;
  const siguiente = cfg.opciones.includes(anterior)
    ? anterior
    : (cfg.opciones.includes(Number(guardadoPorVista)) ? Number(guardadoPorVista) : fallback);

  sel.innerHTML = cfg.opciones
    .map((n) => `<option value="${n}">${n} / pág.</option>`)
    .join('');
  sel.value = String(siguiente);
  if (siguiente !== anterior) estado.por_pagina = siguiente;
  if (resetPagina) estado.pagina = 1;
  if (recargar) cargarOfertas();
}

let _debounceTimer = null;
function debounceBuscar(ms = 400) {
  clearTimeout(_debounceTimer);
  _debounceTimer = setTimeout(buscar, ms);
}

function comunaNormalizada(valor) {
  return String(valor || '')
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function resumenComunasSeleccionadas(comunas = []) {
  if (!Array.isArray(comunas) || comunas.length === 0) return 'Seleccionar comunas';
  if (comunas.length === 1) return comunas[0];
  if (comunas.length === 2) return `${comunas[0]}, ${comunas[1]}`;
  return `${comunas[0]}, ${comunas[1]} +${comunas.length - 2}`;
}

// ── Mapas de tipo de contrato a clase CSS + etiqueta legible ─────────────
const TIPO_CSS = {
  planta:         'badge-planta',
  contrata:       'badge-contrata',
  honorarios:     'badge-honorarios',
  codigo_trabajo: 'badge-codigo',
  reemplazo:      'badge-otro',
  otro:           'badge-otro',
  no_informa:     'badge-sin-dato',
  sin_datos:      'badge-sin-dato',
};

const TIPO_LABEL = {
  planta:         'Planta',
  contrata:       'Contrata',
  honorarios:     'Honorarios',
  codigo_trabajo: 'Código del Trabajo',
  reemplazo:      'Reemplazo',
  otro:           'Otro',
  no_informa:     'No informa',
  sin_datos:      'Sin datos',
};

function tipoClaveNormalizada(valor) {
  if (valor == null) return 'sin_datos';
  const s = String(valor).trim();
  if (!s) return 'sin_datos';
  const key = s.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!key) return 'sin_datos';
  if (key.includes('planta')) return 'planta';
  if (key.includes('contrata')) return 'contrata';
  if (key.includes('honorario')) return 'honorarios';
  if (key.includes('codigo') && key.includes('trabajo')) return 'codigo_trabajo';
  if (key.includes('reemplazo') || key.includes('suplencia')) return 'reemplazo';
  if (key.includes('no informa') || key.includes('no especifica') || key.includes('sin especificar')) return 'no_informa';
  if (key.includes('otro')) return 'otro';
  return 'otro';
}

function tipoEtiqueta(valorBruto) {
  return TIPO_LABEL[tipoClaveNormalizada(valorBruto)] || (valorBruto || 'Sin datos');
}

function tipoClase(valorBruto) {
  return TIPO_CSS[tipoClaveNormalizada(valorBruto)] || 'badge-otro';
}

const ICONOS_SECTOR = {
  'Municipal':            '🏛️',
  'Salud Pública':        '🏥',
  'Ejecutivo Central':    '🏢',
  'Educación Superior':   '🎓',
  'Judicial':             '⚖️',
  'Gobierno Regional':    '🗺️',
  'FF.AA. y Orden':       '🛡️',
  'Empresa del Estado':   '🏦',
  'Autónomo/Regulador':   '📋',
  'Legislativo':          '📜',
};

// ── Escape helpers ────────────────────────────────────────────────────────
function escAttr(s) {
  return (s || '')
    .replace(/&/g,'&amp;')
    .replace(/"/g,'&quot;')
    .replace(/'/g,'&#39;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;');
}
function escHtml(s) {
  return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function sanitizeHighlightHtml(s) {
  const raw = String(s || '');
  const escaped = escHtml(raw);
  return escaped.replace(/&lt;(\/?)mark&gt;/gi, '<$1mark>');
}

const CARGO_SIGLAS_MAYUS = new Set([
  'rrhh','tic','ti','qa','ux','ui','it','pm','api','sql','sap','erp','crm','bi','seo','sem',
  'upc','uci','sapu','aps',
  // Instituciones y grados frecuentes en títulos del sector público.
  'slep','cmsc','sence','minsal','mineduc','minvu','mop','mma','mtt','segpres',
  'cft','ip','dgac','conaf','sernac','sernapesca','sernatur','sernam',
  'indap','inia','corfo','fosis','sii','srcei','sag','sml','senama','senapred',
  'ffaa','pdi','gope','dmc','dom','dideco','daem','dgac','oirs','asistente',
  'eus','esu','eur','icp','etcp','hep',
  // Números romanos (rangos usados en grados y niveles jerárquicos).
  'ii','iii','iv','v','vi','vii','viii','ix','x'
]);

// Siglas con puntos entre letras que deben mostrarse siempre en mayúsculas,
// independientemente de cómo vengan en la fuente. Ej: "e.u.S." → "E.U.S."
const CARGO_SIGLAS_PUNTEADAS = /\b(?:[A-Za-z]\.){2,}(?:[A-Za-z]\.?)?/g;

// Acrónimos a forzar en mayúsculas incluso en títulos con capitalización
// mixta (no sólo en los "gritados"). Complementa CARGO_SIGLAS_MAYUS para
// casos como "Técnico de Auditoría — SLEP Los Libertadores".
const CARGO_ACRONIMOS_FORZADOS = /\b(?:SLEP|CMSC|SENCE|MINSAL|MINEDUC|MINVU|SEGPRES|CFT|DGAC|CONAF|CORFO|FOSIS|SII|SRCEI|SAG|SML|SENAMA|SENAPRED|FFAA|PDI|DAEM|DIDECO|DOM|OIRS|IP|UCI|UPC|SAPU|APS|EUS|ESU|RRHH|TIC|ERP|CRM|API|SQL)\b/gi;

const CARGO_PROPIOS = new Map([
  ['chile', 'Chile'],
  ['santiago', 'Santiago'],
  ['valparaiso', 'Valparaíso'],
  ['biobio', 'Biobío'],
  ['araucania', 'Araucanía'],
  ['o\'higgins', "O'Higgins"],
  ['nuble', 'Ñuble'],
  ['magallanes', 'Magallanes'],
  ['aysen', 'Aysén'],
  ['tarapaca', 'Tarapacá'],
  ['antofagasta', 'Antofagasta'],
  ['coquimbo', 'Coquimbo'],
  ['maule', 'Maule'],
  ['los', 'Los'],
  ['rios', 'Ríos'],
  ['lagos', 'Lagos'],
  ['excel', 'Excel'],
  ['python', 'Python'],
  ['java', 'Java'],
  ['javascript', 'JavaScript'],
  ['typescript', 'TypeScript'],
  ['react', 'React'],
  ['node', 'Node'],
  ['aws', 'AWS'],
  ['power', 'Power'],
  ['office', 'Office']
]);

const CARGO_STOPWORDS = new Set(['de','del','la','las','el','los','y','e','en','para','con','por','al','a','o','u','sin']);
const CARGO_ACENTOS = new Map([
  ['tecnico', 'técnico'],
  ['tecnica', 'técnica'],
  ['administracion', 'administración'],
  ['organizacion', 'organización'],
  ['enfermeria', 'enfermería'],
  ['campana', 'campaña'],
  ['imagenologia', 'imagenología'],
  ['farmacia', 'farmacia'],
  ['gestion', 'gestión'],
  ['publica', 'pública'],
  ['analisis', 'análisis'],
  ['clinica', 'clínica'],
  ['logistica', 'logística'],
  ['academica', 'académica'],
  ['programacion', 'programación']
]);

function _capitalizarPrimera(s) {
  return s ? s.charAt(0).toLocaleUpperCase('es-CL') + s.slice(1) : '';
}

function _normalizarTokenCargo(token, idx) {
  const limpio = String(token || '');
  const base = limpio.toLocaleLowerCase('es-CL').normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (!base) return limpio;
  if (CARGO_SIGLAS_MAYUS.has(base)) return limpio.toUpperCase();
  if (CARGO_PROPIOS.has(base)) return CARGO_PROPIOS.get(base);

  let palabra = limpio.toLocaleLowerCase('es-CL');
  if (CARGO_ACENTOS.has(base)) palabra = CARGO_ACENTOS.get(base);
  if (idx > 0 && CARGO_STOPWORDS.has(base)) return palabra;
  return _capitalizarPrimera(palabra);
}

function _limpiarPrefijosCargo(txt) {
  let s = txt;

  // Elimina prefijos tipo código de concurso: 006AP-26, 011-26, COD-123, etc.
  s = s.replace(/^\s*(?:c[oó]d(?:igo)?[:\s-]*)?(?:[a-z]{0,4}\d{1,5}[a-z]{0,4}(?:[-/]\d{1,5}[a-z]{0,4})+|\d{1,5}[a-z]{0,4}(?:[-/]\d{1,5}[a-z]{0,4})+)\s+/i, '');
  s = s.replace(/^\s*\d{1,3}\s+(?=[a-záéíóúüñ])/i, '');

  // Elimina cantidades no informativas: "2 cargos", "1 vacante", etc.
  s = s.replace(/\b\d+\s*(?:cargos?|vacantes?|cupos?|plazas?)\b/gi, '');
  s = s.replace(/^\s*(?:cargos?|vacantes?|cupos?|plazas?)\b[:\s-]*/i, '');

  // Limpieza de separadores residuales.
  s = s.replace(/^[\s:;,.–—-]+/, '').replace(/\s{2,}/g, ' ').trim();
  return s;
}

function _aplicarAcronimosForzados(texto) {
  if (!texto) return texto;
  let out = texto.replace(CARGO_SIGLAS_PUNTEADAS, (m) => m.toUpperCase());
  out = out.replace(CARGO_ACRONIMOS_FORZADOS, (m) => m.toUpperCase());
  return out;
}

function _sanitizarPuntuacion(texto) {
  if (!texto) return texto;
  // "palabra ," o "palabra  ," → "palabra,"  (coma/punto/; antes debe ir
  // pegado a la palabra anterior, nunca con espacio que lo preceda).
  let out = texto.replace(/\s+([,;:!?])/g, '$1');
  // "palabra,palabra" → "palabra, palabra" (asegura espacio después).
  out = out.replace(/([,;:])(?=[^\s\d])/g, '$1 ');
  // Colapsa espacios múltiples residuales.
  out = out.replace(/[ \t]{2,}/g, ' ').trim();
  return out;
}

function normalizarTituloOferta(cargo) {
  const original = String(cargo || '');
  const sinPrefijos = _limpiarPrefijosCargo(original);
  if (!sinPrefijos) return '';

  const letras = sinPrefijos.replace(/[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]/g, '');
  const mayus = (letras.match(/[A-ZÁÉÍÓÚÜÑ]/g) || []).length;
  const minus = (letras.match(/[a-záéíóúüñ]/g) || []).length;
  const pareceGritado = mayus > 0 && (minus === 0 || mayus / Math.max(1, minus) > 3);

  let resultado;
  if (!pareceGritado) {
    resultado = _capitalizarPrimera(sinPrefijos.replace(/\s{2,}/g, ' ').trim());
  } else {
    const tokens = sinPrefijos.split(/(\s+|[-/(),.:;])/g).filter(Boolean);
    let idxPalabra = 0;
    const normalizado = tokens.map((t) => {
      if (/^\s+$/.test(t) || /^[-/(),.:;]$/.test(t)) return t;
      const token = _normalizarTokenCargo(t, idxPalabra);
      idxPalabra += 1;
      return token;
    }).join('');
    resultado = _capitalizarPrimera(normalizado.replace(/\s{2,}/g, ' ').trim());
  }

  resultado = _aplicarAcronimosForzados(resultado);
  resultado = _sanitizarPuntuacion(resultado);
  return resultado;
}

// Formato de texto libre (descripción / requisitos) a HTML estructurado.
// La lógica vive en rich-text.js y se expone como window.formatRichText.

function isValidHttpUrl(url) {
  if (!url || typeof url !== 'string') return false;
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

function isIsoDate(iso) {
  if (!iso || typeof iso !== 'string') return false;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return false;
  const d = new Date(`${iso}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === iso;
}

function jsonLdScript(payload) {
  return `<script type="application/ld+json">${JSON.stringify(payload).replace(/</g, '\\u003c')}<\/script>`;
}

function buildJobPostingJsonLd(oferta) {
  const titulo = (oferta?.cargo || '').trim();
  const institucion = (oferta?.institucion || '').trim();
  const fechaValida = isIsoDate(oferta?.fecha_cierre) ? oferta.fecha_cierre : null;
  const region = (oferta?.region || '').trim();
  const ciudad = ciudadValida(oferta?.ciudad || '', institucion).trim();
  const urlOficial = (oferta?.url_oferta || '').trim();

  const errores = [];
  if (!titulo) errores.push('cargo');
  if (!institucion) errores.push('institucion');
  if (!fechaValida) errores.push('fecha_cierre (YYYY-MM-DD)');
  if (!region && !ciudad) errores.push('ubicacion');
  if (!isValidHttpUrl(urlOficial)) errores.push('url_oferta');
  if (errores.length) return { valido: false, markup: '', errores };

  const location = {
    "@type": "Place",
    "address": {
      "@type": "PostalAddress",
      "addressCountry": "CL",
      ...(ciudad ? { "addressLocality": ciudad } : {}),
      ...(region ? { "addressRegion": region } : {})
    }
  };

  const payload = {
    "@context": "https://schema.org",
    "@type": "JobPosting",
    "title": titulo,
    "hiringOrganization": {
      "@type": "Organization",
      "name": institucion
    },
    "jobLocation": [location],
    "validThrough": `${fechaValida}T23:59:59-04:00`,
    "datePosted": isIsoDate(oferta?.fecha_publicacion) ? oferta.fecha_publicacion : fechaValida,
    "url": urlOficial
  };

  if (typeof oferta?.renta_bruta_min === 'number' && oferta.renta_bruta_min > 0) {
    payload.baseSalary = {
      "@type": "MonetaryAmount",
      "currency": "CLP",
      "value": {
        "@type": "QuantitativeValue",
        "value": oferta.renta_bruta_min,
        "unitText": "MONTH"
      }
    };
  }

  if (oferta?.jornada) payload.workHours = oferta.jornada;
  if (oferta?.tipo_contrato) payload.employmentType = oferta.tipo_contrato.toUpperCase();
  if (oferta?.descripcion) payload.description = oferta.descripcion;

  return { valido: true, markup: jsonLdScript(payload), errores: [] };
}

// ── Validación de URL (http/https, parseable, no vacía) ───────────────────
const esUrlValida = (typeof isValidHttpUrl === 'function')
  ? isValidHttpUrl
  : function (u) {
      if (!u || typeof u !== 'string') return false;
      const s = u.trim();
      if (!s || s === '#' || s.toLowerCase().startsWith('javascript:')) return false;
      try {
        const url = new URL(s);
        return url.protocol === 'http:' || url.protocol === 'https:';
      } catch {
        return false;
      }
    };

// ── Botón "Bases oficiales": consulta primero el flag del backend ──────────
// Si el backend marcó url_bases_valida === false → deshabilitar.
// Si viene null/ausente → degradación a esUrlValida (validación client-side).
function renderBtnBases(oferta) {
  const tieneBases = oferta.url_bases && oferta.url_bases !== oferta.url_oferta;
  if (!tieneBases) return '';
  const flag = oferta.url_bases_valida;
  const valida = (flag === false) ? false
              : (flag === true)  ? true
              : esUrlValida(oferta.url_bases);
  if (!valida) {
    return `<span class="btn-ver btn-ver-off" title="Enlace de bases no disponible" onclick="event.stopPropagation()">Bases no disponibles</span>`;
  }
  return `<button class="btn-ver" onclick="event.stopPropagation();abrirVisorBasesPorId(${Number(oferta.id) || 0})">Bases oficiales</button>`;
}

// ── ¿La oferta tiene una URL de postulación utilizable? ─────────────────
function ofertaPostulable(oferta) {
  const flag = oferta.url_oferta_valida;
  if (flag === false) return false;
  if (flag === true)  return true;
  return esUrlValida(oferta.url_oferta);
}

// ── Utilidades de formato ──────────────────────────────────────────────────
function formatRenta(min, max, grado) {
  if (min) {
    const minF = '$' + min.toLocaleString('es-CL');
    if (max && max !== min) return minF + ' – $' + max.toLocaleString('es-CL');
    return minF;
  }
  if (grado) return 'Grado ' + grado + ' EUS';
  return null;
}

// Versión HTML para la vista compacta: muestra min/max en dos líneas
// cuando hay rango, para evitar truncar valores largos en columnas angostas.
function formatRentaRow(min, max, grado) {
  if (min) {
    const minF = '$' + min.toLocaleString('es-CL');
    if (max && max !== min) {
      const maxF = '$' + max.toLocaleString('es-CL');
      return `<span class="renta-principal">${minF}</span><span class="renta-rango">hasta ${maxF}</span>`;
    }
    return `<span class="renta-principal">${minF}</span>`;
  }
  if (grado) return `<span class="renta-principal" style="font-size:11.5px">Grado ${grado} EUS</span>`;
  return null;
}

function formatFecha(iso) {
  if (!iso) return null;
  const d = new Date(iso + 'T00:00:00');
  return d.toLocaleDateString('es-CL', { day: 'numeric', month: 'long', year: 'numeric' });
}

// ── Días transcurridos desde una fecha ISO (YYYY-MM-DD o ISO datetime) ────
function diasDesdeHoy(iso) {
  if (!iso) return null;
  try {
    const fecha = new Date(String(iso).length <= 10 ? iso + 'T00:00:00' : iso);
    if (isNaN(fecha.getTime())) return null;
    const hoy = new Date();
    const msDia = 86400000;
    const d0 = Date.UTC(hoy.getFullYear(), hoy.getMonth(), hoy.getDate());
    const d1 = Date.UTC(fecha.getFullYear(), fecha.getMonth(), fecha.getDate());
    return Math.max(0, Math.round((d0 - d1) / msDia));
  } catch { return null; }
}

// ── Texto "hace X días" a partir de fecha_publicacion o fecha_scraped ─────
function frescuraTexto(oferta) {
  const iso = oferta?.fecha_publicacion || oferta?.fecha_scraped || null;
  const d = diasDesdeHoy(iso);
  if (d === null || d > 30) return null;
  if (d <= 2) return `✨ Nueva · hace ${d === 0 ? 'menos de 1 día' : d === 1 ? '1 día' : `${d} días`}`;
  return d === 1 ? 'Publicada hace 1 día' : `Publicada hace ${d} días`;
}

// ── Sugerencias populares para el estado vacío ────────────────────────────
const BUSQUEDAS_POPULARES = [
  { label: 'Psicólogo · RM',        q: 'psicólogo', region: 'Metropolitana de Santiago' },
  { label: 'Contador',              q: 'contador' },
  { label: 'Enfermero · Salud',     q: 'enfermero', sector: 'Salud Pública' },
  { label: 'Profesor',              q: 'profesor' },
  { label: 'Ingeniero · Ejecutivo', q: 'ingeniero', sector: 'Ejecutivo Central' },
];

function parseLocalDate(value) {
  if (!value) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y,m,d] = text.split('-').map(Number);
    return new Date(y, m - 1, d);
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
}

function getOfertaEstado(oferta) {
  if (!oferta) return { key: 'unknown', dias: null };
  const hoy = new Date();
  const hoyLocal = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate());
  const inicio = parseLocalDate(oferta.fecha_inicio);
  const cierre = parseLocalDate(oferta.fecha_cierre);
  if (inicio && inicio > hoyLocal) return { key: 'upcoming', dias: Math.round((inicio - hoyLocal) / 86400000) };
  if (cierre) {
    const dias = Math.round((cierre - hoyLocal) / 86400000);
    if (dias < 0) return { key: 'closed', dias };
    if (dias === 0) return { key: 'closing_today', dias };
    return { key: 'active', dias };
  }
  if (oferta.estado && ['closed','active','closing_today','upcoming'].includes(oferta.estado)) {
    return { key: oferta.estado, dias: oferta.dias_restantes ?? null };
  }
  return { key: 'active', dias: oferta.dias_restantes ?? null };
}

function plazoInfo(oferta) {
  const estadoOferta = getOfertaEstado(oferta);
  const dias = estadoOferta.dias;
  if (estadoOferta.key === 'closed') return { clase: 'status-closed', texto: 'Finalizada', color: 'var(--texto2)' };
  if (estadoOferta.key === 'closing_today') return { clase: 'status-closing-today', texto: 'Cierra hoy', color: 'var(--naran)' };
  if (estadoOferta.key === 'upcoming') return { clase: 'status-upcoming', texto: 'Próximamente', color: 'var(--cielo)' };
  if (dias === null || dias === undefined) return { clase: 'status-active', texto: 'Disponible', color: 'var(--verde)' };
  if (dias === 1) return { clase: 'status-active', texto: 'Cierra mañana', color: 'var(--verde)' };
  if (dias <= 5) return { clase: 'status-closing-today', texto: `Cierra en ${dias} ${dias === 1 ? 'día' : 'días'}`, color: 'var(--naran)' };
  return { clase: 'status-active', texto: `${dias} ${dias === 1 ? 'día disponible' : 'días disponibles'}`, color: 'var(--verde)' };
}

// ── Logos institucionales (dominio oficial + proveedores de respaldo) ─────
const PORTALES_EMPLEO = new Set([
  'empleospublicos.cl','trabajando.com','trabajando.cl','hiringroom.com',
  'buk.cl','chileatiende.cl','empleos.gob.cl','postulaciones.cl',
  'sistemadeconcursos.cl','mitrabajodigno.cl','ucampus.net',
]);

function esDominioPortal(dominio) {
  if (!dominio) return false;
  const d = dominio.toLowerCase().replace(/^www\./, '');
  for (const portal of PORTALES_EMPLEO) {
    if (d === portal || d.endsWith('.' + portal)) return true;
  }
  return false;
}

// Catálogo curado de fallback cuando el backend no provee institucion_sitio_web
// y no podemos inferir el dominio desde url_oferta (p. ej. oferta intermediada
// por empleospublicos.cl). La clave se compara contra el nombre normalizado.
const DOMINIOS_INSTITUCIONALES_REFERENCIA = [
  ['ministerio de salud', 'minsal.cl'],
  ['ministerio de educacion', 'mineduc.cl'],
  ['ministerio de hacienda', 'hacienda.cl'],
  ['ministerio del interior', 'interior.gob.cl'],
  ['ministerio de relaciones exteriores', 'minrel.gob.cl'],
  ['ministerio de defensa', 'defensa.cl'],
  ['ministerio de justicia', 'minjusticia.gob.cl'],
  ['ministerio de obras publicas', 'mop.gob.cl'],
  ['ministerio de vivienda', 'minvu.cl'],
  ['ministerio de transportes', 'mtt.gob.cl'],
  ['ministerio de agricultura', 'minagri.gob.cl'],
  ['ministerio de mineria', 'minmineria.cl'],
  ['ministerio de energia', 'energia.gob.cl'],
  ['ministerio de medio ambiente', 'mma.gob.cl'],
  ['ministerio de economia', 'economia.gob.cl'],
  ['ministerio de desarrollo social', 'desarrollosocialyfamilia.gob.cl'],
  ['ministerio de la mujer', 'minmujeryeg.gob.cl'],
  ['ministerio de cultura', 'cultura.gob.cl'],
  ['ministerio del deporte', 'mindep.cl'],
  ['ministerio de ciencia', 'minciencia.gob.cl'],
  ['ministerio del trabajo', 'mintrab.gob.cl'],
  ['servicio de salud metropolitano', 'redsalud.gob.cl'],
  ['subsecretaria de salud', 'minsal.cl'],
  ['poder judicial', 'pjud.cl'],
  ['fiscalia', 'fiscaliadechile.cl'],
  ['contraloria general de la republica', 'contraloria.cl'],
  ['superintendencia de pensiones', 'spensiones.cl'],
  ['superintendencia de salud', 'supersalud.gob.cl'],
  ['superintendencia de educacion', 'supereduc.cl'],
  ['superintendencia de valores', 'cmfchile.cl'],
  ['comision para el mercado financiero', 'cmfchile.cl'],
  ['servicio de impuestos internos', 'sii.cl'],
  ['tesoreria general de la republica', 'tesoreria.cl'],
  ['direccion del trabajo', 'dt.gob.cl'],
  ['direccion de compras', 'chilecompra.cl'],
  ['junji', 'junji.cl'],
  ['fundacion integra', 'integra.cl'],
  ['junta nacional de jardines', 'junji.cl'],
  ['junta nacional de auxilio escolar', 'junaeb.cl'],
  ['carabineros de chile', 'carabineros.cl'],
  ['policia de investigaciones', 'investigaciones.cl'],
  ['gendarmeria de chile', 'gendarmeria.gob.cl'],
  ['servicio nacional de aduanas', 'aduana.cl'],
  ['servicio agricola y ganadero', 'sag.gob.cl'],
  ['corporacion nacional forestal', 'conaf.cl'],
  ['instituto nacional de estadisticas', 'ine.cl'],
  ['instituto de previsión social', 'ips.gob.cl'],
  ['instituto de seguridad laboral', 'isl.gob.cl'],
  ['sercotec', 'sercotec.cl'],
  ['corfo', 'corfo.cl'],
  ['sence', 'sence.cl'],
  ['conadi', 'conadi.gob.cl'],
  ['sernac', 'sernac.cl'],
  ['sernapesca', 'sernapesca.cl'],
  ['sernatur', 'sernatur.cl'],
  ['sernameg', 'sernameg.gob.cl'],
  ['dipres', 'dipres.gob.cl'],
  ['cgr', 'contraloria.cl'],
  ['municipalidad de santiago', 'municipalidaddesantiago.cl'],
  ['municipalidad de puente alto', 'mpuentealto.cl'],
  ['municipalidad de maipu', 'maipu.cl'],
  ['municipalidad de la florida', 'laflorida.cl'],
  ['municipalidad de las condes', 'lascondes.cl'],
  ['municipalidad de providencia', 'providencia.cl'],
  ['municipalidad de vitacura', 'vitacura.cl'],
  ['municipalidad de nunoa', 'nunoa.cl'],
  ['municipalidad de la reina', 'lareina.cl'],
  ['municipalidad de penalolen', 'penalolen.cl'],
  ['municipalidad de san bernardo', 'sanbernardo.cl'],
  ['municipalidad de concepcion', 'concepcion.cl'],
  ['municipalidad de valparaiso', 'munivalparaiso.cl'],
  ['municipalidad de vina del mar', 'munivina.cl'],
  ['municipalidad de temuco', 'temuco.cl'],
  ['municipalidad de antofagasta', 'municipalidadantofagasta.cl'],
  ['municipalidad de la serena', 'laserena.cl'],
  ['municipalidad de iquique', 'municipioiquique.cl'],
  ['municipalidad de arica', 'municipalidadarica.cl'],
  ['hospital clinico', 'redsalud.gob.cl'],
];

// Cache de resolución para evitar recalcular en cada renderCard.
const _LOGO_CACHE = new Map();

// `imgFavFallback` y `imgFavCheckQuality` vienen de shared-shell.js, que los
// registra sincrónicamente antes del primer render. Evitamos declararlos
// aquí (antes había una versión inline que se quedaba en emoji al fallar la
// primera fuente) para que la cadena robusta siempre gane.

function dominioDesdeUrl(url) {
  if (!url) return '';
  try {
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
}

function dominioInstitucionalPorNombre(nombreInstitucion) {
  const key = comunaNormalizada(nombreInstitucion || '');
  if (!key) return '';
  // Preferimos la coincidencia más larga para evitar falsos positivos cortos
  // (p. ej. "ministerio" choca con muchos, pero "ministerio de salud" es preciso).
  let best = null;
  for (const [needle, domain] of DOMINIOS_INSTITUCIONALES_REFERENCIA) {
    if (key.includes(needle) && (!best || needle.length > best.needle.length)) {
      best = { needle, domain };
    }
  }
  return best ? best.domain : '';
}

function normalizarDominio(dominio) {
  return (dominio || '').trim().toLowerCase().replace(/^www\./, '');
}

function resolverDominioInstitucional(oferta) {
  const cacheKey = [
    oferta?.institucion_id || '',
    oferta?.institucion || '',
    oferta?.institucion_sitio_web || '',
    oferta?.institucion_url_empleo || '',
    oferta?.url_oferta || '',
  ].join('|');
  if (_LOGO_CACHE.has(cacheKey)) return _LOGO_CACHE.get(cacheKey);

  // 1. Sitio web oficial expuesto por la API desde el catálogo maestro.
  //    Es la fuente más confiable: nunca resuelve al portal intermediario.
  const sitioApi = normalizarDominio(dominioDesdeUrl(oferta?.institucion_sitio_web || '') || oferta?.institucion_sitio_web || '');
  if (sitioApi && !esDominioPortal(sitioApi)) {
    const res = { domain: sitioApi, confiable: true, fuente: 'Sitio web oficial declarado en catálogo institucional' };
    _LOGO_CACHE.set(cacheKey, res);
    return res;
  }

  // 2. url_empleo declarada en la tabla instituciones: aceptable sólo si NO
  //    es un portal intermediario (empleospublicos.cl, trabajando, buk, etc.).
  const oficial = normalizarDominio(dominioDesdeUrl(oferta?.institucion_url_empleo || ''));
  if (oficial && !esDominioPortal(oficial)) {
    const res = { domain: oficial, confiable: true, fuente: 'Dominio oficial declarado por la institución' };
    _LOGO_CACHE.set(cacheKey, res);
    return res;
  }

  // 3. Dominio directo de la URL de la oferta: confiable si la institución
  //    publica en su propio dominio y no pasa por un portal.
  const dominioOferta = normalizarDominio(dominioDesdeUrl(oferta?.url_oferta || ''));
  if (dominioOferta && !esDominioPortal(dominioOferta)) {
    const res = { domain: dominioOferta, confiable: true, fuente: 'Dominio directo de la oferta institucional' };
    _LOGO_CACHE.set(cacheKey, res);
    return res;
  }

  // 4. Catálogo curado por nombre normalizado (fallback robusto para ofertas
  //    intermediadas cuando el backend no trajo institucion_sitio_web).
  const porNombre = dominioInstitucionalPorNombre(oferta?.institucion || '');
  if (porNombre) {
    const res = { domain: porNombre, confiable: true, fuente: 'Dominio institucional sugerido por catálogo curado' };
    _LOGO_CACHE.set(cacheKey, res);
    return res;
  }

  // 5. Fallback visual neutro (sin logo): mejor mostrar ícono de sector que
  //    usar el logo de un portal intermediario. NUNCA devolvemos el dominio
  //    del portal como si fuera la institución.
  const res = { domain: '', confiable: false, fuente: 'Sin dominio institucional verificable' };
  _LOGO_CACHE.set(cacheKey, res);
  return res;
}

// SVGs compactos por sector, sincronizados con shared-shell.js.
// Los usamos cuando no hay dominio verificable (evita el flash de la img
// que se reemplaza) o cuando queremos renderizar el placeholder desde JS
// sin depender del onerror.
const SECTOR_SVG_PATHS = {
  municipal:   '<path d="M12 2 L3 7 h18 Z M4 9 v10 M8 9 v10 M12 9 v10 M16 9 v10 M20 9 v10 M3 21 h18"/>',
  salud:       '<path d="M9 3 h6 v6 h6 v6 h-6 v6 h-6 v-6 H3 V9 h6 Z"/>',
  educacion:   '<path d="M2 9 L12 4 L22 9 L12 14 Z M6 11 V17 c0 1 3 3 6 3 s6-2 6-3 V11"/>',
  ejecutivo:   '<path d="M4 20 V8 l8-4 l8 4 V20 Z M9 20 V14 h6 v6 M9 9 h2 M13 9 h2 M9 12 h2 M13 12 h2"/>',
  judicial:    '<path d="M12 3 V21 M6 6 h12 M5 9 h4 M15 9 h4 M4 14 c0-2 2-3 3-3 s3 1 3 3 M14 14 c0-2 2-3 3-3 s3 1 3 3"/>',
  ffaa:        '<path d="M12 2 L4 6 V12 c0 5 4 8 8 10 c4-2 8-5 8-10 V6 Z"/>',
  empresa:     '<path d="M3 7 h18 v13 H3 Z M8 7 V4 h8 v3 M3 11 h18 M10 15 h4"/>',
  regional:    '<path d="M3 6 L9 3 L15 6 L21 3 V18 L15 21 L9 18 L3 21 Z M9 3 V18 M15 6 V21"/>',
  universidad: '<path d="M2 9 L12 4 L22 9 L12 14 Z M6 11 V17 c0 1 3 3 6 3 s6-2 6-3 V11 M19 9 V14"/>',
  default:     '<path d="M4 20 V9 l8-5 l8 5 V20 Z M9 20 V14 h6 v6"/>'
};

function sectorDeNombre(nombre) {
  const n = (nombre || '').toLowerCase();
  if (/municipalidad|municipal|muni\./.test(n)) return 'municipal';
  if (/hospital|salud|clinic|consultori|cesfam|servicio\s+de\s+salud/.test(n)) return 'salud';
  if (/universidad|instituto\s+profesional|centro\s+de\s+formaci/.test(n)) return 'universidad';
  if (/colegio|escuela|liceo|educaci/.test(n)) return 'educacion';
  if (/poder\s+judicial|juzgado|corte|fiscal|tribunal/.test(n)) return 'judicial';
  if (/fuerzas|armada|ej[ée]rcito|carabineros|pdi|gendarmer|bomberos/.test(n)) return 'ffaa';
  if (/gobierno\s+regional|intendencia|gore/.test(n)) return 'regional';
  if (/empresa|banco|metro|tvn|codelco|enap|enami|correos/.test(n)) return 'empresa';
  if (/ministerio|subsecretar|superintendencia|servicio\s+de|direcci[óo]n\s+general/.test(n)) return 'ejecutivo';
  return 'default';
}

function sectorIconHtml(nombreInstitucion) {
  const sec = sectorDeNombre(nombreInstitucion);
  const paths = SECTOR_SVG_PATHS[sec] || SECTOR_SVG_PATHS.default;
  return `<div class="sector-icon-fallback sector-icon-fallback--${sec}" data-sector="${sec}" aria-hidden="true"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="100%" height="100%" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">${paths}</svg></div>`;
}

function getInstIcon(oferta) {
  const resolved = resolverDominioInstitucional(oferta);
  const institucionNombre = oferta?.institucion || 'institución';
  // Sin dominio verificable → renderizamos el ícono SVG del sector
  // directamente (no dependemos del flujo onerror del <img>).
  if (!resolved.domain) {
    return {
      html: sectorIconHtml(institucionNombre),
      confiable: false,
      fuente: resolved.fuente,
    };
  }
  const dom = escAttr(resolved.domain);
  // size=256 → Clearbit devuelve PNG de 256 px, nítido al escalar a 44-128 px.
  // data-attempt=0 inicializa la cadena: si onerror dispara o imgFavCheckQuality
  // detecta naturalWidth<40, advance() pasa a apple-touch-icon → Google → etc.
  const primary = `https://logo.clearbit.com/${dom}?size=256`;
  return {
    html: `<img src="${primary}" data-attempt="0" alt="Logo de ${escAttr(institucionNombre)}" loading="lazy" decoding="async" onerror="imgFavFallback(this)" onload="imgFavCheckQuality(this)">`,
    confiable: Boolean(resolved.confiable),
    fuente: resolved.fuente,
  };
}

// Evita mostrar la ciudad cuando coincide con el nombre de la institución
function ciudadValida(ciudad, institucion) {
  if (!ciudad) return '';
  const a = ciudad.toLowerCase().trim();
  const b = (institucion || '').toLowerCase().trim();
  if (a === b || b.includes(a) || a.includes(b)) return '';
  return ciudad;
}

// Filtra jornadas inválidas como "0 horas" o "00 horas semanales"
function jornadaValida(jornada) {
  if (!jornada) return null;
  if (/^\s*0+\s*horas?/i.test(jornada)) return null;
  return jornada;
}

const REGIONES_ALIAS = {
  'RM': 'Región Metropolitana',
  'METROPOLITANA': 'Región Metropolitana',
  'METROPOLITANA DE SANTIAGO': 'Región Metropolitana',
  'MAULE': 'Región del Maule',
  'BIOBIO': 'Región del Biobío',
  'BIOBÍO': 'Región del Biobío',
  'NUBLE': 'Región de Ñuble',
  'ÑUBLE': 'Región de Ñuble',
  'OHIGGINS': "Región del Libertador General Bernardo O'Higgins",
  "O'HIGGINS": "Región del Libertador General Bernardo O'Higgins",
  'OHÍGGINS': "Región del Libertador General Bernardo O'Higgins",
  'ARICA Y PARINACOTA': 'Región de Arica y Parinacota',
  'ATACAMA': 'Región de Atacama',
  'AYSEN': 'Región de Aysén del General Carlos Ibáñez del Campo',
  'AYSÉN': 'Región de Aysén del General Carlos Ibáñez del Campo',
  'ANTOFAGASTA': 'Región de Antofagasta',
  'COQUIMBO': 'Región de Coquimbo',
  'LA ARAUCANIA': 'Región de La Araucanía',
  'LA ARAUCANÍA': 'Región de La Araucanía',
  'LOS RIOS': 'Región de Los Ríos',
  'LOS RÍOS': 'Región de Los Ríos',
  'LOS LAGOS': 'Región de Los Lagos',
  'TARAPACA': 'Región de Tarapacá',
  'TARAPACÁ': 'Región de Tarapacá',
  'VALPARAISO': 'Región de Valparaíso',
  'VALPARAÍSO': 'Región de Valparaíso',
  'MAGALLANES': 'Región de Magallanes y de la Antártica Chilena',
};

function normalizarRegion(region) {
  return (region || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\./g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();
}

function nombreRegionCompleto(region) {
  if (!region) return '';
  const limpio = region.trim();
  if (!limpio) return '';
  const key = normalizarRegion(limpio);
  if (REGIONES_ALIAS[key]) return REGIONES_ALIAS[key];
  if (/^REGION\s+/i.test(limpio) || /^REGIÓN\s+/i.test(limpio)) return limpio;
  return `Región de ${limpio}`;
}

// ── Renderizar tarjeta de oferta ───────────────────────────────────────────
function renderCard(oferta) {
  const cargoDisplay = normalizarTituloOferta(oferta.cargo);
  const renta = formatRenta(oferta.renta_bruta_min, oferta.renta_bruta_max, oferta.grado_eus);
  const plazo = plazoInfo(oferta);
  const tipoCss = tipoClase(oferta.tipo_contrato);
  const tipoLabel = tipoEtiqueta(oferta.tipo_contrato);
  const regionCompleta = nombreRegionCompleto(oferta.region);
  const sector  = oferta.sector || '';
  const ciudad  = ciudadValida(oferta.ciudad, oferta.institucion);
  const jornada = jornadaValida(oferta.jornada);
  const frescura = frescuraTexto(oferta);
  const instLogo = getInstIcon(oferta);

  const favs = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]');
  const esFav = favs.some(f => f.id === oferta.id);
  const jobPosting = buildJobPostingJsonLd(oferta);
  if (!jobPosting.valido) {
    console.debug('[SEO] JobPosting omitido en card por campos faltantes:', jobPosting.errores, oferta?.id);
  }

  return `
  <div class="oferta-card${esFav ? ' favorita' : ''}" data-oferta-id="${oferta.id}" role="button" tabindex="0" aria-label="Ver detalle: ${escAttr(cargoDisplay)}">
    <button class="btn-fav-card${esFav ? ' activo' : ''}"
      data-id="${oferta.id}"
      data-cargo="${escAttr(cargoDisplay)}"
      data-inst="${escAttr(oferta.institucion)}"
      data-region="${escAttr(oferta.region || '')}"
      data-cierre="${escAttr(oferta.fecha_cierre || '')}"
      data-url="${escAttr(oferta.url_oferta || '')}"
      onclick="event.stopPropagation();toggleFavCard(this)"
      title="${esFav ? 'Quitar de favoritos' : 'Guardar como favorito'}"
    >${esFav ? '♥' : '♡'}</button>
    <div class="oferta-header">
      <div class="oferta-logo${instLogo.confiable ? ' oferta-logo--verificada' : ''}" title="${escAttr(instLogo.fuente)}">${instLogo.html}</div>
      <div class="oferta-meta">
        <div class="oferta-institucion">${escHtml(_aplicarAcronimosForzados(oferta.institucion || '')) || 'Institución pública'}</div>
        <div class="oferta-cargo"><span class="oferta-cargo-link">${escHtml(cargoDisplay)}</span></div>
        <div class="oferta-tipo-wrap">
          ${oferta.tipo_contrato ? `<span class="badge ${tipoCss}">${tipoLabel}</span>` : ''}
          ${regionCompleta ? `<span class="badge badge-region">🗺 ${escHtml(regionCompleta)}</span>` : ''}
          ${sector ? `<span class="badge badge-sector">${escHtml(sector)}</span>` : ''}
        </div>
      </div>
    </div>
    <div class="oferta-detalles">
      ${renta ? `<span class="oferta-renta">${renta}</span>` : '<span class="oferta-renta oferta-renta--muted">Renta no informada</span>'}
      ${ciudad ? `<span class="oferta-detalle">
        <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="12" cy="10" r="3"/><path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z"/></svg>
        ${escHtml(ciudad)}
      </span>` : ''}
      ${jornada ? `<span class="oferta-detalle">⏱ ${escHtml(jornada)}</span>` : ''}
      ${oferta.fecha_publicacion ? `<span class="oferta-detalle">🗓 Publicada ${formatFecha(oferta.fecha_publicacion)}</span>` : ''}
      ${frescura ? `<span class="oferta-frescura${frescura.startsWith('✨') ? ' frescura-nueva' : ''}">${escHtml(frescura)}</span>` : ''}
    </div>
    <div class="oferta-footer">
      <div class="oferta-plazo">
        <div class="plazo-dot ${plazo.clase}"></div>
        <span style="color:${plazo.color}">${plazo.texto}</span>
        ${oferta.fecha_cierre ? `<span style="color:var(--texto3);margin-left:4px">· ${formatFecha(oferta.fecha_cierre)}</span>` : ''}
      </div>
      <div class="oferta-acciones">
        <button class="btn-detalle" onclick="event.stopPropagation();abrirModal(${Number(oferta.id) || 0})">Ver detalle</button>
      </div>
    </div>
    ${jobPosting.markup}
  </div>`;
}

// ── Favorito directo desde tarjeta ────────────────────────────────────────
function toggleFavCard(btn) {
  const KEY = 'fav_contrataoplanta';
  const id  = parseInt(btn.dataset.id);
  let favs  = JSON.parse(localStorage.getItem(KEY) || '[]');
  const idx = favs.findIndex(f => f.id === id);
  const card = btn.closest('.oferta-card, .oferta-row');

  if (idx >= 0) {
    favs.splice(idx, 1);
    btn.textContent = '♡';
    btn.classList.remove('activo');
    btn.title = 'Guardar como favorito';
    card.classList.remove('favorita');
  } else {
    favs.push({
      id,
      cargo:       btn.dataset.cargo,
      institucion: btn.dataset.inst,
      region:      btn.dataset.region,
      fecha_cierre: btn.dataset.cierre || null,
      url_oferta:  btn.dataset.url,
      guardado_en: new Date().toISOString(),
    });
    btn.textContent = '♥';
    btn.classList.add('activo');
    btn.title = 'Quitar de favoritos';
    card.classList.add('favorita');
  }
  localStorage.setItem(KEY, JSON.stringify(favs));
  actualizarNavFavs();
}

// ── Vista compacta (fila densa) ────────────────────────────────────────────
function renderRowCompacta(oferta) {
  const cargoDisplay = normalizarTituloOferta(oferta.cargo);
  const plazo     = plazoInfo(oferta);
  const tipoCss   = tipoClase(oferta.tipo_contrato);
  const tipoLabel = tipoEtiqueta(oferta.tipo_contrato);
  const regionCompleta = nombreRegionCompleto(oferta.region);
  const rentaHtml = formatRentaRow(oferta.renta_bruta_min, oferta.renta_bruta_max, oferta.grado_eus);
  const favs  = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]');
  const esFav = favs.some(f => f.id === oferta.id);
  const jobPosting = buildJobPostingJsonLd(oferta);
  if (!jobPosting.valido) {
    console.debug('[SEO] JobPosting omitido en row por campos faltantes:', jobPosting.errores, oferta?.id);
  }
  return `
  <div class="oferta-row${esFav ? ' favorita' : ''}" data-oferta-id="${oferta.id}" role="button" tabindex="0" aria-label="Ver detalle: ${escAttr(cargoDisplay)}">
    <div class="row-main">
      <div class="row-inst">${escHtml(_aplicarAcronimosForzados(oferta.institucion || '')) || 'Institución pública'}</div>
      <div class="row-cargo" title="${escAttr(cargoDisplay)}">${escHtml(cargoDisplay)}</div>
      ${regionCompleta ? `<div class="row-region">🗺 ${escHtml(regionCompleta)}</div>` : ''}
    </div>
    <div class="row-meta">
      ${tipoLabel ? `<span class="badge ${tipoCss}" style="font-size:10px;white-space:nowrap">${tipoLabel}</span>` : '<span style="color:var(--texto3);font-size:11px">—</span>'}
    </div>
    <div class="row-plazo">
      <div class="plazo-dot ${plazo.clase}"></div>
      <span style="color:${plazo.color}">${plazo.texto}</span>
    </div>
    <div class="row-renta">${rentaHtml || '<span style="color:var(--texto3)">—</span>'}</div>
    <button class="btn-fav-row${esFav ? ' activo' : ''}"
      data-id="${oferta.id}"
      data-cargo="${escAttr(cargoDisplay)}"
      data-inst="${escAttr(oferta.institucion)}"
      data-region="${escAttr(oferta.region || '')}"
      data-cierre="${escAttr(oferta.fecha_cierre || '')}"
      data-url="${escAttr(oferta.url_oferta || '')}"
      onclick="event.stopPropagation();toggleFavCard(this)"
      title="${esFav ? 'Quitar de favoritos' : 'Guardar como favorito'}"
    >${esFav ? '♥' : '♡'}</button>
    ${jobPosting.markup}
  </div>`;
}

// ── Dispatch según vista activa ────────────────────────────────────────────
function renderItem(oferta) {
  if (estado.vista === 'compacta') return renderRowCompacta(oferta);
  return renderCard(oferta); // cards y grid usan la misma tarjeta (grid es CSS)
}

// ── Cabecera ordenable para vista compacta ────────────────────────────────
function renderHeaderCompacta() {
  const cols = [
    { label: 'Institución / Cargo', orden: 'az',        flex: true },
    { label: 'Tipo',                  orden: null,         flex: false },
    { label: 'Plazo',               orden: 'cierre',     flex: false },
    { label: 'Renta',               orden: 'renta_desc', flex: false, toggle: 'renta_asc' },
  ];
  const cells = cols.map(c => {
    if (!c.orden) {
      return `<span class="col-sort-label">${c.label}</span>`;
    }
    // Si el orden activo coincide con esta columna (o su toggle)
    const esActivo = estado.orden === c.orden || (c.toggle && estado.orden === c.toggle);
    const nextOrden = esActivo && c.toggle && estado.orden === c.orden ? c.toggle : c.orden;
    const arrow = !esActivo ? '↕' : (estado.orden === c.toggle ? '↑' : '↓');
    return `<span class="col-sort${esActivo ? ' activo' : ''}" onclick="setOrdenDesdeHeader('${nextOrden}')">
      ${c.label} <span class="sort-ico">${arrow}</span>
    </span>`;
  });
  return `<div class="oferta-row-header">
    ${cells[0]}${cells[1]}${cells[2]}${cells[3]}
    <span></span>
  </div>`;
}

function setOrdenDesdeHeader(orden) {
  estado.orden  = orden;
  estado.pagina = 1;
  guardarPrefs({ orden, orden_personalizado: true });
  // Sincronizar el select de orden
  const sel = document.getElementById('ctrl-orden');
  if (sel) sel.value = orden;
  cargarOfertas();
}

// ── Esqueletos de carga (3 tarjetas grises) ────────────────────────────────
function renderEsqueletos() {
  const lista = document.getElementById('lista-ofertas');
  // Resetear a vista cards durante carga para los esqueletos
  lista.className = 'ofertas-lista' + (estado.vista === 'grid' ? ' cuadricula' : '');
  lista.innerHTML = Array(3).fill(`
    <div class="oferta-card" style="pointer-events:none">
      <div class="oferta-header">
        <div class="oferta-logo skeleton" style="width:44px;height:44px;border-radius:10px"></div>
        <div class="oferta-meta" style="flex:1">
          <div class="skeleton skeleton-line small" style="margin-bottom:6px"></div>
          <div class="skeleton skeleton-line big"></div>
          <div class="skeleton skeleton-line mid" style="margin-top:6px"></div>
        </div>
      </div>
      <div style="margin:12px 0">
        <div class="skeleton skeleton-line mid"></div>
      </div>
      <div class="oferta-footer" style="border-top:1px solid var(--bg2);padding-top:12px">
        <div class="skeleton skeleton-line small"></div>
        <div class="skeleton" style="width:80px;height:30px;border-radius:7px"></div>
      </div>
    </div>`).join('');
}

// ── Estado vacío / error ──────────────────────────────────────────────────
function renderVacio() {
  const chips = BUSQUEDAS_POPULARES.map((s, i) => `
    <button type="button" class="sugerencia-chip" data-idx="${i}">${escHtml(s.label)}</button>
  `).join('');
  document.getElementById('lista-ofertas').innerHTML = `
    <div class="estado-vacio">
      🔍 No se encontraron ofertas con los filtros seleccionados.
      <p class="estado-mensaje">Prueba con otros términos o quita algunos filtros.</p>
      <div class="estado-sugerencias">
        <div class="estado-sugerencias-titulo">Búsquedas populares</div>
        <div class="estado-sugerencias-chips" id="sugerencias-chips">${chips}</div>
      </div>
      <button type="button" class="estado-alerta-cta" id="btn-estado-alerta">
        🔔 Activar alerta con estos filtros
      </button>
    </div>`;
  document.getElementById('paginacion').innerHTML = '';
  // Enlazar listeners
  document.getElementById('sugerencias-chips')?.addEventListener('click', (e) => {
    const btn = e.target.closest('.sugerencia-chip');
    if (!btn) return;
    aplicarSugerencia(BUSQUEDAS_POPULARES[parseInt(btn.dataset.idx, 10)]);
  });
  document.getElementById('btn-estado-alerta')?.addEventListener('click', prellenarAlertaDesdeFiltros);
}

function aplicarSugerencia(sug) {
  if (!sug) return;
  estado.q        = sug.q || '';
  estado.region   = sug.region || '';
  estado.sector   = sug.sector || '';
  estado.pagina   = 1;
  const inputQ = document.getElementById('input-cargo');
  if (inputQ) inputQ.value = estado.q;
  const selRegion = document.getElementById('filtro-region');
  if (selRegion && estado.region) selRegion.value = estado.region;
  const selSector = document.getElementById('filtro-sector');
  if (selSector && estado.sector) selSector.value = estado.sector;
  cargarOfertas();
}

function prellenarAlertaDesdeFiltros() {
  const setVal = (id, val) => { const el = document.getElementById(id); if (el && val != null) el.value = val; };
  setVal('alerta-keywords', estado.q || '');
  setVal('alerta-region',   estado.region || '');
  setVal('alerta-sector',   estado.sector || '');
  // Si el usuario tiene un solo tipo de contrato activo, pre-seleccionarlo
  if (Array.isArray(estado.tipos) && estado.tipos.length === 1) {
    setVal('alerta-tipo', estado.tipos[0]);
  }
  const widget = document.getElementById('alertas');
  if (widget) widget.scrollIntoView({ behavior: 'smooth', block: 'start' });
  setTimeout(() => document.getElementById('alerta-email')?.focus(), 450);
}

let _reintentoTimer = null;
let _reintentoIntervalo = null;
function _resumenErrorConexion(err) {
  if (!err) return 'No recibimos respuesta desde ninguno de los endpoints configurados.';
  const detallePrincipal = err.message || String(err);
  const intentos = Array.isArray(err.detalles) ? err.detalles : [];
  if (!intentos.length) return detallePrincipal;
  const ultimo = intentos[intentos.length - 1];
  return `${detallePrincipal}. Último intento: ${ultimo.url}`;
}

function _tipoError(err) {
  const detalle = String(err?.message || err || '').toLowerCase();
  if (detalle.includes('timeout')) return 'timeout';
  if (detalle.includes('failed to fetch')) return 'network';
  if (detalle.includes('http 5')) return 'server';
  if (detalle.includes('http 4')) return 'client';
  return 'unknown';
}

function renderError(err = null) {
  const tipo = _tipoError(err);
  const detalle = _resumenErrorConexion(err);
  const mensaje = (
    tipo === 'timeout' ? 'El backend tardó demasiado en responder.' :
    tipo === 'network' ? 'No logramos conectarnos al backend (red, DNS o servicio caído).' :
    tipo === 'server' ? 'El backend respondió con error interno (5xx).' :
    tipo === 'client' ? 'La solicitud fue rechazada por el backend (4xx).' :
    'No se pudo completar la solicitud al backend.'
  );
  const quedanReintentos = _reintentosConsecutivos < MAX_REINTENTOS_AUTOMATICOS;
  document.getElementById('lista-ofertas').innerHTML = `
    <div class="estado-error">
      ⚠️ No pudimos cargar ofertas desde el backend.
      <p class="estado-mensaje">${mensaje}</p>
      <p class="estado-mensaje" id="msg-reintento">${quedanReintentos ? 'Reintentando en 30 segundos...' : 'Se detuvieron los reintentos automáticos.'}</p>
      <p class="estado-mensaje">${detalle}</p>
      <button type="button" class="estado-alerta-cta" id="btn-reintento-manual">Reintentar ahora</button>
    </div>`;
  document.getElementById('paginacion').innerHTML = '';
  document.getElementById('btn-reintento-manual')?.addEventListener('click', () => {
    _reintentosConsecutivos = 0;
    cargarOfertas();
  });

  clearTimeout(_reintentoTimer);
  if (_reintentoIntervalo) clearInterval(_reintentoIntervalo);

  if (!quedanReintentos) return;
  let segs = 30;
  _reintentoIntervalo = setInterval(() => {
    segs--;
    const msg = document.getElementById('msg-reintento');
    if (msg) msg.textContent = `Reintentando en ${segs} segundos...`;
    if (segs <= 0 && _reintentoIntervalo) clearInterval(_reintentoIntervalo);
  }, 1000);
  _reintentoTimer = setTimeout(() => {
    _reintentosConsecutivos += 1;
    cargarOfertas();
  }, 30000);
}

// ── Paginación dinámica ────────────────────────────────────────────────────
let _pagTotal = 0, _pagPaginas = 0; // caché para re-renderizar desde abrirInputPagina

function renderPaginacion(total, paginas) {
  _pagTotal = total; _pagPaginas = paginas;
  const cont = document.getElementById('paginacion');
  if (paginas <= 1) { cont.innerHTML = ''; return; }

  const p = estado.pagina;
  let html = '';

  // Botón anterior
  html += `<button class="pag-btn" ${p <= 1 ? 'disabled' : ''} onclick="irPagina(${p-1})">‹</button>`;

  // Páginas (ventana de 5 alrededor de la actual)
  const desde = Math.max(1, p - 2);
  const hasta = Math.min(paginas, p + 2);
  if (desde > 1) { html += `<button class="pag-btn" onclick="irPagina(1)">1</button>`; }
  if (desde > 2) {
    const mid = Math.round((1 + desde) / 2);
    html += `<button class="pag-btn pag-ellipsis" onclick="abrirInputPagina(this,${mid},${paginas})" title="Ir a página…">…</button>`;
  }
  for (let i = desde; i <= hasta; i++) {
    html += `<button class="pag-btn ${i === p ? 'activo' : ''}" onclick="irPagina(${i})">${i}</button>`;
  }
  if (hasta < paginas - 1) {
    const mid = Math.round((hasta + paginas) / 2);
    html += `<button class="pag-btn pag-ellipsis" onclick="abrirInputPagina(this,${mid},${paginas})" title="Ir a página…">…</button>`;
  }
  if (hasta < paginas) { html += `<button class="pag-btn" onclick="irPagina(${paginas})">${paginas}</button>`; }

  // Botón siguiente
  html += `<button class="pag-btn" ${p >= paginas ? 'disabled' : ''} onclick="irPagina(${p+1})">›</button>`;

  cont.innerHTML = html;
}

function abrirInputPagina(btn, mid, max) {
  const inp = document.createElement('input');
  inp.type = 'number';
  inp.min = 1;
  inp.max = max;
  inp.value = mid;
  inp.className = 'pag-input';
  btn.replaceWith(inp);
  inp.focus();
  inp.select();
  let navegado = false;
  inp.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      const n = Math.min(max, Math.max(1, parseInt(inp.value) || 1));
      navegado = true;
      irPagina(n);
    }
    if (e.key === 'Escape') {
      renderPaginacion(_pagTotal, _pagPaginas);
    }
  });
  inp.addEventListener('blur', () => {
    if (!navegado) setTimeout(() => renderPaginacion(_pagTotal, _pagPaginas), 120);
  });
}

function irPagina(n) {
  estado.pagina = n;
  cargarOfertas();
  document.getElementById('lista-ofertas').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function setVistaListado(vista) {
  estado.vista_listado = vista;
  estado.pagina = 1;
  document.getElementById('estado-listado-tabs')?.setAttribute('data-estado', vista);
  document.getElementById('tab-vigentes')?.classList.toggle('activo', vista === 'vigentes');
  document.getElementById('tab-cerradas')?.classList.toggle('activo', vista === 'cerradas');
  document.getElementById('tab-vigentes')?.setAttribute('aria-selected', vista === 'vigentes' ? 'true' : 'false');
  document.getElementById('tab-cerradas')?.setAttribute('aria-selected', vista === 'cerradas' ? 'true' : 'false');
  const copy = document.getElementById('estado-listado-copy');
  if (copy) {
    copy.textContent = vista === 'vigentes'
      ? 'Mostrando concursos vigentes. Revisa los cerrados en la pestaña Cerradas.'
      : 'Mostrando convocatorias ya cerradas para consulta e historial. Vuelve a Vigentes para postular.';
  }
  actualizarVisibilidadCompartirBusqueda();
  cargarOfertas();
}

// ── Carga principal de ofertas ─────────────────────────────────────────────
async function cargarOfertas() {
  renderEsqueletos();

  const params = new URLSearchParams({ pagina: estado.pagina, por_pagina: estado.por_pagina, orden: estado.orden });
  if (estado.q)              params.set('q', estado.q);
  if (estado.region && (!Array.isArray(estado.comunas) || estado.comunas.length === 0)) params.set('region', estado.region);
  if (estado.sector)         params.set('sector', estado.sector);
  if (estado.cierra_pronto && estado.vista_listado === 'vigentes')  params.set('cierra_pronto', 'true');
  if (estado.nuevas)         params.set('nuevas', 'true');
  if (estado.institucion_id) params.set('institucion', estado.institucion_id);
  if (estado.renta_min)      params.set('renta_min', estado.renta_min);
  if (Array.isArray(estado.comunas) && estado.comunas.length > 0) {
    params.set('comunas', estado.comunas.join(','));
  } else if (estado.ciudad) {
    params.set('ciudad', estado.ciudad);
  }
  // Tipos activos: enviar solo cuando es un subconjunto propio del catálogo.
  // Si están todos activos o ninguno, no filtrar para incluir todos los tipos.
  const TIPOS_CATALOGO_UI = ['planta','contrata','honorarios','codigo_trabajo','otro','no_informa'];
  const tiposActivosApi = estado.tipos.flatMap((tipo) => (
    tipo === 'otro' ? ['otro', 'reemplazo'] : [tipo]
  ));
  const tiposUnicosApi = [...new Set(tiposActivosApi)];
  const tiposCatalogoApi = [...new Set(TIPOS_CATALOGO_UI.flatMap((tipo) => (
    tipo === 'otro' ? ['otro', 'reemplazo'] : [tipo]
  )))];
  if (tiposUnicosApi.length > 0 && tiposUnicosApi.length < tiposCatalogoApi.length) {
    params.set('tipo', tiposUnicosApi.join(','));
  }

  try {
    const endpoint = estado.vista_listado === 'cerradas' ? '/api/historial' : '/api/ofertas';
    const resp = await fetchApi(`${endpoint}?${params}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    clearTimeout(_reintentoTimer);
    if (_reintentoIntervalo) clearInterval(_reintentoIntervalo);
    _reintentosConsecutivos = 0;

    const ofertasRaw = data.ofertas || data.historial || [];
    const ofertasFiltradas = ofertasRaw.filter((oferta) => {
      const key = getOfertaEstado(oferta).key;
      return estado.vista_listado === 'cerradas' ? key === 'closed' : key !== 'closed';
    });
    _ofertasPorId = new Map(ofertasFiltradas.map((o) => [Number(o.id), o]));

    // Actualizar contador
    const totalContador = data.total ?? ofertasFiltradas.length;
    document.getElementById('count-num').textContent = totalContador.toLocaleString('es-CL');
    const sub = document.getElementById('count-sub');
    if (sub) sub.textContent = totalContador === 1 ? '· 1 resultado' : '';

    if (!ofertasFiltradas.length) {
      renderVacio();
      return;
    }

    // Aplicar clase de vista al contenedor
    const lista = document.getElementById('lista-ofertas');
    lista.className = 'ofertas-lista' + (estado.vista === 'grid' ? ' cuadricula' : estado.vista === 'compacta' ? ' compacta' : '');

    // Renderizar items según vista activa (con cabecera ordenable en modo compacto)
    const header = estado.vista === 'compacta' ? renderHeaderCompacta() : '';
const itemsHtml = ofertasFiltradas.map((oferta, i) => {
  try {
    return renderItem(oferta);
  } catch (err) {
    console.error('Error renderizando oferta', i, oferta, err);
    return '';
  }
}).join('');

lista.innerHTML = header + itemsHtml;
    // Paginación
    renderPaginacion(data.total ?? ofertasFiltradas.length, data.paginas ?? 1);

  } catch (err) {
    console.error('Error cargando ofertas:', err);
    renderError(err);
  }
}

// ── Estadísticas del sidebar ───────────────────────────────────────────────
// Estados posibles por bloque:
//   1. cargando  → placeholder "—"
//   2. sin datos → mensaje explícito "Sin datos disponibles"
//   3. error     → mensaje explícito "No se pudieron cargar datos"
//   4. con datos → valor real formateado
function _marcarContadoresError() {
  ['activas','nuevas','cierran','instituciones'].forEach(key => {
    const el = document.querySelector(`[data-stat="${key}"]`);
    if (el && (el.textContent || '').trim() === '—') el.textContent = '—';
  });
}
function _renderVacio(elId, mensaje) {
  const el = document.getElementById(elId);
  if (el) el.innerHTML = `<div class="widget-vacio">${mensaje}</div>`;
}
async function cargarEstadisticas() {
  try {
    const resp = await fetchApi(`/api/estadisticas`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    // Contadores sidebar (0 es un valor válido, no placeholder).
    const setStat = (key, val) => {
      const el = document.querySelector(`[data-stat="${key}"]`);
      if (el) el.textContent = Number(val || 0).toLocaleString('es-CL');
    };
    setStat('activas',       data.activas_hoy);
    setStat('nuevas',        data.nuevas_48h);
    setStat('cierran',       data.cierran_hoy);
    setStat('instituciones', data.instituciones_activas);

    // Pills del hero
    const hsA = document.getElementById('hs-activas');
    const hsN = document.getElementById('hs-nuevas');
    const hsI = document.getElementById('hs-instituciones');
    if (hsA) hsA.textContent = Number(data.activas_hoy || 0).toLocaleString('es-CL');
    if (hsN) hsN.textContent = Number(data.nuevas_48h  || 0).toLocaleString('es-CL');
    if (hsI) hsI.textContent = Number(data.instituciones_activas || 0).toLocaleString('es-CL');

    // Sectores
    const sec = Array.isArray(data.por_sector) ? data.por_sector : [];
    if (sec.length) renderSectores(sec);
    else _renderVacio('sectores-lista', 'Aún no hay distribución por sector disponible.');

    // Histórico mensual
    const hist = Array.isArray(data.historico_mensual) ? data.historico_mensual : [];
    if (hist.length) renderHistorico(hist);
    else _renderVacio('historico-lista', 'Histórico mensual no disponible todavía.');

    // Más activas
    const mas = Array.isArray(data.mas_activas) ? data.mas_activas : [];
    if (mas.length) renderMasActivas(mas);
    else _renderVacio('masactivas-lista', 'Sin datos de instituciones activas esta semana.');

  } catch (err) {
    console.warn('No se pudieron cargar estadísticas:', err);
    _marcarContadoresError();
    _renderVacio('sectores-lista',   'No se pudieron cargar los datos. Reintenta en unos minutos.');
    _renderVacio('historico-lista',  'No se pudo cargar el histórico.');
    _renderVacio('masactivas-lista', 'No se pudo cargar la lista de instituciones activas.');
    const lu = document.getElementById('data-last-update');
    if (lu && lu.textContent.includes('cargando')) lu.textContent = 'no disponible';
  }
}

// ── Resumen de fuentes (cuántas están realmente activas en el scraper) ─
async function cargarResumenFuentes() {
  try {
    const resp = await fetchApi(`/api/scraper/resumen`);
    if (!resp.ok) return;
    const data = await resp.json();
    if (!data.disponible) return;
    const activas = data.activas || 0;
    const total = data.total || 0;
    const heroA = document.getElementById('hero-fuentes-activas');
    const heroT = document.getElementById('hero-fuentes-total');
    const footA = document.getElementById('footer-fuentes-activas');
    const footT = document.getElementById('footer-fuentes-total');
    if (heroA) heroA.textContent = activas.toLocaleString('es-CL');
    if (heroT) heroT.textContent = total.toLocaleString('es-CL');
    if (footA) footA.textContent = activas.toLocaleString('es-CL');
    if (footT) footT.textContent = total.toLocaleString('es-CL');
  } catch (err) {
    console.warn('No se pudo cargar resumen de fuentes:', err);
  }
}

function renderSectores(sectores) {
  if (!sectores.length) return;
  const max = sectores[0].total;
  const html = sectores.map(s => {
    const pct = max > 0 ? Math.round((s.total / max) * 100) : 0;
    return `
      <div class="sector-item">
        <span class="sector-nombre">${s.sector || 'Sin sector'}</span>
        <span class="sector-count">${s.total}</span>
      </div>
      <div class="sector-bar"><div class="sector-fill" style="width:${pct}%"></div></div>`;
  }).join('');
  document.getElementById('sectores-lista').innerHTML = html;
}

function renderHistorico(meses) {
  if (!meses.length) return;
  const max = Math.max(...meses.map(m => m.total), 1);
  const MESES_ES = { '01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun',
                     '07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic' };
  const html = meses.map((m, i) => {
    const pct = Math.round((m.total / max) * 100);
    const esUltimo = i === meses.length - 1;
    const [, mes] = (m.mes || '').split('-');
    const label = MESES_ES[mes] || m.mes;
    return `
      <div class="hist-mes">
        <span class="hist-label" ${esUltimo ? 'style="color:var(--cielo);font-weight:600"' : ''}>${label}</span>
        <div class="hist-bar-outer">
          <div class="hist-bar-fill" style="width:${pct}%;${esUltimo ? 'background:var(--acento)' : ''}"></div>
        </div>
        <span class="hist-val" ${esUltimo ? 'style="color:var(--acento)"' : ''}>${m.total.toLocaleString('es-CL')}</span>
      </div>`;
  }).join('');
  document.getElementById('historico-lista').innerHTML = html;
}

function renderMasActivas(instituciones) {
  if (!instituciones.length) return;
  const html = instituciones.map(inst => {
    const icono = ICONOS_SECTOR[''] || '🏢';
    const instId = Number.isFinite(Number(inst.id)) ? Number(inst.id) : 0;
    return `
      <div class="inst-item" role="button" tabindex="0" data-inst-id="${instId}" data-inst-nombre="${escAttr(inst.nombre || '')}">
        <div class="inst-icon">${icono}</div>
        <div class="inst-info">
          <div class="inst-nombre">${inst.nombre || 'Institución'}</div>
          <div class="inst-activos">${inst.activas} activos · ${inst.nuevas_semana} nuevos</div>
        </div>
        <span class="inst-arrow">›</span>
      </div>`;
  }).join('');
  const lista = document.getElementById('masactivas-lista');
  lista.innerHTML = html;
  lista.querySelectorAll('.inst-item').forEach(el => {
    const abrir = () => {
      const id = Number.parseInt(el.dataset.instId || '0', 10);
      const nombre = el.dataset.instNombre || '';
      filtrarInstitucion(id, nombre);
    };
    el.addEventListener('click', abrir);
    el.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' || ev.key === ' ') {
        ev.preventDefault();
        abrir();
      }
    });
  });
}

function filtrarInstitucion(id, nombre) {
  estado.institucion_id  = id;
  const input = document.getElementById('input-institucion');
  const clearBtn = document.getElementById('btn-clear-inst');
  if (input)    { input.value = nombre || `Institución #${id}`; }
  if (clearBtn) { clearBtn.style.display = 'flex'; }
  estado.pagina = 1;
  window.scrollTo({ top: document.getElementById('lista-ofertas').offsetTop - 90, behavior: 'smooth' });
  cargarOfertas();
}

// ── Etiquetas narrativas de plazo para el modal ───────────────────────────
function plazoDetalle(oferta) {
  const estadoOferta = getOfertaEstado(oferta);
  const dias = estadoOferta.dias;
  if (estadoOferta.key === 'closed') return { clase: 'status-closed', label: 'Convocatoria finalizada', icon: '✕', sub: 'El plazo de postulación ya terminó.' };
  if (estadoOferta.key === 'closing_today') return { clase: 'status-closing-today', label: 'Cierra hoy', icon: '⏰', sub: 'Aún puedes postular durante hoy.' };
  if (estadoOferta.key === 'upcoming') return { clase: 'status-upcoming', label: 'Próximamente', icon: 'ℹ', sub: 'La convocatoria aún no abre postulaciones.' };
  if (dias === null || dias === undefined) return { clase: 'status-active', label: 'Disponible', icon: '✓', sub: 'La institución no informó fecha de cierre.' };
  if (dias <= 5) return { clase: 'status-closing-today', label: `Cierra pronto · ${dias} ${dias === 1 ? 'día' : 'días'}`, icon: '⏰', sub: 'Revisa requisitos y postula con anticipación.' };
  return { clase: 'status-active', label: `Disponible · ${dias} ${dias === 1 ? 'día' : 'días'}`, icon: '✓', sub: 'La postulación sigue vigente.' };
}

// ── Modal de detalle ───────────────────────────────────────────────────────
// ── Estado interno del modal: foco previo + listeners de touch ────────────
let _modalLastFocus = null;
let _modalKeydownHandler = null;
let _modalTouchCleanup = null;

function _isWeakSummaryValue(value) {
  if (value == null) return true;
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) return true;
  const weakTokens = [
    '—', '-', 'n/a', 'na', 'sin información', 'no informada', 'no informado',
    'no especificada', 'no especificado', 'consultar bases', 'ver bases',
    'por definir', 'no disponible', 'no aplica'
  ];
  if (weakTokens.includes(normalized)) return true;
  return weakTokens.some((token) => normalized.includes(token));
}

function _setSummaryField(itemId, value, opts = {}) {
  const { allowWeak = false } = opts;
  const item = document.getElementById(itemId);
  if (!item) return false;
  const valueEl = item.querySelector('.modal-info-val');
  const text = (value == null ? '' : String(value)).trim();
  const useful = !!text && (allowWeak || !_isWeakSummaryValue(text));
  item.hidden = !useful;
  if (useful && valueEl) valueEl.textContent = text;
  return useful;
}

function _focusablesModal() {
  const modal = document.querySelector('#modal .modal');
  if (!modal) return [];
  return Array.from(modal.querySelectorAll(
    'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"]), input, select, textarea'
  )).filter(el => el.offsetParent !== null || el === document.activeElement);
}

function _instalarFocusTrap() {
  _modalKeydownHandler = (e) => {
    if (e.key !== 'Tab') return;
    const focusables = _focusablesModal();
    if (focusables.length === 0) return;
    const first = focusables[0];
    const last  = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault(); last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault(); first.focus();
    }
  };
  document.addEventListener('keydown', _modalKeydownHandler);
}
function _desinstalarFocusTrap() {
  if (_modalKeydownHandler) {
    document.removeEventListener('keydown', _modalKeydownHandler);
    _modalKeydownHandler = null;
  }
}

// Swipe-down para cerrar el modal en móvil (≤ 600 px)
function _instalarSwipeCierre() {
  if (!window.matchMedia('(max-width: 600px)').matches) return;
  const modalCaja = document.querySelector('#modal .modal');
  const header    = document.querySelector('#modal .modal-header');
  if (!modalCaja || !header) return;
  let startY = null, deltaY = 0, dragging = false;
  const onStart = (e) => {
    if (e.touches.length !== 1) return;
    startY = e.touches[0].clientY; deltaY = 0; dragging = true;
    modalCaja.classList.add('dragging');
  };
  const onMove = (e) => {
    if (!dragging || startY === null) return;
    deltaY = e.touches[0].clientY - startY;
    if (deltaY > 0) {
      modalCaja.style.transform = `translateY(${deltaY}px)`;
      modalCaja.style.opacity = String(Math.max(0.4, 1 - deltaY / 400));
    }
  };
  const onEnd = () => {
    if (!dragging) return;
    dragging = false;
    modalCaja.classList.remove('dragging');
    if (deltaY > 80) {
      cerrarModal(null, true);
    } else {
      modalCaja.style.transform = '';
      modalCaja.style.opacity   = '';
    }
    startY = null; deltaY = 0;
  };
  header.addEventListener('touchstart', onStart, { passive: true });
  header.addEventListener('touchmove',  onMove,  { passive: true });
  header.addEventListener('touchend',   onEnd);
  header.addEventListener('touchcancel', onEnd);
  _modalTouchCleanup = () => {
    header.removeEventListener('touchstart', onStart);
    header.removeEventListener('touchmove',  onMove);
    header.removeEventListener('touchend',   onEnd);
    header.removeEventListener('touchcancel', onEnd);
    modalCaja.style.transform = '';
    modalCaja.style.opacity   = '';
  };
}

async function abrirModal(ofertaId) {
  window.track?.('offer-view', { id: ofertaId });
  const overlay = document.getElementById('modal');
  _modalLastFocus = document.activeElement;
  overlay.classList.add('open');
  window.__updateBackToTopVisibility?.();
  document.body.style.overflow = 'hidden';
  _instalarFocusTrap();
  _instalarSwipeCierre();
  // Foco inicial en el botón principal cuando esté disponible
  setTimeout(() => {
    document.getElementById('modal-btn-postular')?.focus();
  }, 50);

  // Reset de contenido mientras carga (orden: fechas → ubicación → contrato → jornada → renta)
  document.getElementById('modal-kicker').textContent = 'Cargando…';
  const _instElReset = document.getElementById('modal-institucion');
  if (_instElReset) { _instElReset.textContent = ''; _instElReset.hidden = true; }
  document.getElementById('modal-cargo').textContent = '';
  document.getElementById('modal-badges').innerHTML = '';
  document.getElementById('modal-fecha-publicacion').textContent = '—';
  document.getElementById('modal-fecha-cierre').textContent = '—';
  document.getElementById('modal-ubicacion').textContent = '—';
  document.getElementById('modal-tipo-contrato').textContent = '—';
  document.getElementById('modal-jornada').textContent = '—';
  document.getElementById('modal-renta').textContent = '—';
  document.getElementById('summary-item-publicacion').hidden = false;
  document.getElementById('summary-item-cierre').hidden = false;
  document.getElementById('summary-item-ubicacion').hidden = false;
  document.getElementById('summary-item-tipo').hidden = false;
  document.getElementById('summary-item-jornada').hidden = false;
  document.getElementById('summary-item-renta').hidden = false;
  const summaryNote = document.getElementById('modal-summary-note');
  summaryNote.hidden = true;
  summaryNote.textContent = '';
  document.getElementById('modal-descripcion').innerHTML = '';
  document.getElementById('modal-requisitos').innerHTML = '';
  document.getElementById('modal-btn-postular').onclick = null;
  document.getElementById('modal-btn-bases').style.display = 'none';
  const alertBox = document.getElementById('modal-plazo-alert');
  alertBox.className = 'modal-plazo-alert status-unknown';
  document.getElementById('modal-plazo-icon').textContent = '•';
  document.getElementById('modal-plazo-label').textContent = '—';
  document.getElementById('modal-plazo-fecha').textContent = '';

  try {
    const resp = await fetchApi(`/api/ofertas/${ofertaId}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const o = await resp.json();

    const tipoCss  = tipoClase(o.tipo_contrato);
    const tipoLabel = tipoEtiqueta(o.tipo_contrato);
    const regionCompleta = nombreRegionCompleto(o.region);
    const ciudad = ciudadValida(o.ciudad, o.institucion);
    const sector = o.sector || '';
    const renta = formatRenta(o.renta_bruta_min, o.renta_bruta_max, o.grado_eus);
    const detalle = plazoDetalle(o);

    // Jerarquía visual del header:
    //   - kicker: sector (etiqueta institucional secundaria, en mayúsculas)
    //   - institución: entidad contratante destacada en línea propia
    //   - cargo: título principal, con acento visual lateral (ver CSS)
    const institucionTxt = _aplicarAcronimosForzados((o.institucion || '').trim());
    const sectorTxt      = (sector || '').trim();
    document.getElementById('modal-kicker').textContent = sectorTxt.toUpperCase();
    const instEl = document.getElementById('modal-institucion');
    if (instEl) {
      instEl.textContent = institucionTxt || '';
      instEl.hidden = !institucionTxt;
    }
    document.getElementById('modal-cargo').textContent = normalizarTituloOferta(o.cargo) || 'Sin título';
    document.getElementById('modal-badges').innerHTML = [
      `<span class="badge ${tipoCss}">${tipoLabel}</span>`,
      regionCompleta ? `<span class="badge badge-region">🗺 ${escHtml(regionCompleta)}${ciudad ? ' · ' + escHtml(ciudad) : ''}</span>` : '',
    ].join('');

    // Alerta de plazo — aviso breve de urgencia, sin repetir datos del resumen
    alertBox.className = 'modal-plazo-alert ' + detalle.clase;
    document.getElementById('modal-plazo-icon').textContent = detalle.icon;
    document.getElementById('modal-plazo-label').textContent = detalle.label;
    document.getElementById('modal-plazo-fecha').textContent = detalle.sub || '';

    // Resumen ejecutivo (prioriza datos útiles y oculta ruido)
    const jornadaTexto = jornadaValida(o.jornada);
    const ubicacion = [regionCompleta, ciudad].filter(Boolean).join(' · ');
    const fechaCierreLabel = o.fecha_cierre ? formatFecha(o.fecha_cierre) : 'No informada';
    const fechaPublicacionLabel = o.fecha_publicacion ? formatFecha(o.fecha_publicacion) : 'No informada';
    const visibleCount = [
      _setSummaryField('summary-item-publicacion', fechaPublicacionLabel),
      _setSummaryField('summary-item-cierre', fechaCierreLabel),
      _setSummaryField('summary-item-ubicacion', ubicacion || ''),
      _setSummaryField('summary-item-tipo', tipoLabel || ''),
      _setSummaryField('summary-item-jornada', jornadaTexto || ''),
      _setSummaryField('summary-item-renta', renta || ''),
    ].filter(Boolean).length;
    // Publicamos el conteo visible para que el CSS elija la mejor grilla
    // (1→1 col, 2→2 col, 3/6→3 col, 4→2 col, 5→3 col). Evita huérfanos.
    const summaryGrid = document.getElementById('modal-info-grid');
    if (summaryGrid) summaryGrid.dataset.count = String(visibleCount);
    if (visibleCount < 3) {
      summaryNote.textContent = 'Algunos datos no fueron informados claramente por la fuente original. Revisa las bases para validar el detalle completo.';
      summaryNote.hidden = false;
    }

    // Descripción / funciones — formato enriquecido.
    // Umbral bajo para que ambas secciones (Requisitos + Descripción)
    // quepan en el primer viewport con un "Ver más" si hace falta. Los
    // textos cortos (<= umbral) se muestran completos sin truncar.
    const desc = o.descripcion || '';
    const descHtml = formatRichText(desc, {
      truncate: true,
      truncateAt: 380,
      suppressHeadings: ['descripción', 'descripción del cargo', 'funciones', 'funciones del cargo']
    });
    document.getElementById('modal-descripcion').innerHTML = descHtml ||
      '<p>Revisa las bases del concurso para más información sobre funciones y beneficios.</p>';

    // Requisitos — formato enriquecido (viñetas, numerados, títulos)
    const reqTexto = o.requisitos || '';
    const reqHtml  = formatRichText(reqTexto, {
      truncate: true,
      truncateAt: 360,
      suppressHeadings: ['requisitos', 'requisitos principales', 'requisitos del cargo']
    });
    document.getElementById('modal-requisitos').innerHTML = reqHtml ||
      '<p>Consulta las bases del concurso para conocer los requisitos completos.</p>';

    // Botón postular — gateado primero por flag backend, luego por validación cliente.
    const btnPostular = document.getElementById('modal-btn-postular');
    const ofertaOk = ofertaPostulable(o);
    const basesOk  = (o.url_bases_valida === false) ? false
                  : (o.url_bases_valida === true)  ? true
                  : esUrlValida(o.url_bases);
    const urlPostular = ofertaOk ? o.url_oferta : (basesOk ? o.url_bases : null);
    if (urlPostular) {
      btnPostular.disabled = false;
      btnPostular.textContent = 'Ir a postular →';
      btnPostular.onclick = () => {
        registrarClicPostular(o);
        window.open(urlPostular, '_blank', 'noopener,noreferrer');
      };
    } else {
      btnPostular.disabled = true;
      btnPostular.textContent = 'Enlace no disponible';
      btnPostular.onclick = null;
    }

    // Botón Bases oficiales (solo si es distinto y válido por backend o cliente)
    const btnBases = document.getElementById('modal-btn-bases');
    if (o.url_bases && o.url_bases !== o.url_oferta && basesOk) {
      btnBases.style.display = 'inline-flex';
      btnBases.textContent = '📄 Bases oficiales';
      btnBases.onclick = () => abrirVisorBases(o);
    } else {
      btnBases.style.display = 'none';
      btnBases.onclick = null;
    }

    // Botón favorito
    const favs = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]');
    const btnFav = document.getElementById('modal-btn-favorito');
    btnFav.textContent = favs.some(f => f.id === o.id) ? '♥ Guardado' : '♡ Guardar';
    btnFav.classList.toggle('btn-modal-sec--activo', favs.some(f => f.id === o.id));
    btnFav.onclick = () => toggleFavorito(o);

    // Barra de compartir (RRSS + copiar enlace)
    configurarCompartir(o);

  } catch (err) {
    console.error('Error cargando detalle:', err);
    document.getElementById('modal-kicker').textContent = 'Error al cargar';
    const _instElErr = document.getElementById('modal-institucion');
    if (_instElErr) { _instElErr.textContent = ''; _instElErr.hidden = true; }
    document.getElementById('modal-cargo').textContent = 'No se pudo obtener el detalle de la oferta.';
  }
}

function cerrarModal(e, force) {
  if (force || e.target === document.getElementById('modal')) {
    cerrarVisorBases({ mantenerScroll: true });
    document.getElementById('modal').classList.remove('open');
    window.__updateBackToTopVisibility?.();
    document.body.style.overflow = '';
    _desinstalarFocusTrap();
    if (_modalTouchCleanup) { _modalTouchCleanup(); _modalTouchCleanup = null; }
    if (_modalLastFocus && typeof _modalLastFocus.focus === 'function') {
      try { _modalLastFocus.focus(); } catch { /* nodo desmontado */ }
    }
    _modalLastFocus = null;
  }
}

let _basesViewerUrl = '';
let _basesViewerLastFocus = null;

function cerrarVisorBases(opts = {}) {
  const { mantenerScroll = false } = opts;
  const overlay = document.getElementById('bases-viewer');
  if (overlay.hidden) return;
  overlay.hidden = true;
  if (!mantenerScroll && !document.getElementById('modal').classList.contains('open')) {
    document.body.style.overflow = '';
  }
  if (_basesViewerLastFocus && typeof _basesViewerLastFocus.focus === 'function') {
    try { _basesViewerLastFocus.focus(); } catch { /* noop */ }
  }
  _basesViewerLastFocus = null;
}

async function copiarEnlaceBases() {
  if (!_basesViewerUrl) return;
  const btn = document.getElementById('bases-action-copy');
  const label = btn.querySelector('span');
  const setCopyText = (txt) => { if (label) label.textContent = txt; else btn.textContent = txt; };
  try {
    await navigator.clipboard.writeText(_basesViewerUrl);
    setCopyText('Enlace copiado ✓');
    btn.classList.add('is-copied');
    setTimeout(() => {
      setCopyText('Copiar enlace');
      btn.classList.remove('is-copied');
    }, 1800);
  } catch {
    prompt('Copia este enlace:', _basesViewerUrl);
  }
}

function abrirVisorBases(oferta) {
  if (!oferta) return;
  const basesUrl = (oferta.url_bases && oferta.url_bases !== oferta.url_oferta) ? oferta.url_bases : oferta.url_oferta;
  if (!esUrlValida(basesUrl)) return;

  _basesViewerLastFocus = document.activeElement;
  _basesViewerUrl = basesUrl;

  const titulo = normalizarTituloOferta(oferta.cargo) || 'Bases oficiales del concurso';
  document.getElementById('bases-viewer-title').textContent = titulo;
  document.getElementById('bases-action-open').href = basesUrl;
  // Preview del destino: dominio + resto de la ruta resumida.
  const urlPreview = document.getElementById('bases-viewer-url');
  if (urlPreview) {
    let previewText = basesUrl;
    try {
      const u = new URL(basesUrl);
      const host = u.hostname.replace(/^www\./, '');
      const path = (u.pathname || '/').replace(/\/+$/, '') || '/';
      previewText = host + (path === '/' ? '' : path);
      if (previewText.length > 68) previewText = previewText.slice(0, 65) + '…';
    } catch { /* URL malformada, mostramos el valor original */ }
    urlPreview.textContent = previewText;
  }
  const btnCopiarBases = document.getElementById('bases-action-copy');
  const copyLabel = btnCopiarBases.querySelector('span');
  if (copyLabel) copyLabel.textContent = 'Copiar enlace';
  btnCopiarBases.classList.remove('is-copied');
  btnCopiarBases.onclick = copiarEnlaceBases;

  document.getElementById('bases-viewer').hidden = false;
  document.body.style.overflow = 'hidden';
}

function abrirVisorBasesPorId(ofertaId) {
  const oferta = _ofertasPorId.get(Number(ofertaId)) || null;
  if (oferta) abrirVisorBases(oferta);
}

// Construye el deep-link a esta oferta dentro del dominio actual.
// Ventaja vs compartir oferta.url_oferta directo: el receptor ve nuestra
// previsualización (OG image dinámica) y aterriza en el modal preseleccionado,
// con un solo click para postular.
function urlDeepLinkOferta(oferta) {
  const url = new URL(window.location.href);
  const pathname = (url.pathname || '').replace(/\/+/g, '/');
  const isLegacyRoot = pathname === '/' || pathname === '/index.html' || pathname === '/index_contrataoplanta.html';
  if (isLegacyRoot) {
    // Evita depender del redirect / -> /web/index.html para deep links compartidos.
    url.pathname = '/web/index.html';
  }

  // Conservamos otros filtros existentes y limpiamos solo el hash visual.
  url.hash = '';
  url.pathname = `/oferta/${oferta.id}`;
  return url.toString();
}

// URL absoluta a la imagen OG dinámica de la oferta (servida por el API).
// El backend responde con Cache-Control agresivo + ETag, así que las
// revisitas son baratas; el browser cachea por días.
function urlImagenOG(ofertaId, formato) {
  const base = API_BASE_ACTIVA || API_BASE || RAILWAY_BACKEND;
  const fmt = formato === 'square' ? 'square' : 'horizontal';
  return `${base}/api/og/${ofertaId}.png?format=${fmt}`;
}

function configurarCompartir(oferta) {
  const url = urlDeepLinkOferta(oferta);
  const titulo = normalizarTituloOferta(oferta.cargo) || 'Oferta laboral';
  const texto = `${titulo} — ${oferta.institucion || 'Institución pública'}`;
  const enc = encodeURIComponent;

  const track = (red) => {
    try { window.umami?.track?.('share', { red, id: oferta.id }); } catch { /* noop */ }
  };

  const btnWa = document.getElementById('share-wa');
  if (btnWa) {
    btnWa.href = `https://wa.me/?text=${enc(texto + ' ' + url)}`;
    btnWa.onclick = () => track('whatsapp');
  }

  const btnLi = document.getElementById('share-li');
  if (btnLi) {
    btnLi.href = `https://www.linkedin.com/sharing/share-offsite/?url=${enc(url)}`;
    btnLi.onclick = () => track('linkedin');
  }

  // Instagram abre nuestro modal de "preview + descargar + copiar enlace".
  // Ese mismo modal se carga sólo cuando el usuario decide compartir —
  // nunca está visible por defecto en el modal de detalle.
  const btnIg = document.getElementById('share-ig');
  if (btnIg) {
    btnIg.onclick = () => {
      track('instagram');
      abrirIG(oferta, url);
    };
  }

  const btnCopy = document.getElementById('share-copy');
  btnCopy.onclick = async () => {
    track('copiar');
    try {
      await navigator.clipboard.writeText(url);
    } catch {
      prompt('Copia este enlace:', url);
      return;
    }
    btnCopy.classList.add('is-copied');
    btnCopy.setAttribute('aria-label', 'Enlace copiado');
    btnCopy.setAttribute('title', '¡Copiado!');
    setTimeout(() => {
      btnCopy.classList.remove('is-copied');
      btnCopy.setAttribute('aria-label', 'Copiar enlace');
      btnCopy.setAttribute('title', 'Copiar enlace');
    }, 1800);
  };
}

// ── QR popover (lazy-loaded library) ──
let _qrLibPromise = null;
const _QR_LIB_SRC = 'https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.min.js';

function _buscarScriptQR() {
  return Array.from(document.scripts).find((script) => {
    if (script.dataset && script.dataset.qrLib === 'qrcode-generator') return true;
    return script.src === _QR_LIB_SRC;
  }) || null;
}

function _limpiarScriptQR(script) {
  if (script && script.parentNode) script.parentNode.removeChild(script);
}

function _resolverQRLib() {
  if (window.qrcode) return window.qrcode;
  throw new Error('La librería QR se cargó, pero no pudo inicializarse correctamente');
}

function _cargarQRLib() {
  if (window.qrcode) {
    _qrLibPromise = Promise.resolve(window.qrcode);
    return _qrLibPromise;
  }

  if (_qrLibPromise) return _qrLibPromise;

  let script = _buscarScriptQR();
  if (script && script.dataset.qrEstado === 'error') {
    _limpiarScriptQR(script);
    script = null;
  }
  if (script && !window.qrcode && (script.dataset.qrEstado === 'loaded' || script.readyState === 'complete')) {
    _limpiarScriptQR(script);
    script = null;
  }

  if (!script) {
    script = document.createElement('script');
    script.src = _QR_LIB_SRC;
    script.async = true;
    script.dataset.qrLib = 'qrcode-generator';
    script.dataset.qrEstado = 'loading';
    document.head.appendChild(script);
  }

  _qrLibPromise = new Promise((resolve, reject) => {
    const onLoad = () => {
      try {
        script.dataset.qrEstado = 'loaded';
        resolve(_resolverQRLib());
      } catch (err) {
        script.dataset.qrEstado = 'error';
        _limpiarScriptQR(script);
        reject(err);
      }
    };

    const onError = () => {
      script.dataset.qrEstado = 'error';
      _limpiarScriptQR(script);
      reject(new Error('No se pudo cargar la librería QR'));
    };

    script.addEventListener('load', onLoad, { once: true });
    script.addEventListener('error', onError, { once: true });
  }).then((qrcode) => {
    _qrLibPromise = Promise.resolve(qrcode);
    return qrcode;
  }).catch((err) => {
    _qrLibPromise = null;
    throw err;
  });

  return _qrLibPromise;
}

async function abrirQR(url) {
  const pop = document.getElementById('qr-popover');
  const canvas = document.getElementById('qr-canvas');
  const urlEl = document.getElementById('qr-url');
  canvas.innerHTML = '<span style="color:var(--texto3);font-size:13px">Generando…</span>';
  urlEl.textContent = url;
  pop.hidden = false;
  document.body.style.overflow = 'hidden';
  try {
    const qrcode = await _cargarQRLib();
    const qr = qrcode(0, 'M');
    qr.addData(url);
    qr.make();
    canvas.innerHTML = qr.createSvgTag({ scalable: true, margin: 0 });
  } catch (e) {
    canvas.innerHTML = `<span style="color:var(--rojo);font-size:13px">${e.message || 'Error generando QR'}</span>`;
  }
}

function cerrarQR() {
  document.getElementById('qr-popover').hidden = true;
  document.body.style.overflow = '';
}

// ── Modal "Compartir oferta" (preview + descarga + enlace) ──
// Se invoca sólo cuando el usuario decide compartir (botón Instagram, o a
// futuro otros triggers). Muestra la imagen OG/IG que verán los contactos,
// el título/institución y un input con el enlace listo para copiar.
//
// El parámetro legacy `ogUrl` ya no se usa — el modal deriva ambas URLs
// (1080x1080 y 1200x630) a partir del `oferta.id` y alterna entre ellas
// con los botones de formato.
function abrirIG(oferta, url /* legacy: ogUrl */) {
  const modal = document.getElementById('ig-modal');
  const dl = document.getElementById('ig-descargar');
  const wa = document.getElementById('ig-whatsapp');
  const img = document.getElementById('share-preview-img');
  const frame = document.getElementById('share-preview-frame');
  const estado = document.getElementById('share-preview-estado');
  const kicker = document.getElementById('share-meta-institucion');
  const titulo = document.getElementById('share-meta-cargo');
  const linkInput = document.getElementById('share-link-input');
  const linkCopy = document.getElementById('share-link-copy');
  const swSq = document.getElementById('share-switch-square');
  const swHz = document.getElementById('share-switch-horizontal');

  // Metadatos de la oferta para contexto.
  if (kicker) kicker.textContent = oferta.institucion || 'Institución pública';
  if (titulo) {
    const t = normalizarTituloOferta(oferta.cargo) || oferta.cargo || 'Oferta laboral';
    titulo.textContent = t;
  }
  if (linkInput) linkInput.value = url;

  // WhatsApp share con texto precompuesto + link.
  if (wa) {
    const texto = `${(oferta.cargo || 'Oferta laboral')} — ${(oferta.institucion || 'Institución pública')}`;
    wa.href = `https://wa.me/?text=${encodeURIComponent(texto + ' ' + url)}`;
  }

  // Carga de preview con switch de formato (square por defecto: el caso
  // de uso principal es Instagram / mensajería cuadrada).
  const urlSquare = urlImagenOG(oferta.id, 'square');
  const urlHorizontal = urlImagenOG(oferta.id, 'horizontal');

  const aplicar = (fmt) => {
    const elegido = fmt === 'horizontal' ? urlHorizontal : urlSquare;
    if (frame) {
      frame.classList.remove('share-preview-frame--error');
      frame.classList.add('share-preview-frame--loading');
      frame.classList.toggle('share-preview-frame--horizontal', fmt === 'horizontal');
    }
    if (estado) { estado.hidden = false; estado.textContent = 'Generando imagen…'; }
    if (img) {
      img.onload = () => {
        if (frame) frame.classList.remove('share-preview-frame--loading');
        if (estado) estado.hidden = true;
      };
      img.onerror = () => {
        if (frame) {
          frame.classList.remove('share-preview-frame--loading');
          frame.classList.add('share-preview-frame--error');
        }
        if (estado) {
          estado.hidden = false;
          estado.textContent = 'No se pudo generar la imagen.';
        }
      };
      img.removeAttribute('src');
      img.src = elegido;
    }
    if (dl) {
      dl.href = elegido;
      dl.setAttribute(
        'download',
        `oferta-${oferta.id}-${fmt === 'horizontal' ? 'og' : 'instagram'}.png`,
      );
    }
    if (swSq) swSq.classList.toggle('is-active', fmt === 'square');
    if (swHz) swHz.classList.toggle('is-active', fmt === 'horizontal');
  };

  if (swSq) swSq.onclick = () => aplicar('square');
  if (swHz) swHz.onclick = () => aplicar('horizontal');
  aplicar('square');

  // Copia del enlace desde el input dedicado.
  if (linkCopy) {
    linkCopy.classList.remove('is-copied');
    linkCopy.textContent = '📋 Copiar';
    linkCopy.onclick = async () => {
      try { await navigator.clipboard.writeText(url); }
      catch {
        if (linkInput) { linkInput.select(); linkInput.setSelectionRange(0, 99999); }
        prompt('Copia este enlace:', url);
        return;
      }
      linkCopy.classList.add('is-copied');
      linkCopy.textContent = '✓ Copiado';
    };
  }

  modal.hidden = false;
  document.body.style.overflow = 'hidden';
}

function cerrarIG() {
  document.getElementById('ig-modal').hidden = true;
  document.body.style.overflow = '';
}

// Cierre por click fuera y por tecla Escape para QR e IG
document.addEventListener('click', (e) => {
  if (e.target.id === 'qr-popover') cerrarQR();
  if (e.target.id === 'ig-modal') cerrarIG();
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    if (!document.getElementById('qr-popover')?.hidden) cerrarQR();
    if (!document.getElementById('ig-modal')?.hidden) cerrarIG();
  }
});

function toggleFavorito(oferta) {
  const KEY = 'fav_contrataoplanta';
  let favs = JSON.parse(localStorage.getItem(KEY) || '[]');
  const idx = favs.findIndex(f => f.id === oferta.id);
  const btn = document.getElementById('modal-btn-favorito');
  if (idx >= 0) {
    favs.splice(idx, 1);
    btn.textContent = '♡ Guardar';
    btn.classList.remove('btn-modal-sec--activo');
  } else {
    favs.push({
      id: oferta.id,
      cargo: normalizarTituloOferta(oferta.cargo),
      institucion: oferta.institucion,
      region: oferta.region,
      fecha_cierre: oferta.fecha_cierre,
      url_oferta: oferta.url_oferta,
      guardado_en: new Date().toISOString(),
    });
    btn.textContent = '♥ Guardado';
    btn.classList.add('btn-modal-sec--activo');
  }
  localStorage.setItem(KEY, JSON.stringify(favs));

  // Sincronizar botón de la tarjeta en el listado si está visible
  const cardBtn = document.querySelector(`.btn-fav-card[data-id="${oferta.id}"]`);
  if (cardBtn) {
    const isFav = favs.some(f => f.id === oferta.id);
    cardBtn.textContent = isFav ? '♥' : '♡';
    cardBtn.title = isFav ? 'Quitar de favoritos' : 'Guardar como favorito';
    cardBtn.classList.toggle('activo', isFav);
    cardBtn.closest('.oferta-card').classList.toggle('favorita', isFav);
  }
  actualizarNavFavs();
}

// ── Autocomplete de institución ────────────────────────────────────────────
let _autocompleteData = [];
let _autocompleteTimer = null;

function initAutocompletarInstitucion() {
  const input    = document.getElementById('input-institucion');
  const dropdown = document.getElementById('autocomplete-dropdown');
  const clearBtn = document.getElementById('btn-clear-inst');
  if (!input) return;

  input.addEventListener('input', () => {
    clearTimeout(_autocompleteTimer);
    const q = input.value.trim();
    if (q.length < 2) { dropdown.style.display = 'none'; return; }
    _autocompleteTimer = setTimeout(() => _buscarInstituciones(q), 300);
  });

  input.addEventListener('keydown', e => {
    if (e.key === 'Escape') { dropdown.style.display = 'none'; input.blur(); }
    if (e.key === 'Enter')  { e.preventDefault(); buscar(); }
  });

  input.addEventListener('blur', () => {
    setTimeout(() => { dropdown.style.display = 'none'; }, 200);
  });

  clearBtn.addEventListener('click', () => {
    input.value = '';
    clearBtn.style.display = 'none';
    dropdown.style.display = 'none';
    estado.institucion_id = null;
    estado.pagina = 1;
    actualizarVisibilidadCompartirBusqueda();
    cargarOfertas();
  });
}

async function _buscarInstituciones(q) {
  try {
    const resp = await fetchApi(`/api/instituciones?q=${encodeURIComponent(q)}&por_pagina=8`);
    if (!resp.ok) return;
    const data = await resp.json();
    _autocompleteData = data.instituciones || data || [];
    const dropdown = document.getElementById('autocomplete-dropdown');
    if (!_autocompleteData.length) { dropdown.style.display = 'none'; return; }

    dropdown.innerHTML = _autocompleteData.map((inst, i) =>
      `<div class="autocomplete-item" data-idx="${i}">${escHtml(inst.nombre)}</div>`
    ).join('');

    dropdown.querySelectorAll('.autocomplete-item').forEach(el => {
      el.addEventListener('mousedown', () => {
        const inst = _autocompleteData[parseInt(el.dataset.idx)];
        _seleccionarInstitucion(inst.id, inst.nombre);
      });
    });

    dropdown.style.display = 'block';
  } catch { /* fallo silencioso */ }
}

function _seleccionarInstitucion(id, nombre) {
  estado.institucion_id = id;
  const input    = document.getElementById('input-institucion');
  const clearBtn = document.getElementById('btn-clear-inst');
  const dropdown = document.getElementById('autocomplete-dropdown');
  input.value          = nombre;
  clearBtn.style.display = 'flex';
  dropdown.style.display = 'none';
  estado.pagina = 1;
  actualizarVisibilidadCompartirBusqueda();
  cargarOfertas();
}

// ── Renta libre ───────────────────────────────────────────────────────────
function formatearRentaInput(inp) {
  const raw = inp.value.replace(/\D/g, '');
  const wrap = document.getElementById('renta-wrap');
  if (!raw) {
    inp.value = '';
    wrap.classList.remove('tiene-valor');
    return;
  }
  inp.value = parseInt(raw).toLocaleString('es-CL');
  wrap.classList.add('tiene-valor');
}

function limpiarRenta() {
  const inp = document.getElementById('filtro-renta-min');
  inp.value = '';
  document.getElementById('renta-wrap').classList.remove('tiene-valor');
  buscar();
}

// ── Búsqueda y filtros ─────────────────────────────────────────────────────
function buscar() {
  if (estado.vista_listado !== 'vigentes') {
    estado.vista_listado = 'vigentes';
    document.getElementById('estado-listado-tabs')?.setAttribute('data-estado', 'vigentes');
    document.getElementById('tab-vigentes')?.classList.add('activo');
    document.getElementById('tab-cerradas')?.classList.remove('activo');
    document.getElementById('tab-vigentes')?.setAttribute('aria-selected', 'true');
    document.getElementById('tab-cerradas')?.setAttribute('aria-selected', 'false');
    document.getElementById('estado-listado-copy').textContent = 'Mostrando concursos vigentes. Revisa los cerrados en la pestaña Cerradas.';
  }
  estado.q         = document.getElementById('input-cargo').value.trim();
  estado.region    = document.getElementById('filtro-region').value;
  estado.sector    = document.getElementById('filtro-sector').value;
  estado.comunas   = (document.getElementById('filtro-ciudad').value || '')
    .split(',')
    .map((c) => c.trim())
    .filter(Boolean);
  estado.ciudad    = estado.comunas[0] || '';
  const rawRenta = document.getElementById('filtro-renta-min').value.replace(/\D/g, '');
  estado.renta_min = rawRenta ? rawRenta : null;
  estado.pagina    = 1;
  actualizarVisibilidadCompartirBusqueda();
  // Trackeo de búsqueda: solo disparamos si hay algún filtro aplicado,
  // no en cada keypress vacío. Con debounceBuscar() esto ya sale
  // agregado (1 evento por intento real del usuario).
  if (estado.q || estado.region || estado.sector || estado.renta_min
      || (estado.comunas && estado.comunas.length)) {
    window.track?.('search', {
      q: estado.q || null,
      region: estado.region || null,
      sector: estado.sector || null,
      comunas: (estado.comunas || []).length || null,
      renta_min: estado.renta_min || null,
    });
  }
  cargarOfertas();
}

function toggleFiltro(btn, filtro) {
  btn.classList.toggle('activo');

  if (filtro === 'cierra-hoy') {
    estado.cierra_pronto = btn.classList.contains('activo');
  } else if (filtro === 'nuevos') {
    estado.nuevas = btn.classList.contains('activo');
  } else if (filtro === 'sin-experiencia') {
    // Filtro de experiencia (no es un tipo de contrato)
    estado.sin_experiencia = btn.classList.contains('activo');
  } else {
    // Filtro de tipo de contrato
    if (btn.classList.contains('activo')) {
      if (!estado.tipos.includes(filtro)) estado.tipos.push(filtro);
    } else {
      estado.tipos = estado.tipos.filter(t => t !== filtro);
    }
  }

  estado.pagina = 1;
  actualizarVisibilidadCompartirBusqueda();
  cargarOfertas();
}

function setOrdenSelect(sel) {
  estado.orden  = sel.value;
  estado.pagina = 1;
  actualizarVisibilidadCompartirBusqueda();
  guardarPrefs({ orden: estado.orden, orden_personalizado: true });
  cargarOfertas();
}

function setPorPagina(sel) {
  estado.por_pagina = parseInt(sel.value, 10);
  estado.pagina     = 1;
  const prefs = cargarPrefs();
  const porVista = { ...(prefs.por_pagina_por_vista || {}), [estado.vista]: estado.por_pagina };
  guardarPrefs({ por_pagina: estado.por_pagina, por_pagina_por_vista: porVista });
  cargarOfertas();
}

function setVista(modo) {
  if (!['cards','compacta','grid'].includes(modo)) return;
  const vistaAnterior = estado.vista;
  estado.vista = modo;
  guardarPrefs({ vista: modo });
  syncSelectPorPagina({ resetPagina: true });
  // Actualizar botones activos
  ['cards','compacta','grid'].forEach(v => {
    const btn = document.getElementById(`vista-btn-${v}`);
    if (btn) btn.classList.toggle('activo', v === modo);
  });
  // Re-renderizar con nueva vista (sin volver a hacer fetch si ya hay datos)
  const lista = document.getElementById('lista-ofertas');
  if (!lista || lista.children.length === 0) return;
  // Si hay tarjetas visibles, cambiar layout directamente sin refetch
  const nuevaClase = modo === 'grid' ? 'ofertas-lista cuadricula'
                   : modo === 'compacta' ? 'ofertas-lista compacta'
                   : 'ofertas-lista';
  if (lista.className !== nuevaClase || vistaAnterior !== modo) {
    // Necesita re-render porque el markup cambia entre cards y rows
    cargarOfertas();
  }
}

// ── Formulario de alertas ──────────────────────────────────────────────────
async function enviarAlerta() {
  const email      = document.getElementById('alerta-email')?.value.trim();
  const keywords   = document.getElementById('alerta-keywords')?.value.trim();
  const region     = document.getElementById('alerta-region')?.value;
  const tipo       = document.getElementById('alerta-tipo')?.value;
  const sector     = document.getElementById('alerta-sector')?.value;
  const frecuencia = document.getElementById('alerta-frecuencia')?.value || 'diaria';
  const btn        = document.getElementById('btn-alerta-submit');

  if (!email || !email.includes('@')) {
    btn.textContent = '⚠️ Ingresa un email válido';
    setTimeout(() => btn.textContent = 'Activar alerta gratuita', 2500);
    return;
  }

  btn.textContent = '⏳ Activando...';
  btn.disabled = true;

  try {
    const body = { email, frecuencia };
    if (keywords)  body.termino       = keywords;
    if (region)    body.region        = region;
    if (tipo)      body.tipo_contrato = tipo;
    if (sector)    body.sector        = sector;

    const resp = await fetchApi(`/api/alertas`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (resp.ok) {
      const data = await resp.json();
      btn.textContent = '✓ ¡Alerta activada!';
      btn.style.background = 'var(--verde)';
      // Limpiar el formulario
      document.getElementById('alerta-email').value = '';
      document.getElementById('alerta-keywords').value = '';
      // Umami: track alert subscription
      trackUmami('alert-subscribe', { region: region || 'todas', frecuencia });
      // Show email typo suggestion if any
      if (data.sugerencia_email) {
        const sugDiv = document.getElementById('mailcheck-suggestion');
        if (sugDiv) {
          sugDiv.innerHTML = `Nota: ¿Tu email correcto es <strong>${escHtml(data.sugerencia_email)}</strong>?`;
          sugDiv.style.color = 'var(--naran)';
          sugDiv.style.display = 'block';
        }
      }
    } else {
      const errData = await resp.json().catch(() => ({}));
      throw new Error(errData.detail || `HTTP ${resp.status}`);
    }
  } catch (err) {
    btn.textContent = '⚠️ ' + (err.message || 'Error al activar');
    setTimeout(() => {
      btn.textContent = 'Activar alerta gratuita';
      btn.style.background = '';
      btn.disabled = false;
    }, 3000);
  }
}

// ── Inicialización ─────────────────────────────────────────────────────────
// Nota: el botón "Crear alerta" fue retirado del header. El widget de alertas
// sigue disponible en el sidebar (#alertas) y su formulario en #btn-alerta-submit.
document.getElementById('btn-alerta-submit').addEventListener('click', enviarAlerta);

document.addEventListener('keydown', e => {
  if (e.key !== 'Escape') return;
  const visorAbierto = !document.getElementById('bases-viewer').hidden;
  if (visorAbierto) {
    cerrarVisorBases({ mantenerScroll: true });
    return;
  }
  cerrarModal(null, true);
});

// Activar todos los tipos del catálogo al cargar. Los filtros de cierre/nuevos/
// sin-experiencia quedan inactivos y sólo se activan manualmente.
const _TIPOS_ACTIVABLES = new Set(['planta','contrata','honorarios','codigo_trabajo','otro','no_informa']);
document.querySelectorAll('.filtro-tag.activo[onclick*="toggleFiltro"]').forEach(btn => {
  const m = btn.getAttribute('onclick').match(/'([^']+)'\)/);
  if (m && _TIPOS_ACTIVABLES.has(m[1])) {
    if (!estado.tipos.includes(m[1])) estado.tipos.push(m[1]);
  }
});

// ── Tracking local de clics en Postular (por sector) ─────────────────────
const SECTORES_KEY = 'sectores_explorados_contrataoplanta';
const SECTORES_MAX_EDAD_MS = 1000 * 60 * 60 * 24 * 180; // 6 meses

function _cargarSectoresExplorados() {
  try { return JSON.parse(localStorage.getItem(SECTORES_KEY) || '{}') || {}; }
  catch { return {}; }
}
function _guardarSectoresExplorados(obj) {
  try { localStorage.setItem(SECTORES_KEY, JSON.stringify(obj)); } catch { /* quota */ }
}
function registrarClicPostular(oferta) {
  window.track?.('apply-click', {
    id: oferta?.id,
    sector: (oferta?.sector || '').trim() || null,
    institucion: oferta?.institucion_nombre || null,
  });
  const sector = (oferta?.sector || '').trim();
  if (!sector) return;
  const data = _cargarSectoresExplorados();
  const entrada = data[sector] || { clics: 0, ultimo: null };
  entrada.clics += 1;
  entrada.ultimo = new Date().toISOString();
  data[sector] = entrada;
  // Purga lazy: descarta sectores no tocados en 6 meses
  const ahora = Date.now();
  for (const k of Object.keys(data)) {
    const t = data[k]?.ultimo ? new Date(data[k].ultimo).getTime() : 0;
    if (!t || (ahora - t) > SECTORES_MAX_EDAD_MS) delete data[k];
  }
  _guardarSectoresExplorados(data);
  renderSectoresExplorados();
}
function _totalClicsSectores(data) {
  return Object.values(data).reduce((s, v) => s + (v?.clics || 0), 0);
}
function renderSectoresExplorados() {
  const widget = document.getElementById('widget-sectores-explorados');
  const lista  = document.getElementById('sectores-explorados-lista');
  if (!widget || !lista) return;
  const data = _cargarSectoresExplorados();
  const total = _totalClicsSectores(data);
  if (total < 3) { widget.style.display = 'none'; return; }
  const top = Object.entries(data)
    .map(([sector, v]) => ({ sector, clics: v?.clics || 0 }))
    .sort((a, b) => b.clics - a.clics)
    .slice(0, 3);
  lista.innerHTML = top.map(item => `
    <button type="button" class="sector-explorado" data-sector="${escAttr(item.sector)}">
      <span class="sector-explorado-nombre">${escHtml(item.sector)}</span>
      <span class="sector-explorado-clics">${item.clics} clic${item.clics === 1 ? '' : 's'}</span>
    </button>
  `).join('');
  widget.style.display = '';
  lista.querySelectorAll('.sector-explorado').forEach(btn => {
    btn.addEventListener('click', () => filtrarPorSectorRapido(btn.dataset.sector));
  });
}
function filtrarPorSectorRapido(sector) {
  if (!sector) return;
  const sel = document.getElementById('filtro-sector');
  if (sel) {
    const opt = Array.from(sel.options).find(o => o.value === sector || o.textContent === sector);
    if (opt) sel.value = opt.value || sector;
  }
  estado.sector = sector;
  estado.pagina = 1;
  window.scrollTo({ top: document.getElementById('lista-ofertas').offsetTop - 90, behavior: 'smooth' });
  cargarOfertas();
}
document.addEventListener('DOMContentLoaded', renderSectoresExplorados);
document.getElementById('btn-reset-sectores')?.addEventListener('click', () => {
  _guardarSectoresExplorados({});
  renderSectoresExplorados();
});

// Contador de favoritos en el nav
function actualizarNavFavs() {
  const favs = JSON.parse(localStorage.getItem('fav_contrataoplanta') || '[]');
  const link = document.getElementById('nav-favoritos');
  if (!link) return;
  if (favs.length > 0) {
    link.textContent = `♥ Mis favoritos (${favs.length})`;
    link.style.color = 'white';
  } else {
    link.textContent = '♡ Mis favoritos';
  }
}
actualizarNavFavs();

// ── Aplicar preferencias guardadas a los controles ────────────────────────
(function aplicarPrefs() {
  const orden = document.getElementById('ctrl-orden');
  if (orden) orden.value = estado.orden;
  syncSelectPorPagina();

  ['cards','compacta','grid'].forEach(v => {
    const btn = document.getElementById(`vista-btn-${v}`);
    if (btn) btn.classList.toggle('activo', v === estado.vista);
  });
})();

// ════════════════════════════════════════════════════════════════
//  POST-AUDIT 2026-04-15 — Parche de producción
//  Event delegation, Escape/click-outside, URL state, a11y extra,
//  helpers de formato chileno, last-updated, back-to-top.
// ════════════════════════════════════════════════════════════════

// ── 1. Event delegation para abrir el modal desde cualquier tarjeta ──
// Reemplaza los onclick="abrirModal(id)" inline (anti-patrón de la auditoría 3.2).
document.getElementById('lista-ofertas')?.addEventListener('click', (e) => {
  if (e.target.closest('.btn-fav-card, .btn-fav-row')) return; // el favorito tiene su lógica
  const el = e.target.closest('[data-oferta-id]');
  if (!el) return;
  const id = parseInt(el.dataset.ofertaId, 10);
  if (!isNaN(id)) abrirModal(id);
});
// Accesibilidad: Enter / Space también abren el modal cuando la tarjeta tiene foco.
document.getElementById('lista-ofertas')?.addEventListener('keydown', (e) => {
  if (e.key !== 'Enter' && e.key !== ' ') return;
  const el = e.target.closest('[data-oferta-id]');
  if (!el) return;
  e.preventDefault();
  const id = parseInt(el.dataset.ofertaId, 10);
  if (!isNaN(id)) abrirModal(id);
});

// ── 2. Escape cierra el modal + Ctrl/Cmd+K enfoca el buscador ──
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    const visorAbierto = !document.getElementById('bases-viewer').hidden;
    if (visorAbierto) {
      cerrarVisorBases({ mantenerScroll: true });
      return;
    }
    const modal = document.getElementById('modal');
    if (modal && modal.classList.contains('open')) {
      cerrarModal(null, true);
    }
  }
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
    e.preventDefault();
    document.getElementById('input-cargo')?.focus();
  }
});

document.getElementById('bases-viewer')?.addEventListener('click', (e) => {
  if (e.target === document.getElementById('bases-viewer')) {
    cerrarVisorBases({ mantenerScroll: true });
  }
});

// ── 3. Sincronización con la URL (shareable filters) — auditoría 3.5 ──
function construirUrlBusqueda() {
  const url = new URL(window.location.href);
  // Limpiar params previos
  ['q','region','sector','tipos','pagina','ciudad','comunas','renta_min','institucion','orden','cierra_pronto','nuevas','vista'].forEach(k => url.searchParams.delete(k));
  const setOrDel = (k, v) => {
    if (v === null || v === undefined || v === '' || v === false ||
        (Array.isArray(v) && v.length === 0)) return;
    url.searchParams.set(k, Array.isArray(v) ? v.join(',') : String(v));
  };
  setOrDel('q', estado.q);
  setOrDel('region', estado.region);
  setOrDel('sector', estado.sector);
  setOrDel('tipos', estado.tipos);
  setOrDel('comunas', estado.comunas);
  setOrDel('ciudad', Array.isArray(estado.comunas) && estado.comunas.length > 0 ? '' : estado.ciudad);
  setOrDel('renta_min', estado.renta_min);
  setOrDel('institucion', estado.institucion_id);
  setOrDel('orden', estado.orden);
  setOrDel('cierra_pronto', estado.cierra_pronto && estado.vista_listado === 'vigentes' ? 'true' : '');
  setOrDel('nuevas', estado.nuevas ? 'true' : '');
  setOrDel('vista', estado.vista_listado !== 'vigentes' ? estado.vista_listado : '');
  setOrDel('pagina', estado.pagina > 1 ? estado.pagina : '');
  return url.toString();
}

function sincronizarURL() {
  try {
    const url = new URL(window.location.href);
    const params = url.searchParams;
    const setOrDel = (k, v) => {
      if (v === null || v === undefined || v === '' || v === false ||
          (Array.isArray(v) && v.length === 0)) {
        params.delete(k);
      } else {
        params.set(k, Array.isArray(v) ? v.join(',') : String(v));
      }
    };
    setOrDel('q', estado.q);
    setOrDel('region', estado.region);
    setOrDel('sector', estado.sector);
    setOrDel('tipos', estado.tipos);
    setOrDel('comunas', estado.comunas);
    setOrDel('ciudad', Array.isArray(estado.comunas) && estado.comunas.length > 0 ? '' : estado.ciudad);
    setOrDel('renta_min', estado.renta_min);
    setOrDel('institucion', estado.institucion_id);
    setOrDel('orden', estado.orden);
    setOrDel('cierra_pronto', estado.cierra_pronto && estado.vista_listado === 'vigentes' ? 'true' : '');
    setOrDel('nuevas', estado.nuevas ? 'true' : '');
    setOrDel('vista', estado.vista_listado !== 'vigentes' ? estado.vista_listado : '');
    setOrDel('pagina', estado.pagina > 1 ? estado.pagina : '');
    window.history.replaceState(null, '', url.toString());
  } catch { /* ignorar */ }
}

// ¿Hay filtros activos que valgan la pena compartir?
function _tiposIguales(a = [], b = []) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
  const as = [...a].sort().join('|');
  const bs = [...b].sort().join('|');
  return as === bs;
}

function hayFiltrosActivos() {
  const tiposFiltran = Array.isArray(estado.tipos) && !_tiposIguales(estado.tipos, TIPOS_POR_DEFECTO);
  return Boolean(
    estado.q?.trim() || estado.region || estado.sector || estado.ciudad || (estado.comunas || []).length ||
    estado.renta_min || estado.institucion_id || estado.cierra_pronto || estado.nuevas ||
    estado.vista_listado !== 'vigentes' || estado.orden !== ORDEN_POR_DEFECTO ||
    tiposFiltran
  );
}

function _tiposEsDefault(tipos = []) {
  if (!Array.isArray(tipos) || tipos.length !== TIPOS_POR_DEFECTO.length) return false;
  return [...tipos].sort().join(',') === [...TIPOS_POR_DEFECTO].sort().join(',');
}

// ¿Hay estado realmente útil para compartir?
// Solo cuenta un filtro/búsqueda real — no preferencias de orden ni pestañas.
function hayEstadoCompartibleReal() {
  const tieneTiposPersonalizados = Array.isArray(estado.tipos) && !_tiposEsDefault(estado.tipos);
  return Boolean(
    estado.q || estado.region || estado.sector || estado.ciudad || (estado.comunas || []).length ||
    estado.renta_min || estado.institucion_id ||
    estado.cierra_pronto || estado.nuevas || estado.sin_experiencia ||
    tieneTiposPersonalizados
  );
}

function actualizarVisibilidadCompartirBusqueda() {
  const hayFiltros = hayEstadoCompartibleReal();
  const btnCompartir = document.getElementById('btn-compartir-busqueda');
  if (btnCompartir) {
    btnCompartir.hidden = !hayFiltros;
    if (!hayFiltros) cerrarCompartirBusqueda();
  }
  // El botón "Limpiar filtros" comparte visibilidad con compartir: sólo
  // aparece cuando hay al menos un filtro activo que cambió el default.
  const btnLimpiar = document.getElementById('btn-limpiar-filtros');
  if (btnLimpiar) btnLimpiar.hidden = !hayFiltros;
}

// Restablece todos los filtros a su estado por defecto: input de cargo,
// región, sector, comuna, renta, institución y chips rápidos. También
// vuelve la pestaña a "vigentes". No toca orden ni vista (preferencias
// del usuario, no filtros de contenido).
function limpiarTodosLosFiltros() {
  const setInputValue = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.value = val;
  };
  setInputValue('input-cargo', '');
  setInputValue('filtro-region', '');
  setInputValue('filtro-sector', '');
  setInputValue('filtro-ciudad', '');
  setInputValue('filtro-renta-min', '');
  setInputValue('input-institucion', '');
  document.getElementById('renta-wrap')?.classList.remove('tiene-valor');
  const clearInst = document.getElementById('btn-clear-inst');
  if (clearInst) clearInst.style.display = 'none';
  const dropdown = document.getElementById('autocomplete-dropdown');
  if (dropdown) dropdown.style.display = 'none';

  document
    .querySelectorAll('.filtros-tags-wrap .filtro-tag.activo')
    .forEach((t) => t.classList.remove('activo'));

  estado.q = '';
  estado.region = '';
  estado.sector = '';
  estado.ciudad = '';
  estado.comunas = [];
  estado.renta_min = null;
  estado.institucion_id = null;
  estado.tipos = [...TIPOS_POR_DEFECTO];
  estado.cierra_pronto = false;
  estado.nuevas = false;
  estado.sin_experiencia = false;
  estado.pagina = 1;

  // Volvemos a la pestaña "Vigentes" — si el usuario estaba en "Cerradas",
  // limpiar filtros implica empezar de cero.
  if (estado.vista_listado !== 'vigentes') {
    estado.vista_listado = 'vigentes';
    document.getElementById('estado-listado-tabs')?.setAttribute('data-estado', 'vigentes');
    document.getElementById('tab-vigentes')?.classList.add('activo');
    document.getElementById('tab-cerradas')?.classList.remove('activo');
    document.getElementById('tab-vigentes')?.setAttribute('aria-selected', 'true');
    document.getElementById('tab-cerradas')?.setAttribute('aria-selected', 'false');
    const copy = document.getElementById('estado-listado-copy');
    if (copy) copy.innerHTML = 'Mostrando concursos vigentes. Revisa los cerrados en la pestaña <strong>Cerradas</strong>.';
  }

  actualizarVisibilidadCompartirBusqueda();
  cargarOfertas();
}

function _resumenFiltros() {
  const partes = [];
  if (estado.q) partes.push(`"${estado.q}"`);
  if (estado.region) partes.push(estado.region);
  if (estado.sector) partes.push(estado.sector);
  if (Array.isArray(estado.comunas) && estado.comunas.length) {
    partes.push(resumenComunasSeleccionadas(estado.comunas));
  } else if (estado.ciudad) {
    partes.push(estado.ciudad);
  }
  if (Array.isArray(estado.tipos) && estado.tipos.length) partes.push(estado.tipos.join('/'));
  return partes.length
    ? `Empleos públicos: ${partes.join(' · ')}`
    : 'Empleos públicos en Chile';
}

function compartirBusquedaContextual(btnRef) {
  if (!hayFiltrosActivos()) return;
  const url = construirUrlBusqueda();
  const titulo = _resumenFiltros();
  if (navigator.share && /Mobi|Android|iPhone|iPad/i.test(navigator.userAgent)) {
    try { window.umami?.track?.('share-busqueda', { red: 'nativo' }); } catch { /* noop */ }
    navigator.share({ title: titulo, text: titulo, url }).catch(() => {});
    return;
  }
  toggleCompartirBusqueda(btnRef);
}

function toggleCompartirBusqueda(btnRef) {
  const pop = document.getElementById('popover-compartir-busqueda');
  if (!pop) return;
  if (!pop.hidden) {
    cerrarCompartirBusqueda();
    return;
  }
  abrirCompartirBusqueda(btnRef);
}

function abrirCompartirBusqueda(btnRef) {
  const pop = document.getElementById('popover-compartir-busqueda');
  if (!pop) return;
  const url = construirUrlBusqueda();
  const titulo = _resumenFiltros();
  const enc = encodeURIComponent;
  const track = (red) => {
    try { window.umami?.track?.('share-busqueda', { red }); } catch { /* noop */ }
  };
  const setHref = (id, href, red) => {
    const el = document.getElementById(id);
    el.href = href;
    el.onclick = () => track(red);
  };
  const btnCopy = document.getElementById('psb-copy');
  if (btnCopy) {
    btnCopy.onclick = async () => {
      try {
        if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(url);
        else window.prompt('Copia este enlace:', url);
      } catch {
        window.prompt('Copia este enlace:', url);
      }
      btnCopy.classList.add('is-copied');
      btnCopy.setAttribute('aria-label', 'Enlace copiado');
      setTimeout(() => {
        btnCopy.classList.remove('is-copied');
        btnCopy.setAttribute('aria-label', 'Copiar enlace de búsqueda');
      }, 1600);
      track('copiar');
    };
  }
  setHref('psb-wa',   `https://wa.me/?text=${enc(titulo + ' ' + url)}`,                          'whatsapp');
  setHref('psb-li',   `https://www.linkedin.com/sharing/share-offsite/?url=${enc(url)}`,        'linkedin');
  setHref('psb-fb',   `https://www.facebook.com/sharer/sharer.php?u=${enc(url)}`,               'facebook');
  setHref('psb-x',    `https://twitter.com/intent/tweet?text=${enc(titulo)}&url=${enc(url)}`,   'x');
  setHref('psb-mail', `mailto:?subject=${enc(titulo)}&body=${enc(titulo + '\n\n' + url)}`,      'email');
  pop.hidden = false;
  const btn = document.getElementById('btn-compartir-busqueda');
  if (btn) btn.setAttribute('aria-expanded', 'true');
}


function cerrarCompartirBusqueda() {
  const pop = document.getElementById('popover-compartir-busqueda');
  if (pop) pop.hidden = true;
  const btn = document.getElementById('btn-compartir-busqueda');
  if (btn) btn.setAttribute('aria-expanded', 'false');
}

async function copiarUrlBusqueda(btnRef) {
  const btn = btnRef || document.getElementById('btn-compartir-busqueda');
  if (!btn) return;
  const url = construirUrlBusqueda();
  const textoOrig = btn.dataset.textoOrig || btn.textContent;
  btn.dataset.textoOrig = textoOrig;
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(url);
    } else {
      window.prompt('Copia este enlace:', url);
    }
    btn.textContent = '✓ Copiado';
    btn.classList.add('btn-copiar-ok');
    btn.classList.add('is-copied');
  } catch {
    window.prompt('Copia este enlace:', url);
    btn.textContent = '✓ Copiado';
    btn.classList.add('is-copied');
  }
  setTimeout(() => {
    btn.textContent = textoOrig;
    btn.classList.remove('btn-copiar-ok');
    btn.classList.remove('is-copied');
  }, 2000);
}

function restaurarFiltrosDesdeURL() {
  try {
    const params = new URLSearchParams(window.location.search);
    if (params.has('q')) {
      estado.q = params.get('q') || '';
      const inp = document.getElementById('input-cargo');
      if (inp) inp.value = estado.q;
    }
    if (params.has('region')) {
      estado.region = params.get('region') || '';
      const sel = document.getElementById('filtro-region');
      if (sel) sel.value = estado.region;
    }
    if (params.has('sector')) {
      estado.sector = params.get('sector') || '';
      const sel = document.getElementById('filtro-sector');
      if (sel) sel.value = estado.sector;
    }
    if (params.has('tipos')) {
      estado.tipos = (params.get('tipos') || '').split(',').filter(Boolean);
      // Sincronizar botones de tipo con el estado restaurado
      document.querySelectorAll('.filtro-tag[onclick*="planta"], .filtro-tag[onclick*="contrata"], .filtro-tag[onclick*="honorarios"]').forEach(btn => {
        const match = btn.getAttribute('onclick').match(/toggleFiltro\(this,'(\w+)'\)/);
        if (match) btn.classList.toggle('activo', estado.tipos.includes(match[1]));
      });
    }
    if (params.has('institucion')) {
      estado.institucion_id = params.get('institucion') || null;
    }
    if (params.has('comunas')) {
      estado.comunas = (params.get('comunas') || '').split(',').map((c) => c.trim()).filter(Boolean);
      estado.ciudad = estado.comunas[0] || '';
      const inputComunas = document.getElementById('filtro-ciudad');
      if (inputComunas) inputComunas.value = estado.comunas.join(',');
    } else if (params.has('ciudad')) {
      estado.ciudad = params.get('ciudad') || '';
      estado.comunas = estado.ciudad ? [estado.ciudad] : [];
      const inputComunas = document.getElementById('filtro-ciudad');
      if (inputComunas) inputComunas.value = estado.comunas.join(',');
    }
    if (params.has('renta_min')) {
      const renta = Number(params.get('renta_min'));
      if (!Number.isNaN(renta) && renta > 0) {
        estado.renta_min = renta;
        const inputRenta = document.getElementById('filtro-renta-min');
        if (inputRenta) inputRenta.value = formatearMilesCL(String(renta));
      }
    }
    if (params.has('orden')) {
      estado.orden = params.get('orden') || ORDEN_POR_DEFECTO;
      const selOrden = document.getElementById('ctrl-orden');
      if (selOrden) selOrden.value = estado.orden;
    }
    estado.cierra_pronto = params.get('cierra_pronto') === 'true';
    estado.nuevas = params.get('nuevas') === 'true';
    if (params.has('vista')) {
      const vista = params.get('vista');
      if (vista === 'cerradas' || vista === 'vigentes') estado.vista_listado = vista;
    }
    document.querySelector('.filtro-tag[onclick*="cierra-hoy"]')?.classList.toggle('activo', estado.cierra_pronto);
    document.querySelector('.filtro-tag[onclick*="nuevos"]')?.classList.toggle('activo', estado.nuevas);
    document.getElementById('estado-listado-tabs')?.setAttribute('data-estado', estado.vista_listado);
    document.getElementById('tab-vigentes')?.classList.toggle('activo', estado.vista_listado === 'vigentes');
    document.getElementById('tab-cerradas')?.classList.toggle('activo', estado.vista_listado === 'cerradas');

    if (params.has('pagina')) {
      const p = parseInt(params.get('pagina'), 10);
      if (!isNaN(p) && p > 0) estado.pagina = p;
    }
  } catch { /* ignorar */ }
}

// Enganchar sincronización: cada vez que cargarOfertas() termina, actualiza la URL
const _cargarOfertasOriginal = typeof cargarOfertas === 'function' ? cargarOfertas : null;
if (_cargarOfertasOriginal) {
  window.cargarOfertas = async function(...args) {
    const result = await _cargarOfertasOriginal.apply(this, args);
    sincronizarURL();
    actualizarVisibilidadCompartirBusqueda();
    return result;
  };
}

// ── 4. Formato de fecha chileno DD/MM/YYYY (auditoría 3.8) ──
window.formatFechaCL = function(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return '';
    return new Intl.DateTimeFormat('es-CL', {
      day: '2-digit', month: '2-digit', year: 'numeric'
    }).format(d);
  } catch { return ''; }
};

// ── 5. "Última actualización" basada en estadisticas.ultima_actualizacion ──
async function mostrarUltimaActualizacion() {
  const absoluta = document.getElementById('data-last-update');
  try {
    const resp = await fetchApi(`/api/estadisticas`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (!data.ultima_actualizacion) {
      if (absoluta) absoluta.textContent = 'no disponible';
      return;
    }
    const fecha = new Date(data.ultima_actualizacion);
    if (absoluta) {
      absoluta.textContent = fecha.toLocaleString('es-CL', { dateStyle: 'medium', timeStyle: 'short' });
    }
    const ahora = new Date();
    const minutos = Math.max(0, Math.floor((ahora - fecha) / 60000));
    let txt;
    if (minutos < 60) txt = `hace ${minutos} min`;
    else if (minutos < 1440) txt = `hace ${Math.floor(minutos / 60)} h`;
    else txt = `hace ${Math.floor(minutos / 1440)} d`;
    const label = document.getElementById('count-sub');
    if (label) label.textContent = `· última actualización ${txt}`;
  } catch {
    if (absoluta) absoluta.textContent = 'no disponible';
  }
}

// ── 6. Back-to-top flotante (auditoría 3 UX) ──
(function backToTop() {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'back-to-top-btn';
  btn.textContent = '↑';
  btn.setAttribute('aria-label', 'Volver al inicio de la página');
  btn.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));
  document.body.appendChild(btn);
  const updateVisibility = () => {
    const visible = window.scrollY > 400;
    const modalOpen = document.getElementById('modal')?.classList.contains('open');
    btn.classList.toggle('is-visible', visible && !modalOpen);
  };
  window.__updateBackToTopVisibility = updateVisibility;
  window.addEventListener('scroll', updateVisibility, { passive: true });
  updateVisibility();
})();

// ── 7. Aria-pressed dinámico en filtros activos (auditoría 3.11) ──
function actualizarAriaPressed() {
  document.querySelectorAll('[data-filtro-tipo]').forEach(btn => {
    const tipo = btn.dataset.filtroTipo;
    btn.setAttribute('aria-pressed', estado.tipos.includes(tipo) ? 'true' : 'false');
  });
}
document.addEventListener('filtros:cambio', actualizarAriaPressed);

// ── 8. Restaurar filtros antes de la primera carga ──
restaurarFiltrosDesdeURL();

// ── 8.b Deep-link a oferta concreta: ?oferta=ID abre el modal al cargar ──
(function abrirOfertaDesdeURL() {
  try {
    const params = new URLSearchParams(window.location.search);
    const fromQuery = parseInt(params.get('oferta') || '', 10);
    const fromPathMatch = window.location.pathname.match(/^\/oferta\/(\d+)(?:\/)?$/);
    const fromPath = fromPathMatch ? parseInt(fromPathMatch[1], 10) : NaN;
    const id = !isNaN(fromQuery) && fromQuery > 0 ? fromQuery : fromPath;
    if (!isNaN(id) && id > 0) {
      // Esperar al primer pintado para no pelear con la animación de carga.
      window.addEventListener('load', () => setTimeout(() => abrirModal(id), 200));
    }
  } catch { /* ignorar */ }
})();

// ═══ REGIONES DINÁMICAS (API DPA del Estado) ════════════════════════════════
async function cargarRegiones() {
  try {
    const resp = await fetchApi(`/api/regiones`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const regiones = await resp.json();

    // Poblar todos los selects de región
    ['filtro-region', 'alerta-region'].forEach(id => {
      const sel = document.getElementById(id);
      if (!sel) return;
      const valorActual = sel.value;
      // Keep first option (placeholder)
      while (sel.options.length > 1) sel.remove(1);
      regiones.forEach(r => {
        const opt = document.createElement('option');
        opt.value = r.nombre;
        opt.textContent = r.nombre;
        opt.dataset.codigo = r.codigo;
        sel.appendChild(opt);
      });
      // Restore previous value if any
      if (valorActual) sel.value = valorActual;
    });

    // Store region codes for comuna loading
    window._regionesDPA = regiones;
    await cargarCatalogoComunas();
    renderResumenComunas();
  } catch (err) {
    console.warn('Error cargando regiones DPA:', err);
  }
}

async function cargarCatalogoComunas() {
  try {
    if (!Array.isArray(window._regionesDPA) || window._regionesDPA.length === 0) return;
    const peticiones = window._regionesDPA.map(async (r) => {
      const resp = await fetchApi(`/api/regiones/${r.codigo}/comunas`);
      if (!resp.ok) return [];
      const comunas = await resp.json();
      return comunas.map((c) => ({
        nombre: c.nombre,
        region: r.nombre,
        key: comunaNormalizada(c.nombre),
      }));
    });
    const resueltas = await Promise.all(peticiones);
    _comunasCatalogo = resueltas.flat().sort((a, b) => a.nombre.localeCompare(b.nombre, 'es'));
    renderListaComunas();
  } catch (err) {
    console.warn('Error cargando catálogo de comunas:', err);
  }
}

function renderResumenComunas() {
  const resumen = document.getElementById('filtro-comunas-resumen');
  const meta = document.getElementById('filtro-comunas-meta');
  const input = document.getElementById('filtro-ciudad');
  if (!resumen || !meta || !input) return;
  const seleccion = Array.isArray(estado.comunas) ? estado.comunas : [];
  input.value = seleccion.join(',');
  resumen.textContent = resumenComunasSeleccionadas(seleccion);
  meta.textContent = seleccion.length > 1 ? `${seleccion.length} seleccionadas` : '';
}

function renderSeleccionComunas() {
  const cont = document.getElementById('comunas-seleccionadas');
  if (!cont) return;
  if (!_comunasDraft.length) {
    cont.innerHTML = '';
    return;
  }
  cont.innerHTML = _comunasDraft.map((c) => (
    `<span class="comuna-chip">${escHtml(c)} <button type="button" data-remove-comuna="${escAttr(c)}">✕</button></span>`
  )).join('');
  cont.querySelectorAll('button[data-remove-comuna]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const comuna = btn.dataset.removeComuna;
      _comunasDraft = _comunasDraft.filter((c) => c !== comuna);
      renderSeleccionComunas();
      renderListaComunas();
    });
  });
}

function renderListaComunas() {
  const lista = document.getElementById('comunas-lista');
  const q = document.getElementById('filtro-comunas-busqueda')?.value || '';
  if (!lista) return;
  const qNorm = comunaNormalizada(q);
  const filtradas = _comunasCatalogo.filter((item) => {
    if (!qNorm) return true;
    return item.key.includes(qNorm) || comunaNormalizada(item.region).includes(qNorm);
  });
  if (!filtradas.length) {
    lista.innerHTML = '<div class="comunas-vacio">No hay coincidencias para tu búsqueda.</div>';
    return;
  }
  lista.innerHTML = filtradas.map((item) => {
    const checked = _comunasDraft.includes(item.nombre) ? 'checked' : '';
    return `
      <label class="comuna-item">
        <input type="checkbox" value="${escAttr(item.nombre)}" ${checked}>
        <span><strong>${escHtml(item.nombre)}</strong><small>${escHtml(item.region)}</small></span>
      </label>
    `;
  }).join('');
  lista.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
    checkbox.addEventListener('change', () => {
      const comuna = checkbox.value;
      if (checkbox.checked) {
        if (!_comunasDraft.includes(comuna)) _comunasDraft.push(comuna);
      } else {
        _comunasDraft = _comunasDraft.filter((c) => c !== comuna);
      }
      _comunasDraft.sort((a, b) => a.localeCompare(b, 'es'));
      renderSeleccionComunas();
    });
  });
}

function abrirSelectorComunas() {
  const panel = document.getElementById('comunas-panel');
  const trigger = document.getElementById('filtro-comunas-trigger');
  if (!panel || !trigger) return;
  _comunasDraft = [...(estado.comunas || [])];
  _comunasPanelAbierto = true;
  panel.hidden = false;
  trigger.setAttribute('aria-expanded', 'true');
  renderSeleccionComunas();
  renderListaComunas();
  document.getElementById('filtro-comunas-busqueda')?.focus();
}

function cerrarSelectorComunas() {
  const panel = document.getElementById('comunas-panel');
  const trigger = document.getElementById('filtro-comunas-trigger');
  if (!panel || !trigger) return;
  _comunasPanelAbierto = false;
  panel.hidden = true;
  trigger.setAttribute('aria-expanded', 'false');
}

function aplicarSelectorComunas() {
  estado.comunas = [..._comunasDraft];
  estado.ciudad = estado.comunas[0] || '';
  renderResumenComunas();
  cerrarSelectorComunas();
  debounceBuscar(150);
}

function inicializarSelectorComunas() {
  const trigger = document.getElementById('filtro-comunas-trigger');
  const cerrar = document.getElementById('comunas-cerrar');
  const limpiar = document.getElementById('comunas-limpiar');
  const aplicar = document.getElementById('comunas-aplicar');
  const search = document.getElementById('filtro-comunas-busqueda');
  if (!trigger || !cerrar || !limpiar || !aplicar || !search) return;

  trigger.addEventListener('click', () => (_comunasPanelAbierto ? cerrarSelectorComunas() : abrirSelectorComunas()));
  cerrar.addEventListener('click', cerrarSelectorComunas);
  limpiar.addEventListener('click', () => {
    _comunasDraft = [];
    search.value = '';
    renderSeleccionComunas();
    renderListaComunas();
  });
  aplicar.addEventListener('click', aplicarSelectorComunas);
  search.addEventListener('input', () => renderListaComunas());
  document.addEventListener('click', (e) => {
    if (!_comunasPanelAbierto) return;
    const wrap = document.getElementById('comunas-selector');
    if (wrap && !wrap.contains(e.target)) cerrarSelectorComunas();
  });
  // Cascading región → comunas: al cambiar la región, limpiamos la
  // selección de comunas (pueden no pertenecer a la nueva región) y
  // refrescamos la UI antes de disparar la búsqueda. Sin esto el
  // usuario puede terminar con comuna de RM + región Valparaíso
  // filtrando y ver 0 resultados sin entender por qué.
  document.getElementById('filtro-region')?.addEventListener('change', () => {
    estado.comunas = [];
    estado.ciudad = '';
    const inputCiudad = document.getElementById('filtro-ciudad');
    if (inputCiudad) inputCiudad.value = '';
    _comunasDraft = [];
    renderResumenComunas();
    if (_comunasPanelAbierto) {
      renderSeleccionComunas();
      renderListaComunas();
    }
    debounceBuscar();
  });
  renderResumenComunas();
}

// ═══ MAILCHECK — Validación de email en tiempo real ═══════════════════════
let _mailcheckTimer = null;
function initMailcheck() {
  const emailInput = document.getElementById('alerta-email');
  if (!emailInput) return;

  // Create suggestion element
  const sugDiv = document.createElement('div');
  sugDiv.id = 'mailcheck-suggestion';
  sugDiv.style.cssText = 'font-size:12px;color:var(--naran);margin-top:4px;display:none;cursor:pointer';
  emailInput.parentNode.insertBefore(sugDiv, emailInput.nextSibling);

  emailInput.addEventListener('blur', async function() {
    const email = this.value.trim();
    if (!email || !email.includes('@')) { sugDiv.style.display = 'none'; return; }

    try {
      const resp = await fetchApi(`/api/validar-email?email=${encodeURIComponent(email)}`);
      if (!resp.ok) return;
      const data = await resp.json();

      if (data.desechable) {
        sugDiv.innerHTML = '⚠️ Este dominio es temporal. Usa un email permanente.';
        sugDiv.style.color = 'var(--rojo)';
        sugDiv.style.display = 'block';
        sugDiv.style.cursor = 'default';
      } else if (data.sugerencia) {
        sugDiv.innerHTML = `¿Quisiste decir <strong>${escHtml(data.sugerencia)}</strong>? <u>Corregir</u>`;
        sugDiv.style.color = 'var(--naran)';
        sugDiv.style.display = 'block';
        sugDiv.style.cursor = 'pointer';
        sugDiv.onclick = () => {
          emailInput.value = data.sugerencia;
          sugDiv.style.display = 'none';
        };
      } else if (!data.valido) {
        sugDiv.innerHTML = `⚠️ ${escHtml(data.motivo || 'Email inválido')}`;
        sugDiv.style.color = 'var(--rojo)';
        sugDiv.style.display = 'block';
        sugDiv.style.cursor = 'default';
      } else {
        sugDiv.style.display = 'none';
      }
    } catch {
      sugDiv.style.display = 'none';
    }
  });

  emailInput.addEventListener('input', () => { sugDiv.style.display = 'none'; });
}

// ═══ MEILISEARCH — Autocompletado de cargos ═══════════════════════════════
let _meiliTimer = null;
function initMeilisearchAutocomplete() {
  const input = document.getElementById('input-cargo');
  if (!input) return;

  // Create dropdown container
  const wrap = input.parentNode;
  wrap.style.position = 'relative';
  const dropdown = document.createElement('div');
  dropdown.id = 'meili-autocomplete';
  dropdown.style.cssText = `
    position:absolute;top:100%;left:0;right:0;z-index:50;
    background:var(--blanco);border:1px solid var(--borde);border-top:none;
    border-radius:0 0 8px 8px;box-shadow:var(--sombra);
    max-height:280px;overflow-y:auto;display:none;
  `;
  wrap.appendChild(dropdown);

  input.addEventListener('input', function() {
    const q = this.value.trim();
    clearTimeout(_meiliTimer);
    if (q.length < 2) { dropdown.style.display = 'none'; return; }

    _meiliTimer = setTimeout(async () => {
      try {
        const resp = await fetchApi(`/api/autocompletar?q=${encodeURIComponent(q)}&limite=8`);
        if (!resp.ok) { dropdown.style.display = 'none'; return; }
        const sugerencias = await resp.json();

        if (!sugerencias.length) { dropdown.style.display = 'none'; return; }

        dropdown.innerHTML = sugerencias.map(s => `
          <div class="meili-item" style="padding:10px 14px;cursor:pointer;border-bottom:1px solid var(--bg2);
            font-size:13px;transition:background .1s"
            onmouseenter="this.style.background='var(--bg2)'"
            onmouseleave="this.style.background=''"
            data-cargo="${escAttr(s.cargo)}">
            <div style="font-weight:500;color:var(--texto)">${s.cargo_highlight ? sanitizeHighlightHtml(s.cargo_highlight) : escHtml(s.cargo)}</div>
            <div style="font-size:11px;color:var(--texto3);margin-top:2px">${escHtml(s.institucion)}${s.region ? ' · ' + escHtml(s.region) : ''}</div>
          </div>
        `).join('');
        dropdown.style.display = 'block';

        dropdown.querySelectorAll('.meili-item').forEach(item => {
          item.addEventListener('click', () => {
            input.value = item.dataset.cargo;
            dropdown.style.display = 'none';
            buscar();
            // Umami: track search selection
            if (typeof umami !== 'undefined') umami.track('search-autocomplete', { cargo: item.dataset.cargo });
          });
        });
      } catch {
        dropdown.style.display = 'none';
      }
    }, 150);
  });

  // Hide dropdown on blur (with delay for click)
  input.addEventListener('blur', () => setTimeout(() => dropdown.style.display = 'none', 200));
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') dropdown.style.display = 'none';
  });
}

// ═══ UMAMI — Eventos personalizados ═══════════════════════════════════════
function trackUmami(evento, datos) {
  if (typeof umami !== 'undefined') {
    try { umami.track(evento, datos); } catch {}
  }
}

// Track search events
const _originalBuscar = typeof buscar === 'function' ? buscar : null;

// Wrap buscar to add analytics
(function() {
  const realBuscar = window.buscar;
  if (!realBuscar) return;
  window.buscar = function() {
    const q = document.getElementById('input-cargo')?.value?.trim();
    const region = document.getElementById('filtro-region')?.value;
    if (q || region) {
      trackUmami('search', { query: q || '', region: region || '' });
    }
    return realBuscar.apply(this, arguments);
  };
})();

// ═══ INICIALIZACIÓN DE INTEGRACIONES ══════════════════════════════════════
inicializarSelectorComunas();
cargarRegiones();
initMailcheck();
initMeilisearchAutocomplete();

// Carga inicial
initAutocompletarInstitucion();
cargarOfertas();
cargarEstadisticas();
cargarResumenFuentes();
mostrarUltimaActualizacion();
setInterval(mostrarUltimaActualizacion, 5 * 60 * 1000); // refrescar cada 5 min

