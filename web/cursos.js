/* cursos.js · Render y filtro del directorio de cursos. Depende de cursos-data.js. */
(function () {
  'use strict';
  var D = window.CURSOS_DATA;
  if (!D) return;

  var contFiltros = document.getElementById('cursos-filtros');
  var contLista = document.getElementById('cursos-lista');
  if (!contLista) return;

  var filtroActivo = 'todas';
  var busqueda = { q: '', fpago: '', inst: '', tipo: '' };

  function _fold(s) { return String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase(); }

  // Buscador + filtros (texto, gratis/pagado, institución). Wire una sola vez;
  // el <select> de instituciones se repuebla cada vez (los datos pueden venir
  // de la API después).
  var _buscadorWired = false;
  function pintarBuscador() {
    var sel = document.getElementById('cursos-inst');
    if (sel) {
      var provs = [];
      D.avisos.forEach(function (a) { if (a.proveedor && provs.indexOf(a.proveedor) === -1) provs.push(a.proveedor); });
      provs.sort();
      var actual = sel.value;
      sel.innerHTML = '<option value="">Todas las instituciones</option>'
        + provs.map(function (p) { return '<option value="' + p.replace(/"/g, '&quot;') + '">' + p + '</option>'; }).join('');
      sel.value = actual;
    }
    if (_buscadorWired) return;
    _buscadorWired = true;
    var q = document.getElementById('cursos-q');
    if (q) q.addEventListener('input', function () { busqueda.q = q.value.trim(); pintarLista(false); });
    var fp = document.getElementById('cursos-fpago');
    if (fp) fp.addEventListener('change', function () { busqueda.fpago = fp.value; repintarAnimado(); });
    var ins = document.getElementById('cursos-inst');
    if (ins) ins.addEventListener('change', function () { busqueda.inst = ins.value; repintarAnimado(); });
    var tp = document.getElementById('cursos-tipo');
    if (tp) tp.addEventListener('change', function () { busqueda.tipo = tp.value; repintarAnimado(); });
  }

  // Deep-link: ?cat=<slug> o ?area=<area_profesional de la API>
  (function aplicarParam() {
    try {
      var p = new URLSearchParams(location.search);
      var cat = (p.get('cat') || '').toLowerCase();
      var area = (p.get('area') || '').toLowerCase().trim();
      if (cat && D.categorias.some(function (c) { return c.slug === cat; })) {
        filtroActivo = cat;
      } else if (area && D.mapaAreas && D.mapaAreas[area]) {
        filtroActivo = D.mapaAreas[area];
      }
    } catch (e) {}
  })();

  var fmtCLP = new Intl.NumberFormat('es-CL', { style: 'currency', currency: 'CLP', maximumFractionDigits: 0 });

  function pintarPlanes() {
    var cont = document.getElementById('cursos-planes');
    if (!cont || !D.planes) return;
    cont.innerHTML = D.planes.map(function (pl) {
      var precio = pl.precioRef != null ? fmtCLP.format(pl.precioRef) + '/' + pl.periodo : pl.periodo;
      return '<div class="anunciate-nivel"><span>' + pl.nombre + '</span><b>' + precio + '</b></div>';
    }).join('');
  }

  function wireMailto() {
    if (!D.contacto || !D.contacto.email) return;
    var cta = document.querySelector('.anunciate-cta');
    if (cta) cta.href = 'mailto:' + D.contacto.email +
      '?subject=' + encodeURIComponent(D.contacto.asuntoBase || 'Quiero anunciar un curso');
  }

  function pintarFiltros() {
    var on = function (slug) { return slug === filtroActivo ? ' curso-chip--on' : ''; };
    var chips = ['<button class="curso-chip' + on('todas') + '" data-cat="todas">Todas</button>'];
    D.categorias.forEach(function (c) {
      chips.push('<button class="curso-chip' + on(c.slug) + '" data-cat="' + c.slug + '">' + c.etiqueta + '</button>');
    });
    contFiltros.innerHTML = chips.join('');
    Array.prototype.forEach.call(contFiltros.querySelectorAll('.curso-chip'), function (b) {
      b.addEventListener('click', function () {
        filtroActivo = b.dataset.cat;
        Array.prototype.forEach.call(contFiltros.querySelectorAll('.curso-chip'), function (x) {
          x.classList.toggle('curso-chip--on', x === b);
        });
        repintarAnimado();
      });
    });
  }

  function etiquetaCat(slug) {
    var c = D.categorias.find(function (x) { return x.slug === slug; });
    return c ? c.etiqueta : slug;
  }

  // Dominio del curso → logo institucional. Usamos el favicon del PROPIO sitio
  // que dicta el curso (primera parte, sin terceros): la marca real de CEA,
  // ChileCompra, Servicio Civil, SUBDERE, etc. Si el curso trae `logo`
  // explícito (futuro campo del admin) tiene prioridad. Si nada carga, queda
  // el monograma de respaldo (ver _monoDe + wireLogos).
  function _hostDe(url) { try { return new URL(url).hostname.replace(/^www\./, ''); } catch (e) { return ''; } }
  function _logoDe(a) {
    if (a.logo) return a.logo;
    var h = _hostDe(a.url);
    return h ? 'https://' + h + '/favicon.ico' : '';
  }
  function _monoDe(a) {
    var base = String(a.proveedor || a.titulo || '?').split('·')[0].trim();
    var caps = (base.match(/[A-ZÁÉÍÓÚÑ]/g) || []).join('');
    if (caps.length >= 2) return caps.slice(0, 3);
    var palabras = base.split(/\s+/).filter(Boolean);
    return (palabras.slice(0, 2).map(function (p) { return p.charAt(0); }).join('') || base.slice(0, 2)).toUpperCase();
  }

  function tarjeta(a) {
    var destacado = a.nivel === 'destacado';
    var sello = destacado ? '<span class="curso-sello">Destacado</span>' : '';
    var grat = a.gratuito ? '<span class="curso-grat">Gratis</span>' : '';
    var demo = a.demo ? '<span class="curso-demo" title="Aviso de demostración">Ejemplo</span>' : '';
    // Los cursos oficiales gratuitos llevan un enlace normal (sin nofollow/
    // sponsored, que se reservan para los avisos pagados).
    var rel = a.gratuito ? 'noopener' : 'noopener nofollow sponsored';
    var cta = a.url && a.url !== '#'
      ? '<a class="curso-cta" href="' + a.url + '" target="_blank" rel="' + rel + '">' + (a.gratuito ? 'Ir al curso →' : 'Ver curso →') + '</a>'
      : '<span class="curso-cta curso-cta--off">Próximamente</span>';
    var logoUrl = _logoDe(a);
    var logo = '<span class="curso-logo-wrap" aria-hidden="true">' + _monoDe(a) +
      (logoUrl ? '<img class="curso-logo" src="' + logoUrl + '" alt="" loading="lazy">' : '') + '</span>';
    return '<article class="curso-card' + (destacado ? ' curso-card--top' : '') + (a.gratuito ? ' curso-card--gratis' : '') + '">' +
      '<div class="curso-band"><span class="curso-cat">' + etiquetaCat(a.categoria) + '</span>' +
        '<span class="curso-flags">' + sello + grat + demo + '</span></div>' +
      '<div class="curso-body">' +
        '<div class="curso-head">' + logo +
          '<div class="curso-headtxt"><h3 class="curso-titulo">' + a.titulo + '</h3>' +
          '<div class="curso-prov">' + a.proveedor + '</div></div></div>' +
        '<p class="curso-desc">' + a.descripcion + '</p>' +
        '<div class="curso-meta"><span>📋 ' + a.modalidad + '</span><span>⏱ ' + a.duracion + '</span></div>' +
        cta +
      '</div>' +
    '</article>';
  }

  // Si un favicon no carga, lo quitamos para que se vea el monograma de respaldo.
  function wireLogos() {
    contLista.querySelectorAll('.curso-logo').forEach(function (img) {
      img.addEventListener('error', function () { img.remove(); }, { once: true });
    });
  }

  function slotLibre() {
    return '<a class="curso-card curso-slot" href="#anunciate">' +
      '<div class="curso-slot-ico">＋</div>' +
      '<div class="curso-slot-txt"><strong>Tu curso aquí</strong><span>Llega a quienes buscan empleo público en esta área.</span></div>' +
    '</a>';
  }

  var _reduce = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);

  // Repinta con transición suave: fade-out breve del grid y entrada animada
  // (escalonada) de las tarjetas. Se usa al cambiar filtros o chips; al tipear
  // se repinta directo para no introducir lag.
  function repintarAnimado() {
    if (_reduce) { pintarLista(false); return; }
    contLista.style.opacity = '0';
    setTimeout(function () { pintarLista(true); contLista.style.opacity = '1'; }, 150);
  }

  function pintarLista(animar) {
    var q = _fold(busqueda.q);
    var lista = D.avisos.filter(function (a) {
      if (filtroActivo !== 'todas' && a.categoria !== filtroActivo) return false;
      if (busqueda.fpago === 'gratis' && !a.gratuito) return false;
      if (busqueda.fpago === 'pagado' && a.gratuito) return false;
      if (busqueda.inst && a.proveedor !== busqueda.inst) return false;
      if (busqueda.tipo && (a.tipo || 'curso') !== busqueda.tipo) return false;
      if (q) {
        var heno = _fold((a.titulo || '') + ' ' + (a.proveedor || '') + ' ' + (a.descripcion || '') + ' ' + etiquetaCat(a.categoria));
        if (heno.indexOf(q) === -1) return false;
      }
      return true;
    });
    // destacados primero
    lista.sort(function (x, y) {
      return (y.nivel === 'destacado' ? 1 : 0) - (x.nivel === 'destacado' ? 1 : 0);
    });
    if (!lista.length) {
      contLista.classList.remove('is-animando');
      contLista.innerHTML = '<p style="grid-column:1/-1;color:var(--texto3);padding:24px 4px">No encontramos cursos con esos criterios. Prueba con otras palabras o quita un filtro.</p>';
      return;
    }
    contLista.innerHTML = lista.map(tarjeta).join('') + slotLibre();
    wireLogos();
    if (animar) {
      contLista.classList.remove('is-animando');
      void contLista.offsetWidth; // fuerza reflow para reiniciar la animación
      contLista.classList.add('is-animando');
      // Quita la clase al terminar para que el hover (transform) vuelva a
      // funcionar y la animación no quede "pegada" por animation-fill-mode.
      clearTimeout(contLista._animT);
      contLista._animT = setTimeout(function () { contLista.classList.remove('is-animando'); }, 650);
    } else {
      contLista.classList.remove('is-animando');
    }
  }

  pintarFiltros();
  pintarBuscador();
  pintarLista(true);
  pintarPlanes();
  wireMailto();

  // Directorio administrable: si la API responde con cursos (gestionados desde
  // el panel admin), reemplaza el set estático y repinta. Si falla o viene
  // vacía, queda el directorio del archivo cursos-data.js (fallback robusto).
  try {
    var RAILWAY = 'https://contrataoplanta-production.up.railway.app';
    var _h = location.hostname;
    var API = window.__API_BASE ||
      ((_h === 'localhost' || _h === '127.0.0.1' || _h === '') ? 'http://localhost:8000' : RAILWAY);
    fetch(API + '/api/cursos')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (d && Array.isArray(d.cursos) && d.cursos.length) {
          D.avisos = d.cursos;
          pintarBuscador();
          pintarLista(true);
        }
      })
      .catch(function () { /* sin conexión: queda el fallback estático */ });

    // Categorías administrables: si la API responde, reemplaza las del archivo
    // y repinta los chips.
    fetch(API + '/api/cursos/categorias')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (d && Array.isArray(d.categorias) && d.categorias.length) {
          D.categorias = d.categorias; // [{slug, etiqueta}]
          if (filtroActivo !== 'todas' && !D.categorias.some(function (c) { return c.slug === filtroActivo; })) {
            filtroActivo = 'todas';
          }
          pintarFiltros();
          pintarLista(true);
        }
      })
      .catch(function () { /* queda el set estático */ });
  } catch (e) { /* noop */ }
})();
