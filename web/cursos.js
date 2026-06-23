/* cursos.js · Render y filtro del directorio de cursos. Depende de cursos-data.js. */
(function () {
  'use strict';
  var D = window.CURSOS_DATA;
  if (!D) return;

  var contFiltros = document.getElementById('cursos-filtros');
  var contLista = document.getElementById('cursos-lista');
  if (!contLista) return;

  var filtroActivo = 'todas';

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
        pintarLista();
      });
    });
  }

  function etiquetaCat(slug) {
    var c = D.categorias.find(function (x) { return x.slug === slug; });
    return c ? c.etiqueta : slug;
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
    return '<article class="curso-card' + (destacado ? ' curso-card--top' : '') + (a.gratuito ? ' curso-card--gratis' : '') + '">' +
      '<div class="curso-card-head">' + sello + grat + demo +
        '<span class="curso-cat">' + etiquetaCat(a.categoria) + '</span></div>' +
      '<h3 class="curso-titulo">' + a.titulo + '</h3>' +
      '<div class="curso-prov">' + a.proveedor + '</div>' +
      '<p class="curso-desc">' + a.descripcion + '</p>' +
      '<div class="curso-meta"><span>📋 ' + a.modalidad + '</span><span>⏱ ' + a.duracion + '</span></div>' +
      cta +
    '</article>';
  }

  function slotLibre() {
    return '<a class="curso-card curso-slot" href="#anunciate">' +
      '<div class="curso-slot-ico">＋</div>' +
      '<div class="curso-slot-txt"><strong>Tu curso aquí</strong><span>Llega a quienes buscan empleo público en esta área.</span></div>' +
    '</a>';
  }

  function pintarLista() {
    var lista = D.avisos.filter(function (a) {
      return filtroActivo === 'todas' || a.categoria === filtroActivo;
    });
    // destacados primero
    lista.sort(function (x, y) {
      return (y.nivel === 'destacado' ? 1 : 0) - (x.nivel === 'destacado' ? 1 : 0);
    });
    var html = lista.map(tarjeta).join('');
    html += slotLibre();
    contLista.innerHTML = html;
  }

  pintarFiltros();
  pintarLista();
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
          pintarLista();
        }
      })
      .catch(function () { /* sin conexión: queda el fallback estático */ });
  } catch (e) { /* noop */ }
})();
