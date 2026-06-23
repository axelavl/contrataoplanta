/* cursos-sugeridos.js · En Favoritos, sugiere cursos del Estado afines a los
   cargos que el usuario guardó. No agrega backend: infiere el área desde el
   texto del cargo y la cruza con el directorio de cursos (GET /api/cursos, con
   fallback a window.CURSOS_DATA). CSP-safe: archivo externo, sin inline.

   Se monta sobre #cursos-sugeridos. Si no hay favoritos o no hay cursos
   afines, no muestra nada (el contenedor queda oculto). */
(function () {
  'use strict';

  var cont = document.getElementById('cursos-sugeridos');
  if (!cont) return;

  var FAV_KEY = 'fav_contrataoplanta';
  function getFavs() {
    try { return JSON.parse(localStorage.getItem(FAV_KEY) || '[]'); } catch (e) { return []; }
  }
  function fold(s) {
    return String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
  }
  function esc(s) {
    return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  var favs = getFavs();
  if (!favs.length) return; // sin favoritos no hay nada que sugerir

  // Heurística cargo → categoría de curso. El orden importa: es la prioridad
  // con la que se decide a qué área pertenece un cargo ambiguo.
  var REGLAS = [
    ['salud',         /enferm|medic|salud|clinic|odonto|matron|kinesi|nutricion|tens|farmac|psicolog|fonoaud|paramedic|obstetr|terapeut|dental|hospital/],
    ['educacion',     /profesor|docente|educ|parvul|pedagog|escuela|liceo|jardin infantil|asistente de la educacion/],
    ['compras',       /compra|adquisic|abastec|licitac|mercado publico|chilecompra|convenio marco/],
    ['finanzas',      /contad|contab|finan|presupuest|tesorer|remuneracion|auditor|sigfe|cobranza|recaudac|control de gestion/],
    ['derecho',       /abogad|juridic|legal|fiscal|sumari|derecho/],
    ['ti',            /informatic|desarroll|software|program|sistemas|tecnolog|ciberseg|analista de datos|soporte tecnico|redes|base de datos/],
    ['prevencion',    /prevencion de riesgo|prevencionista|seguridad laboral|experto en prevencion|higiene/],
    ['rrhh',          /recursos humanos|rrhh|gestion de personas|reclutam|bienestar de personal|capacitacion/],
    ['atencion',      /atencion a|atencion de|atencion al|atencion ciudadan|oirs|orientacion al usuario|atencion de publico/],
    ['admin-publica', /administrativ|administrad|gestion|jefatur|encargad|profesional|tecnico|analista|secretari|coordinad|jefe|director/]
  ];

  function categoriaDeCargo(txt) {
    var t = fold(txt);
    for (var i = 0; i < REGLAS.length; i++) {
      if (REGLAS[i][1].test(t)) return REGLAS[i][0];
    }
    return 'admin-publica';
  }

  // Cuenta cuántos favoritos caen en cada categoría → prioriza las más fuertes.
  var conteo = {};
  favs.forEach(function (f) {
    var cat = categoriaDeCargo((f.cargo || '') + ' ' + (f.institucion || ''));
    conteo[cat] = (conteo[cat] || 0) + 1;
  });
  var catsOrdenadas = Object.keys(conteo).sort(function (a, b) { return conteo[b] - conteo[a]; });

  // API base (mismo patrón que cursos.js / favoritos.js).
  var RAILWAY = 'https://contrataoplanta-production.up.railway.app';
  var _h = location.hostname;
  var API = window.__API_BASE ||
    ((_h === 'localhost' || _h === '127.0.0.1' || _h === '') ? 'http://localhost:8000' : RAILWAY);

  function fallback() {
    var D = window.CURSOS_DATA || {};
    return { cursos: D.avisos || [], categorias: D.categorias || [] };
  }

  Promise.all([
    fetch(API + '/api/cursos').then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
    fetch(API + '/api/cursos/categorias').then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
  ]).then(function (res) {
    var fb = fallback();
    var cursos = (res[0] && Array.isArray(res[0].cursos) && res[0].cursos.length) ? res[0].cursos : fb.cursos;
    var cats = (res[1] && Array.isArray(res[1].categorias) && res[1].categorias.length) ? res[1].categorias : fb.categorias;
    pintar(cursos, cats);
  });

  function etiquetaDe(cats, slug) {
    var c = cats.find(function (x) { return x.slug === slug; });
    return c ? c.etiqueta : slug;
  }

  function pintar(cursos, cats) {
    if (!cursos || !cursos.length) return;

    // Selección: recorre las categorías sugeridas (las más repetidas primero),
    // junta cursos sin repetir, gratis arriba, hasta 3.
    var elegidos = [];
    var vistos = {};
    catsOrdenadas.forEach(function (cat) {
      cursos.filter(function (c) { return c.categoria === cat; })
        .sort(function (a, b) { return (b.gratuito ? 1 : 0) - (a.gratuito ? 1 : 0); })
        .forEach(function (c) {
          if (!vistos[c.id] && elegidos.length < 3) { vistos[c.id] = 1; elegidos.push(c); }
        });
    });
    // Relleno por si las categorías afines tenían pocos cursos.
    if (elegidos.length < 3) {
      cursos.forEach(function (c) {
        if (!vistos[c.id] && elegidos.length < 3) { vistos[c.id] = 1; elegidos.push(c); }
      });
    }
    if (!elegidos.length) return;

    var topCat = catsOrdenadas[0] || 'todas';
    var cards = elegidos.map(function (c) {
      var grat = c.gratuito ? '<span class="cs-grat">Gratis</span>' : '';
      var rel = c.gratuito ? 'noopener' : 'noopener nofollow sponsored';
      var cta = (c.url && c.url !== '#')
        ? '<a class="cs-cta" href="' + c.url + '" target="_blank" rel="' + rel + '">Ir al curso →</a>'
        : '';
      return '<article class="cs-card">' +
        '<div class="cs-cat">' + esc(etiquetaDe(cats, c.categoria)) + grat + '</div>' +
        '<h3 class="cs-tit">' + esc(c.titulo) + '</h3>' +
        '<div class="cs-prov">' + esc(c.proveedor) + '</div>' +
        cta +
      '</article>';
    }).join('');

    cont.innerHTML =
      '<div class="cs-head">' +
        '<h2 class="cs-h2">Cursos que te pueden servir</h2>' +
        '<p class="cs-sub">Según los cargos que guardaste. Capacitación gratuita del Estado.</p>' +
      '</div>' +
      '<div class="cs-grid">' + cards + '</div>' +
      '<a class="cs-vermas" href="cursos.html?cat=' + encodeURIComponent(topCat) + '">Ver más cursos en Capacítate →</a>';
    cont.style.display = 'block';
  }
})();
