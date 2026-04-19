/* logo-fallback.js — búsqueda exhaustiva de logos + fallback visual por sector.
   Orden de fallback:
   1. Clearbit (size 128)
   2. DuckDuckGo icons (ip3)
   3. Google favicons (sz=128)
   4. /apple-touch-icon.png del dominio de la institución
   5. /favicon.ico del dominio
   6. Ícono SVG genérico según sector de la institución
*/
(function () {
  'use strict';

  /* Iconos genéricos consistentes con la paleta navy + gold del sitio */
  var SECTOR_ICONS = {
    municipal: iconSvg('<path d="M12 2 L3 7 h18 Z M4 9 v10 M8 9 v10 M12 9 v10 M16 9 v10 M20 9 v10 M3 21 h18"/>'),
    salud: iconSvg('<path d="M9 3 h6 v6 h6 v6 h-6 v6 h-6 v-6 H3 V9 h6 Z"/>'),
    educacion: iconSvg('<path d="M2 9 L12 4 L22 9 L12 14 Z M6 11 V17 c0 1 3 3 6 3 s6-2 6-3 V11"/>'),
    ejecutivo: iconSvg('<path d="M4 20 V8 l8-4 l8 4 V20 Z M9 20 V14 h6 v6 M9 9 h2 M13 9 h2 M9 12 h2 M13 12 h2"/>'),
    judicial: iconSvg('<path d="M12 3 V21 M6 6 h12 M5 9 h4 M15 9 h4 M4 14 c0-2 2-3 3-3 s3 1 3 3 M14 14 c0-2 2-3 3-3 s3 1 3 3"/>'),
    ffaa: iconSvg('<path d="M12 2 L4 6 V12 c0 5 4 8 8 10 c4-2 8-5 8-10 V6 Z"/>'),
    empresa: iconSvg('<path d="M3 7 h18 v13 H3 Z M8 7 V4 h8 v3 M3 11 h18 M10 15 h4"/>'),
    regional: iconSvg('<path d="M3 6 L9 3 L15 6 L21 3 V18 L15 21 L9 18 L3 21 Z M9 3 V18 M15 6 V21"/>'),
    universidad: iconSvg('<path d="M2 9 L12 4 L22 9 L12 14 Z M6 11 V17 c0 1 3 3 6 3 s6-2 6-3 V11 M19 9 V14"/>'),
    default: iconSvg('<path d="M4 20 V9 l8-5 l8 5 V20 Z M9 20 V14 h6 v6"/>')
  };

  function iconSvg(paths) {
    return (
      '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="100%" height="100%" ' +
      'fill="none" stroke="#254BA0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" ' +
      'aria-hidden="true">' + paths + '</svg>'
    );
  }

  function sectorIconFromInstitution(name) {
    var n = (name || '').toLowerCase();
    if (/municipalidad|municipal|muni\./i.test(n)) return SECTOR_ICONS.municipal;
    if (/hospital|salud|clinic|consultori|cesfam|servicio\s+de\s+salud/i.test(n)) return SECTOR_ICONS.salud;
    if (/universidad|instituto\s+profesional|centro\s+de\s+formaci/i.test(n)) return SECTOR_ICONS.universidad;
    if (/colegio|escuela|liceo|educaci/i.test(n)) return SECTOR_ICONS.educacion;
    if (/ministerio|subsecretar|superintendencia|servicio\s+de/i.test(n)) return SECTOR_ICONS.ejecutivo;
    if (/poder\s+judicial|juzgado|corte|fiscal|tribunal/i.test(n)) return SECTOR_ICONS.judicial;
    if (/fuerzas|armada|ejercito|carabineros|pdi|gendarmer/i.test(n)) return SECTOR_ICONS.ffaa;
    if (/gobierno\s+regional|intendencia|gore/i.test(n)) return SECTOR_ICONS.regional;
    if (/empresa|banco|metro|tvn|codelco|enap|enami/i.test(n)) return SECTOR_ICONS.empresa;
    return SECTOR_ICONS.default;
  }

  function domainFromImg(img) {
    if (img.dataset.domain) return img.dataset.domain;
    var src = img.src || '';
    var m =
      src.match(/logo\.clearbit\.com\/([^?#/]+)/) ||
      src.match(/duckduckgo\.com\/ip3\/([^.]+\.[^/]+)\.ico/) ||
      src.match(/google\.com\/s2\/favicons.*domain=([^&]+)/) ||
      src.match(/^https?:\/\/([^/]+)\/(?:favicon\.ico|apple-touch-icon\.png)/);
    var domain = m ? decodeURIComponent(m[1]) : '';
    if (domain) img.dataset.domain = domain;
    return domain;
  }

  function sourcesFor(domain) {
    return [
      'https://logo.clearbit.com/' + encodeURIComponent(domain) + '?size=128',
      'https://icons.duckduckgo.com/ip3/' + encodeURIComponent(domain) + '.ico',
      'https://www.google.com/s2/favicons?domain=' + encodeURIComponent(domain) + '&sz=128',
      'https://' + domain + '/apple-touch-icon.png',
      'https://' + domain + '/favicon.ico',
    ];
  }

  function replaceWithSectorIcon(img) {
    var altText = img.getAttribute('alt') || '';
    var nameMatch = altText.match(/Logo de (.+)/i);
    var institutionName = nameMatch ? nameMatch[1] : altText;

    var svg = sectorIconFromInstitution(institutionName);
    var wrapper = document.createElement('div');
    wrapper.className = 'sector-icon-fallback';
    wrapper.innerHTML = svg;
    wrapper.style.cssText =
      'width:100%;height:100%;display:flex;align-items:center;justify-content:center;' +
      'padding:18%;background:#E1E8F6;border-radius:10px;box-sizing:border-box;';
    img.replaceWith(wrapper);
  }

  window.imgFavFallback = function (img) {
    var domain = domainFromImg(img);
    if (!domain) {
      replaceWithSectorIcon(img);
      return;
    }

    var sources = sourcesFor(domain);
    var tried = parseInt(img.dataset.attempt || '0', 10);
    var nextIdx = tried + 1;
    if (nextIdx >= sources.length) {
      replaceWithSectorIcon(img);
      return;
    }
    img.dataset.attempt = String(nextIdx);
    img.src = sources[nextIdx];
  };
})();
