// Buscar ofertas con mi CV: sube el archivo a POST /api/cv/match y pinta
// el perfil detectado y las ofertas sugeridas. El CV no se guarda en el
// navegador ni en el servidor. Todo el texto se inserta con textContent
// (nada de innerHTML con datos de la API).
(function () {
  'use strict';

  var RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
  var host = window.location.hostname;
  var esLocal = host === 'localhost' || host === '127.0.0.1' || host === '';
  var forzada = (typeof window.__API_BASE === 'string' ? window.__API_BASE.trim() : '');
  var API_BASE = forzada ? forzada.replace(/\/+$/, '') : (esLocal ? 'http://localhost:8000' : RAILWAY_BACKEND);
  var MAX_BYTES = 5 * 1024 * 1024;

  var form = document.getElementById('cv-form');
  var input = document.getElementById('cv-archivo');
  var drop = document.getElementById('cv-drop');
  var nombre = document.getElementById('cv-archivo-nombre');
  var consent = document.getElementById('cv-consent');
  var btn = document.getElementById('cv-btn');
  var estado = document.getElementById('cv-estado');
  var secPerfil = document.getElementById('cv-perfil');
  var dlPerfil = document.getElementById('cv-perfil-datos');
  var secRes = document.getElementById('cv-res');
  var subRes = document.getElementById('cv-res-sub');
  var lista = document.getElementById('cv-lista');

  var archivo = null;

  function el(tag, cls, texto) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (texto != null) n.textContent = texto;
    return n;
  }

  function setEstado(msg, esError) {
    estado.textContent = msg || '';
    estado.className = 'cv-estado' + (esError ? ' cv-estado--error' : '');
  }

  function actualizarBoton() {
    btn.disabled = !(archivo && consent.checked);
  }

  function elegir(f) {
    archivo = null;
    nombre.textContent = '';
    setEstado('');
    if (!f) { actualizarBoton(); return; }
    if (!/\.(pdf|docx)$/i.test(f.name)) {
      setEstado('Formato no soportado: sube un PDF o un Word (.docx).', true);
    } else if (f.size > MAX_BYTES) {
      setEstado('El archivo supera los 5 MB.', true);
    } else {
      archivo = f;
      nombre.textContent = f.name;
    }
    actualizarBoton();
  }

  input.addEventListener('change', function () { elegir(input.files && input.files[0]); });
  consent.addEventListener('change', actualizarBoton);
  ['dragenter', 'dragover'].forEach(function (ev) {
    drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.add('is-over'); });
  });
  ['dragleave', 'drop'].forEach(function (ev) {
    drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.remove('is-over'); });
  });
  drop.addEventListener('drop', function (e) {
    var f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    elegir(f);
  });

  function fechaLarga(iso) {
    if (!iso) return '';
    var d = new Date(iso + 'T12:00:00');
    if (isNaN(d)) return '';
    return d.toLocaleDateString('es-CL', { day: 'numeric', month: 'long' });
  }

  function rutaOferta(o) {
    if (typeof window.rutaOferta === 'function') return window.rutaOferta(o.id, o.cargo);
    return '/oferta/' + encodeURIComponent(o.id);
  }

  function pintarPerfil(p) {
    dlPerfil.textContent = '';
    var filas = [
      ['Área', (p.areas || []).join(', ')],
      ['Formación', (p.titulos || []).join(' · ')],
      ['Experiencia', p.anios_experiencia ? '~' + p.anios_experiencia + ' años' : ''],
      ['Región', p.region || ''],
      ['Cargos previos', (p.cargos_previos || []).join(' · ')],
    ];
    filas.forEach(function (f) {
      dlPerfil.appendChild(el('dt', null, f[0]));
      dlPerfil.appendChild(el('dd', null, f[1] || 'No detectado'));
    });
    secPerfil.hidden = false;
  }

  var ETIQUETA_BANDA = { alta: 'Afinidad alta', media: 'Afinidad media', baja: 'Afinidad baja' };

  function pintarResultados(data) {
    lista.textContent = '';
    var res = data.resultados || [];
    subRes.textContent = res.length
      ? 'Revisamos ' + data.evaluadas + ' ofertas vigentes. Estas son las más afines; confirma siempre los requisitos en las bases antes de postular.'
      : '';
    if (!res.length) {
      lista.appendChild(el('li', 'cv-vacio',
        'No encontramos ofertas vigentes con suficiente afinidad. Prueba más adelante (se publican ofertas nuevas a diario) o busca manualmente en el buscador.'));
    }
    res.forEach(function (r) {
      var o = r.oferta || {};
      var li = el('li', 'cv-item');
      var top = el('div', 'cv-item-top');
      var info = el('div');
      info.appendChild(el('div', 'cv-item-inst', o.institucion || ''));
      var h = el('h3', 'cv-item-cargo');
      var a = el('a', null, o.cargo || 'Oferta');
      a.href = rutaOferta(o);
      h.appendChild(a);
      info.appendChild(h);
      top.appendChild(info);
      top.appendChild(el('span', 'cv-banda cv-banda--' + r.banda, ETIQUETA_BANDA[r.banda] || r.banda));
      li.appendChild(top);

      var meta = [o.region, o.tipo_contrato, o.fecha_cierre ? 'Cierra el ' + fechaLarga(o.fecha_cierre) : '']
        .filter(Boolean).join(' · ');
      if (meta) li.appendChild(el('div', 'cv-item-meta', meta));

      var ul = el('ul', 'cv-motivos');
      (r.razones || []).forEach(function (t) { ul.appendChild(el('li', 'si', t)); });
      (r.reparos || []).forEach(function (t) { ul.appendChild(el('li', 'no', t)); });
      li.appendChild(ul);
      lista.appendChild(li);
    });
    secRes.hidden = false;
  }

  form.addEventListener('submit', function (e) {
    e.preventDefault();
    if (!archivo || !consent.checked) return;
    var fd = new FormData();
    fd.append('archivo', archivo, archivo.name);
    fd.append('consentimiento', 'true');
    btn.disabled = true;
    setEstado('Leyendo tu CV y comparándolo con las ofertas vigentes…');

    fetch(API_BASE + '/api/cv/match', { method: 'POST', body: fd })
      .then(function (resp) {
        return resp.json().catch(function () { return {}; }).then(function (body) {
          if (!resp.ok) {
            var msg = typeof body.detail === 'string' ? body.detail : '';
            if (resp.status === 429) msg = 'Hiciste varias búsquedas seguidas. Espera unos minutos y vuelve a intentarlo.';
            throw new Error(msg || 'No pudimos procesar tu CV. Intenta de nuevo.');
          }
          return body;
        });
      })
      .then(function (data) {
        setEstado('');
        pintarPerfil(data.perfil || {});
        pintarResultados(data);
        secPerfil.scrollIntoView({ behavior: 'smooth', block: 'start' });
      })
      .catch(function (err) {
        var msg = err && err.message;
        if (!msg || /Failed to fetch|NetworkError|Load failed/i.test(msg)) {
          msg = 'No pudimos conectar con el servidor. Revisa tu conexión e intenta de nuevo.';
        }
        setEstado(msg, true);
      })
      .then(function () { actualizarBoton(); });
  });
})();
