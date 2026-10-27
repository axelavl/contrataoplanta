// Componente "Buscar ofertas con mi CV" (POST /api/cv/match).
//
// Se monta sobre cualquier <form data-cv-form> que contenga:
//   input[type=file][data-cv-archivo], [data-cv-drop], [data-cv-nombre],
//   input[type=checkbox][data-cv-consent], button[data-cv-btn], [data-cv-estado]
// y el atributo data-cv-destino="#id" con el contenedor donde se pintan el
// perfil y las ofertas. Si el destino está dentro de un <dialog>, se abre
// como ventana (index); si no, se muestra en la página (mi-cv.html).
//
// El CV no se guarda en el navegador ni en el servidor. Todo el texto se
// inserta con textContent (nunca innerHTML con datos de la API).
(function () {
  'use strict';

  var RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';
  var host = window.location.hostname;
  var esLocal = host === 'localhost' || host === '127.0.0.1' || host === '';
  var forzada = (typeof window.__API_BASE === 'string' ? window.__API_BASE.trim() : '');
  var API_BASE = forzada ? forzada.replace(/\/+$/, '') : (esLocal ? 'http://localhost:8000' : RAILWAY_BACKEND);
  var MAX_BYTES = 5 * 1024 * 1024;
  var ETIQUETA_BANDA = { alta: 'Afinidad alta', media: 'Afinidad media', baja: 'Afinidad baja' };

  function el(tag, cls, texto) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (texto != null) n.textContent = texto;
    return n;
  }

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
    var sec = el('section', 'cv-perfil');
    sec.appendChild(el('h2', 'cv-subtitulo', 'Lo que leímos en tu CV'));
    var dl = el('dl');
    [
      ['Área', (p.areas || []).join(', ')],
      ['Formación', (p.titulos || []).join(' · ')],
      ['Experiencia', p.anios_experiencia ? '~' + p.anios_experiencia + ' años' : ''],
      ['Región', p.region || ''],
      ['Cargos previos', (p.cargos_previos || []).join(' · ')],
    ].forEach(function (f) {
      dl.appendChild(el('dt', null, f[0]));
      dl.appendChild(el('dd', null, f[1] || 'No detectado'));
    });
    sec.appendChild(dl);
    sec.appendChild(el('p', 'cv-perfil-nota',
      'Si algo no corresponde, revisa que tu CV indique claramente tu título, tus cargos con sus años (por ejemplo «2019 – actualidad: Analista…») y tu ciudad.'));
    return sec;
  }

  function pintarResultados(data) {
    var sec = el('section', 'cv-res');
    var res = data.resultados || [];
    sec.appendChild(el('h2', 'cv-subtitulo', 'Ofertas sugeridas'));
    if (res.length) {
      sec.appendChild(el('p', 'cv-res-sub', 'Revisamos ' + data.evaluadas +
        ' ofertas vigentes. Estas son las más afines; confirma siempre los requisitos en las bases antes de postular.'));
    }
    var lista = el('ol', 'cv-lista');
    if (!res.length) {
      lista.appendChild(el('li', 'cv-vacio',
        'No encontramos ofertas vigentes con suficiente afinidad. Prueba más adelante (se publican ofertas nuevas a diario) o usa el buscador.'));
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
    sec.appendChild(lista);
    return sec;
  }

  function montar(form) {
    var q = function (s) { return form.querySelector(s); };
    var input = q('[data-cv-archivo]');
    var drop = q('[data-cv-drop]');
    var nombre = q('[data-cv-nombre]');
    var consent = q('[data-cv-consent]');
    var btn = q('[data-cv-btn]');
    var estado = q('[data-cv-estado]');
    var destino = document.querySelector(form.getAttribute('data-cv-destino'));
    var dialog = destino && destino.closest('dialog');
    if (!input || !consent || !btn || !destino) return;
    var archivo = null;

    function setEstado(msg, esError) {
      estado.textContent = msg || '';
      estado.classList.toggle('cv-estado--error', !!esError);
    }
    function actualizarBoton() { btn.disabled = !(archivo && consent.checked); }

    function elegir(f) {
      archivo = null;
      nombre.textContent = '';
      setEstado('');
      if (f) {
        if (!/\.(pdf|docx)$/i.test(f.name)) {
          setEstado('Formato no soportado: sube un PDF o un Word (.docx).', true);
        } else if (f.size > MAX_BYTES) {
          setEstado('El archivo supera los 5 MB.', true);
        } else {
          archivo = f;
          nombre.textContent = f.name;
        }
      }
      actualizarBoton();
    }

    input.addEventListener('change', function () { elegir(input.files && input.files[0]); });
    consent.addEventListener('change', actualizarBoton);
    if (drop) {
      ['dragenter', 'dragover'].forEach(function (ev) {
        drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.add('is-over'); });
      });
      ['dragleave', 'drop'].forEach(function (ev) {
        drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.remove('is-over'); });
      });
      drop.addEventListener('drop', function (e) {
        elegir(e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]);
      });
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
          destino.textContent = '';
          destino.appendChild(pintarPerfil(data.perfil || {}));
          destino.appendChild(pintarResultados(data));
          destino.hidden = false;
          if (dialog && typeof dialog.showModal === 'function') {
            if (!dialog.open) dialog.showModal();
            dialog.scrollTop = 0;
          } else {
            destino.scrollIntoView({ behavior: 'smooth', block: 'start' });
          }
        })
        .catch(function (err) {
          var msg = err && err.message;
          if (!msg || /Failed to fetch|NetworkError|Load failed/i.test(msg)) {
            msg = 'No pudimos conectar con el servidor. Revisa tu conexión e intenta de nuevo.';
          }
          setEstado(msg, true);
        })
        .then(actualizarBoton);
    });
  }

  function montarDialogos() {
    document.querySelectorAll('dialog.cv-dialog').forEach(function (d) {
      d.querySelectorAll('[data-cv-cerrar]').forEach(function (b) {
        b.addEventListener('click', function () { d.close(); });
      });
      // Clic en el fondo (fuera del panel) cierra.
      d.addEventListener('click', function (e) { if (e.target === d) d.close(); });
    });
  }

  function iniciar() {
    document.querySelectorAll('form[data-cv-form]').forEach(montar);
    montarDialogos();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
  else iniciar();
})();
