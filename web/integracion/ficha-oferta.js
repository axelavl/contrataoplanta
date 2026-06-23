/* ============================================================================
 * ficha-oferta.js — Ficha (modal) de detalle de oferta para contrata o planta
 * ----------------------------------------------------------------------------
 * - Estructura jerárquica fija: cabecera → motivo de coincidencia → plazo →
 *   resumen → objetivo → requisitos → funciones → condiciones → cómo postular.
 * - OCULTA automáticamente los títulos cuyo dato no existe o llega vacío.
 * - Si la oferta llegó por una búsqueda (palabra clave / profesión / región),
 *   RESALTA en el texto lo que el usuario buscó y explica por qué aparece.
 * Vanilla JS. Expone window.FichaOferta. Requiere profesiones.js (opcional,
 * para resaltar por familia) y estilos.css.
 *
 * Esquema de `oferta` (mapealo desde tu API; los vacíos se ocultan):
 *   {
 *     cargo, institucion, verificada:Boolean, sector, tipo, region, comuna,
 *     jornada, renta:Number|null, fechaPublicacion:String, fechaCierre:String,
 *     diasRestantes:Number, portal:String, portalUrl:String, basesUrl:String|null,
 *     objetivo:String|null,
 *     funciones:[String],
 *     requisitos:{ obligatorios:[], formacion:[], experiencia:[],
 *                  especialidades:[], competencias:[], documentos:[], deseables:[] },
 *     condiciones:[String],
 *     comoPostular:[String]
 *   }
 *
 * Uso:
 *   FichaOferta.abrir(oferta, {
 *     query: 'abogada',          // texto que buscó el usuario (opcional)
 *     profesion: 'juridico',     // familia seleccionada (opcional)
 *     region: 'Metropolitana',   // región filtrada (opcional)
 *     onPostular: (oferta) => location.assign(oferta.portalUrl)
 *   });
 * ========================================================================== */
(function (global) {
  'use strict';

  const norm = (s) => (s || '').toString().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
  const escHtml = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const escRx = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  // Acepta Number (lo formatea) o String ya formateado (lo deja tal cual,
  // p.ej. rangos "$1.200.000 – $1.800.000" o con grado EUS). null => oculta.
  const fmtRenta = (n) => {
    if (n == null) return null;
    if (typeof n === 'string') { const s = n.trim(); return s || null; }
    return isNaN(n) ? null : '$' + Number(n).toLocaleString('es-CL');
  };

  // ── Reconocimiento de correo de contacto ──────────────────────────────
  // Opera sobre el texto ya scrapeado (descripción + requisitos), así sirve
  // para CUALQUIER fuente. Clasifica por contexto: postulación / consultas /
  // contacto, y devuelve el más relevante.
  const _EMAIL_RX = /[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}/ig;
  const _HINTS_POST = ['enviar antecedentes', 'remitir postulac', 'recepc', 'postular', 'hacer llegar', 'enviar cv', 'enviar su cv', 'enviar documentos', 'curriculum a', 'cv a', 'antecedentes a'];
  const _HINTS_CONS = ['consulta', 'duda', 'informaci', 'contacto', 'escribir a', 'comunicarse', 'mesa de ayuda'];
  const _normTxt = (s) => (s || '').toString().toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
  function extraerCorreoOferta(texto) {
    const src = texto || '';
    if (!src) return null;
    const out = [];
    let m;
    _EMAIL_RX.lastIndex = 0;
    while ((m = _EMAIL_RX.exec(src)) !== null) {
      const email = m[0].toLowerCase();
      if (out.some((e) => e.email === email)) continue;
      const ctx = _normTxt(src.slice(Math.max(0, m.index - 90), m.index + email.length + 60));
      let tipo = 'contacto';
      if (_HINTS_POST.some((h) => ctx.indexOf(h) >= 0)) tipo = 'postulacion';
      else if (_HINTS_CONS.some((h) => ctx.indexOf(h) >= 0)) tipo = 'consultas';
      out.push({ email, tipo });
    }
    if (!out.length) return null;
    const orden = { postulacion: 0, consultas: 1, contacto: 2 };
    out.sort((a, b) => orden[a.tipo] - orden[b.tipo]);
    const best = out[0];
    const label = best.tipo === 'postulacion' ? 'Postular por correo'
      : best.tipo === 'consultas' ? 'Consultas'
        : 'Correo de contacto';
    return { email: best.email, tipo: best.tipo, label };
  }
  global.extraerCorreoOferta = extraerCorreoOferta;

  let TERMS = [];
  function hl(text) {
    const out = escHtml(text);
    if (!TERMS.length) return out;
    try {
      const rx = new RegExp('(' + TERMS.map(escRx).join('|') + ')', 'gi');
      return out.replace(rx, '<mark class="cop-hl">$1</mark>');
    } catch (e) { return out; }
  }

  // Humaniza valores de máquina a etiquetas legibles. Nunca debe sobrevivir
  // snake_case en la UI (Parte 1.4). Mapa explícito para los casos conocidos
  // y fallback genérico (guion_bajo → "Palabras Capitalizadas") para el resto.
  var _VALOR_LABELS = {
    codigo_trabajo: 'Código del Trabajo',
    planta: 'Planta', contrata: 'Contrata', honorarios: 'Honorarios',
    suplencia: 'Suplencia', reemplazo: 'Reemplazo', otro: 'Otro',
    no_informa: 'No informa', sin_datos: '', no_aplica: '',
  };
  function humanizarValor(v) {
    if (v == null) return '';
    var s = String(v).trim();
    if (!s) return '';
    var key = s.toLowerCase();
    if (Object.prototype.hasOwnProperty.call(_VALOR_LABELS, key)) return _VALOR_LABELS[key];
    // Fallback: sólo para valores de máquina (con guion bajo o todo minúscula);
    // los textos ya legibles se devuelven tal cual.
    if (s.indexOf('_') !== -1 || s === s.toLowerCase()) {
      return s.replace(/_+/g, ' ').replace(/\s+/g, ' ').trim()
        .replace(/\b\w/g, function (c) { return c.toUpperCase(); });
    }
    return s;
  }

  // --- helpers de bloques (devuelven '' si no hay datos => se ocultan) ---
  const kv = (k, v, isRenta) => v ? `<div class="cop-kv"><div class="cop-kv-k">${k}</div><div class="cop-kv-v${isRenta ? ' is-renta' : ''}">${escHtml(v)}</div></div>` : '';
  const sub = (t, items) => (items && items.length)
    ? `<div class="cop-sub"><h6>${t}</h6><ul class="cop-list">${items.map((x) => `<li>${hl(x)}</li>`).join('')}</ul></div>` : '';
  const sec = (t, items, check) => (items && items.length)
    ? `<div class="cop-sec"><div class="cop-sec-t">${t}</div><ul class="cop-list${check ? ' is-check' : ''}">${items.map((x) => `<li>${hl(x)}</li>`).join('')}</ul></div>` : '';

  const ICONO_VERIF = '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true"><path d="M12 2 4 5v6c0 5 3.4 8.5 8 11 4.6-2.5 8-6 8-11V5l-8-3Z" opacity=".22"/><path d="m9.5 12 1.8 1.8 3.4-3.6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>';

  let overlay = null;
  function montar() {
    if (overlay) return;
    overlay = document.createElement('div');
    overlay.className = 'cop-modal-overlay';
    overlay.innerHTML = `
      <div class="cop-modal" role="dialog" aria-modal="true" aria-labelledby="cop-cargo">
        <div class="cop-head">
          <button class="cop-close" type="button" aria-label="Cerrar">✕</button>
          <div class="cop-kicker" id="cop-kicker"></div>
          <div class="cop-inst" id="cop-inst"></div>
          <h2 class="cop-cargo" id="cop-cargo"></h2>
          <div class="cop-tags" id="cop-tags"></div>
        </div>
        <div class="cop-why" id="cop-why" hidden></div>
        <div class="cop-plazo" id="cop-plazo">
          <div class="cop-plazo-ic"><svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg></div>
          <div><div class="cop-plazo-l">Postulaciones</div><div class="cop-plazo-v" id="cop-plazo-v"></div></div>
        </div>
        <div class="cop-body" id="cop-body"></div>
        <div class="cop-actions">
          <button class="cop-apply" id="cop-apply" type="button">Ir al portal de postulación →</button>
          <div class="cop-actions-sec">
            <button class="cop-btn2" id="cop-bases" type="button">Ver bases</button>
            <button class="cop-btn2" id="cop-cmp" type="button" hidden>⇆ Comparar</button>
            <button class="cop-btn2" id="cop-share" type="button">↗ Compartir</button>
            <button class="cop-btn2" id="cop-fav" type="button">♡ Guardar</button>
          </div>
          <p class="cop-portal" id="cop-portal"></p>
        </div>
      </div>`;
    document.body.appendChild(overlay);
    overlay.querySelector('.cop-close').addEventListener('click', cerrar);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) cerrar(); });
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && overlay.classList.contains('is-open')) cerrar(); });
  }

  function diasLabel(d, fecha) {
    let estado = 'ok', txt;
    if (d == null) { estado = 'unknown'; txt = 'Plazo no informado'; }
    else if (d < 0) { estado = 'closed'; txt = 'Postulaciones cerradas'; }
    else if (d === 0) { estado = 'urg'; txt = 'Cierra hoy'; }
    else if (d === 1) { estado = 'urg'; txt = 'Cierra mañana'; }
    else if (d <= 5) { estado = 'mid'; txt = 'Cierra en ' + d + ' días'; }
    else { estado = 'ok'; txt = 'Cierra en ' + d + ' días'; }
    if (fecha) txt += ' · ' + fecha;
    return { estado, txt };
  }

  function abrir(oferta, ctx) {
    montar();
    ctx = ctx || {};

    // términos a resaltar + razones de coincidencia
    TERMS = [];
    const razones = [];
    const q = (ctx.query || '').trim();
    if (q.length >= 2) {
      TERMS.push(q);
      razones.push(['Palabra clave', q]);
      if (global.Profesiones) {
        const fam = global.Profesiones.detectar(q);
        if (fam) TERMS.push.apply(TERMS, global.Profesiones.terminos(fam));
      }
    }
    if (ctx.profesion && global.Profesiones && global.Profesiones.FAMILIAS[ctx.profesion]) {
      TERMS.push.apply(TERMS, global.Profesiones.terminos(ctx.profesion));
      razones.push(['Profesión', global.Profesiones.FAMILIAS[ctx.profesion].label]);
    }
    if (ctx.region) { TERMS.push(ctx.region); razones.push(['Región', ctx.region]); }

    const $ = (id) => overlay.querySelector('#' + id);

    // cabecera
    $('cop-kicker').textContent = [oferta.sector, oferta.tipo].filter(Boolean).join(' · ');
    $('cop-inst').innerHTML = hl(oferta.institucion || '') +
      (oferta.verificada ? ` <span class="cop-vf" title="Institución verificada">${ICONO_VERIF}</span>` : '');
    $('cop-cargo').innerHTML = hl(oferta.cargo || '');
    const ubic = [oferta.region, oferta.comuna].filter(Boolean).join(' · ');
    $('cop-tags').innerHTML = [oferta.tipo, ubic, oferta.jornada].filter(Boolean)
      .map((t) => `<span class="cop-tag">${escHtml(t)}</span>`).join('');

    // motivo de coincidencia
    const why = $('cop-why');
    if (razones.length) {
      why.hidden = false;
      why.innerHTML = `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 7.5h.01"/></svg>
        <div>Coincide con tu búsqueda: ${razones.map((r) => `<span class="cop-qk">${escHtml(r[1])}</span>`).join(' · ')} — lo resaltamos más abajo.</div>`;
    } else { why.hidden = true; why.innerHTML = ''; }

    // plazo
    const pl = diasLabel(oferta.diasRestantes, oferta.fechaCierre);
    const mp = $('cop-plazo'); mp.className = 'cop-plazo is-' + pl.estado;
    $('cop-plazo-v').textContent = pl.txt;

    // resumen (oculta lo vacío)
    // Modalidad ya viene humanizada (oferta.tipo); calidad jurídica puede
    // llegar cruda ("codigo_trabajo") → humanizar. 6.4: si ambas coinciden,
    // no repetir el mismo valor en dos campos (mostramos sólo Modalidad).
    const _modalidadLbl = oferta.tipo || '';
    const _calidadLbl = humanizarValor(oferta.calidadJuridica);
    const _calidadMostrar = (_calidadLbl && _calidadLbl.toLowerCase() === String(_modalidadLbl).toLowerCase())
      ? '' : _calidadLbl;
    // 6.5: vacantes 0 → "No especificado" (no "0"); ausente → se oculta.
    const _vacantesLbl = oferta.numeroVacantes === 0 ? 'No especificado'
      : (oferta.numeroVacantes != null ? String(oferta.numeroVacantes) : '');
    const grid = `<div class="cop-grid">
      ${kv('Renta bruta', fmtRenta(oferta.renta), true)}
      ${kv('Jornada', oferta.jornada)}
      ${kv('Modalidad', _modalidadLbl)}
      ${kv('Calidad jurídica', _calidadMostrar)}
      ${kv('Estamento', humanizarValor(oferta.estamento))}
      ${kv('Vacantes', _vacantesLbl)}
      ${kv('Ubicación', ubic)}
      ${kv('Lugar de desempeño', oferta.lugarDesempenio)}
      ${kv('Publicación', oferta.fechaPublicacion)}
    </div>`;

    const objetivo = oferta.objetivo
      ? `<div class="cop-sec"><div class="cop-sec-t">Objetivo del cargo</div><p class="cop-objetivo">${hl(oferta.objetivo)}</p></div>` : '';

    // Correo de contacto (reconocido del texto de la oferta).
    const contacto = oferta.email
      ? `<div class="cop-sec cop-contacto"><div class="cop-sec-t">Contacto</div><p class="cop-contacto-p">${escHtml(oferta.emailLabel || 'Correo')}: <a class="cop-mail" href="mailto:${escHtml(oferta.email)}">${escHtml(oferta.email)}</a></p>${oferta.emailConsultas ? `<p class="cop-contacto-p">Correo de consultas: <a class="cop-mail" href="mailto:${escHtml(oferta.emailConsultas)}">${escHtml(oferta.emailConsultas)}</a></p>` : ''}</div>` : '';

    const rq = oferta.requisitos || {};
    const reqInner = sub('Obligatorios', rq.obligatorios) + sub('Formación', rq.formacion) +
      sub('Experiencia', rq.experiencia) + sub('Licencias y certificaciones', rq.especialidades) +
      sub('Competencias y habilidades', rq.competencias) + sub('Documentos exigidos', rq.documentos) +
      sub('Deseables', rq.deseables);
    // Los subgrupos fluyen en 2 columnas (.cop-subgrid) para reducir la altura.
    const requisitos = reqInner
      ? `<div class="cop-sec"><div class="cop-sec-t">Requisitos para postular</div><div class="cop-subgrid">${reqInner}</div></div>` : '';

    // Parte 6.6 — tabla compacta de renta por región (solo si el backend la
    // entregó parseada; si no, queda el campo "Renta bruta" del grid).
    const _rentaReg = Array.isArray(oferta.rentaRegional) ? oferta.rentaRegional : [];
    const _fmtPesos = (n) => (n == null ? '—' : '$' + Number(n).toLocaleString('es-CL'));
    const _thS = 'padding:6px 8px;border-bottom:1px solid var(--borde,#e5e5e5);color:var(--texto3,#6b7280);font-weight:700;font-size:11px;text-transform:uppercase;letter-spacing:.4px';
    const _tdS = 'padding:6px 8px;border-bottom:1px solid var(--borde,#eee);font-size:13px';
    const rentaTabla = _rentaReg.length
      ? `<div class="cop-sec"><div class="cop-sec-t">Renta por región</div>`
        + `<table style="width:100%;border-collapse:collapse;margin-top:4px">`
        + `<thead><tr><th style="${_thS};text-align:left">Región</th><th style="${_thS};text-align:right">Sin bono</th><th style="${_thS};text-align:right">Con bono</th></tr></thead>`
        + `<tbody>` + _rentaReg.map((r) =>
            `<tr><td style="${_tdS}">${escHtml(r.region || '—')}</td>`
            + `<td style="${_tdS};text-align:right">${_fmtPesos(r.sin_bono)}</td>`
            + `<td style="${_tdS};text-align:right">${_fmtPesos(r.con_bono)}</td></tr>`).join('')
        + `</tbody></table></div>`
      : '';

    $('cop-body').innerHTML = grid + rentaTabla + contacto + objetivo + requisitos +
      sec('Funciones principales', oferta.funciones) +
      sec('Condiciones del cargo', oferta.condiciones) +
      sec('Cómo postular', oferta.comoPostular, true);

    // acciones
    $('cop-bases').style.display = oferta.basesUrl ? '' : 'none';
    $('cop-portal').textContent = oferta.portal ? ('Te llevamos a ' + oferta.portal + ' · sitio oficial del organismo') : '';
    $('cop-apply').onclick = () => (ctx.onPostular ? ctx.onPostular(oferta) : (oferta.portalUrl && window.open(oferta.portalUrl, '_blank', 'noopener')));
    $('cop-bases').onclick = () => (ctx.onBases ? ctx.onBases(oferta) : (oferta.basesUrl && window.open(oferta.basesUrl, '_blank', 'noopener')));
    // Botón guardar: refleja estado inicial (ctx.guardada) y alterna al click.
    let _guardada = !!ctx.guardada;
    const _btnFav = $('cop-fav');
    const _pintarFav = () => {
      _btnFav.innerHTML = _guardada ? '♥ Guardada' : '♡ Guardar';
      _btnFav.classList.toggle('is-activo', _guardada);
    };
    _pintarFav();
    _btnFav.onclick = () => {
      if (ctx.onGuardar) ctx.onGuardar(oferta);
      _guardada = !_guardada;
      _pintarFav();
    };

    // Botón comparar (solo si el host pasó onComparar): alterna agregar/quitar.
    const _btnCmp = $('cop-cmp');
    if (ctx.onComparar) {
      _btnCmp.hidden = false;
      let _cmp = !!ctx.comparando;
      const _pintarCmp = () => {
        _btnCmp.innerHTML = _cmp ? '✓ Comparar oferta' : '⇆ Comparar oferta';
        _btnCmp.classList.toggle('is-activo', _cmp);
      };
      _pintarCmp();
      _btnCmp.onclick = () => {
        // onComparar devuelve el nuevo estado (true=en comparador, false=fuera).
        _cmp = ctx.onComparar(oferta) === true;
        _pintarCmp();
      };
    } else {
      _btnCmp.hidden = true;
    }

    // Compartir: navigator.share nativo o copia el enlace al portapapeles.
    const _btnShare = $('cop-share');
    _btnShare.textContent = '↗ Compartir';
    _btnShare.onclick = () => {
      const url = oferta.shareUrl || oferta.portalUrl || (global.location && global.location.href) || '';
      const titulo = oferta.cargo || 'Oferta de empleo público';
      const texto = titulo + (oferta.institucion ? ' — ' + oferta.institucion : '');
      if (ctx.onCompartir) { ctx.onCompartir(oferta); return; }
      if (global.navigator && global.navigator.share) {
        global.navigator.share({ title: titulo, text: texto, url: url }).catch(function () {});
      } else if (global.navigator && global.navigator.clipboard && url) {
        global.navigator.clipboard.writeText(url).then(function () {
          _btnShare.textContent = '✓ Enlace copiado';
          setTimeout(function () { _btnShare.textContent = '↗ Compartir'; }, 1800);
        }).catch(function () {});
      }
    };

    overlay.classList.add('is-open');
    overlay.scrollTop = 0;
    document.body.style.overflow = 'hidden';
  }

  function cerrar() {
    if (!overlay) return;
    overlay.classList.remove('is-open');
    document.body.style.overflow = '';
  }

  global.FichaOferta = { abrir, cerrar };
})(window);
