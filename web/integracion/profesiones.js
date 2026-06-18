/* ============================================================================
 * profesiones.js — Buscador por profesión para contrata o planta .cl
 * ----------------------------------------------------------------------------
 * Agrupa los cargos en FAMILIAS profesionales (no por institución) y reconoce
 * variantes en femenino y masculino: "abogado" y "abogada" caen en el mismo
 * grupo. Vanilla JS, sin dependencias. Expone window.Profesiones.
 *
 * Uso típico:
 *   // 1) pintar los chips de familias (con recuentos opcionales)
 *   Profesiones.renderChips(document.getElementById('chips'), {
 *     counts: { salud: 342, educacion: 88, ... },   // opcional
 *     selected: 'all',
 *     onSelect: (familiaKey) => { ...vuelve a pedir ofertas al backend... }
 *   });
 *
 *   // 2) detectar la familia desde texto libre (input de búsqueda)
 *   const fam = Profesiones.detectar('abogada');   // -> 'juridico'
 *
 *   // 3) expandir una familia a términos para tu query SQL / API
 *   Profesiones.terminos('salud'); // -> ['enfermer','matron','kinesi',...]
 * ========================================================================== */
(function (global) {
  'use strict';

  /** Familias profesionales. `variantes` se muestran al usuario;
   *  `raices` son lexemas (sin género) para hacer match en cargos. */
  const FAMILIAS = {
    salud:       { label: 'Salud',
                   variantes: ['médico/a', 'enfermero/a', 'matrón/a', 'kinesiólogo/a', 'TENS', 'nutricionista'],
                   raices: ['enfermer', 'matron', 'kinesi', 'medic', 'médic', 'tens', 'nutricion', 'clinic', 'clínic', 'salud', 'paramedic', 'odontolog', 'tecnologo medic'] },
    educacion:   { label: 'Educación',
                   variantes: ['profesor/a', 'educador/a de párvulos', 'docente'],
                   raices: ['profesor', 'educador', 'parvul', 'párvul', 'docente', 'pedag', 'asistente de la educacion'] },
    juridico:    { label: 'Jurídico / Legal',
                   variantes: ['abogado/a', 'fiscalizador/a', 'procurador/a'],
                   raices: ['abogad', 'fiscalizador', 'procurador', 'juridic', 'jurídic', 'litig', 'legal', 'derecho'] },
    ingenieria:  { label: 'Ingeniería',
                   variantes: ['ingeniero/a civil', 'ingeniero/a comercial'],
                   raices: ['ingenier', 'obra', 'proyecto', 'construccion'] },
    admin:       { label: 'Administración y RR.HH.',
                   variantes: ['administrativo/a', 'analista', 'recursos humanos'],
                   raices: ['administrativ', 'analista', 'recursos humanos', 'rrhh', 'secretari', 'gestion', 'vinculacion'] },
    psicosocial: { label: 'Psicosocial',
                   variantes: ['psicólogo/a', 'trabajador/a social', 'asistente social'],
                   raices: ['psicolog', 'psicólog', 'trabajador social', 'trabajadora social', 'asistente social', 'psicosocial'] },
    finanzas:    { label: 'Finanzas y Contabilidad',
                   variantes: ['contador/a', 'auditor/a'],
                   raices: ['contad', 'auditor', 'tributari', 'financ', 'finanz', 'contabil', 'presupuest'] },
    ti:          { label: 'Tecnología / TI',
                   variantes: ['desarrollador/a', 'analista de sistemas', 'soporte'],
                   raices: ['informatic', 'informátic', 'desarrollador', 'programador', 'sistemas', 'soporte ti', 'ciberseg', 'datos'] }
  };

  /** Orden de aparición de los chips. */
  const ORDEN = ['salud', 'educacion', 'juridico', 'ingenieria', 'admin', 'psicosocial', 'finanzas', 'ti'];

  /** Normaliza: minúsculas, sin tildes, sin espacios extremos. */
  const norm = (s) => (s || '').toString().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();

  /** Devuelve la clave de familia que mejor calza con un texto libre, o null.
   *  Reconoce femenino y masculino vía los lexemas `raices`. */
  function detectar(texto) {
    const q = norm(texto);
    if (q.length < 3) return null;
    for (const key of ORDEN) {
      for (const raiz of FAMILIAS[key].raices) {
        const r = norm(raiz);
        if (q.includes(r) || r.includes(q)) return key;
      }
    }
    return null;
  }

  /** ¿El cargo (string) pertenece a la familia? Útil para clasificar ofertas
   *  en el cliente si tu backend aún no entrega la familia. */
  function clasificarCargo(cargo) {
    const c = norm(cargo);
    for (const key of ORDEN) {
      if (FAMILIAS[key].raices.some((r) => c.includes(norm(r)))) return key;
    }
    return null;
  }

  /** Lexemas de una familia, para construir tu WHERE / filtro en la API. */
  function terminos(familiaKey) {
    return FAMILIAS[familiaKey] ? FAMILIAS[familiaKey].raices.slice() : [];
  }

  /** Pinta la fila de chips. Devuelve una función para re-pintar (sync de estado). */
  function renderChips(container, opts) {
    opts = opts || {};
    const counts = opts.counts || null;
    let selected = opts.selected || 'all';
    const onSelect = opts.onSelect || function () {};

    function paint() {
      const total = counts ? Object.values(counts).reduce((a, b) => a + b, 0) : null;
      let html = chip('all', 'Todas', total, selected === 'all');
      ORDEN.forEach((k) => {
        const n = counts ? (counts[k] || 0) : null;
        if (counts && !n) return; // si hay recuentos, oculta familias vacías
        html += chip(k, FAMILIAS[k].label, n, selected === k);
      });
      container.innerHTML = html;
    }
    function chip(key, label, n, on) {
      const badge = (n === null || n === undefined) ? '' : `<span class="cop-chip-n">${n}</span>`;
      return `<button type="button" class="cop-chip${on ? ' is-on' : ''}" data-fam="${key}">${label} ${badge}</button>`;
    }

    container.addEventListener('click', (e) => {
      const b = e.target.closest('[data-fam]');
      if (!b) return;
      selected = b.dataset.fam;
      paint();
      onSelect(selected === 'all' ? null : selected);
    });

    paint();
    return {
      set: (key) => { selected = key || 'all'; paint(); },
      get: () => selected
    };
  }

  global.Profesiones = {
    FAMILIAS, ORDEN, norm,
    detectar, clasificarCargo, terminos, renderChips
  };
})(window);
