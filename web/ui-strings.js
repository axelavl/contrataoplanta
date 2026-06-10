/**
 * ui-strings.js — Catálogo único de microcopy del frontend.
 *
 * Convención: mantener aquí todo string visible del UI que se repite o que
 * podría variar entre vistas, para que la ficha de detalle y el listado
 * hablen el mismo idioma y los cambios editoriales sean atómicos.
 *
 * Los títulos de sección en `index.html` son literales estáticos pero deben
 * coincidir con los valores `SEC_*` de acá; usa esto como fuente de verdad.
 */
(function () {
  'use strict';

  const UI_STRINGS = Object.freeze({
    // Títulos de bloque (coinciden con index.html)
    SEC_RESUMEN: 'Resumen para decidir',
    SEC_REQUISITOS: 'Requisitos para postular',
    SEC_FUNCIONES: 'Funciones principales',
    SEC_CONDICIONES: 'Condiciones del cargo',
    SEC_OBJETIVO: 'Objetivo del cargo',
    SEC_POSTULACION: 'Cómo postular',
    SEC_AVISO: 'Texto completo del aviso',

    // Subtítulos de Requisitos
    SUB_OBLIGATORIOS: 'Obligatorios',
    SUB_DESEABLES: 'Deseables',
    SUB_FORMACION: 'Formación',
    SUB_EXPERIENCIA: 'Experiencia',
    SUB_LICENCIAS: 'Licencias y certificaciones',
    SUB_COMPETENCIAS: 'Competencias y habilidades',
    SUB_DOCUMENTOS: 'Documentos exigidos',

    // CTAs del modal
    CTA_POSTULAR: 'Ir al portal de postulación →',
    CTA_POSTULAR_OFF: 'Postulación no disponible',
    CTA_BASES: 'Ver bases oficiales',
    CTA_GUARDAR: 'Guardar',
    CTA_GUARDADA: 'Guardada',

    // Estados de plazo (usados por plazoDetalle/plazoInfo)
    ESTADO_CIERRA_HOY: 'Cierra hoy',
    ESTADO_CIERRA_MANANA: 'Cierra mañana',
    ESTADO_SIN_CIERRE: 'Sin fecha de cierre informada',

    // Fallback único para toda la ficha — sólo se muestra cuando no hay
    // ninguna sección semántica renderizable. No se usa como fallback
    // por sección: una sección sin contenido SIEMPRE se oculta.
    FALLBACK_GLOBAL: 'Revisa las bases oficiales para ver el detalle completo.',

    // Notas informativas (distintas del fallback; contextualizan calidad de datos)
    NOTE_DATOS_PARCIALES: 'Algunos datos no fueron informados claramente por la fuente. Revisa las bases para el detalle completo.',
    NOTE_INSTITUCION_AUTODETECTADA: 'Institución detectada automáticamente; revisa las bases oficiales antes de postular.',
    NOTE_UBICACION_FALTANTE: 'La ubicación no fue informada con claridad en el aviso original.',
    NOTE_SIN_FECHA_CIERRE: 'No hay fecha de cierre explícita; confirma vigencia en el portal oficial.',

    // Vista de listado
    CTA_VER_DETALLE: 'Ver detalle →',
  });

  window.UI_STRINGS = UI_STRINGS;
})();
