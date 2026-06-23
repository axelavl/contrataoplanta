/* ──────────────────────────────────────────────────────────────────────────
   escala-eus-data.js · Datos de la Escala Única de Sueldos (EUS)
   ──────────────────────────────────────────────────────────────────────────
   Fuente: escala de remuneraciones publicada en Transparencia Activa
   (Subsecretaría del Interior). Los montos corresponden al TOTAL DE HABERES
   imponible mensual por estamento y grado, e incluyen sueldo base, incremento
   D.L. 3.500 y las asignaciones generales de la Administración Central.

   IMPORTANTE — esto es REFERENCIAL, no la liquidación exacta de un cargo:
   - Cada servicio tiene asignaciones propias (modernización, zona, bienios,
     responsabilidad, etc.) que cambian el total final.
   - El reajuste del sector público 2026 (Ley 21.806, +3,4% en dos tramos)
     se aplica sobre estos valores; usa REAJUSTE_2026 para proyectar.
   - El líquido depende del sistema de salud (Fonasa 7% vs. Isapre variable),
     AFP y tipo de vínculo (planta/contrata).

   Estructura: window.ESCALA_EUS = { meta, sueldoBase, estamentos }
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  // Sueldo base mensual por grado (común a los estamentos de la EUS).
  var sueldoBase = {
    3: 671113, 4: 633142, 5: 608584, 6: 574091, 7: 529171, 8: 489934,
    9: 453597, 10: 420027, 11: 388940, 12: 360127, 13: 333440, 14: 308692,
    15: 285846, 16: 264620, 17: 245026, 18: 226883, 19: 212046, 20: 198183,
    21: 185200, 22: 173098
  };

  /* Total de haberes imponible por estamento → grado.
     Valores tomados directamente de la escala publicada. */
  var estamentos = {
    directivo: {
      etiqueta: 'Directivo (profesional)',
      descripcion: 'Jefaturas con título profesional. Incluye asignación profesional y de responsabilidad.',
      grados: {
        3: 3148847, 4: 3000214, 5: 2750121, 6: 2502917, 7: 2275282,
        8: 2068839, 9: 1881459, 10: 1725897
      }
    },
    directivo_no_prof: {
      etiqueta: 'Directivo (no profesional)',
      descripcion: 'Jefaturas sin título profesional habilitante.',
      grados: {
        4: 1809914, 5: 1751079, 6: 1660826, 7: 1533738,
        8: 1422679, 9: 1293407, 10: 1187946
      }
    },
    profesional: {
      etiqueta: 'Profesional',
      descripcion: 'Cargos que exigen título profesional. Incluye asignación profesional.',
      grados: {
        4: 2691694, 5: 2606198, 6: 2452766, 7: 2249330, 8: 2048904,
        9: 1882585, 10: 1726902, 11: 1587127, 12: 1458715, 13: 1328188,
        14: 1219118, 15: 1119204
      }
    },
    tecnico: {
      etiqueta: 'Técnico / Administrativo / Auxiliar',
      descripcion: 'Estamentos técnico, administrativo y auxiliar de la EUS.',
      grados: {
        10: 870325, 11: 822954, 12: 784697, 13: 744223, 14: 704277,
        15: 668726, 16: 626271, 17: 596284, 18: 570867, 19: 547957,
        20: 514822, 21: 487666, 22: 445691
      }
    }
  };

  // Reajuste sector público 2026 (Ley 21.806): 2,0% (dic-2025) + 1,4% (jun-2026).
  var REAJUSTE_2026 = 0.034;

  // Descuentos previsionales estimados sobre el imponible (referencial):
  // AFP ~11,4% (10% + comisión prom.) + Salud 7% + Seguro cesantía 0,6% (contrata).
  var DESCUENTO_ESTIMADO = {
    planta: 0.184,    // sin seguro de cesantía
    contrata: 0.19    // con seguro de cesantía
  };

  window.ESCALA_EUS = {
    meta: {
      fuente: 'Transparencia Activa — Subsecretaría del Interior (escala EUS)',
      nota: 'Valores referenciales del total de haberes imponible. La liquidación real varía por servicio, asignaciones específicas, sistema de salud y antigüedad.',
      reajuste2026: REAJUSTE_2026
    },
    sueldoBase: sueldoBase,
    estamentos: estamentos,
    descuento: DESCUENTO_ESTIMADO,

    /* Devuelve {bruto, brutoReajustado, base, liquidoEstimado} o null. */
    calcular: function (estamento, grado, opts) {
      opts = opts || {};
      var est = estamentos[estamento];
      if (!est) return null;
      var bruto = est.grados[grado];
      if (bruto == null) return null;
      var base = sueldoBase[grado] || null;
      var conReajuste = opts.aplicarReajuste !== false;
      var brutoFinal = conReajuste ? Math.round(bruto * (1 + REAJUSTE_2026)) : bruto;
      var tasaDesc = DESCUENTO_ESTIMADO[opts.vinculo === 'planta' ? 'planta' : 'contrata'];
      var liquido = Math.round(brutoFinal * (1 - tasaDesc));
      return {
        bruto: bruto,
        brutoReajustado: brutoFinal,
        base: base,
        liquidoEstimado: liquido,
        tasaDescuento: tasaDesc,
        reajusteAplicado: conReajuste
      };
    },

    gradosDe: function (estamento) {
      var est = estamentos[estamento];
      if (!est) return [];
      return Object.keys(est.grados).map(Number).sort(function (a, b) { return a - b; });
    }
  };
})();
