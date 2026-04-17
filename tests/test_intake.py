"""Tests para scrapers/intake.py — la capa de validación transversal."""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from scrapers.intake import (
    ANTIGUEDAD_DESCARTE_DIAS,
    ANTIGUEDAD_OK_DIAS,
    ANTIGUEDAD_REVISION_DIAS,
    RENTA_MAX_SOSPECHOSA,
    assess_minimum_fields,
    assess_salary,
    assess_vigencia,
    intake_validate_offer,
    is_garbage_text,
    is_garbage_url,
    is_internal_only,
)


class GarbageTextTests(unittest.TestCase):
    def test_resultado_es_basura(self):
        self.assertTrue(is_garbage_text("Resultados del concurso público N° 12/2024"))
        self.assertTrue(is_garbage_text("Nómina de seleccionados disponible aquí"))
        self.assertTrue(is_garbage_text("Lista de seleccionados publicada"))
        self.assertTrue(is_garbage_text("Proceso adjudicado al postulante X"))
        self.assertTrue(is_garbage_text("Convocatoria cerrada"))

    def test_noticia_es_basura(self):
        self.assertTrue(is_garbage_text("Comunicado oficial sobre el proceso"))
        self.assertTrue(is_garbage_text("Boletín mensual de la institución"))

    def test_evento_es_basura(self):
        self.assertTrue(is_garbage_text("Cuenta pública 2025 en vivo"))
        self.assertTrue(is_garbage_text("Seminario regional de salud"))

    def test_compras_no_es_oferta_laboral(self):
        self.assertTrue(is_garbage_text("Licitación pública 1234-A"))
        self.assertTrue(is_garbage_text("Subvención municipal vigente"))
        self.assertTrue(is_garbage_text("Fondos concursables abiertos"))

    def test_oferta_real_no_es_basura(self):
        self.assertFalse(is_garbage_text("Concurso público para Analista de Personas"))
        self.assertFalse(is_garbage_text("Ingeniero de Datos a contrata, grado 12 EUS"))
        self.assertFalse(
            is_garbage_text("Llamado a postulación: Director(a) de Operaciones")
        )

    def test_palabras_aisladas_no_descartan(self):
        # 'cerrado' NO debe gatillar; debe ser frase explícita.
        self.assertFalse(is_garbage_text("Cargo: cerrador en planta de proceso"))
        # 'noticia' aislada no debe gatillar 'noticias?' bound; pero 'noticia' sí
        # — caso real: si aparece la palabra "noticia" usualmente sí es basura.
        self.assertTrue(is_garbage_text("Noticia institucional de la semana"))


class GarbageURLTests(unittest.TestCase):
    def test_url_de_noticias(self):
        for url in (
            "https://muni.cl/noticias/abril-2026",
            "https://x.gob.cl/blog/post-1",
            "https://x.cl/comunicados/2025/marzo",
            "https://x.cl/agenda/eventos",
            "https://x.cl/cuenta-publica-2025",
        ):
            self.assertTrue(is_garbage_url(url), url)

    def test_url_laboral_ok(self):
        for url in (
            "https://muni.cl/concursos-publicos/llamado-1",
            "https://x.cl/trabaja-con-nosotros/oferta",
            "https://x.cl/empleos/2026/analista",
        ):
            self.assertFalse(is_garbage_url(url), url)


class InternalOnlyTests(unittest.TestCase):
    def test_solo_difusion_descartada(self):
        self.assertTrue(is_internal_only("Solo Difusión"))
        self.assertTrue(is_internal_only("Solo Difusión Interna"))
        self.assertTrue(is_internal_only("Vacante - difusion interna"))
        self.assertFalse(is_internal_only("Convocatoria pública abierta"))


class SalaryAssessmentTests(unittest.TestCase):
    def test_renta_normal_pasa(self):
        lo, hi, motivo = assess_salary(800_000, 1_500_000, "renta bruta mensual")
        self.assertEqual(lo, 800_000)
        self.assertEqual(hi, 1_500_000)
        self.assertIsNone(motivo)

    def test_monto_inverosimil_descarta(self):
        lo, hi, motivo = assess_salary(None, RENTA_MAX_SOSPECHOSA + 1)
        self.assertIsNone(lo)
        self.assertIsNone(hi)
        self.assertEqual(motivo, "renta_descarte_monto_inverosimil")

    def test_monto_alto_sin_contexto_no_confiable(self):
        lo, hi, motivo = assess_salary(None, 11_000_000, "presupuesto del programa")
        self.assertIsNone(lo)
        self.assertIsNone(hi)
        self.assertEqual(motivo, "renta_no_confiable_sin_contexto")

    def test_monto_alto_con_contexto_pasa(self):
        lo, hi, motivo = assess_salary(
            None, 11_000_000, "remuneracion bruta mensual del cargo"
        )
        self.assertEqual(hi, 11_000_000)
        self.assertIsNone(motivo)

    def test_monto_invertido_se_ordena(self):
        lo, hi, _ = assess_salary(2_000_000, 1_000_000, "renta")
        self.assertEqual(lo, 1_000_000)
        self.assertEqual(hi, 2_000_000)

    def test_un_solo_extremo_se_replica(self):
        lo, hi, _ = assess_salary(1_500_000, None, "sueldo mensual")
        self.assertEqual(lo, 1_500_000)
        self.assertEqual(hi, 1_500_000)

    def test_ambos_none_no_motivo(self):
        lo, hi, motivo = assess_salary(None, None)
        self.assertIsNone(lo)
        self.assertIsNone(hi)
        self.assertIsNone(motivo)


class VigenciaTests(unittest.TestCase):
    def test_cierre_pasado_descarta(self):
        descartar, motivo, review = assess_vigencia(
            None, date.today() - timedelta(days=1)
        )
        self.assertTrue(descartar)
        self.assertEqual(motivo, "fecha_cierre_vencida")
        self.assertFalse(review)

    def test_cierre_futuro_pasa(self):
        descartar, motivo, review = assess_vigencia(
            date.today() - timedelta(days=200),  # publicación antigua...
            date.today() + timedelta(days=10),   # ...pero cierre futuro vigente
        )
        self.assertFalse(descartar)
        self.assertIsNone(motivo)
        self.assertFalse(review)

    def test_publicacion_en_zona_revision(self):
        descartar, motivo, review = assess_vigencia(
            date.today() - timedelta(days=ANTIGUEDAD_OK_DIAS + 5),
            None,
        )
        self.assertFalse(descartar)
        self.assertTrue(review)

    def test_publicacion_supera_180_dias_descarta(self):
        descartar, motivo, _ = assess_vigencia(
            date.today() - timedelta(days=ANTIGUEDAD_REVISION_DIAS + 5),
            None,
        )
        self.assertTrue(descartar)
        self.assertEqual(motivo, "publicacion_excede_180_dias_sin_cierre")

    def test_publicacion_supera_365_dias_descarta(self):
        descartar, motivo, _ = assess_vigencia(
            date.today() - timedelta(days=ANTIGUEDAD_DESCARTE_DIAS + 5),
            None,
        )
        self.assertTrue(descartar)
        self.assertEqual(motivo, "publicacion_excede_365_dias")

    def test_sin_fechas_pasa(self):
        descartar, motivo, review = assess_vigencia(None, None)
        self.assertFalse(descartar)
        self.assertFalse(review)


class MinimumFieldsTests(unittest.TestCase):
    def test_sin_cargo_descarta(self):
        d, motivo = assess_minimum_fields({"url_oferta": "https://x.cl/1"})
        self.assertTrue(d)
        self.assertEqual(motivo, "sin_cargo")

    def test_sin_url_descarta(self):
        d, motivo = assess_minimum_fields({"cargo": "Analista"})
        self.assertTrue(d)
        self.assertEqual(motivo, "sin_url")

    def test_cargo_demasiado_corto_descarta(self):
        d, motivo = assess_minimum_fields(
            {"cargo": "X", "url_oferta": "https://x.cl/1"}
        )
        self.assertTrue(d)
        self.assertEqual(motivo, "cargo_demasiado_corto")

    def test_minimo_pasa(self):
        d, _ = assess_minimum_fields(
            {"cargo": "Analista de Personas", "url_oferta": "https://x.cl/1"}
        )
        self.assertFalse(d)


class IntakeValidateOfferTests(unittest.TestCase):
    def _offer(self, **kw) -> dict:
        base = {
            "cargo": "Analista de Personas",
            "url_oferta": "https://x.cl/concursos/1",
        }
        base.update(kw)
        return base

    def test_oferta_normal_pasa(self):
        decision = intake_validate_offer(self._offer())
        self.assertFalse(decision.discard)
        self.assertFalse(decision.needs_review)

    def test_solo_difusion_descarta(self):
        d = intake_validate_offer(self._offer(cargo="Solo Difusión"))
        self.assertTrue(d.discard)
        self.assertEqual(d.motivo_descarte, "solo_difusion_interna")

    def test_url_basura_descarta(self):
        d = intake_validate_offer(
            self._offer(url_oferta="https://x.cl/noticias/abril-2026")
        )
        self.assertTrue(d.discard)
        self.assertEqual(d.motivo_descarte, "url_no_laboral")

    def test_cargo_es_resultado_descarta(self):
        d = intake_validate_offer(
            self._offer(cargo="Resultados del concurso público anual")
        )
        self.assertTrue(d.discard)

    def test_cierre_pasado_descarta(self):
        d = intake_validate_offer(
            self._offer(fecha_cierre=date.today() - timedelta(days=2))
        )
        self.assertTrue(d.discard)
        self.assertEqual(d.motivo_descarte, "fecha_cierre_vencida")

    def test_renta_inverosimil_se_limpia_y_marca_review(self):
        offer = self._offer(
            renta_bruta_min=20_000_000,
            renta_bruta_max=20_000_000,
            renta_texto="Presupuesto programa anual",
        )
        d = intake_validate_offer(offer)
        self.assertFalse(d.discard)
        self.assertTrue(d.needs_review)
        self.assertIsNone(offer["renta_bruta_min"])
        self.assertIsNone(offer["renta_bruta_max"])
        self.assertEqual(
            offer["renta_validation_status"], "renta_descarte_monto_inverosimil"
        )

    def test_publicacion_antigua_marca_review_pero_no_descarta(self):
        offer = self._offer(
            fecha_publicacion=date.today() - timedelta(days=ANTIGUEDAD_OK_DIAS + 10),
        )
        d = intake_validate_offer(offer)
        self.assertFalse(d.discard)
        self.assertTrue(d.needs_review)
        self.assertEqual(offer.get("needs_review"), True)

    def test_publicacion_muy_antigua_descarta(self):
        d = intake_validate_offer(
            self._offer(
                fecha_publicacion=date.today()
                - timedelta(days=ANTIGUEDAD_REVISION_DIAS + 10),
            )
        )
        self.assertTrue(d.discard)

    def test_oferta_con_palabra_noticia_pero_cargo_real_marca_review(self):
        # Caso real: el blob tiene "noticia" pero el cargo es claramente
        # un puesto profesional. Debe pasar pero marcado para revisión.
        offer = self._offer(
            cargo="Jefe(a) de Comunicaciones Internas",
            descripcion="Se busca profesional con experiencia en gestión de noticias.",
        )
        d = intake_validate_offer(offer)
        self.assertFalse(d.discard)
        self.assertTrue(d.needs_review)

    def test_oferta_periodista_con_blob_noticia_no_se_descarta(self):
        # Evita falsos negativos por lista cerrada de cargos "válidos".
        offer = self._offer(
            cargo="Periodista",
            descripcion="Cobertura de noticias institucionales y contenidos web.",
        )
        d = intake_validate_offer(offer)
        self.assertFalse(d.discard)
        self.assertTrue(d.needs_review)


if __name__ == "__main__":
    unittest.main()
