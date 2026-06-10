"""Tests for the extraction/ satellite modules.

Covers date_extractor, salary_extractor, requirements_extractor,
functions_extractor, contract_extractor and attachment_parser in
isolation. field_extractors.extract_structured_fields is already
exercised by tests/test_job_pipeline.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from extraction.attachment_parser import is_relevant_attachment, parse_attachments
from extraction.contract_extractor import extract_contract_info
from extraction.date_extractor import (
    extract_dates_from_attachments,
    extract_dates_from_tables,
    extract_dates_from_text,
    resolve_best_dates,
)
from extraction.field_extractors import extract_job_title
from extraction.functions_extractor import extract_functions
from extraction.requirements_extractor import extract_requirements
from extraction.salary_extractor import extract_salary
from models.date_models import DateEvidence
from models.raw_page import RawPage


class SalaryExtractorTests(unittest.TestCase):
    def test_dollar_prefixed_amount(self):
        amount, currency, raw = extract_salary("Renta bruta $1.500.000 mensual")
        self.assertEqual(amount, 1_500_000.0)
        self.assertEqual(currency, "CLP")
        self.assertIn("1.500.000", raw)

    def test_clp_prefix(self):
        amount, currency, _ = extract_salary("Sueldo CLP 2.000.000")
        self.assertEqual(amount, 2_000_000.0)
        self.assertEqual(currency, "CLP")

    def test_pesos_prefix(self):
        # The regex requires `$`/`CLP`/`pesos` before the digits.
        amount, currency, _ = extract_salary("Renta pesos 1.200.000 brutos")
        self.assertEqual(amount, 1_200_000.0)
        self.assertEqual(currency, "CLP")

    def test_currency_keyword_must_precede_digits(self):
        # Suffix forms are NOT matched by the current regex.
        amount, _, _ = extract_salary("Renta de 1.200.000 pesos brutos")
        self.assertIsNone(amount)

    def test_no_match_returns_triple_none(self):
        self.assertEqual(extract_salary("Sin información salarial"), (None, None, None))

    def test_empty_and_none_input(self):
        self.assertEqual(extract_salary(""), (None, None, None))
        self.assertEqual(extract_salary(None), (None, None, None))

    def test_case_insensitive(self):
        amount, currency, _ = extract_salary("sueldo clp 900.000")
        self.assertEqual(amount, 900_000.0)
        self.assertEqual(currency, "CLP")

    def test_first_match_is_returned(self):
        # The pattern requires at least 4 digits; `$500` is too short.
        amount, _, _ = extract_salary("Anticipo $500 luego renta $1.500.000")
        self.assertEqual(amount, 1_500_000.0)


class ContractExtractorTests(unittest.TestCase):
    def test_honorarios(self):
        c, _, _ = extract_contract_info("Contratación a honorarios suma alzada")
        self.assertEqual(c, "honorarios")

    def test_contrata(self):
        c, _, _ = extract_contract_info("Vínculo: a contrata anual")
        self.assertEqual(c, "contrata")

    def test_planta(self):
        c, _, _ = extract_contract_info("Cargo de planta titular")
        self.assertEqual(c, "planta")

    def test_codigo_del_trabajo_with_accent(self):
        c, _, _ = extract_contract_info("Contrato regido por el Código del Trabajo")
        self.assertEqual(c, "codigo_trabajo")

    def test_codigo_del_trabajo_without_accent(self):
        c, _, _ = extract_contract_info("Codigo del trabajo articulo 22")
        self.assertEqual(c, "codigo_trabajo")

    def test_plazo_fijo(self):
        c, _, _ = extract_contract_info("Contrato plazo fijo de 6 meses")
        self.assertEqual(c, "plazo_fijo")

    def test_reemplazo(self):
        c, _, _ = extract_contract_info("Reemplazo por licencia médica")
        self.assertEqual(c, "reemplazo")

    def test_no_contract_match_returns_none(self):
        c, _, _ = extract_contract_info("Sin información de vínculo")
        self.assertIsNone(c)

    def test_workday_completa(self):
        _, w, _ = extract_contract_info("Jornada completa de 44 horas")
        self.assertEqual(w, "completa")

    def test_workday_media_jornada(self):
        _, w, _ = extract_contract_info("Se ofrece media jornada")
        self.assertEqual(w, "parcial")

    def test_workday_parcial(self):
        _, w, _ = extract_contract_info("Jornada parcial de 22 horas")
        self.assertEqual(w, "parcial")

    def test_workday_none_when_absent(self):
        _, w, _ = extract_contract_info("Sin información de jornada")
        self.assertIsNone(w)

    def test_modality_hibrida(self):
        _, _, m = extract_contract_info("Modalidad híbrida 3 días oficina")
        self.assertEqual(m, "hibrida")
        _, _, m2 = extract_contract_info("Modalidad hibrida")
        self.assertEqual(m2, "hibrida")

    def test_modality_remota_variants(self):
        for text in ["Trabajo remoto", "Teletrabajo permanente"]:
            _, _, m = extract_contract_info(text)
            self.assertEqual(m, "remota")

    def test_modality_presencial(self):
        _, _, m = extract_contract_info("Asistencia presencial obligatoria")
        self.assertEqual(m, "presencial")

    def test_hibrida_takes_precedence_over_remota(self):
        _, _, m = extract_contract_info("Modalidad híbrida con teletrabajo 2 días")
        self.assertEqual(m, "hibrida")

    def test_empty_input(self):
        self.assertEqual(extract_contract_info(""), (None, None, None))
        self.assertEqual(extract_contract_info(None), (None, None, None))

    def test_first_matching_contract_wins(self):
        # CONTRACT_PATTERNS dict order: honorarios, contrata, planta, ...
        # Both words present ⇒ first rule in iteration order wins.
        c, _, _ = extract_contract_info("Honorarios o planta según corresponda")
        self.assertEqual(c, "honorarios")


class RequirementsExtractorTests(unittest.TestCase):
    def test_required_section_captured(self):
        text = (
            "Requisitos del cargo\n"
            "Título profesional en ingeniería\n"
            "Experiencia mínima de 3 años\n"
            "Conocimientos en Python\n"
        )
        required, desirable, documents = extract_requirements(text)
        self.assertIn("Título profesional en ingeniería", required)
        self.assertIn("Experiencia mínima de 3 años", required)
        self.assertEqual(desirable, [])

    def test_desirable_section_captured(self):
        text = (
            "Requisitos deseables\n"
            "- Manejo de inglés avanzado\n"
            "- Postgrado en el área\n"
        )
        _, desirable, _ = extract_requirements(text)
        self.assertIn("Manejo de inglés avanzado", desirable)
        self.assertIn("Postgrado en el área", desirable)

    def test_single_word_lines_are_dropped(self):
        text = "Requisitos del cargo\nTítulo\nExperiencia profesional comprobable\n"
        required, _, _ = extract_requirements(text)
        self.assertNotIn("Título", required)
        self.assertIn("Experiencia profesional comprobable", required)

    def test_section_break_stops_capture(self):
        text = (
            "Requisitos del cargo\n"
            "Título profesional en el área\n"
            "Observaciones:\n"
            "No se pagarán viáticos por el proceso\n"
        )
        required, _, _ = extract_requirements(text)
        self.assertIn("Título profesional en el área", required)
        self.assertNotIn("No se pagarán viáticos por el proceso", required)

    def test_dedup_preserves_order(self):
        text = (
            "Requisitos del cargo\n"
            "Título profesional universitario\n"
            "Título profesional universitario\n"
            "Experiencia mínima 3 años\n"
        )
        required, _, _ = extract_requirements(text)
        self.assertEqual(
            required,
            ["Título profesional universitario", "Experiencia mínima 3 años"],
        )

    def test_documents_detected_anywhere(self):
        text = (
            "Debe adjuntar CV actualizado\n"
            "Certificado de título profesional\n"
            "Cédula de identidad escaneada\n"
        )
        _, _, documents = extract_requirements(text)
        self.assertEqual(len(documents), 3)

    def test_limits_applied(self):
        req_lines = ["Requisitos del cargo"] + [
            f"Requisito número {i} con dos palabras" for i in range(20)
        ]
        required, _, _ = extract_requirements("\n".join(req_lines))
        # extract_section scans up to `limit=10` lines below the header,
        # then the outer [:12] cap applies.
        self.assertLessEqual(len(required), 12)

    def test_bullet_characters_trimmed(self):
        text = "Requisitos del cargo\n- Título profesional vigente\n* Dos años de experiencia\n"
        required, _, _ = extract_requirements(text)
        self.assertIn("Título profesional vigente", required)
        self.assertIn("Dos años de experiencia", required)


class FunctionsExtractorTests(unittest.TestCase):
    def test_funciones_del_cargo_section(self):
        text = (
            "Funciones del cargo\n"
            "- Coordinar proyectos\n"
            "- Elaborar reportes mensuales\n"
            "- Liderar equipo técnico\n"
        )
        result = extract_functions(text)
        self.assertIn("Coordinar proyectos", result)
        self.assertIn("Elaborar reportes mensuales", result)
        self.assertIn("Liderar equipo técnico", result)

    def test_responsabilidades_section(self):
        text = (
            "Responsabilidades principales\n"
            "Implementar políticas institucionales\n"
            "Evaluar cumplimiento de metas\n"
        )
        result = extract_functions(text)
        self.assertIn("Implementar políticas institucionales", result)
        self.assertIn("Evaluar cumplimiento de metas", result)

    def test_objetivo_del_cargo(self):
        text = "Objetivo del cargo\nDiseñar estrategia de comunicación institucional\n"
        result = extract_functions(text)
        self.assertIn("Diseñar estrategia de comunicación institucional", result)

    def test_proposito_with_accent(self):
        text = "Propósito del cargo\nCoordinar el área de bienestar\n"
        result = extract_functions(text)
        self.assertIn("Coordinar el área de bienestar", result)

    def test_proposito_without_accent(self):
        text = "Proposito del cargo\nCoordinar el area de bienestar\n"
        result = extract_functions(text)
        self.assertIn("Coordinar el area de bienestar", result)

    def test_single_word_lines_dropped(self):
        text = "Funciones del cargo\nLiderar\nCoordinar equipos técnicos\n"
        result = extract_functions(text)
        self.assertNotIn("Liderar", result)
        self.assertIn("Coordinar equipos técnicos", result)

    def test_dedup_preserves_order(self):
        text = (
            "Funciones del cargo\n"
            "Coordinar proyectos clave\n"
            "Coordinar proyectos clave\n"
            "Elaborar presupuestos anuales\n"
        )
        result = extract_functions(text)
        self.assertEqual(
            result, ["Coordinar proyectos clave", "Elaborar presupuestos anuales"]
        )

    def test_cap_at_12(self):
        lines = ["Funciones del cargo"] + [
            f"Función número {i} descripción" for i in range(50)
        ]
        result = extract_functions("\n".join(lines))
        self.assertLessEqual(len(result), 12)

    def test_no_section_returns_empty(self):
        self.assertEqual(extract_functions("Texto sin sección de funciones"), [])

    def test_bullet_characters_trimmed(self):
        text = "Funciones del cargo\n- Redactar informes técnicos\n* Supervisar equipo\n"
        result = extract_functions(text)
        self.assertIn("Redactar informes técnicos", result)
        self.assertIn("Supervisar equipo", result)


class AttachmentParserTests(unittest.TestCase):
    def test_is_relevant_requires_pdf_and_keyword(self):
        self.assertTrue(is_relevant_attachment("https://x.cl/docs/bases-concurso.pdf"))
        self.assertTrue(is_relevant_attachment("https://x.cl/PERFIL-cargo.PDF"))
        self.assertTrue(is_relevant_attachment("https://x.cl/anexo-1.pdf"))
        self.assertTrue(is_relevant_attachment("https://x.cl/tdr.pdf"))
        self.assertTrue(
            is_relevant_attachment("https://x.cl/términos de referencia.pdf")
        )

    def test_hyphenated_keywords_not_matched(self):
        # The keyword regex uses spaces between tokens ("t[eé]rminos? de
        # referencia"), so hyphen-separated filenames slip through even
        # though a human would consider them relevant.
        self.assertFalse(
            is_relevant_attachment("https://x.cl/terminos-de-referencia.pdf")
        )

    def test_is_relevant_rejects_non_pdf(self):
        self.assertFalse(is_relevant_attachment("https://x.cl/bases-concurso.docx"))
        self.assertFalse(is_relevant_attachment("https://x.cl/perfil-cargo.html"))

    def test_is_relevant_rejects_pdf_without_keyword(self):
        self.assertFalse(is_relevant_attachment("https://x.cl/archivo.pdf"))
        self.assertFalse(is_relevant_attachment("https://x.cl/reporte-anual.pdf"))

    def test_parse_attachments_preserves_order(self):
        urls = [
            "https://x.cl/a/bases.pdf",
            "https://x.cl/b/reporte.pdf",
            "https://x.cl/c/perfil.pdf",
        ]
        parsed = parse_attachments(urls)
        self.assertEqual([p.url for p in parsed], urls)
        self.assertEqual([p.relevant for p in parsed], [True, False, True])

    def test_texts_align_by_index(self):
        urls = ["https://x.cl/bases.pdf", "https://x.cl/perfil.pdf"]
        texts = ["texto bases", "texto perfil"]
        parsed = parse_attachments(urls, texts)
        self.assertEqual(parsed[0].extracted_text, "texto bases")
        self.assertEqual(parsed[1].extracted_text, "texto perfil")

    def test_missing_text_falls_back_to_empty(self):
        parsed = parse_attachments(["https://x.cl/bases.pdf"], [])
        self.assertEqual(parsed[0].extracted_text, "")
        self.assertFalse(parsed[0].used_ocr)

    def test_ocr_flag_set_when_relevant_and_empty_and_allowed(self):
        parsed = parse_attachments(
            ["https://x.cl/bases.pdf"], [""], allow_ocr=True
        )
        self.assertTrue(parsed[0].used_ocr)
        self.assertIn("ocr_not_executed_in_unit_test_environment", parsed[0].extracted_text)

    def test_ocr_not_triggered_when_disabled(self):
        parsed = parse_attachments(["https://x.cl/bases.pdf"], [""], allow_ocr=False)
        self.assertFalse(parsed[0].used_ocr)

    def test_ocr_not_triggered_when_irrelevant(self):
        parsed = parse_attachments(
            ["https://x.cl/reporte.pdf"], [""], allow_ocr=True
        )
        self.assertFalse(parsed[0].used_ocr)

    def test_empty_inputs(self):
        self.assertEqual(parse_attachments([]), [])


class DateExtractorTests(unittest.TestCase):
    def test_cierre_de_postulacion_captured(self):
        evidences = extract_dates_from_text("Cierre de postulación: 15 de abril de 2026")
        labels = {e.label for e in evidences}
        self.assertIn("application_end", labels)

    def test_hasta_el_captured(self):
        evidences = extract_dates_from_text("Postulaciones hasta el 20/04/2026")
        labels = {e.label for e in evidences}
        self.assertIn("application_end", labels)

    def test_desde_el_maps_to_start(self):
        evidences = extract_dates_from_text("Postulaciones desde el 01/04/2026")
        labels = {e.label for e in evidences}
        self.assertIn("application_start", labels)

    def test_inicio_de_postulacion(self):
        evidences = extract_dates_from_text("Inicio de postulación: 1 de abril 2026")
        labels = {e.label for e in evidences}
        self.assertIn("application_start", labels)

    def test_publicado_captured(self):
        evidences = extract_dates_from_text("Publicado el 10 de marzo de 2026")
        labels = {e.label for e in evidences}
        self.assertIn("published", labels)

    def test_fecha_de_publicacion_with_accent(self):
        evidences = extract_dates_from_text("Fecha de publicación: 10 de marzo de 2026")
        labels = {e.label for e in evidences}
        self.assertIn("published", labels)

    def test_empty_text_yields_no_evidence(self):
        self.assertEqual(extract_dates_from_text(""), [])
        self.assertEqual(extract_dates_from_text(None), [])

    def test_unparseable_fragment_still_recorded_with_low_confidence(self):
        evidences = extract_dates_from_text("Cierre de postulación: fecha por confirmar xyz")
        # The regex should match, even if dateutil fails — falls back to 0.35.
        match = [e for e in evidences if e.label == "application_end"]
        self.assertTrue(match)

    def test_tables_source_upgraded_to_0_9(self):
        evidences = extract_dates_from_tables(["Cierre de postulación: 15 de abril 2026"])
        self.assertTrue(evidences)
        for e in evidences:
            self.assertEqual(e.source, "tables")
            self.assertGreaterEqual(e.confidence, 0.9)

    def test_attachments_source_upgraded_to_0_8(self):
        evidences = extract_dates_from_attachments(["Cierre de postulación: 15 de abril 2026"])
        self.assertTrue(evidences)
        for e in evidences:
            self.assertEqual(e.source, "attachments")
            self.assertGreaterEqual(e.confidence, 0.8)

    def test_tables_handles_empty_list(self):
        self.assertEqual(extract_dates_from_tables([]), [])
        self.assertEqual(extract_dates_from_tables(None), [])

    def test_attachments_handles_empty_list(self):
        self.assertEqual(extract_dates_from_attachments([]), [])
        self.assertEqual(extract_dates_from_attachments(None), [])


class ResolveBestDatesTests(unittest.TestCase):
    def _ev(self, label, value, confidence=0.9, raw_text="some text"):
        return DateEvidence(
            label=label, raw_text=raw_text, value=value, confidence=confidence
        )

    def test_future_end_date_not_expired(self):
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        result = resolve_best_dates([self._ev("application_end", future)])
        self.assertFalse(result.is_expired)
        self.assertIsNone(result.expiration_reason)
        self.assertEqual(result.date_confidence, "high")
        self.assertEqual(result.application_end_at, future)

    def test_past_end_date_marks_expired(self):
        past = datetime(2000, 1, 1, tzinfo=timezone.utc)
        result = resolve_best_dates([self._ev("application_end", past)])
        self.assertTrue(result.is_expired)
        self.assertEqual(result.expiration_reason, "application_end_at_in_past")

    def test_highest_confidence_wins_per_label(self):
        low = self._ev(
            "application_end",
            datetime(2099, 1, 1, tzinfo=timezone.utc),
            confidence=0.4,
        )
        high = self._ev(
            "application_end",
            datetime(2099, 12, 31, tzinfo=timezone.utc),
            confidence=0.95,
        )
        result = resolve_best_dates([low, high])
        self.assertEqual(result.application_end_at, high.value)

    def test_explicit_closed_signal_marks_expired(self):
        evidence = DateEvidence(
            label="application_end",
            raw_text="Proceso finalizado — nómina de seleccionados publicada",
            value=None,
            confidence=0.2,
        )
        result = resolve_best_dates([evidence])
        self.assertTrue(result.is_expired)
        self.assertEqual(result.expiration_reason, "explicit_closed_signal")
        self.assertEqual(result.date_confidence, "high")

    def test_historical_year_without_current_signals_expires(self):
        evidence = DateEvidence(
            label="published",
            raw_text="Publicado en 2022, concurso histórico",
            value=None,
            confidence=0.2,
        )
        result = resolve_best_dates([evidence])
        self.assertTrue(result.is_expired)
        self.assertEqual(result.expiration_reason, "historical_year_without_current_signals")

    def test_historical_year_ignored_when_current_year_also_present(self):
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        evidences = [
            self._ev(
                "application_end",
                future,
                raw_text="proceso 2022 reabierto y vigente",
            ),
        ]
        result = resolve_best_dates(evidences)
        self.assertFalse(result.is_expired)

    def test_explicit_closed_signal_overrides_future_end_date(self):
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        evidences = [
            self._ev("application_end", future, raw_text="Cierre el 2099"),
            DateEvidence(
                label="published",
                raw_text="Proceso adjudicado",
                value=None,
                confidence=0.2,
            ),
        ]
        result = resolve_best_dates(evidences)
        self.assertTrue(result.is_expired)
        self.assertEqual(result.expiration_reason, "explicit_closed_signal")

    def test_no_evidence_yields_unknown(self):
        result = resolve_best_dates([])
        self.assertIsNone(result.application_end_at)
        self.assertIsNone(result.is_expired)
        self.assertEqual(result.date_confidence, "low")

    def test_explicit_now_is_used(self):
        frozen_now = datetime(2030, 6, 15, tzinfo=timezone.utc)
        past = datetime(2030, 6, 14, tzinfo=timezone.utc)
        result = resolve_best_dates(
            [self._ev("application_end", past)], now=frozen_now
        )
        self.assertTrue(result.is_expired)

    def test_evidence_is_preserved_in_resolution(self):
        evidences = [
            self._ev("application_end", datetime(2099, 1, 1, tzinfo=timezone.utc)),
            self._ev("published", datetime(2026, 3, 1, tzinfo=timezone.utc)),
        ]
        result = resolve_best_dates(evidences)
        self.assertEqual(len(result.evidence), 2)


class ExtractJobTitleTests(unittest.TestCase):
    def _page(self, **kwargs) -> RawPage:
        base = dict(
            source_id="s1",
            source_name="Fuente",
            platform=None,
            url="https://x.cl",
            title=None,
            html_text="",
            tables_text=[],
            attachment_urls=[],
            attachment_texts=[],
            found_dates=[],
            discovered_at=datetime.now(timezone.utc),
            headings=[],
        )
        base.update(kwargs)
        return RawPage(**base)

    def test_nombre_del_cargo_takes_precedence(self):
        # extract_job_title scans title + headings + tables + attachments,
        # but NOT html_text. Pattern must be visible in one of those fields.
        page = self._page(
            title="Página institucional",
            tables_text=["Nombre del cargo: Analista Senior"],
        )
        self.assertEqual(extract_job_title(page), "Analista Senior")

    def test_cargo_pattern(self):
        page = self._page(
            title="",
            headings=["Cargo: Coordinador Regional"],
        )
        self.assertEqual(extract_job_title(page), "Coordinador Regional")

    def test_se_requiere_pattern(self):
        page = self._page(
            title="",
            attachment_texts=["Se requiere: Ingeniero Forestal"],
        )
        self.assertEqual(extract_job_title(page), "Ingeniero Forestal")

    def test_html_text_is_not_scanned_for_patterns(self):
        # Documents the current behavior: html_text is ignored by the title
        # extractor, so a pattern present only there falls through to the
        # title fallback.
        page = self._page(
            title="Concurso Público",
            html_text="Nombre del cargo: Analista Senior.",
        )
        self.assertEqual(extract_job_title(page), "Concurso Público")

    def test_heading_fallback_when_no_pattern_matches(self):
        page = self._page(
            title="Genérico",
            headings=["Jefe", "Analista de Planificación Territorial"],
        )
        self.assertEqual(
            extract_job_title(page), "Analista de Planificación Territorial"
        )

    def test_title_used_as_last_resort(self):
        page = self._page(title="Solo Título Largo Sin Patrón", headings=["Único"])
        self.assertEqual(extract_job_title(page), "Solo Título Largo Sin Patrón")

    def test_pattern_search_scans_tables_and_attachments(self):
        page = self._page(
            title="",
            tables_text=["Cargo: Arquitecto Municipal"],
        )
        self.assertEqual(extract_job_title(page), "Arquitecto Municipal")


if __name__ == "__main__":
    unittest.main()
