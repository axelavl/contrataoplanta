"""Tests para los refinamientos de heurísticas del rule engine.

Los refinamientos incluyen:

1. Detección histórica relativa al año actual (no años hardcodeados).
2. ``news_without_deadline_guard`` que sólo se gatilla por contexto
   estructural (URL, breadcrumbs, section_hint, título), no por mención
   casual en el cuerpo del aviso.
3. Versión del ruleset publicada en el rule_trace.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from classification.content_classifier import ContentClassifier
from classification.policy import POSITIVE_KEYWORDS, RULESET_VERSION
from classification.rule_engine import RuleEngine, _historical_year_patterns
from models.raw_page import RawPage


def make_page(**overrides) -> RawPage:
    base = dict(
        source_id="src",
        source_name="Test",
        platform="generic",
        url="https://muni.cl/concursos/oferta-1",
        final_url=None,
        title="Concurso Público Profesional",
        meta_description=None,
        breadcrumbs=["Inicio", "Concursos"],
        section_hint="concursos",
        html_text="Cargo: Profesional. Requisitos del cargo: título profesional. Fecha de cierre: 15 de mayo de 2026.",
        tables_text=[],
        attachment_urls=[],
        attachment_texts=[],
        found_dates=[],
        discovered_at=datetime.now(timezone.utc),
        http_status=200,
        headings=["Concurso Público Profesional"],
    )
    base.update(overrides)
    return RawPage(**base)


class HistoricalYearTests(unittest.TestCase):
    def test_pattern_uses_current_year_window(self):
        hist_re, current_re = _historical_year_patterns(date(2026, 5, 1))
        # años 2020-2024 (inclusive) son históricos en 2026.
        self.assertRegex("2022", hist_re)
        self.assertRegex("2024", hist_re)
        # 2025/2026/2027 son current_hint.
        self.assertRegex("2025", current_re)
        self.assertRegex("2027", current_re)
        self.assertNotRegex("2018", current_re)

    def test_pattern_rolls_with_year(self):
        # En 2027, 2025 deja de ser current y 2026 sigue siendo válido como
        # publication-year (porque es current_year - 1).
        hist_re_2027, current_re_2027 = _historical_year_patterns(date(2027, 1, 15))
        self.assertRegex("2025", hist_re_2027)
        self.assertNotRegex("2025", current_re_2027)
        self.assertRegex("2026", current_re_2027)

    def test_is_historical_uses_today(self):
        # Texto con sólo año 2022 y sin current hint: histórico.
        self.assertTrue(RuleEngine._is_historical("Proceso 2022 finalizado", today=date(2026, 5, 1)))
        # Mismo texto pero con un hint de vigencia: NO histórico.
        self.assertFalse(
            RuleEngine._is_historical("Concurso 2022, vigente para 2026", today=date(2026, 5, 1))
        )
        # Texto con sólo año 2026: NO histórico (no hay año viejo).
        self.assertFalse(RuleEngine._is_historical("Concurso 2026", today=date(2026, 5, 1)))


class NewsContextGuardTests(unittest.TestCase):
    def test_news_in_body_only_does_not_trigger(self):
        # Un cargo legítimo que dice "comunicado" en la descripción no debe
        # ser penalizado por news_without_deadline_guard.
        page = make_page(
            url="https://muni.cl/trabaja-con-nosotros/director-comunicaciones",
            title="Director(a) de Comunicaciones",
            html_text=(
                "Cargo: Director(a) de Comunicaciones. Funciones del cargo: liderar "
                "el comunicado oficial del municipio. Requisitos del cargo: título "
                "profesional. Fecha de cierre: 30 de mayo de 2026."
            ),
        )
        result = RuleEngine().classify_with_rules(page)
        rule_ids = {tr.rule_id for tr in result.rule_trace}
        self.assertNotIn("news_without_deadline_guard", rule_ids)

    def test_news_in_url_triggers_guard(self):
        page = make_page(
            url="https://muni.cl/noticias/llamado-a-concurso",
            title="Llamado a concurso publicado",
            breadcrumbs=["Inicio", "Noticias"],
            section_hint="noticias",
            html_text="El municipio llamó a concurso. Más información disponible.",
        )
        result = RuleEngine().classify_with_rules(page)
        rule_ids = {tr.rule_id for tr in result.rule_trace}
        self.assertIn("news_without_deadline_guard", rule_ids)


class RulesetVersionTests(unittest.TestCase):
    def test_rule_trace_carries_version(self):
        page = make_page()
        result = RuleEngine().classify_with_rules(page)
        version_traces = [tr for tr in result.rule_trace if tr.rule_id == "ruleset_version"]
        self.assertEqual(len(version_traces), 1)
        self.assertEqual(version_traces[0].reason, RULESET_VERSION)


class NewPositiveKeywordsTests(unittest.TestCase):
    def test_positive_recall_for_oferta_laboral(self):
        # 'oferta laboral' es una nueva positive keyword. Debe contribuir
        # al score positivo cuando aparece.
        page = make_page(
            title="Oferta laboral - Profesional grado 12",
            html_text=(
                "Oferta laboral abierta. Cargo: Profesional. Requisitos del cargo: "
                "título profesional. Fecha de cierre: 31 de mayo de 2026."
            ),
        )
        result = RuleEngine().classify_with_rules(page)
        # Buscar señal positiva relacionada a 'oferta laboral'.
        matched = any("oferta laboral" in s.lower() for s in result.positive_signals)
        self.assertTrue(matched, f"positive_signals: {result.positive_signals}")

    def test_positive_keywords_are_normalized(self):
        # Sanity check: las keywords nuevas existen en el set canónico.
        kws = {k.lower() for k in POSITIVE_KEYWORDS}
        self.assertIn("oferta laboral", kws)
        self.assertIn("vacantes", kws)
        self.assertIn("terminos de referencia", kws)


class SaladePrensaUrlPartTests(unittest.TestCase):
    def test_sala_de_prensa_url_part_penalizes(self):
        page = make_page(
            url="https://muni.cl/sala-de-prensa/comunicado-oficial",
            title="Comunicado oficial",
            breadcrumbs=["Inicio", "Sala de Prensa"],
            section_hint="sala-de-prensa",
            html_text="Comunicado del alcalde sobre el plan anual.",
        )
        result = ContentClassifier().classify(page)
        self.assertFalse(result.is_job_posting)


if __name__ == "__main__":
    unittest.main()
