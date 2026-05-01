"""Tests para scrapers.evaluation.classification_quality_audit.

Cubren:

- Carga desde JSON / JSONL.
- Cálculo correcto de distribuciones por decisión y reason_code.
- Detección de anomalías (publish con URL negativa, reject con sólo
  positivos, review por encima del umbral, etc.).
- Sugerencias de heurísticas (negative patterns / URL parts) con discriminación
  reject/publish y soporte mínimo.
- Determinismo y robustez frente a entradas vacías.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scrapers.evaluation.classification_quality_audit import (
    ClassificationQualityAudit,
    iter_records_auto,
    iter_records_from_json,
    iter_records_from_jsonl,
)


def _record(**overrides) -> dict:
    base = {
        "oferta_id": 1,
        "fuente_id": 1,
        "institucion_id": 100,
        "institucion_nombre": "Inst Test",
        "plataforma": "wordpress",
        "decision": "publish",
        "primary_reason_code": None,
        "reason_codes": [],
        "reason_detail": None,
        "quality_score": 0.9,
        "policy_score": 0.5,
        "classification_score": 0.85,
        "url_oferta": "https://test.cl/concursos/x",
        "cargo": "Analista de Personas",
        "policy_reason_codes": ["positive_signals_detected", "policy_accept"],
        "positive_signals": ["palabra clave positiva: cargo"],
        "negative_signals": [],
        "rule_trace": [],
        "is_job_posting": True,
        "used_llm": False,
        "needs_review": False,
        "created_at": "2026-04-30T12:00:00Z",
    }
    base.update(overrides)
    return base


class EmptyInputTests(unittest.TestCase):
    def test_runs_on_empty(self):
        report = ClassificationQualityAudit().run([])
        self.assertEqual(report.total_records, 0)
        self.assertEqual(report.by_decision, [])
        self.assertEqual(report.suggestions, [])


class DistributionTests(unittest.TestCase):
    def test_decision_counts_and_percentages(self):
        records = [
            _record(decision="publish"),
            _record(decision="publish"),
            _record(decision="reject"),
            _record(decision="review"),
        ]
        report = ClassificationQualityAudit().run(records)
        decision_map = {d.decision: d for d in report.by_decision}
        self.assertEqual(decision_map["publish"].count, 2)
        self.assertEqual(decision_map["publish"].percentage, 0.5)
        self.assertEqual(decision_map["reject"].count, 1)
        self.assertEqual(decision_map["review"].count, 1)

    def test_score_summary_uses_only_numeric(self):
        records = [
            _record(decision="publish", quality_score=0.9),
            _record(decision="publish", quality_score=0.8),
            _record(decision="publish", quality_score=None),
        ]
        report = ClassificationQualityAudit().run(records)
        publish = [d for d in report.by_decision if d.decision == "publish"][0]
        self.assertEqual(publish.quality_score["count"], 2)
        # mean = (0.9 + 0.8) / 2
        self.assertAlmostEqual(publish.quality_score["mean"], 0.85, places=3)

    def test_reason_codes_aggregated(self):
        records = [
            _record(decision="reject", primary_reason_code="not_job_related",
                    reason_codes=["not_job_related", "missing_publishable_url"]),
            _record(decision="reject", primary_reason_code="not_job_related",
                    reason_codes=["not_job_related"]),
            _record(decision="review", primary_reason_code="missing_validity_signal",
                    reason_codes=["missing_validity_signal"]),
        ]
        report = ClassificationQualityAudit().run(records)
        codes = {r.reason_code: r for r in report.by_reason_code}
        self.assertEqual(codes["not_job_related"].count, 2)
        self.assertEqual(codes["missing_publishable_url"].count, 1)
        self.assertEqual(codes["missing_validity_signal"].count, 1)


class AnomalyDetectionTests(unittest.TestCase):
    def test_publish_with_negative_url(self):
        records = [
            _record(decision="publish", url_oferta="https://muni.cl/noticias/oferta-fake"),
            _record(decision="publish", url_oferta="https://muni.cl/concursos/real"),
        ]
        report = ClassificationQualityAudit().run(records)
        anomalies = {a.name: a for a in report.anomalies}
        self.assertIn("publish_with_negative_url", anomalies)
        self.assertEqual(anomalies["publish_with_negative_url"].count, 1)

    def test_reject_with_only_positive_signals(self):
        records = [
            _record(
                decision="reject",
                positive_signals=["URL de reclutamiento"],
                negative_signals=[],
            ),
        ]
        report = ClassificationQualityAudit().run(records)
        anomalies = {a.name: a for a in report.anomalies}
        self.assertIn("reject_with_only_positive_signals", anomalies)
        self.assertEqual(anomalies["reject_with_only_positive_signals"].count, 1)

    def test_review_above_accept_threshold(self):
        records = [_record(decision="review", quality_score=0.92)]
        report = ClassificationQualityAudit().run(records)
        anomalies = {a.name: a for a in report.anomalies}
        self.assertIn("review_above_accept_threshold", anomalies)

    def test_publish_below_ambiguity_threshold(self):
        records = [_record(decision="publish", quality_score=0.30)]
        report = ClassificationQualityAudit().run(records)
        anomalies = {a.name: a for a in report.anomalies}
        self.assertIn("publish_below_ambiguity_threshold", anomalies)

    def test_reject_with_high_classification_score(self):
        records = [_record(decision="reject", classification_score=0.95)]
        report = ClassificationQualityAudit().run(records)
        anomalies = {a.name: a for a in report.anomalies}
        self.assertIn("reject_with_high_classification_score", anomalies)

    def test_needs_review_without_reason(self):
        records = [_record(decision="publish", needs_review=True,
                           primary_reason_code=None, reason_codes=[])]
        report = ClassificationQualityAudit().run(records)
        anomalies = {a.name: a for a in report.anomalies}
        self.assertIn("needs_review_without_reason", anomalies)

    def test_llm_fallback_rejected(self):
        records = [_record(decision="reject", used_llm=True)]
        report = ClassificationQualityAudit().run(records)
        anomalies = {a.name: a for a in report.anomalies}
        self.assertIn("llm_fallback_rejected", anomalies)


class SuggestionTests(unittest.TestCase):
    def test_suggests_negative_url_part_with_enough_support(self):
        # 4 ofertas rechazadas con segmento '/asistencia-social' que NO está
        # en NEGATIVE_URL_PARTS — debería sugerirse.
        records = [
            _record(decision="reject", url_oferta=f"https://muni.cl/asistencia-social/post-{i}")
            for i in range(4)
        ]
        report = ClassificationQualityAudit(suggestion_min_support=3).run(records)
        targets = {(s.target, s.candidate) for s in report.suggestions}
        self.assertIn(("NEGATIVE_URL_PARTS", "/asistencia-social"), targets)

    def test_does_not_suggest_segments_already_covered(self):
        # /noticias ya está en NEGATIVE_URL_PARTS, no debería sugerirse.
        records = [
            _record(decision="reject", url_oferta=f"https://muni.cl/noticias/{i}")
            for i in range(5)
        ]
        report = ClassificationQualityAudit(suggestion_min_support=3).run(records)
        targets = {(s.target, s.candidate) for s in report.suggestions}
        self.assertNotIn(("NEGATIVE_URL_PARTS", "/noticias"), targets)

    def test_does_not_suggest_token_present_in_publish(self):
        # token aparece tanto en publish como reject → no es discriminante.
        records = []
        for i in range(5):
            records.append(_record(decision="reject", cargo=f"Concurso anual {i} mantenedor"))
        for i in range(5):
            records.append(_record(decision="publish", cargo=f"Mantenedor industrial {i}"))
        report = ClassificationQualityAudit(suggestion_min_support=3).run(records)
        targets = {s.candidate for s in report.suggestions if s.target == "NEGATIVE_PATTERNS"}
        # 'mantenedor' aparece igual en publish y reject, no debe sugerirse.
        self.assertNotIn(r"\bmantenedor\b", targets)

    def test_suggests_token_only_in_rejects(self):
        records = []
        # 5 ofertas rechazadas con un token particular y único en rejects.
        for i in range(5):
            records.append(_record(
                decision="reject",
                cargo=f"Limpieza barrido calles {i}",
            ))
        # 5 ofertas publish con cargos genuinos.
        for i in range(5):
            records.append(_record(
                decision="publish",
                cargo=f"Analista de Datos {i}",
            ))
        report = ClassificationQualityAudit(suggestion_min_support=3).run(records)
        candidates = {s.candidate for s in report.suggestions if s.target == "NEGATIVE_PATTERNS"}
        # 'barrido' aparece sólo en rejects.
        self.assertIn(r"\bbarrido\b", candidates)


class LoadingTests(unittest.TestCase):
    def test_load_jsonl(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            with path.open("w", encoding="utf-8") as fh:
                fh.write(json.dumps(_record(decision="publish")) + "\n")
                fh.write(json.dumps(_record(decision="reject")) + "\n")
            rows = list(iter_records_from_jsonl(path))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]["decision"], "reject")

    def test_load_json_with_records_key(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.json"
            path.write_text(
                json.dumps({"records": [_record(), _record(decision="reject")]}),
                encoding="utf-8",
            )
            rows = list(iter_records_from_json(path))
            self.assertEqual(len(rows), 2)

    def test_iter_records_auto_picks_jsonl_by_extension(self):
        with TemporaryDirectory() as tmp:
            jsonl = Path(tmp) / "x.jsonl"
            jsonl.write_text(json.dumps(_record()) + "\n", encoding="utf-8")
            rows = list(iter_records_auto(jsonl))
            self.assertEqual(len(rows), 1)


class CoverageAndDeterminismTests(unittest.TestCase):
    def test_coverage_counts_numeric_fields(self):
        records = [
            _record(quality_score=0.7, policy_score=0.5, classification_score=0.6),
            _record(quality_score=None, policy_score=0.5, classification_score=None),
        ]
        report = ClassificationQualityAudit().run(records)
        self.assertEqual(report.coverage["with_quality_score"], 1)
        self.assertEqual(report.coverage["with_policy_score"], 2)
        self.assertEqual(report.coverage["with_classification_score"], 1)

    def test_run_is_deterministic(self):
        records = [
            _record(decision="publish", url_oferta="https://x.cl/a"),
            _record(decision="reject", url_oferta="https://x.cl/noticias/b"),
            _record(decision="review", quality_score=0.91),
        ]
        a = ClassificationQualityAudit().run(records).to_json()
        b = ClassificationQualityAudit().run(records).to_json()
        # Excluir generated_at_utc que se mueve por segundo.
        a.pop("generated_at_utc")
        b.pop("generated_at_utc")
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
