from __future__ import annotations

from datetime import datetime, timezone
import unittest

from models.classification import ClassificationResult
from models.date_models import DateResolution
from models.extraction import ExtractionBundle
from validation.expiry_validator import validate_expiry
from validation.job_validator import validate_minimum_completeness
from validation.quality_scoring import score_quality


def make_extraction(**kwargs) -> ExtractionBundle:
    return ExtractionBundle(**kwargs)


def make_classification(**kwargs) -> ClassificationResult:
    base = dict(
        is_job_posting=True,
        content_type="job_posting",
        confidence=0.8,
    )
    base.update(kwargs)
    return ClassificationResult(**base)


def make_dates(**kwargs) -> DateResolution:
    return DateResolution(**kwargs)


class MinimumCompletenessTests(unittest.TestCase):
    def test_empty_extraction_fails(self):
        ok, missing = validate_minimum_completeness(make_extraction())
        self.assertFalse(ok)
        self.assertEqual(
            set(missing),
            {"title", "functions_or_requirements", "salary_or_contract", "documents_or_attachments"},
        )

    def test_two_signals_pass_default_threshold(self):
        extraction = make_extraction(
            job_title="Analista",
            requirements=["Título profesional"],
        )
        ok, missing = validate_minimum_completeness(extraction)
        self.assertTrue(ok)
        self.assertEqual(set(missing), {"salary_or_contract", "documents_or_attachments"})

    def test_one_signal_fails_default_threshold(self):
        extraction = make_extraction(job_title="Analista")
        ok, _ = validate_minimum_completeness(extraction)
        self.assertFalse(ok)

    def test_functions_or_requirements_disjunction(self):
        only_functions = make_extraction(job_title="X", functions=["Liderar"])
        only_reqs = make_extraction(job_title="X", requirements=["Experiencia"])
        self.assertTrue(validate_minimum_completeness(only_functions)[0])
        self.assertTrue(validate_minimum_completeness(only_reqs)[0])

    def test_salary_or_contract_disjunction(self):
        only_salary = make_extraction(job_title="X", salary_amount=1_500_000.0)
        only_contract = make_extraction(job_title="X", contract_type="contrata")
        self.assertTrue(validate_minimum_completeness(only_salary)[0])
        self.assertTrue(validate_minimum_completeness(only_contract)[0])

    def test_documents_or_attachments_disjunction(self):
        only_docs = make_extraction(job_title="X", documents_required=["CV"])
        only_att = make_extraction(job_title="X", attachments_used=["bases.pdf"])
        self.assertTrue(validate_minimum_completeness(only_docs)[0])
        self.assertTrue(validate_minimum_completeness(only_att)[0])

    def test_custom_min_signals_threshold(self):
        extraction = make_extraction(job_title="X", requirements=["Req"])
        self.assertTrue(validate_minimum_completeness(extraction, min_signals=2)[0])
        self.assertFalse(validate_minimum_completeness(extraction, min_signals=3)[0])

    def test_all_four_signals_pass(self):
        extraction = make_extraction(
            job_title="Analista",
            functions=["Liderar"],
            salary_amount=1_500_000.0,
            documents_required=["CV"],
        )
        ok, missing = validate_minimum_completeness(extraction, min_signals=4)
        self.assertTrue(ok)
        self.assertEqual(missing, [])


class ExpiryValidatorTests(unittest.TestCase):
    def test_results_page_is_expired(self):
        expired, reason = validate_expiry(
            make_classification(content_type="results_page", is_job_posting=False),
            make_dates(),
        )
        self.assertTrue(expired)
        self.assertIn("results_page", reason)

    def test_historical_archive_is_expired(self):
        expired, reason = validate_expiry(
            make_classification(content_type="historical_archive", is_job_posting=False),
            make_dates(),
        )
        self.assertTrue(expired)
        self.assertIn("historical_archive", reason)

    def test_news_article_is_expired(self):
        expired, _ = validate_expiry(
            make_classification(content_type="news_article", is_job_posting=False),
            make_dates(),
        )
        self.assertTrue(expired)

    def test_event_is_expired(self):
        expired, _ = validate_expiry(
            make_classification(content_type="event", is_job_posting=False),
            make_dates(),
        )
        self.assertTrue(expired)

    def test_expired_dates_override(self):
        expired, reason = validate_expiry(
            make_classification(),
            make_dates(is_expired=True, expiration_reason="past_deadline"),
        )
        self.assertTrue(expired)
        self.assertEqual(reason, "past_deadline")

    def test_expired_without_reason_defaults_to_expired(self):
        expired, reason = validate_expiry(
            make_classification(),
            make_dates(is_expired=True),
        )
        self.assertTrue(expired)
        self.assertEqual(reason, "expired")

    def test_informational_non_posting_is_expired(self):
        expired, reason = validate_expiry(
            make_classification(
                content_type="informational_page", is_job_posting=False
            ),
            make_dates(),
        )
        self.assertTrue(expired)
        self.assertEqual(reason, "informational_non_posting")

    def test_informational_page_that_is_a_posting_not_expired(self):
        expired, reason = validate_expiry(
            make_classification(
                content_type="informational_page", is_job_posting=True
            ),
            make_dates(),
        )
        self.assertFalse(expired)
        self.assertIsNone(reason)

    def test_active_job_posting_not_expired(self):
        expired, reason = validate_expiry(
            make_classification(),
            make_dates(is_expired=False),
        )
        self.assertFalse(expired)
        self.assertIsNone(reason)


class QualityScoringTests(unittest.TestCase):
    def test_score_shape_and_rounding(self):
        result = score_quality(
            make_classification(confidence=0.8),
            make_extraction(job_title="X"),
            make_dates(date_confidence="medium"),
        )
        expected_keys = {
            "classification_confidence",
            "field_completeness_score",
            "date_confidence_score",
            "salary_confidence_score",
            "requirements_confidence_score",
            "overall_quality_score",
            "needs_review",
        }
        self.assertEqual(set(result.keys()), expected_keys)
        # All numeric scores are rounded to <=4 decimals
        for key in expected_keys - {"needs_review"}:
            value = result[key]
            self.assertIsInstance(value, float)
            self.assertAlmostEqual(value, round(value, 4))

    def test_field_completeness_all_present(self):
        now = datetime(2026, 5, 1, tzinfo=timezone.utc)
        result = score_quality(
            make_classification(confidence=1.0),
            make_extraction(
                job_title="X",
                functions=["a"],
                requirements=["b"],
                salary_amount=1_000_000.0,
                contract_type="contrata",
            ),
            make_dates(application_end_at=now, date_confidence="high"),
        )
        self.assertEqual(result["field_completeness_score"], 1.0)

    def test_field_completeness_empty(self):
        result = score_quality(
            make_classification(confidence=0.0),
            make_extraction(),
            make_dates(),
        )
        self.assertEqual(result["field_completeness_score"], 0.0)

    def test_date_confidence_mapping(self):
        for label, expected in [("low", 0.35), ("medium", 0.65), ("high", 0.95)]:
            result = score_quality(
                make_classification(),
                make_extraction(),
                make_dates(date_confidence=label),
            )
            self.assertEqual(result["date_confidence_score"], expected)

    def test_unknown_date_confidence_defaults_to_low(self):
        result = score_quality(
            make_classification(),
            make_extraction(),
            make_dates(date_confidence="garbage"),
        )
        self.assertEqual(result["date_confidence_score"], 0.35)

    def test_salary_confidence_switches(self):
        with_salary = score_quality(
            make_classification(),
            make_extraction(salary_amount=1_000_000.0),
            make_dates(),
        )
        without = score_quality(
            make_classification(),
            make_extraction(),
            make_dates(),
        )
        self.assertEqual(with_salary["salary_confidence_score"], 0.85)
        self.assertEqual(without["salary_confidence_score"], 0.25)

    def test_news_penalty_reduces_overall(self):
        posting = score_quality(
            make_classification(content_type="job_posting"),
            make_extraction(job_title="X"),
            make_dates(),
        )
        news = score_quality(
            make_classification(content_type="news_article"),
            make_extraction(job_title="X"),
            make_dates(),
        )
        self.assertAlmostEqual(
            posting["overall_quality_score"] - news["overall_quality_score"], 0.2, places=4
        )

    def test_results_page_penalty(self):
        posting = score_quality(
            make_classification(content_type="job_posting"),
            make_extraction(job_title="X"),
            make_dates(),
        )
        results = score_quality(
            make_classification(content_type="results_page"),
            make_extraction(job_title="X"),
            make_dates(),
        )
        self.assertGreater(posting["overall_quality_score"], results["overall_quality_score"])

    def test_overall_clamped_to_0_1(self):
        # Very low inputs + news penalty could go negative without clamping.
        low = score_quality(
            make_classification(confidence=0.0, content_type="news_article"),
            make_extraction(),
            make_dates(date_confidence="low"),
        )
        self.assertGreaterEqual(low["overall_quality_score"], 0.0)

        # Maximum inputs shouldn't exceed 1.
        now = datetime(2026, 5, 1, tzinfo=timezone.utc)
        high = score_quality(
            make_classification(confidence=1.0),
            make_extraction(
                job_title="X",
                functions=["a"],
                requirements=["b"],
                salary_amount=1.0,
                contract_type="contrata",
            ),
            make_dates(application_end_at=now, date_confidence="high"),
        )
        self.assertLessEqual(high["overall_quality_score"], 1.0)

    def test_needs_review_when_low_score(self):
        result = score_quality(
            make_classification(confidence=0.1),
            make_extraction(),
            make_dates(date_confidence="low"),
        )
        self.assertTrue(result["needs_review"])

    def test_needs_review_when_classification_flags_it(self):
        result = score_quality(
            make_classification(confidence=1.0, needs_review=True),
            make_extraction(
                job_title="X",
                functions=["a"],
                requirements=["b"],
                salary_amount=1.0,
                contract_type="contrata",
            ),
            make_dates(date_confidence="high"),
        )
        self.assertTrue(result["needs_review"])

    def test_needs_review_threshold_boundary(self):
        # Overall >= 0.60 and classification.needs_review=False ⇒ no review.
        result = score_quality(
            make_classification(confidence=1.0),
            make_extraction(
                job_title="X",
                requirements=["a"],
                salary_amount=1.0,
            ),
            make_dates(date_confidence="high"),
        )
        self.assertFalse(result["needs_review"])
        self.assertGreaterEqual(result["overall_quality_score"], 0.60)


if __name__ == "__main__":
    unittest.main()
