"""Tests for normalization/job_normalizer.py.

`normalize_job_posting` wires extraction, classification, dates and
quality scores into a final JobPosting. The function is pure glue code;
the tests ensure each output field is sourced from the correct input.
"""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from models.classification import ClassificationResult
from models.date_models import DateResolution
from models.extraction import ExtractionBundle
from models.raw_page import RawPage
from normalization.job_normalizer import normalize_job_posting


def _raw(**overrides) -> RawPage:
    base = dict(
        source_id="src-123",
        source_name="Municipalidad Demo",
        platform="wordpress",
        url="https://demo.cl/oferta/1",
        final_url=None,
        title="Analista Profesional",
        meta_description=None,
        html_text="contenido",
        tables_text=[],
        attachment_urls=[],
        attachment_texts=[],
        found_dates=[],
        discovered_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        headings=[],
    )
    base.update(overrides)
    return RawPage(**base)


def _classification(**overrides) -> ClassificationResult:
    base = dict(
        is_job_posting=True,
        content_type="job_posting",
        confidence=0.8,
    )
    base.update(overrides)
    return ClassificationResult(**base)


def _extraction(**overrides) -> ExtractionBundle:
    return ExtractionBundle(**overrides)


def _dates(**overrides) -> DateResolution:
    return DateResolution(**overrides)


def _scores(**overrides) -> dict:
    base = {
        "classification_confidence": 0.75,
        "field_completeness_score": 0.60,
        "date_confidence_score": 0.65,
        "salary_confidence_score": 0.25,
        "requirements_confidence_score": 0.85,
        "overall_quality_score": 0.70,
        "needs_review": False,
    }
    base.update(overrides)
    return base


class JobNormalizerTests(unittest.TestCase):
    def test_source_fields_come_from_raw_page(self):
        raw = _raw(source_id="SRC-9", source_name="Fuente X", platform="generic")
        result = normalize_job_posting(
            raw, _classification(), _extraction(), _dates(), _scores()
        )
        self.assertEqual(result.source_id, "SRC-9")
        self.assertEqual(result.source_name, "Fuente X")
        self.assertEqual(result.platform, "generic")

    def test_job_url_prefers_final_url(self):
        raw = _raw(
            url="https://demo.cl/original",
            final_url="https://demo.cl/final-canonical",
        )
        result = normalize_job_posting(
            raw, _classification(), _extraction(), _dates(), _scores()
        )
        self.assertEqual(result.job_url, "https://demo.cl/final-canonical")

    def test_job_url_falls_back_to_url_when_final_is_none(self):
        raw = _raw(final_url=None)
        result = normalize_job_posting(
            raw, _classification(), _extraction(), _dates(), _scores()
        )
        self.assertEqual(result.job_url, raw.url)

    def test_job_title_comes_from_extraction(self):
        result = normalize_job_posting(
            _raw(),
            _classification(),
            _extraction(job_title="Ingeniero Civil"),
            _dates(),
            _scores(),
        )
        self.assertEqual(result.job_title, "Ingeniero Civil")

    def test_job_type_is_classification_content_type(self):
        result = normalize_job_posting(
            _raw(),
            _classification(content_type="public_competition"),
            _extraction(),
            _dates(),
            _scores(),
        )
        self.assertEqual(result.job_type, "public_competition")

    def test_salary_and_contract_copied_from_extraction(self):
        extraction = _extraction(
            salary_amount=1_500_000.0,
            salary_currency="CLP",
            salary_raw="$1.500.000",
            contract_type="contrata",
            workday="completa",
            modality="presencial",
        )
        result = normalize_job_posting(
            _raw(), _classification(), extraction, _dates(), _scores()
        )
        self.assertEqual(result.salary_amount, 1_500_000.0)
        self.assertEqual(result.salary_currency, "CLP")
        self.assertEqual(result.salary_raw, "$1.500.000")
        self.assertEqual(result.contract_type, "contrata")
        self.assertEqual(result.workday, "completa")
        self.assertEqual(result.modality, "presencial")

    def test_date_fields_copied_from_resolution(self):
        pub = datetime(2026, 3, 10, tzinfo=timezone.utc)
        start = datetime(2026, 3, 15, tzinfo=timezone.utc)
        end = datetime(2026, 4, 20, tzinfo=timezone.utc)
        dates = _dates(
            published_at=pub,
            application_start_at=start,
            application_end_at=end,
            is_expired=False,
            expiration_reason=None,
            date_confidence="high",
        )
        result = normalize_job_posting(
            _raw(), _classification(), _extraction(), dates, _scores()
        )
        self.assertEqual(result.published_at, pub)
        self.assertEqual(result.application_start_at, start)
        self.assertEqual(result.application_end_at, end)
        self.assertFalse(result.is_expired)
        self.assertEqual(result.date_confidence, "high")

    def test_list_fields_default_to_empty_when_extraction_empty(self):
        result = normalize_job_posting(
            _raw(), _classification(), _extraction(), _dates(), _scores()
        )
        self.assertEqual(result.functions, [])
        self.assertEqual(result.requirements, [])
        self.assertEqual(result.desirable_requirements, [])
        self.assertEqual(result.documents_required, [])
        self.assertEqual(result.benefits, [])  # always empty at this stage
        self.assertEqual(result.attachments, [])

    def test_list_fields_populated_from_extraction(self):
        extraction = _extraction(
            functions=["Coordinar"],
            requirements=["Título profesional"],
            desirable_requirements=["Inglés"],
            documents_required=["CV"],
        )
        result = normalize_job_posting(
            _raw(), _classification(), extraction, _dates(), _scores()
        )
        self.assertEqual(result.functions, ["Coordinar"])
        self.assertEqual(result.requirements, ["Título profesional"])
        self.assertEqual(result.desirable_requirements, ["Inglés"])
        self.assertEqual(result.documents_required, ["CV"])

    def test_attachments_come_from_raw_page_not_extraction(self):
        urls = ["https://x.cl/bases.pdf", "https://x.cl/perfil.pdf"]
        raw = _raw(attachment_urls=urls)
        result = normalize_job_posting(
            raw,
            _classification(),
            _extraction(attachments_used=["https://x.cl/bases.pdf"]),
            _dates(),
            _scores(),
        )
        # `attachments` mirrors raw_page.attachment_urls (the full list),
        # not extraction.attachments_used (the filtered, relevant subset).
        self.assertEqual(result.attachments, urls)

    def test_quality_scores_propagated(self):
        result = normalize_job_posting(
            _raw(),
            _classification(),
            _extraction(),
            _dates(),
            _scores(
                classification_confidence=0.91,
                field_completeness_score=0.80,
                overall_quality_score=0.77,
                needs_review=True,
            ),
        )
        self.assertAlmostEqual(result.classification_confidence, 0.91)
        self.assertAlmostEqual(result.field_completeness_score, 0.80)
        self.assertAlmostEqual(result.overall_quality_score, 0.77)
        self.assertTrue(result.needs_review)

    def test_needs_review_coerced_to_bool(self):
        # The scores dict uses `float | bool`, but JobPosting expects bool.
        result = normalize_job_posting(
            _raw(),
            _classification(),
            _extraction(),
            _dates(),
            _scores(needs_review=1),  # truthy non-bool
        )
        self.assertIs(result.needs_review, True)

    def test_description_copied_from_extraction(self):
        result = normalize_job_posting(
            _raw(),
            _classification(),
            _extraction(description="Resumen de la oferta"),
            _dates(),
            _scores(),
        )
        self.assertEqual(result.description, "Resumen de la oferta")

    def test_scraped_at_is_recent_utc_timestamp(self):
        before = datetime.now(timezone.utc)
        result = normalize_job_posting(
            _raw(), _classification(), _extraction(), _dates(), _scores()
        )
        after = datetime.now(timezone.utc)
        self.assertIsNotNone(result.scraped_at.tzinfo)
        self.assertLessEqual(before, result.scraped_at)
        self.assertLessEqual(result.scraped_at, after)

    def test_location_and_region_default_to_none(self):
        # The normalizer doesn't infer location/region yet; these are None
        # until a downstream step fills them.
        result = normalize_job_posting(
            _raw(), _classification(), _extraction(), _dates(), _scores()
        )
        self.assertIsNone(result.location)
        self.assertIsNone(result.region)
        self.assertIsNone(result.vacancies_count)
        self.assertIsNone(result.external_job_id)

    def test_expiration_reason_flows_through(self):
        dates = _dates(is_expired=True, expiration_reason="past_deadline")
        result = normalize_job_posting(
            _raw(), _classification(), _extraction(), dates, _scores()
        )
        self.assertTrue(result.is_expired)
        self.assertEqual(result.expiration_reason, "past_deadline")


if __name__ == "__main__":
    unittest.main()
