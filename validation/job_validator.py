from __future__ import annotations

from models.extraction import ExtractionBundle


def validate_minimum_completeness(extraction: ExtractionBundle, min_signals: int = 2) -> tuple[bool, list[str]]:
    checks = {
        "title": bool(extraction.job_title),
        "functions_or_requirements": bool(extraction.functions or extraction.requirements),
        "salary_or_contract": bool(extraction.salary_amount or extraction.contract_type),
        "documents_or_attachments": bool(extraction.documents_required or extraction.attachments_used),
    }
    passed = sum(1 for value in checks.values() if value)
    missing = [k for k, value in checks.items() if not value]
    return passed >= min_signals, missing
