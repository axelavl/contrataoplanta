from __future__ import annotations

import re

from extraction.attachment_parser import parse_attachments
from extraction.contract_extractor import extract_contract_info
from extraction.email_extractor import extract_and_classify_emails
from extraction.functions_extractor import extract_functions
from extraction.requirements_extractor import extract_requirements
from extraction.salary_extractor import extract_salary
from models.extraction import ExtractionBundle
from models.raw_page import RawPage


def extract_job_title(raw_page: RawPage) -> str | None:
    title_sources = [
        raw_page.title or "",
        " ".join(raw_page.headings),
        " ".join(raw_page.tables_text),
        " ".join(raw_page.attachment_texts),
    ]
    joined = "\n".join(title_sources)
    for pattern in [r"nombre del cargo[:\s]+([^\n\.]+)", r"cargo[:\s]+([^\n\.]+)", r"se requiere[:\s]+([^\n\.]+)"]:
        found = re.search(pattern, joined, flags=re.IGNORECASE)
        if found:
            return found.group(1).strip()
    for heading in raw_page.headings:
        if len(heading.split()) >= 2:
            return heading.strip()
    return raw_page.title


def extract_structured_fields(raw_page: RawPage) -> ExtractionBundle:
    parsed_attachments = parse_attachments(raw_page.attachment_urls, raw_page.attachment_texts)
    relevant_attachment_text = "\n".join(a.extracted_text for a in parsed_attachments if a.relevant)
    full_text = "\n".join(
        [
            raw_page.html_text,
            "\n".join(raw_page.tables_text),
            relevant_attachment_text,
        ]
    )

    functions = extract_functions(full_text)
    requirements, desirable, documents = extract_requirements(full_text)
    salary = extract_salary(full_text)
    email_info = extract_and_classify_emails(full_text)
    contract_type, workday, modality = extract_contract_info(full_text)

    emails = [item.email for item in email_info.classified]
    email_postulacion = [item.email for item in email_info.classified if "email_postulacion" in item.kinds]
    email_consultas = [item.email for item in email_info.classified if "email_consultas" in item.kinds]
    email_indeterminado = [item.email for item in email_info.classified if "email_indeterminado" in item.kinds]
    email_contexts = [f"{item.email}::{', '.join(item.kinds)}::{item.context[:180]}" for item in email_info.classified]

    return ExtractionBundle(
        job_title=extract_job_title(raw_page),
        functions=functions,
        requirements=requirements,
        desirable_requirements=desirable,
        salary_amount=salary.amount,
        salary_currency=salary.currency,
        salary_raw=salary.raw,
        contract_type=contract_type,
        workday=workday,
        modality=modality,
        documents_required=documents,
        description=(raw_page.meta_description or raw_page.html_text[:800]).strip() or None,
        attachments_used=[a.url for a in parsed_attachments if a.relevant],
        traces=[f"attachment:{a.url}:relevant={a.relevant}:ocr={a.used_ocr}" for a in parsed_attachments],
        emails_found=emails,
        email_postulacion=email_postulacion,
        email_consultas=email_consultas,
        email_indeterminado=email_indeterminado,
        email_contexts=email_contexts,
        postulacion_channel=email_info.postulacion_channel,
        has_consultation_channel=email_info.has_consultation_channel,
        salary_validation_status=salary.validation_status,
        salary_trace=salary.trace,
    )
