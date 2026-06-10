from __future__ import annotations

import re
from dataclasses import dataclass


RELEVANT_ATTACHMENT_PATTERNS = [
    r"bases",
    r"perfil",
    r"tdr",
    r"t[eé]rminos? de referencia",
    r"anexo",
    r"concurso",
    r"convocatoria",
]


@dataclass
class ParsedAttachment:
    url: str
    relevant: bool
    used_ocr: bool
    extracted_text: str


def is_relevant_attachment(url: str) -> bool:
    filename = url.rsplit("/", 1)[-1].lower()
    return filename.endswith(".pdf") and any(
        re.search(pattern, filename, re.IGNORECASE) for pattern in RELEVANT_ATTACHMENT_PATTERNS
    )


def parse_attachments(
    attachment_urls: list[str],
    attachment_texts: list[str] | None = None,
    allow_ocr: bool = False,
) -> list[ParsedAttachment]:
    texts = attachment_texts or []
    parsed: list[ParsedAttachment] = []
    for idx, url in enumerate(attachment_urls):
        relevant = is_relevant_attachment(url)
        extracted_text = texts[idx] if idx < len(texts) else ""
        used_ocr = False
        if relevant and not extracted_text.strip() and allow_ocr:
            used_ocr = True
            extracted_text = "[ocr_not_executed_in_unit_test_environment]"
        parsed.append(
            ParsedAttachment(
                url=url,
                relevant=relevant,
                used_ocr=used_ocr,
                extracted_text=extracted_text,
            )
        )
    return parsed
