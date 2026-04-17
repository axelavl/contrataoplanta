from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ClassifiedEmail:
    email: str
    kinds: tuple[str, ...]
    context: str


@dataclass(frozen=True)
class EmailExtraction:
    classified: list[ClassifiedEmail]
    postulacion_channel: str
    has_consultation_channel: bool


_EMAIL_RE = re.compile(r"\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b", re.IGNORECASE)
_POSTULACION_HINTS = (
    "enviar antecedentes",
    "remitir postulación",
    "remitir postulacion",
    "recepción de antecedentes",
    "recepcion de antecedentes",
    "postular a",
    "postulaciones se recibirán en",
    "postulaciones se recibiran en",
    "hacer llegar cv",
    "enviar documentos a",
    "postular al correo",
)
_CONSULTAS_HINTS = (
    "consultas a",
    "consultas del proceso a",
    "para consultas",
    "dudas al correo",
    "informaciones en",
    "consultas sobre el proceso a",
)


def _context(text: str, start: int, end: int, radius: int = 100) -> str:
    lo = max(0, start - radius)
    hi = min(len(text), end + radius)
    return text[lo:hi]


def _classify_kind(context: str) -> tuple[str, ...]:
    c = context.lower()
    kinds: list[str] = []
    if any(h in c for h in _POSTULACION_HINTS):
        kinds.append("email_postulacion")
    if any(h in c for h in _CONSULTAS_HINTS):
        kinds.append("email_consultas")
    if not kinds:
        if "postul" in c:
            kinds.append("email_postulacion")
        elif "consulta" in c or "informaci" in c:
            kinds.append("email_consultas")
        else:
            kinds.append("email_indeterminado")
    return tuple(kinds)


def extract_and_classify_emails(text: str) -> EmailExtraction:
    source = text or ""
    seen: dict[str, ClassifiedEmail] = {}
    for match in _EMAIL_RE.finditer(source):
        email = match.group(0).lower()
        ctx = _context(source, match.start(), match.end())
        kinds = _classify_kind(ctx)
        if email in seen:
            merged = tuple(sorted(set(seen[email].kinds + kinds)))
            seen[email] = ClassifiedEmail(email=email, kinds=merged, context=seen[email].context)
            continue
        seen[email] = ClassifiedEmail(email=email, kinds=kinds, context=ctx.strip())

    classified = list(seen.values())
    has_postulacion = any("email_postulacion" in item.kinds for item in classified)
    has_consultas = any("email_consultas" in item.kinds for item in classified)

    if has_postulacion and has_consultas:
        channel = "postulacion_mixta"
    elif has_postulacion:
        channel = "postulacion_por_email"
    elif re.search(r"postular|formulario|portal de empleo|trabajando\.cl|bne\.cl", source.lower()):
        channel = "postulacion_por_portal_externo"
    elif re.search(r"presencial|oficina de partes|entrega presencial", source.lower()):
        channel = "postulacion_presencial"
    else:
        channel = "canal_no_determinado"

    return EmailExtraction(
        classified=classified,
        postulacion_channel=channel,
        has_consultation_channel=has_consultas,
    )
