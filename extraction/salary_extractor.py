from __future__ import annotations

import re
from dataclasses import dataclass


_POSITIVE_CONTEXT = (
    "renta bruta mensual",
    "renta bruta",
    "renta líquida",
    "renta liquida",
    "remuneración mensual",
    "remuneracion mensual",
    "sueldo base",
    "honorarios mensuales",
    "monto mensual",
    "total haberes",
    "renta ofrecida",
    "renta",
    "sueldo",
    "remuneración",
    "honorarios",
)
_NEGATIVE_CONTEXT = (
    "presupuesto",
    "monto total",
    "convenio",
    "programa",
    "financiamiento",
    "total proyecto",
    "resolución",
    "resolucion",
    "decreto",
    "licitación",
    "licitacion",
    "inversión",
    "inversion",
    "recursos",
    "anual",
    "semestral",
    "global",
)


@dataclass(frozen=True)
class SalaryExtraction:
    amount: float | None = None
    currency: str | None = None
    raw: str | None = None
    confidence: str = "none"
    validation_status: str | None = None
    trace: str | None = None

    def as_tuple(self) -> tuple[float | None, str | None, str | None]:
        """Compatibilidad retroactiva con el contrato histórico (amount, currency, raw)."""
        return (self.amount, self.currency, self.raw)

    def __iter__(self):
        """Permite unpacking como: amount, currency, raw = extract_salary(...)."""
        return iter(self.as_tuple())

    def __eq__(self, other):
        """Permite comparar contra tuplas históricas sin romper el nuevo formato enriquecido."""
        if isinstance(other, tuple) and len(other) == 3:
            return self.as_tuple() == other
        return super().__eq__(other)


def _window(text: str, start: int, end: int, radius: int = 80) -> str:
    lo = max(0, start - radius)
    hi = min(len(text), end + radius)
    return text[lo:hi].lower()


def _score_context(fragment: str) -> tuple[int, int]:
    pos = sum(1 for token in _POSITIVE_CONTEXT if token in fragment)
    neg = sum(1 for token in _NEGATIVE_CONTEXT if token in fragment)
    return pos, neg


def _status_for_amount(amount: int, pos_hits: int, neg_hits: int) -> tuple[str | None, str]:
    if amount >= 15_000_000:
        return "remuneracion_descartada", "monto >= 15MM detectado"
    if amount >= 10_000_000:
        if pos_hits == 0 or neg_hits > pos_hits:
            return "remuneracion_no_confiable", "monto >= 10MM sin contexto fuerte"
        return "remuneracion_sospechosa", "monto >= 10MM con contexto parcial"
    if neg_hits > pos_hits:
        return "remuneracion_no_confiable", "contexto financiero no laboral"
    return None, "monto validado"


def extract_salary(text: str) -> SalaryExtraction:
    source = text or ""
    pattern = re.compile(r"((?:\$|clp|pesos?)\s*[\d\.\,]{4,})", flags=re.IGNORECASE)
    matches = list(pattern.finditer(source))
    if not matches:
        return SalaryExtraction()

    candidates: list[tuple[int, str, int, int, int, str | None]] = []
    for m in matches:
        raw = m.group(1)
        number = re.sub(r"[^\d]", "", raw)
        if not number:
            continue
        amount = int(number)
        context = _window(source, m.start(1), m.end(1))
        pos_hits, neg_hits = _score_context(context)
        status, _ = _status_for_amount(amount, pos_hits, neg_hits)
        score = (pos_hits * 3) - (neg_hits * 2)
        if amount >= 250_000:
            candidates.append((amount, raw, score, pos_hits, neg_hits, status))

    if not candidates:
        return SalaryExtraction()

    candidates.sort(key=lambda item: (item[2], item[3] - item[4], -item[0]), reverse=True)
    amount, raw, _, pos_hits, neg_hits, status = candidates[0]
    derived_status, reason = _status_for_amount(amount, pos_hits, neg_hits)
    final_status = status or derived_status
    if final_status in {"remuneracion_descartada", "remuneracion_no_confiable"}:
        return SalaryExtraction(
            amount=None,
            currency=None,
            raw=raw,
            confidence="low",
            validation_status=final_status,
            trace=reason,
        )

    confidence = "high" if final_status is None else "medium"
    return SalaryExtraction(
        amount=float(amount),
        currency="CLP",
        raw=raw,
        confidence=confidence,
        validation_status=final_status,
        trace=reason,
    )
