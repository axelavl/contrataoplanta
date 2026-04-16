from __future__ import annotations

import re
from dataclasses import dataclass

from models.classification import ClassificationResult, RuleTrace
from models.raw_page import RawPage


@dataclass(frozen=True)
class Rule:
    rule_id: str
    pattern: str
    weight: float
    reason: str
    target: str = "content"


class RuleEngine:
    def __init__(self, min_required_signals: int = 2) -> None:
        self.min_required_signals = min_required_signals
        self.positive_rules = self._build_positive_rules()
        self.negative_rules = self._build_negative_rules()

    def classify_with_rules(
        self,
        raw_page: RawPage,
        accept_threshold: float = 0.80,
        ambiguity_threshold: float = 0.55,
    ) -> ClassificationResult:
        text = self._make_text_blob(raw_page)
        score = 0.0
        positives: list[str] = []
        negatives: list[str] = []
        rule_trace: list[RuleTrace] = []
        essential_hits = {
            "title": False,
            "deadline": False,
            "functions_or_requirements": False,
            "attachments": False,
            "contract_or_salary": False,
        }

        for rule in self.positive_rules:
            if self._matches(rule, text, raw_page.url, raw_page.attachment_urls, raw_page.tables_text):
                score += rule.weight
                positives.append(rule.reason)
                rule_trace.append(RuleTrace(rule_id=rule.rule_id, weight=rule.weight, reason=rule.reason))
                self._mark_essential(essential_hits, rule.rule_id)

        for rule in self.negative_rules:
            if self._matches(rule, text, raw_page.url, raw_page.attachment_urls, raw_page.tables_text):
                score += rule.weight
                negatives.append(rule.reason)
                rule_trace.append(RuleTrace(rule_id=rule.rule_id, weight=rule.weight, reason=rule.reason))

        essentials = sum(1 for hit in essential_hits.values() if hit)
        if essentials == 0:
            score -= 0.25
            negatives.append("ausencia total de cargo/requisitos/fechas")
            rule_trace.append(RuleTrace(rule_id="missing_core_signals", weight=-0.25, reason="sin señales núcleo"))
        score = max(0.0, min(1.0, 0.5 + score))

        if raw_page.http_status and raw_page.http_status >= 400:
            return ClassificationResult(
                is_job_posting=False,
                content_type="broken_page",
                confidence=1.0,
                positive_signals=positives,
                negative_signals=negatives,
                rejection_reasons=[f"http_status={raw_page.http_status}"],
                score=0.0,
                rule_trace=rule_trace,
            )

        if self._is_historical(text):
            return ClassificationResult(
                is_job_posting=False,
                content_type="historical_archive",
                confidence=0.95,
                positive_signals=positives,
                negative_signals=negatives + ["aviso antiguo detectado"],
                rejection_reasons=["publicación histórica sin vigencia actual"],
                score=score,
                rule_trace=rule_trace,
            )

        rejection_reasons: list[str] = []
        is_job_posting = False
        content_type = "unknown"

        if score >= accept_threshold:
            if essentials >= self.min_required_signals:
                is_job_posting = True
                content_type = "public_competition" if "concurso" in text else "job_posting"
            else:
                rejection_reasons.append("faltan señales esenciales mínimas")
                content_type = "informational_page"
        elif ambiguity_threshold <= score < accept_threshold:
            content_type = "unknown"
            rejection_reasons.append("zona ambigua")
        else:
            content_type = self._infer_negative_type(text, raw_page.url)
            rejection_reasons.append("score bajo por reglas")

        return ClassificationResult(
            is_job_posting=is_job_posting,
            content_type=content_type,
            confidence=score,
            positive_signals=positives,
            negative_signals=negatives,
            rejection_reasons=rejection_reasons,
            score=score,
            rule_trace=rule_trace,
        )

    @staticmethod
    def _make_text_blob(raw_page: RawPage) -> str:
        chunks = [
            raw_page.title or "",
            raw_page.meta_description or "",
            " ".join(raw_page.breadcrumbs),
            raw_page.section_hint or "",
            raw_page.html_text or "",
            " ".join(raw_page.tables_text),
            " ".join(raw_page.attachment_texts),
            " ".join(raw_page.found_dates),
        ]
        return "\n".join(chunks).lower()

    @staticmethod
    def _matches(rule: Rule, text: str, url: str, attachments: list[str], tables: list[str]) -> bool:
        target_value = text
        if rule.target == "url":
            target_value = (url or "").lower()
        elif rule.target == "attachments":
            target_value = " ".join(attachments).lower()
        elif rule.target == "tables":
            target_value = " ".join(tables).lower()
        return bool(re.search(rule.pattern, target_value))

    @staticmethod
    def _mark_essential(essentials: dict[str, bool], rule_id: str) -> None:
        if "title" in rule_id or "cargo" in rule_id:
            essentials["title"] = True
        if "fecha" in rule_id or "cierre" in rule_id:
            essentials["deadline"] = True
        if "funciones" in rule_id or "requisitos" in rule_id:
            essentials["functions_or_requirements"] = True
        if "adjunto" in rule_id or "bases" in rule_id:
            essentials["attachments"] = True
        if "renta" in rule_id or "contrat" in rule_id or "jornada" in rule_id:
            essentials["contract_or_salary"] = True

    @staticmethod
    def _is_historical(text: str) -> bool:
        old_year = re.search(r"\b(2020|2021|2022|2023|2024)\b", text)
        current_hint = re.search(r"\b(2025|2026|vigente|abierto|postulaciones hasta)\b", text)
        return bool(old_year and not current_hint)

    @staticmethod
    def _infer_negative_type(text: str, url: str) -> str:
        candidate = f"{url} {text}"
        if re.search(r"resultados?|n[oó]mina de seleccionados|adjudicaci[oó]n", candidate):
            return "results_page"
        if re.search(r"noticias?|prensa|comunicado|bolet[ií]n|blog", candidate):
            return "news_article"
        if re.search(r"evento|seminario|charla|taller|agenda", candidate):
            return "event"
        if re.search(r"archivo|hist[oó]rico|memoria anual", candidate):
            return "historical_archive"
        return "informational_page"

    @staticmethod
    def _build_positive_rules() -> list[Rule]:
        return [
            Rule("cargo_keyword", r"\bcargo\b|nombre del cargo", 0.18, "menciona cargo"),
            Rule("postulacion_keyword", r"postulaci[oó]n|postular", 0.16, "menciona postulación"),
            Rule("concurso_keyword", r"concurso p[úu]blico|concurso", 0.14, "menciona concurso"),
            Rule("funciones_keyword", r"funciones del cargo|principales funciones|objetivo del cargo", 0.14, "incluye funciones"),
            Rule("requisitos_keyword", r"requisitos del cargo|requisitos exigibles|requisitos deseables|requisitos", 0.14, "incluye requisitos"),
            Rule("salary_keyword", r"renta bruta|remuneraci[oó]n|honorarios", 0.12, "incluye renta/remuneración"),
            Rule("contract_keyword", r"contrata|planta|c[oó]digo del trabajo|calidad jur[ií]dica", 0.10, "incluye calidad contractual"),
            Rule("deadline_keyword", r"fecha de cierre|cierre de postulaci[oó]n|postulaciones hasta", 0.18, "incluye fecha de cierre"),
            Rule("url_recruitment", r"/trabaja-con-nosotros|/concursos|/postulacion|/ofertas-laborales", 0.14, "URL de reclutamiento", target="url"),
            Rule("table_job_columns", r"cargo\s+.*renta.*cierre|renta.*cargo", 0.14, "tabla con estructura laboral", target="tables"),
            Rule("adjunto_bases", r"bases|perfil|concurso|tdr|convocatoria", 0.12, "adjunto de bases/perfil", target="attachments"),
        ]

    @staticmethod
    def _build_negative_rules() -> list[Rule]:
        return [
            Rule("news_keyword", r"noticias?|prensa|comunicado|bolet[ií]n|actualidad|novedades?", -0.24, "contenido de noticias/prensa"),
            Rule("events_keyword", r"agenda|evento|seminario|charla|taller|ceremonia|aniversario", -0.22, "contenido de evento/agenda"),
            Rule("institutional_keyword", r"publicaci[oó]n institucional|art[ií]culo|blog|memoria anual", -0.18, "contenido institucional genérico"),
            Rule("results_keyword", r"resultados del concurso|n[oó]mina de seleccionados|adjudicaci[oó]n|proceso finalizado|concurso cerrado", -0.35, "página de resultados/cierre"),
            Rule("historical_keyword", r"archivo|hist[oó]rico", -0.2, "contenido histórico"),
            Rule("url_news", r"/noticias/|/prensa/|/blog/|/novedades/", -0.25, "URL de noticias", target="url"),
        ]
