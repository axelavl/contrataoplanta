from __future__ import annotations

from classification.llm_fallback_classifier import classify_with_llm_fallback
from classification.rule_engine import RuleEngine
from models.classification import ClassificationResult
from models.raw_page import RawPage


class ContentClassifier:
    def __init__(
        self,
        accept_threshold: float = 0.80,
        ambiguity_threshold: float = 0.55,
        min_required_signals: int = 2,
    ) -> None:
        self.accept_threshold = accept_threshold
        self.ambiguity_threshold = ambiguity_threshold
        self.rule_engine = RuleEngine(min_required_signals=min_required_signals)

    def classify(self, raw_page: RawPage) -> ClassificationResult:
        result = self.rule_engine.classify_with_rules(
            raw_page,
            accept_threshold=self.accept_threshold,
            ambiguity_threshold=self.ambiguity_threshold,
        )

        should_use_llm = (
            self.ambiguity_threshold <= result.score < self.accept_threshold
            or (
                result.positive_signals
                and result.negative_signals
                and result.score >= 0.40
                and result.content_type in {"unknown", "news_article", "informational_page"}
            )
        )

        if should_use_llm:
            result = classify_with_llm_fallback(raw_page, result)

        return result
