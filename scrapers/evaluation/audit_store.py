from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger(__name__)

from .models import EvaluationResult, QualityValidationResult


class AuditStore:
    """Persistencia defensiva de eventos del gatekeeper y de calidad."""

    def save_source_evaluation(self, conn, *, source_id: int | None, institucion_id: int | None, evaluation: EvaluationResult) -> None:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT sp_source_eval")
            try:
                cur.execute(
                    """
                    INSERT INTO source_evaluations (
                        source_id,
                        institucion_id,
                        source_url,
                        availability,
                        http_status,
                        page_type,
                        job_relevance,
                        open_calls_status,
                        validity_status,
                        recommended_extractor,
                        decision,
                        reason_code,
                        reason_detail,
                        confidence,
                        retry_policy,
                        signals_json,
                        evaluated_at,
                        profile_name
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s
                    )
                    """,
                    (
                        source_id,
                        institucion_id,
                        evaluation.source_url,
                        evaluation.availability.value,
                        evaluation.http_status,
                        evaluation.page_type.value,
                        evaluation.job_relevance.value,
                        evaluation.open_calls_status.value,
                        evaluation.validity_status.value,
                        evaluation.recommended_extractor.value if evaluation.recommended_extractor else None,
                        evaluation.decision.value,
                        evaluation.reason_code.value if evaluation.reason_code else None,
                        evaluation.reason_detail,
                        evaluation.confidence,
                        evaluation.retry_policy.value,
                        json.dumps(evaluation.signals_json, ensure_ascii=False),
                        evaluation.evaluated_at,
                        evaluation.profile_name,
                    ),
                )
                cur.execute("RELEASE SAVEPOINT sp_source_eval")
            except Exception as e:
                log.debug("save_source_evaluation fallo (institucion_id=%s): %s", institucion_id, e)
                cur.execute("ROLLBACK TO SAVEPOINT sp_source_eval")

    def save_quality_event(
        self,
        conn,
        *,
        oferta_id: int | None,
        fuente_id: int | None,
        institucion_id: int | None,
        url_oferta: str | None,
        validation: QualityValidationResult,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT sp_quality_event")
            try:
                cur.execute(
                    """
                    INSERT INTO offer_quality_events (
                        oferta_id,
                        fuente_id,
                        institucion_id,
                        url_oferta,
                        decision,
                        primary_reason_code,
                        reason_codes,
                        reason_detail,
                        quality_score,
                        signals_json
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb
                    )
                    """,
                    (
                        oferta_id,
                        fuente_id,
                        institucion_id,
                        url_oferta,
                        validation.decision.value,
                        validation.primary_reason_code.value if validation.primary_reason_code else None,
                        json.dumps([code.value for code in validation.reason_codes]),
                        validation.reason_detail,
                        validation.quality_score,
                        json.dumps(validation.signals_json, ensure_ascii=False),
                    ),
                )
                cur.execute("RELEASE SAVEPOINT sp_quality_event")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT sp_quality_event")

    def save_catalog_event(self, conn, *, institucion_id: int | None, event_type: str, detail: str, payload: dict[str, Any] | None = None) -> None:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT sp_catalog_event")
            try:
                cur.execute(
                    """
                    INSERT INTO catalog_integrity_events (
                        institucion_id,
                        event_type,
                        detail,
                        payload
                    ) VALUES (%s, %s, %s, %s::jsonb)
                    """,
                    (
                        institucion_id,
                        event_type,
                        detail,
                        json.dumps(payload or {}, ensure_ascii=False),
                    ),
                )
                cur.execute("RELEASE SAVEPOINT sp_catalog_event")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT sp_catalog_event")

    def get_institution_noise_ratio(self, conn, institucion_id: int | None) -> float:
        if not institucion_id:
            return 0.0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(
                            SUM(CASE WHEN decision IN ('reject', 'manual_review') THEN 1 ELSE 0 END)::float
                            / NULLIF(COUNT(*), 0),
                            0
                        )
                    FROM offer_quality_events
                    WHERE institucion_id = %s
                    """,
                    (institucion_id,),
                )
                row = cur.fetchone()
                return float(row[0] or 0.0)
        except Exception:
            return 0.0
