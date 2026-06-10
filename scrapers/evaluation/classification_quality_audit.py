"""Auditoría estadística de calidad de clasificación.

Esta herramienta consume eventos reales producidos por el pipeline
(``offer_quality_events`` y `policy_*` que ``intake_validate_offer`` agrega
al dict de la oferta) y emite un reporte cuantitativo con:

- Distribución de decisiones (publish/review/reject) y reason_codes.
- Distribución del ``policy_score`` y ``quality_score`` por decisión.
- Detección de **anomalías** que delatan miscalibración:
    * ``publish`` con señales URL negativas presentes (posible falso positivo).
    * ``reject`` con sólo señales positivas detectadas (posible falso negativo).
    * ``review`` con score muy alto (umbral mal calibrado).
- Análisis por institución y por plataforma.
- Sugerencias accionables: tokens recurrentes en cargos rechazados que aún
  no figuran en ``NEGATIVE_PATTERNS``, y *path components* recurrentes en
  URLs rechazadas que faltan en ``NEGATIVE_URL_PARTS``.

Acepta tres orígenes de datos:

1. **JSONL en disco** (un evento por línea) — formato preferido para corridas
   reproducibles. Cada línea es un dict con los campos relevantes; ver
   ``AUDIT_RECORD_FIELDS`` para el contrato.
2. **JSON con array** (``{"records": [...]}``) — mismo contrato, conveniente
   cuando el productor exporta de una sola vez.
3. **Conexión psycopg2** — si la variable de entorno ``DB_PASSWORD`` está
   disponible, ``run_audit_from_db`` lee de ``offer_quality_events``.

Diseño:
- Sin estado global. ``ClassificationQualityAudit.run`` es puro y determinístico.
- No modifica la base. Sólo lee y agrega.
- El módulo es importable sin DB ni red; los conectores son opcionales.

CLI:

    python -m scrapers.evaluation.classification_quality_audit \
        --input reports/quality_events.jsonl \
        --output reports/classification_audit.json
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence
from urllib.parse import urlparse

from classification.policy import NEGATIVE_PATTERNS, NEGATIVE_URL_PARTS, POSITIVE_KEYWORDS


AUDIT_RECORD_FIELDS = (
    # Identificadores
    "oferta_id",
    "fuente_id",
    "institucion_id",
    "institucion_nombre",
    "plataforma",
    # Decisión y razones
    "decision",                # "publish" | "review" | "reject"
    "primary_reason_code",     # ReasonCode.value | None
    "reason_codes",            # list[str]
    "reason_detail",           # str | None
    # Scores
    "quality_score",           # float [0, 1]
    "policy_score",            # float [-1, 1]  (intake.py)
    "classification_score",    # float [0, 1]   (rule_engine)
    # Texto
    "url_oferta",
    "cargo",
    "descripcion_excerpt",     # primeros ~600 chars opcionales
    "policy_reason_codes",     # list[str] de classify_offer_candidate
    "positive_signals",        # list[str]
    "negative_signals",        # list[str]
    "rule_trace",              # list[dict] (rule_id, weight, reason)
    "is_job_posting",          # bool del classifier (post-LLM si aplica)
    "used_llm",                # bool
    "needs_review",            # bool
    # Persistencia
    "created_at",              # ISO8601 str
)


# ─────────────────────────── Estructuras de salida ────────────────────


@dataclass(slots=True)
class DecisionStats:
    decision: str
    count: int
    percentage: float
    quality_score: dict[str, float]
    policy_score: dict[str, float]
    classification_score: dict[str, float]


@dataclass(slots=True)
class ReasonCodeStats:
    reason_code: str
    count: int
    percentage: float
    by_decision: dict[str, int]


@dataclass(slots=True)
class AnomalyBucket:
    name: str
    description: str
    count: int
    sample_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class HeuristicSuggestion:
    target: str  # "NEGATIVE_PATTERNS" | "NEGATIVE_URL_PARTS" | "POSITIVE_KEYWORDS"
    candidate: str
    support: int  # cuántos eventos lo respaldan
    rationale: str


@dataclass(slots=True)
class ClassificationAuditReport:
    generated_at_utc: str
    total_records: int
    ruleset_version: str | None
    by_decision: list[DecisionStats]
    by_reason_code: list[ReasonCodeStats]
    by_institution: dict[str, dict[str, int]]
    by_platform: dict[str, dict[str, int]]
    score_buckets: dict[str, dict[str, int]]
    anomalies: list[AnomalyBucket]
    suggestions: list[HeuristicSuggestion]
    coverage: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {
            "generated_at_utc": self.generated_at_utc,
            "total_records": self.total_records,
            "ruleset_version": self.ruleset_version,
            "by_decision": [asdict(s) for s in self.by_decision],
            "by_reason_code": [asdict(r) for r in self.by_reason_code],
            "by_institution": self.by_institution,
            "by_platform": self.by_platform,
            "score_buckets": self.score_buckets,
            "anomalies": [
                {
                    "name": b.name,
                    "description": b.description,
                    "count": b.count,
                    "sample_records": b.sample_records,
                }
                for b in self.anomalies
            ],
            "suggestions": [asdict(s) for s in self.suggestions],
            "coverage": self.coverage,
        }


# ─────────────────────────── Helpers determinísticos ──────────────────


_TOKEN_RE = re.compile(r"[a-záéíóúñ]{4,}", re.IGNORECASE)
# Stopwords + palabras frecuentes que no aportan a heurísticas
_STOPWORDS = frozenset({
    "para", "este", "esta", "estos", "estas", "como", "sobre", "entre",
    "desde", "hasta", "donde", "cuando", "porque", "tambien", "tambi",
    "anos", "ano", "todos", "todas", "cada", "siendo", "sera", "fueron",
    "nuestro", "nuestra", "nuestros", "nuestras", "haber", "hacer", "hace",
    "deben", "debera", "tener", "tiene", "tuvo", "puede", "pueden",
    "antes", "despu", "luego", "menos", "mucho", "mucha", "muchos", "muchas",
    "sino", "tras", "mientras", "luego", "general", "especial", "siempre",
    "publica", "publico", "publicas", "publicos",  # demasiado genérico
    "respec", "frente", "demas", "plazo",
    "informacion", "datos", "documento", "documentos", "archivo", "archivos",
    "favor", "modo", "manera", "forma", "tipo", "casos", "caso",
    "parte", "lugar", "fecha", "fechas", "hora", "horas",
})


def _norm(text: Any) -> str:
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", str(text))
    return s.encode("ascii", "ignore").decode("ascii").lower().strip()


def _tokens(text: str | None) -> list[str]:
    if not text:
        return []
    norm = _norm(text)
    return [tok for tok in _TOKEN_RE.findall(norm) if tok not in _STOPWORDS]


def _percentile(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    arr = sorted(values)
    k = (len(arr) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(arr[int(k)])
    return float(arr[f] * (c - k) + arr[c] * (k - f))


def _score_summary(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {"count": 0, "mean": 0.0, "median": 0.0, "p25": 0.0, "p75": 0.0, "min": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 4),
        "median": round(statistics.median(values), 4),
        "p25": round(_percentile(values, 0.25), 4),
        "p75": round(_percentile(values, 0.75), 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
    }


def _bucket(score: float, *, edges: Sequence[float] = (0.0, 0.2, 0.4, 0.55, 0.7, 0.8, 0.9)) -> str:
    s = max(0.0, min(1.0, score))
    for hi in edges[1:]:
        if s < hi:
            return f"<{hi:.2f}"
    return f">={edges[-1]:.2f}"


def _url_path(url: str | None) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).path.lower()
    except Exception:
        return ""


def _path_segments(url: str | None) -> list[str]:
    path = _url_path(url)
    return [seg for seg in path.split("/") if seg]


def _existing_negative_path_parts() -> set[str]:
    """Pre-procesa NEGATIVE_URL_PARTS sin la barra inicial para comparar."""
    out: set[str] = set()
    for part in NEGATIVE_URL_PARTS:
        out.add(part.lstrip("/"))
        out.add(part.replace("/", "").strip())
    return {p for p in out if p}


# ─────────────────────────── Carga de datos ───────────────────────────


def iter_records_from_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        for ln, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"linea {ln} no es JSON valido: {exc}") from exc


def iter_records_from_json(path: Path) -> Iterator[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        yield from payload
    elif isinstance(payload, dict):
        rows = payload.get("records") or payload.get("results") or payload.get("events") or []
        if not isinstance(rows, list):
            raise ValueError("'records'/'results'/'events' debe ser una lista")
        yield from rows
    else:
        raise ValueError("formato JSON no reconocido (esperaba list o dict con 'records')")


def iter_records_auto(path: Path) -> Iterator[dict[str, Any]]:
    if path.suffix.lower() in (".jsonl", ".ndjson"):
        return iter_records_from_jsonl(path)
    return iter_records_from_json(path)


def iter_records_from_db(conn, *, since_iso: str | None = None, limit: int | None = None) -> Iterator[dict[str, Any]]:
    """Lector defensivo de ``offer_quality_events``.

    Genera dicts compatibles con ``AUDIT_RECORD_FIELDS``. Falla silenciosamente
    si la columna esperada no existe (versiones antiguas del schema).
    """
    sql = (
        "SELECT oqe.id, oqe.oferta_id, oqe.fuente_id, oqe.institucion_id, "
        "       oqe.url_oferta, oqe.decision, oqe.primary_reason_code, "
        "       oqe.reason_codes, oqe.reason_detail, oqe.quality_score, "
        "       oqe.signals_json, oqe.created_at "
        "FROM offer_quality_events oqe "
    )
    params: list[Any] = []
    if since_iso:
        sql += " WHERE oqe.created_at >= %s"
        params.append(since_iso)
    sql += " ORDER BY oqe.created_at DESC"
    if limit:
        sql += " LIMIT %s"
        params.append(int(limit))

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        cols = [c[0] for c in cur.description]
        for row in cur.fetchall():
            payload = dict(zip(cols, row))
            signals = payload.get("signals_json") or {}
            yield {
                "oferta_id": payload.get("oferta_id"),
                "fuente_id": payload.get("fuente_id"),
                "institucion_id": payload.get("institucion_id"),
                "decision": payload.get("decision"),
                "primary_reason_code": payload.get("primary_reason_code"),
                "reason_codes": payload.get("reason_codes") or [],
                "reason_detail": payload.get("reason_detail"),
                "quality_score": float(payload.get("quality_score") or 0.0),
                "url_oferta": payload.get("url_oferta"),
                "cargo": signals.get("cargo") if isinstance(signals, dict) else None,
                "policy_score": signals.get("policy_score") if isinstance(signals, dict) else None,
                "policy_reason_codes": signals.get("policy_reason_codes") if isinstance(signals, dict) else [],
                "classification_score": signals.get("classification_score") if isinstance(signals, dict) else None,
                "positive_signals": signals.get("positive_signals") if isinstance(signals, dict) else [],
                "negative_signals": signals.get("negative_signals") if isinstance(signals, dict) else [],
                "rule_trace": signals.get("rule_trace") if isinstance(signals, dict) else [],
                "is_job_posting": signals.get("is_job_posting") if isinstance(signals, dict) else None,
                "used_llm": signals.get("used_llm") if isinstance(signals, dict) else None,
                "needs_review": signals.get("needs_review") if isinstance(signals, dict) else None,
                "institucion_nombre": signals.get("institucion_nombre") if isinstance(signals, dict) else None,
                "plataforma": signals.get("plataforma") if isinstance(signals, dict) else None,
                "descripcion_excerpt": signals.get("descripcion_excerpt") if isinstance(signals, dict) else None,
                "created_at": payload.get("created_at").isoformat() if payload.get("created_at") else None,
            }


# ─────────────────────────── Núcleo de la auditoría ───────────────────


class ClassificationQualityAudit:
    """Calculadora pura de estadísticas de calidad de clasificación.

    El método ``run`` es determinístico: dada una secuencia de records,
    siempre produce el mismo reporte.
    """

    def __init__(
        self,
        *,
        ruleset_version: str | None = None,
        accept_threshold: float = 0.80,
        ambiguity_threshold: float = 0.55,
        sample_size_per_anomaly: int = 5,
        suggestion_min_support: int = 3,
        suggestion_max: int = 25,
    ) -> None:
        self.ruleset_version = ruleset_version
        self.accept_threshold = accept_threshold
        self.ambiguity_threshold = ambiguity_threshold
        self.sample_size_per_anomaly = sample_size_per_anomaly
        self.suggestion_min_support = suggestion_min_support
        self.suggestion_max = suggestion_max

    def run(self, records: Iterable[dict[str, Any]]) -> ClassificationAuditReport:
        records = list(records)
        total = len(records)
        if total == 0:
            return ClassificationAuditReport(
                generated_at_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
                total_records=0,
                ruleset_version=self.ruleset_version,
                by_decision=[],
                by_reason_code=[],
                by_institution={},
                by_platform={},
                score_buckets={},
                anomalies=[],
                suggestions=[],
                coverage={"with_quality_score": 0, "with_policy_score": 0, "with_classification_score": 0},
            )

        decision_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        reason_decision: dict[str, Counter[str]] = defaultdict(Counter)
        institution_decision: dict[str, Counter[str]] = defaultdict(Counter)
        platform_decision: dict[str, Counter[str]] = defaultdict(Counter)
        score_bucket_counts: dict[str, Counter[str]] = {
            "quality_score": Counter(),
            "policy_score": Counter(),
            "classification_score": Counter(),
        }

        coverage = {"with_quality_score": 0, "with_policy_score": 0, "with_classification_score": 0}

        # Soporte para sugerencias: tokens en cargo de records REJECT que no
        # estén ya cubiertos por NEGATIVE_PATTERNS, y segmentos de URL en REJECT
        # que no estén ya en NEGATIVE_URL_PARTS.
        reject_token_counter: Counter[str] = Counter()
        reject_url_segment_counter: Counter[str] = Counter()
        publish_token_counter: Counter[str] = Counter()  # contraste

        # Anomalías
        anomaly_records: dict[str, list[dict[str, Any]]] = defaultdict(list)

        existing_negatives_norm = " ".join(NEGATIVE_PATTERNS).lower()
        existing_url_parts = _existing_negative_path_parts()
        existing_positives = {_norm(k) for k in POSITIVE_KEYWORDS}

        for rec in records:
            decision = (rec.get("decision") or "unknown").lower()
            decision_buckets[decision].append(rec)

            primary_reason = rec.get("primary_reason_code") or "none"
            reason_decision[primary_reason][decision] += 1

            for code in rec.get("reason_codes") or []:
                if code != primary_reason:
                    reason_decision[code][decision] += 1

            inst_key = str(rec.get("institucion_id") or rec.get("institucion_nombre") or "sin_institucion")
            institution_decision[inst_key][decision] += 1

            platform_key = (rec.get("plataforma") or "unknown").lower()
            platform_decision[platform_key][decision] += 1

            for field_name in ("quality_score", "policy_score", "classification_score"):
                val = rec.get(field_name)
                if isinstance(val, (int, float)):
                    coverage[f"with_{field_name}"] += 1
                    # policy_score puede ser negativo: lo desplazamos a [0,1] sólo para bucket.
                    score_for_bucket = val if field_name != "policy_score" else (val + 1) / 2
                    score_bucket_counts[field_name][_bucket(float(score_for_bucket))] += 1

            cargo_tokens = _tokens(rec.get("cargo"))
            if decision == "reject":
                for tok in set(cargo_tokens):
                    reject_token_counter[tok] += 1
                for seg in set(_path_segments(rec.get("url_oferta"))):
                    reject_url_segment_counter[seg] += 1
            elif decision == "publish":
                for tok in set(cargo_tokens):
                    publish_token_counter[tok] += 1

            self._collect_anomalies(rec, decision, anomaly_records)

        by_decision = self._compute_decision_stats(decision_buckets, total)
        by_reason_code = self._compute_reason_stats(reason_decision, total)
        by_institution = {k: dict(v) for k, v in sorted(institution_decision.items(), key=lambda kv: -sum(kv[1].values()))[:50]}
        by_platform = {k: dict(v) for k, v in sorted(platform_decision.items(), key=lambda kv: -sum(kv[1].values()))}
        score_buckets = {k: dict(v) for k, v in score_bucket_counts.items()}

        anomalies = [
            AnomalyBucket(
                name=name,
                description=ANOMALY_DESCRIPTIONS[name],
                count=len(items),
                sample_records=items[: self.sample_size_per_anomaly],
            )
            for name, items in anomaly_records.items()
            if items
        ]
        anomalies.sort(key=lambda a: -a.count)

        suggestions = self._build_suggestions(
            reject_token_counter,
            publish_token_counter,
            reject_url_segment_counter,
            existing_negatives_norm,
            existing_url_parts,
            existing_positives,
        )

        return ClassificationAuditReport(
            generated_at_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            total_records=total,
            ruleset_version=self.ruleset_version,
            by_decision=by_decision,
            by_reason_code=by_reason_code,
            by_institution=by_institution,
            by_platform=by_platform,
            score_buckets=score_buckets,
            anomalies=anomalies,
            suggestions=suggestions,
            coverage=coverage,
        )

    # ── helpers internos ─────────────────────────────────────────────

    def _compute_decision_stats(
        self,
        decision_buckets: dict[str, list[dict[str, Any]]],
        total: int,
    ) -> list[DecisionStats]:
        out: list[DecisionStats] = []
        for decision, items in sorted(decision_buckets.items(), key=lambda kv: -len(kv[1])):
            quality_vals = [float(r["quality_score"]) for r in items if isinstance(r.get("quality_score"), (int, float))]
            policy_vals = [float(r["policy_score"]) for r in items if isinstance(r.get("policy_score"), (int, float))]
            classif_vals = [float(r["classification_score"]) for r in items if isinstance(r.get("classification_score"), (int, float))]
            out.append(
                DecisionStats(
                    decision=decision,
                    count=len(items),
                    percentage=round(len(items) / total, 4),
                    quality_score=_score_summary(quality_vals),
                    policy_score=_score_summary(policy_vals),
                    classification_score=_score_summary(classif_vals),
                )
            )
        return out

    def _compute_reason_stats(
        self,
        reason_decision: dict[str, Counter[str]],
        total: int,
    ) -> list[ReasonCodeStats]:
        out: list[ReasonCodeStats] = []
        for reason, counts in sorted(reason_decision.items(), key=lambda kv: -sum(kv[1].values())):
            count = sum(counts.values())
            out.append(
                ReasonCodeStats(
                    reason_code=reason,
                    count=count,
                    percentage=round(count / total, 4),
                    by_decision=dict(counts),
                )
            )
        return out

    def _collect_anomalies(
        self,
        rec: dict[str, Any],
        decision: str,
        bucket: dict[str, list[dict[str, Any]]],
    ) -> None:
        compact = self._compact_record(rec)
        url_path = _url_path(rec.get("url_oferta"))
        has_negative_url = any(part in url_path for part in NEGATIVE_URL_PARTS)
        positives = rec.get("positive_signals") or []
        negatives = rec.get("negative_signals") or []
        q = rec.get("quality_score")
        c = rec.get("classification_score")

        # 1. Publicado pese a URL negativa → posible falso positivo.
        if decision == "publish" and has_negative_url:
            bucket["publish_with_negative_url"].append(compact)

        # 2. Rechazado pero con señales positivas y sin negativas → posible
        #    falso negativo.
        if decision == "reject" and positives and not negatives:
            bucket["reject_with_only_positive_signals"].append(compact)

        # 3. Review con quality_score >= accept_threshold → umbral mal
        #    calibrado (debería ser publish).
        if decision == "review" and isinstance(q, (int, float)) and float(q) >= self.accept_threshold:
            bucket["review_above_accept_threshold"].append(compact)

        # 4. Publish con quality_score < ambiguity_threshold → posiblemente
        #    necesita revisión manual.
        if decision == "publish" and isinstance(q, (int, float)) and float(q) < self.ambiguity_threshold:
            bucket["publish_below_ambiguity_threshold"].append(compact)

        # 5. Reject con classification_score >= accept_threshold → contradicción
        #    interna.
        if decision == "reject" and isinstance(c, (int, float)) and float(c) >= self.accept_threshold:
            bucket["reject_with_high_classification_score"].append(compact)

        # 6. needs_review explícito sin reason_codes → trazabilidad débil.
        if rec.get("needs_review") and not (rec.get("reason_codes") or rec.get("primary_reason_code")):
            bucket["needs_review_without_reason"].append(compact)

        # 7. used_llm con decisión final reject → caso ambiguo que el LLM no
        #    salvó; útil para iterar el prompt de fallback.
        if rec.get("used_llm") and decision == "reject":
            bucket["llm_fallback_rejected"].append(compact)

    @staticmethod
    def _compact_record(rec: dict[str, Any]) -> dict[str, Any]:
        keep = (
            "oferta_id",
            "fuente_id",
            "institucion_id",
            "institucion_nombre",
            "plataforma",
            "decision",
            "primary_reason_code",
            "reason_codes",
            "quality_score",
            "policy_score",
            "classification_score",
            "url_oferta",
            "cargo",
        )
        return {k: rec.get(k) for k in keep if k in rec}

    def _build_suggestions(
        self,
        reject_tokens: Counter[str],
        publish_tokens: Counter[str],
        reject_url_segments: Counter[str],
        existing_negatives_norm: str,
        existing_url_parts: set[str],
        existing_positives: set[str],
    ) -> list[HeuristicSuggestion]:
        suggestions: list[HeuristicSuggestion] = []

        # Tokens fuertemente ligados a REJECT y casi nulos en PUBLISH.
        for token, support in reject_tokens.most_common():
            if support < self.suggestion_min_support:
                break
            if token in existing_negatives_norm:
                continue
            if token in existing_positives:
                continue
            publish_support = publish_tokens.get(token, 0)
            # discriminación: aparece >=3x más en reject que en publish.
            if publish_support * 3 > support:
                continue
            suggestions.append(
                HeuristicSuggestion(
                    target="NEGATIVE_PATTERNS",
                    candidate=rf"\b{token}\b",
                    support=support,
                    rationale=(
                        f"Aparece en {support} cargos rechazados y solo {publish_support} "
                        f"publicados. Discriminación >= 3x."
                    ),
                )
            )
            if len(suggestions) >= self.suggestion_max:
                return suggestions

        # Segmentos de URL recurrentes en REJECT, no presentes en NEGATIVE_URL_PARTS.
        for seg, support in reject_url_segments.most_common():
            if support < self.suggestion_min_support:
                break
            if seg in existing_url_parts:
                continue
            # Saltamos segmentos triviales o numéricos.
            if seg.isdigit() or len(seg) < 4:
                continue
            suggestions.append(
                HeuristicSuggestion(
                    target="NEGATIVE_URL_PARTS",
                    candidate=f"/{seg}",
                    support=support,
                    rationale=(
                        f"Segmento '/{seg}' aparece en {support} URLs de ofertas rechazadas, "
                        f"sin coincidencia previa en NEGATIVE_URL_PARTS."
                    ),
                )
            )
            if len(suggestions) >= self.suggestion_max:
                break

        return suggestions


ANOMALY_DESCRIPTIONS = {
    "publish_with_negative_url": (
        "Ofertas publicadas cuya URL contiene un path típicamente no-laboral. "
        "Probable falso positivo: el aviso pasó pero la URL sugiere noticias/eventos."
    ),
    "reject_with_only_positive_signals": (
        "Ofertas rechazadas con señales positivas detectadas y sin señales negativas. "
        "Posible falso negativo: revisar si el rechazo proviene de campos faltantes."
    ),
    "review_above_accept_threshold": (
        "Ofertas marcadas como review con quality_score sobre el umbral de aceptación. "
        "Sugiere recalibrar el umbral o el ranking de razones."
    ),
    "publish_below_ambiguity_threshold": (
        "Ofertas publicadas con quality_score por debajo del umbral de ambigüedad. "
        "Suelen ser fuentes confiables que pasan por catálogo; revisar si el score es justo."
    ),
    "reject_with_high_classification_score": (
        "Ofertas rechazadas pese a un classification_score alto: contradicción entre "
        "rule_engine y quality_validator."
    ),
    "needs_review_without_reason": (
        "Registros marcados needs_review sin reason_codes. Trazabilidad débil — el "
        "operador no puede entender por qué fueron flaggeadas."
    ),
    "llm_fallback_rejected": (
        "Casos en los que el fallback LLM se invocó y la decisión final fue reject. "
        "Útil para iterar el prompt o ajustar el umbral de fallback."
    ),
}


# ─────────────────────────── CLI ─────────────────────────────────────


def _resolve_input(args: argparse.Namespace) -> Iterable[dict[str, Any]]:
    if args.input:
        path = Path(args.input)
        if not path.exists():
            raise SystemExit(f"input no existe: {path}")
        return iter_records_auto(path)

    if args.from_db:
        try:
            import psycopg2  # noqa: F401  # late import: opcional
            from db.config import DB_CONFIG
        except Exception as exc:  # pragma: no cover - sólo CLI con DB
            raise SystemExit(f"--from-db no disponible: {exc}")

        import psycopg2

        conn = psycopg2.connect(**DB_CONFIG)
        try:
            return list(iter_records_from_db(conn, since_iso=args.since, limit=args.limit))
        finally:
            conn.close()

    raise SystemExit("debe indicarse --input PATH o --from-db")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Auditoría estadística de calidad de clasificación.")
    parser.add_argument("--input", help="Ruta a archivo JSON o JSONL con eventos de calidad.")
    parser.add_argument("--from-db", action="store_true", help="Lee desde offer_quality_events vía psycopg2.")
    parser.add_argument("--since", help="ISO8601 mínimo para filtrar created_at (sólo --from-db).")
    parser.add_argument("--limit", type=int, default=None, help="Máximo de filas a leer (sólo --from-db).")
    parser.add_argument("--output", help="Ruta de salida JSON. Si se omite, imprime a stdout.")
    parser.add_argument("--accept-threshold", type=float, default=0.80)
    parser.add_argument("--ambiguity-threshold", type=float, default=0.55)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--suggestion-min-support", type=int, default=3)
    parser.add_argument("--ruleset-version", default=None)
    args = parser.parse_args(argv)

    audit = ClassificationQualityAudit(
        ruleset_version=args.ruleset_version,
        accept_threshold=args.accept_threshold,
        ambiguity_threshold=args.ambiguity_threshold,
        sample_size_per_anomaly=args.sample_size,
        suggestion_min_support=args.suggestion_min_support,
    )
    report = audit.run(_resolve_input(args))
    text = json.dumps(report.to_json(), ensure_ascii=False, indent=2)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text, encoding="utf-8")
        print(f"reporte escrito en {args.output}")
    else:
        print(text)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))


__all__ = [
    "AUDIT_RECORD_FIELDS",
    "AnomalyBucket",
    "ClassificationAuditReport",
    "ClassificationQualityAudit",
    "DecisionStats",
    "HeuristicSuggestion",
    "ReasonCodeStats",
    "iter_records_auto",
    "iter_records_from_db",
    "iter_records_from_json",
    "iter_records_from_jsonl",
    "main",
]
