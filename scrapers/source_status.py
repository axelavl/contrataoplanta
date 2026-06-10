"""
Clasificación operativa de fuentes de scraping.

Decide, para cada institución del catálogo, qué estado tiene y qué scraper
corresponde ejecutar. El objetivo es separar "fuentes que vale la pena correr
en producción" de "fuentes que hoy sólo generan ruido" sin tener que borrar
nada del catálogo maestro.

La lógica tiene dos capas:

1. Reglas automáticas sobre los campos del JSON maestro
   (``plataforma_empleo``, ``estado_verificacion``, ``requiere_js``,
   ``publica_en_empleospublicos``, URLs, etc.).

2. Un archivo opcional de overrides (``scrapers/source_overrides.json``)
   que permite forzar el estado de IDs específicos sin tocar el catálogo.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable

from scrapers.base import clean_text, normalize_key


# ─────────────────────────────── Enums ────────────────────────────────

class SourceStatus(str, Enum):
    """Estado operativo de una fuente para el scraper."""

    ACTIVE = "active"                # se ejecuta en corridas normales
    EXPERIMENTAL = "experimental"    # sólo con --include-experimental
    MANUAL_REVIEW = "manual_review"  # sólo con --include-manual-review
    JS_REQUIRED = "js_required"      # requiere navegador con JS (fuera de scope)
    BLOCKED = "blocked"              # 403 / captcha / WAF confirmado
    BROKEN = "broken"                # DNS roto, URL inválida, 500 permanente
    NO_DATA = "no_data"              # respondió 200 pero nunca publicó ofertas
    DISABLED = "disabled"            # deshabilitada manualmente


class ScraperKind(str, Enum):
    """Qué scraper concreto le corresponde a una fuente."""

    EMPLEOS_PUBLICOS = "empleos_publicos"  # batch, no por-fuente
    WORDPRESS = "wordpress"
    GENERIC = "generic"
    CUSTOM_TRABAJANDO = "custom_trabajando"
    CUSTOM_HIRINGROOM = "custom_hiringroom"
    CUSTOM_BUK = "custom_buk"
    CUSTOM_PLAYWRIGHT = "custom_playwright"
    CUSTOM_POLICIA = "custom_policia"
    CUSTOM_FFAA = "custom_ffaa"
    SKIP = "skip"


class Confidence(str, Enum):
    VALIDATED = "validated"
    MEDIUM = "medium"
    LOW = "low"


# ─────────────────── Estados que entran a producción ──────────────────

DEFAULT_RUN_STATUSES: frozenset[SourceStatus] = frozenset({SourceStatus.ACTIVE})

RUNNABLE_KINDS: frozenset[ScraperKind] = frozenset(
    {
        ScraperKind.EMPLEOS_PUBLICOS,
        ScraperKind.WORDPRESS,
        ScraperKind.GENERIC,
        ScraperKind.CUSTOM_TRABAJANDO,
        ScraperKind.CUSTOM_HIRINGROOM,
        ScraperKind.CUSTOM_BUK,
        ScraperKind.CUSTOM_PLAYWRIGHT,
        ScraperKind.CUSTOM_POLICIA,
        ScraperKind.CUSTOM_FFAA,
    }
)


# ─────────────────────────── Decisión por fuente ───────────────────────

@dataclass(slots=True)
class SourceDecision:
    status: SourceStatus
    kind: ScraperKind
    confidence: Confidence
    reason: str
    covered_by_central: bool = False
    notes: str = ""
    # Tier de frecuencia (string para evitar dependencia circular con
    # ``scrapers.frequency_policy``). Si queda en None, run_scrapers.py
    # lo resuelve via ``frequency_policy.resolve_tier``.
    frequency_tier: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "kind": self.kind.value,
            "confidence": self.confidence.value,
            "reason": self.reason,
            "covered_by_central": self.covered_by_central,
            "notes": self.notes,
            "frequency_tier": self.frequency_tier,
        }


# ───────────────────────────── Overrides ──────────────────────────────

_OVERRIDES_PATH = Path(__file__).resolve().parent / "source_overrides.json"


@dataclass(slots=True)
class _OverridesCache:
    loaded: bool = False
    by_id: dict[int, dict[str, Any]] = field(default_factory=dict)


_cache = _OverridesCache()


def _load_overrides() -> dict[int, dict[str, Any]]:
    if _cache.loaded:
        return _cache.by_id
    _cache.loaded = True
    if not _OVERRIDES_PATH.exists():
        return _cache.by_id
    try:
        payload = json.loads(_OVERRIDES_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        return _cache.by_id
    items = payload if isinstance(payload, list) else payload.get("overrides", [])
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            ident = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        _cache.by_id[ident] = item
    return _cache.by_id


def _apply_override(inst_id: int, decision: SourceDecision) -> SourceDecision:
    override = _load_overrides().get(inst_id)
    if not override:
        return decision
    status = override.get("status")
    kind = override.get("kind")
    reason = override.get("reason") or decision.reason
    notes = override.get("notes") or ""
    tier = override.get("frequency_tier") or override.get("frecuencia")
    try:
        new_status = SourceStatus(status) if status else decision.status
    except ValueError:
        new_status = decision.status
    try:
        new_kind = ScraperKind(kind) if kind else decision.kind
    except ValueError:
        new_kind = decision.kind
    return SourceDecision(
        status=new_status,
        kind=new_kind,
        confidence=Confidence.VALIDATED,
        reason=f"override: {reason}",
        covered_by_central=decision.covered_by_central,
        notes=notes,
        frequency_tier=str(tier).strip().lower() if tier else None,
    )


# ────────────────────────── Lógica de clasificación ────────────────────

_JS_PLATFORM_MARKERS = ("spa", "vue", "react", "javascript")
_TRANSPARENCIA_MARKERS = ("transparencia", "transparen")


def _is_yes(value: Any) -> bool:
    return normalize_key(value) in {"si", "sí", "yes", "true", "1"}


def classify_source(institucion: dict[str, Any]) -> SourceDecision:
    """Devuelve la decisión operativa para la institución dada."""
    url_empleo = clean_text(institucion.get("url_empleo"))
    sitio_web = clean_text(institucion.get("sitio_web"))
    plataforma = normalize_key(institucion.get("plataforma_empleo"))
    sector = clean_text(institucion.get("sector"))
    verificado = normalize_key(institucion.get("estado_verificacion")) == "verificado"
    publica_ep = normalize_key(institucion.get("publica_en_empleospublicos"))
    covered_by_central = publica_ep in {"si", "parcialmente"}
    requiere_js = _is_yes(institucion.get("requiere_js"))

    decision: SourceDecision

    # 1. Sin URL: imposible scrapear
    if not url_empleo and not sitio_web:
        decision = SourceDecision(
            status=SourceStatus.BROKEN,
            kind=ScraperKind.SKIP,
            confidence=Confidence.VALIDATED,
            reason="sin url_empleo ni sitio_web",
            covered_by_central=covered_by_central,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    # 2. JS requerido: fuera del alcance del stack HTTP actual
    js_plat = any(marker in plataforma for marker in _JS_PLATFORM_MARKERS)
    if requiere_js or js_plat:
        decision = SourceDecision(
            status=SourceStatus.JS_REQUIRED,
            kind=ScraperKind.SKIP,
            confidence=Confidence.VALIDATED,
            reason="requiere navegador con JS",
            covered_by_central=covered_by_central,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    # 3. Publica en empleospublicos.cl "Sí": batch lo cubre, no corremos
    # scraper por-fuente. Esto evita duplicar trabajo.
    if publica_ep == "si":
        decision = SourceDecision(
            status=SourceStatus.ACTIVE,
            kind=ScraperKind.EMPLEOS_PUBLICOS,
            confidence=Confidence.VALIDATED,
            reason="cubierto por empleospublicos.cl batch",
            covered_by_central=True,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    # 4. Plataformas de terceros con módulo custom
    if "trabajando.cl" in plataforma or "trabajando cl" in plataforma:
        decision = SourceDecision(
            status=SourceStatus.ACTIVE if verificado else SourceStatus.EXPERIMENTAL,
            kind=ScraperKind.CUSTOM_TRABAJANDO,
            confidence=Confidence.VALIDATED if verificado else Confidence.MEDIUM,
            reason="portal trabajando.cl",
            covered_by_central=covered_by_central,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    if "hiringroom" in plataforma:
        decision = SourceDecision(
            status=SourceStatus.ACTIVE if verificado else SourceStatus.EXPERIMENTAL,
            kind=ScraperKind.CUSTOM_HIRINGROOM,
            confidence=Confidence.VALIDATED if verificado else Confidence.MEDIUM,
            reason="portal HiringRoom",
            covered_by_central=covered_by_central,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    if "buk" in plataforma:
        decision = SourceDecision(
            status=SourceStatus.ACTIVE if verificado else SourceStatus.EXPERIMENTAL,
            kind=ScraperKind.CUSTOM_BUK,
            confidence=Confidence.VALIDATED if verificado else Confidence.MEDIUM,
            reason="portal Buk",
            covered_by_central=covered_by_central,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    # 5. WordPress explícito → active si verificado, experimental si no
    if "wordpress" in plataforma:
        decision = SourceDecision(
            status=SourceStatus.ACTIVE if verificado else SourceStatus.EXPERIMENTAL,
            kind=ScraperKind.WORDPRESS,
            confidence=Confidence.VALIDATED if verificado else Confidence.MEDIUM,
            reason="WordPress" + (" verificado" if verificado else " sin verificar"),
            covered_by_central=covered_by_central,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    # 6. Sitios propios (Joomla, PHP, CMS, etc.)
    is_sitio_propio = (
        "sitio propio" in plataforma
        or "portal propio" in plataforma
        or "cms" in plataforma
        or "joomla" in plataforma
        or "php" in plataforma
    )
    is_transparencia = any(marker in plataforma for marker in _TRANSPARENCIA_MARKERS)

    if is_sitio_propio or is_transparencia:
        if verificado:
            decision = SourceDecision(
                status=SourceStatus.ACTIVE,
                kind=ScraperKind.GENERIC,
                confidence=Confidence.VALIDATED,
                reason="sitio propio verificado manualmente",
                covered_by_central=covered_by_central,
            )
        elif sector == "Municipal":
            # El gran bloque de 354 municipios sin verificar.
            # No los corremos en producción: cuestan tiempo y no retornan nada.
            decision = SourceDecision(
                status=SourceStatus.MANUAL_REVIEW,
                kind=ScraperKind.GENERIC,
                confidence=Confidence.LOW,
                reason="municipio con sitio propio sin verificar",
                covered_by_central=covered_by_central,
            )
        else:
            decision = SourceDecision(
                status=SourceStatus.MANUAL_REVIEW,
                kind=ScraperKind.GENERIC,
                confidence=Confidence.LOW,
                reason="sitio propio sin verificar",
                covered_by_central=covered_by_central,
            )
        return _apply_override(institucion.get("id") or -1, decision)

    # 7. Plataformas conocidas sin módulo: dejamos para revisión manual
    if plataforma:
        decision = SourceDecision(
            status=SourceStatus.MANUAL_REVIEW,
            kind=ScraperKind.SKIP,
            confidence=Confidence.LOW,
            reason=f"plataforma sin módulo dedicado: {plataforma}",
            covered_by_central=covered_by_central,
        )
        return _apply_override(institucion.get("id") or -1, decision)

    # 8. Default defensivo: sin datos suficientes para clasificar
    decision = SourceDecision(
        status=SourceStatus.DISABLED,
        kind=ScraperKind.SKIP,
        confidence=Confidence.LOW,
        reason="sin plataforma declarada",
        covered_by_central=covered_by_central,
    )
    return _apply_override(institucion.get("id") or -1, decision)


# ─────────────────────────── Helpers de corrida ────────────────────────

def enrich_with_status(
    instituciones: Iterable[dict[str, Any]],
) -> list[tuple[dict[str, Any], SourceDecision]]:
    """Devuelve tuplas (institución, decisión) para todas las del catálogo."""
    return [(inst, classify_source(inst)) for inst in instituciones]


def split_by_status(
    enriched: Iterable[tuple[dict[str, Any], SourceDecision]],
) -> dict[SourceStatus, list[dict[str, Any]]]:
    buckets: dict[SourceStatus, list[dict[str, Any]]] = {s: [] for s in SourceStatus}
    for inst, decision in enriched:
        buckets[decision.status].append(inst)
    return buckets


def filter_runnable(
    enriched: Iterable[tuple[dict[str, Any], SourceDecision]],
    allowed_statuses: set[SourceStatus],
    only_kind: ScraperKind | None = None,
) -> list[tuple[dict[str, Any], SourceDecision]]:
    result: list[tuple[dict[str, Any], SourceDecision]] = []
    for inst, decision in enriched:
        if decision.status not in allowed_statuses:
            continue
        if decision.kind not in RUNNABLE_KINDS:
            continue
        if only_kind is not None and decision.kind != only_kind:
            continue
        result.append((inst, decision))
    return result


def status_breakdown(
    enriched: Iterable[tuple[dict[str, Any], SourceDecision]],
) -> dict[str, int]:
    """Para imprimir al inicio de run_all.py y exponer en la API."""
    counter: dict[str, int] = {s.value: 0 for s in SourceStatus}
    for _, decision in enriched:
        counter[decision.status.value] += 1
    return counter


def kind_breakdown(
    enriched: Iterable[tuple[dict[str, Any], SourceDecision]],
) -> dict[str, int]:
    counter: dict[str, int] = {k.value: 0 for k in ScraperKind}
    for _, decision in enriched:
        counter[decision.kind.value] += 1
    return counter
