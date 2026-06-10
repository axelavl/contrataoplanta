"""
Política de frecuencia de scraping por familia de fuente.

Contexto del problema:
- Empleos Públicos (Servicio Civil) publica varias veces al día y los
  procesos pueden cerrar el mismo día. Necesita revisión cada pocas horas.
- Servicios autónomos grandes (Banco Central, Contraloría, Fiscalía) suelen
  tener publicación diaria/semanal pero más estable. Cada 12 h alcanza.
- Hospitales, universidades estatales, empresas del Estado: actividad media
  con periodos de poca actividad. Cada 12–24 h.
- Municipalidades: 345+ comunas, cada una publica unas pocas ofertas al
  año. Una corrida diaria carga el sistema sin retorno proporcional.
  Cada 48 h es suficiente; las muy chicas, semanal.
- Fuentes "experimentales" o "manual_review": chequeo semanal o eventual.

Esta política se materializa como ``FrequencyTier`` aplicable a cada
fuente. ``run_scrapers.py`` usa el tier para decidir si la fuente debe
ejecutarse en la pasada actual; permite también overrides manuales en
``source_overrides.json`` (campo ``frequency_tier``).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from scrapers.source_status import ScraperKind, SourceStatus


class FrequencyTier(str, Enum):
    """Cada cuánto debe consultarse una fuente.

    Nombres pensados para ser auto-explicativos.
    """

    CRITICAL = "critical"     # cada 3 h     - Empleos Públicos
    HIGH = "high"             # cada 6 h     - portales centralizados grandes
    MEDIUM = "medium"         # cada 12 h    - servicios públicos estándar
    LOW = "low"               # cada 48 h    - municipios, sitios poco activos
    EVENTUAL = "eventual"     # cada 168 h   - fuentes casi inactivas
    EXPLORATORY = "exploratory"  # cada 720 h - sólo descubrimiento manual


# Mapping tier → horas entre corridas. Ajustable sin tocar la enum.
TIER_HOURS: dict[FrequencyTier, int] = {
    FrequencyTier.CRITICAL: 3,
    FrequencyTier.HIGH: 6,
    FrequencyTier.MEDIUM: 12,
    FrequencyTier.LOW: 48,
    FrequencyTier.EVENTUAL: 168,
    FrequencyTier.EXPLORATORY: 720,
}


# Política de timeouts/reintentos por tier. Tier crítico merece más paciencia
# (no queremos perder ofertas por un timeout fugaz); tier bajo se corta antes
# para no gastar tiempo en sitios que no van a aportar.
@dataclass(frozen=True, slots=True)
class TierProfile:
    timeout_seg: int
    max_retries: int
    delay_seg: float
    max_candidate_urls: int
    open_pdf: bool
    use_playwright_fallback: bool


TIER_PROFILES: dict[FrequencyTier, TierProfile] = {
    FrequencyTier.CRITICAL: TierProfile(
        timeout_seg=20,
        max_retries=3,
        delay_seg=0.3,
        max_candidate_urls=10,
        open_pdf=True,
        use_playwright_fallback=True,
    ),
    FrequencyTier.HIGH: TierProfile(
        timeout_seg=15,
        max_retries=2,
        delay_seg=0.5,
        max_candidate_urls=6,
        open_pdf=True,
        use_playwright_fallback=True,
    ),
    FrequencyTier.MEDIUM: TierProfile(
        timeout_seg=12,
        max_retries=2,
        delay_seg=1.0,
        max_candidate_urls=4,
        open_pdf=True,
        use_playwright_fallback=False,
    ),
    FrequencyTier.LOW: TierProfile(
        timeout_seg=8,
        max_retries=1,
        delay_seg=1.5,
        max_candidate_urls=3,
        open_pdf=False,
        use_playwright_fallback=False,
    ),
    FrequencyTier.EVENTUAL: TierProfile(
        timeout_seg=6,
        max_retries=1,
        delay_seg=2.0,
        max_candidate_urls=2,
        open_pdf=False,
        use_playwright_fallback=False,
    ),
    FrequencyTier.EXPLORATORY: TierProfile(
        timeout_seg=10,
        max_retries=2,
        delay_seg=2.0,
        max_candidate_urls=7,
        open_pdf=True,
        use_playwright_fallback=False,
    ),
}


def hours_for_tier(tier: FrequencyTier) -> int:
    return TIER_HOURS[tier]


def profile_for_tier(tier: FrequencyTier) -> TierProfile:
    return TIER_PROFILES[tier]


def cooldown_hours_for_retry_policy(retry_policy: str | None) -> int:
    """Horas de cooldown antes de re-evaluar una fuente según su última retry_policy.

    ``RetryPolicy`` en models.py usa los mismos valores de string que
    ``FrequencyTier``, por lo que se resuelven directamente en ``TIER_HOURS``.
    Si el valor no es reconocido, se aplica el cooldown de MEDIUM (12 h).
    """
    if not retry_policy:
        return TIER_HOURS[FrequencyTier.MEDIUM]
    try:
        tier = FrequencyTier(retry_policy.strip().lower())
        return TIER_HOURS[tier]
    except ValueError:
        return TIER_HOURS[FrequencyTier.MEDIUM]


def should_evaluate_now(
    *,
    retry_policy: str | None,
    last_evaluated_at: "datetime | None",  # type: ignore[name-defined]  # forward ref
    now: "datetime | None" = None,
) -> bool:
    """Decide si una fuente debe re-evaluarse en la corrida actual.

    Una fuente entra en cooldown desde su última evaluación por el número de
    horas que indica su ``retry_policy``.  Si nunca fue evaluada, siempre
    se incluye.

    Args:
        retry_policy: Valor de ``RetryPolicy`` de la última evaluación guardada.
        last_evaluated_at: ``evaluated_at`` de esa evaluación (timezone-aware).
        now: Momento de referencia (por defecto ``datetime.now(UTC)``).

    Returns:
        ``True`` si la fuente debe evaluarse ahora, ``False`` si sigue en cooldown.
    """
    if last_evaluated_at is None:
        return True
    from datetime import datetime, timezone, timedelta
    _now = now or datetime.now(tz=timezone.utc)
    # Normalizar a UTC si last_evaluated_at es naive
    if last_evaluated_at.tzinfo is None:
        last_evaluated_at = last_evaluated_at.replace(tzinfo=timezone.utc)
    cooldown = timedelta(hours=cooldown_hours_for_retry_policy(retry_policy))
    return (_now - last_evaluated_at) >= cooldown


def default_tier_for(
    *,
    kind: ScraperKind,
    sector: str | None,
    status: SourceStatus,
    publica_en_empleospublicos: str | None = None,
) -> FrequencyTier:
    """Asignación automática de tier cuando no hay override manual.

    Reglas, en orden de prioridad:
    1. Status no-active → EXPLORATORY (apenas chequeo periódico).
    2. Empleos Públicos batch → CRITICAL (la fuente más densa del país).
    3. Plataformas centralizadas (Trabajando/Hiringroom/Buk) en grandes
       empresas del Estado → HIGH.
    4. Sitios de servicios públicos / autónomos / universidades → MEDIUM.
    5. Municipios → LOW.
    6. Resto (default) → MEDIUM.
    """
    if status != SourceStatus.ACTIVE:
        return FrequencyTier.EXPLORATORY

    if kind == ScraperKind.EMPLEOS_PUBLICOS:
        return FrequencyTier.CRITICAL

    sector_norm = (sector or "").strip().lower()

    # Municipalidades: explícitamente bajos
    if sector_norm.startswith("municip") or "municip" in sector_norm:
        return FrequencyTier.LOW

    # Plataformas centralizadas (alto volumen)
    if kind in {
        ScraperKind.CUSTOM_TRABAJANDO,
        ScraperKind.CUSTOM_HIRINGROOM,
        ScraperKind.CUSTOM_BUK,
    }:
        return FrequencyTier.HIGH

    # FFAA / Policía: actualizan poco pero son críticos cuando publican
    if kind in {ScraperKind.CUSTOM_FFAA, ScraperKind.CUSTOM_POLICIA}:
        return FrequencyTier.MEDIUM

    # Sitios propios verificados → media
    if kind in {ScraperKind.GENERIC, ScraperKind.WORDPRESS}:
        # Si además publica en EP, podemos espaciar más (EP cubre la urgencia)
        if (publica_en_empleospublicos or "").strip().lower() == "si":
            return FrequencyTier.LOW
        return FrequencyTier.MEDIUM

    return FrequencyTier.MEDIUM


def resolve_tier(
    institucion: dict[str, Any],
    *,
    kind: ScraperKind,
    status: SourceStatus,
    override: dict[str, Any] | None = None,
) -> FrequencyTier:
    """Combina default automático con override manual.

    Si ``override`` trae ``frequency_tier`` válido, gana. Si no, se aplica
    ``default_tier_for``.
    """
    if override:
        raw = override.get("frequency_tier") or override.get("frecuencia")
        if raw:
            try:
                return FrequencyTier(str(raw).strip().lower())
            except ValueError:
                pass

    return default_tier_for(
        kind=kind,
        sector=institucion.get("sector"),
        status=status,
        publica_en_empleospublicos=institucion.get("publica_en_empleospublicos"),
    )


__all__ = [
    "FrequencyTier",
    "TIER_HOURS",
    "TIER_PROFILES",
    "TierProfile",
    "cooldown_hours_for_retry_policy",
    "default_tier_for",
    "hours_for_tier",
    "profile_for_tier",
    "resolve_tier",
    "should_evaluate_now",
]
