"""Cliente Anthropic real para el punto de extensión ``LLMClient``.

``classification/llm_fallback_classifier.py`` define el Protocol
``LLMClient.classify_content(prompt: dict) -> dict`` y trae un mock
determinístico (``HeuristicLLMClient``). Este módulo aporta la
implementación productiva: usa la API de Claude para dictaminar si una
página es realmente una **oferta laboral** o ruido (noticia, comunicado,
página institucional, evento…).

Se mantiene en español (convención del repo). El módulo importa sin la
dependencia ``anthropic`` instalada: el SDK se importa de forma perezosa
sólo al primer ``classify_content`` para que el resto del código siga
arrancando sin la librería ni la API key.

Uso:

    from classification.anthropic_client import AnthropicLLMClient

    client = AnthropicLLMClient()          # lee ANTHROPIC_API_KEY del entorno
    veredicto = client.classify_content(prompt_dict)

``prompt_dict`` sigue el contrato de ``build_llm_summary`` (claves
``title``, ``content_preview``, ``job_fragments``, etc.); también acepta
los campos extra que produce la auditoría (``institucion``, ``url``,
``region``, ``fecha_cierre``).
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

# Precio público por 1M de tokens (input, output) en USD. Sólo para estimar
# costo en los reportes — NO se usa para facturar nada.
PRECIOS_USD_POR_1M: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-opus-4-8": (5.0, 25.0),
}

MODELO_DEFAULT = "claude-haiku-4-5"

_CONTENT_TYPES = (
    "job_posting",
    "news_article",
    "institutional_page",
    "event",
    "other",
)

_SYSTEM_PROMPT = (
    "Eres un revisor experto del agregador chileno de empleo público "
    "contrataoplanta. Tu única tarea es decidir si el contenido entregado es "
    "REALMENTE una oferta o concurso laboral vigente (un cargo al que una "
    "persona puede postular), o si es otra cosa que se coló por error: una "
    "noticia, un comunicado de prensa, un acta o resultado de concurso ya "
    "cerrado, una página institucional genérica, un evento, una nómina de "
    "transparencia, etc.\n\n"
    "Criterios:\n"
    "- ES oferta si describe un cargo concreto a proveer con señales como "
    "funciones, requisitos, renta o grado, plazo o forma de postulación.\n"
    "- NO es oferta si es informativa (noticia/comunicado/evento), si anuncia "
    "RESULTADOS o el CIERRE de un proceso ya terminado, o si es una página "
    "índice/transparencia sin un cargo postulable.\n"
    "- Ante la duda real, deja confidence baja (cerca de 0.5) y explica por qué.\n\n"
    "No inventes datos que no estén en el texto. Responde SOLO con un objeto "
    "JSON válido, sin texto adicional ni markdown, con exactamente estas "
    "claves:\n"
    '{"is_job_posting": bool, "content_type": '
    '"job_posting|news_article|institutional_page|event|other", '
    '"confidence": number entre 0 y 1, "reason": "frase breve en español"}'
)


def _coerce_response(data: dict[str, Any], *, fallback_title: str | None) -> dict[str, Any]:
    """Normaliza la respuesta del modelo al contrato de ``LLMClassifierResponse``."""
    is_job = bool(data.get("is_job_posting"))
    content_type = str(data.get("content_type") or ("job_posting" if is_job else "other"))
    if content_type not in _CONTENT_TYPES:
        content_type = "job_posting" if is_job else "other"
    try:
        confidence = float(data.get("confidence"))
    except (TypeError, ValueError):
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))
    reason = str(data.get("reason") or "sin justificación del modelo").strip()[:500]
    return {
        "is_job_posting": is_job,
        "content_type": content_type,
        "confidence": confidence,
        "reason": reason,
        "likely_job_title": data.get("likely_job_title") or fallback_title,
        "evidence_for_job": list(data.get("evidence_for_job") or []),
        "evidence_against_job": list(data.get("evidence_against_job") or []),
    }


def _extraer_json(texto: str) -> dict[str, Any]:
    """Parser tolerante: intenta ``json.loads`` y, si falla, extrae el primer
    objeto ``{...}`` del texto. Haiku con un esquema chico casi siempre
    devuelve JSON limpio; esto es defensa en profundidad."""
    texto = (texto or "").strip()
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{.*\}", texto, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    raise ValueError(f"el modelo no devolvió JSON parseable: {texto[:200]!r}")


def _construir_mensaje(prompt: dict[str, Any]) -> str:
    """Arma el texto del usuario a partir del dict de contexto."""
    partes: list[str] = []
    if prompt.get("title"):
        partes.append(f"Título / cargo: {prompt['title']}")
    if prompt.get("institucion"):
        partes.append(f"Institución: {prompt['institucion']}")
    if prompt.get("region"):
        partes.append(f"Región: {prompt['region']}")
    if prompt.get("fecha_cierre"):
        partes.append(f"Fecha de cierre informada: {prompt['fecha_cierre']}")
    if prompt.get("url"):
        partes.append(f"URL: {prompt['url']}")
    if prompt.get("breadcrumbs"):
        partes.append(f"Migas de pan: {prompt['breadcrumbs']}")
    if prompt.get("job_fragments"):
        partes.append("Fragmentos laborales detectados: " + " | ".join(prompt["job_fragments"][:8]))
    if prompt.get("non_job_fragments"):
        partes.append("Fragmentos NO laborales detectados: " + " | ".join(prompt["non_job_fragments"][:8]))
    contenido = (prompt.get("content_preview") or "").strip()
    if contenido:
        partes.append("\nContenido de la página (recortado):\n" + contenido[:4000])
    if not partes:
        partes.append("(sin contenido)")
    return "\n".join(partes)


class AnthropicLLMClient:
    """Implementación de ``LLMClient`` sobre la API de Claude.

    Acumula el uso de tokens en la instancia para que la auditoría pueda
    reportar costo. ``model`` por defecto es Haiku 4.5 (barato y suficiente
    para esta decisión binaria); se puede subir a Sonnet si se quiere más
    precisión en los casos límite.
    """

    def __init__(
        self,
        *,
        model: str = MODELO_DEFAULT,
        api_key: str | None = None,
        max_tokens: int = 400,
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens
        self._api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self._client = None  # se inicializa perezosamente
        # Contadores acumulados para estimar costo.
        self.calls = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        if not self._api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY no está definida. Expórtala en el entorno "
                "(o pásala como api_key=) antes de usar AnthropicLLMClient."
            )
        try:
            import anthropic  # import perezoso
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "Falta la dependencia 'anthropic'. Instálala con "
                "`pip install anthropic` (ya está en requirements.txt)."
            ) from exc
        self._client = anthropic.Anthropic(api_key=self._api_key)
        return self._client

    def classify_content(self, prompt: dict[str, Any]) -> dict[str, Any]:
        client = self._ensure_client()
        mensaje = _construir_mensaje(prompt)
        resp = client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": mensaje}],
        )
        # Contabiliza uso.
        self.calls += 1
        usage = getattr(resp, "usage", None)
        if usage is not None:
            self.total_input_tokens += int(getattr(usage, "input_tokens", 0) or 0)
            self.total_output_tokens += int(getattr(usage, "output_tokens", 0) or 0)
        texto = "".join(
            block.text for block in resp.content if getattr(block, "type", None) == "text"
        )
        data = _extraer_json(texto)
        return _coerce_response(data, fallback_title=prompt.get("title"))

    # ── Estimación de costo (sólo reportes) ───────────────────────────

    def estimated_cost_usd(self) -> float:
        precio_in, precio_out = PRECIOS_USD_POR_1M.get(self.model, (0.0, 0.0))
        return (
            self.total_input_tokens / 1_000_000 * precio_in
            + self.total_output_tokens / 1_000_000 * precio_out
        )

    def usage_summary(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "calls": self.calls,
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "estimated_cost_usd": round(self.estimated_cost_usd(), 4),
        }


__all__ = ["AnthropicLLMClient", "MODELO_DEFAULT", "PRECIOS_USD_POR_1M"]
