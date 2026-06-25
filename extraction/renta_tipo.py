"""Clasificación del *tipo* de renta: bruta vs líquida.

Regla de negocio (estándar del proyecto): la renta que se informa al usuario
debe ser la **bruta**. Cuando la fuente solo publica la renta **líquida**, ese
hecho tiene que quedar **señalado** explícitamente para no inducir a error
(una renta líquida es siempre menor que la bruta equivalente).

Este módulo es la única fuente de verdad para:

- ``clasificar_tipo_renta(texto)`` → ``"bruta"`` | ``"liquida"`` | ``"indeterminada"``.
- ``resolver_tipo_renta(*fuentes)`` → clasifica usando varias fuentes de texto
  por orden de prioridad (p.ej. ``renta_texto`` antes que ``descripcion``).
- ``anotar_renta_texto(texto, tipo)`` → devuelve ``renta_texto`` con la marca
  explícita del tipo cuando corresponde, sin duplicarla si ya estaba.

Todo el pipeline (scrapers legacy, pipeline nuevo y capa de persistencia)
converge aquí para no repetir heurísticas dispersas.
"""

from __future__ import annotations

import re
import unicodedata

# Valores canónicos almacenados en la columna ``ofertas.renta_tipo``.
TIPO_BRUTA = "bruta"
TIPO_LIQUIDA = "liquida"
TIPO_INDETERMINADA = "indeterminada"

# Longitud máxima del campo ``renta_texto`` en la BD (VARCHAR(200)).
_MAX_RENTA_TEXTO = 200

# Marcas legibles que se anexan a ``renta_texto``.
_SUFIJO = {
    TIPO_BRUTA: "(renta bruta)",
    TIPO_LIQUIDA: "(renta líquida)",
}

# "líquid" cubre líquida/líquido/liquida/liquido (con y sin tilde, tras plegar
# acentos queda "liquid"). "brut" cubre bruta/bruto/brutos.
_RE_LIQUIDA = re.compile(r"\bliquid")
_RE_BRUTA = re.compile(r"\bbrut")


def _plegar(texto: str | None) -> str:
    """Minúsculas sin acentos para comparar de forma robusta."""
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(texto))
    sin_acentos = "".join(c for c in nfkd if not unicodedata.combining(c))
    return sin_acentos.lower()


def clasificar_tipo_renta(texto: str | None) -> str:
    """Determina si el texto describe una renta bruta, líquida o indeterminada.

    - Si menciona "bruta/bruto" → ``"bruta"`` (es el tipo que se reporta; si el
      texto trae ambos, bruta tiene prioridad porque es el valor canónico).
    - Si solo menciona "líquida/líquido" → ``"liquida"``.
    - Si no hay señal explícita → ``"indeterminada"`` (NO se asume bruta).
    """
    plano = _plegar(texto)
    if not plano:
        return TIPO_INDETERMINADA
    tiene_bruta = bool(_RE_BRUTA.search(plano))
    tiene_liquida = bool(_RE_LIQUIDA.search(plano))
    if tiene_bruta:
        return TIPO_BRUTA
    if tiene_liquida:
        return TIPO_LIQUIDA
    return TIPO_INDETERMINADA


def resolver_tipo_renta(*fuentes: str | None) -> str:
    """Clasifica usando varias fuentes de texto en orden de prioridad.

    Devuelve el primer tipo concluyente (bruta/líquida). Si ninguna fuente es
    concluyente, devuelve ``"indeterminada"``.
    """
    for fuente in fuentes:
        tipo = clasificar_tipo_renta(fuente)
        if tipo != TIPO_INDETERMINADA:
            return tipo
    return TIPO_INDETERMINADA


def _ya_marcado(plano: str, tipo: str) -> bool:
    if tipo == TIPO_BRUTA:
        return bool(_RE_BRUTA.search(plano))
    if tipo == TIPO_LIQUIDA:
        return bool(_RE_LIQUIDA.search(plano))
    return True  # indeterminada: nunca se anexa marca


def anotar_renta_texto(texto: str | None, tipo: str) -> str | None:
    """Devuelve ``renta_texto`` con el tipo señalado de forma explícita.

    - No duplica la marca si el texto ya menciona bruta/líquida.
    - Para ``indeterminada`` no inventa una marca (no se asume que sea bruta).
    - Respeta el tope de 200 caracteres de la columna.
    - Si no hay texto pero el tipo es líquida, deja constancia mínima.
    """
    base = (texto or "").strip()
    sufijo = _SUFIJO.get(tipo)
    if not sufijo:
        return base or None

    plano = _plegar(base)
    if base and _ya_marcado(plano, tipo):
        return base[:_MAX_RENTA_TEXTO]

    if not base:
        # Sin texto previo: al menos dejamos señalado el tipo (relevante cuando
        # solo hay líquida y un monto numérico en otra columna).
        return sufijo

    candidato = f"{base} {sufijo}"
    if len(candidato) <= _MAX_RENTA_TEXTO:
        return candidato
    # No cabe el sufijo: recortamos el texto base para que la marca quepa.
    espacio = _MAX_RENTA_TEXTO - len(sufijo) - 1
    if espacio <= 0:
        return sufijo
    return f"{base[:espacio].rstrip()} {sufijo}"
