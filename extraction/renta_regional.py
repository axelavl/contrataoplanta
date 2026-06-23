"""
Parser de tablas de renta multi-región (Plan Parte 1.5).

Muchas bases publican la renta como una tabla aplanada en el texto, p. ej.:

    "Renta bruta mes sin bono   Renta bruta mes con bono
     Metropolitana   $818.341   $1.396.510
     Valparaíso      $750.000   $1.200.000"

Este módulo reconstruye esa tabla en una lista de filas estructuradas
``RentaRegion(region, sin_bono, con_bono, notas)``. Si NO se puede parsear con
confianza, devuelve ``None`` y el caller debe conservar el texto crudo
(``renta_texto``) — nunca inventa ni arma una tabla rota.

Es una función pura (sin DB, sin red): testeable de forma aislada.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


# ── Catálogo de regiones de Chile: nombre canónico → alias plegados ──────────
# Los alias están sin tildes y en minúscula porque el matching se hace sobre el
# texto plegado. Se ordenan por longitud (desc) al construir el regex para que
# "los rios" gane antes que un eventual "los".
_REGIONES: dict[str, tuple[str, ...]] = {
    "Arica y Parinacota": ("arica y parinacota", "arica"),
    "Tarapacá": ("tarapaca",),
    "Antofagasta": ("antofagasta",),
    "Atacama": ("atacama",),
    "Coquimbo": ("coquimbo",),
    "Valparaíso": ("valparaiso",),
    "Metropolitana de Santiago": (
        "metropolitana de santiago", "region metropolitana", "metropolitana",
    ),
    "O'Higgins": (
        "libertador general bernardo o'higgins", "libertador bernardo o'higgins",
        "o'higgins", "ohiggins",
    ),
    "Maule": ("maule",),
    "Ñuble": ("nuble",),
    "Biobío": ("biobio", "bio bio", "bio-bio"),
    "La Araucanía": ("la araucania", "araucania"),
    "Los Ríos": ("los rios",),
    "Los Lagos": ("los lagos",),
    "Aysén": ("aysen", "aisen"),
    "Magallanes": ("magallanes y la antartica chilena", "magallanes"),
}

# Alias → canónico, ordenado por longitud de alias (desc) para alternancia regex.
_ALIAS_A_CANON: dict[str, str] = {}
for _canon, _alias in _REGIONES.items():
    for _a in _alias:
        _ALIAS_A_CANON[_a] = _canon
_ALIAS_ORDENADOS = sorted(_ALIAS_A_CANON, key=len, reverse=True)

# Regex de región: alternancia ordenada, con fronteras que no son \w.
_RE_REGION = re.compile(
    r"(?<![\w])(" + "|".join(re.escape(a) for a in _ALIAS_ORDENADOS) + r")(?![\w])"
)

# Monto en formato chileno: exige agrupación de miles con puntos para no
# capturar dígitos de prosa ("2 años", "grado 5"). El "$" es opcional.
_RE_MONTO = re.compile(r"\$?\s?(\d{1,3}(?:\.\d{3})+)")


@dataclass(frozen=True)
class RentaRegion:
    region: str
    sin_bono: int | None = None
    con_bono: int | None = None
    notas: str | None = None

    def as_dict(self) -> dict:
        return {
            "region": self.region,
            "sin_bono": self.sin_bono,
            "con_bono": self.con_bono,
            "notas": self.notas,
        }


def _plegar(texto: str) -> str:
    """Minúsculas sin tildes (conserva apóstrofes para o'higgins)."""
    desc = unicodedata.normalize("NFD", texto)
    sin_tildes = "".join(c for c in desc if unicodedata.category(c) != "Mn")
    return sin_tildes.lower()


def _orden_columnas(plegado: str) -> tuple[str, str | None]:
    """Determina qué columna es 'sin bono' y cuál 'con bono' según el encabezado.

    Retorna (primera, segunda) donde cada una es 'sin' | 'con' | None.
    Por defecto asume (sin, con) — el orden más común en las bases.
    """
    i_sin = plegado.find("sin bono")
    i_con = plegado.find("con bono")
    if i_sin == -1 and i_con == -1:
        return ("sin", "con")
    if i_sin != -1 and i_con != -1:
        return ("con", "sin") if i_con < i_sin else ("sin", "con")
    if i_sin != -1:
        return ("sin", "con")
    return ("con", "sin")  # solo "con bono" en el encabezado


def _a_entero(token: str) -> int:
    return int(token.replace(".", ""))


def _asignar(montos: list[int], orden: tuple[str, str | None]) -> tuple[int | None, int | None]:
    """Mapea hasta 2 montos a (sin_bono, con_bono) según el orden de columnas."""
    sin_b = con_b = None
    if not montos:
        return (None, None)
    primera, segunda = orden
    # Primer monto
    if primera == "sin":
        sin_b = montos[0]
    else:
        con_b = montos[0]
    # Segundo monto (si existe)
    if len(montos) >= 2 and segunda:
        if segunda == "sin":
            sin_b = montos[1]
        else:
            con_b = montos[1]
    return (sin_b, con_b)


def parse_renta_regional(texto: str | None) -> list[RentaRegion] | None:
    """Reconstruye una tabla de renta por región a partir de texto aplanado.

    Devuelve una lista de ``RentaRegion`` o ``None`` si no hay al menos una
    región con un monto asociado (en cuyo caso el caller conserva el crudo).
    """
    if not texto:
        return None

    plegado = _plegar(str(texto))
    orden = _orden_columnas(plegado)

    # Posiciones de cada región mencionada, en orden de aparición.
    regiones: list[tuple[int, int, str]] = []
    for m in _RE_REGION.finditer(plegado):
        regiones.append((m.start(), m.end(), _ALIAS_A_CANON[m.group(1)]))
    if not regiones:
        return None

    # Montos con su posición.
    montos: list[tuple[int, int]] = [
        (m.start(), _a_entero(m.group(1))) for m in _RE_MONTO.finditer(plegado)
    ]
    if not montos:
        return None

    filas: list[RentaRegion] = []
    vistas: set[str] = set()
    for idx, (_ini, fin, canon) in enumerate(regiones):
        # Ventana de esta región: desde su fin hasta el inicio de la siguiente.
        sig_ini = regiones[idx + 1][0] if idx + 1 < len(regiones) else len(plegado)
        en_fila = [val for (pos, val) in montos if fin <= pos < sig_ini]
        if not en_fila:
            continue  # región sin monto en su renglón → se ignora (no se inventa)
        if canon in vistas:
            continue  # evita duplicar la misma región
        vistas.add(canon)
        sin_b, con_b = _asignar(en_fila[:2], orden)
        filas.append(RentaRegion(region=canon, sin_bono=sin_b, con_bono=con_b))

    return filas or None
