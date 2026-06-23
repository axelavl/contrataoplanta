"""
Resumen de ingesta por corrida (Plan Parte 1.10) + detección de nulos (1.7).

Objetivo: que cada pasada de scraping emita un log claro de cuántas ofertas
quedaron normalizadas OK, cuántas con campos faltantes, cuántas con renta o
descripción no parseable, y cuántas se descartaron — sin frenar la corrida si
una fuente cambia de formato.

Funciones puras, sin DB ni red: testeables de forma aislada.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable


# Valores "basura" que las fuentes usan como relleno y que deben tratarse como
# ausentes (Parte 1.7): nunca deben llegar a la UI como dato real.
_VALORES_NULOS = {
    "", "-", "--", "---", "—", "–", "n/a", "na", "n.a.", "s/i", "s/d",
    "sin datos", "sin dato", "sin informacion", "sin información",
    "no informa", "no informado", "no aplica", "null", "none",
}


def es_valor_nulo(v) -> bool:
    """True si el valor es ausente o un relleno típico ('-', 'N/A', 'Sin datos')."""
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in _VALORES_NULOS


def campos_faltantes(oferta: dict, campos: Iterable[str]) -> list[str]:
    """Lista de campos cuyo valor está ausente o es relleno, preservando orden."""
    return [c for c in campos if es_valor_nulo(oferta.get(c))]


@dataclass
class IngestSummary:
    """Acumula el resultado de una corrida de ingesta."""

    ok: int = 0
    con_campos_faltantes: int = 0
    renta_no_parseable: int = 0
    descripcion_no_parseable: int = 0
    descartadas: int = 0
    total: int = 0
    faltantes_por_campo: Counter = field(default_factory=Counter)

    def registrar_oferta(
        self,
        *,
        faltantes: list[str] | None = None,
        renta_no_parseable: bool = False,
        descripcion_no_parseable: bool = False,
    ) -> None:
        """Registra una oferta aceptada (no descartada)."""
        self.total += 1
        faltantes = faltantes or []
        if faltantes:
            self.con_campos_faltantes += 1
            self.faltantes_por_campo.update(faltantes)
        else:
            self.ok += 1
        if renta_no_parseable:
            self.renta_no_parseable += 1
        if descripcion_no_parseable:
            self.descripcion_no_parseable += 1

    def registrar_descarte(self) -> None:
        """Registra una oferta descartada por el intake."""
        self.total += 1
        self.descartadas += 1

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "ok": self.ok,
            "con_campos_faltantes": self.con_campos_faltantes,
            "renta_no_parseable": self.renta_no_parseable,
            "descripcion_no_parseable": self.descripcion_no_parseable,
            "descartadas": self.descartadas,
            "faltantes_por_campo": dict(self.faltantes_por_campo),
        }

    def log_line(self) -> str:
        """Línea de log legible para cerrar la corrida."""
        top = ", ".join(
            f"{campo}={n}" for campo, n in self.faltantes_por_campo.most_common(5)
        )
        return (
            "ingesta_resumen "
            f"total={self.total} ok={self.ok} "
            f"faltantes={self.con_campos_faltantes} "
            f"renta_no_parseable={self.renta_no_parseable} "
            f"descripcion_no_parseable={self.descripcion_no_parseable} "
            f"descartadas={self.descartadas}"
            + (f" | campos_faltantes[{top}]" if top else "")
        )
