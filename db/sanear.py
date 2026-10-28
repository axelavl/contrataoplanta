"""Saneo de una oferta contra las reglas CHECK de la tabla `ofertas`.

Por qué existe
--------------
`db/schema.sql` define CHECKs estrictos (tipo_cargo en minúsculas
canónicas, renta entre 300.000 y 20.000.000, cierre >= publicación, horas
1–88, fechas en rango). Los scrapers mandan valores como "Código del
Trabajo (Plazo Fijo)" o rentas mal parseadas, y Postgres rechazaba la
oferta COMPLETA por un campo opcional: en oct-2026 Codelco encontró 10
ofertas y no guardó ninguna.

Regla: un campo opcional inválido se deja en NULL (o se canoniza); la
oferta se guarda igual. Solo se tocan claves presentes en el dict.
"""
from __future__ import annotations

import logging
import unicodedata
from datetime import date, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger("db.sanear")

TIPOS_CARGO = ("planta", "contrata", "honorarios", "reemplazo", "codigo_trabajo")
RENTA_MIN, RENTA_MAX = 300_000, 20_000_000
HORAS_MIN, HORAS_MAX = 1, 88
FECHA_MIN = date(2020, 1, 1)


def _fold(texto: Any) -> str:
    s = unicodedata.normalize("NFD", str(texto or "").lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def canonizar_tipo_cargo(valor: Any) -> str | None:
    """Texto libre del tipo de vínculo → valor canónico del CHECK, o None."""
    t = _fold(valor).strip()
    if not t:
        return None
    if t in TIPOS_CARGO:
        return t
    if "honorario" in t:
        return "honorarios"
    if "reemplazo" in t or "suplencia" in t:
        return "reemplazo"
    if "contrata" in t:
        return "contrata"
    if "planta" in t:
        return "planta"
    if ("codigo del trabajo" in t or "codigo_trabajo" in t or "plazo fijo" in t
            or "indefinido" in t or "contrato de trabajo" in t):
        return "codigo_trabajo"
    return None


def hoy_chile() -> date:
    """Fecha actual en Chile. El servidor (Railway) corre en UTC: entre las
    21:00 y las 24:00 de Chile, `date.today()` ya es el día siguiente."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Santiago")).date()
    except Exception:  # sin base de zonas horarias: UTC-3 (horario de verano)
        return datetime.now(timezone(timedelta(hours=-3))).date()


def _a_fecha(valor: Any) -> date | None:
    if valor is None or valor == "":
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(str(valor)[:10])
    except ValueError:
        return None


def _a_entero(valor: Any) -> int | None:
    if valor is None or valor == "":
        return None
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


def sanear_oferta(datos: dict[str, Any], *, hoy: date | None = None) -> dict[str, Any]:
    """Copia de `datos` que cumple los CHECK de `ofertas`."""
    d = dict(datos)
    hoy_utc = datetime.now(timezone.utc).date()
    hoy = hoy or hoy_chile()
    corregidos: list[str] = []

    if "tipo_cargo" in d and d["tipo_cargo"] is not None:
        canon = canonizar_tipo_cargo(d["tipo_cargo"])
        if canon != d["tipo_cargo"]:
            corregidos.append("tipo_cargo")
        d["tipo_cargo"] = canon

    for campo in ("renta_bruta_min", "renta_bruta_max"):
        if campo in d and d[campo] is not None:
            v = _a_entero(d[campo])
            if v is None or not (RENTA_MIN <= v <= RENTA_MAX):
                d[campo] = None
                corregidos.append(campo)
    if d.get("renta_bruta_min") is not None and d.get("renta_bruta_max") is not None \
            and d["renta_bruta_min"] > d["renta_bruta_max"]:
        d["renta_bruta_min"], d["renta_bruta_max"] = d["renta_bruta_max"], d["renta_bruta_min"]
        corregidos.append("renta_orden")

    if "horas_semanales" in d and d["horas_semanales"] is not None:
        v = _a_entero(d["horas_semanales"])
        if v is None or not (HORAS_MIN <= v <= HORAS_MAX):
            d["horas_semanales"] = None
            corregidos.append("horas_semanales")

    # Muchos scrapers usan `date.today()` como fecha de publicación cuando la
    # fuente no la informa. En un servidor UTC, de 21:00 a 24:00 de Chile eso
    # da "mañana" y el sitio la trata como oferta próxima (la oculta de las
    # vigentes). Se corrige SOLO esa huella exacta: una fecha futura legítima
    # (inicio de postulación de Empleos Públicos) no coincide con hoy UTC.
    pub_raw = _a_fecha(d.get("fecha_publicacion"))
    if pub_raw and pub_raw == hoy_utc and pub_raw > hoy:
        d["fecha_publicacion"] = hoy
        corregidos.append("fecha_publicacion_utc")

    limites = {"fecha_publicacion": hoy + timedelta(days=365),
               "fecha_cierre": hoy + timedelta(days=3 * 365)}
    for campo, tope in limites.items():
        if campo in d and d[campo] not in (None, ""):
            f = _a_fecha(d[campo])
            if f is None or not (FECHA_MIN <= f <= tope):
                d[campo] = None
                corregidos.append(campo)
    pub, cierre = _a_fecha(d.get("fecha_publicacion")), _a_fecha(d.get("fecha_cierre"))
    if pub and cierre and cierre < pub:
        # La fecha de cierre es la que importa al postulante: se descarta la
        # de publicación, que suele ser la mal parseada.
        d["fecha_publicacion"] = None
        corregidos.append("fecha_publicacion>cierre")

    if corregidos:
        logger.debug("sanear_oferta %s: %s", d.get("url_oferta") or d.get("url_original"),
                     ", ".join(corregidos))
    return d
