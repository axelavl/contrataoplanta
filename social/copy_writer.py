"""Redacción automática del copy por plataforma a partir de una oferta.

Recibe el dict serializado de la oferta (mismas claves que produce
`api.services.seo.serialize_offer`: `cargo`, `institucion`, `region`,
`ciudad`, `renta_bruta_min/max`, `fecha_cierre`, `dias_restantes`, ...) y
devuelve el texto listo para Instagram y para LinkedIn, con enlace UTM y
hashtags. El copy es editable después en la UI de revisión; esto es el
borrador automático.
"""
from __future__ import annotations

import unicodedata
from datetime import date, datetime
from typing import Any
from urllib.parse import urlencode

from . import config as cfg

_MESES = [
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
]


def _norm(texto: str | None) -> str:
    if not texto:
        return ""
    sin = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in sin if not unicodedata.combining(c)).lower().strip()


def _fmt_clp(valor: int | None) -> str | None:
    if not valor:
        return None
    return "$" + f"{int(valor):,}".replace(",", ".")


def formatear_renta(of: dict[str, Any]) -> str | None:
    mn, mx = of.get("renta_bruta_min"), of.get("renta_bruta_max")
    a, b = _fmt_clp(mn), _fmt_clp(mx)
    if a and b and a != b:
        return f"{a} – {b} bruto"
    if a or b:
        return f"{a or b} bruto"
    if of.get("renta_texto"):
        return str(of["renta_texto"])
    if of.get("grado_eus"):
        return f"Grado {of['grado_eus']} EUS"
    return None


def formatear_cierre(of: dict[str, Any]) -> tuple[str | None, bool]:
    """Devuelve (texto, urgente). urgente=True si cierra en <=3 días."""
    dias = of.get("dias_restantes")
    fc = of.get("fecha_cierre")
    if isinstance(fc, str):
        try:
            fc = datetime.fromisoformat(fc).date()
        except ValueError:
            fc = None
    if isinstance(fc, datetime):
        fc = fc.date()
    urgente = isinstance(dias, int) and 0 <= dias <= 3
    if isinstance(dias, int):
        if dias < 0:
            return None, False
        if dias == 0:
            return "hoy", True
        if dias == 1:
            return "mañana", True
    if isinstance(fc, date):
        return f"{fc.day} {_MESES[fc.month - 1]}", urgente
    return None, urgente


def ubicacion(of: dict[str, Any]) -> str | None:
    partes = [of.get("ciudad"), of.get("region")]
    partes = [p for p in partes if p]
    return " · ".join(dict.fromkeys(partes)) if partes else None


def enlace_utm(of: dict[str, Any], plataforma: str) -> str:
    base = f"{cfg.SITE_URL}/oferta/{of.get('id')}" if of.get("id") else cfg.SITE_URL
    params = urlencode({
        "utm_source": plataforma,
        "utm_medium": cfg.UTM_MEDIUM,
        "utm_campaign": cfg.UTM_CAMPAIGN,
    })
    return f"{base}?{params}"


def hashtags(of: dict[str, Any], plataforma: str) -> str:
    tags = list(cfg.HASHTAGS_BASE)
    extra = cfg.HASHTAGS_IG_EXTRA if plataforma == "instagram" else cfg.HASHTAGS_LINKEDIN_EXTRA
    tags += extra
    region_tag = cfg.HASHTAG_REGION.get(_norm(of.get("region")))
    if region_tag:
        tags.append(region_tag)
    # Únicos preservando orden.
    return " ".join(dict.fromkeys(tags))


def _e(clave: str) -> str:
    em = cfg.EMOJI.get(clave, "")
    return f"{em} " if em else ""


def caption_instagram(of: dict[str, Any]) -> str:
    cargo = (of.get("cargo") or "Oferta laboral").strip()
    inst = (of.get("institucion") or "Institución pública").strip()
    ren = formatear_renta(of)
    loc = ubicacion(of)
    cierre, urgente = formatear_cierre(of)

    lineas: list[str] = []
    if urgente and cierre:
        lineas.append(f"{_e('alerta')}CIERRA {cierre.upper()}")
    lineas.append(f"{_e('cargo')}{cargo}")
    lineas.append(f"{_e('institucion')}{inst}")
    if loc:
        lineas.append(f"{_e('ubicacion')}{loc}")
    if ren:
        lineas.append(f"{_e('renta')}{ren}")
    if cierre and not urgente:
        lineas.append(f"{_e('cierre')}Cierra {cierre}")
    lineas.append("")
    lineas.append(f"{_e('cta')}Postula desde el link en la bio o en {cfg.MARCA}.pages.dev")
    lineas.append("")
    lineas.append(hashtags(of, "instagram"))
    return "\n".join(lineas)


def caption_linkedin(of: dict[str, Any]) -> str:
    cargo = (of.get("cargo") or "Oferta laboral").strip()
    inst = (of.get("institucion") or "Institución pública").strip()
    ren = formatear_renta(of)
    loc = ubicacion(of)
    cierre, urgente = formatear_cierre(of)
    link = enlace_utm(of, "linkedin")

    encabezado = f"{inst} busca {cargo}."
    detalle: list[str] = []
    if loc:
        detalle.append(f"Ubicación: {loc}.")
    if ren:
        detalle.append(f"Remuneración: {ren}.")
    if cierre:
        detalle.append(f"Postulaciones {'cierran ' + cierre if not urgente else 'CIERRAN ' + cierre.upper()}.")

    cuerpo = [
        encabezado,
        " ".join(detalle) if detalle else "",
        "",
        f"Revisa los requisitos y postula aquí: {link}",
        "",
        hashtags(of, "linkedin"),
    ]
    return "\n".join([c for c in cuerpo if c is not None]).strip()


def construir_copy(of: dict[str, Any], plataforma: str) -> str:
    if plataforma == "instagram":
        return caption_instagram(of)
    if plataforma == "linkedin":
        return caption_linkedin(of)
    raise ValueError(f"Plataforma no soportada: {plataforma}")
