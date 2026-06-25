"""
Resend — Alertas de empleo por email
Envía notificaciones automáticas cuando aparecen ofertas que
coinciden con el perfil del usuario suscrito.
"""

from __future__ import annotations

import html as _html_mod
import logging
import os
from typing import Any


def _esc(s: object) -> str:
    return _html_mod.escape(str(s)) if s else ""

logger = logging.getLogger("api.email_alerts")

_RESEND_API_KEY = os.getenv("RESEND_API_KEY", os.getenv("EMAIL_API_KEY", ""))
_EMAIL_FROM = os.getenv("EMAIL_FROM", "alertas@contrataoplanta.cl")
_SITE_URL = os.getenv("SITE_URL", "https://contrataoplanta.cl")
# Destinatario de alertas OPERACIONALES (caídas de scraping). Cae a EMAIL_FROM.
_ALERT_EMAIL = os.getenv("ALERT_EMAIL", os.getenv("ADMIN_EMAIL", ""))


def _get_resend():
    """Lazy import to avoid startup crash if key is missing."""
    import resend
    resend.api_key = _RESEND_API_KEY
    return resend


def enviar_alerta_operacional(
    asunto: str,
    mensaje: str,
    destino: str | None = None,
) -> dict[str, Any]:
    """Alerta OPERACIONAL al administrador (no a usuarios suscritos).

    Avisa caídas de scraping —p.ej. el portal central devolviendo 0 ofertas—.
    Destinatario: ``destino`` → env ``ALERT_EMAIL``/``ADMIN_EMAIL`` → ``EMAIL_FROM``.
    No-op silencioso si falta ``RESEND_API_KEY`` o no hay destinatario.
    """
    destinatario = destino or _ALERT_EMAIL or _EMAIL_FROM
    if not _RESEND_API_KEY:
        logger.warning(
            "RESEND_API_KEY no configurada; alerta operacional NO enviada: %s", asunto
        )
        return {"ok": False, "error": "API key no configurada"}
    if not destinatario:
        logger.warning("Sin destinatario para alerta operacional: %s", asunto)
        return {"ok": False, "error": "sin destinatario"}

    cuerpo = _esc(mensaje or "").replace("\n", "<br>")
    html = (
        '<div style="font-family:system-ui,Arial,sans-serif;max-width:560px;'
        'margin:auto;padding:16px;color:#111">'
        f'<h2 style="color:#B91C1C;margin:0 0 8px">⚠ {_esc(asunto)}</h2>'
        f'<div style="font-size:14px;line-height:1.5">{cuerpo}</div>'
        '<p style="margin-top:16px;font-size:11px;color:#9CA3AF">'
        'Alerta automática del orquestador de scrapers (contrata o planta).</p>'
        "</div>"
    )
    try:
        resend = _get_resend()
        result = resend.Emails.send({
            "from": f"contrata o planta <{_EMAIL_FROM}>",
            "to": [destinatario],
            "subject": f"[scrapers] {asunto}",
            "html": html,
        })
        logger.info(
            "Alerta operacional enviada a %s (id=%s): %s",
            destinatario, result.get("id"), asunto,
        )
        return {"ok": True, "id": result.get("id")}
    except Exception as exc:
        logger.error("Error enviando alerta operacional a %s: %s", destinatario, exc)
        return {"ok": False, "error": str(exc)}


def enviar_alerta_ofertas(
    email: str,
    ofertas: list[dict[str, Any]],
    filtros: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Send an email alert with matching job offers to a subscriber.
    Returns {'ok': True, 'id': <resend_id>} on success.
    """
    if not _RESEND_API_KEY:
        logger.warning("RESEND_API_KEY no configurada, email no enviado a %s", email)
        return {"ok": False, "error": "API key no configurada"}

    filtros = filtros or {}
    n = len(ofertas)
    subject = f"🔔 {n} nueva{'s' if n != 1 else ''} oferta{'s' if n != 1 else ''} en contrataoplanta.cl"

    # Build HTML email
    ofertas_html = ""
    for o in ofertas[:10]:  # Max 10 per email
        renta = ""
        if o.get("renta_bruta_min"):
            renta = f"${o['renta_bruta_min']:,.0f}".replace(",", ".")
            if o.get("renta_bruta_max") and o["renta_bruta_max"] != o["renta_bruta_min"]:
                renta += f" – ${o['renta_bruta_max']:,.0f}".replace(",", ".")
        elif o.get("renta_texto"):
            renta = o["renta_texto"]

        region = _esc(o.get("region", ""))
        institucion = _esc(o.get("institucion") or o.get("institucion_nombre") or "—")
        tipo = _esc(o.get("tipo_contrato") or o.get("tipo_cargo") or "")
        cargo = _esc(o.get("cargo", "Sin cargo"))
        url = _esc(o.get("url_oferta") or o.get("url_original") or "")
        renta = _esc(renta) if renta else ""

        ofertas_html += f"""
        <tr style="border-bottom:1px solid #eee">
          <td style="padding:12px 8px;vertical-align:top">
            <div style="font-weight:600;color:#0A2E6E;font-size:14px;margin-bottom:4px">{cargo}</div>
            <div style="font-size:12px;color:#4B5563">{institucion}</div>
            <div style="font-size:11px;color:#9CA3AF;margin-top:2px">
              {region}{' · ' + tipo if tipo else ''}{' · ' + renta if renta else ''}
            </div>
          </td>
          <td style="padding:12px 8px;vertical-align:middle;text-align:right">
            <a href="{url}" target="_blank"
               style="display:inline-block;padding:6px 14px;background:#1557C0;color:white;
                      border-radius:6px;text-decoration:none;font-size:12px;font-weight:600">
              Ver oferta →
            </a>
          </td>
        </tr>
        """

    filtros_text = ""
    if filtros:
        parts = []
        if filtros.get("region"):
            parts.append(f"Región: {_esc(filtros['region'])}")
        if filtros.get("termino"):
            parts.append(f"Palabras clave: {_esc(filtros['termino'])}")
        if filtros.get("tipo_contrato"):
            parts.append(f"Tipo: {_esc(filtros['tipo_contrato'])}")
        if filtros.get("sector"):
            parts.append(f"Sector: {_esc(filtros['sector'])}")
        if parts:
            filtros_text = " · ".join(parts)

    extra_text = ""
    if n > 10:
        extra_text = f"""
        <p style="text-align:center;padding:16px;color:#4B5563;font-size:13px">
          ... y {n - 10} oferta{'s' if n - 10 != 1 else ''} más.
          <a href="{_SITE_URL}" style="color:#1557C0">Ver todas en contrataoplanta.cl →</a>
        </p>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;font-family:'Inter',Helvetica,Arial,sans-serif;background:#F5F4EF">
      <div style="max-width:600px;margin:0 auto;padding:24px">
        <div style="text-align:center;margin-bottom:24px">
          <span style="font-family:Georgia,serif;font-size:20px;color:#0A2E6E">
            contrata <em style="color:#E8A820">o</em> planta
            <span style="color:#9CA3AF">.cl</span>
          </span>
        </div>

        <div style="background:white;border-radius:12px;border:1px solid #D9D7CF;overflow:hidden">
          <div style="background:#0A2E6E;padding:20px 24px;text-align:center">
            <div style="color:white;font-size:18px;font-weight:600">
              {n} nueva{'s' if n != 1 else ''} oferta{'s' if n != 1 else ''} para ti
            </div>
            {f'<div style="color:rgba(255,255,255,0.6);font-size:12px;margin-top:6px">{filtros_text}</div>' if filtros_text else ''}
          </div>

          <table style="width:100%;border-collapse:collapse">
            {ofertas_html}
          </table>

          {extra_text}
        </div>

        <div style="text-align:center;padding:24px 0;font-size:11px;color:#9CA3AF">
          <p>Recibes este email porque te suscribiste a alertas en contrataoplanta.cl</p>
          <p style="margin-top:8px">
            <a href="{_SITE_URL}" style="color:#1557C0;text-decoration:none">Ver todas las ofertas</a>
          </p>
        </div>
      </div>
    </body>
    </html>
    """

    try:
        resend = _get_resend()
        result = resend.Emails.send({
            "from": f"contrata o planta <{_EMAIL_FROM}>",
            "to": [email],
            "subject": subject,
            "html": html,
        })
        logger.info("Alerta enviada a %s: %d ofertas (id=%s)", email, n, result.get("id"))
        return {"ok": True, "id": result.get("id")}
    except Exception as exc:
        logger.error("Error enviando alerta a %s: %s", email, exc)
        return {"ok": False, "error": str(exc)}


def enviar_verificacion(email: str, token: str) -> dict[str, Any]:
    """Send email verification link."""
    if not _RESEND_API_KEY:
        return {"ok": False, "error": "API key no configurada"}

    from urllib.parse import quote
    verify_url = f"{_SITE_URL}?verificar={quote(token, safe='')}"

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;font-family:'Inter',Helvetica,Arial,sans-serif;background:#F5F4EF">
      <div style="max-width:500px;margin:0 auto;padding:24px">
        <div style="text-align:center;margin-bottom:24px">
          <span style="font-family:Georgia,serif;font-size:20px;color:#0A2E6E">
            contrata <em style="color:#E8A820">o</em> planta <span style="color:#9CA3AF">.cl</span>
          </span>
        </div>
        <div style="background:white;border-radius:12px;border:1px solid #D9D7CF;padding:32px;text-align:center">
          <h2 style="color:#0A2E6E;margin-bottom:12px">Confirma tu suscripción</h2>
          <p style="color:#4B5563;font-size:14px;line-height:1.6;margin-bottom:24px">
            Haz clic en el botón para activar tus alertas de empleo público.
          </p>
          <a href="{verify_url}"
             style="display:inline-block;padding:12px 32px;background:#1557C0;color:white;
                    border-radius:8px;text-decoration:none;font-weight:600;font-size:14px">
            Confirmar email
          </a>
        </div>
      </div>
    </body>
    </html>
    """

    try:
        resend = _get_resend()
        result = resend.Emails.send({
            "from": f"contrata o planta <{_EMAIL_FROM}>",
            "to": [email],
            "subject": "Confirma tu suscripción — contrataoplanta.cl",
            "html": html,
        })
        return {"ok": True, "id": result.get("id")}
    except Exception as exc:
        logger.error("Error enviando verificación a %s: %s", email, exc)
        return {"ok": False, "error": str(exc)}
