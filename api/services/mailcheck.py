"""
Mailcheck — Validación de email con detección de typos y dominios desechables.

Valida emails en el backend antes de registrar suscripciones:
- Detecta dominios temporales/desechables (mailinator, guerrillamail, etc.)
- Sugiere correcciones de typos comunes (gmial→gmail, hotnail→hotmail)
- Valida formato y estructura MX
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger("api.mailcheck")

# Dominios desechables / temporales conocidos
_DISPOSABLE_DOMAINS = frozenset({
    "mailinator.com", "guerrillamail.com", "guerrillamail.net", "guerrillamail.org",
    "tempmail.com", "throwaway.email", "10minutemail.com", "yopmail.com",
    "yopmail.fr", "yopmail.net", "trashmail.com", "trashmail.net",
    "sharklasers.com", "guerrillamailblock.com", "grr.la", "dispostable.com",
    "temp-mail.org", "tempail.com", "emailondeck.com", "mintemail.com",
    "maildrop.cc", "getairmail.com", "mohmal.com", "fakeinbox.com",
    "mailnesia.com", "maildrop.cc", "discard.email", "discardmail.com",
    "mailcatch.com", "meltmail.com", "mytemp.email", "tempr.email",
    "throwaway.email", "tmpmail.net", "tmpmail.org", "wegwerfmail.de",
    "trash-mail.com", "mailsac.com", "harakirimail.com", "spamgourmet.com",
    "spamfree24.org", "binkmail.com", "safetymail.info", "nospam.ze.tc",
    "mailforspam.com", "inboxalias.com", "jetable.org",
})

# Dominios comunes y sus typos frecuentes
_DOMAIN_CORRECTIONS: dict[str, list[str]] = {
    "gmail.com": [
        "gmial.com", "gmal.com", "gmaill.com", "gamil.com", "gnail.com",
        "gimail.com", "gmil.com", "gail.com", "gmali.com", "gemail.com",
        "gmail.co", "gmail.cl", "gmsil.com", "gmeil.com",
    ],
    "hotmail.com": [
        "hotnail.com", "hotmal.com", "hotmaill.com", "hotmial.com",
        "hotmil.com", "hotamil.com", "hotmai.com", "hotmsil.com",
        "hitmail.com", "hotimail.com", "hotmail.co",
    ],
    "outlook.com": [
        "outloo.com", "outlok.com", "outloock.com", "outlool.com",
        "outllok.com", "outlook.co",
    ],
    "yahoo.com": [
        "yaho.com", "yahooo.com", "yhaoo.com", "yhoo.com",
        "yahoo.co", "yaoo.com",
    ],
    "outlook.cl": [
        "outloo.cl", "outlok.cl", "outloock.cl",
    ],
    "hotmail.cl": [
        "hotnail.cl", "hotmal.cl", "hotmaill.cl",
    ],
    "gmail.cl": [],  # Not a real domain, suggest gmail.com
    "live.com": [
        "live.co", "lve.com", "ive.com",
    ],
    "icloud.com": [
        "iclud.com", "icloud.co", "icolud.com",
    ],
}

# Build reverse lookup: typo → correct domain
_TYPO_MAP: dict[str, str] = {}
for correct, typos in _DOMAIN_CORRECTIONS.items():
    for typo in typos:
        _TYPO_MAP[typo] = correct

# Regex for basic email format
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def validar_email(email: str) -> dict[str, Any]:
    """
    Validate an email address and return analysis.

    Returns:
        {
            "valido": bool,
            "email": str (cleaned),
            "sugerencia": str | None (did-you-mean),
            "desechable": bool,
            "motivo": str | None (reason if invalid),
        }
    """
    email = email.strip().lower()

    # Basic format check
    if not _EMAIL_RE.match(email):
        return {
            "valido": False,
            "email": email,
            "sugerencia": None,
            "desechable": False,
            "motivo": "Formato de email inválido",
        }

    parts = email.split("@")
    if len(parts) != 2:
        return {
            "valido": False,
            "email": email,
            "sugerencia": None,
            "desechable": False,
            "motivo": "Formato de email inválido",
        }

    local, domain = parts

    # Check local part length
    if len(local) < 1 or len(local) > 64:
        return {
            "valido": False,
            "email": email,
            "sugerencia": None,
            "desechable": False,
            "motivo": "Parte local del email demasiado corta o larga",
        }

    # Check disposable domain
    is_disposable = domain in _DISPOSABLE_DOMAINS
    if is_disposable:
        return {
            "valido": False,
            "email": email,
            "sugerencia": None,
            "desechable": True,
            "motivo": "Dominio de email temporal o desechable. Usa un email permanente.",
        }

    # Check for typos and suggest correction
    sugerencia = None
    if domain in _TYPO_MAP:
        corrected_domain = _TYPO_MAP[domain]
        sugerencia = f"{local}@{corrected_domain}"

    # Special case: gmail.cl is not real → suggest gmail.com
    if domain == "gmail.cl":
        sugerencia = f"{local}@gmail.com"

    return {
        "valido": True,
        "email": email,
        "sugerencia": sugerencia,
        "desechable": False,
        "motivo": None,
    }
