"""Los scrapers que conocen el correo de postulación deben poblar el CAMPO
dedicado email_postulacion (que la ficha muestra), no sólo el texto.
"""
from datetime import date

import scrapers.divsal as D
import scrapers.famae as F


def test_famae_puebla_email_postulacion():
    o = F.construir_oferta({"cargo": "Analista", "url": "https://famae.cl/empleo/analista"},
                           {"cargo": "Analista", "descripcion": "x"})
    assert o["email_postulacion"] == F.EMAIL_POSTULACION


def test_divsal_puebla_email_postulacion_desde_correo():
    campos = {"correo": "postulaciones@divsal.cl", "tipo": "Honorarios"}
    o = D.construir_oferta("Médico General - Santiago", "https://x/1", campos,
                           date(2026, 7, 1))
    assert o["email_postulacion"] == "postulaciones@divsal.cl"


def test_divsal_email_none_si_no_hay_correo():
    o = D.construir_oferta("Médico General - Santiago", "https://x/2",
                           {"tipo": "Honorarios"}, date(2026, 7, 1))
    assert o["email_postulacion"] is None
