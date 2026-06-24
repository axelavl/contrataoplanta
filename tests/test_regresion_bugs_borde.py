"""Tests de regresión para bugs de borde encontrados en la auditoría.

Cada caso aquí corresponde a un bug de corrección real que estaba vivo y se
corrigió. El objetivo es que no reaparezcan: son los caminos de borde que la
suite original no cubría (de ahí que los bugs pasaran desapercibidos).

Cubre:
  1. salary_extractor: formato chileno con decimales (',NN') producía errores
     de magnitud ~100x o descartaba rentas válidas.
  2. intake.fecha_cierre_desde_texto: una frase de cierre sin año cuyo mes ya
     pasó se fechaba en el pasado, descartando ofertas vigentes.
"""
from __future__ import annotations

import unittest
from datetime import date

from extraction.salary_extractor import extract_salary
from scrapers.intake import fecha_cierre_desde_texto


class SalaryDecimalesTests(unittest.TestCase):
    """El separador decimal ',' no debe pegarse a los miles.

    Antes: re.sub(r'[^\\d]', '', raw) convertía '$1.800.000,00' en '180000000'
    (descartado por el tope de 15MM) y '$3.500,00' en '350000' (100x).
    """

    def test_monto_con_decimales_no_se_infla(self):
        # $3.500,00 → 3500 (bajo el umbral de 250k → no se reporta monto),
        # NO 350000.
        r = extract_salary("honorarios mensuales $3.500,00")
        self.assertIsNone(r.amount)

    def test_monto_alto_con_decimales_se_conserva(self):
        # El caso más dañino: una renta legítima de 1.8M con ',00' se
        # descartaba por completo (180000000 ≥ 15MM). Debe extraerse 1.8M.
        r = extract_salary("renta bruta mensual $1.800.000,00")
        self.assertEqual(r.amount, 1_800_000.0)
        self.assertEqual(r.currency, "CLP")

    def test_monto_con_separador_de_miles_intacto(self):
        # Regresión inversa: el formato sin decimales debe seguir funcionando.
        r = extract_salary("renta bruta mensual $1.500.000")
        self.assertEqual(r.amount, 1_500_000.0)

    def test_decimales_no_convierten_monto_valido_en_inflado(self):
        # $12.345,50 daba 1234550 (≈1.2M) y pasaba como válido. Ahora 12345,
        # que está bajo el umbral → sin monto reportado.
        r = extract_salary("renta bruta mensual $12.345,50")
        self.assertIsNone(r.amount)


class FechaCierreSinAnioTests(unittest.TestCase):
    """Una fecha de cierre es futura: si la frase no trae año y el mes ya pasó,
    debe asumirse el año siguiente, no el actual."""

    def test_cierre_enero_publicado_en_diciembre(self):
        # Aviso de dic-2025: "postular hasta el 15 de enero" → enero 2026.
        # Antes devolvía 2025-01-15 (11 meses en el pasado) → oferta descartada.
        hoy = date(2025, 12, 23)
        f = fecha_cierre_desde_texto("postular hasta el 15 de enero.", today=hoy)
        self.assertEqual(f, date(2026, 1, 15))

    def test_cierre_mismo_anio_futuro_intacto(self):
        # Mes posterior al actual: se mantiene el año en curso.
        hoy = date(2026, 6, 23)
        f = fecha_cierre_desde_texto("postular hasta el 10 de julio.", today=hoy)
        self.assertEqual(f, date(2026, 7, 10))

    def test_cierre_con_anio_explicito_no_se_altera(self):
        # Si la frase trae año explícito, se respeta tal cual.
        hoy = date(2026, 6, 23)
        f = fecha_cierre_desde_texto(
            "el proceso durará hasta el 19 de marzo del 2026.", today=hoy
        )
        self.assertEqual(f, date(2026, 3, 19))

    def test_cierre_fin_de_mes_actual_intacto(self):
        # Borde: cierre dentro del mes en curso, fecha futura cercana.
        hoy = date(2026, 6, 23)
        f = fecha_cierre_desde_texto(
            "recepcion de antecedentes hasta el 30 de junio.", today=hoy
        )
        self.assertEqual(f, date(2026, 6, 30))


if __name__ == "__main__":
    unittest.main()
