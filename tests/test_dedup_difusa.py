from __future__ import annotations

import datetime
import unittest

from db.database import clave_dedup_difusa


class DedupDifusaTests(unittest.TestCase):
    def test_misma_oferta_distinto_formato_misma_clave(self):
        # Mismo cargo/institución/fecha/región con tildes, mayúsculas y espacios
        # distintos (como llegaría desde dos portales) → misma clave.
        a = clave_dedup_difusa(
            "Municipalidad de Valparaíso",
            "Analista de Presupuesto",
            datetime.date(2026, 7, 15),
            "Valparaíso",
        )
        b = clave_dedup_difusa(
            "MUNICIPALIDAD DE VALPARAISO",
            "  analista  de presupuesto ",
            "2026-07-15",
            "valparaiso",
        )
        self.assertEqual(a, b)

    def test_cargo_distinto_clave_distinta(self):
        a = clave_dedup_difusa("Min. Salud", "Enfermero", "2026-07-15", "RM")
        b = clave_dedup_difusa("Min. Salud", "Médico", "2026-07-15", "RM")
        self.assertNotEqual(a, b)

    def test_fecha_distinta_clave_distinta(self):
        a = clave_dedup_difusa("Min. Salud", "Enfermero", "2026-07-15", "RM")
        b = clave_dedup_difusa("Min. Salud", "Enfermero", "2026-08-01", "RM")
        self.assertNotEqual(a, b)

    def test_clave_es_estable_y_acotada(self):
        clave = clave_dedup_difusa("X", "Y", "2026-01-01", "Z")
        self.assertIsInstance(clave, str)
        self.assertEqual(len(clave), 40)


if __name__ == "__main__":
    unittest.main()
