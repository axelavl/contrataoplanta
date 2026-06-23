from __future__ import annotations

import unittest

from scrapers.ingest_summary import (
    IngestSummary,
    campos_faltantes,
    es_valor_nulo,
)


class ValorNuloTests(unittest.TestCase):
    def test_reconoce_rellenos_como_nulos(self):
        for v in [None, "", "  ", "-", "N/A", "n/a", "Sin datos", "—", "No informa", "null"]:
            self.assertTrue(es_valor_nulo(v), f"esperaba nulo: {v!r}")

    def test_valores_reales_no_son_nulos(self):
        for v in ["Planta", "$1.000.000", "Valparaíso", 0, "0"]:
            self.assertFalse(es_valor_nulo(v), f"no debía ser nulo: {v!r}")

    def test_campos_faltantes_preserva_orden(self):
        oferta = {"cargo": "Analista", "renta": "-", "region": None, "jornada": "Completa"}
        self.assertEqual(
            campos_faltantes(oferta, ["cargo", "renta", "region", "jornada"]),
            ["renta", "region"],
        )


class IngestSummaryTests(unittest.TestCase):
    def test_cuenta_ok_y_faltantes(self):
        s = IngestSummary()
        s.registrar_oferta(faltantes=[])
        s.registrar_oferta(faltantes=["renta"])
        s.registrar_oferta(faltantes=["renta", "region"], renta_no_parseable=True)
        s.registrar_descarte()

        d = s.as_dict()
        self.assertEqual(d["total"], 4)
        self.assertEqual(d["ok"], 1)
        self.assertEqual(d["con_campos_faltantes"], 2)
        self.assertEqual(d["renta_no_parseable"], 1)
        self.assertEqual(d["descartadas"], 1)
        self.assertEqual(d["faltantes_por_campo"]["renta"], 2)
        self.assertEqual(d["faltantes_por_campo"]["region"], 1)

    def test_log_line_incluye_metricas(self):
        s = IngestSummary()
        s.registrar_oferta(faltantes=["renta"])
        linea = s.log_line()
        self.assertIn("ingesta_resumen", linea)
        self.assertIn("total=1", linea)
        self.assertIn("faltantes=1", linea)
        self.assertIn("renta=1", linea)


if __name__ == "__main__":
    unittest.main()
