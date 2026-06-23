from __future__ import annotations

import unittest

from extraction.renta_regional import RentaRegion, parse_renta_regional


class RentaRegionalTests(unittest.TestCase):
    def test_ejemplo_del_plan_una_region(self):
        texto = (
            "Renta bruta mes sin bono   Renta bruta mes con bono\n"
            "Metropolitana   $818.341   $1.396.510"
        )
        filas = parse_renta_regional(texto)
        self.assertEqual(
            filas,
            [RentaRegion("Metropolitana de Santiago", 818341, 1396510)],
        )

    def test_multiples_regiones_en_orden(self):
        texto = (
            "Renta bruta mes sin bono / con bono\n"
            "Metropolitana $818.341 $1.396.510\n"
            "Valparaíso $750.000 $1.200.000\n"
            "Biobío $700.000 $1.100.000"
        )
        filas = parse_renta_regional(texto)
        self.assertEqual([f.region for f in filas],
                         ["Metropolitana de Santiago", "Valparaíso", "Biobío"])
        self.assertEqual(filas[1].sin_bono, 750000)
        self.assertEqual(filas[2].con_bono, 1100000)

    def test_respeta_orden_de_columnas_del_encabezado(self):
        # Encabezado declara primero "con bono", luego "sin bono".
        texto = "Renta mes con bono   Renta mes sin bono\nCoquimbo $900.000 $800.000"
        filas = parse_renta_regional(texto)
        self.assertEqual(filas, [RentaRegion("Coquimbo", sin_bono=800000, con_bono=900000)])

    def test_un_solo_monto_va_a_sin_bono(self):
        texto = "Renta bruta sin bono\nAtacama $850.000"
        filas = parse_renta_regional(texto)
        self.assertEqual(filas, [RentaRegion("Atacama", sin_bono=850000, con_bono=None)])

    def test_sin_region_devuelve_none(self):
        self.assertIsNone(parse_renta_regional("Renta bruta mensual: $1.450.000."))

    def test_prosa_sin_tabla_devuelve_none(self):
        # Menciona una región pero sin monto en su renglón → no se inventa fila.
        texto = "Se valorará experiencia de 5 años en la Región de Valparaíso."
        self.assertIsNone(parse_renta_regional(texto))

    def test_texto_vacio_o_none(self):
        self.assertIsNone(parse_renta_regional(""))
        self.assertIsNone(parse_renta_regional(None))

    def test_no_duplica_misma_region(self):
        texto = (
            "Renta sin bono / con bono\n"
            "Maule $700.000 $1.000.000\n"
            "Maule $700.000 $1.000.000"
        )
        filas = parse_renta_regional(texto)
        self.assertEqual(len(filas), 1)


if __name__ == "__main__":
    unittest.main()
