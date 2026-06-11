"""Calidad contractual y renta en avisos de empresas del estado.

Las empresas del estado (Codelco, ENAP, Metro, BancoEstado, portuarias, etc.)
operan bajo Código del Trabajo. Sus avisos mencionan "planta" (instalación
física) y "contratación" (verbo), que NO son calidades contractuales. Estos
tests fijan el comportamiento riguroso: solo menciones explícitas cuentan, y
sin mención explícita la categoría de la institución define el régimen.
"""

from __future__ import annotations

import unittest

from scrapers.base import normalizar_tipo_cargo


DESCRIPCION_PLANTA_FISICA = (
    "Buscamos operador de equipos móviles para la planta de chancado de la "
    "división. La empresa contrata personal comprometido con la seguridad. "
    "Proceso de contratación sujeto a examen preocupacional."
)


class NormalizarTipoCargoTests(unittest.TestCase):
    # ── Falsos positivos eliminados ──────────────────────────────────────
    def test_planta_fisica_en_descripcion_larga_no_es_planta(self):
        self.assertIsNone(normalizar_tipo_cargo(DESCRIPCION_PLANTA_FISICA))

    def test_contratacion_no_es_contrata(self):
        texto = (
            "Gran proceso de contratación de profesionales para nuestras "
            "oficinas regionales. Postula con tu CV actualizado en el portal."
        )
        self.assertIsNone(normalizar_tipo_cargo(texto))

    # ── Default por categoría de institución ─────────────────────────────
    def test_empresa_del_estado_sin_mencion_explicita(self):
        self.assertEqual(
            normalizar_tipo_cargo(DESCRIPCION_PLANTA_FISICA, categoria="Empresa del Estado"),
            "codigo_trabajo",
        )

    def test_empresa_ffaa_sin_mencion_explicita(self):
        self.assertEqual(
            normalizar_tipo_cargo("Mecánico estructural para maestranza", categoria="Empresa FFAA"),
            "codigo_trabajo",
        )

    def test_banco_central_sin_mencion_explicita(self):
        self.assertEqual(
            normalizar_tipo_cargo("Analista económico senior", categoria="Banco central"),
            "codigo_trabajo",
        )

    def test_servicio_publico_no_recibe_default(self):
        self.assertIsNone(
            normalizar_tipo_cargo("Profesional de apoyo a la gestión", categoria="Servicio")
        )

    def test_mencion_explicita_gana_al_default(self):
        texto = "Cargo a contrata, grado 10 EUS, calidad jurídica contrata"
        self.assertEqual(
            normalizar_tipo_cargo(texto, categoria="Empresa del Estado"),
            "contrata",
        )

    # ── Menciones explícitas siguen funcionando ──────────────────────────
    def test_etiqueta_corta_contrata(self):
        self.assertEqual(normalizar_tipo_cargo("Contrata"), "contrata")

    def test_etiqueta_corta_planta(self):
        self.assertEqual(normalizar_tipo_cargo("Planta"), "planta")

    def test_calidad_juridica_contrata_en_texto_largo(self):
        texto = (
            "Concurso para proveer el cargo de analista. " * 3
            + "Calidad jurídica: contrata, asimilada a grado 11 EUS."
        )
        self.assertEqual(normalizar_tipo_cargo(texto), "contrata")

    def test_cargo_de_planta_en_texto_largo(self):
        texto = (
            "Llamado a concurso público interno y externo. " * 3
            + "El cargo de planta directiva exige experiencia previa."
        )
        self.assertEqual(normalizar_tipo_cargo(texto), "planta")

    def test_codigo_del_trabajo_explicito(self):
        self.assertEqual(
            normalizar_tipo_cargo("Contrato indefinido regido por el Código del Trabajo"),
            "codigo_trabajo",
        )

    def test_idempotencia_slug(self):
        for slug in ("planta", "contrata", "honorarios", "reemplazo", "codigo_trabajo"):
            self.assertEqual(normalizar_tipo_cargo(slug), slug)

    def test_none_y_vacio(self):
        self.assertIsNone(normalizar_tipo_cargo(None))
        self.assertIsNone(normalizar_tipo_cargo(""))
        self.assertEqual(
            normalizar_tipo_cargo(None, categoria="Empresa del Estado"),
            "codigo_trabajo",
        )


class TrabajandoHelpersTests(unittest.TestCase):
    """Heurísticas del scraper de trabajando.cl (ENAP, Metro, BancoEstado...)."""

    @classmethod
    def setUpClass(cls):
        from scrapers.trabajando import _detectar_tipo_cargo, _extraer_renta

        cls.detectar = staticmethod(_detectar_tipo_cargo)
        cls.extraer = staticmethod(_extraer_renta)

    def test_planta_fisica_no_marca_planta(self):
        self.assertIsNone(self.detectar(
            "Técnico de mantenimiento para planta de revisión técnica, "
            "turnos rotativos"
        ))

    def test_contratacion_no_marca_contrata(self):
        self.assertIsNone(self.detectar(
            "Metro S.A. inicia proceso de contratación de conductores"
        ))

    def test_calidad_explicita_si_marca(self):
        self.assertEqual(self.detectar("Vínculo: a contrata por el año 2026"), "Contrata")

    def test_renta_bruta(self):
        bruta, texto = self.extraer("Renta bruta mensual de $1.450.000 según experiencia")
        self.assertEqual(bruta, 1_450_000)
        self.assertIsNone(texto)

    def test_renta_liquida_no_se_reporta_como_bruta(self):
        bruta, texto = self.extraer("Renta líquida: $950.000")
        self.assertIsNone(bruta)
        self.assertIn("950.000", texto)

    def test_renta_sin_calificativo_se_asume_bruta(self):
        bruta, _ = self.extraer("Se ofrece renta de $1.200.000 y beneficios")
        self.assertEqual(bruta, 1_200_000)

    def test_monto_fuera_de_rango_se_descarta(self):
        bruta, texto = self.extraer("Renta: $50.000.000 anuales del proyecto")
        self.assertIsNone(bruta)
        self.assertIsNone(texto)

    def test_sin_renta(self):
        self.assertEqual(self.extraer("Sin información de remuneración"), (None, None))


if __name__ == "__main__":
    unittest.main()
