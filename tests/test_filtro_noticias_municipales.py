"""Tests de regresión: noticias municipales que se colaban como ofertas.

Casos reales detectados en producción (junio 2026): los sitios WordPress de
Loncoche, Galvarino, Cunco, Tirúa y Santa Juana publicaban noticias de gestión
(actividades del alcalde, reuniones, subvenciones, elecciones) que el filtro
`_parece_oferta` aceptaba por keywords de rol demasiado amplias ("director",
"encargado", "cargo" como substring de "descargos", etc.).

Cubre dos capas:
1. `WordPressScraper._parece_oferta` — gate del scraper.
2. `scrapers.intake` (`titulo_es_noticia` + `intake_validate_offer`) — capa
   transversal usada por todos los scrapers y por
   `scripts/limpiar_ofertas_no_laborales.py` para dar de baja filas ya
   persistidas.
"""

from __future__ import annotations

import unittest

from scrapers.intake import intake_validate_offer, titulo_es_noticia
from scrapers.plataformas.wordpress import WordPressScraper

# Titulares reales que NO son ofertas y deben rechazarse.
TITULOS_NOTICIA = (
    "Alcalde y Encargado de Turismo participan en Asamblea Nacional de Comunas Mágicas en Futrono",
    "Alcalde, Secplan y Didel Se Reúne con la Directora Regional de Junji en el Marco del Proyecto Jardín Corazón Contento",
    "Reunión con Representantes de los Asistentes de la Educación",
    "Nuevos Formatos para la Renovación de Directorios",
    "Postulacion Primer Concurso de Subvenciones 2025",
    "Descargos Elecciones",
    "Alcalde José Linco y Director (s) SS Arauco revisaron avances de implementación de Base SAMU en Tirúa",
    "Departamento de tránsito te ayuda: descarga el nuevo manual del conductor 2025",
    "Alcalde José Linco encabezó entrega de terreno y presentación de empresa a cargo de obra de reconstrucción de Posta Loncotripay",
    "Inspectoría de Vialidad ya es una realidad en Tirúa: ¡Juntos trabajamos por mejor conectividad y conservación de caminos!",
)

# Titulares reales de concursos legítimos que deben seguir pasando.
TITULOS_OFERTA = (
    "Concurso Público para Director (a) Cesfam Huiscapi.",
    'Bases de concurso Publico para el cargo de "APOYO FAMILIAR INTEGRAL"',
    "Concurso público para proveer el cargo de 4 profesionales categoría B, del Departamento de Salud de Cunco",
    "Concurso Publico para cargo a plazo fijo de Coordinador (a) de Equipo de Salud Rural",
    "Concurso interno ley 21308 departamento de Salud Los Álamos",
    "La Ilustre Municipalidad de Los Álamos en conjunto con el Programa SENDA llaman a Concurso Público",
    "Presentacion de Antecedentes Cargo COORDINADOR/A para el Dispositivo Centro de la Mujer de Santa Juana",
    "Concurso Público para Proveer Cargo de Técnico, Administrativo y Auxiliar.",
    "Bases Concurso Público para Proveer Cargo Grado 18 Escalafón Auxiliar",
    "Concurso Publico Encargado Red Local",
    "Llamado a Concurso Público de Antecedentes.",
    "Concurso Público para Proveer el Cargo de Coordinadora o Coordinador Programa 4 a 7",
)


class _DummyWordPressScraper(WordPressScraper):
    def descubrir_ofertas(self):  # pragma: no cover - no se usa en estos tests
        return []


def _build_scraper() -> WordPressScraper:
    return _DummyWordPressScraper.__new__(_DummyWordPressScraper)


class PareceOfertaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = _build_scraper()

    def test_noticias_municipales_se_rechazan(self):
        for titulo in TITULOS_NOTICIA:
            with self.subTest(titulo=titulo):
                self.assertFalse(
                    self.scraper._parece_oferta(titulo, "Contenido genérico de la noticia."),
                    f"Se aceptó como oferta: {titulo}",
                )

    def test_concursos_reales_se_aceptan(self):
        for titulo in TITULOS_OFERTA:
            with self.subTest(titulo=titulo):
                self.assertTrue(
                    self.scraper._parece_oferta(titulo, ""),
                    f"Se rechazó un concurso real: {titulo}",
                )

    def test_concurso_a_secas_requiere_contexto_laboral(self):
        # "Concurso Programa 4 a 7" sin más señales en título: sólo pasa si el
        # contenido tiene contexto de postulación.
        self.assertTrue(
            self.scraper._parece_oferta(
                "Concurso Programa 4 a 7",
                "Recepción de antecedentes y requisitos en oficina de partes.",
            )
        )
        self.assertFalse(
            self.scraper._parece_oferta("Concurso de pintura infantil", "Bases en la municipalidad.")
        )

    def test_substring_no_gatilla_keywords(self):
        # "Descargos" no es "cargos"; "Directorios" no es "director".
        self.assertFalse(self.scraper._parece_oferta("Descargos Elecciones", ""))
        self.assertFalse(
            self.scraper._parece_oferta("Nuevos Formatos para la Renovación de Directorios", "")
        )

    def test_rol_solo_con_contexto(self):
        # Rol genérico sin contexto laboral → no pasa.
        self.assertFalse(
            self.scraper._parece_oferta(
                "Directora regional visitó la comuna", "Actividad protocolar."
            )
        )
        # Rol genérico + contexto de contratación → pasa.
        self.assertTrue(
            self.scraper._parece_oferta(
                "Coordinador/a Programa Mujeres Jefas de Hogar",
                "Recepción de antecedentes hasta el 30 de junio. Requisitos: título profesional.",
            )
        )


class TituloEsNoticiaTests(unittest.TestCase):
    def test_titulares_noticiosos(self):
        for titulo in (
            TITULOS_NOTICIA[0],
            TITULOS_NOTICIA[1],
            TITULOS_NOTICIA[2],
            TITULOS_NOTICIA[6],
            TITULOS_NOTICIA[8],
            TITULOS_NOTICIA[9],
        ):
            with self.subTest(titulo=titulo):
                self.assertTrue(titulo_es_noticia(titulo))

    def test_senal_laboral_fuerte_anula_patron_noticioso(self):
        # "¡Participa!" es patrón noticioso pero el concurso público manda.
        self.assertFalse(titulo_es_noticia("¡Participa! Concurso Público Profesional SECPLAN"))

    def test_concursos_reales_no_son_noticia(self):
        for titulo in TITULOS_OFERTA:
            with self.subTest(titulo=titulo):
                self.assertFalse(titulo_es_noticia(titulo))


class IntakeRechazaNoticiasTests(unittest.TestCase):
    """La capa transversal debe rechazar estas filas: es lo que usa
    scripts/limpiar_ofertas_no_laborales.py para darlas de baja."""

    def _offer(self, cargo: str) -> dict:
        return {
            "cargo": cargo,
            "url_oferta": "https://munidemo.cl/" + "x" * 5,
            "descripcion": "Contenido del post.",
        }

    def test_noticias_se_descartan(self):
        for titulo in TITULOS_NOTICIA:
            with self.subTest(titulo=titulo):
                decision = intake_validate_offer(self._offer(titulo))
                self.assertTrue(decision.discard, f"Intake aceptó: {titulo}")

    def test_concursos_reales_no_se_descartan(self):
        for titulo in TITULOS_OFERTA:
            with self.subTest(titulo=titulo):
                decision = intake_validate_offer(self._offer(titulo))
                self.assertFalse(
                    decision.discard,
                    f"Intake descartó un concurso real: {titulo} ({decision.motivo_descarte})",
                )


if __name__ == "__main__":
    unittest.main()
