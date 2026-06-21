"""Regresiones de la limpieza de junio 2026 (ofertas sin fecha de cierre).

Cubre tres correcciones detectadas verificando las ofertas contra el origen:

1. Fix 1 — WordPress no debe ingerir la sección "Concursos Públicos Anteriores"
   de un listado (caso Municipalidad de Pucón: 8 vigentes vs 80 históricos en la
   misma página). `WordPressScraper._parse_html_listing`.
2. Fix 2 — los avisos de elecciones de organizaciones comunitarias (Ley 19.418),
   tipo "Oficio Conductor Junta de Vecinos…", NO son ofertas de empleo (caso
   Cobquecura: el rol "conductor" los colaba). `WordPressScraper._parece_oferta`.
3. Fix 3 — el validador de URLs no debe marcar como "muerta" una URL viva por
   chequear N veces la misma página de un listado con anclas `#` (caso Pucón:
   `?page_id=89#a`, `?page_id=89#b`, …). `validate_offer_urls._verificar_url`.
"""

from __future__ import annotations

import asyncio
import unittest

from scrapers.plataformas.wordpress import WordPressScraper
from validate_offer_urls import _verificar_url


class _DummyWordPressScraper(WordPressScraper):
    def descubrir_ofertas(self):  # pragma: no cover - no se usa
        return []


def _build_scraper() -> WordPressScraper:
    return _DummyWordPressScraper.__new__(_DummyWordPressScraper)


# ── Fix 1: corte del archivo histórico en el listado HTML ────────────────────
LISTADO_PUCON = """
<main>
  <h2>Concursos Públicos en Curso</h2>
  <ul>
    <li><a href="/?page_id=89#cur1">Concurso Público para proveer el cargo de Director(a) de CESFAM</a></li>
    <li><a href="/?page_id=89#cur2">Llamado Concurso Público Programa Jefas de Hogar Convenio SERNAMEG</a></li>
  </ul>
  <h2>Concursos Públicos Anteriores</h2>
  <ul>
    <li><a href="/?page_id=89#old1">Llamado Concurso Público Planta Municipal 2019</a></li>
    <li><a href="/?page_id=89#old2">Concurso Director Departamento de Salud Pucón</a></li>
    <li><a href="/?page_id=89#old3">Llamado a Concurso Público Técnico grado 15°</a></li>
  </ul>
</main>
"""


class ArchivoHistoricoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = _build_scraper()

    def test_solo_se_ingieren_concursos_en_curso(self):
        ofertas = self.scraper._parse_html_listing(LISTADO_PUCON, "https://municipalidadpucon.cl/")
        titulos = " || ".join(o["title"] for o in ofertas)
        # Vigentes presentes
        self.assertIn("CESFAM", titulos)
        self.assertIn("Jefas de Hogar", titulos)
        # Históricos excluidos
        self.assertNotIn("Planta Municipal 2019", titulos)
        self.assertNotIn("Departamento de Salud", titulos)
        self.assertNotIn("Técnico grado 15", titulos)
        self.assertEqual(len(ofertas), 2)

    def test_sin_seccion_anteriores_no_se_corta_nada(self):
        html = (
            "<main><h2>Concursos Públicos</h2><ul>"
            "<li><a href='/c#1'>Concurso Público para proveer cargo de Auxiliar</a></li>"
            "<li><a href='/c#2'>Llamado a Concurso Público Profesional grado 10°</a></li>"
            "</ul></main>"
        )
        ofertas = self.scraper._parse_html_listing(html, "https://muni.cl/")
        self.assertEqual(len(ofertas), 2)


# ── Fix 2: avisos de elecciones de organizaciones (no son empleo) ────────────
TITULOS_ELECCIONES = (
    "Oficio Conductor Junta de Vecinos La Maravilla",
    "Oficio Conductor Club de Adulto Mayor El Amanecer de Buchupureo",
    "Oficio Conductor Centro General de Padres y/o Apoderados Escuela de Lenguaje",
    "Oficio Conductor Asociación de Fútbol Particular Cobquecura",
)


class OficioEleccionesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = _build_scraper()

    def test_avisos_de_elecciones_se_rechazan(self):
        for titulo in TITULOS_ELECCIONES:
            with self.subTest(titulo=titulo):
                self.assertFalse(
                    self.scraper._parece_oferta(titulo, "Elecciones de organizaciones comunitarias."),
                    f"Se aceptó como oferta un aviso de elecciones: {titulo}",
                )

    def test_concurso_real_de_conductor_sigue_pasando(self):
        # Un concurso REAL de conductor (con contexto laboral) no debe verse afectado.
        self.assertTrue(
            self.scraper._parece_oferta(
                "Concurso Público para proveer cargo de Conductor de Ambulancia",
                "Recepción de antecedentes y requisitos hasta el 30 de junio.",
            )
        )


# ── Fix 3: validador de URLs robusto a fragmentos y a páginas compartidas ────
class _FakeResp:
    def __init__(self, status: int) -> None:
        self.status = status

    async def __aenter__(self) -> "_FakeResp":
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False


class _FakeSession:
    """Cuenta las llamadas de red para verificar el cacheo por URL real."""

    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.calls: list[tuple[str, str]] = []

    def head(self, url: str, **_: object) -> _FakeResp:
        self.calls.append(("head", url))
        return _FakeResp(self.status)

    def get(self, url: str, **_: object) -> _FakeResp:
        self.calls.append(("get", url))
        return _FakeResp(self.status)


class ValidadorUrlTests(unittest.TestCase):
    def test_fragmento_se_ignora_y_se_cachea(self):
        async def run():
            sem = asyncio.Semaphore(5)
            cache: dict[str, bool | None] = {}
            session = _FakeSession(status=200)
            r1 = await _verificar_url(session, "https://muni.cl/?page_id=89#a", sem, cache)
            r2 = await _verificar_url(session, "https://muni.cl/?page_id=89#b", sem, cache)
            return r1, r2, session.calls, cache

        r1, r2, calls, cache = asyncio.run(run())
        self.assertTrue(r1)
        self.assertTrue(r2)
        # Dos ofertas que comparten página → una sola consulta de red.
        self.assertEqual(len(calls), 1)
        # La clave del cache no lleva fragmento.
        self.assertIn("https://muni.cl/?page_id=89", cache)

    def test_url_404_es_falsa(self):
        async def run():
            sem = asyncio.Semaphore(5)
            session = _FakeSession(status=404)
            return await _verificar_url(session, "https://muni.cl/no-existe", sem, {})

        self.assertFalse(asyncio.run(run()))

    def test_url_no_http_devuelve_none(self):
        async def run():
            sem = asyncio.Semaphore(5)
            return await _verificar_url(_FakeSession(), "#", sem, {})

        self.assertIsNone(asyncio.run(run()))


if __name__ == "__main__":
    unittest.main()
