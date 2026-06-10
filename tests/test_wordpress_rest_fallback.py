from __future__ import annotations

import logging
import unittest
from datetime import datetime, timedelta, timezone
from types import MethodType

from scrapers.plataformas.wordpress import WordPressScraper


class DummyWordPressScraper(WordPressScraper):
    def descubrir_ofertas(self):
        return []


class WordPressRestFallbackTests(unittest.TestCase):
    def _build_scraper(self) -> DummyWordPressScraper:
        scraper = DummyWordPressScraper.__new__(DummyWordPressScraper)
        scraper.base_url = "https://demo.cl"
        scraper.nombre = "scraper.wordpress.demo"
        scraper.logger = logging.getLogger("tests.wordpress_rest_fallback")
        scraper._parece_oferta = MethodType(lambda self, title, content: "cargo" in (title or "").lower(), scraper)
        return scraper

    def test_expandido_si_consulta_inicial_no_retorna_vacantes(self):
        scraper = self._build_scraper()
        llamadas: list[str] = []

        def fake_request_json(self, url: str):
            llamadas.append(url)
            # Ventana inicial (180 días): no retorna resultados
            if "after=" in url:
                return []
            # Barrido ampliado sin after: retorna un post válido en página 1
            if "page=1" in url:
                return [
                    {
                        "title": {"rendered": "Cargo Analista"},
                        "content": {"rendered": "Concurso público"},
                        "date": "2026-04-20T10:00:00",
                        "link": "https://demo.cl/oferta-1",
                    }
                ]
            return []

        scraper.request_json = MethodType(fake_request_json, scraper)

        ofertas = scraper._fetch_via_rest_api()

        self.assertEqual(len(ofertas), 1)
        self.assertTrue(any("after=" in url for url in llamadas))
        self.assertTrue(any("page=1" in url and "after=" not in url for url in llamadas))

    def test_no_expandido_si_primera_consulta_ya_retorna_vacantes(self):
        scraper = self._build_scraper()
        llamadas: list[str] = []

        def fake_request_json(self, url: str):
            llamadas.append(url)
            if "page=1" in url:
                return [
                    {
                        "title": {"rendered": "Cargo Profesional"},
                        "content": {"rendered": "Bases del concurso"},
                        "date": "2026-04-20T10:00:00",
                        "link": "https://demo.cl/oferta-2",
                    }
                ]
            return []

        scraper.request_json = MethodType(fake_request_json, scraper)

        ofertas = scraper._fetch_via_rest_api()

        self.assertEqual(len(ofertas), 1)
        # No debería usar barrido ampliado sin after porque ya hubo vacantes en la primera consulta.
        self.assertEqual(sum(1 for url in llamadas if "after=" not in url), 0)

    def test_expandido_usa_365_dias_si_sin_after_no_retorna(self):
        scraper = self._build_scraper()
        llamadas: list[str] = []
        cutoff_180 = (datetime.now(timezone.utc) - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%S")
        cutoff_365 = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%S")

        def fake_request_json(self, url: str):
            llamadas.append(url)
            # Consulta inicial (180 días): cero vacantes.
            if f"after={cutoff_180}" in url:
                return []
            # Segundo barrido sin after: falla/sin datos.
            if "after=" not in url:
                return []
            # Tercer barrido con 365 días: retorna una vacante.
            if f"after={cutoff_365}" in url:
                return [
                    {
                        "title": {"rendered": "Cargo Jurídico"},
                        "content": {"rendered": "Concurso público"},
                        "date": "2026-04-20T10:00:00",
                        "link": "https://demo.cl/oferta-3",
                    }
                ]
            return []

        scraper.request_json = MethodType(fake_request_json, scraper)

        ofertas = scraper._fetch_via_rest_api()

        self.assertEqual(len(ofertas), 1)
        self.assertTrue(any("after=" not in url for url in llamadas))
        self.assertTrue(any(f"after={cutoff_365}" in url for url in llamadas))


if __name__ == "__main__":
    unittest.main()
