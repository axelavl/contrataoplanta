from __future__ import annotations

from types import MethodType

from scrapers.plataformas.generic_site import GenericSiteScraper


class _LegacyWrapperScraper(GenericSiteScraper):
    """Simula scrapers legacy que exponen un wrapper público."""

    def looks_like_offer(self, title: str, content: str, url: str | None = None) -> bool:
        return self._looks_like_offer(title, content, url=url)



def _make_scraper(scraper_cls: type[GenericSiteScraper] = GenericSiteScraper) -> GenericSiteScraper:
    institucion = {
        "id": 9999,
        "nombre": "Institución de Prueba",
        "sitio_web": "https://example.org",
        "url_empleo": "https://example.org/empleos",
        "region": "Nacional",
    }
    return scraper_cls(fuente_id=9999, institucion=institucion)


def test_looks_like_offer_delegates_to_score_offer_candidate() -> None:
    scraper = _make_scraper()
    calls: list[tuple[str, str, str | None]] = []

    def _fake_score(self: GenericSiteScraper, title: str, content: str, *, url: str | None = None):
        calls.append((title, content, url))
        return True, "forced_true"

    scraper._score_offer_candidate = MethodType(_fake_score, scraper)

    result = scraper._looks_like_offer("Analista", "Concurso público", url="https://example.org/1")

    assert result is True
    assert calls == [("Analista", "Concurso público", "https://example.org/1")]


def test_legacy_wrapper_remains_compatible_with_genericsite_changes() -> None:
    scraper = _make_scraper(_LegacyWrapperScraper)

    def _fake_score(self: GenericSiteScraper, title: str, content: str, *, url: str | None = None):
        assert title == "Chofer"
        assert content == "Planta"
        assert url is None
        return False, "forced_false"

    scraper._score_offer_candidate = MethodType(_fake_score, scraper)

    assert scraper.looks_like_offer("Chofer", "Planta") is False
