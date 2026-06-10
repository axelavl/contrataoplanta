from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import MethodType, SimpleNamespace

from scrapers.evaluation.models import Decision, ExtractorKind
from scrapers.evaluation.reason_codes import ReasonCode
from scrapers.evaluation.source_evaluator import SourceEvaluator
from scrapers.plataformas.generic_site import RawCandidate
from scrapers.plataformas.hiringroom import HiringRoomScraper
from scrapers.plataformas.playwright_scraper import PlaywrightScraper
from scrapers.plataformas.wordpress import WordPressScraper


class FakeAsyncHttp:
    def __init__(self, payloads: dict[str, str]) -> None:
        self.payloads = payloads

    async def get(self, url: str):
        return self.payloads.get(url, "")


class FakeFetchClient:
    def __init__(self, payloads: dict[str, tuple[int, str, dict[str, str]]]) -> None:
        self.payloads = payloads

    async def fetch(self, url: str):
        status, body, headers = self.payloads[url]
        return SimpleNamespace(
            final_url=url,
            status=status,
            headers=headers,
            body=body,
            error_type=None,
            error_detail=None,
        )


class DummyWordPressScraper(WordPressScraper):
    def descubrir_ofertas(self):
        return []


def _build_wordpress_scraper() -> DummyWordPressScraper:
    scraper = DummyWordPressScraper.__new__(DummyWordPressScraper)
    scraper.base_url = "https://municipio-demo.cl"
    scraper.nombre = "scraper.wordpress.demo"
    scraper.institucion = {
        "id": 1,
        "nombre": "Municipalidad Demo",
        "region": "Metropolitana",
    }
    scraper._parece_oferta = MethodType(lambda self, title, content: "concurso" in (title or "").lower(), scraper)
    scraper.normalize_offer = MethodType(lambda self, offer: offer, scraper)
    return scraper


def test_smoke_wordpress_real_publications_have_offers_gt_zero():
    scraper = _build_wordpress_scraper()
    future_deadline = (datetime.now(timezone.utc) + timedelta(days=20)).strftime("%d-%m-%Y")

    def fake_request_json(self, url: str):
        return [
            {
                "title": {"rendered": "Concurso público profesional"},
                "content": {"rendered": f"Postulación abierta. Cierre {future_deadline}."},
                "date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                "link": "https://municipio-demo.cl/oferta-1",
            }
        ]

    scraper.request_json = MethodType(fake_request_json, scraper)
    scraper.request_text = MethodType(lambda self, _url: f"<p>Cierre {future_deadline}</p>", scraper)

    raw_offers = scraper.fetch_ofertas()
    parsed = [scraper.parse_oferta(item) for item in raw_offers]
    parsed = [item for item in parsed if item is not None]

    assert len(parsed) > 0


def test_smoke_hiringroom_real_publications_have_offers_gt_zero():
    institucion = {
        "id": 2,
        "nombre": "Empresa Demo",
        "url_empleo": "https://empresa.hiringroom.com/jobs",
        "sitio_web": "https://empresa.hiringroom.com",
        "region": "Metropolitana",
    }
    scraper = HiringRoomScraper(fuente_id=2, institucion=institucion)
    scraper._candidate_urls = MethodType(
        lambda self: super(HiringRoomScraper, self)._candidate_urls(
            max_urls=self.max_candidate_urls,
            preferred_url=None,
        ),
        scraper,
    )
    listing_html = """
    <html><body>
      <script type=\"application/ld+json\">{
        \"@context\": \"https://schema.org\",
        \"@type\": \"JobPosting\",
        \"title\": \"Analista de Datos\",
        \"description\": \"Vacante laboral modalidad hibrida\",
        \"url\": \"https://empresa.hiringroom.com/jobs/analista-datos\",
        \"datePosted\": \"2026-04-20\"
      }</script>
    </body></html>
    """
    detail_html = "<html><body><h1>Analista de Datos</h1><p>Concurso público, postulación abierta.</p></body></html>"
    scraper.http = FakeAsyncHttp(
        {
            "https://empresa.hiringroom.com/jobs": listing_html,
            "https://empresa.hiringroom.com": "",
            "https://empresa.hiringroom.com/empleos": "",
            "https://empresa.hiringroom.com/trabajos": "",
            "https://empresa.hiringroom.com/careers": "",
            "https://empresa.hiringroom.com/jobs/analista-datos": detail_html,
        }
    )

    offers = asyncio.run(scraper.descubrir_ofertas())

    assert len(offers) > 0


def test_smoke_playwright_real_publications_have_offers_gt_zero(monkeypatch):
    institucion = {
        "id": 3,
        "nombre": "Institución JS",
        "url_empleo": "https://jobs.js-demo.cl/careers",
        "sitio_web": "https://jobs.js-demo.cl",
        "region": "Metropolitana",
    }
    scraper = PlaywrightScraper(fuente_id=3, institucion=institucion)
    scraper.http = FakeAsyncHttp({})

    async def fake_extract(self, *, entry_url: str, timeout_ms: int, user_agent: str):
        del entry_url, timeout_ms, user_agent
        return [
            RawCandidate(
                title="Ingeniero de Plataforma",
                content_text=(
                    "Vacante de empleo. Requisitos del cargo, funciones, "
                    "postulación abierta y fecha limite 30-12-2026."
                ),
                url="https://jobs.js-demo.cl/jobs/ingeniero-plataforma",
                date_value="2026-04-20",
                closing_value="30-12-2026",
                pdf_links=["https://jobs.js-demo.cl/docs/bases.pdf"],
            )
        ]

    monkeypatch.setattr(PlaywrightScraper, "_extract_with_playwright", fake_extract)

    offers = asyncio.run(scraper.descubrir_ofertas())

    assert len(offers) > 0
    assert scraper.signals_json["playwright"]["offers"] > 0


def test_smoke_family_reason_codes_when_no_real_publications():
    evaluator = SourceEvaluator(
        http_client=FakeFetchClient(
            payloads={
                "https://municipio-demo.cl/trabaja-con-nosotros": (
                    200,
                    "<html><title>Noticias</title><body>Noticias, talleres, subsidio y beneficio vecinal.</body></html>",
                    {"Content-Type": "text/html"},
                ),
                "https://municipio-demo.cl/": (
                    200,
                    "<html><body>Beca, noticia y curso municipal sin vacantes.</body></html>",
                    {"Content-Type": "text/html"},
                ),
                "https://empresa.hiringroom.com/jobs": (
                    200,
                    "<html><body><h1>Careers</h1><p>News, workshop and benefits page.</p></body></html>",
                    {"Content-Type": "text/html"},
                ),
                "https://empresa.hiringroom.com/empleos": (
                    200,
                    "<html><body>Noticia, trámite y taller institucional.</body></html>",
                    {"Content-Type": "text/html"},
                ),
                "https://www.bcentral.cl/": (
                    200,
                    "<html><body>Please enable JavaScript to continue.</body></html>",
                    {"Content-Type": "text/html"},
                ),
                "https://www.bcentral.cl/careers": (
                    200,
                    "<html><body>Please enable JavaScript.</body></html>",
                    {"Content-Type": "text/html"},
                ),
            }
        )
    )

    wordpress_result = asyncio.run(
        evaluator.evaluate(
            {
                "id": 10,
                "url_empleo": "https://municipio-demo.cl/trabaja-con-nosotros",
                "sitio_web": "https://municipio-demo.cl/",
                "plataforma_empleo": "WordPress",
            }
        )
    )
    hiringroom_result = asyncio.run(
        evaluator.evaluate(
            {
                "id": 11,
                "url_empleo": "https://empresa.hiringroom.com/jobs",
                "sitio_web": "https://empresa.hiringroom.com/",
                "plataforma_empleo": "HiringRoom",
            }
        )
    )
    playwright_result = asyncio.run(
        evaluator.evaluate(
            {
                "id": 145,
                "url_empleo": "https://www.bcentral.cl/",
                "sitio_web": "https://www.bcentral.cl/",
            }
        )
    )

    assert wordpress_result.reason_code == ReasonCode.NOT_JOB_RELATED
    assert hiringroom_result.profile_name == "ats_hiringroom"
    assert hiringroom_result.reason_code is None
    assert playwright_result.reason_code == ReasonCode.MANUAL_REVIEW_REQUIRED
    assert playwright_result.availability.value == "js_required"
    assert playwright_result.recommended_extractor == ExtractorKind.SCRAPER_PLAYWRIGHT
    assert playwright_result.decision in {Decision.MANUAL_REVIEW, Decision.SKIP}
