from __future__ import annotations

import asyncio
from types import SimpleNamespace

from scrapers.evaluation.models import Decision, ExtractorKind
from scrapers.evaluation.source_evaluator import SourceEvaluator


class FakeHttpClient:
    def __init__(self, payloads: dict[str, tuple[int, str, dict[str, str]]]) -> None:
        self.payloads = payloads
        self.visited: list[str] = []

    async def fetch(self, url: str):
        self.visited.append(url)
        status, body, headers = self.payloads[url]
        return SimpleNamespace(
            final_url=url,
            status=status,
            headers=headers,
            body=body,
            error_type=None,
            error_detail=None,
        )


def test_pre_discovery_aggregates_profile_routes_before_extractor_selection():
    source = {
        "id": 162,
        "url_empleo": "https://www.pdichile.cl/institucion/concursos-publicos/portada",
        "sitio_web": "https://www.pdichile.cl/",
    }
    payloads = {
        "https://www.pdichile.cl/institucion/concursos-publicos/portada": (
            200,
            "<html><head><title>Portada</title></head><body>Inicio institucional.</body></html>",
            {"Content-Type": "text/html"},
        ),
        "https://www.pdichile.cl/": (
            200,
            (
                "<html><head><title>Postulaciones</title></head><body>"
                "<h1>Concurso Público Analista</h1>"
                "<p>Recepción de antecedentes y perfil del cargo.</p>"
                '<a href="/docs/bases-analista.pdf">Descargar bases</a>'
                "</body></html>"
            ),
            {"Content-Type": "text/html"},
        ),
    }
    client = FakeHttpClient(payloads=payloads)
    evaluator = SourceEvaluator(http_client=client)

    result = asyncio.run(evaluator.evaluate(source))

    assert result.decision == Decision.EXTRACT
    assert result.recommended_extractor == ExtractorKind.SCRAPER_PDF_JOBS
    assert len(client.visited) == 2
    assert result.signals_json["pdf_links_count"] == 1
    assert len(result.signals_json["evaluated_urls_snapshot"]) == 2
    assert result.signals_json["institucion_id"] == 162
    assert result.signals_json["pre_discovery"]["institucion_id"] == 162
