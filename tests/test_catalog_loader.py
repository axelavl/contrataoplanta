from __future__ import annotations

import json
from pathlib import Path

from openpyxl import Workbook

from scrapers.evaluation.catalog_loader import CatalogLoader


def test_catalog_loader_merges_json_and_xlsx(tmp_path: Path):
    json_path = tmp_path / "catalog.json"
    json_path.write_text(
        json.dumps(
            {
                "metadata": {"source": "json"},
                "instituciones": [
                    {
                        "id": 1,
                        "nombre": "Municipalidad Demo",
                        "url_empleo": "https://demo.cl/empleos",
                        "estado_verificacion": "verificado",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    xlsx_path = tmp_path / "catalog.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Repositorio Maestro"
    ws.append([])
    ws.append([])
    ws.append([])
    ws.append(
        [
            "ID",
            "Nombre Oficial",
            "Sigla",
            "Categoría",
            "Sector",
            "Dependencia",
            "Región",
            "Sitio Web Oficial",
            "URL Portal de Empleos",
            "Plataforma Empleo",
            "En empleospublicos.cl",
            "Dificultad Scraping",
            "Requiere JS",
            "Notas Técnicas",
            "Población Censo 2024",
            "Estado Verificación",
        ]
    )
    ws.append([1, "Municipalidad Demo", "MD", "Municipal", "Municipal", "", "Metropolitana", "https://demo.cl", "https://demo.cl/empleos", "WordPress", "No", "Media", "No", "ok", 1000, ""])
    wb.save(xlsx_path)

    loader = CatalogLoader(json_path=json_path, xlsx_path=xlsx_path)
    bundle = loader.load(prefer_json=True)
    assert bundle.source == "json"
    assert bundle.metadata["secondary_source"] == "xlsx"
    assert bundle.instituciones[0]["id"] == 1
    assert bundle.instituciones[0]["nombre"] == "Municipalidad Demo"
