from __future__ import annotations

import json
import zipfile
from pathlib import Path

from scrapers.evaluation.catalog_loader import CatalogLoader
from scrapers.evaluation.historical_quality_audit import run_historical_audit


def _write_minimal_ods(path: Path) -> None:
    content = """<?xml version="1.0" encoding="UTF-8"?>
<office:document-content xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" office:version="1.2">
  <office:body>
    <office:spreadsheet>
      <table:table table:name="ofertas">
        <table:table-row>
          <table:table-cell><text:p>institucion_id</text:p></table:table-cell>
          <table:table-cell><text:p>institucion_nombre</text:p></table:table-cell>
          <table:table-cell><text:p>cargo</text:p></table:table-cell>
          <table:table-cell><text:p>descripcion</text:p></table:table-cell>
          <table:table-cell><text:p>fecha_cierre</text:p></table:table-cell>
          <table:table-cell><text:p>url_bases</text:p></table:table-cell>
          <table:table-cell><text:p>estado</text:p></table:table-cell>
        </table:table-row>
        <table:table-row>
          <table:table-cell><text:p>705</text:p></table:table-cell>
          <table:table-cell><text:p>Institucion Fantasma</text:p></table:table-cell>
          <table:table-cell><text:p>Concursos</text:p></table:table-cell>
          <table:table-cell><text:p>Listado de concursos y noticias.</text:p></table:table-cell>
          <table:table-cell><text:p>2026-04-01</text:p></table:table-cell>
          <table:table-cell><text:p>https://www.empleospublicos.cl/documentos/politicaprivacidad.pdf</text:p></table:table-cell>
          <table:table-cell><text:p>activo</text:p></table:table-cell>
        </table:table-row>
      </table:table>
    </office:spreadsheet>
  </office:body>
</office:document-content>"""
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("content.xml", content)


def test_historical_audit_reports_reason_codes(tmp_path: Path):
    ods_path = tmp_path / "ofertas.ods"
    _write_minimal_ods(ods_path)

    json_path = tmp_path / "catalog.json"
    json_path.write_text(
        json.dumps({"instituciones": [{"id": 1, "nombre": "Servicio Real"}]}),
        encoding="utf-8",
    )
    report = run_historical_audit(ods_path, catalog_loader=CatalogLoader(json_path=json_path, xlsx_path=tmp_path / "missing.xlsx"))
    assert report["total_rows"] == 1
    assert "invalid_institution_reference" in report["reason_counts"]
    assert "placeholder_bases_url" in report["reason_counts"]
