from __future__ import annotations

import argparse
import json
import zipfile
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from .catalog_loader import CatalogLoader
from .quality_validator import QualityValidator, build_duplicate_fingerprint
from .reason_codes import ReasonCode


ODS_TABLE_NS = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
}


def _read_ods_rows(path: Path) -> list[dict[str, Any]]:
    with zipfile.ZipFile(path) as archive:
        xml = archive.read("content.xml")
    root = ET.fromstring(xml)
    table = root.find(".//table:table[@table:name='ofertas']", ODS_TABLE_NS)
    if table is None:
        raise ValueError("No se encontro la hoja 'ofertas' en el ODS.")

    rows: list[list[str]] = []
    for row in table.findall("table:table-row", ODS_TABLE_NS):
        values: list[str] = []
        for cell in row.findall("table:table-cell", ODS_TABLE_NS):
            repeat = int(cell.attrib.get(f"{{{ODS_TABLE_NS['table']}}}number-columns-repeated", "1"))
            text_nodes = cell.findall(".//text:p", ODS_TABLE_NS)
            content = " ".join("".join(node.itertext()).strip() for node in text_nodes).strip()
            for _ in range(repeat):
                values.append(content)
        rows.append(values)

    if not rows:
        return []
    header = rows[0]
    records: list[dict[str, Any]] = []
    for row in rows[1:]:
        record = {}
        for index, key in enumerate(header):
            if not key:
                continue
            record[key] = row[index] if index < len(row) else ""
        records.append(record)
    return records


def run_historical_audit(input_path: Path, *, catalog_loader: CatalogLoader | None = None) -> dict[str, Any]:
    catalog = (catalog_loader or CatalogLoader()).load(prefer_json=True)
    valid_ids = {
        int(item["id"])
        for item in catalog.instituciones
        if item.get("id") not in (None, "")
    }
    validator = QualityValidator(valid_institution_ids=valid_ids)
    rows = _read_ods_rows(input_path)
    seen_fingerprints: set[str] = set()
    reason_counter: Counter[str] = Counter()
    institution_counter: dict[str, Counter[str]] = defaultdict(Counter)
    remediation: dict[str, list[str]] = defaultdict(list)

    for row in rows:
        oferta = {
            "institucion_id": int(row["institucion_id"]) if row.get("institucion_id", "").isdigit() else None,
            "institucion_nombre": row.get("institucion_nombre") or row.get("institucion"),
            "cargo": row.get("cargo") or row.get("titulo"),
            "descripcion": row.get("descripcion"),
            "fecha_publicacion": row.get("fecha_publicacion"),
            "fecha_cierre": row.get("fecha_cierre"),
            "url_bases": row.get("url_bases"),
            "renta_bruta_min": row.get("renta_bruta_min"),
            "renta_bruta_max": row.get("renta_bruta_max"),
            "estado": row.get("estado"),
            "activa": row.get("activa") in {"1", "true", "TRUE", "si", "Si", "Sí"},
        }
        validation = validator.validate(oferta, seen_fingerprints=seen_fingerprints, today=date.today())
        if validation.reason_codes:
            for code in validation.reason_codes:
                reason_counter[code.value] += 1
                institution_counter[str(oferta.get("institucion_id") or oferta.get("institucion_nombre") or "sin_institucion")][code.value] += 1
                if code == ReasonCode.STALE_ACTIVE_OFFER:
                    remediation[code.value].append("Marcar oferta como cerrada o rechazarla al recalcular vigencia.")
                elif code == ReasonCode.PLACEHOLDER_BASES_URL:
                    remediation[code.value].append("Nullear url_bases placeholder y forzar reextraccion de bases reales.")
                elif code == ReasonCode.INVALID_INSTITUTION_REFERENCE:
                    remediation[code.value].append("Mover a quarantine/manual_review y reconciliar catalogo.")
                elif code == ReasonCode.NOT_JOB_RELATED:
                    remediation[code.value].append("Revalidar con gatekeeper WordPress/noticias y rebajar frecuencia de la fuente.")
                elif code == ReasonCode.SALARY_OUTLIER:
                    remediation[code.value].append("Limpiar renta parseada y reextraer con parser de remuneraciones.")
                elif code == ReasonCode.DUPLICATE_CANDIDATE:
                    remediation[code.value].append("Consolidar duplicados por (institucion_id, cargo, fecha_cierre).")

    return {
        "total_rows": len(rows),
        "reason_counts": dict(reason_counter.most_common()),
        "by_institution": {key: dict(counter.most_common()) for key, counter in institution_counter.items()},
        "remediation_plan": {key: sorted(set(items)) for key, items in remediation.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Auditoria historica de calidad sobre ofertas.ods")
    parser.add_argument("--input", required=True, help="Ruta al archivo ofertas.ods")
    parser.add_argument("--output", help="Ruta opcional para guardar un reporte JSON")
    args = parser.parse_args()

    report = run_historical_audit(Path(args.input))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.output:
        Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
