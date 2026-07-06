#!/usr/bin/env python3
"""Auditoría de salud del catálogo de fuentes de ofertas.

Recorre `repositorio_instituciones_publicas_chile.json`, clasifica cada
institución con `scrapers.source_status` y reporta la salud del catálogo:
cobertura efectiva, fuentes con URL de empleo débil (== homepage), sin URL,
sin verificar, y URLs compartidas. NO requiere red ni base de datos.

Uso:
    python scripts/auditar_catalogo.py            # resumen
    python scripts/auditar_catalogo.py --listar   # + listado de fuentes a corregir
    python scripts/auditar_catalogo.py --csv salida.csv   # exporta las débiles
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

_RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_RAIZ))
_CATALOGO = _RAIZ / "repositorio_instituciones_publicas_chile.json"

# La clasificación es pura, pero la cadena de imports (config → db.config) exige
# credenciales de BD. Como esta auditoría NO toca la BD, ponemos un DSN dummy si
# no hay uno, para que el script corra en cualquier entorno sin configurar nada.
os.environ.setdefault("DATABASE_URL", "postgresql://u:p@localhost:5432/audit")

from scrapers.source_status import classify_source  # noqa: E402

try:
    from scrapers.source_status import RUNNABLE_KINDS
    _RUNNABLE = {k.value for k in RUNNABLE_KINDS}
except Exception:  # pragma: no cover
    _RUNNABLE = set()


def _cargar() -> list[dict]:
    data = json.loads(_CATALOGO.read_text(encoding="utf-8-sig"))
    return data["instituciones"] if isinstance(data, dict) else data


def _norm(url: str | None) -> str:
    return (url or "").strip().rstrip("/").lower()


def _clasificar(insts: list[dict]) -> list[dict]:
    """Anota cada institución con status/kind y una etiqueta de cobertura."""
    filas = []
    for i in insts:
        d = classify_source(i)
        status = d.status.value if d.status else "?"
        kind = d.kind.value if d.kind else "?"
        url, sitio = _norm(i.get("url_empleo")), _norm(i.get("sitio_web"))
        es_ep = "empleospublicos.cl" in url
        runnable = kind in _RUNNABLE and not es_ep
        if not url:
            cobertura = "sin_url"
        elif es_ep:
            cobertura = "empleos_publicos"
        elif kind not in _RUNNABLE:
            cobertura = "no_runnable"
        elif url == sitio:
            cobertura = "url_debil"       # apunta a la homepage → suele no traer ofertas
        else:
            cobertura = "efectiva"
        filas.append({**i, "_status": status, "_kind": kind,
                      "_runnable": runnable, "_cobertura": cobertura})
    return filas


def main() -> int:
    ap = argparse.ArgumentParser(description="Auditoría del catálogo de fuentes")
    ap.add_argument("--listar", action="store_true",
                    help="Lista las fuentes con URL débil / sin URL a corregir")
    ap.add_argument("--csv", metavar="ARCHIVO",
                    help="Exporta a CSV las fuentes con URL débil / sin URL")
    a = ap.parse_args()

    insts = _cargar()
    filas = _clasificar(insts)
    n = len(filas)

    print(f"Catálogo: {n} instituciones\n")

    def _cuenta(campo: str) -> Counter:
        return Counter(f[campo] for f in filas)

    print("── Por estado (source_status) ──")
    for k, v in _cuenta("_status").most_common():
        print(f"  {k:16} {v}")

    print("\n── Por cobertura ──")
    etiquetas = {
        "efectiva": "cobertura efectiva (URL propia de empleo, scraper corre)",
        "empleos_publicos": "publican vía empleospublicos.cl (portal central)",
        "url_debil": "URL de empleo == homepage (probable 0 ofertas)",
        "no_runnable": "clasificada en un kind no ejecutable (skip/…)",
        "sin_url": "sin URL de empleo ni sitio",
    }
    cob = _cuenta("_cobertura")
    for k in ("efectiva", "empleos_publicos", "url_debil", "no_runnable", "sin_url"):
        print(f"  {k:16} {cob.get(k, 0):4}  {etiquetas[k]}")

    verificados = sum(1 for f in filas
                      if "verificar" not in (f.get("estado_verificacion") or "").lower())
    print(f"\n── Verificación ──\n  verificados {verificados} / por verificar {n - verificados}")

    # URLs compartidas por >1 institución (excluye el portal central).
    por_url: dict[str, list[int]] = defaultdict(list)
    for f in filas:
        u = _norm(f.get("url_empleo"))
        if u and "empleospublicos.cl" not in u:
            por_url[u].append(f["id"])
    compartidas = {u: ids for u, ids in por_url.items() if len(ids) > 1}
    print(f"\n── URLs compartidas por >1 institución (fuera del portal): {len(compartidas)} ──")
    if a.listar:
        for u, ids in sorted(compartidas.items()):
            print(f"    {u[:64]:64} ids={ids}")

    a_corregir = [f for f in filas if f["_cobertura"] in ("url_debil", "sin_url")]
    print(f"\n── A CORREGIR (URL débil o ausente): {len(a_corregir)} ──")
    porcat = Counter(f.get("categoria") for f in a_corregir)
    for k, v in porcat.most_common():
        print(f"  {k:26} {v}")

    if a.listar:
        print("\n── Detalle de fuentes a corregir ──")
        for f in sorted(a_corregir, key=lambda x: (x.get("region") or "", x["id"])):
            print(f"  id={f['id']:4} [{f['_cobertura']:9}] {(f['nombre'] or '')[:44]:44} "
                  f"{(f.get('region') or '')[:20]:20} {(f.get('url_empleo') or '(sin url)')[:44]}")

    if a.csv:
        with open(a.csv, "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["id", "nombre", "categoria", "region", "cobertura",
                        "url_empleo", "sitio_web", "estado_verificacion"])
            for f in sorted(a_corregir, key=lambda x: (x.get("categoria") or "", x["id"])):
                w.writerow([f["id"], f.get("nombre"), f.get("categoria"),
                            f.get("region"), f["_cobertura"], f.get("url_empleo"),
                            f.get("sitio_web"), f.get("estado_verificacion")])
        print(f"\nExportado a {a.csv} ({len(a_corregir)} filas)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
