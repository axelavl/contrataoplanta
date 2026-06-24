"""Regresión de la incorporación de scrapers/municipios.py (junio 2026).

El zip original asignaba a cada fuente un `id` secuencial (180, 181, …) que NO
correspondía al catálogo: el 180 era "Delegación Presidencial Regional de
Valparaíso", no "Municipalidad de Peñalolén". Como `municipios.py` usa ese id
como `institucion_id` para persistir y, sobre todo, para `marcar_ofertas_cerradas`,
el bug habría mis-atribuido ofertas y CERRADO ofertas de la institución
equivocada. Este test fija el remapeo correcto y la no-duplicación con el
scraper genérico.
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import scrapers.municipios as muni

RAIZ = Path(__file__).resolve().parents[1]
CATALOGO = RAIZ / "repositorio_instituciones_publicas_chile.json"
MODOS_FUNCIONALES = {"pdf_links", "secciones", "enlaces_detalle", "headings", "lista_o_vacio"}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s).strip()


def _catalogo() -> dict[int, str]:
    data = json.loads(CATALOGO.read_text(encoding="utf-8-sig"))
    items = data.get("instituciones") if isinstance(data, dict) else data
    return {i.get("id"): i.get("nombre", "") for i in items}


def test_cada_fuente_apunta_a_municipalidad_o_corporacion():
    cat = _catalogo()
    for f in muni.FUENTES:
        iid = int(f["id"])
        assert iid in cat, f"{f['clave']}: id {iid} no existe en el catálogo"
        nombre = _norm(cat[iid])
        # El bug original apuntaba a Gobiernos Regionales / Delegaciones.
        assert "gobierno regional" not in nombre, f"{f['clave']} apunta a un GORE: {cat[iid]}"
        assert "delegacion presidencial" not in nombre, f"{f['clave']} apunta a una Delegación: {cat[iid]}"
        # Toda fuente municipal debe mapear a una municipalidad o corporación.
        assert ("municipal" in nombre or "corporaci" in nombre or "coresam" in nombre
                or "codep" in nombre), f"{f['clave']} ({cat[iid]}) no parece municipal"


def test_remapeo_conocido():
    cat = _catalogo()
    esperado = {"penalolen": 401, "renca": 407, "cerronavia": 382,
                "puentealto": 419, "vina": 345, "sanmiguel": 409}
    by_clave = {f["clave"]: int(f["id"]) for f in muni.FUENTES}
    for clave, iid in esperado.items():
        assert by_clave[clave] == iid, f"{clave} debería mapear a {iid}, no {by_clave.get(clave)}"
        # el nombre del catálogo para ese id ya se valida en el test de municipalidad
        assert iid in cat


def test_ids_funcionales_excluidos_del_generico():
    """Los municipios con modo funcional deben estar en _IDS_NUEVO_ESTANDAR de
    run_all.py, para que el scraper genérico no los procese en paralelo."""
    funcionales = {int(f["id"]) for f in muni.FUENTES if f["modo"] in MODOS_FUNCIONALES}
    run_all_src = (RAIZ / "scrapers" / "run_all.py").read_text(encoding="utf-8")
    m = re.search(r"_IDS_NUEVO_ESTANDAR.*?frozenset\(\{(.*?)\}\)", run_all_src, re.DOTALL)
    assert m, "no se encontró _IDS_NUEVO_ESTANDAR en run_all.py"
    ids_excluidos = {int(x) for x in re.findall(r"\b(\d{2,4})\b", m.group(1))}
    faltan = funcionales - ids_excluidos
    assert not faltan, f"IDs municipales funcionales no excluidos del genérico: {sorted(faltan)}"


def test_fuentes_compartiendo_institucion_solo_difieren_en_modo_no_funcional():
    """Si dos fuentes comparten institucion_id y ambas son funcionales, el cierre
    por ausencia agrupado debe existir (acumulación por institución)."""
    por_id: dict[int, list[str]] = {}
    for f in muni.FUENTES:
        if f["modo"] in MODOS_FUNCIONALES:
            por_id.setdefault(int(f["id"]), []).append(f["clave"])
    compartidos = {iid: cs for iid, cs in por_id.items() if len(cs) > 1}
    # Renca (407) tiene dos fuentes funcionales: exige el cierre agrupado.
    if compartidos:
        src = (RAIZ / "scrapers" / "municipios.py").read_text(encoding="utf-8")
        assert "urls_por_inst" in src, (
            f"Fuentes funcionales comparten institucion_id {list(compartidos)} "
            "pero no hay cierre agrupado (urls_por_inst) en municipios.py")


def test_ejecutar_acepta_dry_run():
    import inspect
    params = inspect.signature(muni.ejecutar).parameters
    assert "dry_run" in params and params["dry_run"].default is False
