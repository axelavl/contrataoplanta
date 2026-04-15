"""Aplica los findings recolectados via WebSearch (fase 2) al JSON maestro."""
import json
from datetime import datetime
from pathlib import Path

JSON_PATH = Path("repositorio_instituciones_publicas_chile.json")

def normalizar_plataforma(p: str) -> str:
    mapping = {
        "WordPress": "WordPress",
        "Sitio propio Joomla": "Sitio propio Joomla",
        "Sitio propio": "Sitio propio",
        "SPA JavaScript": "SPA JavaScript",
        "PHP propio": "PHP propio",
        "HiringRoom": "HiringRoom",
        "Buk (RRHH)": "Buk (RRHH)",
        "Trabajando.cl/OMIL": "Trabajando.cl/OMIL",
        "empleospublicos.cl": "empleospublicos.cl",
    }
    return mapping.get(p, "Sitio propio")


def dificultad_desde_plataforma(plat: str) -> str:
    if plat == "WordPress":
        return "Media"
    if plat == "Sitio propio Joomla":
        return "Media"
    if plat in ("empleospublicos.cl",):
        return "Baja"
    if plat in ("HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
        return "Baja"
    if plat in ("SPA JavaScript", "Vue.js SPA"):
        return "Alta"
    return "Media"


def requiere_js_desde_plataforma(plat: str) -> str:
    return "Sí" if plat in ("SPA JavaScript", "Vue.js SPA") else "No"


def main():
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    by_id = {i["id"]: i for i in data["instituciones"]}
    findings = json.loads(Path("logs/findings_ambiguos.json").read_text(encoding="utf-8"))

    aplicados = 0
    por_plat = {}
    for id_str, f in findings.items():
        muni = by_id.get(int(id_str))
        if not muni:
            continue
        plat = normalizar_plataforma(f["plataforma"])
        if f.get("sitio_web_nuevo"):
            muni["sitio_web"] = f["sitio_web_nuevo"]
        muni["url_empleo"] = f["url_empleo"]
        muni["plataforma_empleo"] = plat
        muni["requiere_js"] = requiere_js_desde_plataforma(plat)
        muni["dificultad_scraping"] = dificultad_desde_plataforma(plat)
        muni["notas_tecnicas"] = (f.get("notas", "") or plat)[:190]
        muni["estado_verificacion"] = f.get("estado", "Verificado")
        if plat in ("HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
            muni["publica_en_empleospublicos"] = "No"
        elif plat == "empleospublicos.cl":
            muni["publica_en_empleospublicos"] = "Sí"
        aplicados += 1
        por_plat[plat] = por_plat.get(plat, 0) + 1

    data["metadata"]["fecha_generacion"] = datetime.now().strftime("%Y-%m-%d")
    JSON_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Findings aplicados: {aplicados}")
    print("Por plataforma:")
    for k, v in sorted(por_plat.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
