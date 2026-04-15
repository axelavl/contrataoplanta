"""Aplica al JSON maestro los 103 munis re-encontrados con dominio alternativo."""
import json
from datetime import datetime
from pathlib import Path

JSON_PATH = Path("repositorio_instituciones_publicas_chile.json")


def normalizar_plataforma(p):
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


def dificultad_desde_plataforma(plat):
    if plat == "WordPress":
        return "Media"
    if plat == "Sitio propio Joomla":
        return "Media"
    if plat == "empleospublicos.cl":
        return "Baja"
    if plat in ("HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
        return "Baja"
    if plat in ("SPA JavaScript", "Vue.js SPA"):
        return "Alta"
    return "Media"


def main():
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    by_id = {i["id"]: i for i in data["instituciones"]}
    re_triaje = json.loads(
        Path("logs/triaje_reencontrados_v3.json").read_text(encoding="utf-8")
    )

    aplicados = 0
    por_plat = {}
    sin_link_count = 0
    for r in re_triaje:
        if not r.get("ok"):
            continue
        muni = by_id.get(r["id"])
        if not muni:
            continue
        plat = normalizar_plataforma(r.get("plataforma", "Sitio propio"))
        # url_empleo: link alta>=8, media>=4, si no homepage
        links = r.get("links_empleo", [])
        if links and links[0]["score"] >= 4:
            url_empleo = links[0]["url"]
            nota_link = "link home"
        else:
            url_empleo = r.get("url_final") or r.get("sitio_web")
            nota_link = "sin URL dedicada - usar home"
            sin_link_count += 1

        # Publica en empleospublicos
        publica_ep = muni.get("publica_en_empleospublicos", "Parcialmente")
        if plat == "empleospublicos.cl":
            publica_ep = "Sí"
        elif plat in ("HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
            publica_ep = "No"

        # requiere_js
        requiere_js = (
            "Sí" if plat in ("SPA JavaScript", "Vue.js SPA") else "No"
        )

        nota = (
            f"{plat} | dominio corregido {r.get('sitio_web','')[:50]} | {nota_link}"
        )[:190]

        muni["sitio_web"] = r.get("url_final") or r.get("sitio_web")
        muni["url_empleo"] = url_empleo
        muni["plataforma_empleo"] = plat
        muni["publica_en_empleospublicos"] = publica_ep
        muni["requiere_js"] = requiere_js
        muni["dificultad_scraping"] = dificultad_desde_plataforma(plat)
        muni["notas_tecnicas"] = nota
        muni["estado_verificacion"] = "Verificado"
        aplicados += 1
        por_plat[plat] = por_plat.get(plat, 0) + 1

    data["metadata"]["fecha_generacion"] = datetime.now().strftime("%Y-%m-%d")
    JSON_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Re-encontrados aplicados: {aplicados}")
    print(f"  (de los cuales {sin_link_count} sin URL dedicada -> home)")
    print("Por plataforma:")
    for k, v in sorted(por_plat.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
