"""Aplica al JSON:
1. Los 13 re-encontrados en variaciones v2
2. Los findings_muertos (de WebSearch manual)
3. Los muertos restantes sin alternativa -> marcarlos como 'Sitio caído'
"""
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


def dificultad(plat):
    if plat == "WordPress":
        return "Media"
    if plat == "Sitio propio Joomla":
        return "Media"
    if plat in ("empleospublicos.cl", "HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
        return "Baja"
    if plat in ("SPA JavaScript", "Vue.js SPA"):
        return "Alta"
    return "Media"


def req_js(plat):
    return "Sí" if plat in ("SPA JavaScript", "Vue.js SPA") else "No"


def main():
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    by_id = {i["id"]: i for i in data["instituciones"]}
    changes = 0

    # 1. Triaje v2 re-encontrados
    v2 = json.loads(Path("logs/triaje_v2_encontrados.json").read_text(encoding="utf-8"))
    for r in v2:
        if not r.get("ok"):
            continue
        muni = by_id.get(r["id"])
        if not muni:
            continue
        plat = normalizar_plataforma(r.get("plataforma", "Sitio propio"))
        links = r.get("links_empleo", [])
        if links and links[0]["score"] >= 4:
            url_empleo = links[0]["url"]
        else:
            url_empleo = r.get("url_final") or r.get("sitio_web")
        publica_ep = muni.get("publica_en_empleospublicos", "Parcialmente")
        if plat == "empleospublicos.cl":
            publica_ep = "Sí"
        elif plat in ("HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
            publica_ep = "No"
        muni["sitio_web"] = r.get("url_final") or r.get("sitio_web")
        muni["url_empleo"] = url_empleo
        muni["plataforma_empleo"] = plat
        muni["publica_en_empleospublicos"] = publica_ep
        muni["requiere_js"] = req_js(plat)
        muni["dificultad_scraping"] = dificultad(plat)
        muni["notas_tecnicas"] = f"{plat} | dominio re-encontrado variaciones v2"[:190]
        muni["estado_verificacion"] = "Verificado"
        changes += 1

    # 2. Findings muertos (manual WebSearch)
    fm = json.loads(Path("logs/findings_muertos.json").read_text(encoding="utf-8"))
    for id_str, f in fm.items():
        muni = by_id.get(int(id_str))
        if not muni:
            continue
        plat = normalizar_plataforma(f["plataforma"])
        if f.get("sitio_web_nuevo"):
            muni["sitio_web"] = f["sitio_web_nuevo"]
        muni["url_empleo"] = f["url_empleo"]
        muni["plataforma_empleo"] = plat
        muni["requiere_js"] = req_js(plat)
        muni["dificultad_scraping"] = dificultad(plat)
        muni["notas_tecnicas"] = (f.get("notas", "") or plat)[:190]
        muni["estado_verificacion"] = "Verificado"
        if plat in ("HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
            muni["publica_en_empleospublicos"] = "No"
        elif plat == "empleospublicos.cl":
            muni["publica_en_empleospublicos"] = "Sí"
        changes += 1

    # 3. Muertos sin alternativa -> marcar como 'Sitio caído'
    # Los que siguen con estado='Por verificar' despues de todo lo anterior
    # Y estan en la lista original de muertos
    triaje = json.loads(Path("logs/triaje_munis_resultado.json").read_text(encoding="utf-8"))
    muertos_ids = {r["id"] for r in triaje if not r.get("ok")}
    sitios_caidos = 0
    for mid in muertos_ids:
        muni = by_id.get(mid)
        if not muni:
            continue
        if muni.get("estado_verificacion") == "Verificado":
            continue  # ya procesado por alguna fase
        # Marcar como sitio caido
        muni["estado_verificacion"] = "Sitio caído"
        muni["notas_tecnicas"] = (
            f"Dominio muerto: {muni.get('sitio_web','')} "
            f"(DNS o HTTP fallo; no se encontro alternativa viva). "
            f"Revisar periodicamente."
        )[:190]
        sitios_caidos += 1
        changes += 1

    # Actualizar metadata
    data["metadata"]["fecha_generacion"] = datetime.now().strftime("%Y-%m-%d")
    JSON_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Cambios aplicados: {changes}")
    print(f"  Marcados como 'Sitio caído': {sitios_caidos}")

    # Conteo final
    munis = [i for i in data["instituciones"] if i.get("sector") == "Municipal"]
    from collections import Counter
    estados = Counter(m.get("estado_verificacion") for m in munis)
    print("\nEstados municipios (final):")
    for k, v in sorted(estados.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
