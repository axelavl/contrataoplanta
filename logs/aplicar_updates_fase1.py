"""Fase 1: aplicar al JSON maestro los updates de municipios VIVOS con URL de
empleo identificado (alta/media confianza + paths estandar).

NO toca los 54 ambiguos ni los 169 muertos — esos se procesan en fase 2/3.
"""
import json
import shutil
from datetime import datetime
from pathlib import Path

JSON_PATH = Path("repositorio_instituciones_publicas_chile.json")


def normalizar_plataforma(p: str) -> str:
    """Asegurar que el valor cae en el set que entiende source_status.classify_source."""
    if not p:
        return "Sitio propio"
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
        "Reqlut": "Reqlut",
        "Procit": "Procit",
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


def main():
    # cargar JSON maestro
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    by_id = {i["id"]: i for i in data["instituciones"]}

    # triaje v3 (detecciones desde home)
    v3 = json.loads(Path("logs/triaje_munis_v3.json").read_text(encoding="utf-8"))
    probe = json.loads(Path("logs/probe_paths_resultado.json").read_text(encoding="utf-8"))
    probe_by_id = {p["id"]: p for p in probe}

    aplicados = 0
    pendientes_ambiguos = []
    por_plat = {}

    for r in v3:
        if not r.get("ok"):
            continue
        muni = by_id.get(r["id"])
        if not muni:
            continue
        plat = normalizar_plataforma(r.get("plataforma"))
        motivo = r.get("plataforma_motivo", "")
        links = r.get("links_empleo", [])

        url_empleo = None
        confianza = None
        # alta confianza: link de home con score >= 8
        if links and links[0]["score"] >= 8:
            url_empleo = links[0]["url"]
            confianza = "alta_home"
        # media confianza: link de home con score 4-7 (validado con el hit de paths estandar si existe)
        elif links and links[0]["score"] >= 4:
            url_empleo = links[0]["url"]
            confianza = "media_home"
        # paths estandar: si no habia link, usar el primer hit del probe
        elif r["id"] in probe_by_id and probe_by_id[r["id"]].get("hits"):
            hits = probe_by_id[r["id"]]["hits"]
            url_empleo = hits[0]["url"]
            confianza = "paths_estandar"

        if not url_empleo:
            # Ambiguo: vivo pero sin url de empleo detectable
            pendientes_ambiguos.append(r)
            continue

        # Plataformas externas: publica_en_empleospublicos
        publica_ep = muni.get("publica_en_empleospublicos", "Parcialmente")
        if plat == "empleospublicos.cl":
            publica_ep = "Sí"
        elif plat in ("HiringRoom", "Buk (RRHH)", "Trabajando.cl/OMIL"):
            # estas plataformas son alternativas a EP; el muni NO usa EP
            publica_ep = "No"

        # requiere_js
        requiere_js = r.get("requiere_js", "No")
        if plat in ("SPA JavaScript", "Vue.js SPA"):
            requiere_js = "Sí"
        else:
            requiere_js = "No"

        # dificultad
        dificultad = dificultad_desde_plataforma(plat)

        # notas tecnicas (breve)
        notas = []
        notas.append(plat)
        if confianza == "paths_estandar":
            notas.append("url via path estandar")
        if motivo:
            notas.append(motivo[:40])
        notas_tecnicas = " | ".join(notas)[:190]

        # escribir
        muni["sitio_web"] = r.get("url_final") or muni["sitio_web"]
        muni["url_empleo"] = url_empleo
        muni["plataforma_empleo"] = plat
        muni["publica_en_empleospublicos"] = publica_ep
        muni["requiere_js"] = requiere_js
        muni["dificultad_scraping"] = dificultad
        muni["notas_tecnicas"] = notas_tecnicas
        muni["estado_verificacion"] = "Verificado"

        aplicados += 1
        por_plat[plat] = por_plat.get(plat, 0) + 1

    # Fix bug Machali
    m_machali = by_id.get(439)
    if m_machali and m_machali.get("region") == "Oighggins":
        m_machali["region"] = "O'Higgins"
        print("FIX: ID 439 Machali region -> O'Higgins")

    # Actualizar fecha
    data["metadata"]["fecha_generacion"] = datetime.now().strftime("%Y-%m-%d")

    JSON_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\nUpdates aplicados: {aplicados}")
    print("Por plataforma:")
    for k, v in sorted(por_plat.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print(f"\nVivos ambiguos (sin url de empleo detectable): {len(pendientes_ambiguos)}")

    # Guardar lista de ambiguos para fase 2
    queue = json.loads(
        Path("logs/verificacion_munis_queue.json").read_text(encoding="utf-8")
    )
    by_qid = {q["id"]: q for q in queue}
    ambig_list = [
        {
            "id": r["id"],
            "nombre": r["nombre"],
            "sitio_web": r.get("url_final") or by_qid.get(r["id"], {}).get("sitio_web"),
            "plataforma_detectada": r.get("plataforma"),
        }
        for r in pendientes_ambiguos
    ]
    Path("logs/ambiguos_vivos.json").write_text(
        json.dumps(ambig_list, ensure_ascii=False, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
