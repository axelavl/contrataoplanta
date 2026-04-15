"""Prueba variaciones comunes de dominio para los 169 munis muertos.
Patrones:
- www.muniX.cl  -> muniX.cl (sin www)
- www.muniX.cl  -> www.X.cl
- www.muniX.cl  -> www.municipalidadX.cl
- www.muniX.cl  -> www.municipalidaddeX.cl
- http in vez de https
- typo fixes conocidos
"""
import asyncio
import json
import re
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

TIMEOUT = 10
WORKERS = 15
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


def generar_variaciones(url_original: str, nombre_muni: str) -> list[str]:
    """Genera candidatos de dominios alternativos."""
    p = urlparse(url_original)
    host = p.netloc.lower()
    # quitar www.
    bare = host.replace("www.", "", 1)

    # extraer slug del nombre: "Municipalidad de Alto Hospicio" -> "altohospicio"
    nombre_clean = nombre_muni.lower()
    nombre_clean = re.sub(r"municipalidad\s+de\s+", "", nombre_clean)
    nombre_clean = re.sub(r"^la\s+", "", nombre_clean)
    nombre_clean = re.sub(r"^el\s+", "", nombre_clean)
    nombre_clean = re.sub(r"^los\s+", "", nombre_clean)
    nombre_clean = re.sub(r"^las\s+", "", nombre_clean)
    # Normalizar acentos y caracteres especiales
    acentos = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        "ü": "u", "Á": "a", "É": "e", "Í": "i", "Ó": "o", "Ú": "u",
    }
    for k, v in acentos.items():
        nombre_clean = nombre_clean.replace(k, v)
    slug = re.sub(r"[^a-z0-9]", "", nombre_clean)
    slug_con_guion = re.sub(r"[^a-z0-9]", "-", nombre_clean).strip("-")

    variaciones = set()
    # Base: el dominio original sin www (porque ya probamos con www)
    variaciones.add(f"https://{bare}")
    variaciones.add(f"http://{bare}")

    # Slug directo
    for dom in [slug, slug_con_guion]:
        if dom:
            for prefix in ["www.", ""]:
                variaciones.add(f"https://{prefix}{dom}.cl")
                variaciones.add(f"https://{prefix}{dom}.gob.cl")
                variaciones.add(f"https://{prefix}municipalidad{dom}.cl")
                variaciones.add(f"https://{prefix}municipalidadde{dom}.cl")
                variaciones.add(f"https://{prefix}muni{dom}.cl")
                variaciones.add(f"https://{prefix}i{dom}.cl")
                variaciones.add(f"http://{prefix}{dom}.cl")

    # Quitar del conjunto la URL original que ya sabemos muerta
    variaciones.discard(url_original)
    variaciones.discard(url_original.replace("https://", "http://"))
    return list(variaciones)


async def probar_url(session, sem, url):
    async with sem:
        try:
            async with session.get(
                url,
                headers={"User-Agent": UA},
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    text = await resp.text(encoding="utf-8", errors="ignore")
                    low = text[:4000].lower()
                    # heuristic: the returned page mentions the municipality keywords
                    return {
                        "url": url,
                        "url_final": str(resp.url),
                        "status": 200,
                        "has_muni": any(
                            k in low for k in ["municipalidad", "alcalde", "comuna", "ilustre"]
                        ),
                    }
                else:
                    return {"url": url, "status": resp.status}
        except Exception as e:
            return {"url": url, "error": str(e)[:60]}


async def main():
    triaje = json.loads(
        Path("logs/triaje_munis_resultado.json").read_text(encoding="utf-8")
    )
    muertos = [r for r in triaje if not r.get("ok")]
    queue = json.loads(
        Path("logs/verificacion_munis_queue.json").read_text(encoding="utf-8")
    )
    by_id = {q["id"]: q for q in queue}

    targets = []
    for m in muertos:
        q = by_id.get(m["id"])
        if not q:
            continue
        vars_list = generar_variaciones(m["sitio_web"], m["nombre"])
        targets.append(
            {
                "id": m["id"],
                "nombre": m["nombre"],
                "sitio_web": m["sitio_web"],
                "variaciones": vars_list,
            }
        )

    print(
        f"Probando variaciones para {len(targets)} muertos "
        f"(~{sum(len(t['variaciones']) for t in targets)} URLs total)"
    )
    sem = asyncio.Semaphore(WORKERS)
    conn = aiohttp.TCPConnector(limit=WORKERS, limit_per_host=2, ttl_dns_cache=300)

    results = {}
    async with aiohttp.ClientSession(connector=conn) as session:
        # correr todas las variaciones en paralelo, agrupar por id
        tareas = []
        for t in targets:
            for v in t["variaciones"]:
                tareas.append((t["id"], probar_url(session, sem, v)))
        print(f"Total requests: {len(tareas)}")
        done = 0
        batch = 200
        for i in range(0, len(tareas), batch):
            grupo = tareas[i : i + batch]
            coros = [r for _, r in grupo]
            ids = [i_ for i_, _ in grupo]
            res = await asyncio.gather(*coros, return_exceptions=True)
            for id_muni, r in zip(ids, res):
                if isinstance(r, Exception):
                    continue
                if r.get("status") == 200 and r.get("has_muni"):
                    results.setdefault(id_muni, []).append(r)
            done += len(grupo)
            print(f"  [{done}/{len(tareas)}] con hit: {len(results)}")

    # Pick best variation per id
    out = []
    for t in targets:
        found = results.get(t["id"], [])
        out.append(
            {
                "id": t["id"],
                "nombre": t["nombre"],
                "sitio_web_original": t["sitio_web"],
                "candidatos": found[:3],
            }
        )
    Path("logs/variaciones_dominio_resultado.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    con_hit = [r for r in out if r["candidatos"]]
    sin_hit = [r for r in out if not r["candidatos"]]
    print(f"\nCon alternativa viva: {len(con_hit)}")
    print(f"Sin alternativa (WebSearch necesario): {len(sin_hit)}")
    print("\nCon hit (primeros 20):")
    for r in con_hit[:20]:
        c = r["candidatos"][0]
        print(f"  {r['id']:>3} {r['nombre'][:35]:<35} | {r['sitio_web_original'][:35]:<35} -> {c['url'][:45]}")


if __name__ == "__main__":
    asyncio.run(main())
