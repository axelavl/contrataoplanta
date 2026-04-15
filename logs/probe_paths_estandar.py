"""Para sitios vivos sin link de empleo en la home, prueba paths estandar.
Tambien reintenta los timeouts.
"""
import asyncio
import json
from pathlib import Path
from urllib.parse import urljoin

import aiohttp

TIMEOUT = 12
WORKERS = 10

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

# Paths comunes en munis chilenos para sección de empleo
PATHS_ESTANDAR = [
    # Wordpress permalinks
    "/concursos-publicos/",
    "/concursos/",
    "/concurso-publicos/",
    "/empleo/",
    "/empleos/",
    "/trabaja-con-nosotros/",
    "/trabaja/",
    "/ofertas-laborales/",
    "/oferta-laboral/",
    "/ofertas-de-empleo/",
    "/recursos-humanos/",
    "/departamento-de-recursos-humanos/",
    "/rrhh/",
    "/omil/",
    "/departamento-omil/",
    "/bolsa-de-empleo/",
    # Wordpress categorias
    "/category/concursos-publicos/",
    "/category/concursos/",
    "/category/rrhh/",
    # Joomla
    "/index.php/concursos-publicos",
    "/index.php/concursos",
    "/index.php/empleos",
    "/index.php/trabaja-con-nosotros",
    # Joomla item (anchor)
    "/concursos-publicos",
    "/concursos",
    # Genericos
    "/servicios/empleo",
    "/servicios/omil",
]


async def probe_one(session, sem, base_url):
    """Prueba los paths estandar y devuelve el primero que responda 200 con contenido relevante."""
    hits = []
    async with sem:
        for path in PATHS_ESTANDAR:
            url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
            try:
                async with session.get(
                    url,
                    headers={"User-Agent": UA, "Accept": "text/html,*/*"},
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                    allow_redirects=True,
                    ssl=False,
                ) as resp:
                    if resp.status != 200:
                        continue
                    html = await resp.text(encoding="utf-8", errors="ignore")
                    low = html[:10000].lower()
                    # heuristica: debe tener palabras de empleo en el contenido
                    kw_count = sum(
                        low.count(k)
                        for k in [
                            "concurso",
                            "empleo",
                            "postul",
                            "vacante",
                            "convocatoria",
                            "ofert",
                            "recursos humanos",
                            "omil",
                            "trabaja",
                        ]
                    )
                    if kw_count >= 2:
                        hits.append(
                            {
                                "url": str(resp.url),
                                "path": path,
                                "kw_hits": kw_count,
                                "len": len(html),
                            }
                        )
                        # Si tenemos un hit muy fuerte, salimos
                        if kw_count >= 10:
                            break
            except asyncio.TimeoutError:
                continue
            except aiohttp.ClientError:
                continue
            except Exception:
                continue
    # orden por kw_hits descendente
    hits.sort(key=lambda x: -x["kw_hits"])
    return hits[:3]


async def main():
    v3 = json.loads(Path("logs/triaje_munis_v3.json").read_text(encoding="utf-8"))
    # Los 65 sin link + los 41 medios (para validar que el candidato es real)
    sin_link = [
        r
        for r in v3
        if r.get("ok")
        and (not r.get("links_empleo") or r["links_empleo"][0]["score"] < 4)
    ]
    timeouts = [r for r in v3 if not r.get("ok") and "timeout" in str(r.get("error", ""))]
    queue = json.loads(
        Path("logs/verificacion_munis_queue.json").read_text(encoding="utf-8")
    )
    by_id = {q["id"]: q for q in queue}

    targets = []
    for r in sin_link:
        entry = by_id.get(r["id"])
        if entry:
            # usar url_final del triaje si existe, si no sitio_web original
            base = r.get("url_final") or entry["sitio_web"]
            targets.append({"id": r["id"], "nombre": r["nombre"], "base": base})
    # reintentar timeouts
    for r in timeouts:
        entry = by_id.get(r["id"])
        if entry:
            targets.append(
                {"id": r["id"], "nombre": r["nombre"], "base": entry["sitio_web"]}
            )

    print(f"Probando paths estandar sobre {len(targets)} sitios...")
    sem = asyncio.Semaphore(WORKERS)
    conn = aiohttp.TCPConnector(limit=WORKERS, limit_per_host=2, ttl_dns_cache=300)
    results = []
    async with aiohttp.ClientSession(connector=conn) as session:
        tareas = [probe_one(session, sem, t["base"]) for t in targets]
        done = 0
        for i, coro in enumerate(asyncio.as_completed(tareas)):
            hits = await coro
            # No tenemos referencia directa al target, pero as_completed preserva orden interno — usaremos gather entonces
            done += 1
        # Re-run as gather para tener pares
        results_gather = await asyncio.gather(
            *[probe_one(session, sem, t["base"]) for t in targets]
        )

    for t, hits in zip(targets, results_gather):
        results.append({**t, "hits": hits})

    Path("logs/probe_paths_resultado.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    con_hit = [r for r in results if r.get("hits")]
    sin_hit = [r for r in results if not r.get("hits")]
    print(f"\nCon hit en paths estandar: {len(con_hit)}")
    print(f"Sin hit en paths estandar:  {len(sin_hit)}")
    print("\nCon hit (primeros 15):")
    for r in con_hit[:15]:
        h = r["hits"][0]
        print(
            f"  {r['id']:>3} {r['nombre'][:35]:<35} | {h['path']:<30} kw={h['kw_hits']}"
        )
    print("\nSin hit (primeros 15):")
    for r in sin_hit[:15]:
        print(f"  {r['id']:>3} {r['nombre'][:35]:<35} | {r['base']}")


if __name__ == "__main__":
    asyncio.run(main())
