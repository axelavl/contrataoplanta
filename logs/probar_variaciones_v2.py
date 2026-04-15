"""Variaciones v2 con mas patrones (im{X}.cl, .gob.cl, com, etc.)."""
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


def slugs(nombre: str) -> list[str]:
    n = nombre.lower()
    n = re.sub(r"municipalidad\s+de\s+", "", n)
    n = re.sub(r"municipalidad\s+", "", n)
    n = re.sub(r"corporaci[oó]n\s+municipal\s+de\s+", "", n)
    for prefix in ("la ", "el ", "los ", "las "):
        if n.startswith(prefix):
            # versiones con y sin articulo
            sin_art = n[len(prefix):]
            break
    else:
        sin_art = n
    # normalize accents
    ac = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n", "ü": "u"}
    for k, v in ac.items():
        n = n.replace(k, v)
        sin_art = sin_art.replace(k, v)
    s1 = re.sub(r"[^a-z0-9]", "", n)
    s2 = re.sub(r"[^a-z0-9]", "", sin_art)
    s3 = re.sub(r"[^a-z0-9]", "-", n).strip("-")
    return list(dict.fromkeys([s1, s2, s3]))  # dedup preservando orden


def variaciones(nombre: str) -> list[str]:
    out = set()
    for s in slugs(nombre):
        if not s:
            continue
        for prefix in ("www.", ""):
            for tld in (".cl", ".gob.cl", ".com"):
                out.add(f"https://{prefix}{s}{tld}")
                out.add(f"https://{prefix}muni{s}{tld}")
                out.add(f"https://{prefix}im{s}{tld}")
                out.add(f"https://{prefix}i{s}{tld}")
                out.add(f"https://{prefix}municipalidad{s}{tld}")
                out.add(f"https://{prefix}municipalidadde{s}{tld}")
                out.add(f"https://{prefix}comuna{s}{tld}")
                out.add(f"https://{prefix}comunade{s}{tld}")
                out.add(f"https://{prefix}municipio{s}{tld}")
    # http fallback para los principales
    for url in list(out):
        out.add(url.replace("https://", "http://"))
    return list(out)


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
                    low = text[:5000].lower()
                    return {
                        "url": url,
                        "url_final": str(resp.url),
                        "status": 200,
                        "has_muni": any(
                            k in low
                            for k in ["municipalidad", "alcalde", "comuna", "ilustre"]
                        ),
                    }
                return {"url": url, "status": resp.status}
        except Exception as e:
            return {"url": url, "error": str(e)[:60]}


async def main():
    variaciones_v1 = json.loads(
        Path("logs/variaciones_dominio_resultado.json").read_text(encoding="utf-8")
    )
    findings_m = json.loads(Path("logs/findings_muertos.json").read_text(encoding="utf-8"))
    procesados = set(int(k) for k in findings_m.keys())

    # los que siguen sin alternativa y sin finding
    sin_alt = [
        v for v in variaciones_v1
        if not v.get("candidatos") and v["id"] not in procesados
    ]
    print(f"Procesando {len(sin_alt)} muertos pendientes")

    sem = asyncio.Semaphore(WORKERS)
    conn = aiohttp.TCPConnector(limit=WORKERS, limit_per_host=2, ttl_dns_cache=300)

    results = {}
    async with aiohttp.ClientSession(connector=conn) as session:
        tareas = []
        for v in sin_alt:
            vars_list = variaciones(v["nombre"])
            for url in vars_list:
                tareas.append((v["id"], url, probar_url(session, sem, url)))
        print(f"Total requests: {len(tareas)}")
        BATCH = 150
        done = 0
        for i in range(0, len(tareas), BATCH):
            grupo = tareas[i : i + BATCH]
            coros = [c for _, _, c in grupo]
            meta = [(id_, url) for id_, url, _ in grupo]
            res = await asyncio.gather(*coros, return_exceptions=True)
            for (id_, url), r in zip(meta, res):
                if isinstance(r, Exception):
                    continue
                if r.get("status") == 200 and r.get("has_muni"):
                    results.setdefault(id_, []).append(r)
            done += len(grupo)
            print(f"  [{done}/{len(tareas)}] con hit: {len(results)}")

    out = []
    for v in sin_alt:
        out.append(
            {
                "id": v["id"],
                "nombre": v["nombre"],
                "sitio_web_original": v["sitio_web_original"],
                "candidatos": results.get(v["id"], [])[:3],
            }
        )
    Path("logs/variaciones_v2_resultado.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    con_hit = [x for x in out if x["candidatos"]]
    sin_hit = [x for x in out if not x["candidatos"]]
    print(f"\nCon hit v2: {len(con_hit)}")
    print(f"Sin hit v2: {len(sin_hit)}")
    for x in con_hit[:20]:
        c = x["candidatos"][0]
        print(f"  {x['id']:>3} {x['nombre'][:32]:<32} | {x['sitio_web_original'][:30]:<30} -> {c['url'][:45]}")


if __name__ == "__main__":
    asyncio.run(main())
