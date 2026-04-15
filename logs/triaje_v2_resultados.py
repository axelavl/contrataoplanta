"""Re-triajar los 13 encontrados en v2."""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, "logs")
from triaje_v3_reencontrados import analizar, WORKERS
import aiohttp


async def main():
    v2 = json.loads(
        Path("logs/variaciones_v2_resultado.json").read_text(encoding="utf-8")
    )
    entries = []
    for v in v2:
        if not v.get("candidatos"):
            continue
        entries.append({"id": v["id"], "nombre": v["nombre"], "base": v["candidatos"][0]["url"]})
    print(f"Re-triaje v2 de {len(entries)} encontrados...")
    sem = asyncio.Semaphore(WORKERS)
    conn = aiohttp.TCPConnector(limit=WORKERS, limit_per_host=2, ttl_dns_cache=300)
    results = []
    async with aiohttp.ClientSession(connector=conn) as session:
        tareas = [analizar(session, sem, e) for e in entries]
        for coro in asyncio.as_completed(tareas):
            r = await coro
            results.append(r)
    results.sort(key=lambda x: x["id"])
    Path("logs/triaje_v2_encontrados.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    for r in results:
        if r.get("ok"):
            links = r.get("links_empleo", [])
            top = links[0] if links else {"url": "(home)", "score": 0}
            print(f"  {r['id']:>3} {r['nombre'][:30]:<30} [{r.get('plataforma','?')[:15]:<15}] -> {top['url'][:50]} (s={top.get('score',0)})")


if __name__ == "__main__":
    asyncio.run(main())
