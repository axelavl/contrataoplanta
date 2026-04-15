"""Re-analizar los 103 dominios re-encontrados por variaciones, usando la
misma logica del triaje v3 (scoring estricto + plataformas externas)."""
import asyncio
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

# Reusar la logica - importa del v3
import sys
sys.path.insert(0, "logs")
from triaje_munis_v3 import (
    score_link,
    detectar_plataforma_externa,
    detectar_plataforma_html,
    UA,
    TIMEOUT,
    WORKERS,
)


async def analizar(session, sem, entry):
    url = entry["base"]
    out = {"id": entry["id"], "nombre": entry["nombre"], "sitio_web": url}
    async with sem:
        try:
            async with session.get(
                url,
                headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"},
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                out["status"] = resp.status
                out["url_final"] = str(resp.url)
                if resp.status != 200:
                    out["ok"] = False
                    out["error"] = f"HTTP {resp.status}"
                    return out
                out["ok"] = True
                html = await resp.text(encoding="utf-8", errors="ignore")
                headers = dict(resp.headers)
                soup = BeautifulSoup(html, "html.parser")
                all_hrefs = []
                candidatos = []
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if not href or href.startswith("#") or href.startswith("javascript:"):
                        continue
                    full = urljoin(str(resp.url), href)
                    all_hrefs.append(full)
                    texto = " ".join((a.get_text() or "").split())
                    score, matched = score_link(texto, full)
                    if score >= 4:
                        candidatos.append(
                            {"url": full, "texto": texto[:120], "score": score, "match": matched[:4]}
                        )
                for iframe in soup.find_all("iframe", src=True):
                    all_hrefs.append(urljoin(str(resp.url), iframe["src"]))

                ext = detectar_plataforma_externa(all_hrefs)
                if ext:
                    out["plataforma"] = ext["plataforma"]
                    out["plataforma_motivo"] = f"link a {ext['link_externo'][:80]}"
                    if ext["es_ep"]:
                        out["publica_en_empleospublicos"] = "Sí"
                else:
                    out.update(detectar_plataforma_html(html, headers))

                seen = set()
                uniq = []
                for c in sorted(candidatos, key=lambda x: -x["score"]):
                    if c["url"] in seen:
                        continue
                    seen.add(c["url"])
                    uniq.append(c)
                out["links_empleo"] = uniq[:5]
        except asyncio.TimeoutError:
            out["ok"] = False
            out["error"] = f"timeout {TIMEOUT}s"
        except aiohttp.ClientConnectorError as e:
            out["ok"] = False
            out["error"] = "dns/conn: " + str(e)[:80]
        except Exception as e:
            out["ok"] = False
            out["error"] = type(e).__name__ + ": " + str(e)[:80]
    return out


async def main():
    variaciones = json.loads(
        Path("logs/variaciones_dominio_resultado.json").read_text(encoding="utf-8")
    )
    entries = []
    for v in variaciones:
        if not v.get("candidatos"):
            continue
        mejor = v["candidatos"][0]  # primer hit
        entries.append({"id": v["id"], "nombre": v["nombre"], "base": mejor["url"]})

    print(f"Re-triaje de {len(entries)} dominios re-encontrados...")
    sem = asyncio.Semaphore(WORKERS)
    conn = aiohttp.TCPConnector(limit=WORKERS, limit_per_host=2, ttl_dns_cache=300)
    results = []
    async with aiohttp.ClientSession(connector=conn) as session:
        tareas = [analizar(session, sem, e) for e in entries]
        done = 0
        for coro in asyncio.as_completed(tareas):
            r = await coro
            results.append(r)
            done += 1
            if done % 20 == 0:
                print(f"  [{done}/{len(entries)}]")
    results.sort(key=lambda x: x["id"])
    Path("logs/triaje_reencontrados_v3.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    plats = {}
    con_link_alta = 0
    con_link_media = 0
    sin_link = 0
    for r in results:
        if not r.get("ok"):
            plats["[error]"] = plats.get("[error]", 0) + 1
            continue
        p = r.get("plataforma", "?")
        plats[p] = plats.get(p, 0) + 1
        links = r.get("links_empleo", [])
        if links and links[0]["score"] >= 8:
            con_link_alta += 1
        elif links and links[0]["score"] >= 4:
            con_link_media += 1
        else:
            sin_link += 1
    print("\nPlataforma:")
    for k, v in sorted(plats.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print(f"\nCon link alta: {con_link_alta}")
    print(f"Con link media: {con_link_media}")
    print(f"Sin link: {sin_link}")


if __name__ == "__main__":
    asyncio.run(main())
