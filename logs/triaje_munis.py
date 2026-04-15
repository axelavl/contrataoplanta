"""Triaje rapido: chequea cual de los 325 municipios pendientes tiene sitio vivo.
Solo DNS + HTTP status, sin analizar HTML. El analisis de plataforma se hace despues
con WebFetch (uno por uno) sobre los sitios vivos.
"""
import asyncio
import json
import time
from pathlib import Path

import aiohttp

TIMEOUT = 12
WORKERS = 15

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


async def check(session, semaforo, entry):
    url = entry["sitio_web"]
    out = {"id": entry["id"], "nombre": entry["nombre"], "sitio_web": url}
    async with semaforo:
        try:
            t0 = time.monotonic()
            async with session.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept-Language": "es-CL,es;q=0.9"},
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                dur = int((time.monotonic() - t0) * 1000)
                out["status"] = resp.status
                out["url_final"] = str(resp.url)
                out["ms"] = dur
                out["ok"] = resp.status == 200
                # Read a small window to detect SPA / wordpress / joomla cheaply
                try:
                    text = await resp.text(encoding="utf-8", errors="ignore")
                    head = text[:6000].lower()
                    out["has_wp"] = "wp-content" in head or "wp-includes" in head
                    out["has_joomla"] = "joomla" in head
                    out["has_spa"] = (
                        ('id="app"' in head or "id='app'" in head)
                        and ("vue" in head or "react" in head or "angular" in head)
                    )
                    out["has_iframe_ep"] = "empleospublicos.cl" in head
                    out["has_iframe_buk"] = "buk.cl" in head or "buk.works" in head
                    out["has_iframe_hr"] = "hiringroom" in head
                    out["has_iframe_tr"] = "trabajando.cl" in head
                    keywords = ["concurso", "empleo", "postul", "vacante", "convocatoria"]
                    out["has_empleo_kw"] = any(k in head for k in keywords)
                except Exception as e:
                    out["body_err"] = str(e)[:60]
        except aiohttp.ClientConnectorError as e:
            out["status"] = None
            out["ok"] = False
            out["error"] = "dns/conn: " + str(e)[:80]
        except asyncio.TimeoutError:
            out["status"] = None
            out["ok"] = False
            out["error"] = f"timeout {TIMEOUT}s"
        except Exception as e:
            out["status"] = None
            out["ok"] = False
            out["error"] = type(e).__name__ + ": " + str(e)[:80]
    return out


async def main():
    queue = json.loads(Path("logs/verificacion_munis_queue.json").read_text(encoding="utf-8"))
    print(f"Chequeando {len(queue)} municipios...")
    sem = asyncio.Semaphore(WORKERS)
    conn = aiohttp.TCPConnector(limit=WORKERS, limit_per_host=2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        resultados = []
        tareas = [check(session, sem, e) for e in queue]
        done = 0
        for coro in asyncio.as_completed(tareas):
            r = await coro
            resultados.append(r)
            done += 1
            if done % 25 == 0:
                print(f"  [{done}/{len(queue)}] ok={sum(1 for x in resultados if x.get('ok'))}")
    # orden por id
    resultados.sort(key=lambda x: x["id"])
    Path("logs/triaje_munis_resultado.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    vivos = [r for r in resultados if r.get("ok")]
    muertos = [r for r in resultados if not r.get("ok")]
    print(f"\nVivos (200): {len(vivos)}")
    print(f"Muertos/error: {len(muertos)}")
    print(f"\nMuertos (primeros 20):")
    for m in muertos[:20]:
        print(f"  {m['id']:>3} {m['nombre']:<40} | {m.get('error') or m.get('status')}")


if __name__ == "__main__":
    asyncio.run(main())
