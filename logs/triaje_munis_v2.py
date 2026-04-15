"""Triaje v2: sobre los vivos, extrae links candidatos de empleo y detecta
plataforma con mas precision (lee TODO el HTML, no solo 6000 chars).
"""
import asyncio
import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

TIMEOUT = 15
WORKERS = 12

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

EMPLEO_KW = [
    "concurso",
    "concursos",
    "empleo",
    "empleos",
    "trabaja",
    "trabaje",
    "trabajo",
    "bolsa",
    "vacant",
    "postul",
    "omil",
    "rrhh",
    "recursos-humanos",
    "recursos humanos",
    "carrera",
    "carreras",
    "convocatoria",
    "llamado",
    "ofertas laboral",
    "ofertas de trabajo",
    "trabaja con nosotros",
]

# dominios externos conocidos que indican plataforma
EXTERNAL_MARKERS = {
    "empleospublicos.cl": "empleospublicos.cl",
    "buk.cl": "Buk (RRHH)",
    "buk.works": "Buk (RRHH)",
    "hiringroom.com": "HiringRoom",
    "trabajando.cl": "Trabajando.cl/OMIL",
    ".omil.cl": "Trabajando.cl/OMIL",
    "reqlut": "Reqlut",
    "procit": "Procit",
}


def detectar_plataforma_html(html: str, headers: dict, links_externos: list[str]) -> dict:
    """Clasifica plataforma a partir del HTML + headers + links externos encontrados."""
    out = {}
    html_low = html.lower()

    # Primero, plataformas externas por links
    for link in links_externos:
        link_low = link.lower()
        for marker, nombre in EXTERNAL_MARKERS.items():
            if marker in link_low:
                out["plataforma"] = nombre
                out["motivo"] = f"link externo a {marker}"
                return out

    # Meta generator
    m = re.search(r'<meta\s+name=["\']generator["\']\s+content=["\']([^"\']+)', html_low)
    if m:
        gen = m.group(1)
        if "wordpress" in gen:
            out["plataforma"] = "WordPress"
            out["motivo"] = f"meta generator: {gen[:50]}"
            return out
        if "joomla" in gen:
            out["plataforma"] = "Sitio propio Joomla"
            out["motivo"] = f"meta generator: {gen[:50]}"
            return out
        if "drupal" in gen:
            out["plataforma"] = "Sitio propio"
            out["motivo"] = f"Drupal (meta gen: {gen[:50]})"
            return out

    # wp-content / wp-includes fuerte senal WP
    if "/wp-content/" in html_low or "/wp-includes/" in html_low:
        out["plataforma"] = "WordPress"
        out["motivo"] = "wp-content/wp-includes en HTML"
        return out

    # Joomla paths
    if (
        "/components/com_" in html_low
        or "/templates/" in html_low and "joomla" in html_low
        or "media/jui/" in html_low
    ):
        out["plataforma"] = "Sitio propio Joomla"
        out["motivo"] = "paths Joomla en HTML"
        return out

    # SPA detection: body con solo id=app/root + referencias a vue/react
    spa_markers = ['id="app"', "id='app'", 'id="root"']
    has_spa_el = any(m in html_low for m in spa_markers)
    has_vue = "vue.js" in html_low or "vue.min.js" in html_low or "__vue" in html_low
    has_react = "react" in html_low and "__react" in html_low
    if has_spa_el and (has_vue or has_react):
        out["plataforma"] = "SPA JavaScript"
        out["requiere_js"] = "Sí"
        out["motivo"] = "SPA Vue/React detectado"
        return out

    # PHP propio: header X-Powered-By PHP + URLs .php
    if "php" in str(headers.get("X-Powered-By", "")).lower():
        out["plataforma"] = "PHP propio"
        out["motivo"] = "X-Powered-By: PHP"
        return out

    # Default: sitio propio sin CMS detectable
    out["plataforma"] = "Sitio propio"
    out["motivo"] = "HTML estatico sin CMS detectable"
    return out


def extraer_links_empleo(html: str, base_url: str) -> list[dict]:
    """Devuelve lista de links candidatos a seccion de empleo ordenados por relevancia."""
    soup = BeautifulSoup(html, "html.parser")
    candidatos = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        texto = (a.get_text() or "").strip().lower()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        # absolutizar
        full = urljoin(base_url, href)
        href_low = full.lower()
        texto_low = texto.lower()
        score = 0
        matched = []
        for kw in EMPLEO_KW:
            if kw in texto_low:
                score += 3
                matched.append(f"texto:{kw}")
            if kw in href_low:
                score += 2
                matched.append(f"href:{kw}")
        # Bonus si es path absoluto dentro del mismo dominio
        if score > 0:
            candidatos.append(
                {
                    "url": full,
                    "texto": texto[:80],
                    "score": score,
                    "match": matched[:3],
                }
            )
    # dedup por url
    seen = set()
    out = []
    for c in sorted(candidatos, key=lambda x: -x["score"]):
        if c["url"] in seen:
            continue
        seen.add(c["url"])
        out.append(c)
    return out[:8]


async def analizar(session, sem, entry):
    url = entry["sitio_web"]
    out = {"id": entry["id"], "nombre": entry["nombre"], "sitio_web": url}
    async with sem:
        try:
            async with session.get(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                    "Accept-Language": "es-CL,es;q=0.9",
                },
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
                links = extraer_links_empleo(html, str(resp.url))
                out["links_empleo"] = links
                # links externos detectados en el HTML (iframes, src, hrefs)
                externos = set()
                for m in re.finditer(
                    r'(?:src|href)=["\']([^"\']+)', html, re.IGNORECASE
                ):
                    u = m.group(1)
                    parsed = urlparse(u)
                    host = parsed.netloc.lower()
                    if host and host != urlparse(str(resp.url)).netloc:
                        externos.add(u)
                # clasificar
                clasif = detectar_plataforma_html(html, headers, list(externos)[:200])
                out.update(clasif)
                # contar matches de keywords en todo el html
                low = html.lower()
                out["kw_hits"] = {kw: low.count(kw) for kw in ["concurso", "empleo", "trabaja con nosotros", "omil", "rrhh"]}
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
    # Solo los vivos del triaje v1
    triaje_v1 = json.loads(
        Path("logs/triaje_munis_resultado.json").read_text(encoding="utf-8")
    )
    vivos_v1 = [r for r in triaje_v1 if r.get("ok")]
    # Queue original para datos completos
    queue = json.loads(
        Path("logs/verificacion_munis_queue.json").read_text(encoding="utf-8")
    )
    by_id = {q["id"]: q for q in queue}
    entries = [by_id[v["id"]] for v in vivos_v1 if v["id"] in by_id]
    print(f"Reanalizando {len(entries)} sitios vivos con HTML completo...")
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
            if done % 25 == 0:
                print(f"  [{done}/{len(entries)}]")
    results.sort(key=lambda x: x["id"])
    Path("logs/triaje_munis_v2.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    # Breakdown
    plats = {}
    con_url_empleo = 0
    sin_url_empleo = 0
    for r in results:
        if not r.get("ok"):
            plats["[error]"] = plats.get("[error]", 0) + 1
            continue
        p = r.get("plataforma", "?")
        plats[p] = plats.get(p, 0) + 1
        if r.get("links_empleo"):
            con_url_empleo += 1
        else:
            sin_url_empleo += 1
    print("\nPlataforma detectada:")
    for k, v in sorted(plats.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print(f"\nCon link de empleo detectable en home: {con_url_empleo}")
    print(f"Sin link de empleo detectable en home: {sin_url_empleo}")


if __name__ == "__main__":
    asyncio.run(main())
