"""Triaje v3: scoring estricto + deteccion de plataformas externas en TODO el HTML.
Salida: para cada sitio vivo, plataforma clasificada y mejor candidato a url_empleo.
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

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

# keywords fuertes (score +5 en texto, +4 en href)
KW_FUERTES_TEXTO = [
    "concurso publico", "concursos publicos", "concursos públicos", "concurso público",
    "trabaja con nosotros", "bolsa de empleo", "bolsa nacional de empleo",
    "bolsa de trabajo", "ofertas laborales", "ofertas de empleo", "ofertas de trabajo",
    "recursos humanos", "departamento de personal", "convocatorias publicas",
    "postulaciones abiertas",
]

# keywords medias (score +3 en texto/href)
KW_MEDIAS_TEXTO = [
    "concursos", "concurso", "empleos", "empleo", "trabajo", "omil",
    "postulaciones", "convocatoria", "vacantes",
]

# keywords fuertes en href path
KW_FUERTES_HREF = [
    "concursos-publicos", "concurso-publico", "concursos_publicos",
    "concursos-municipales", "trabaja-con-nosotros", "trabajaconnosotros",
    "recursos-humanos", "recursos_humanos", "rrhh",
    "ofertas-laborales", "ofertas-empleo", "bolsa-empleo", "bolsa-de-empleo",
    "empleos-publicos", "concurso_publicos", "/empleos/", "/empleo/",
    "/concursos/", "/concurso/", "/omil/", "omil-",
]

KW_MEDIAS_HREF = [
    "concurs", "empleo", "trabaja", "omil", "postul", "convocatoria",
    "vacant", "carrera", "rrhh", "recursos",
]

# penalizaciones
PATH_FECHA = re.compile(r"/20\d{2}/\d{1,2}(?:/\d{1,2})?/")  # /2025/12/01/
PATH_NOTICIA = re.compile(r"/noticias?/")

# plataformas externas detectables por substring en cualquier link.
# NO incluir portaltransparencia.cl ni bne.cl: aparecen por obligacion legal
# en casi todos los sitios de gobierno chileno y son falsos positivos.
EXTERNAL_PLATFORM_MARKERS = [
    (r"empleospublicos\.cl", "empleospublicos.cl", True),   # 3er flag: publica_en_empleospublicos=Si
    (r"hiringroomcampus\.com", "HiringRoom", False),
    (r"\.hiringroom\.com", "HiringRoom", False),
    (r"\.buk\.cl", "Buk (RRHH)", False),
    (r"buk\.works", "Buk (RRHH)", False),
    (r"trabajando\.cl", "Trabajando.cl/OMIL", False),
    (r"\.omil\.cl", "Trabajando.cl/OMIL", False),
    (r"reqlut", "Reqlut", False),
    (r"procit", "Procit", False),
]


def score_link(texto: str, href: str) -> tuple[int, list[str]]:
    """Calcula score positivo/negativo de un link candidato a seccion de empleo."""
    texto_low = texto.lower().strip()
    href_low = href.lower().strip()
    score = 0
    matched = []

    for kw in KW_FUERTES_TEXTO:
        if kw in texto_low:
            score += 5
            matched.append(f"T+{kw}")
    for kw in KW_FUERTES_HREF:
        if kw in href_low:
            score += 4
            matched.append(f"H+{kw}")
    # medias solo si no match fuerte
    if score < 4:
        for kw in KW_MEDIAS_TEXTO:
            if kw in texto_low:
                score += 3
                matched.append(f"t+{kw}")
        for kw in KW_MEDIAS_HREF:
            if kw in href_low:
                score += 2
                matched.append(f"h+{kw}")

    # penalizaciones
    if PATH_FECHA.search(href_low):
        score -= 10
        matched.append("-fecha")
    if PATH_NOTICIA.search(href_low):
        score -= 5
        matched.append("-noticia")
    if href_low.endswith(".pdf"):
        score -= 3
        matched.append("-pdf")
    # texto muy largo = probablemente titular de noticia
    if len(texto) > 100:
        score -= 3
        matched.append("-texto_largo")

    return score, matched


def detectar_plataforma_externa(all_links: list[str]) -> dict | None:
    """Retorna plataforma si algun link apunta a una plataforma externa conocida."""
    for href in all_links:
        low = href.lower()
        for pattern, nombre, es_ep in EXTERNAL_PLATFORM_MARKERS:
            if re.search(pattern, low):
                return {"plataforma": nombre, "link_externo": href, "es_ep": es_ep}
    return None


def detectar_plataforma_html(html: str, headers: dict) -> dict:
    html_low = html.lower()
    m = re.search(
        r'<meta\s+name=["\']generator["\']\s+content=["\']([^"\']+)', html_low
    )
    if m:
        gen = m.group(1)
        if "wordpress" in gen:
            return {"plataforma": "WordPress", "motivo": f"meta:{gen[:40]}"}
        if "joomla" in gen:
            return {"plataforma": "Sitio propio Joomla", "motivo": f"meta:{gen[:40]}"}
        if "drupal" in gen:
            return {"plataforma": "Sitio propio", "motivo": f"Drupal meta:{gen[:40]}"}
    if "/wp-content/" in html_low or "/wp-includes/" in html_low:
        return {"plataforma": "WordPress", "motivo": "wp-content"}
    if (
        "/components/com_" in html_low
        or "media/jui/" in html_low
        or "/templates/system/" in html_low
    ):
        return {"plataforma": "Sitio propio Joomla", "motivo": "paths joomla"}
    spa = ('id="app"' in html_low or 'id="root"' in html_low) and (
        "vue.js" in html_low or "vue.min" in html_low or "react-dom" in html_low
    )
    if spa:
        return {
            "plataforma": "SPA JavaScript",
            "requiere_js": "Sí",
            "motivo": "SPA Vue/React",
        }
    if "php" in str(headers.get("X-Powered-By", "")).lower():
        return {"plataforma": "PHP propio", "motivo": "X-Powered-By:PHP"}
    return {"plataforma": "Sitio propio", "motivo": "html estatico"}


async def analizar(session, sem, entry):
    url = entry["sitio_web"]
    out = {
        "id": entry["id"],
        "nombre": entry["nombre"],
        "sitio_web": url,
        "region": entry.get("region"),
    }
    async with sem:
        try:
            async with session.get(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "text/html,application/xhtml+xml",
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
                soup = BeautifulSoup(html, "html.parser")

                # Todos los links absolutos del HTML (para detectar plataformas externas)
                all_hrefs = []
                candidatos = []
                for a in soup.find_all("a", href=True):
                    href_raw = a["href"].strip()
                    if not href_raw or href_raw.startswith("#") or href_raw.startswith(
                        "javascript:"
                    ):
                        continue
                    full = urljoin(str(resp.url), href_raw)
                    all_hrefs.append(full)
                    texto = " ".join((a.get_text() or "").split())
                    score, matched = score_link(texto, full)
                    if score >= 4:
                        candidatos.append(
                            {
                                "url": full,
                                "texto": texto[:120],
                                "score": score,
                                "match": matched[:4],
                            }
                        )
                # iframe src
                for iframe in soup.find_all("iframe", src=True):
                    all_hrefs.append(urljoin(str(resp.url), iframe["src"]))
                # link tags (no relevantes)

                # plataforma externa (prioridad alta)
                ext = detectar_plataforma_externa(all_hrefs)
                if ext:
                    out["plataforma"] = ext["plataforma"]
                    out["plataforma_motivo"] = f"link a {ext['link_externo'][:80]}"
                    if ext["es_ep"]:
                        out["publica_en_empleospublicos"] = "Sí"
                else:
                    clasif = detectar_plataforma_html(html, headers)
                    out.update(clasif)
                    out["plataforma_motivo"] = clasif.get("motivo", "")

                # deduplicar candidatos
                seen = set()
                uniq = []
                for c in sorted(candidatos, key=lambda x: -x["score"]):
                    if c["url"] in seen:
                        continue
                    seen.add(c["url"])
                    uniq.append(c)
                out["links_empleo"] = uniq[:5]
                out["total_links"] = len(all_hrefs)
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
    triaje_v1 = json.loads(
        Path("logs/triaje_munis_resultado.json").read_text(encoding="utf-8")
    )
    vivos_ids = {r["id"] for r in triaje_v1 if r.get("ok")}
    queue = json.loads(
        Path("logs/verificacion_munis_queue.json").read_text(encoding="utf-8")
    )
    entries = [q for q in queue if q["id"] in vivos_ids]
    print(f"Reanalizando {len(entries)} sitios vivos v3 (scoring estricto)...")
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
    Path("logs/triaje_munis_v3.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # breakdown
    plats = {}
    con_alto = 0  # score >= 8
    con_medio = 0  # score 4-7
    sin_link = 0
    for r in results:
        if not r.get("ok"):
            plats["[error]"] = plats.get("[error]", 0) + 1
            continue
        p = r.get("plataforma", "?")
        plats[p] = plats.get(p, 0) + 1
        links = r.get("links_empleo", [])
        if links and links[0]["score"] >= 8:
            con_alto += 1
        elif links and links[0]["score"] >= 4:
            con_medio += 1
        else:
            sin_link += 1
    print("\nPlataforma:")
    for k, v in sorted(plats.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print(f"\nCon link score >= 8 (alta confianza): {con_alto}")
    print(f"Con link score 4-7 (media): {con_medio}")
    print(f"Sin link o baja confianza: {sin_link}")


if __name__ == "__main__":
    asyncio.run(main())
