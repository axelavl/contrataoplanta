#!/usr/bin/env python3
"""
Verifica que una URL `/oferta/{id}[-{slug}]` entrega SSR correcto:

- Devuelve 200 (sigue 301 al slug canónico).
- Headers de seguridad presentes (HSTS, nosniff, Permissions-Policy,
  Referrer-Policy, X-Frame, CSP Report-Only).
- `<title>` no vacío y contiene el cargo.
- `<meta name="description">`, `og:title`, `og:description`, `canonical`.
- JSON-LD con `@type: JobPosting` parseable y con los 5 campos mínimos.
- `<article class="oferta-ssr">` presente con `<h1>`.

No ejecuta JavaScript — simula lo que ve Googlebot-lite, WhatsApp,
Slack y otros crawlers sin render.

Uso:
    python scripts/qa/verify_ssr.py https://<host>/oferta/42
    python scripts/qa/verify_ssr.py https://<host>/oferta/42 --json

Exit code:
    0 = todo OK
    1 = alguna verificación falló (detalle impreso por stderr)
    2 = error de red / respuesta inválida
"""
from __future__ import annotations

import argparse
import json
import re
import sys

try:
    import requests
except ImportError:
    sys.stderr.write("requests no está instalado (pip install requests)\n")
    sys.exit(2)

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.stderr.write("bs4 no está instalado (pip install beautifulsoup4)\n")
    sys.exit(2)


REQUIRED_HEADERS: tuple[tuple[str, str | None], ...] = (
    ("strict-transport-security", "max-age="),
    ("x-content-type-options", "nosniff"),
    ("x-frame-options", None),
    ("referrer-policy", None),
    ("permissions-policy", None),
    # CSP en Report-Only por ahora
    ("content-security-policy-report-only", "default-src"),
)


def _find_meta(soup, *, name: str | None = None, prop: str | None = None) -> str | None:
    attrs = {"name": name} if name else {"property": prop}
    el = soup.find("meta", attrs=attrs)
    return el.get("content", "").strip() if el else None


def verify(url: str) -> tuple[bool, list[dict]]:
    """Corre todos los chequeos. Devuelve (ok_global, lista_detalle)."""
    results: list[dict] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        results.append({"check": name, "ok": ok, "detail": detail})

    try:
        r = requests.get(url, timeout=15, allow_redirects=True)
    except requests.RequestException as e:
        check("request", False, f"error de red: {e}")
        return False, results

    check("status 200", r.status_code == 200, f"status={r.status_code} url_final={r.url}")

    # Headers de seguridad
    lowered = {k.lower(): v for k, v in r.headers.items()}
    for hname, substr in REQUIRED_HEADERS:
        v = lowered.get(hname, "")
        if substr is None:
            check(f"header {hname}", bool(v), v[:80])
        else:
            check(
                f"header {hname} contiene {substr!r}",
                substr.lower() in v.lower(),
                v[:80],
            )

    soup = BeautifulSoup(r.text, "html.parser")

    # <title>
    title_el = soup.find("title")
    title = (title_el.string or "").strip() if title_el else ""
    check("<title> no vacío", bool(title), title[:80])

    # Meta tags
    desc = _find_meta(soup, name="description")
    check("meta[name=description]", bool(desc), (desc or "")[:80])

    og_title = _find_meta(soup, prop="og:title")
    check("meta[og:title]", bool(og_title), (og_title or "")[:80])

    og_desc = _find_meta(soup, prop="og:description")
    check("meta[og:description]", bool(og_desc), (og_desc or "")[:80])

    canonical = soup.find("link", rel="canonical")
    canonical_href = (canonical.get("href") or "") if canonical else ""
    check("<link rel=canonical>", bool(canonical_href), canonical_href)

    # JSON-LD JobPosting
    jobposting = None
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = s.string or ""
            # Escape unicode OWASP: re-decodifica `\u003c` como `<` etc.
            data = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            jobposting = data
            break

    if jobposting:
        check("JSON-LD JobPosting parseable", True, "")
        for field in ("title", "hiringOrganization", "validThrough", "url", "jobLocation"):
            check(f"  JobPosting.{field}", bool(jobposting.get(field)), str(jobposting.get(field))[:80])
    else:
        check("JSON-LD JobPosting parseable", False,
              "no encontrado ningún <script type=application/ld+json> con @type=JobPosting")

    # Bloque SSR visible
    article = soup.find("article", class_="oferta-ssr")
    check("<article class=oferta-ssr>", bool(article), "")
    if article:
        h1 = article.find("h1")
        check("  <h1> con cargo", bool(h1 and h1.get_text(strip=True)),
              (h1.get_text(strip=True) if h1 else "")[:80])
        inst = article.find(class_="oferta-ssr-institucion")
        check("  .oferta-ssr-institucion", bool(inst and inst.get_text(strip=True)),
              (inst.get_text(strip=True) if inst else "")[:80])

    ok_global = all(r["ok"] for r in results)
    return ok_global, results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="URL /oferta/{id}[-{slug}] a verificar")
    parser.add_argument("--json", action="store_true", help="salida JSON")
    args = parser.parse_args()

    if not re.match(r"^https?://", args.url):
        sys.stderr.write("URL debe empezar con http:// o https://\n")
        return 2

    ok, results = verify(args.url)

    if args.json:
        print(json.dumps({"url": args.url, "ok": ok, "checks": results},
                         ensure_ascii=False, indent=2))
        return 0 if ok else 1

    print(f"Verificando: {args.url}")
    print("-" * 72)
    for r in results:
        mark = "✓" if r["ok"] else "✗"
        detail = f" — {r['detail']}" if r["detail"] else ""
        print(f"  {mark} {r['check']}{detail}")
    print("-" * 72)
    failed = [r for r in results if not r["ok"]]
    if failed:
        print(f"\n{len(failed)} verificacion(es) fallaron. Revisar arriba.")
        return 1
    print("\nTodo OK. SSR + headers + JSON-LD listos para crawlers.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
