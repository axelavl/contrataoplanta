#!/usr/bin/env python3
"""Descarga y cachea los logos institucionales en web/logos/.

Motivo: hoy el frontend pide los logos en runtime a Clearbit (con cascada a
Google favicon, etc.). Eso causa cargas lentas, saltos de layout y caídas
cuando el servicio externo falla. Como las instituciones son estables, cachear
sus logos en el repo y servirlos desde el CDN de Cloudflare Pages es rápido y
sin dependencia externa.

Qué hace:
  1. Lee el catálogo maestro (repositorio_instituciones_publicas_chile.json).
  2. Resuelve el dominio de cada institución desde `sitio_web`
     (descarta portales intermediarios tipo empleospublicos.cl).
  3. Por cada dominio único intenta, en orden:
        a) DuckDuckGo ip3 (mejor ícono del sitio, 60-180 px)
        b) Google s2 favicons @128 px
        c) apple-touch-icon del dominio
        d) apple-touch-icon-precomposed
     y guarda el resultado en web/logos/{dominio}.png
  4. Escribe web/logos/manifest.json = { "dominio": "archivo.png", ... }
     con los que sí se descargaron. El frontend sirve logos locales desde
     /logos/ antes de intentar fuentes externas.

Valida calidad: rechaza imágenes menores a 40 px (favicons diminutos).
Es idempotente: salta los que ya existen salvo --force.

Uso:
    python scripts/descargar_logos.py --dry-run            # solo listar
    python scripts/descargar_logos.py --max 20             # probar con 20
    python scripts/descargar_logos.py --workers 8          # descarga real
    python scripts/descargar_logos.py --force              # re-descargar todo

NOTA: requiere salida a internet hacia icons.duckduckgo.com y google.com.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

RAIZ = Path(__file__).resolve().parent.parent
CATALOGO = RAIZ / "repositorio_instituciones_publicas_chile.json"
OUT_DIR = RAIZ / "web" / "logos"
MANIFEST = OUT_DIR / "manifest.json"

# Dominios de portales/intermediarios: no representan a la institución, se saltan.
PORTALES = {
    "empleospublicos.cl", "www.empleospublicos.cl",
    "trabajando.cl", "trabajando.com", "laborum.cl",
    "hiringroom.com", "buk.cl", "talana.com", "linkedin.com",
    "google.com", "facebook.com", "instagram.com", "x.com", "twitter.com",
}

UA = "Mozilla/5.0 (compatible; contrataoplanta-logos/1.0)"


def dominio_de(sitio_web: str | None) -> str | None:
    if not sitio_web:
        return None
    url = sitio_web.strip()
    if not url:
        return None
    if "://" not in url:
        url = "https://" + url
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return None
    host = host.lower().lstrip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host or "." not in host or host in PORTALES:
        return None
    return host


def fuentes(dominio: str) -> list[str]:
    # Clearbit fue discontinuado por HubSpot en dic-2025. DuckDuckGo ip3
    # devuelve el mejor ícono del sitio (apple-touch-icon, 60-180 px) y
    # es la fuente primaria del frontend (shared-shell.js). Google s2 es
    # respaldo estable. Los assets directos del dominio son último recurso.
    enc = dominio
    return [
        f"https://icons.duckduckgo.com/ip3/{enc}.ico",
        f"https://www.google.com/s2/favicons?domain={enc}&sz=128",
        f"https://{dominio}/apple-touch-icon.png",
        f"https://{dominio}/apple-touch-icon-precomposed.png",
    ]


MIN_LOGO_PX = 40  # Bajo esto se ve pixelado al escalar a 44×44 (card) o 56×56 (detalle).


def _check_image_quality(data: bytes) -> bool:
    """Rechaza imágenes diminutas (favicons 16×16 / 32×32)."""
    try:
        from PIL import Image
        from io import BytesIO
        img = Image.open(BytesIO(data))
        return max(img.size) >= MIN_LOGO_PX
    except Exception:
        # Sin Pillow: aceptar todo lo que pese más de 200 bytes.
        return len(data) >= 200


def descargar(dominio: str, destino: Path, timeout: float = 12.0) -> bool:
    for url in fuentes(dominio):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status != 200:
                    continue
                data = resp.read()
            if len(data) < 200:
                continue
            if not _check_image_quality(data):
                continue
            destino.write_bytes(data)
            return True
        except Exception:
            continue
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Cachea logos institucionales en web/logos/")
    ap.add_argument("--max", type=int, default=None, help="Límite de instituciones a procesar")
    ap.add_argument("--workers", type=int, default=6, help="Descargas en paralelo (default 6)")
    ap.add_argument("--dry-run", action="store_true", help="Solo lista, no descarga")
    ap.add_argument("--force", action="store_true", help="Re-descarga aunque ya exista")
    ap.add_argument("--json", default=str(CATALOGO), help="Ruta del catálogo")
    args = ap.parse_args()

    catalogo = json.loads(Path(args.json).read_text(encoding="utf-8"))
    instituciones = catalogo.get("instituciones", catalogo) if isinstance(catalogo, dict) else catalogo

    # Dominios únicos (un logo por dominio aunque varias instituciones lo compartan).
    dominios: dict[str, str] = {}
    for inst in instituciones:
        dom = dominio_de(inst.get("sitio_web"))
        if dom and dom not in dominios:
            dominios[dom] = inst.get("nombre", dom)

    items = list(dominios.items())
    if args.max:
        items = items[: args.max]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, str] = {}
    if MANIFEST.exists():
        try:
            manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}

    print(f"Instituciones: {len(instituciones)} · dominios únicos: {len(dominios)} · a procesar: {len(items)}")

    if args.dry_run:
        for dom, nombre in items[:50]:
            print(f"  {dom:40s}  {nombre}")
        if len(items) > 50:
            print(f"  … (+{len(items) - 50} más)")
        return 0

    pendientes = []
    for dom, _ in items:
        destino = OUT_DIR / f"{dom}.png"
        if destino.exists() and not args.force:
            manifest[dom] = destino.name
            continue
        pendientes.append((dom, destino))

    ok = 0
    fail = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futuros = {ex.submit(descargar, dom, destino): dom for dom, destino in pendientes}
        for fut in concurrent.futures.as_completed(futuros):
            dom = futuros[fut]
            try:
                if fut.result():
                    manifest[dom] = f"{dom}.png"
                    ok += 1
                    print(f"  ✓ {dom}")
                else:
                    fail += 1
                    print(f"  ✗ {dom} (sin logo)")
            except Exception as e:
                fail += 1
                print(f"  ✗ {dom} ({e})")

    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nDescargados: {ok} · fallidos: {fail} · total en manifest: {len(manifest)}")
    print(f"Manifest → {MANIFEST.relative_to(RAIZ)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
