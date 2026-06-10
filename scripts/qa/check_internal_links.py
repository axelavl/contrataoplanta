#!/usr/bin/env python3
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / 'web'
HTMLS = list(WEB.glob('*.html'))

# Rutas servidas dinámicamente por FastAPI (SSR) — no existen como
# archivos y no las podemos validar con el filesystem. Ver
# `api/routers/web.py` para el catálogo canónico de endpoints.
#
# Si agregas una ruta SSR nueva al backend y quieres enlazarla desde el
# HTML estático, agrégala a esta lista de prefijos.
SSR_PREFIXES = (
    '/oferta/',
    '/share/oferta/',
    '/empleos/region/',
    '/empleos/sector/',
    '/empleos/institucion/',
    '/api/',
    '/health',
    '/sitemap.xml',
    '/robots.txt',
)


def is_ssr_route(path: str) -> bool:
    return any(path == p.rstrip('/') or path.startswith(p) for p in SSR_PREFIXES)


errors = []
for html in HTMLS:
    txt = html.read_text(encoding='utf-8', errors='ignore')
    for href in re.findall(r'href=["\']([^"\']+)["\']', txt, flags=re.I):
        if href.startswith(('#', 'mailto:', 'tel:', 'javascript:', 'http://', 'https://')) or '${' in href:
            continue
        path = href.split('#', 1)[0].split('?', 1)[0]
        if not path:
            continue
        if path.startswith('/') and is_ssr_route(path):
            continue
        if path.startswith('/'):
            candidate = (ROOT / path.lstrip('/')).resolve()
        else:
            candidate = (html.parent / path).resolve()
        if not candidate.exists():
            errors.append(f'{html.relative_to(ROOT)} -> href="{href}" no existe')

if errors:
    print('\n'.join(errors[:300]))
    sys.exit(1)

print(f'OK: enlaces internos validados en {len(HTMLS)} páginas.')
