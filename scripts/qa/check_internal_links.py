#!/usr/bin/env python3
from pathlib import Path
import re
import sys
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / 'web'
HTMLS = list(WEB.glob('*.html'))

errors = []
for html in HTMLS:
    txt = html.read_text(encoding='utf-8', errors='ignore')
    for href in re.findall(r'href=["\']([^"\']+)["\']', txt, flags=re.I):
        if href.startswith(('#', 'mailto:', 'tel:', 'javascript:', 'http://', 'https://')) or '${' in href:
            continue
        path = href.split('#', 1)[0].split('?', 1)[0]
        if not path:
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
