#!/usr/bin/env python3
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / 'web'

errors = []
pattern = re.compile(r'^.+ — contrata o planta \.cl$')

for html in sorted(WEB.glob('*.html')):
    txt = html.read_text(encoding='utf-8', errors='ignore')
    m = re.search(r'<title>(.*?)</title>', txt, re.I | re.S)
    if not m:
        errors.append(f'{html}: falta <title>')
        continue
    title = ' '.join(m.group(1).split())
    if html.name in {'instituciones.html'}:
        # redirect page allowed
        continue
    if not pattern.match(title):
        errors.append(f'{html}: título fuera de estándar -> {title}')

if errors:
    print('\n'.join(errors))
    sys.exit(1)

print(f'OK: {len(list(WEB.glob("*.html")))} títulos revisados.')
