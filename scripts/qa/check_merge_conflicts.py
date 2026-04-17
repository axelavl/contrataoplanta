#!/usr/bin/env python3
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / 'web'

errors = []
markers = (r'^<<<<<<<\s', r'^=======\s*$', r'^>>>>>>>\s')
pattern = re.compile('|'.join(markers), re.M)

# files explicitly requested by reviewer (normalized .htm1 typos -> .html)
expected = [
    '404.html', 'descargo.html', 'estadisticas.html', 'faq.html', 'favoritos.html',
    'formacion-sector-publico-chile.html', 'glosario-laboral-publico.html',
    'guia-busqueda-empleo-publico.html', 'guia-postulacion-empleos-publicos.html',
    'guia-preparacion-cv-sector-publico.html', 'guia-seguimiento-postulacion-publica.html',
    'historial.html', 'index.html', 'instituciones.html'
]

for name in expected:
    if not (WEB / name).exists():
        errors.append(f'Falta archivo requerido: web/{name}')

for html in WEB.glob('*.html'):
    txt = html.read_text(encoding='utf-8', errors='ignore')
    if pattern.search(txt):
        errors.append(f'Marcador de conflicto detectado en {html.relative_to(ROOT)}')

if errors:
    print('\n'.join(errors))
    sys.exit(1)

print(f'OK: sin marcadores de conflicto en {len(list(WEB.glob("*.html")))} páginas y archivos críticos presentes.')
