#!/usr/bin/env python3
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / 'web'
TOKENS = WEB / 'styles' / 'design-tokens.css'

errors = []
for html in sorted(WEB.glob('*.html')):
    txt = html.read_text(encoding='utf-8', errors='ignore')
    if 'http-equiv="refresh"' in txt.lower():
        continue
    if not re.search(r'<html[^>]*\blang="es"', txt, re.I):
        errors.append(f'{html.name}: falta lang="es"')
    if '<main' not in txt:
        errors.append(f'{html.name}: falta landmark <main>')
    if 'skip-link' not in txt:
        errors.append(f'{html.name}: falta skip link')
    if 'site-header' not in txt and '<nav' not in txt:
        errors.append(f'{html.name}: falta nav/header')

# contraste mínimo con tokens clave
css = TOKENS.read_text(encoding='utf-8', errors='ignore')
def hex_of(var):
    m = re.search(rf'{re.escape(var)}\s*:\s*(#[0-9A-Fa-f]{{6}})', css)
    return m.group(1) if m else None

def luminance(hex_color):
    c = hex_color.lstrip('#')
    rgb = [int(c[i:i+2], 16)/255 for i in (0,2,4)]
    def f(u):
        return u/12.92 if u <= 0.03928 else ((u+0.055)/1.055)**2.4
    r,g,b = [f(v) for v in rgb]
    return 0.2126*r + 0.7152*g + 0.0722*b

def contrast(a,b):
    la, lb = luminance(a), luminance(b)
    l1, l2 = (la, lb) if la > lb else (lb, la)
    return (l1 + 0.05) / (l2 + 0.05)

pairs = [('--texto','--bg',4.5), ('--texto2','--bg',4.5), ('--blanco','--azul',4.5)]
for fg,bg,min_ratio in pairs:
    hf,hb = hex_of(fg), hex_of(bg)
    if not hf or not hb:
        errors.append(f'design-tokens: falta token {fg} o {bg}')
        continue
    ratio = contrast(hf,hb)
    if ratio < min_ratio:
        errors.append(f'contraste insuficiente {fg}/{bg}: {ratio:.2f} < {min_ratio}')

if errors:
    print('\n'.join(errors[:300]))
    sys.exit(1)

print('OK: checks de accesibilidad mínima completados.')
