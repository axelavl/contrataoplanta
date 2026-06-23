#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, os, sys, csv
from datetime import date, datetime
from collections import Counter

SUFFIXES = [
    "1781842505054","1781842508373","1781842511540","1781842518914",
    "1781842521255","1781842524145","1781842972698","1781842975165",
    "1781842977656","1781842983130","1781842985607","1781842987805",
]
BASE = sys.argv[1]
CSV_PATH = sys.argv[2]
sep = "/" if "/" in BASE else "\\"

def build_path(suf):
    return BASE + sep + "mcp-workspace-web_fetch-" + suf + ".txt"

def salvage_ofertas(raw):
    marker = '"ofertas":['
    i = raw.find(marker)
    if i == -1:
        return []
    j = i + len(marker)
    n = len(raw)
    ofertas = []
    while j < n:
        while j < n and raw[j] in ' \t\r\n,':
            j += 1
        if j >= n or raw[j] == ']':
            break
        if raw[j] != '{':
            nxt = raw.find('{', j)
            if nxt == -1:
                break
            j = nxt
        depth = 0; in_str = False; esc = False
        start = j; k = j; complete = False
        while k < n:
            c = raw[k]
            if in_str:
                if esc: esc = False
                elif c == '\\': esc = True
                elif c == '"': in_str = False
            else:
                if c == '"': in_str = True
                elif c == '{': depth += 1
                elif c == '}':
                    depth -= 1
                    if depth == 0:
                        complete = True; k += 1; break
            k += 1
        if complete:
            try: ofertas.append(json.loads(raw[start:k]))
            except Exception: pass
            j = k
        else:
            break
    return ofertas

all_ofertas = []; loaded_files = 0; file_errors = []; per_file = {}
for suf in SUFFIXES:
    p = build_path(suf)
    if not os.path.exists(p):
        file_errors.append((suf, "no existe")); continue
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception as e:
        file_errors.append((suf, "read error: %r" % e)); continue
    ofs = None
    idx = raw.find("{")
    if idx != -1:
        try:
            ofs = json.loads(raw[idx:]).get("ofertas", [])
        except Exception:
            ofs = None
    if ofs is None:
        ofs = salvage_ofertas(raw)
        file_errors.append((suf, "JSON truncado -> recuperadas %d ofertas via salvage" % len(ofs)))
    per_file[suf] = len(ofs)
    all_ofertas.extend(ofs); loaded_files += 1

total_raw = len(all_ofertas)
by_id = {}
for o in all_ofertas:
    oid = o.get("id")
    if oid not in by_id: by_id[oid] = o
unique_ids = len(by_id)
sin_fecha = [o for o in by_id.values() if o.get("fecha_cierre") is None]
n_sin_fecha = len(sin_fecha)

plat = Counter(o.get("plataforma") for o in sin_fecha)
sect = Counter(o.get("sector") for o in sin_fecha)
inst = Counter(o.get("institucion") for o in sin_fecha)
uv = Counter()
for o in sin_fecha:
    v = o.get("url_oferta_valida")
    if v is True: uv["True"]+=1
    elif v is False: uv["False"]+=1
    elif v is None: uv["None"]+=1
    else: uv["otro(%r)"%v]+=1

HOY = date(2026,6,19)
def parse_fecha(s):
    if not s or not isinstance(s,str): return None
    part = s.strip().split("T")[0].split(" ")[0]
    try: return datetime.strptime(part,"%Y-%m-%d").date()
    except Exception: return None
ant_gt90=ant_30_90=ant_lt30=ant_sin=0
for o in sin_fecha:
    d=parse_fecha(o.get("fecha_scraped"))
    if d is None: ant_sin+=1; continue
    delta=(HOY-d).days
    if delta>90: ant_gt90+=1
    elif delta>=30: ant_30_90+=1
    else: ant_lt30+=1

cols=["id","institucion","sigla","plataforma","sector","region","cargo","fecha_publicacion","fecha_scraped","url_oferta","url_bases","url_oferta_valida","url_bases_valida"]
rows_written=0
with open(CSV_PATH,"w",encoding="utf-8",newline="") as f:
    w=csv.writer(f,quoting=csv.QUOTE_MINIMAL)
    w.writerow(cols)
    for o in sin_fecha:
        w.writerow([o.get(c) for c in cols]); rows_written+=1

L=[]
L.append("=== COBERTURA ===")
L.append("Archivos procesados: %d de 12"%loaded_files)
L.append("Ofertas por pagina: "+", ".join("p%d=%d"%(i+1,per_file.get(s,0)) for i,s in enumerate(SUFFIXES)))
L.append("Ofertas totales leidas (con duplicados): %d"%total_raw)
L.append("IDs unicos (todas, dedup): %d"%unique_ids)
for s,e in file_errors: L.append("  NOTA p?(%s): %s"%(s,e))
L.append("")
L.append("=== (1) sin_fecha (fecha_cierre null, dedup por id): %d ==="%n_sin_fecha)
L.append("")
L.append("=== (2) PLATAFORMA (desc) ===")
for k,v in plat.most_common(): L.append("  %6d  %s"%(v,k))
L.append("")
L.append("=== (3) SECTOR (desc) ===")
for k,v in sect.most_common(): L.append("  %6d  %s"%(v,k))
L.append("")
L.append("=== (4) TOP 40 INSTITUCIONES (desc) ===")
for k,v in inst.most_common(40): L.append("  %6d  %s"%(v,k))
L.append("")
L.append("=== (5) url_oferta_valida en sin_fecha ===")
L.append("  True : %d"%uv.get("True",0))
L.append("  False: %d"%uv.get("False",0))
L.append("  None : %d"%uv.get("None",0))
for k,v in uv.items():
    if k not in ("True","False","None"): L.append("  %s: %d"%(k,v))
L.append("")
L.append("=== (6) Antiguedad fecha_scraped vs 2026-06-19 ===")
L.append("  Criterio: >90d ; 30-90d (>=30 y <=90) ; <30d")
L.append("  >90 dias         : %d"%ant_gt90)
L.append("  30-90 dias       : %d"%ant_30_90)
L.append("  <30 dias         : %d"%ant_lt30)
L.append("  sin_fecha_scraped: %d"%ant_sin)
L.append("")
L.append("=== CSV ===")
L.append("Escrito en: %s"%CSV_PATH)
L.append("Filas (sin encabezado): %d  (debe igualar (1)=%d)"%(rows_written,n_sin_fecha))
print("\n".join(L))
