"""
EmpleoEstado.cl — Scraper de concursos de universidades con PORTAL PROPIO
(no WordPress, no Trabajando). Patrón estándar de sesión, config-driven.

MODOS (verificados 06/2026):
  unap_sispartime  Portal SISPARTIME de la U. Arturo Prat. La página
        llamado.php lista TODOS los llamados con sus campos en texto plano,
        uno tras otro:
          Cargo : ... Código : ... Tipo Llamado : ... Facultad o Unidad : ...
          Área : ... Sede : ... Jornada : ... Fecha Inicio : ... Fecha Fin : ...
        Más enlace "Perfil" (PDF descriptor) y "Postule aquí". Es el caso más
        rico: cargo, código (id estable), sede (→ ciudad/región), jornada
        (→ tipo_cargo), fecha de inicio y fin (→ publicación y cierre).

  ufro_tabla  Portal de Concursos de la U. de La Frontera. La vista
        ver_tipo_administrativo.php trae una tabla (Código, Descripción,
        Link, Tipo, Estado). Hoy responde "No hay Ofertas Laborales
        Disponibles" (tabla vacía); el parser está listo para cuando publiquen.
        Las vistas Académicos/Post-Doctoral se cargan por JS (menús '#') y no
        se pudieron verificar sin navegador → declaradas, no construidas.

Sector = "Educación Superior". Renta: estos portales no publican monto →
renta_* en null. Vigencia: UNAP filtra por Fecha Fin; UFRO por la columna
Estado / presencia.

Uso:
    python scrapers/universidades_portal.py --dry-run --verbose
    python scrapers/universidades_portal.py --dry-run --solo unap --export salida
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── Integración con el proyecto (con fallback standalone) ───────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from config import config
    from db.database import (
        SessionLocal,
        generar_id_estable,
        limpiar_texto,
        marcar_ofertas_cerradas,
        normalizar_area,
        normalizar_region,
        registrar_log,
        upsert_oferta,
    )
    STANDALONE = False
except ImportError:
    STANDALONE = True

    class _Cfg:
        LOG_DIR = "logs"
        LOG_LEVEL = "INFO"
        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]

    config = _Cfg()  # type: ignore[assignment]
    SessionLocal = None  # type: ignore[assignment]

    def generar_id_estable(*partes: Any) -> str:  # type: ignore[misc]
        import hashlib
        return hashlib.sha1("||".join(str(p) for p in partes).encode()).hexdigest()[:24]

    def limpiar_texto(t: str | None) -> str:  # type: ignore[misc]
        return re.sub(r"\s+", " ", t or "").strip()

    def normalizar_area(cargo: str) -> str | None:  # type: ignore[misc]
        return None

    def normalizar_region(r: str) -> str | None:  # type: ignore[misc]
        return (r or "").strip() or None

    def marcar_ofertas_cerradas(*a: Any, **k: Any) -> int:  # type: ignore[misc]
        return 0

    def registrar_log(*a: Any, **k: Any) -> None:  # type: ignore[misc]
        return None

    def upsert_oferta(*a: Any, **k: Any) -> tuple[bool, bool]:  # type: ignore[misc]
        return False, False

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("scraper.universidades_portal")
logger.setLevel(getattr(logging, config.LOG_LEVEL))
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / "universidades_portal.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
logger.propagate = False

HTTP_TIMEOUT = 25
MAX_RETRIES = 3
SECTOR = "Educación Superior"

# Sede UNAP → (ciudad, región)
_SEDE_UNAP = {
    "iquique": ("Iquique", "Tarapacá"),
    "arica": ("Arica", "Arica y Parinacota"),
    "antofagasta": ("Antofagasta", "Antofagasta"),
    "santiago": ("Santiago", "Metropolitana de Santiago"),
    "victoria": ("Victoria", "La Araucanía"),
}

CAMPOS_EXPORT = ["id_externo", "fuente_id", "institucion_nombre", "sector", "cargo",
                 "area_profesional", "tipo_cargo", "nivel", "region", "ciudad",
                 "renta_bruta_min", "renta_bruta_max", "renta_texto",
                 "fecha_publicacion", "fecha_cierre", "url_original",
                 "descripcion", "requisitos_texto", "url_bases"]

FUENTES: list[dict[str, Any]] = [
    {"clave": "unap", "id": 251, "nombre": "Universidad Arturo Prat",
     "sigla": "UNAP", "region": "Tarapacá", "ciudad": "Iquique",
     "modo": "unap_sispartime",
     "url": "http://portal.unap.cl/kb/dewey/app/sispartime/llamado.php"},
    {"clave": "ufro", "id": 246, "nombre": "Universidad de La Frontera",
     "sigla": "UFRO", "region": "La Araucanía", "ciudad": "Temuco",
     "modo": "ufro_tabla",
     "url": "https://extranet.ufro.cl/concursos/ver_tipo_administrativo.php",
     "base": "https://extranet.ufro.cl/concursos/"},
     # Portal por POST: la vista carga con POST cod_concar=1/2/3 (Administrativos/
     # Académicos/Post-Doctoral); cada concurso abre sus cargos (Vacantes, Fecha
     # Término) y cada cargo su ficha (Descripción, Requisitos) también por POST.
]


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> requests.Response | None:
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s en %s", r.status_code, url[:75])
                return None
            r.encoding = r.encoding or "utf-8"
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo %s: %s", url[:70], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


def _post(session: requests.Session, url: str, data: dict) -> requests.Response | None:
    """POST con reintentos (el portal UFRO carga cada vista/ficha por POST)."""
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(url, data=data, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                logger.info("  HTTP %s (POST) en %s", r.status_code, url[:75])
                return None
            return r
        except requests.RequestException as exc:
            if intento == MAX_RETRIES:
                logger.info("  Fallo POST %s: %s", url[:70], type(exc).__name__)
                return None
            time.sleep(2 ** intento)
    return None


# ── Utilidades de parseo (puras) ─────────────────────────────────────────────
def _fecha_dmy(s: str) -> date | None:
    if m := re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", s or ""):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("jefatura", "jefe", "jefa", "director", "decano",
                            "vicerrector", "coordinador")):
        return "Directivo"
    if any(w in c for w in ("docente", "académic", "academic", "profesor",
                            "profesional", "ingenier", "analista")):
        return "Profesional"
    if any(w in c for w in ("técnic", "tecnic", "administrativ", "auxiliar",
                            "asistente", "secretari", "operador")):
        return "Técnico"
    return "Profesional"


def _tipo_desde_jornada(jornada: str) -> str:
    j = (jornada or "").lower()
    if "honorario" in j:
        return "Honorarios"
    if "contrata" in j:
        return "Contrata"
    if "planta" in j:
        return "Planta"
    return "Concurso Universitario"


# ── Modo UNAP (SISPARTIME) ───────────────────────────────────────────────────
_CAMPOS_UNAP = ("Cargo", "Código", "Tipo Llamado", "Facultad o Unidad",
                "Área", "Sede", "Jornada", "Fecha Inicio", "Fecha Fin")
_SIG_CAMPO = (r"(?=\s+(?:Cargo|C[óo]digo|Tipo Llamado|Facultad o Unidad|"
              r"[ÁA]rea|Sede|Jornada|Fecha Inicio|Fecha Fin|Ver Requisitos|"
              r"Perfil|Postule)\s*:?|$)")


def parsear_unap(html: str, fuente: dict) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.body.get_text(" ", strip=True)) if soup.body else ""
    perfiles = [a["href"] for a in soup.find_all("a", href=True)
                if "perfil" in limpiar_texto(a.get_text()).lower()
                or "docs_perfiles" in a["href"].lower()]

    bloques = [b for b in re.split(r"(?=Cargo\s*:)", texto)
               if b.strip().startswith("Cargo")]
    items = []
    for b in bloques:
        campos = {}
        for c in _CAMPOS_UNAP:
            if m := re.search(re.escape(c) + r"\s*:\s*(.+?)" + _SIG_CAMPO, b):
                campos[c] = limpiar_texto(m.group(1))
        if not campos.get("Cargo"):
            continue
        items.append({
            "cargo": campos["Cargo"],
            "codigo": campos.get("Código"),
            "tipo_llamado": campos.get("Tipo Llamado"),
            "unidad": campos.get("Facultad o Unidad") or campos.get("Área"),
            "sede": campos.get("Sede"),
            "jornada": campos.get("Jornada"),
            "fecha_inicio": _fecha_dmy(campos.get("Fecha Inicio", "")),
            "fecha_fin": _fecha_dmy(campos.get("Fecha Fin", "")),
            "perfil": None,
        })
    _asignar_perfiles_unap(items, perfiles)
    return items


def _asignar_perfiles_unap(items: list[dict], perfiles: list[str]) -> None:
    """Asocia cada PDF 'Perfil' a su cargo. Primero por CÓDIGO en el href (robusto);
    el fallback POSICIONAL sólo se usa si hay exactamente un perfil por cargo (1:1),
    porque bloques saltados desalineaban el índice y adjuntaban el PDF equivocado."""
    usados: set[int] = set()
    for it in items:
        cod = (it.get("codigo") or "").strip().lower()
        cod_num = re.sub(r"\D", "", cod)
        for j, p in enumerate(perfiles):
            if j in usados:
                continue
            pl = p.lower()
            if (cod and cod in pl) or (len(cod_num) >= 4 and cod_num in pl):
                it["perfil"] = p
                usados.add(j)
                break
    # Fallback posicional SÓLO si nadie matcheó por código y las cantidades calzan.
    if not usados and len(perfiles) == len(items):
        for i, it in enumerate(items):
            it["perfil"] = perfiles[i]


def _construir_unap(it: dict, fuente: dict) -> dict:
    fuente_id = int(fuente["id"])
    nombre = fuente["nombre"]
    cargo = limpiar_texto(it["cargo"]).title()[:500] \
        if it["cargo"].isupper() else limpiar_texto(it["cargo"])[:500]
    sede = (it.get("sede") or "").lower()
    ciudad, region = _SEDE_UNAP.get(sede, (it.get("sede") or fuente["ciudad"],
                                           fuente["region"]))
    codigo = it.get("codigo")
    url = (f"http://portal.unap.cl/kb/dewey/app/sispartime/llamado.php#{codigo}"
           if codigo else fuente["url"])

    perfil_url = urljoin(fuente["url"], it["perfil"]) if it.get("perfil") else None

    desc_partes = []
    if codigo:
        desc_partes.append(f"Código: {codigo}")
    if it.get("tipo_llamado"):
        desc_partes.append(f"Tipo de llamado: {it['tipo_llamado']}")
    if it.get("unidad"):
        desc_partes.append(f"Unidad: {it['unidad']}")
    if it.get("jornada"):
        desc_partes.append(f"Jornada: {it['jornada']}")
    desc_partes.append("Postulación: http://trabajo.unap.cl")

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo,
                                         codigo or cargo),
        "fuente_id": fuente_id,
        "url_original": url,
        "cargo": cargo,
        "descripcion": limpiar_texto(" | ".join(desc_partes))[:2000],
        "institucion_nombre": nombre,
        "sector": SECTOR,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": _tipo_desde_jornada(it.get("jornada", "")),
        "nivel": _nivel(cargo),
        "region": normalizar_region(region) or region,
        "ciudad": ciudad,
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": it.get("fecha_inicio") or date.today(),
        "fecha_cierre": it.get("fecha_fin"),
        "requisitos_texto": None,
        "url_bases": perfil_url,
    }


# ── Modo UFRO (portal por POST) ──────────────────────────────────────────────
# El "VER" de cada fila es javascript:ver_consulta('806','CON0806', …). De ahí
# salen los identificadores que hay que POSTear para abrir el nivel siguiente.
_RE_VER_CONSULTA = re.compile(r"ver_consulta\(([^)]*)\)", re.I)


def _args_ver_consulta(href: str | None) -> list[str]:
    m = _RE_VER_CONSULTA.search(href or "")
    if not m:
        return []
    return [a.strip().strip("'\"") for a in m.group(1).split(",") if a.strip()]


def _tabla_ufro(html: Any) -> tuple[list[str], list[tuple[list[str], list[str]]]]:
    """(encabezados_lower, filas) de la primera <table> del portal. Cada fila es
    (celdas_texto, args_de_ver_consulta). Acepta bytes (autodetecta charset)."""
    soup = BeautifulSoup(html, "html.parser")
    tabla = soup.find("table")
    if not tabla or not tabla.find("tr"):
        return [], []
    heads = [limpiar_texto(c.get_text()).lower()
             for c in tabla.find("tr").find_all(["th", "td"])]
    filas = []
    for fila in tabla.find_all("tr")[1:]:
        celdas = fila.find_all("td")
        if not celdas:
            continue
        txt = [limpiar_texto(c.get_text(" ", strip=True)) for c in celdas]
        a = fila.find("a", href=True)
        filas.append((txt, _args_ver_consulta(a["href"]) if a else []))
    return heads, filas


def _idx_col(heads: list[str], *claves: str) -> int | None:
    for i, e in enumerate(heads):
        if any(k in e for k in claves):
            return i
    return None


def parsear_ufro_concursos(html: Any) -> list[dict]:
    """Nivel 1 (ver_tipo_administrativo.php): concursos con código, descripción,
    tipo, estado y (cod_concur, id_concur) para abrir sus cargos."""
    heads, filas = _tabla_ufro(html)
    i_cod, i_desc = _idx_col(heads, "codig", "código"), _idx_col(heads, "descrip", "cargo")
    i_tipo, i_estado = _idx_col(heads, "tipo"), _idx_col(heads, "estado")
    out = []
    for txt, args in filas:
        def cel(i):
            return txt[i] if i is not None and i < len(txt) else None
        desc = cel(i_desc)
        if not desc or len(args) < 2:
            continue
        out.append({"codigo": cel(i_cod), "descripcion": desc, "tipo": cel(i_tipo),
                    "estado": cel(i_estado), "cod_concur": args[0], "id_concur": args[1]})
    return out


def parsear_ufro_cargos(html: Any) -> list[dict]:
    """Nivel 2 (ver_cargos_concurso.php): cargos con nombre, vacantes, fecha de
    inicio y término, y los 4 ids para abrir la ficha de postulación."""
    heads, filas = _tabla_ufro(html)
    i_cod, i_nom = _idx_col(heads, "codig", "código"), _idx_col(heads, "nombre", "cargo")
    i_vac = _idx_col(heads, "vacante")
    i_ini, i_ter = _idx_col(heads, "inicio"), _idx_col(heads, "término", "termino", "cierre")
    out = []
    for txt, args in filas:
        def cel(i):
            return txt[i] if i is not None and i < len(txt) else None
        nombre = cel(i_nom)
        if not nombre:
            continue
        vac = None
        if (vt := cel(i_vac)) and (mv := re.search(r"\d+", vt)):
            vac = int(mv.group(0))
        out.append({
            "codigo": cel(i_cod), "nombre": nombre, "vacantes": vac,
            "fecha_inicio": _fecha_dmy(cel(i_ini) or ""),
            "fecha_termino": _fecha_dmy(cel(i_ter) or ""),
            "cod_concar": args[0] if len(args) > 0 else None,
            "cod_concur": args[1] if len(args) > 1 else None,
            "id_concar": args[2] if len(args) > 2 else None,
            "id_concur": args[3] if len(args) > 3 else None,
        })
    return out


# Etiquetas de la ficha de detalle UFRO (ver captura). Se extrae el valor entre
# una etiqueta y la siguiente. "Requisitos Específicos" va ANTES que "Requisitos"
# en el lookahead para que el corte de "Requisitos" se detenga en ella.
_UFRO_SIG_CAMPO = (
    r"(?=\s*(?:Cargo|Descripci[oó]n|Vacantes|Fecha\s*T[eé]rmino|Estado|"
    r"Requisitos\s*Espec[ií]ficos|Requisitos|Preguntas|Responder)\s*:|$)"
)


def _campo_ufro(texto: str, etiqueta: str) -> str | None:
    m = re.search(etiqueta + r"\s*:\s*(.+?)" + _UFRO_SIG_CAMPO, texto,
                  re.I | re.S)
    return limpiar_texto(m.group(1)) if m else None


def parsear_ufro_detalle(html: str) -> dict:
    """Extrae los campos de la ficha del concurso: Cargo, Descripción,
    Vacantes, Fecha Término, Estado, Requisitos y Requisitos Específicos."""
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True))
    d: dict[str, Any] = {}
    if v := _campo_ufro(texto, r"Cargo"):
        d["cargo"] = v
    if v := _campo_ufro(texto, r"Descripci[oó]n"):
        d["descripcion"] = v
    if v := _campo_ufro(texto, r"Vacantes"):
        if mv := re.search(r"\d+", v):
            d["vacantes"] = int(mv.group(0))
    if v := _campo_ufro(texto, r"Fecha\s*T[eé]rmino"):
        if f := _fecha_dmy(v):
            d["fecha_cierre"] = f
    if v := _campo_ufro(texto, r"Estado"):
        d["estado"] = v
    partes_req = []
    if v := _campo_ufro(texto, r"Requisitos"):
        partes_req.append(v)
    if v := _campo_ufro(texto, r"Requisitos\s*Espec[ií]ficos"):
        partes_req.append("Requisitos específicos: " + v)
    if partes_req:
        d["requisitos"] = limpiar_texto(" | ".join(partes_req))[:2000]
    return d


def _construir_ufro_cargo(fuente: dict, concurso: dict, cargo: dict,
                          detalle: dict, tipo_concurso: str) -> dict:
    """Arma la oferta desde el concurso (nivel 1) + cargo (nivel 2) + ficha
    (nivel 3). El cierre y vacantes vienen del nivel 2; requisitos/descripción
    del nivel 3."""
    fuente_id = int(fuente["id"])
    nombre = fuente["nombre"]
    cargo_txt = limpiar_texto(
        cargo.get("nombre") or detalle.get("cargo") or concurso.get("descripcion") or "")[:500]
    estado = concurso.get("estado") or detalle.get("estado")
    cod_concur = concurso.get("id_concur") or concurso.get("codigo") or ""
    cod_cargo = cargo.get("codigo") or cargo.get("id_concar") or ""
    fecha_cierre = cargo.get("fecha_termino") or detalle.get("fecha_cierre")
    vacantes = cargo.get("vacantes") or detalle.get("vacantes")

    desc_partes = []
    if detalle.get("descripcion"):
        desc_partes.append(detalle["descripcion"])
    if tipo_concurso:
        desc_partes.append(f"Tipo de concurso: {tipo_concurso}")
    if concurso.get("codigo"):
        desc_partes.append(f"Código: {concurso['codigo']}")
    if vacantes:
        desc_partes.append(f"Vacantes: {vacantes}")
    if estado:
        desc_partes.append(f"Estado: {estado}")
    descripcion = limpiar_texto(" | ".join(desc_partes)) or None

    # URL estable por cargo (el portal es POST; usamos un fragmento con los ids).
    url = f"{fuente.get('url')}#{cod_concur}-{cod_cargo}"

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo_txt,
                                         f"{cod_concur}-{cod_cargo}" or cargo_txt),
        "fuente_id": fuente_id,
        "url_original": url,
        "cargo": cargo_txt,
        "descripcion": descripcion[:2000] if descripcion else None,
        "institucion_nombre": nombre,
        "sector": SECTOR,
        "area_profesional": normalizar_area(cargo_txt),
        "tipo_cargo": "Concurso Universitario",
        "nivel": _nivel(cargo_txt),
        "region": normalizar_region(fuente["region"]) or fuente["region"],
        "ciudad": fuente.get("ciudad"),
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": cargo.get("fecha_inicio") or date.today(),
        "fecha_cierre": fecha_cierre,
        "requisitos_texto": detalle.get("requisitos"),
        "numero_vacantes": vacantes,
        "url_bases": None,
        "_estado": estado,
    }


# ── Procesamiento por fuente ─────────────────────────────────────────────────
def _enriquecer(oferta: dict, texto: str | None, session) -> None:
    """Completa lo que el parseo del portal no trae, minando el texto del detalle
    y el PDF de perfil/bases (url_bases): requisitos, funciones, correo, salario y
    fecha de cierre. Sólo rellena campos vacíos. Reduce la revisión manual."""
    try:
        from scrapers.enrich import enriquecer_oferta
        pdf = oferta.get("url_bases")
        pdf_urls = [pdf] if (pdf and str(pdf).lower().endswith(".pdf")) else None
        enriquecer_oferta(oferta, texto_html=texto or None,
                          pdf_urls=pdf_urls, session=session)
    except Exception:
        pass


def _procesar_unap(fuente: dict, session, incluir_cerrados: bool) -> list[dict]:
    r = _get(session, fuente["url"])
    if r is None:
        logger.warning("  Sin acceso a %s", fuente["url"])
        return []
    hoy = date.today()
    ofertas, omitidas, vistos = [], 0, set()
    for it in parsear_unap(r.text, fuente):
        o = _construir_unap(it, fuente)
        if o["id_externo"] in vistos or not o["cargo"]:
            continue
        vistos.add(o["id_externo"])
        # Mina el PDF de perfil (url_bases) para requisitos/renta/funciones.
        _enriquecer(o, o.get("descripcion"), session)
        cierre = o.get("fecha_cierre")
        if cierre and cierre < hoy and not incluir_cerrados:
            omitidas += 1
            continue
        ofertas.append(o)
    logger.info("  → %d vigentes (%d omitidas por plazo vencido)",
                len(ofertas), omitidas)
    return ofertas


_UFRO_ESTADO_CERRADO = re.compile(r"cerrad|finaliz|vencid|no\s+vigente", re.I)


# Vistas del portal por POST: cod_concar 1=Administrativos, 2=Académicos,
# 3=Post-Doctoral. El campo del form es Formulario1[cod_concar].
_UFRO_TIPOS = (("1", "Administrativo"), ("2", "Académico"), ("3", "Post-Doctoral"))


def _procesar_ufro(fuente: dict, session, incluir_cerrados: bool) -> list[dict]:
    """Portal UFRO por POST: recorre las 3 vistas (Administrativos/Académicos/
    Post-Doctoral); por cada concurso abre sus cargos (Vacantes, Fecha Término) y
    por cada cargo su ficha (Descripción, Requisitos). Todo vía POST."""
    hoy = date.today()
    base = fuente.get("base") or "https://extranet.ufro.cl/concursos/"
    tipo_url = urljoin(base, "ver_tipo_administrativo.php")
    cargos_url = urljoin(base, "ver_cargos_concurso.php")
    ficha_url = urljoin(base, "ver_postulacion_cargo.php")
    ofertas, omitidas, vistos = [], 0, set()

    for cod_tipo, etiqueta in _UFRO_TIPOS:
        r = _post(session, tipo_url, {"Formulario1[cod_concar]": cod_tipo})
        concursos = parsear_ufro_concursos(r.content) if r is not None else []
        if not concursos:
            continue
        logger.info("  %s: %d concursos", etiqueta, len(concursos))
        for c in concursos:
            if not incluir_cerrados and c.get("estado") and \
                    _UFRO_ESTADO_CERRADO.search(c["estado"]):
                omitidas += 1
                continue
            time.sleep(0.5)
            rc = _post(session, cargos_url, {
                "Formulario[cod_concur]": c["cod_concur"],
                "Formulario[id_concur]": c["id_concur"]})
            cargos = parsear_ufro_cargos(rc.content) if rc is not None else []
            # Sin desglose de cargos: la oferta es el concurso mismo.
            if not cargos:
                cargos = [{"nombre": c["descripcion"], "codigo": c.get("codigo"),
                           "vacantes": None, "fecha_inicio": None, "fecha_termino": None,
                           "cod_concar": None, "cod_concur": c["cod_concur"],
                           "id_concar": None, "id_concur": c["id_concur"]}]
            for cg in cargos:
                detalle: dict = {}
                if cg.get("cod_concar"):
                    time.sleep(0.5)
                    rd = _post(session, ficha_url, {
                        "Formulario[cod_concar]": cg["cod_concar"],
                        "Formulario[cod_concur]": cg["cod_concur"],
                        "Formulario[id_concar]": cg["id_concar"],
                        "Formulario[id_concur]": cg["id_concur"]})
                    if rd is not None:
                        detalle = parsear_ufro_detalle(rd.content)
                o = _construir_ufro_cargo(fuente, c, cg, detalle, etiqueta)
                if not o["cargo"] or o["id_externo"] in vistos:
                    continue
                vistos.add(o["id_externo"])
                if not incluir_cerrados and o.get("_estado") and \
                        _UFRO_ESTADO_CERRADO.search(o["_estado"]):
                    omitidas += 1
                    continue
                _texto = " ".join(filter(None, [o.get("descripcion"),
                                                o.get("requisitos_texto")]))
                _enriquecer(o, _texto or None, session)
                if o["fecha_cierre"] and o["fecha_cierre"] < hoy and not incluir_cerrados:
                    omitidas += 1
                    continue
                o.pop("_estado", None)
                ofertas.append(o)
    logger.info("  → %d vigentes (%d omitidas por estado/plazo)",
                len(ofertas), omitidas)
    return ofertas


def _procesar(fuente: dict, session, incluir_cerrados: bool) -> list[dict]:
    logger.info("─── %s [%s] ───", fuente["nombre"], fuente["modo"])
    if fuente["modo"] == "unap_sispartime":
        return _procesar_unap(fuente, session, incluir_cerrados)
    return _procesar_ufro(fuente, session, incluir_cerrados)


# ── Persistencia / export ────────────────────────────────────────────────────
def _exportar(ofertas, prefijo):
    ser = []
    for o in ofertas:
        f = {k: o.get(k) for k in CAMPOS_EXPORT}
        for k in ("fecha_publicacion", "fecha_cierre"):
            if isinstance(f[k], date):
                f[k] = f[k].isoformat()
        ser.append(f)
    Path(prefijo + ".json").write_text(
        json.dumps(ser, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(prefijo + ".csv", "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS_EXPORT)
        w.writeheader()
        w.writerows(ser)
    logger.info("Exportado: %s.json / %s.csv (%d ofertas)", prefijo, prefijo, len(ser))


def ejecutar(dry_run=False, verbose=False, max_results=None, solo=None,
             incluir_cerrados=False, export=None) -> dict:
    inicio = time.time()
    logger.info("=" * 60)
    logger.info("INICIO - Scraper universidades (portal propio)%s",
                " (standalone)" if STANDALONE else "")
    logger.info("=" * 60)
    if STANDALONE and not dry_run:
        logger.warning("Sin módulos del proyecto: forzando --dry-run")
        dry_run = True

    fuentes = [f for f in FUENTES if not solo or f["clave"] == solo]
    session = _session()
    agg = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0,
           "encontradas": 0}
    todas = []

    for fuente in fuentes:
        db = SessionLocal() if (SessionLocal and not dry_run) else None
        urls_activas = []
        f_stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
        try:
            ofertas = _procesar(fuente, session, incluir_cerrados)
            if max_results:
                ofertas = ofertas[:max_results]
            agg["encontradas"] += len(ofertas)
            todas.extend(ofertas)
            for datos in ofertas:
                urls_activas.append(datos["url_original"])
                if verbose or dry_run:
                    print(f"  [{fuente['sigla']:6}] {datos['cargo'][:46]:46} | "
                          f"{datos['ciudad'][:12] if datos['ciudad'] else '':12} | "
                          f"{datos['tipo_cargo'][:11]:11} | cierre: {datos['fecha_cierre']}")
                if dry_run or db is None:
                    continue
                try:
                    nueva, act = upsert_oferta(db, datos)
                    if nueva:
                        f_stats["nuevas"] += 1
                    elif act:
                        f_stats["actualizadas"] += 1
                except Exception as exc:
                    f_stats["errores"] += 1
                    db.rollback()
                    logger.exception("  Error upsert: %s", exc)
            if db is not None and urls_activas:
                f_stats["cerradas"] = marcar_ofertas_cerradas(
                    db, int(fuente["id"]), sorted(set(urls_activas)))
        except Exception as exc:
            f_stats["errores"] += 1
            if db is not None:
                db.rollback()
            logger.exception("  Error fuente %s: %s", fuente["clave"], exc)
        finally:
            if db is not None:
                try:
                    db.rollback()
                    registrar_log(db, int(fuente["id"]),
                                  "OK" if f_stats["errores"] == 0 else "PARCIAL",
                                  ofertas_nuevas=f_stats["nuevas"],
                                  ofertas_actualizadas=f_stats["actualizadas"],
                                  ofertas_cerradas=f_stats["cerradas"],
                                  paginas=0, duracion=time.time() - inicio)
                except Exception:
                    logger.exception("  No se pudo registrar log")
                db.close()
        for k in f_stats:
            agg[k] += f_stats[k]

    if export and todas:
        _exportar(todas, export)

    dur = time.time() - inicio
    logger.info("RESUMEN universidades portal: fuentes=%d encontradas=%d "
                "nuevas=%d err=%d (%.1fs)", len(fuentes), agg["encontradas"],
                agg["nuevas"], agg["errores"], dur)
    agg["duracion_seg"] = round(dur, 2)
    agg["status"] = "OK" if agg["errores"] == 0 else "PARCIAL"
    return agg


if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)
    p = argparse.ArgumentParser(
        description="Scraper concursos de universidades con portal propio")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--max", type=int, default=None, help="Tope por fuente")
    p.add_argument("--solo", default=None, choices=[f["clave"] for f in FUENTES])
    p.add_argument("--incluir-cerrados", action="store_true")
    p.add_argument("--export", default=None, metavar="PREFIJO")
    a = p.parse_args()
    ejecutar(dry_run=a.dry_run, verbose=a.verbose, max_results=a.max, solo=a.solo,
             incluir_cerrados=a.incluir_cerrados, export=a.export)
