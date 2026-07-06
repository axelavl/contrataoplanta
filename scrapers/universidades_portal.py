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
     # Cada fila del listado enlaza a la ficha del concurso, de donde se extraen
     # Cargo, Descripción, Vacantes, Fecha Término, Estado y Requisitos.
     # NOTA: `ver_tipo_academico.php` responde 404 (URL tentativa incorrecta); se
     # deja sólo la vista administrativa hasta confirmar la URL real de académicos.
     "urls": [
         "https://extranet.ufro.cl/concursos/ver_tipo_administrativo.php",
     ]},
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


# ── Modo UFRO (tabla + ficha de detalle) ─────────────────────────────────────
def parsear_ufro(html: str, fuente: dict) -> list[dict]:
    """Lista de concursos con su enlace a la ficha. Soporta la vista de tabla
    (Código, Descripción, Link, Tipo, Estado) y, como respaldo, una lista de
    enlaces a fichas (`ver_concurso*.php`, `ficha*.php`, `?id=`)."""
    soup = BeautifulSoup(html, "html.parser")
    texto = limpiar_texto(soup.get_text(" ", strip=True))
    if re.search(r"no hay ofertas? laborales? disponibles?", texto, re.I):
        return []
    items = []
    for tabla in soup.find_all("table"):
        encabezados = [limpiar_texto(c.get_text()).lower()
                       for c in (tabla.find("tr").find_all(["th", "td"])
                                 if tabla.find("tr") else [])]
        if not any("descrip" in e or "cargo" in e for e in encabezados):
            continue

        def idx(*claves):
            for i, e in enumerate(encabezados):
                if any(k in e for k in claves):
                    return i
            return None

        i_cod, i_desc = idx("código", "codigo"), idx("descrip", "cargo")
        i_tipo, i_estado = idx("tipo"), idx("estado")
        for fila in tabla.find_all("tr")[1:]:
            celdas = fila.find_all("td")
            if len(celdas) < 2:
                continue
            txt = [limpiar_texto(c.get_text(" ", strip=True)) for c in celdas]

            def cel(i):
                return txt[i] if i is not None and i < len(txt) else None

            desc = cel(i_desc)
            if not desc:
                continue
            estado = cel(i_estado)
            # Enlace a la ficha (columna Link "VER"): preferimos un href real,
            # ignorando anclas "#"/javascript que no llevan a ninguna página.
            link = None
            for a in fila.find_all("a", href=True):
                href = (a["href"] or "").strip()
                if href and not href.startswith(("#", "javascript:")):
                    link = a
                    break
            items.append({
                "cargo": desc, "codigo": cel(i_cod), "tipo": cel(i_tipo),
                "estado": estado,
                "url": urljoin(fuente["url"], link["href"]) if link else None,
            })
        break

    # Respaldo: sin tabla reconocible, tomar los enlaces a fichas de concurso.
    if not items:
        vistos: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.search(r"(ver_?concurso|ficha|detalle|postul|concurso)"
                             r"[^/]*\.php|[?&]id=\d+", href, re.I):
                continue
            url = urljoin(fuente["url"], href)
            if url in vistos or url.rstrip("/") == fuente["url"].rstrip("/"):
                continue
            vistos.add(url)
            items.append({
                "cargo": limpiar_texto(a.get_text(" ", strip=True)) or None,
                "codigo": None, "tipo": None, "estado": None, "url": url,
            })
    return items


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


def _construir_ufro(it: dict, fuente: dict, detalle: dict | None = None) -> dict:
    detalle = detalle or {}
    fuente_id = int(fuente["id"])
    nombre = fuente["nombre"]
    # El cargo de la ficha suele ser el bueno; si no, el del listado.
    cargo = limpiar_texto(detalle.get("cargo") or it.get("cargo") or "")[:500]
    codigo = it.get("codigo")
    estado = detalle.get("estado") or it.get("estado")

    desc_partes = []
    if detalle.get("descripcion"):
        desc_partes.append(detalle["descripcion"])
    if codigo:
        desc_partes.append(f"Código: {codigo}")
    if it.get("tipo"):
        desc_partes.append(f"Tipo: {it['tipo']}")
    if detalle.get("vacantes"):
        desc_partes.append(f"Vacantes: {detalle['vacantes']}")
    if estado:
        desc_partes.append(f"Estado: {estado}")
    descripcion = limpiar_texto(" | ".join(desc_partes)) or None

    return {
        "id_externo": generar_id_estable(fuente_id, nombre, cargo,
                                         codigo or it.get("url") or cargo),
        "fuente_id": fuente_id,
        "url_original": it.get("url") or fuente["url"],
        "cargo": cargo,
        "descripcion": descripcion[:2000] if descripcion else None,
        "institucion_nombre": nombre,
        "sector": SECTOR,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": (it.get("tipo") or "Concurso Universitario"),
        "nivel": _nivel(cargo),
        "region": normalizar_region(fuente["region"]) or fuente["region"],
        "ciudad": fuente.get("ciudad"),
        "renta_bruta_min": None,
        "renta_bruta_max": None,
        "renta_texto": None,
        "fecha_publicacion": date.today(),
        "fecha_cierre": detalle.get("fecha_cierre"),
        "requisitos_texto": detalle.get("requisitos"),
        "numero_vacantes": detalle.get("vacantes"),
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


def _procesar_ufro(fuente: dict, session, incluir_cerrados: bool) -> list[dict]:
    """Recorre cada vista (administrativo/académico), entra a la ficha de cada
    concurso y arma la oferta con Cargo, Descripción, Fecha Término, Vacantes,
    Estado y Requisitos. Sin ficha (fila sin enlace) se usa lo del listado."""
    hoy = date.today()
    ofertas, omitidas, vistos = [], 0, set()
    urls = fuente.get("urls") or [fuente["url"]]
    for url_listado in urls:
        r = _get(session, url_listado)
        if r is None:
            logger.warning("  Sin acceso a %s", url_listado)
            continue
        crudos = parsear_ufro(r.text, {**fuente, "url": url_listado})
        if not crudos:
            logger.info("  Sin concursos en %s", url_listado.rsplit("/", 1)[-1])
            continue
        for it in crudos:
            detalle: dict = {}
            if it.get("url"):
                time.sleep(0.6)
                rd = _get(session, it["url"])
                if rd is not None:
                    detalle = parsear_ufro_detalle(rd.text)
            o = _construir_ufro(it, fuente, detalle)
            if not o["cargo"] or o["id_externo"] in vistos:
                continue
            vistos.add(o["id_externo"])
            # Estado cerrado/finalizado → no publicar (salvo --incluir-cerrados).
            if not incluir_cerrados and o.get("_estado") and \
                    _UFRO_ESTADO_CERRADO.search(o["_estado"]):
                omitidas += 1
                continue
            # Enriquecer con el texto de la ficha (correo, funciones, salario…).
            _texto_ficha = " ".join(filter(None, [o.get("descripcion"),
                                                  o.get("requisitos_texto")]))
            _enriquecer(o, _texto_ficha or None, session)
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
