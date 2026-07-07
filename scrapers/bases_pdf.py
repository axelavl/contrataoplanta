"""Lector de BASES de concursos municipales en PDF (decretos alcaldicios).

Los municipios publican las bases como PDF — muchas veces un ESCANEO sin capa
de texto (Viña del Mar). La tarjeta del listado trae solo el título; los datos
reales (cargo, planta, grado, funciones, requisitos, CRONOGRAMA con el cierre
de postulaciones) viven en el PDF. Este módulo:

  1. leer_pdf(contenido)      → texto + método ("texto" | "ocr" | "sin_texto").
                                Extracción normal (pypdf/pdfplumber) y, si el
                                PDF es imagen, OCR con tesseract (mismo patrón
                                que scrapers/carabineros.py; el binario lo
                                instala nixpacks.toml en Railway).
  2. parsear_bases(texto)     → campos estructurados del decreto: número/fecha
                                del acto, planta, grado, vacantes, cargo,
                                objetivo, funciones, competencias,
                                conocimientos, habilidades, requisitos
                                generales/específicos, título requerido,
                                destinación, cronograma completo.
  3. calcular_vigencia(bases) → ("vigente"|"vencido"|"sin_fecha", motivo) según
                                el CRONOGRAMA del PDF, no según la tarjeta
                                ("Nueva · hace menos de 1 día" es la fecha de
                                scrapeo, no la del concurso).
  4. nivel_confianza(...)     → "alto" | "medio" | "bajo" | "manual".
  5. enriquecer_desde_bases() → vuelca todo en el dict de oferta del pipeline
                                de municipios y devuelve (estado, motivo,
                                confianza) para log y filtrado.

Estructura verificada con el decreto real 7738/2024 de Viña del Mar
(Profesional Maestranza): tabla PLANTA/GRADO/VACANTES/CARGO A DESEMPEÑAR,
secciones 2.1 Objetivo · 2.2 Funciones · 2.3 Competencias · 2.4 Conocimientos ·
2.5 Habilidades · 3.1/3.2 Requisitos · 4. Destinaciones · 8. Cronograma.
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date
from typing import Any

logger = logging.getLogger("scraper.bases_pdf")

try:
    from pypdf import PdfReader  # type: ignore
except Exception:  # pragma: no cover
    PdfReader = None  # type: ignore[assignment]

try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None  # type: ignore[assignment]

# OCR perezoso (mismo patrón que carabineros.py): pytesseract está en
# requirements y el binario tesseract(+spa) lo instala nixpacks.toml.
try:
    import pytesseract  # type: ignore
except Exception:  # pragma: no cover
    pytesseract = None  # type: ignore[assignment]

#: Menos de esto de texto alfanumérico por página promedio = PDF escaneado.
_MIN_CHARS_PAGINA = 80
#: Tope de páginas a procesar (los decretos rondan 8-12 páginas).
MAX_PAGINAS = 12

MESES = {
    "enero": 1, "ene": 1, "febrero": 2, "feb": 2, "marzo": 3, "mar": 3,
    "abril": 4, "abr": 4, "mayo": 5, "may": 5, "junio": 6, "jun": 6,
    "julio": 7, "jul": 7, "agosto": 8, "ago": 8,
    "septiembre": 9, "setiembre": 9, "sep": 9, "set": 9,
    "octubre": 10, "oct": 10, "noviembre": 11, "nov": 11,
    "diciembre": 12, "dic": 12,
}

_RE_FECHA_ES = re.compile(
    r"(\d{1,2})\s*(?:de\s+)?([A-Za-zÁÉÍÓÚáéíóú]{3,12})\.?\s*(?:de(?:l)?\s+)?(\d{4})")


def _fecha_es(texto: str | None) -> date | None:
    """'17 de Junio de 2024', '04 JUN. 2024', '1 Agosto 2024' → date."""
    if not texto:
        return None
    m = _RE_FECHA_ES.search(texto)
    if not m:
        return None
    mes = MESES.get(m.group(2).lower().rstrip("."))
    if not mes:
        return None
    try:
        return date(int(m.group(3)), mes, int(m.group(1)))
    except ValueError:
        return None


def _ultima_fecha(texto: str | None) -> date | None:
    """Última fecha de un rango ('17 de Junio al 08 de Julio de 2024')."""
    if not texto:
        return None
    fechas = []
    for m in _RE_FECHA_ES.finditer(texto):
        mes = MESES.get(m.group(2).lower().rstrip("."))
        if mes:
            try:
                fechas.append(date(int(m.group(3)), mes, int(m.group(1))))
            except ValueError:
                pass
    return max(fechas) if fechas else None


# ── 1) Lectura del PDF (texto → OCR) ─────────────────────────────────────────

def _texto_capa(contenido: bytes, max_paginas: int) -> str:
    """Capa de texto del PDF (pypdf, con pdfplumber de respaldo)."""
    if contenido[:5] != b"%PDF-":
        return ""
    if PdfReader is not None:
        try:
            r = PdfReader(io.BytesIO(contenido))
            return "\n".join((p.extract_text() or "") for p in r.pages[:max_paginas])
        except Exception:
            pass
    if pdfplumber is not None:
        try:
            with pdfplumber.open(io.BytesIO(contenido)) as pdf:
                return "\n".join((p.extract_text() or "") for p in pdf.pages[:max_paginas])
        except Exception:
            pass
    return ""


_TESSERACT_OK: bool | None = None


def _tesseract_listo() -> bool:
    """¿Está el BINARIO tesseract en el sistema? pytesseract (pip) solo lo
    envuelve; sin binario, image_to_string lanza TesseractNotFoundError en cada
    página — y eso se tragaba mudo, dejando todo en "sin_texto" sin pista.
    Se chequea una vez por proceso y se avisa FUERTE si falta."""
    global _TESSERACT_OK
    if _TESSERACT_OK is None:
        try:
            pytesseract.get_tesseract_version()
            _TESSERACT_OK = True
        except Exception as exc:
            _TESSERACT_OK = False
            logger.warning(
                "OCR deshabilitado: el binario tesseract no está instalado en el "
                "sistema (%s). En Railway (builder Railpack) se instala con "
                "railpack.json → deploy.aptPackages o con la variable "
                "RAILPACK_DEPLOY_APT_PACKAGES=tesseract-ocr,tesseract-ocr-spa; "
                "nixpacks.toml solo aplica si el builder es Nixpacks.",
                type(exc).__name__)
    return _TESSERACT_OK


def ocr_disponible() -> tuple[bool, str]:
    """(operativo, detalle) — para loguear al inicio de cada corrida y que el
    estado del OCR quede a la vista en vez de deducirse de 77 'sin_texto'."""
    if pdfplumber is None:
        return False, "pdfplumber no instalado (pip)"
    if pytesseract is None:
        return False, "pytesseract no instalado (pip)"
    if not _tesseract_listo():
        return False, "binario tesseract ausente en el sistema (aptPackages)"
    return True, "tesseract operativo (spa)"


def _ocr(contenido: bytes, max_paginas: int) -> str:
    """OCR de un PDF escaneado (pdfplumber render + tesseract spa).

    Se corta temprano si ya apareció el cronograma con una fecha de cierre,
    porque el OCR es lo caro (~segundos por página)."""
    if pdfplumber is None or pytesseract is None:
        logger.warning("  OCR no disponible (pytesseract/pdfplumber no instalado)")
        return ""
    if not _tesseract_listo():
        return ""
    paginas: list[str] = []
    err_paginas, primer_error = 0, ""
    try:
        with pdfplumber.open(io.BytesIO(contenido)) as pdf:
            for p in pdf.pages[:max_paginas]:
                try:
                    img = p.to_image(resolution=200).original
                    paginas.append(pytesseract.image_to_string(img, lang="spa"))
                except Exception as exc:
                    # No tragar el error: era exactamente lo que ocultaba el
                    # tesseract faltante en producción.
                    err_paginas += 1
                    if not primer_error:
                        primer_error = f"{type(exc).__name__}: {exc}"[:160]
                    paginas.append("")
                acumulado = "\n".join(paginas)
                if re.search(r"cronograma", acumulado, re.I) and \
                   re.search(r"cierre\s+de\s+recepci[oó]n[^\n]*\d{4}", acumulado, re.I):
                    break   # ya tenemos lo crítico: no OCR-ear anexos
    except Exception as exc:
        logger.warning("  OCR falló: %s: %s", type(exc).__name__, str(exc)[:120])
        return ""
    if err_paginas:
        logger.warning("  OCR: %d página(s) fallaron (%s)", err_paginas, primer_error)
    return "\n".join(paginas)


def leer_pdf(contenido: bytes, *, allow_ocr: bool = True,
             max_paginas: int = MAX_PAGINAS) -> tuple[str, str]:
    """Texto del PDF y método usado: ("texto"|"ocr"|"sin_texto")."""
    if not contenido:
        return "", "sin_texto"
    texto = _texto_capa(contenido, max_paginas)
    alnum = len(re.sub(r"[^A-Za-z0-9ÁÉÍÓÚÑáéíóúñ]", "", texto))
    if alnum >= _MIN_CHARS_PAGINA:
        return texto, "texto"
    if allow_ocr:
        ocr = _ocr(contenido, max_paginas)
        if len(re.sub(r"[^A-Za-z0-9ÁÉÍÓÚÑáéíóúñ]", "", ocr)) >= _MIN_CHARS_PAGINA:
            return ocr, "ocr"
    return texto, "sin_texto"


# ── 2) Parseo del decreto ────────────────────────────────────────────────────

# Encabezados de sección del decreto (2.1, 2.2, …). Tolerantes a OCR: sin
# exigir la numeración exacta, con acentos opcionales.
_SECCIONES = [
    ("objetivo", r"objetivo\s+del\s+cargo"),
    ("funciones", r"funciones"),
    ("competencias", r"competencias"),
    ("conocimientos", r"conocimientos\s+espec[ií]ficos|conocimientos"),
    ("habilidades", r"habilidades"),
    ("requisitos_generales", r"requisitos\s+generales"),
    ("requisitos_especificos", r"requisitos\s+espec[ií]ficos"),
    ("destinacion", r"destinacion(?:es)?|destinaci[oó]n(?:es)?"),
    ("documentos", r"documentos\s+(?:requeridos|a\s+presentar|de\s+postulaci[oó]n)|antecedentes\s+de\s+postulaci[oó]n"),
    ("postulacion", r"(?:de\s+la\s+|modalidad\s+de\s+)?postulaci[oó]n(?:es)?"),
    ("cronograma", r"cronograma(?:\s+(?:del\s+)?concurso)?"),
]

#: Etapas del cronograma → clave normalizada. El cierre usa _ultima_fecha por
#: si viene con hora o rango.
_ETAPAS = [
    ("publicacion", r"publicaci[oó]n\s+del\s+llamado|entrega\s+y\s+publicaci[oó]n\s+de\s+bases"),
    ("cierre_postulacion", r"cierre\s+de\s+recepci[oó]n\s+de\s+postulaci|cierre\s+de\s+postulaci"),
    ("evaluacion", r"evaluaci[oó]n"),
    ("entrevistas", r"entrevistas?"),
    ("resolucion_concurso", r"resoluci[oó]n\s+del\s+concurso"),
    ("asuncion_cargo", r"asunci[oó]n\s+(?:al|del)\s+cargo"),
]

# Actas de RESULTADOS del concurso (terna/selección/desierto): el municipio
# reutiliza el número D.A y a veces reemplaza el PDF de bases por el acta.
# Un "resultado" implica proceso RESUELTO → vencido, aunque no haya cronograma.
_RE_RESULTADOS = re.compile(
    r"informa\s+terna|ha\s+seleccionado|puntaje\s+final|"
    r"resultado[s]?\s+(?:del\s+)?concurso|declara(?:r|do)?\s+desierto", re.I)

_RE_JORNADA_HRS = re.compile(
    r"jornada\s+(?:laboral|de\s+trabajo)?\s*(?:de|:)?\s*(\d{1,2})\s*horas", re.I)

_RE_DECRETO_NUM = re.compile(
    r"(?:decreto(?:\s+alcaldicio)?|resoluci[oó]n(?:\s+exenta)?|n[°º]\s*)\s*[:n°º]*\s*([\d.]{3,9})\b", re.I)

# Fila de la tabla "PLANTA | GRADO | VACANTES | CARGO A DESEMPEÑAR":
# "Profesionales 11° 1 Profesional Maestranza" (el OCR la aplana en una línea).
_RE_FILA_CARGO = re.compile(
    r"\b(Profesional(?:es)?|Jefatura?s?|T[eé]cnic[oa]s?|Administrativ[oa]s?|"
    r"Auxiliar(?:es)?|Directiv[oa]s?)\s+(\d{1,2})[°º]?\s+(\d{1,3})\s+(.{4,80}?)\s*$",
    re.I | re.M)


def _bullets(texto: str) -> list[str]:
    """Divide una sección en viñetas ('- x', '• x' o líneas sueltas)."""
    items: list[str] = []
    for trozo in re.split(r"(?:^|\n)\s*[-•·—]\s+", texto):
        t = re.sub(r"\s+", " ", trozo).strip(" .;-")
        if not (8 <= len(t) <= 400) or re.match(r"^\d+(\.\d+)?\s*$", t):
            continue
        # Frases-intro de la sección ("Los requisitos asociados al presente
        # concurso son los siguientes:") no son viñetas.
        if t.endswith(":") or re.search(r"son\s+los\s+siguientes:?$", t, re.I):
            continue
        items.append(t)
    return items[:20]


def _cortar_secciones(texto: str) -> dict[str, str]:
    """Parte el texto del decreto por encabezados de sección conocidos."""
    plano = re.sub(r"[ \t]+", " ", texto)
    marcas: list[tuple[int, str]] = []
    for clave, patron in _SECCIONES:
        # Encabezado: al inicio de línea, opcionalmente numerado ("2.2 Funciones")
        # y con cola parentética opcional ("Conocimientos específicos (deseables,
        # no excluyentes):").
        rx = re.compile(
            rf"(?:^|\n)\s*(?:\d+(?:\.\d+)?\.?\s*)?(?:{patron})\s*"
            rf"(?:\([^)\n]{{0,60}}\))?\s*:?\s*(?:\n|:)", re.I)
        m = rx.search(plano)
        if m:
            # (inicio_para_ordenar, fin_del_encabezado, clave): el cuerpo parte
            # DESPUÉS del encabezado — antes se colaba "2.1 Objetivo del cargo:"
            # dentro del propio contenido de la sección.
            marcas.append((m.start(), m.end(), clave))
    marcas.sort()
    out: dict[str, str] = {}
    for i, (pos, fin_enc, clave) in enumerate(marcas):
        fin = marcas[i + 1][0] if i + 1 < len(marcas) else min(len(plano), fin_enc + 4000)
        out.setdefault(clave, plano[fin_enc:fin].strip())
    return out


def parsear_bases(texto: str) -> dict[str, Any]:
    """Campos estructurados del decreto de bases. Devuelve {} si el texto es
    demasiado pobre para extraer algo."""
    if not texto or len(texto) < 120:
        return {}
    b: dict[str, Any] = {"texto_len": len(texto)}

    if m := _RE_DECRETO_NUM.search(texto[:1500]):
        b["decreto_numero"] = m.group(1).rstrip(".")
    # Fecha del decreto: primera fecha del encabezado ("VIÑA DEL MAR, 04 JUN. 2024")
    if f := _fecha_es(texto[:600]):
        b["decreto_fecha"] = f

    if m := _RE_FILA_CARGO.search(texto[:4000]):
        b["planta"] = m.group(1).title()
        b["grado"] = m.group(2)
        b["vacantes"] = int(m.group(3))
        b["cargo_desempenar"] = re.sub(r"\s+", " ", m.group(4)).strip(" .").title()

    if m := re.search(r"(?:renta|remuneraci[oó]n|sueldo)\s+(?:bruta?|l[ií]quida)?[^\n]{0,60}?\$?\s*([\d.]{6,12})", texto, re.I):
        b["renta_texto"] = re.sub(r"\s+", " ", m.group(0))[:120]
    if m := _RE_JORNADA_HRS.search(texto):
        b["jornada"] = f"{m.group(1)} horas"
    # Acta de resultados (terna/selección) sin cronograma de postulación: el
    # proceso ya está resuelto — NO es una convocatoria.
    if _RE_RESULTADOS.search(texto) and not re.search(
            r"cierre\s+de\s+(?:recepci[oó]n|postulaci)", texto, re.I):
        b["es_resultado"] = True

    secciones = _cortar_secciones(texto)
    if s := secciones.get("objetivo"):
        b["objetivo"] = re.sub(r"\s+", " ", s)[:1200].strip()
    for clave in ("funciones", "competencias", "conocimientos", "habilidades",
                  "requisitos_especificos", "documentos"):
        if s := secciones.get(clave):
            items = _bullets(s)
            if items:
                b[clave] = items
    if s := secciones.get("requisitos_generales"):
        b["requisitos_generales"] = re.sub(r"\s+", " ", s)[:1500].strip()
    if s := secciones.get("destinacion"):
        b["destinacion"] = re.sub(r"\s+", " ", s)[:400].strip()

    # Título requerido: dentro de requisitos específicos.
    for item in b.get("requisitos_especificos", []):
        if re.search(r"t[ií]tulo\s+(?:profesional|t[eé]cnico)", item, re.I):
            b.setdefault("titulo_requerido", item)

    # Cronograma: buscar cada etapa en la sección (o en todo el texto si el
    # corte falló — el OCR a veces pierde el encabezado).
    zona = secciones.get("cronograma") or texto
    crono: dict[str, Any] = {}
    for clave, patron in _ETAPAS:
        if m := re.search(rf"(?:{patron})[^\n]{{0,80}}", zona, re.I):
            if f := _ultima_fecha(m.group(0)):
                crono[clave] = f
    if crono:
        b["cronograma"] = crono
        if "cierre_postulacion" in crono:
            b["fecha_cierre"] = crono["cierre_postulacion"]
        if "publicacion" in crono:
            b["fecha_publicacion"] = crono["publicacion"]
    return b


# ── 3) Vigencia según el PDF ─────────────────────────────────────────────────

def calcular_vigencia(bases: dict[str, Any], hoy: date | None = None) -> tuple[str, str]:
    """("vigente"|"vencido"|"sin_fecha", motivo). La verdad es el cronograma
    del PDF — nunca la etiqueta "Nueva" de la tarjeta (esa es fecha de scrapeo)."""
    hoy = hoy or date.today()
    if bases.get("es_resultado"):
        return "vencido", ("El PDF corresponde a resultados del concurso "
                           "(terna/selección): proceso ya resuelto")
    cierre = bases.get("fecha_cierre")
    if cierre is None:
        # Acto administrativo antiguo sin cierre detectable: en páginas que
        # acumulan concursos de años anteriores, publicarlo como vigente es
        # peor que omitirlo. >90 días desde el decreto → vencido.
        df = bases.get("decreto_fecha")
        if df and (hoy - df).days > 90:
            return "vencido", (f"Las bases datan del {df.isoformat()} (>90 días) "
                               "y no traen fecha de cierre detectable")
        return "sin_fecha", "Las bases no traen fecha de cierre detectable; revisar manualmente"
    if cierre < hoy:
        return "vencido", f"La fecha de cierre de postulación fue {cierre.isoformat()}"
    return "vigente", f"Postulaciones abiertas hasta {cierre.isoformat()}"


# ── 4) Confianza de la extracción ────────────────────────────────────────────

def nivel_confianza(bases: dict[str, Any], metodo: str) -> str:
    """alto: fechas y cargo detectados · medio: parcial · bajo: OCR pobre ·
    manual: nada utilizable (vigencia indeterminable)."""
    if not bases:
        return "manual"
    tiene_cierre = bases.get("fecha_cierre") is not None
    tiene_cargo = bool(bases.get("cargo_desempenar") or bases.get("funciones"))
    if tiene_cierre and tiene_cargo:
        return "alto"
    if tiene_cierre or tiene_cargo:
        return "medio"
    return "bajo" if metodo == "ocr" else "manual"


# ── 5) Integración con el pipeline de municipios ─────────────────────────────

def enriquecer_desde_bases(oferta: dict[str, Any], contenido_pdf: bytes,
                           *, allow_ocr: bool = True,
                           hoy: date | None = None) -> dict[str, Any]:
    """Lee el PDF de bases y vuelca los campos en la oferta. Ver
    enriquecer_desde_texto para el detalle; esto solo antepone la lectura."""
    texto, metodo = leer_pdf(contenido_pdf, allow_ocr=allow_ocr)
    return enriquecer_desde_texto(oferta, texto, metodo, hoy=hoy)


def enriquecer_desde_texto(oferta: dict[str, Any], texto: str, metodo: str,
                           *, hoy: date | None = None) -> dict[str, Any]:
    """Vuelca los campos de las bases en la oferta (solo completa, no pisa
    datos ya presentes salvo la fecha de cierre, donde el PDF manda).

    Devuelve un resumen para log/decisiones:
      {metodo, estado, motivo, confianza, campos: [..], cronograma}"""
    bases = parsear_bases(texto)
    estado, motivo = calcular_vigencia(bases, hoy)
    confianza = nivel_confianza(bases, metodo)

    campos: list[str] = []
    if bases.get("fecha_cierre"):
        oferta["fecha_cierre"] = bases["fecha_cierre"]   # el PDF manda
        campos.append("fecha_cierre")
    if bases.get("vacantes") and not oferta.get("numero_vacantes"):
        oferta["numero_vacantes"] = bases["vacantes"]
        campos.append("vacantes")
    if bases.get("renta_texto") and not oferta.get("renta_texto"):
        oferta["renta_texto"] = bases["renta_texto"]
        campos.append("renta")
    if bases.get("jornada") and not oferta.get("jornada"):
        oferta["jornada"] = bases["jornada"]
        campos.append("jornada")

    # Cargo más específico: "Profesional Maestranza" le gana al título del
    # listado si el actual es genérico o arrastra basura del bloque.
    cargo_pdf = bases.get("cargo_desempenar")
    if cargo_pdf and (not oferta.get("cargo")
                      or len(oferta["cargo"]) > 70
                      or re.search(r"seg[uú]n$|desempeñar", oferta["cargo"], re.I)):
        oferta["cargo"] = cargo_pdf[:500]
        campos.append("cargo")

    partes_desc: list[str] = []
    ficha = " · ".join(p for p in (
        f"Planta {bases['planta']}" if bases.get("planta") else "",
        f"Grado {bases['grado']}" if bases.get("grado") else "",
        f"{bases['vacantes']} vacante(s)" if bases.get("vacantes") else "",
        f"Decreto {bases['decreto_numero']}" if bases.get("decreto_numero") else "",
    ) if p)
    if bases.get("destinacion"):
        # Dentro de la línea "Condiciones:" el frontend la clasifica ahí; como
        # línea propia caía dentro de Funciones (no conoce ese encabezado).
        ficha = (ficha + " · " if ficha else "") + "Destinación: " + bases["destinacion"][:250]
    if ficha:
        # Como "Condiciones:" el frontend la clasifica en su sección en vez de
        # dejar "Planta X"/"Decreto N" sueltos en Información adicional.
        partes_desc.append("Condiciones: " + ficha + ".")
    if bases.get("objetivo"):
        partes_desc.append("Objetivo del cargo: " + bases["objetivo"])
        campos.append("objetivo")
    if bases.get("funciones"):
        partes_desc.append("Funciones: " + " ".join(f"- {f}." for f in bases["funciones"]))
        campos.append("funciones")
    if partes_desc:
        nueva_desc = "\n".join(partes_desc)[:2000]
        if len(nueva_desc) > len(oferta.get("descripcion") or ""):
            oferta["descripcion"] = nueva_desc

    partes_req: list[str] = []
    if bases.get("titulo_requerido"):
        partes_req.append("Título requerido: " + bases["titulo_requerido"])
        campos.append("titulo_requerido")
    for etiqueta, clave in (("Requisitos específicos", "requisitos_especificos"),
                            ("Conocimientos", "conocimientos"),
                            ("Competencias", "competencias"),
                            ("Habilidades", "habilidades")):
        if bases.get(clave):
            items = bases[clave]
            if clave == "conocimientos":
                # "Conocimientos Ley 18.695…" → "Conocimientos de la Ley…": la
                # cascada del frontend clasifica "Conocimientos en|de …" como
                # competencias; sin la preposición caía a residual.
                items = [re.sub(r"^Conocimientos\s+(?=Ley\b)", "Conocimientos de la ", x)
                         for x in items]
            partes_req.append(f"{etiqueta}: " + " ".join(f"- {x}." for x in items))
            campos.append(clave)
    if bases.get("requisitos_generales"):
        gen = re.sub(r"^.{0,160}?siguientes\s*:\s*", "", bases["requisitos_generales"])
        partes_req.append("Requisitos generales: " + gen[:600])
    if partes_req:
        nueva_req = "\n".join(partes_req)[:2000]
        if len(nueva_req) > len(oferta.get("requisitos_texto") or ""):
            oferta["requisitos_texto"] = nueva_req

    return {"metodo": metodo, "estado": estado, "motivo": motivo,
            "confianza": confianza, "campos": campos,
            "cronograma": bases.get("cronograma") or {}}
