"""
Módulo compartido de enriquecimiento para scrapers.

Centraliza la extracción de campos que los scrapers ad-hoc no cubren:
salario, requisitos, email, tipo de contrato, funciones, jornada/modalidad.
También descarga y parsea PDFs de bases/perfil para extraer información
adicional.

Uso típico desde cualquier scraper:

    from scrapers.enrich import enriquecer_oferta

    oferta = construir_oferta(item, detalle)
    enriquecer_oferta(oferta, texto_html=detalle.get("texto_completo"),
                      pdf_urls=[bases_url], session=session)
"""

from __future__ import annotations

import io
import logging
import re
from typing import Any

logger = logging.getLogger("scraper.enrich")

# ── Importaciones opcionales (no rompen si faltan) ────────────────────────

try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None

try:
    from extraction.salary_extractor import extract_salary
except ImportError:
    extract_salary = None  # type: ignore[assignment]

try:
    from extraction.requirements_extractor import extract_requirements
except ImportError:
    extract_requirements = None  # type: ignore[assignment]

try:
    from extraction.email_extractor import extract_and_classify_emails
except ImportError:
    extract_and_classify_emails = None  # type: ignore[assignment]

try:
    from extraction.contract_extractor import extract_contract_info
except ImportError:
    extract_contract_info = None  # type: ignore[assignment]

try:
    from extraction.functions_extractor import extract_functions
except ImportError:
    extract_functions = None  # type: ignore[assignment]

try:
    from extraction.text_sections import (
        extraer_pasos_postulacion,
        unir_lineas_envueltas,
    )
except ImportError:  # pragma: no cover
    unir_lineas_envueltas = None  # type: ignore[assignment]
    extraer_pasos_postulacion = None  # type: ignore[assignment]


# ── PDF ───────────────────────────────────────────────────────────────────

_RELEVANT_PDF_PATTERNS = re.compile(
    r"(?:bases|perfil|tdr|t[eé]rminos?\s*de\s*referencia|concurso|convocatoria"
    r"|postulaci[oó]n|cargo|llamado|requisitos|antecedentes)",
    re.IGNORECASE,
)


def _es_pdf_relevante(url: str) -> bool:
    """True si el nombre del archivo sugiere que contiene info de la oferta."""
    nombre = url.rsplit("/", 1)[-1].lower()
    if not nombre.endswith(".pdf"):
        return False
    return bool(_RELEVANT_PDF_PATTERNS.search(nombre))


def descargar_pdf(url: str, session: Any = None, timeout: int = 20) -> bytes:
    """Descarga un PDF. Acepta requests.Session o usa requests directo."""
    try:
        if session is not None:
            r = session.get(url, timeout=timeout)
        else:
            import requests
            r = requests.get(url, timeout=timeout, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
            })
        if r.status_code == 200 and r.content[:5] == b"%PDF-":
            return r.content
    except Exception as exc:
        logger.debug("  No se pudo descargar PDF %s: %s", url[:80], exc)
    return b""


def extraer_texto_pdf(contenido: bytes, max_paginas: int = 8) -> str:
    """Extrae texto de un PDF con pdfplumber. '' si no se puede."""
    if pdfplumber is None or not contenido or contenido[:5] != b"%PDF-":
        return ""
    try:
        with pdfplumber.open(io.BytesIO(contenido)) as pdf:
            return "\n".join(
                (p.extract_text() or "") for p in pdf.pages[:max_paginas]
            )
    except Exception:
        return ""


def descargar_y_leer_pdfs(
    urls: list[str],
    session: Any = None,
    solo_relevantes: bool = True,
    max_pdfs: int = 3,
) -> list[tuple[str, str]]:
    """Descarga y extrae texto de PDFs. Retorna [(url, texto), ...]."""
    resultados: list[tuple[str, str]] = []
    procesados = 0
    for url in urls:
        if procesados >= max_pdfs:
            break
        if solo_relevantes and not _es_pdf_relevante(url):
            continue
        binario = descargar_pdf(url, session=session)
        if not binario:
            continue
        texto = extraer_texto_pdf(binario)
        if texto.strip():
            resultados.append((url, texto))
            procesados += 1
    return resultados


# ── Extracción de campos desde texto ──────────────────────────────────────

def _limpiar(t: str | None) -> str:
    return re.sub(r"\s+", " ", t or "").strip()


def _limpiar_lineas(t: str | None) -> str:
    """Como ``_limpiar`` pero PRESERVA los saltos de línea.

    Las secciones rotuladas ("Funciones:", "Cómo postular:") se anexan en
    líneas propias para que el front (rich-text) detecte los encabezados y
    clasifique cada bloque. Colapsa solo espacios/tabs dentro de cada línea.
    """
    lineas = [re.sub(r"[ \t]+", " ", ln).strip()
              for ln in (t or "").replace("\r", "\n").split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lineas)).strip()


def _extraer_salario(texto: str, oferta: dict) -> None:
    """Extrae salario si no está ya presente en la oferta."""
    if extract_salary is None:
        return
    if oferta.get("renta_bruta_min") or oferta.get("renta_texto"):
        return
    sal = extract_salary(texto)
    if sal.amount:
        oferta["renta_bruta_min"] = int(sal.amount)
        oferta["renta_bruta_max"] = int(sal.amount)
        if sal.raw:
            oferta["renta_texto"] = _limpiar(sal.raw)[:200]


def _extraer_requisitos(texto: str, oferta: dict) -> None:
    """Extrae requisitos si no están ya presentes."""
    if extract_requirements is None:
        return
    if oferta.get("requisitos_texto"):
        return
    req, des, docs = extract_requirements(texto)
    partes: list[str] = []
    if req:
        partes.append("Requisitos: " + "; ".join(req))
    if des:
        partes.append("Deseables: " + "; ".join(des))
    if docs:
        partes.append("Documentos: " + "; ".join(docs[:6]))
    if partes:
        # Saltos de línea entre bloques rotulados → el front los clasifica.
        oferta["requisitos_texto"] = _limpiar_lineas("\n".join(partes))[:1900]


def _extraer_email(texto: str, oferta: dict) -> None:
    """Extrae emails y los agrega a la descripción si no están presentes."""
    if extract_and_classify_emails is None:
        return
    result = extract_and_classify_emails(texto)
    if not result.classified:
        return

    # Poblar los CAMPOS dedicados (email_postulacion / email_consultas) si el
    # scraper no los trajo: son los que la ficha muestra como "Correo de
    # postulación / consultas". Antes sólo se anexaban al texto de la descripción
    # y las columnas quedaban NULL, así que la tarjeta no mostraba el correo.
    #
    # Conservador a propósito: sólo poblamos `email_postulacion` y sólo con
    # clasificación EXPLÍCITA (kind == email_postulacion). El clasificador no
    # siempre acierta postulación vs consultas; ante la duda NO tocamos la
    # columna para no mostrar un correo bajo una etiqueta equivocada — el
    # extractor del front igual lo encuentra en el texto de la descripción (que
    # se sigue anexando abajo). No auto-poblamos `email_consultas` desde aquí
    # por ese riesgo de mala clasificación; los scrapers que lo sepan con
    # certeza (p.ej. municipios) lo setean directo.
    posts = [e.email for e in result.classified if "email_postulacion" in e.kinds]
    if posts and not oferta.get("email_postulacion"):
        oferta["email_postulacion"] = ", ".join(dict.fromkeys(posts))[:200]

    desc = oferta.get("descripcion") or ""
    emails_ya = re.findall(r"\b[\w.+-]+@[\w.-]+\.\w{2,}\b", desc.lower())
    emails_nuevos = [
        e for e in result.classified
        if e.email.lower() not in emails_ya
    ]
    if not emails_nuevos:
        return
    partes_email: list[str] = []
    for em in emails_nuevos:
        if "email_postulacion" in em.kinds or "correo_postulacion" in em.kinds:
            partes_email.append(f"Correo postulación: {em.email}")
        elif "email_consultas" in em.kinds or "correo_contacto" in em.kinds:
            partes_email.append(f"Correo consultas: {em.email}")
        else:
            partes_email.append(f"Correo: {em.email}")
    if partes_email:
        sep = "\n" if desc else ""
        oferta["descripcion"] = _limpiar_lineas(
            desc + sep + "\n".join(partes_email)
        )[:2000]


def _extraer_contrato(texto: str, oferta: dict) -> None:
    """Mejora tipo_cargo si el actual es genérico o ausente."""
    if extract_contract_info is None:
        return
    actual = (oferta.get("tipo_cargo") or "").lower()
    genericos = {"", "concurso público", "concurso publico", "código del trabajo",
                 "codigo del trabajo", "no informado"}
    if actual not in genericos:
        return
    contrato, jornada, modalidad = extract_contract_info(texto)
    if contrato:
        MAPA = {
            "honorarios": "Honorarios",
            "contrata": "Contrata",
            "planta": "Planta",
            "codigo_trabajo": "Código del Trabajo",
            "plazo_fijo": "Código del Trabajo",
            "reemplazo": "Reemplazo",
        }
        oferta["tipo_cargo"] = MAPA.get(contrato, contrato.title())
    if jornada or modalidad:
        desc = oferta.get("descripcion") or ""
        extras: list[str] = []
        if jornada and f"jornada {jornada}" not in desc.lower():
            extras.append(f"Jornada: {jornada}")
        if modalidad and modalidad not in desc.lower():
            extras.append(f"Modalidad: {modalidad}")
        if extras:
            sep = "\n" if desc else ""
            oferta["descripcion"] = _limpiar_lineas(
                desc + sep + "\n".join(extras)
            )[:2000]


def _extraer_funciones(texto: str, oferta: dict) -> None:
    """Agrega funciones a la descripción si no están presentes."""
    if extract_functions is None:
        return
    desc = oferta.get("descripcion") or ""
    if "funciones" in desc.lower():
        return
    funciones = extract_functions(texto)
    if funciones:
        # Encabezado + viñetas en líneas propias → el front las clasifica como
        # "Funciones principales" en vez de fundirlas en el párrafo.
        bloque = "Funciones del cargo:\n" + "\n".join(f"- {f}" for f in funciones[:8])
        sep = "\n" if desc else ""
        oferta["descripcion"] = _limpiar_lineas(desc + sep + bloque)[:2000]


def _extraer_postulacion(texto: str, oferta: dict) -> None:
    """Agrega los pasos para postular ('Cómo postular') si no están presentes.

    Detecta por palabras/frases clave (formulario online, recepción de
    antecedentes, adjuntar CV, expectativas de renta, dudas/consultas) y los
    anexa como bloque rotulado en líneas propias.
    """
    if extraer_pasos_postulacion is None:
        return
    desc = oferta.get("descripcion") or ""
    low = desc.lower()
    if "cómo postular" in low or "como postular" in low:
        return
    pasos = extraer_pasos_postulacion(texto)
    if not pasos:
        return
    bloque = "Cómo postular:\n" + "\n".join(f"- {p}" for p in pasos)
    sep = "\n" if desc else ""
    oferta["descripcion"] = _limpiar_lineas(desc + sep + bloque)[:2000]


# ── Función principal ─────────────────────────────────────────────────────

def enriquecer_oferta(
    oferta: dict,
    texto_html: str | None = None,
    texto_detalle: str | None = None,
    pdf_urls: list[str] | None = None,
    session: Any = None,
    descargar_pdfs: bool = True,
    max_pdfs: int = 3,
) -> dict:
    """Enriquece un dict de oferta con extractores compartidos.

    Aplica sobre el texto disponible (HTML del detalle y/o PDFs descargados)
    los extractores de: salario, requisitos, email, tipo de contrato,
    funciones y modalidad. Solo rellena campos que estén vacíos/None.

    Args:
        oferta: dict con los campos estándar (cargo, descripcion, etc.).
        texto_html: texto plano del HTML del detalle (soup.get_text()).
        texto_detalle: texto adicional (otra fuente de detalle).
        pdf_urls: URLs de PDFs a descargar y parsear.
        session: requests.Session para descargas.
        descargar_pdfs: si False, no descarga PDFs (útil para tests).
        max_pdfs: máximo de PDFs a descargar.

    Returns:
        El mismo dict oferta, modificado in-place.
    """
    textos: list[str] = []

    if texto_html:
        textos.append(texto_html)
    if texto_detalle:
        textos.append(texto_detalle)

    # Descargar y leer PDFs
    textos_pdf: list[tuple[str, str]] = []
    if pdf_urls and descargar_pdfs:
        textos_pdf = descargar_y_leer_pdfs(
            pdf_urls, session=session, max_pdfs=max_pdfs,
        )
        for _url, t in textos_pdf:
            textos.append(t)

    if textos_pdf and not oferta.get("url_bases"):
        oferta["url_bases"] = textos_pdf[0][0]

    if not textos:
        return oferta

    texto_combinado = "\n\n".join(textos)
    # Reúne renglones partidos por ancho de página ANTES de extraer: así los
    # títulos profesionales ("Título profesional de Ingeniería…, o carrera
    # profesional afín…") y las funciones no se pierden al cortarse en dos.
    if unir_lineas_envueltas is not None:
        texto_combinado = unir_lineas_envueltas(texto_combinado)

    _extraer_salario(texto_combinado, oferta)
    _extraer_requisitos(texto_combinado, oferta)
    _extraer_email(texto_combinado, oferta)
    _extraer_contrato(texto_combinado, oferta)
    _extraer_funciones(texto_combinado, oferta)
    _extraer_postulacion(texto_combinado, oferta)

    return oferta


def encontrar_pdf_urls(html: str, base_url: str = "") -> list[str]:
    """Extrae URLs de PDFs de un HTML. Útil para scrapers que no buscan PDFs."""
    patron = re.compile(r'href=["\']([^"\']*\.pdf(?:\?[^"\']*)?)["\']', re.I)
    urls: list[str] = []
    vistos: set[str] = set()
    for m in patron.finditer(html):
        href = m.group(1)
        if href.startswith("/") and base_url:
            from urllib.parse import urljoin
            href = urljoin(base_url, href)
        elif not href.startswith("http"):
            if base_url:
                from urllib.parse import urljoin
                href = urljoin(base_url, href)
            else:
                continue
        if href.lower() not in vistos:
            vistos.add(href.lower())
            urls.append(href)
    return urls
