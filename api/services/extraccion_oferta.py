"""Extracción de datos de oferta laboral desde una URL (web, PDF o imagen),
un PDF o imágenes subidas, para pre-llenar el formulario "Nueva oferta" del
panel de gestión.

No inventa heurísticas nuevas: reutiliza la inteligencia ya existente:

- `scrapers.bases_pdf.leer_pdf`             → capa de texto + OCR (tesseract,
                                              español) para PDFs ESCANEADOS.
- OCR de imágenes (Pillow + tesseract)      → mismo patrón que
                                              `scrapers.carga_manual.extraer_de_imagenes`:
                                              capturas de pantalla/fotos del
                                              aviso, varias en orden.
- `scrapers.bases_pdf.enriquecer_desde_texto` → decretos/bases de concursos
                                              (cargo, cronograma con cierre,
                                              funciones, requisitos, grado…).
- `scrapers.carga_manual`                   → institución contra el catálogo,
                                              cargo, fecha de cierre y campos
                                              del perfil de cargo genérico.
- `extraction.*`                            → renta, tipo de contrato, correos
                                              de postulación/consultas.

No persiste nada: devuelve `campos` con las mismas claves que acepta
`POST /api/{ADMIN_PATH}/ofertas`, más `meta` (método, OCR, advertencias) y
`texto` (vista previa) para que el editor revise antes de crear la oferta.
"""
from __future__ import annotations

import ipaddress
import logging
import re
import socket
from datetime import date
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# OCR de imágenes perezoso (mismo patrón que scrapers/bases_pdf.py): Pillow y
# pytesseract están en requirements; el binario tesseract(+spa) lo instala el
# deploy. Si falta algo, la ruta de imágenes degrada con error legible.
try:
    from PIL import Image  # type: ignore
except Exception:  # pragma: no cover
    Image = None  # type: ignore[assignment]

try:
    import pytesseract  # type: ignore
except Exception:  # pragma: no cover
    pytesseract = None  # type: ignore[assignment]

logger = logging.getLogger("api.extraccion_oferta")

# Límites de descarga: los PDFs de bases rondan 1-10 MB; un escaneo grande
# puede llegar a ~20. Más que eso casi seguro no es una oferta laboral.
MAX_BYTES = 25 * 1024 * 1024
TIMEOUT = (10, 60)  # (conexión, lectura)
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 contrataoplanta-admin")

# Mapeo etiqueta del contract_extractor → valores del select del panel
# (EDIT_TIPOS_CONTRATO en web/gestion.js).
_TIPO_CONTRATO = {
    "honorarios": "Honorarios",
    "codigo_trabajo": "Código del Trabajo",
    "contrata": "Contrata",
    "planta": "Planta",
    "reemplazo": "Reemplazo",
    "plazo_fijo": "Otro",
}

# "PLANTA" de la tabla del decreto (Profesionales, Técnicos…) → estamento del
# panel (EDIT_ESTAMENTOS en web/gestion.js).
_ESTAMENTOS = {
    "profesional": "Profesional",
    "jefatura": "Jefatura",
    "tecnic": "Técnico",
    "técnic": "Técnico",
    "administrativ": "Administrativo",
    "auxiliar": "Auxiliar",
    "directiv": "Directivo",
    "fiscalizador": "Fiscalizador",
}


class ExtraccionError(Exception):
    """Error de extracción con mensaje apto para mostrar en el panel."""


# ══════════════════════════════════════════════════════════════════════════════
# Descarga del recurso
# ══════════════════════════════════════════════════════════════════════════════
def _host_privado(host: str) -> bool:
    """True si el host resuelve a una IP privada/loopback (SSRF básico).

    El endpoint es solo-admin con JWT, pero no hay razón para permitir que el
    panel haga fetch de la red interna del despliegue.
    """
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return False  # si no resuelve, requests fallará con mensaje claro
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            continue
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            return True
    return False


def descargar_recurso(url: str) -> tuple[bytes, str, str]:
    """Descarga la URL y devuelve (contenido, content_type, url_final)."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ExtraccionError("La URL debe ser http(s) válida")
    if _host_privado(parsed.hostname):
        raise ExtraccionError("La URL apunta a una dirección interna/privada")

    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT,
                            stream=True, allow_redirects=True)
        resp.raise_for_status()
        chunks: list[bytes] = []
        total = 0
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            total += len(chunk)
            if total > MAX_BYTES:
                raise ExtraccionError(
                    f"El recurso supera el límite de {MAX_BYTES // (1024*1024)} MB")
            chunks.append(chunk)
        contenido = b"".join(chunks)
    except ExtraccionError:
        raise
    except requests.RequestException as exc:
        raise ExtraccionError(f"No se pudo descargar la URL: {exc}") from exc

    content_type = (resp.headers.get("Content-Type") or "").lower()
    return contenido, content_type, str(resp.url or url)


def es_pdf(contenido: bytes, content_type: str = "", nombre: str = "") -> bool:
    return (contenido[:5] == b"%PDF-"
            or "application/pdf" in (content_type or "")
            or (nombre or "").lower().endswith(".pdf"))


# Magic bytes de los formatos de imagen que Pillow abre sin dependencias extra.
_IMG_MAGIC = (
    b"\x89PNG",          # PNG
    b"\xff\xd8\xff",     # JPEG
    b"GIF8",             # GIF
    b"BM",               # BMP
    b"II*\x00",          # TIFF little-endian
    b"MM\x00*",          # TIFF big-endian
)
_IMG_EXTENSIONES = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp")


def es_imagen(contenido: bytes, content_type: str = "", nombre: str = "") -> bool:
    if any(contenido.startswith(m) for m in _IMG_MAGIC):
        return True
    if contenido[:4] == b"RIFF" and contenido[8:12] == b"WEBP":
        return True
    if (content_type or "").startswith("image/"):
        return True
    return (nombre or "").lower().endswith(_IMG_EXTENSIONES)


def texto_desde_imagenes(imagenes: list[bytes]) -> str:
    """OCR (tesseract, español) de una o más imágenes, concatenadas en orden.

    Mismo pre-proceso que `scrapers.carga_manual.extraer_de_imagenes`
    (escala de grises). Lanza ExtraccionError si el OCR no está disponible
    o no se pudo leer texto de ninguna imagen.
    """
    if Image is None or pytesseract is None:
        raise ExtraccionError(
            "OCR de imágenes no disponible en el servidor (falta Pillow o "
            "tesseract). Sube un PDF o ingresa los datos a mano.")
    import io as _io

    fragmentos: list[str] = []
    for idx, contenido in enumerate(imagenes, 1):
        try:
            img = Image.open(_io.BytesIO(contenido))
            if img.mode != "L":
                img = img.convert("L")
            # Upscale 2x de capturas chicas: a Tesseract se le pierden signos
            # con texto de pantalla pequeño.
            if min(img.size) < 1400:
                img = img.resize((img.width * 2, img.height * 2),
                                 getattr(Image, "LANCZOS", 1))
            # spa+eng: el traineddata español solo pierde el "@" (lee
            # "correo@x.cl" como "correotOx.cl"); combinado con el inglés lo
            # reconoce. Si eng no está instalado, se degrada a spa.
            try:
                fragmentos.append(pytesseract.image_to_string(img, lang="spa+eng"))
            except Exception:
                fragmentos.append(pytesseract.image_to_string(img, lang="spa"))
        except ExtraccionError:
            raise
        except Exception as exc:
            logger.warning("OCR falló en imagen %d: %s", idx, exc)
    texto = "\n".join(f for f in fragmentos if f.strip())
    if len(re.sub(r"\s", "", texto)) < 40:
        raise ExtraccionError(
            "El OCR no pudo leer texto útil de la(s) imagen(es). Prueba con "
            "una captura más nítida o con el PDF original.")
    return texto


# ══════════════════════════════════════════════════════════════════════════════
# HTML → texto estructurado
# ══════════════════════════════════════════════════════════════════════════════
def extraer_texto_html(html: str, base_url: str = "") -> dict[str, Any]:
    """Título, encabezados, tablas, texto plano y links a PDF de una página."""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "iframe", "svg",
                     "nav", "footer", "header", "form", "button"]):
        tag.decompose()

    titulo = (soup.title.get_text(" ", strip=True) if soup.title else "") or ""
    headings = [h.get_text(" ", strip=True)
                for h in soup.find_all(["h1", "h2", "h3"])
                if h.get_text(strip=True)][:15]

    tablas: list[str] = []
    for table in soup.find_all("table")[:10]:
        filas = []
        for tr in table.find_all("tr"):
            celdas = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if any(celdas):
                filas.append(" | ".join(celdas))
        if filas:
            tablas.append("\n".join(filas))

    pdf_links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"])
        if href.lower().split("?")[0].endswith(".pdf") and href not in pdf_links:
            pdf_links.append(href)

    texto = re.sub(r"\n{3,}", "\n\n", soup.get_text("\n", strip=True))
    return {"titulo": titulo, "headings": headings, "tablas": tablas,
            "texto": texto, "pdf_links": pdf_links[:20]}


# ══════════════════════════════════════════════════════════════════════════════
# Texto → campos del formulario "Nueva oferta"
# ══════════════════════════════════════════════════════════════════════════════
def _renta_desde_texto(renta_texto: str | None) -> int | None:
    if not renta_texto:
        return None
    m = re.search(r"\$?\s*([\d]{1,3}(?:\.\d{3})+|\d{6,9})", renta_texto)
    if not m:
        return None
    try:
        valor = int(m.group(1).replace(".", ""))
    except ValueError:
        return None
    return valor if 100_000 <= valor <= 20_000_000 else None


def _corto(valor: str | None, tope: int = 100) -> str | None:
    """Recorta valores clave-valor del perfil al primer corte de frase.

    El texto plano de una página aplana todo en una línea, y el patrón
    `Modalidad: (.+)` termina capturando "Contrata. Jornada completa. Renta…".
    """
    if not valor:
        return None
    corto = re.split(r"[.;|·]\s", valor, maxsplit=1)[0].strip(" .;")
    return corto[:tope] or None


def _mapear_estamento(planta: str | None) -> str | None:
    p = (planta or "").lower()
    for clave, valor in _ESTAMENTOS.items():
        if clave in p:
            return valor
    return None


def extraer_campos_oferta(
    texto: str,
    *,
    metodo: str = "texto",
    titulo: str | None = None,
    headings: list[str] | None = None,
    tablas: list[str] | None = None,
    url: str | None = None,
    pdf_links: list[str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Combina los extractores del proyecto y devuelve (campos, advertencias).

    `campos` usa las mismas claves que `POST /api/{ADMIN_PATH}/ofertas`.
    """
    from scrapers import bases_pdf, carga_manual
    from extraction.contract_extractor import extract_contract_info
    from extraction.date_extractor import (
        extract_dates_from_tables,
        extract_dates_from_text,
        resolve_best_dates,
    )
    from extraction.email_extractor import extract_and_classify_emails
    from extraction.salary_extractor import extract_salary

    advertencias: list[str] = []
    texto_completo = "\n".join(filter(None, [titulo, texto, *(tablas or [])]))

    # 1) Parser de bases/decretos (cargo, cronograma, funciones, requisitos,
    #    grado, vacantes…). Diseñado para PDFs de concursos; en texto web
    #    genérico simplemente aporta poco.
    oferta: dict[str, Any] = {}
    resumen = bases_pdf.enriquecer_desde_texto(oferta, texto_completo, metodo)
    bases = bases_pdf.parsear_bases(texto_completo)

    # 2) Publicación genérica (LinkedIn/web): institución, cargo, cierre, URL.
    pub = carga_manual.extraer_de_publicacion(texto_completo)
    # 3) Perfil de cargo clave-valor (Cargo:, Modalidad:, Renta:, Vacantes:…).
    perfil = carga_manual._extraer_campos_perfil(texto_completo)

    campos: dict[str, Any] = {}

    def _set(clave: str, valor: Any) -> None:
        if valor in (None, "", []):
            return
        campos.setdefault(clave, valor)

    # ── Cargo: bases (decreto) > perfil clave-valor > publicación > heading ──
    _set("cargo", oferta.get("cargo"))
    _set("cargo", perfil.get("cargo"))
    _set("cargo", pub.get("cargo"))
    for h in (headings or []):
        if re.search(r"concurso|convocatoria|llamado|contrataci[oó]n|cargo|"
                     r"profesional|t[eé]cnico|administrativo", h, re.I):
            _set("cargo", h.strip()[:500])
            break
    _set("cargo", (titulo or "").strip()[:500] or None)

    # ── Institución: texto contra el catálogo de ~640 instituciones ──────────
    inst_nombre = pub.get("institucion_nombre")
    inst = carga_manual.buscar_institucion(inst_nombre) if inst_nombre else None
    if inst is None and titulo:
        inst = carga_manual.buscar_institucion(titulo)
    if inst:
        _set("institucion_nombre", inst["nombre"])
        _set("institucion_id", inst.get("id"))
        _set("region", inst.get("region"))
        _set("sector", inst.get("sector"))
    else:
        _set("institucion_nombre", inst_nombre)
        if not inst_nombre:
            advertencias.append("No se detectó la institución; complétala a mano")

    # ── Fechas ────────────────────────────────────────────────────────────────
    fc = oferta.get("fecha_cierre") or pub.get("fecha_cierre")
    if not fc:
        evidencias = extract_dates_from_text(texto_completo)
        evidencias.extend(extract_dates_from_tables(tablas or []))
        resol = resolve_best_dates(evidencias)
        if resol.application_end_at:
            fc = resol.application_end_at.date()
        if not campos.get("fecha_publicacion") and resol.published_at:
            _set("fecha_publicacion", resol.published_at.date().isoformat())
    if isinstance(fc, date):
        _set("fecha_cierre", fc.isoformat())
    elif fc:
        _set("fecha_cierre", str(fc))
    else:
        advertencias.append("No se detectó fecha de cierre")
    fp = bases.get("fecha_publicacion")
    if isinstance(fp, date):
        _set("fecha_publicacion", fp.isoformat())

    # ── Contrato / estamento / jornada ───────────────────────────────────────
    etiqueta, workday, _modalidad = extract_contract_info(texto_completo)
    _set("tipo_contrato", _TIPO_CONTRATO.get(etiqueta or ""))
    modalidad_perfil = _corto(perfil.get("modalidad"), 60)
    if modalidad_perfil:
        _set("calidad_juridica", modalidad_perfil)
        for lbl, patron in (("Honorarios", r"honorario"), ("Contrata", r"contrata"),
                            ("Planta", r"planta"), ("Código del Trabajo", r"c[oó]digo")):
            if re.search(patron, modalidad_perfil, re.I):
                _set("tipo_contrato", lbl)
                break
    _set("estamento", _mapear_estamento(bases.get("planta")))
    _set("grado_eus", str(bases.get("grado")) if bases.get("grado") else None)
    _set("jornada", _corto(oferta.get("jornada") or perfil.get("horario")))
    if workday == "completa":
        _set("jornada", "Jornada completa")
    elif workday == "parcial":
        _set("jornada", "Jornada parcial")

    # ── Vacantes ─────────────────────────────────────────────────────────────
    _set("numero_vacantes", oferta.get("numero_vacantes"))
    _set("numero_vacantes", carga_manual._safe_int(perfil.get("vacantes")))
    # El patrón del perfil exige valores de ≥3 caracteres y pierde "2":
    # fallback propio para vacantes de 1-2 dígitos.
    if not campos.get("numero_vacantes"):
        if m := re.search(r"(?:n[°º]\s*(?:de\s+)?)?vacantes?\s*[:\|]?\s*(\d{1,3})\b",
                          texto_completo, re.I):
            _set("numero_vacantes", int(m.group(1)))

    # ── Renta ────────────────────────────────────────────────────────────────
    salario = extract_salary(texto_completo)
    monto = None
    if salario.amount and (salario.currency or "CLP") == "CLP":
        monto = int(salario.amount)
    monto = monto or perfil.get("renta_bruta") or _renta_desde_texto(
        oferta.get("renta_texto") or perfil.get("renta_texto"))
    if monto:
        _set("renta_bruta_min", monto)
        _set("renta_bruta_max", monto)
        _set("renta_tipo", salario.kind if salario.kind != "indeterminada" else None)

    # ── Correos ──────────────────────────────────────────────────────────────
    # El nombre local del correo (postulaciones@…, consultas@…) es señal más
    # fuerte que la ventana de contexto del clasificador (±100 caracteres
    # alrededor suelen mezclar ambas frases).
    email_info = extract_and_classify_emails(texto_completo)
    for item in email_info.classified:
        local = item.email.split("@", 1)[0]
        kinds = set(item.kinds)
        if re.search(r"postulaci|empleo|reclutamiento|rrhh|selecci", local):
            _set("email_postulacion", item.email)
        elif re.search(r"consulta|informaci|contacto", local):
            _set("email_consultas", item.email)
        elif {"email_postulacion", "correo_postulacion"} & kinds:
            _set("email_postulacion", item.email)
        elif {"email_consultas", "correo_contacto"} & kinds:
            _set("email_consultas", item.email)
    if not campos.get("email_postulacion"):
        indeterminados = [i.email for i in email_info.classified
                          if "email_indeterminado" in i.kinds]
        _set("email_postulacion", indeterminados[0] if indeterminados else None)

    # ── Descripción / requisitos ─────────────────────────────────────────────
    _set("descripcion", oferta.get("descripcion") or perfil.get("descripcion"))
    _set("requisitos", oferta.get("requisitos_texto") or perfil.get("requisitos_texto"))

    # ── Región (si el catálogo no la dio): "Región de/del X" en el texto ─────
    if not campos.get("region"):
        m = re.search(r"regi[oó]n\s+(?:de\s+|del\s+)?((?:la\s+)?[\wÁÉÍÓÚÑáéíóúñ'’]+"
                      r"(?:\s+[\wÁÉÍÓÚÑáéíóúñ'’]+){0,3})", texto_completo, re.I)
        if m:
            try:
                from db.database import normalizar_region
                _set("region", normalizar_region(m.group(1)))
            except Exception:  # entorno sin BD configurada
                pass

    # ── Ubicación / URLs ─────────────────────────────────────────────────────
    _set("lugar_desempenio", _corto(perfil.get("dependencia"), 200))
    if url:
        _set("url_oferta", url)
        if es_pdf(b"", "", url):
            _set("url_bases", url)
    # PDF subido sin URL: si la publicación traía link de postulación, úsalo.
    _set("url_oferta", pub.get("url_postulacion"))
    for link in (pdf_links or []):
        # Solo absolutas: un HTML subido como archivo no tiene base para
        # resolver hrefs relativos, y el form exige URLs http(s).
        if link.startswith(("http://", "https://")) and re.search(
                r"bases|concurso|convocatoria|perfil|tdr",
                link.rsplit("/", 1)[-1], re.I):
            _set("url_bases", link)
            break

    # Estado del proceso según el propio documento (acta de resultados, cierre
    # pasado…): se informa, la decisión queda en manos del editor.
    if resumen.get("estado") == "vencido":
        advertencias.append(f"Ojo: el documento parece VENCIDO — {resumen.get('motivo')}")

    return campos, advertencias


# ══════════════════════════════════════════════════════════════════════════════
# Puntos de entrada
# ══════════════════════════════════════════════════════════════════════════════
def extraer_desde_contenido(
    contenido: bytes,
    content_type: str = "",
    *,
    url: str | None = None,
    nombre_archivo: str | None = None,
    permitir_ocr: bool = True,
) -> dict[str, Any]:
    """Extrae campos desde bytes ya descargados/subidos (PDF, imagen o HTML)."""
    from scrapers import bases_pdf

    meta: dict[str, Any] = {"url": url, "nombre_archivo": nombre_archivo}
    if es_imagen(contenido, content_type, nombre_archivo or ""):
        texto = texto_desde_imagenes([contenido])
        meta.update({"tipo": "imagen", "metodo": "ocr", "usado_ocr": True})
        campos, advertencias = extraer_campos_oferta(texto, metodo="ocr", url=url)
        advertencias.insert(0, "Imagen leída por OCR — revisa los campos con atención")
    elif es_pdf(contenido, content_type, nombre_archivo or ""):
        texto, metodo = bases_pdf.leer_pdf(contenido, allow_ocr=permitir_ocr)
        if metodo == "sin_texto":
            raise ExtraccionError(
                "No se pudo extraer texto del PDF (¿escaneo ilegible o sin "
                "tesseract instalado?). Revisa el archivo o ingresa los datos a mano.")
        meta.update({"tipo": "pdf", "metodo": metodo, "usado_ocr": metodo == "ocr"})
        campos, advertencias = extraer_campos_oferta(texto, metodo=metodo, url=url)
        if metodo == "ocr":
            advertencias.insert(0, "PDF escaneado: texto obtenido por OCR — revisa "
                                   "los campos con atención")
    else:
        try:
            html = contenido.decode("utf-8")
        except UnicodeDecodeError:
            html = contenido.decode("latin-1", errors="replace")
        pagina = extraer_texto_html(html, base_url=url or "")
        if len(re.sub(r"\s", "", pagina["texto"])) < 120:
            raise ExtraccionError(
                "La página casi no tiene texto (¿contenido cargado por "
                "JavaScript?). Prueba con la URL del PDF de las bases.")
        meta.update({"tipo": "html", "metodo": "html", "usado_ocr": False,
                     "titulo": pagina["titulo"], "pdf_links": pagina["pdf_links"]})
        texto = pagina["texto"]
        campos, advertencias = extraer_campos_oferta(
            texto,
            titulo=pagina["titulo"],
            headings=pagina["headings"],
            tablas=pagina["tablas"],
            url=url,
            pdf_links=pagina["pdf_links"],
        )

    meta["caracteres"] = len(texto)
    meta["advertencias"] = advertencias
    meta["campos_detectados"] = sorted(campos.keys())
    return {"campos": campos, "meta": meta, "texto": texto[:4000]}


def extraer_desde_archivos(
    archivos: list[tuple[bytes, str, str]],
    *,
    url: str | None = None,
    permitir_ocr: bool = True,
) -> dict[str, Any]:
    """Extrae campos desde uno o más archivos subidos.

    `archivos` = lista de (contenido, content_type, nombre). Un solo archivo
    sigue la ruta normal (PDF/imagen/HTML). Varios archivos deben ser TODOS
    imágenes (capturas del aviso en varias páginas): se les hace OCR y el
    texto se concatena en el orden recibido, igual que
    `carga_manual.extraer_de_imagenes`.
    """
    if not archivos:
        raise ExtraccionError("Sin archivos para procesar")
    if len(archivos) == 1:
        contenido, ctype, nombre = archivos[0]
        return extraer_desde_contenido(
            contenido, ctype, url=url, nombre_archivo=nombre,
            permitir_ocr=permitir_ocr)

    no_imagen = [n for c, t, n in archivos if not es_imagen(c, t, n)]
    if no_imagen:
        raise ExtraccionError(
            "Para subir varios archivos a la vez deben ser todos imágenes "
            f"(capturas del aviso). No parecen imagen: {', '.join(no_imagen[:3])}")

    texto = texto_desde_imagenes([c for c, _t, _n in archivos])
    nombres = [n for _c, _t, n in archivos]
    campos, advertencias = extraer_campos_oferta(texto, metodo="ocr", url=url)
    advertencias.insert(
        0, f"{len(archivos)} imágenes leídas por OCR (en orden) — revisa los "
           "campos con atención")
    meta: dict[str, Any] = {
        "url": url, "nombre_archivo": ", ".join(nombres), "tipo": "imagen",
        "metodo": "ocr", "usado_ocr": True, "caracteres": len(texto),
        "advertencias": advertencias, "campos_detectados": sorted(campos.keys()),
    }
    return {"campos": campos, "meta": meta, "texto": texto[:4000]}


def extraer_desde_url(url: str, *, permitir_ocr: bool = True) -> dict[str, Any]:
    """Descarga la URL (página web, PDF o imagen) y extrae los campos."""
    contenido, content_type, url_final = descargar_recurso(url)
    return extraer_desde_contenido(
        contenido, content_type, url=url_final, permitir_ocr=permitir_ocr)
