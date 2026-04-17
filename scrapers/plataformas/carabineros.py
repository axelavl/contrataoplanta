"""Scraper dedicado para postulaciones.carabineros.cl.

Portal Laravel server-rendered (no es SPA). Flujo verificado empíricamente
contra el índice público del sitio:

    GET /                                       → listado HTML
    GET /?page={N}                              → paginación
    GET /concursos/{id}                         → detalle HTML
    GET /concursos/download/{id}/Descriptor     → PDF bases/descriptor
    GET /concursos/download/{id}/Perfil         → PDF perfil del cargo

La fuente de verdad para campos estructurados (requisitos, funciones,
formación, experiencia, competencias, documentos) son los dos PDFs.
El HTML del detalle aporta título, código (C-NN-YYYY), región, comuna,
jornada, renta, vacantes y fechas de publicación/cierre.

No se usa Playwright: no hay JS rendering ni anti-bot detectado.
"""
from __future__ import annotations

import argparse
import io
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scrapers.base import (
    BaseScraper,
    clean_text,
    extract_host_like_pattern,
    normalize_key,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
    truncate,
)
from scrapers.job_pipeline import JobExtractionPipeline, build_raw_page_from_generic

try:  # pragma: no cover - dependencia opcional
    import pdfplumber  # type: ignore
except BaseException:  # noqa: BLE001 - cubre ImportError y PanicException de pyo3
    pdfplumber = None  # si no está disponible, se omite el enriquecimiento desde PDF


# ────────────────────────── Constantes del portal ───────────────────────

BASE_URL = "https://postulaciones.carabineros.cl"
SITIO_INSTITUCIONAL = "https://www.carabineros.cl/"
LISTADO_URL = f"{BASE_URL}/"
DETALLE_TMPL = f"{BASE_URL}/concursos/{{id}}"
PDF_TMPL = f"{BASE_URL}/concursos/download/{{id}}/{{tipo}}"  # Descriptor | Perfil

# Regex tolerantes a variaciones
RE_CONCURSO_HREF = re.compile(r"/concursos/(\d+)(?:/|$|\?|#)")
RE_CODIGO = re.compile(r"\bC[-\s]?\d{1,3}[-\s]?\d{4}\b", re.I)
RE_NUM = re.compile(r"\d+")

# Defensivos de corrida
MAX_PAGES = 30
DELAY_SEC = 1.8
TIMEOUT_HTML = 20
TIMEOUT_PDF = 30

# Mapeo de subtítulos de PDF (Perfil y Descriptor) a campos canónicos
HEADERS_PERFIL: dict[str, tuple[str, ...]] = {
    "requisitos": (
        "REQUISITOS LEGALES",
        "REQUISITOS GENERALES",
        "REQUISITOS DE POSTULACION",
        "REQUISITOS DE POSTULACIÓN",
        "REQUISITOS",
    ),
    "funciones": (
        "FUNCIONES DEL CARGO",
        "PRINCIPALES FUNCIONES",
        "FUNCIONES",
        "OBJETIVO DEL CARGO",
    ),
    "formacion": (
        "FORMACIÓN EDUCACIONAL",
        "FORMACION EDUCACIONAL",
        "NIVEL EDUCACIONAL",
        "ESTUDIOS REQUERIDOS",
        "ESTUDIOS",
    ),
    "experiencia": (
        "EXPERIENCIA LABORAL",
        "EXPERIENCIA REQUERIDA",
        "EXPERIENCIA",
    ),
    "capacitacion": (
        "ESPECIALIZACIÓN Y CAPACITACIÓN",
        "ESPECIALIZACION Y CAPACITACION",
        "CAPACITACIÓN",
        "CAPACITACION",
        "ESPECIALIZACIÓN",
        "ESPECIALIZACION",
    ),
    "competencias": (
        "CONOCIMIENTOS Y COMPETENCIAS",
        "CONOCIMIENTOS ESPECÍFICOS",
        "CONOCIMIENTOS ESPECIFICOS",
        "COMPETENCIAS",
    ),
}

HEADERS_DESCRIPTOR: dict[str, tuple[str, ...]] = {
    "descripcion": (
        "OBJETIVO DEL CARGO",
        "DESCRIPCIÓN DEL CARGO",
        "DESCRIPCION DEL CARGO",
        "ANTECEDENTES GENERALES",
        "ANTECEDENTES",
    ),
    "documentos": (
        "DOCUMENTACIÓN REQUERIDA",
        "DOCUMENTACION REQUERIDA",
        "ANTECEDENTES DE POSTULACIÓN",
        "ANTECEDENTES DE POSTULACION",
        "DOCUMENTOS REQUERIDOS",
        "DOCUMENTOS",
    ),
}


# ──────────────────────────── Data classes ──────────────────────────────


@dataclass
class ConcursoRef:
    """Referencia obtenida del listado, antes de ir al detalle."""

    id: int
    url_detalle: str
    preview_title: str = ""


@dataclass
class OfertaCarabineros:
    """Oferta enriquecida con HTML del detalle + texto de PDFs."""

    id: int
    url_detalle: str
    codigo_concurso: str | None = None
    titulo_cargo: str | None = None
    region: str | None = None
    comuna: str | None = None
    tipo_contrato: str | None = None
    jornada: str | None = None
    renta_texto: str | None = None
    vacantes: int | None = None
    fecha_publicacion: Any = None
    fecha_cierre: Any = None
    descripcion: str = ""
    requisitos: str = ""
    funciones: str = ""
    formacion: str = ""
    experiencia: str = ""
    capacitacion: str = ""
    competencias: str = ""
    documentos: str = ""
    url_descriptor: str = ""
    url_perfil: str = ""
    pdf_descriptor_text: str = ""
    pdf_perfil_text: str = ""
    html_text: str = ""
    headings: list[str] = field(default_factory=list)

    def consolidated_text(self) -> str:
        """Texto unificado para alimentar el pipeline de clasificación."""
        parts: list[str] = []
        if self.html_text:
            parts.append(self.html_text)
        if self.pdf_perfil_text:
            parts.append(self.pdf_perfil_text)
        if self.pdf_descriptor_text:
            parts.append(self.pdf_descriptor_text)
        return "\n\n".join(parts)


# ─────────────────────────────── Scraper ────────────────────────────────


class CarabinerosScraper(BaseScraper):
    """Scraper de postulaciones.carabineros.cl (institución id=161)."""

    def __init__(
        self,
        institucion: dict[str, Any],
        instituciones_catalogo: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        max_results: int | None = None,
    ) -> None:
        self.institucion = institucion
        self.url_empleo = clean_text(
            institucion.get("url_empleo") or institucion.get("url_portal_empleos")
        ) or LISTADO_URL
        self.sitio_web = clean_text(institucion.get("sitio_web")) or SITIO_INSTITUCIONAL

        super().__init__(
            nombre="scraper.carabineros",
            instituciones=instituciones_catalogo or [institucion],
            dry_run=dry_run,
            delay=DELAY_SEC,
            timeout=TIMEOUT_HTML,
            max_results=max_results,
            max_retries=2,
        )
        if institucion.get("id") is not None:
            self.scope_institucion_ids = [institucion["id"]]
        patterns: list[str] = []
        for url in (self.url_empleo, self.sitio_web):
            pattern = extract_host_like_pattern(url)
            if pattern and pattern not in patterns:
                patterns.append(pattern)
        self.scope_url_patterns = patterns
        self.pipeline = JobExtractionPipeline()
        self._warmed_up = False

    # ─────────────────── FASE A: warmup de sesión ─────────────────────

    def _warmup(self) -> None:
        """Siembra cookies (XSRF-TOKEN, session) con un GET al sitio institucional
        y luego al listado. Best-effort: si falla, seguimos igual."""
        if self._warmed_up:
            return
        self._warmed_up = True
        for url in (SITIO_INSTITUCIONAL, LISTADO_URL):
            try:
                self.request(url, timeout=self.timeout)
            except Exception as exc:  # pragma: no cover - defensa runtime
                self.logger.info(
                    "evento=carabineros_warmup_fail scraper=%s url=%s error=%s",
                    self.nombre,
                    url,
                    type(exc).__name__,
                )

    # ─────────────────── FASE B+C+D: fetch_ofertas ────────────────────

    def fetch_ofertas(self) -> list[dict[str, Any]]:
        self._warmup()
        refs = self._enumerar_listado()
        self.logger.info(
            "evento=carabineros_listado scraper=%s ids=%s",
            self.nombre,
            len(refs),
        )

        crudas: list[dict[str, Any]] = []
        limit = self.max_results or len(refs)
        for ref in refs[:limit]:
            try:
                oferta = self._fetch_detalle(ref)
                if not oferta:
                    continue
                self._enrich_with_pdfs(oferta)
                crudas.append(self._oferta_to_raw(oferta))
            except Exception as exc:  # pragma: no cover - defensa runtime
                self.logger.warning(
                    "evento=carabineros_detalle_error scraper=%s id=%s error=%s",
                    self.nombre,
                    ref.id,
                    type(exc).__name__,
                )
        return crudas

    # ───── Enumeración del listado con paginación ?page=N ─────────────

    def _enumerar_listado(self) -> list[ConcursoRef]:
        refs: dict[int, ConcursoRef] = {}
        last_url = SITIO_INSTITUCIONAL
        for page in range(1, MAX_PAGES + 1):
            url = LISTADO_URL if page == 1 else f"{LISTADO_URL}?page={page}"
            try:
                response = self.request(
                    url,
                    headers={"Referer": last_url},
                    timeout=self.timeout,
                )
            except Exception as exc:
                self.logger.info(
                    "evento=carabineros_listado_fail scraper=%s page=%s error=%s",
                    self.nombre,
                    page,
                    type(exc).__name__,
                )
                break
            nuevos = self._parse_listado(response.text, refs)
            if not nuevos:
                break
            last_url = url
            if self.max_results and len(refs) >= self.max_results:
                break
        return list(refs.values())

    @staticmethod
    def _parse_listado(
        html: str, acc: dict[int, ConcursoRef]
    ) -> set[int]:
        """Extrae IDs únicos desde el HTML del listado.

        Estrategia tolerante al layout: cualquier `<a href="/concursos/{N}">`
        que NO sea un /download/ cuenta como un concurso del listado.
        """
        soup = BeautifulSoup(html, "html.parser")
        nuevos: set[int] = set()
        for anchor in soup.select('a[href*="/concursos/"]'):
            href = (anchor.get("href") or "").strip()
            if not href or "/download/" in href:
                continue
            match = RE_CONCURSO_HREF.search(href)
            if not match:
                continue
            cid = int(match.group(1))
            if cid in acc:
                continue
            container = anchor.find_parent(["article", "li", "div", "tr"]) or anchor
            preview = clean_text(anchor.get_text(" ", strip=True))
            if not preview:
                preview = clean_text(container.get_text(" ", strip=True))[:240]
            acc[cid] = ConcursoRef(
                id=cid,
                url_detalle=urljoin(BASE_URL, f"/concursos/{cid}"),
                preview_title=preview,
            )
            nuevos.add(cid)
        return nuevos

    # ──────────── Detalle HTML /concursos/{id} ────────────────────────

    def _fetch_detalle(self, ref: ConcursoRef) -> OfertaCarabineros | None:
        try:
            response = self.request(
                ref.url_detalle,
                headers={"Referer": LISTADO_URL},
                timeout=self.timeout,
            )
        except Exception as exc:
            # 404 se trata como oferta cerrada/retirada; BaseScraper ya lo marca como terminal.
            message = str(exc).lower()
            if "404" in message:
                self.logger.info(
                    "evento=carabineros_detalle_404 scraper=%s id=%s",
                    self.nombre,
                    ref.id,
                )
                return None
            raise

        oferta = OfertaCarabineros(
            id=ref.id,
            url_detalle=ref.url_detalle,
            titulo_cargo=ref.preview_title or None,
            url_descriptor=PDF_TMPL.format(id=ref.id, tipo="Descriptor"),
            url_perfil=PDF_TMPL.format(id=ref.id, tipo="Perfil"),
        )
        self._parse_detalle_html(response.text, oferta)
        return oferta

    def _parse_detalle_html(self, html: str, o: OfertaCarabineros) -> None:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        main = soup.select_one("main") or soup.select_one("section") or soup.body or soup
        o.html_text = clean_text(main.get_text(" ", strip=True))
        o.headings = [
            clean_text(h.get_text(" ", strip=True))
            for h in soup.select("h1, h2, h3")
            if clean_text(h.get_text(" ", strip=True))
        ]

        # Título del cargo: primer h1/h2 significativo del contenido principal.
        for tag in main.select("h1, h2, .card-title, .title"):
            text = clean_text(tag.get_text(" ", strip=True))
            if text and len(text) > 3:
                o.titulo_cargo = text
                break

        # Código C-NN-YYYY en cualquier lugar del texto o del título.
        for source_text in (o.titulo_cargo or "", o.html_text):
            match = RE_CODIGO.search(source_text)
            if match:
                o.codigo_concurso = re.sub(r"\s+", "", match.group(0)).upper()
                break

        # Campos clave en pares key/value típicos de Blade (dt/dd, th/td, label/value).
        for key, value in _iter_kv_pairs(soup):
            key_norm = normalize_key(key)
            if not key_norm or not value:
                continue
            if ("region" in key_norm) and not o.region:
                o.region = normalize_region(value)
            elif "comuna" in key_norm and not o.comuna:
                o.comuna = value
            elif "jornada" in key_norm and not o.jornada:
                o.jornada = value
            elif ("renta" in key_norm or "remuneracion" in key_norm) and not o.renta_texto:
                o.renta_texto = value
            elif "vacante" in key_norm and o.vacantes is None:
                match = RE_NUM.search(value)
                if match:
                    o.vacantes = int(match.group(0))
            elif "cierre" in key_norm or "termino" in key_norm or "hasta" in key_norm:
                parsed = parse_date(value)
                if parsed and not o.fecha_cierre:
                    o.fecha_cierre = parsed
            elif "publicacion" in key_norm or "inicio" in key_norm or "desde" in key_norm:
                parsed = parse_date(value)
                if parsed and not o.fecha_publicacion:
                    o.fecha_publicacion = parsed
            elif ("tipo" in key_norm and "contrato" in key_norm) or "calidad" in key_norm:
                if not o.tipo_contrato:
                    o.tipo_contrato = value

    # ──────────────── PDFs (Descriptor + Perfil) ──────────────────────

    def _enrich_with_pdfs(self, o: OfertaCarabineros) -> None:
        """Descarga y parsea los dos PDFs. Fuente de verdad de los campos ricos."""
        if not pdfplumber:
            self.logger.info(
                "evento=carabineros_pdfplumber_missing scraper=%s",
                self.nombre,
            )
            return

        tasks = (
            ("Descriptor", o.url_descriptor, HEADERS_DESCRIPTOR, "pdf_descriptor_text"),
            ("Perfil",     o.url_perfil,     HEADERS_PERFIL,     "pdf_perfil_text"),
        )
        for tipo, url, mapping, attr in tasks:
            texto = self._fetch_pdf_text(url, referer=o.url_detalle)
            if not texto:
                continue
            setattr(o, attr, texto)
            for field_name, value in split_pdf_sections(texto, mapping).items():
                if not value:
                    continue
                current = getattr(o, field_name, "") or ""
                if len(value) > len(current):
                    setattr(o, field_name, value)

    def _fetch_pdf_text(self, url: str, referer: str) -> str:
        assert pdfplumber is not None  # garantizado por el caller
        try:
            response = self.request(
                url,
                headers={
                    "Referer": referer,
                    "Accept": "application/pdf,*/*;q=0.8",
                },
                timeout=TIMEOUT_PDF,
            )
        except Exception as exc:
            self.logger.info(
                "evento=carabineros_pdf_fail scraper=%s url=%s error=%s",
                self.nombre,
                url,
                type(exc).__name__,
            )
            return ""
        content = response.content or b""
        if not content.startswith(b"%PDF"):
            self.logger.info(
                "evento=carabineros_pdf_no_binary scraper=%s url=%s bytes=%s",
                self.nombre,
                url,
                len(content),
            )
            return ""
        try:
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
            return clean_pdf_text("\n".join(pages))
        except Exception as exc:  # pragma: no cover - defensa runtime
            self.logger.info(
                "evento=carabineros_pdf_parse_fail scraper=%s url=%s error=%s",
                self.nombre,
                url,
                type(exc).__name__,
            )
            return ""

    # ─────────── Conversión al formato que consume el pipeline ────────

    def _oferta_to_raw(self, o: OfertaCarabineros) -> dict[str, Any]:
        return {
            "id": o.id,
            "title": o.titulo_cargo or f"Concurso {o.codigo_concurso or o.id}",
            "content_text": o.consolidated_text(),
            "html_text": o.html_text,
            "pdf_descriptor": o.pdf_descriptor_text,
            "pdf_perfil": o.pdf_perfil_text,
            "date": o.fecha_publicacion,
            "fecha_cierre": o.fecha_cierre,
            "url": o.url_detalle,
            "pdf_links": [u for u in (o.url_descriptor, o.url_perfil) if u],
            "attachment_texts": [
                txt for txt in (o.pdf_descriptor_text, o.pdf_perfil_text) if txt
            ],
            "headings": o.headings,
            "section_hint": "concursos",
            "oferta": o,   # se preserva para parse_oferta sin re-parsear
        }

    # ─────────── parse_oferta: pipeline + merge con HTML/PDF ──────────

    def parse_oferta(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        o: OfertaCarabineros = raw.get("oferta")  # type: ignore[assignment]
        if o is None:
            # fallback defensivo si alguien inyecta un raw sin el objeto.
            return None

        raw_page = build_raw_page_from_generic(
            source_id=str(self.institucion.get("id") or self.nombre),
            source_name=str(self.institucion.get("nombre") or self.nombre),
            source_url=self.url_empleo or BASE_URL,
            raw=raw,
            platform="carabineros",
        )
        posting, trace = self.pipeline.run(raw_page)
        if not posting:
            self.logger.info(
                "evento=carabineros_rechazada scraper=%s id=%s reasons=%s",
                self.nombre,
                o.id,
                trace.get("rejection_reasons"),
            )
            return None

        # Merge: HTML detalle gana en campos tabulados; pipeline gana en texto libre.
        renta_min, renta_max, grado_from_text = parse_renta(o.renta_texto or "")
        if posting.get("salary_amount"):
            renta_min = renta_min or int(posting["salary_amount"])
            renta_max = renta_max or int(posting["salary_amount"])

        cargo = o.titulo_cargo or posting.get("job_title") or f"Concurso {o.id}"
        descripcion = _join_nonempty(
            [o.descripcion, posting.get("description") or "", o.html_text[:600]]
        )
        requisitos = _join_nonempty(
            [
                o.requisitos,
                "\n".join(posting.get("requirements") or []),
                o.formacion,
                o.experiencia,
                o.capacitacion,
                o.competencias,
                o.documentos,
            ]
        )

        oferta = {
            "institucion_id": self.institucion.get("id"),
            "institucion_nombre": self.institucion.get("nombre"),
            "cargo": cargo,
            "descripcion": descripcion,
            "requisitos": requisitos,
            "tipo_contrato": (
                normalize_tipo_contrato(o.tipo_contrato)
                or posting.get("contract_type")
            ),
            "region": o.region or normalize_region(self.institucion.get("region")),
            "ciudad": o.comuna,
            "renta_bruta_min": renta_min,
            "renta_bruta_max": renta_max,
            "grado_eus": grado_from_text,
            "jornada": o.jornada or posting.get("workday"),
            "area_profesional": None,
            "fecha_publicacion": o.fecha_publicacion or posting.get("published_at"),
            "fecha_cierre": o.fecha_cierre or posting.get("application_end_at"),
            "url_oferta": o.url_detalle,
            "url_bases": o.url_descriptor or o.url_perfil,
            "estado": "cerrado" if posting.get("is_expired") else "activo",
        }
        return self.normalize_offer(oferta)


# ────────────────────────── Helpers puros ───────────────────────────────


def _iter_kv_pairs(soup: BeautifulSoup) -> Iterable[tuple[str, str]]:
    """Itera pares clave/valor desde dl>dt/dd y table>tr>th/td.

    Se usan selectores tolerantes para absorber variaciones del layout Blade.
    """
    # 1. <dl><dt>K</dt><dd>V</dd></dl>
    for dl in soup.select("dl"):
        dts = dl.select("dt")
        dds = dl.select("dd")
        for dt, dd in zip(dts, dds):
            yield (
                clean_text(dt.get_text(" ", strip=True)),
                clean_text(dd.get_text(" ", strip=True)),
            )

    # 2. <table><tr><th>K</th><td>V</td></tr></table>
    #    y <tr><td>K</td><td>V</td></tr> cuando es tabla key/value.
    for row in soup.select("tr"):
        cells = row.find_all(["th", "td"], recursive=False)
        if len(cells) >= 2:
            key = clean_text(cells[0].get_text(" ", strip=True))
            value = clean_text(cells[1].get_text(" ", strip=True))
            if key and value:
                yield key, value

    # 3. Patrón "label: value" dentro de párrafos, separado por <br>.
    for tag in soup.select("p, li, div.info, div.detail"):
        text = clean_text(tag.get_text(" ", strip=True))
        if not text or ":" not in text or len(text) > 260:
            continue
        # Toma solo la primera oración tipo "Clave: Valor".
        key, _, value = text.partition(":")
        key = clean_text(key)
        value = clean_text(value)
        if key and value and len(key) <= 60:
            yield key, value


def split_pdf_sections(
    text: str, mapping: dict[str, tuple[str, ...]]
) -> dict[str, str]:
    """Particiona el texto del PDF por headers canónicos.

    Para cada campo busca el primer header que aparezca y corta hasta el
    siguiente header detectado (del mismo o de cualquier campo), preservando
    el orden original del documento.
    """
    if not text:
        return {}
    upper = text.upper()
    hits: list[tuple[int, str, str]] = []  # (offset, field_name, header_match)
    for field_name, headers in mapping.items():
        for header in headers:
            i = upper.find(header.upper())
            if i != -1:
                hits.append((i, field_name, header))
                break
    if not hits:
        return {}
    hits.sort(key=lambda x: x[0])
    result: dict[str, str] = {}
    for idx, (offset, field_name, header) in enumerate(hits):
        end = hits[idx + 1][0] if idx + 1 < len(hits) else len(text)
        chunk = text[offset + len(header) : end]
        cleaned = clean_pdf_text(chunk)
        if cleaned:
            result[field_name] = cleaned
    return result


def clean_pdf_text(value: str) -> str:
    """Normaliza texto crudo extraído de PDF."""
    if not value:
        return ""
    # Quitar separadores de página y numeración típica.
    value = re.sub(r"Página\s*\d+\s*(?:de\s*\d+)?", "", value, flags=re.I)
    # Colapsar espacios horizontales sin destruir saltos de línea.
    value = re.sub(r"[ \t]+", " ", value)
    # Colapsar saltos excesivos.
    value = re.sub(r"\n{3,}", "\n\n", value)
    # Arreglar palabras cortadas por guión al final de línea.
    value = re.sub(r"-\n(\w)", r"\1", value)
    return value.strip(" \n\t:")


def _join_nonempty(values: Iterable[str | None]) -> str:
    seen: list[str] = []
    for value in values:
        text = clean_text(value or "")
        if not text:
            continue
        # Evitar duplicar bloques que ya están contenidos en otro previo.
        if any(text in existing for existing in seen):
            continue
        seen.append(text)
    return "\n\n".join(seen).strip()


# ──────────────────────────── Entry points ──────────────────────────────


def load_instituciones(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return payload.get("instituciones") if isinstance(payload, dict) else payload


def ejecutar(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
    max_results: int | None = None,
) -> dict[str, Any]:
    scraper = CarabinerosScraper(
        institucion=institucion,
        instituciones_catalogo=instituciones_catalogo,
        dry_run=dry_run,
        max_results=max_results,
    )
    return scraper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraper de postulaciones.carabineros.cl"
    )
    parser.add_argument("--json", required=True, help="Ruta al JSON maestro de instituciones")
    parser.add_argument("--id", type=int, default=161, help="ID de la institución (por defecto 161)")
    parser.add_argument("--dry-run", action="store_true", help="No escribir en PostgreSQL")
    parser.add_argument("--max", type=int, default=None, help="Limite de ofertas a procesar")
    args = parser.parse_args()

    instituciones = load_instituciones(args.json)
    objetivo = next((i for i in instituciones if i.get("id") == args.id), None)
    if not objetivo:
        raise SystemExit(f"No se encontró la institución con id={args.id}")

    print(
        ejecutar(
            institucion=objetivo,
            instituciones_catalogo=instituciones,
            dry_run=args.dry_run,
            max_results=args.max,
        )
    )
