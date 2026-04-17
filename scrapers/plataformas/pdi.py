"""Scraper para Policía de Investigaciones (PDI) — institución id=162.

El portal de trámite postulaciones.investigaciones.cl es una SPA React
(Create React App) con listado bajo login → inviable para scraping anónimo.

Este scraper ataca la fuente pública alternativa:

    https://www.pdichile.cl/institución/concursos-publicos/portada

El portal institucional publica los PDFs de perfil del cargo y las bases
del concurso directamente en el CMS Sitecore, sin auth.  Los PDFs son
la fuente de verdad para requisitos, funciones, formación, experiencia,
competencias y documentos.

Estructura de URLs observada vía índice público:

    /docs/default-source/cargo/perfil-*.pdf                   → perfil del cargo
    /docs/default-source/concurso-público---cargos/*.pdf      → bases del concurso
    /docs/default-source/pdf/*.pdf                            → calendarios, resoluciones

Los PDFs usan headers canónicos estables año a año
(DETALLES DE LA VACANTE, I. IDENTIFICACIÓN …, II. PROPÓSITO, III. FUNCIONES, …).

No se usa Playwright: la portada es HTML server-rendered clásico.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scrapers.base import (
    BaseScraper,
    clean_text,
    extract_host_like_pattern,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
)
from scrapers.job_pipeline import JobExtractionPipeline, build_raw_page_from_generic
from scrapers.plataformas.carabineros import clean_pdf_text, split_pdf_sections

try:  # pragma: no cover - dependencia opcional
    import pdfplumber  # type: ignore
except BaseException:  # noqa: BLE001 - cubre ImportError y PanicException de pyo3
    pdfplumber = None


# ────────────────────────── Constantes del portal ───────────────────────

BASE_HOST = "https://www.pdichile.cl"
# Usamos el path con tilde tal como lo publica el CMS. urllib/requests
# lo codifica correctamente al enviarlo.
PORTADA_URL = f"{BASE_HOST}/institución/concursos-publicos/portada"
PORTAL_TRAMITE = "https://postulaciones.investigaciones.cl/"  # para logging

# Patrones de los paths que interesan dentro del CMS Sitecore de PDI.
RE_PERFIL_HREF = re.compile(
    r"/docs/default-source/(?:cargo|perfil)[^\"'#?]+\.pdf", re.I
)
RE_BASES_HREF = re.compile(
    r"/docs/default-source/(?:concurso[^\"'#?]*|pdf|bases)[^\"'#?]+\.pdf", re.I
)
RE_PDF_HREF = re.compile(r"/docs/default-source/[^\"'#?]+\.pdf", re.I)

# Tokens usados para emparejar perfil ↔ bases del mismo concurso
# (comparten fecha o código resolex).
RE_TOKENS = re.compile(
    r"\b("
    r"resolex[-_]?\d+"
    r"|20\d{2}"
    r"|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre"
    r")\b",
    re.I,
)

DELAY_SEC = 2.0
TIMEOUT_HTML = 20
TIMEOUT_PDF = 30
MAX_PORTADA_SUBPAGES = 5  # portada + hasta N sub-secciones si existieran

# Headers canónicos observados en múltiples PDFs de perfil de PDI.
HEADERS_PERFIL_PDI: dict[str, tuple[str, ...]] = {
    "descripcion": (
        "PROPÓSITO DEL CARGO",
        "PROPOSITO DEL CARGO",
        "OBJETIVO DEL CARGO",
        "DETALLES DE LA VACANTE",
    ),
    "funciones": (
        "FUNCIONES DEL CARGO",
        "III. FUNCIONES",
        "FUNCIONES PRINCIPALES",
        "FUNCIONES",
    ),
    "requisitos": (
        "REQUISITOS DEL CARGO",
        "REQUISITOS LEGALES",
        "IV. REQUISITOS",
        "REQUISITOS",
    ),
    "formacion": (
        "FORMACIÓN EDUCACIONAL",
        "FORMACION EDUCACIONAL",
        "V. FORMACIÓN",
        "V. FORMACION",
        "NIVEL EDUCACIONAL",
        "ESTUDIOS REQUERIDOS",
    ),
    "experiencia": (
        "EXPERIENCIA LABORAL",
        "VI. EXPERIENCIA",
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
        "VII. COMPETENCIAS",
        "CONOCIMIENTOS Y COMPETENCIAS",
        "CONOCIMIENTOS ESPECÍFICOS",
        "CONOCIMIENTOS ESPECIFICOS",
        "COMPETENCIAS",
    ),
    "documentos": (
        "VIII. DOCUMENTOS",
        "DOCUMENTOS DE POSTULACIÓN",
        "DOCUMENTOS DE POSTULACION",
        "ANTECEDENTES DE POSTULACIÓN",
        "ANTECEDENTES DE POSTULACION",
        "DOCUMENTOS REQUERIDOS",
    ),
}

HEADERS_BASES_PDI: dict[str, tuple[str, ...]] = {
    "descripcion_bases": (
        "ANTECEDENTES GENERALES",
        "OBJETO DEL CONCURSO",
        "ANTECEDENTES",
    ),
    "calendario": (
        "CALENDARIZACIÓN",
        "CALENDARIZACION",
        "CALENDARIO",
        "ETAPAS DEL CONCURSO",
        "ETAPAS",
    ),
    "documentos_bases": (
        "DOCUMENTOS REQUERIDOS",
        "DOCUMENTACIÓN",
        "DOCUMENTACION",
        "ANTECEDENTES DE POSTULACIÓN",
    ),
}


# ──────────────────────────── Data classes ──────────────────────────────


@dataclass
class ConcursoPDI:
    """Representación de un concurso armada desde la portada + PDFs."""

    slug: str
    url_perfil: str | None = None
    url_bases: str | None = None
    titulo_cargo: str | None = None
    portada_snippet: str = ""
    fecha_publicacion: Any = None
    fecha_cierre: Any = None
    perfil_text: str = ""
    bases_text: str = ""
    secciones: dict[str, str] = field(default_factory=dict)

    @property
    def id(self) -> str:
        """ID estable: hash corto del slug. Resiste cambios del
        query string `?sfvrsn=...` del CMS Sitecore."""
        return hashlib.sha1(self.slug.encode("utf-8")).hexdigest()[:12]

    @property
    def source_url(self) -> str:
        return self.url_perfil or self.url_bases or PORTADA_URL


# ─────────────────────────────── Scraper ────────────────────────────────


class PdiScraper(BaseScraper):
    """Scraper de PDI vía portal institucional pdichile.cl."""

    def __init__(
        self,
        institucion: dict[str, Any],
        instituciones_catalogo: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        max_results: int | None = None,
    ) -> None:
        self.institucion = institucion
        self.url_empleo = clean_text(institucion.get("url_empleo")) or PORTADA_URL
        self.sitio_web = clean_text(institucion.get("sitio_web")) or BASE_HOST

        super().__init__(
            nombre="scraper.pdi",
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
        for url in (BASE_HOST, PORTAL_TRAMITE):
            pattern = extract_host_like_pattern(url)
            if pattern and pattern not in patterns:
                patterns.append(pattern)
        self.scope_url_patterns = patterns
        self.pipeline = JobExtractionPipeline()
        self._warmed_up = False

    # ─────────────────── FASE A: warmup de sesión ─────────────────────

    def _warmup(self) -> None:
        if self._warmed_up:
            return
        self._warmed_up = True
        try:
            self.request(BASE_HOST + "/", timeout=self.timeout)
        except Exception as exc:  # pragma: no cover - defensa runtime
            self.logger.info(
                "evento=pdi_warmup_fail scraper=%s error=%s",
                self.nombre,
                type(exc).__name__,
            )

    # ─────────────────── FASE B+C+D: fetch_ofertas ────────────────────

    def fetch_ofertas(self) -> list[dict[str, Any]]:
        self._warmup()
        concursos = self._enumerar_portada()
        self.logger.info(
            "evento=pdi_portada scraper=%s concursos=%s",
            self.nombre,
            len(concursos),
        )

        crudas: list[dict[str, Any]] = []
        limit = self.max_results or len(concursos)
        for concurso in concursos[:limit]:
            try:
                self._enrich_with_pdf(concurso, "perfil")
                self._enrich_with_pdf(concurso, "bases")
            except Exception as exc:  # pragma: no cover
                self.logger.warning(
                    "evento=pdi_enrich_fail scraper=%s slug=%s error=%s",
                    self.nombre,
                    concurso.slug,
                    type(exc).__name__,
                )
            # Sin texto útil, no emitimos (el pipeline lo rechazaría igual).
            if not concurso.perfil_text and not concurso.bases_text \
                    and not concurso.portada_snippet:
                continue
            crudas.append(self._concurso_to_raw(concurso))
        return crudas

    # ───── Enumeración de la portada + sub-secciones si aparecen ──────

    def _enumerar_portada(self) -> list[ConcursoPDI]:
        """Recorre la portada de concursos públicos y deriva la lista
        canónica de concursos emparejando PDFs de perfil y bases."""
        visitadas: set[str] = set()
        pendientes: list[str] = [PORTADA_URL]

        perfiles: dict[str, ConcursoPDI] = {}
        bases: dict[str, str] = {}   # slug → url_bases
        subsection_links: set[str] = set()

        while pendientes and len(visitadas) < MAX_PORTADA_SUBPAGES:
            url = pendientes.pop(0)
            if url in visitadas:
                continue
            visitadas.add(url)
            try:
                response = self.request(
                    url,
                    headers={"Referer": BASE_HOST + "/"},
                    timeout=self.timeout,
                )
            except Exception as exc:
                self.logger.info(
                    "evento=pdi_portada_fail scraper=%s url=%s error=%s",
                    self.nombre,
                    url,
                    type(exc).__name__,
                )
                continue

            self._parse_portada(response.text, url, perfiles, bases, subsection_links)

            # Seed sub-secciones en el mismo bloque "concursos-publicos/..."
            for sub in sorted(subsection_links):
                if sub not in visitadas and sub not in pendientes:
                    pendientes.append(sub)

        # Emparejar bases huérfanas a perfiles por tokens compartidos.
        for slug_bases, url_bases in bases.items():
            for perfil in perfiles.values():
                if perfil.url_bases:
                    continue
                if _slugs_relacionados(perfil.slug, slug_bases):
                    perfil.url_bases = url_bases
                    break

        # Bases sin ningún perfil → emitir como concurso "sólo bases"
        # para no perder información del listado.
        for slug_bases, url_bases in bases.items():
            if any(p.url_bases == url_bases for p in perfiles.values()):
                continue
            perfiles[slug_bases] = ConcursoPDI(
                slug=slug_bases,
                url_bases=url_bases,
                titulo_cargo=_slug_to_title(slug_bases),
            )

        return list(perfiles.values())

    def _parse_portada(
        self,
        html: str,
        source_url: str,
        perfiles: dict[str, ConcursoPDI],
        bases: dict[str, str],
        subsection_links: set[str],
    ) -> None:
        soup = BeautifulSoup(html, "html.parser")

        # 1. PDFs de perfil/bases en el CMS
        for anchor in soup.select('a[href*="/docs/default-source/"]'):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            lower = href.lower()
            # Aceptar .pdf con o sin query string (?sfvrsn=...).
            if not (lower.endswith(".pdf") or ".pdf?" in lower):
                continue
            full_url = urljoin(BASE_HOST, href)
            text_ancla = clean_text(anchor.get_text(" ", strip=True))
            container = anchor.find_parent(["li", "p", "div", "tr", "td"]) or anchor
            snippet = clean_text(container.get_text(" ", strip=True))[:400]
            slug = _slug_from_url(full_url)

            if RE_PERFIL_HREF.search(href):
                concurso = perfiles.setdefault(slug, ConcursoPDI(slug=slug))
                concurso.url_perfil = full_url
                if text_ancla and not concurso.titulo_cargo:
                    concurso.titulo_cargo = text_ancla
                if snippet and len(snippet) > len(concurso.portada_snippet):
                    concurso.portada_snippet = snippet
            elif RE_BASES_HREF.search(href):
                bases.setdefault(slug, full_url)

        # 2. Enlaces a sub-secciones del área de concursos (para seguir
        #    recorriendo en otra pasada si existieran).
        for anchor in soup.select('a[href*="concursos-publicos"]'):
            href = (anchor.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            full = urljoin(source_url, href)
            if "pdichile" not in urlparse(full).netloc:
                continue
            if full == source_url or full == PORTADA_URL:
                continue
            subsection_links.add(full)

    # ─────────────── PDFs (perfil del cargo + bases) ──────────────────

    def _enrich_with_pdf(self, concurso: ConcursoPDI, tipo: str) -> None:
        url = concurso.url_perfil if tipo == "perfil" else concurso.url_bases
        if not url or not pdfplumber:
            if url and not pdfplumber:
                self.logger.info(
                    "evento=pdi_pdfplumber_missing scraper=%s tipo=%s",
                    self.nombre,
                    tipo,
                )
            return

        texto = self._fetch_pdf_text(url, referer=PORTADA_URL)
        if not texto:
            return

        if tipo == "perfil":
            concurso.perfil_text = texto
            mapping = HEADERS_PERFIL_PDI
        else:
            concurso.bases_text = texto
            mapping = HEADERS_BASES_PDI
            _parsear_fechas_bases(concurso)

        for field_name, chunk in split_pdf_sections(texto, mapping).items():
            if not chunk:
                continue
            current = concurso.secciones.get(field_name, "") or ""
            if len(chunk) > len(current):
                concurso.secciones[field_name] = chunk

        # Intentar derivar título del cargo del propio PDF si la portada
        # no lo tenía (caso bases huérfanas).
        if not concurso.titulo_cargo:
            concurso.titulo_cargo = _extraer_titulo_desde_pdf(texto)

    def _fetch_pdf_text(self, url: str, referer: str) -> str:
        assert pdfplumber is not None
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
                "evento=pdi_pdf_fail scraper=%s url=%s error=%s",
                self.nombre,
                url,
                type(exc).__name__,
            )
            return ""
        content = response.content or b""
        if not content.startswith(b"%PDF"):
            return ""
        try:
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
            return clean_pdf_text("\n".join(pages))
        except Exception as exc:  # pragma: no cover
            self.logger.info(
                "evento=pdi_pdf_parse_fail scraper=%s url=%s error=%s",
                self.nombre,
                url,
                type(exc).__name__,
            )
            return ""

    # ─────────── Conversión al formato que consume el pipeline ────────

    def _concurso_to_raw(self, c: ConcursoPDI) -> dict[str, Any]:
        content_parts = [
            c.portada_snippet,
            c.secciones.get("descripcion", ""),
            c.secciones.get("funciones", ""),
            c.secciones.get("requisitos", ""),
            c.secciones.get("formacion", ""),
            c.secciones.get("experiencia", ""),
            c.secciones.get("capacitacion", ""),
            c.secciones.get("competencias", ""),
            c.secciones.get("documentos", ""),
            c.bases_text[:4000] if c.bases_text else "",
        ]
        consolidated = "\n\n".join(p for p in content_parts if p)
        return {
            "id": c.id,
            "title": c.titulo_cargo or f"PDI — {_slug_to_title(c.slug)}",
            "content_text": consolidated,
            "pdf_descriptor": c.bases_text,
            "pdf_perfil": c.perfil_text,
            "date": c.fecha_publicacion,
            "fecha_cierre": c.fecha_cierre,
            "url": c.source_url,
            "pdf_links": [u for u in (c.url_perfil, c.url_bases) if u],
            "attachment_texts": [t for t in (c.perfil_text, c.bases_text) if t],
            "headings": [],
            "section_hint": "concursos-publicos",
            "concurso": c,
        }

    # ─────────── parse_oferta: pipeline + merge ───────────────────────

    def parse_oferta(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        concurso: ConcursoPDI = raw.get("concurso")  # type: ignore[assignment]
        if concurso is None:
            return None

        raw_page = build_raw_page_from_generic(
            source_id=str(self.institucion.get("id") or self.nombre),
            source_name=str(self.institucion.get("nombre") or "PDI"),
            source_url=PORTADA_URL,
            raw=raw,
            platform="pdi_institucional",
        )
        posting, trace = self.pipeline.run(raw_page)
        if not posting:
            self.logger.info(
                "evento=pdi_rechazada scraper=%s slug=%s reasons=%s",
                self.nombre,
                concurso.slug,
                trace.get("rejection_reasons"),
            )
            return None

        # Renta/grado: texto combinado de perfil + bases tiende a tener
        # el grado E.U.S. y la renta bruta.
        renta_texto = " ".join(
            [
                concurso.secciones.get("requisitos", ""),
                concurso.secciones.get("formacion", ""),
                concurso.perfil_text,
                concurso.bases_text,
            ]
        )
        renta_min, renta_max, grado = parse_renta(renta_texto)
        if posting.get("salary_amount"):
            salario = int(posting["salary_amount"])
            renta_min = renta_min or salario
            renta_max = renta_max or salario

        requisitos_full = _join_nonempty(
            [
                concurso.secciones.get("requisitos", ""),
                concurso.secciones.get("formacion", ""),
                concurso.secciones.get("experiencia", ""),
                concurso.secciones.get("capacitacion", ""),
                concurso.secciones.get("competencias", ""),
                concurso.secciones.get("documentos", ""),
                "\n".join(posting.get("requirements") or []),
            ]
        )

        oferta = {
            "institucion_id": self.institucion.get("id"),
            "institucion_nombre": self.institucion.get("nombre"),
            "cargo": concurso.titulo_cargo or posting.get("job_title") or _slug_to_title(concurso.slug),
            "descripcion": concurso.secciones.get("descripcion")
                           or posting.get("description")
                           or concurso.portada_snippet,
            "requisitos": requisitos_full,
            "tipo_contrato": normalize_tipo_contrato(
                concurso.bases_text + " " + concurso.perfil_text
            ) or posting.get("contract_type"),
            "region": normalize_region(self.institucion.get("region")),
            "ciudad": None,
            "renta_bruta_min": renta_min,
            "renta_bruta_max": renta_max,
            "grado_eus": grado,
            "jornada": posting.get("workday"),
            "area_profesional": None,
            "fecha_publicacion": concurso.fecha_publicacion or posting.get("published_at"),
            "fecha_cierre": concurso.fecha_cierre or posting.get("application_end_at"),
            "url_oferta": concurso.source_url,
            "url_bases": concurso.url_bases or concurso.url_perfil,
            "estado": "cerrado" if posting.get("is_expired") else "activo",
        }
        return self.normalize_offer(oferta)


# ────────────────────────── Helpers puros ───────────────────────────────


def _slug_from_url(url: str) -> str:
    """Slug canónico del PDF, sin query string ni extensión."""
    path = urlparse(url).path
    filename = unquote(path.rsplit("/", 1)[-1])
    return re.sub(r"\.pdf.*$", "", filename, flags=re.I).lower()


def _slug_to_title(slug: str) -> str:
    """Heurística de título legible desde un slug tipo
    `perfil-adm-publico-resolex-222`."""
    if not slug:
        return ""
    words = re.split(r"[-_]+", slug)
    keep = [w for w in words if w and not w.isdigit() and w.lower() != "resolex"]
    return " ".join(w.capitalize() for w in keep).strip()


def _slugs_relacionados(slug_a: str, slug_b: str) -> bool:
    """Dos PDFs pertenecen al mismo concurso si comparten al menos un
    token temporal (mes/año) o un código resolex-NNN."""
    tokens_a = {m.group(0).lower() for m in RE_TOKENS.finditer(slug_a)}
    tokens_b = {m.group(0).lower() for m in RE_TOKENS.finditer(slug_b)}
    return bool(tokens_a & tokens_b)


def _extraer_titulo_desde_pdf(text: str) -> str | None:
    """Busca una línea tipo `Cargo: XYZ` al inicio de un PDF de perfil."""
    if not text:
        return None
    match = re.search(
        r"cargo\s*[:\-]\s*([^\n\r]{3,120})",
        text,
        re.I,
    )
    if match:
        return clean_text(match.group(1))
    return None


def _parsear_fechas_bases(concurso: ConcursoPDI) -> None:
    """Extrae fecha de publicación y cierre desde texto de bases.

    PDI suele exponer un cuadro "CALENDARIZACIÓN" con filas tipo
    "Publicación ... dd/mm/yyyy". Aquí usamos regex defensivas
    porque los layouts varían por año.
    """
    text = concurso.bases_text or concurso.secciones.get("calendario", "")
    if not text:
        return
    # Patrones: "publicación ... 03/03/2026" o "publicación ... 3 de marzo de 2026".
    if not concurso.fecha_publicacion:
        m = re.search(
            r"publicaci[oó]n[^.\n]{0,120}?"
            r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{1,2}\s+de\s+\w+\s+de\s+\d{4})",
            text,
            re.I,
        )
        if m:
            concurso.fecha_publicacion = parse_date(m.group(1))
    if not concurso.fecha_cierre:
        m = re.search(
            r"(?:cierre|t[eé]rmino|hasta|recepci[oó]n)[^.\n]{0,120}?"
            r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{1,2}\s+de\s+\w+\s+de\s+\d{4})",
            text,
            re.I,
        )
        if m:
            concurso.fecha_cierre = parse_date(m.group(1))


def _join_nonempty(values: list[str | None]) -> str:
    kept: list[str] = []
    for value in values:
        text = clean_text(value or "")
        if not text:
            continue
        if any(text in existing for existing in kept):
            continue
        kept.append(text)
    return "\n\n".join(kept).strip()


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
    scraper = PdiScraper(
        institucion=institucion,
        instituciones_catalogo=instituciones_catalogo,
        dry_run=dry_run,
        max_results=max_results,
    )
    return scraper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraper de concursos públicos PDI (fuente: pdichile.cl)"
    )
    parser.add_argument("--json", required=True, help="Ruta al JSON maestro")
    parser.add_argument("--id", type=int, default=162, help="ID institución (default 162)")
    parser.add_argument("--dry-run", action="store_true", help="Sin escribir BD")
    parser.add_argument("--max", type=int, default=None, help="Limite de ofertas")
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
