from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
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
    normalize_key,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
)


class WordPressScraper(BaseScraper):
    """Scraper generico para sitios WordPress de municipios."""

    def __init__(
        self,
        institucion: dict[str, Any],
        instituciones_catalogo: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        max_results: int | None = None,
    ) -> None:
        self.institucion = institucion
        self.base_url = self._resolver_base_url(institucion)
        self.url_empleo = clean_text(
            institucion.get("url_empleo") or institucion.get("url_portal_empleos")
        )
        super().__init__(
            nombre=f"scraper.wordpress.{self._slug(institucion.get('nombre'))}",
            instituciones=instituciones_catalogo or [institucion],
            dry_run=dry_run,
            delay=2.0,
            timeout=10,
            max_results=max_results,
        )
        if institucion.get("id") is not None:
            self.scope_institucion_ids = [institucion["id"]]
        host_pattern = extract_host_like_pattern(self.url_empleo or self.base_url)
        if host_pattern:
            self.scope_url_patterns = [host_pattern]

    def fetch_ofertas(self) -> list[dict[str, Any]]:
        ofertas = self._fetch_via_rest_api()
        if ofertas:
            return ofertas
        ofertas = self._fetch_via_feed_json()
        if ofertas:
            return ofertas
        return self._fetch_via_html()

    def parse_oferta(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        content_text = self._limpiar_boilerplate(clean_text(raw.get("content_text")))
        title = clean_text(raw.get("title"))
        url_oferta = clean_text(raw.get("url"))
        pdf_links = raw.get("pdf_links") or []

        fecha_publicacion = parse_date(raw.get("date"))
        today = datetime.now(timezone.utc).date()
        cutoff = today - timedelta(days=180)

        # Siempre intentar extraer fecha_cierre desde contenido y detalle antes de descartar.
        fecha_cierre = parse_date(raw.get("fecha_cierre")) or self._extraer_fecha_cierre(content_text)

        detail_text = ""
        detail_pdfs: list[str] = []
        if url_oferta:
            detail_text, detail_pdfs = self._extraer_contexto_detalle(url_oferta)
            if detail_pdfs:
                pdf_links = self._deduplicate_urls([*pdf_links, *detail_pdfs])
            if not fecha_cierre:
                fecha_cierre = self._extraer_fecha_cierre(detail_text)
            if not fecha_cierre:
                fecha_cierre = self._extraer_fecha_cierre_desde_adjuntos(pdf_links)

        # Descartar ofertas con fecha de cierre ya vencida (motivo explícito)
        if fecha_cierre and fecha_cierre < today:
            return self._descartar_oferta(raw, url_oferta, "wordpress_expired_deadline")

        # Publicación > 180 días: sólo aceptar si hay evidencia de plazo vigente.
        if fecha_publicacion and fecha_publicacion < cutoff and not (
            fecha_cierre and fecha_cierre >= today
        ):
            return self._descartar_oferta(raw, url_oferta, "wordpress_old_without_deadline")

        renta_min, renta_max, grado_eus = parse_renta(content_text)
        descripcion, requisitos = self._separar_descripcion_requisitos(content_text)

        oferta = {
            "institucion_id": self.institucion.get("id"),
            "institucion_nombre": self.institucion.get("nombre"),
            "cargo": title,
            "descripcion": descripcion,
            "requisitos": requisitos,
            "tipo_contrato": normalize_tipo_contrato(f"{title} {content_text}"),
            "region": normalize_region(self.institucion.get("region")),
            "ciudad": self._inferir_ciudad(self.institucion.get("nombre")),
            "renta_bruta_min": renta_min,
            "renta_bruta_max": renta_max,
            "grado_eus": grado_eus,
            "jornada": self._extraer_jornada(content_text),
            "area_profesional": self._inferir_area_profesional(title),
            "fecha_publicacion": fecha_publicacion,
            "fecha_cierre": fecha_cierre,
            "url_oferta": url_oferta,
            "url_bases": pdf_links[0] if pdf_links else url_oferta,
            "plataforma_empleo": "wordpress",
            "estado": "activo",
        }
        return self.normalize_offer(oferta)

    def _descartar_oferta(
        self,
        raw: dict[str, Any],
        url_oferta: str,
        motivo: str,
    ) -> None:
        raw["motivo_descarte"] = motivo
        self.logger.info(
            "evento=wordpress_descarte scraper=%s url=%s motivo=%s",
            self.nombre,
            url_oferta,
            motivo,
        )
        return None

    def _extraer_contexto_detalle(self, url_oferta: str) -> tuple[str, list[str]]:
        try:
            html_detalle = self.request_text(url_oferta)
        except Exception as exc:
            self.logger.info(
                "evento=wordpress_detalle_skip scraper=%s url=%s error=%s",
                self.nombre,
                url_oferta,
                exc,
            )
            return "", []

        texto_detalle = self._html_to_text(html_detalle)
        pdfs_detalle = self._extract_pdf_links_from_html(html_detalle, url_oferta)
        return texto_detalle, pdfs_detalle

    def _extraer_fecha_cierre_desde_adjuntos(self, pdf_links: list[str]) -> date | None:
        for link in pdf_links:
            parsed = urlparse(link)
            candidates = [
                clean_text(unquote(parsed.path)).replace("_", " "),
                clean_text(unquote(parsed.query)).replace("_", " "),
                clean_text(unquote(link)).replace("_", " "),
            ]
            for text in candidates:
                if not text:
                    continue
                fecha = self._extraer_fecha_cierre(text) or parse_date(text)
                if fecha:
                    return fecha
        return None

    def _fetch_via_rest_api(self) -> list[dict[str, Any]]:
        # Consulta inicial acotada (180 días). Si retorna cero vacantes, ejecutar
        # un segundo barrido ampliado sin "after" y, si tampoco retorna vacantes
        # (o falla), un tercer barrido con ventana de 365 días para cubrir sitios
        # con fechas desfasadas en WP REST. La vigencia real se valida después en
        # parse_oferta (fecha_cierre y reglas de antigüedad).
        ventanas_dias: list[int | None] = [180]
        modo_ampliado = False
        while ventanas_dias:
            dias = ventanas_dias.pop(0)
            ofertas: list[dict[str, Any]] = []
            pagina = 1
            while pagina <= 10:
                query = f"per_page=20&page={pagina}"
                if dias is not None:
                    cutoff = (datetime.now(timezone.utc) - timedelta(days=dias)).strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    )
                    query = f"{query}&after={cutoff}"
                url = f"{self.base_url}/wp-json/wp/v2/posts?{query}"
                try:
                    payload = self.request_json(url)
                except Exception as exc:
                    if pagina == 1:
                        self.logger.info(
                            "evento=wordpress_rest_skip scraper=%s url=%s error=%s",
                            self.nombre,
                            url,
                            exc,
                        )
                    break

                if not isinstance(payload, list) or not payload:
                    break

                for post in payload:
                    title_html = ((post.get("title") or {}).get("rendered")) or ""
                    content_html = ((post.get("content") or {}).get("rendered")) or ""
                    title_text = self._html_to_text(title_html)
                    content_text = self._html_to_text(content_html)
                    if not self._parece_oferta(title_text, content_text):
                        continue
                    ofertas.append(
                        {
                            "title": title_text,
                            "content_text": content_text,
                            "date": post.get("date"),
                            "fecha_cierre": None,
                            "url": clean_text(post.get("link")),
                            "pdf_links": self._extract_pdf_links_from_html(
                                content_html,
                                clean_text(post.get("link")),
                            ),
                        }
                    )

                # Si la página vino con menos de 20 resultados, no hay más páginas
                if len(payload) < 20:
                    break
                pagina += 1

            ofertas_deduplicadas = self._deduplicate(ofertas)
            if ofertas_deduplicadas:
                if dias is not None and dias > 180:
                    self.logger.info(
                        "evento=wordpress_rest_fallback_window scraper=%s dias=%s total=%s",
                        self.nombre,
                        dias,
                        len(ofertas_deduplicadas),
                    )
                if dias is None:
                    self.logger.info(
                        "evento=wordpress_rest_fallback_no_after scraper=%s total=%s",
                        self.nombre,
                        len(ofertas_deduplicadas),
                    )
                return ofertas_deduplicadas
            if not modo_ampliado:
                modo_ampliado = True
                ventanas_dias = [None, 365]
                self.logger.info(
                    "evento=wordpress_rest_expandido scraper=%s razon=sin_vacantes_consulta_inicial",
                    self.nombre,
                )
        return []

    def _fetch_via_feed_json(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/category/concursos-publicos/feed/json"
        try:
            payload = self.request_json(url)
        except Exception as exc:
            self.logger.info(
                "evento=wordpress_feed_skip scraper=%s url=%s error=%s",
                self.nombre,
                url,
                exc,
            )
            return []

        items = payload.get("items") if isinstance(payload, dict) else None
        if not items:
            return []

        ofertas: list[dict[str, Any]] = []
        for item in items:
            content_html = item.get("content_html") or item.get("content_text") or ""
            link = clean_text(item.get("url") or item.get("link"))
            ofertas.append(
                {
                    "title": clean_text(item.get("title")),
                    "content_text": self._html_to_text(content_html),
                    "date": item.get("date_published") or item.get("date_modified"),
                    "fecha_cierre": None,
                    "url": link,
                    "pdf_links": self._extract_pdf_links_from_html(content_html, link),
                }
            )
        return self._deduplicate(ofertas)

    def _fetch_via_html(self) -> list[dict[str, Any]]:
        candidate_urls = []
        if self.url_empleo:
            candidate_urls.append(self.url_empleo)
        candidate_urls.append(f"{self.base_url}/?cat=concurso")

        ofertas: list[dict[str, Any]] = []
        for url in candidate_urls:
            try:
                html = self.request_text(url)
            except Exception as exc:
                self.logger.info(
                    "evento=wordpress_html_skip scraper=%s url=%s error=%s",
                    self.nombre,
                    url,
                    exc,
                )
                continue

            ofertas = self._parse_html_listing(html, url)
            if ofertas:
                return self._deduplicate(ofertas)
        return []

    def _parse_html_listing(self, html: str, source_url: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        containers = soup.select(
            "article, div.post, div.type-post, div.entry, li.post, div.blog-item"
        )
        ofertas: list[dict[str, Any]] = []

        for node in containers:
            title_el = node.select_one("h1 a, h2 a, h3 a, .entry-title a, a[href]")
            title = clean_text(title_el.get_text(" ", strip=True) if title_el else "")
            href = clean_text(title_el.get("href") if title_el else "")
            content_text = clean_text(node.get_text(" ", strip=True))
            if not self._parece_oferta(title, content_text):
                continue

            time_el = node.select_one("time[datetime], .entry-date, .posted-on time")
            date_value = None
            if time_el:
                date_value = time_el.get("datetime") or time_el.get_text(" ", strip=True)

            ofertas.append(
                {
                    "title": title or self._recortar_cargo(content_text),
                    "content_text": content_text,
                    "date": date_value,
                    "fecha_cierre": None,
                    "url": urljoin(source_url, href) if href else source_url,
                    "pdf_links": self._extract_pdf_links_from_node(node, source_url),
                }
            )

        if ofertas:
            return ofertas

        # Fallback final: enlaces sueltos a concursos o bases.
        for anchor in soup.select("a[href]"):
            href = anchor.get("href", "")
            title = clean_text(anchor.get_text(" ", strip=True))
            parent = anchor.find_parent(["li", "p", "div", "article", "section"])
            if parent is not None:
                context = clean_text(parent.get_text(" ", strip=True))
            else:
                context = title or clean_text(anchor.get("title"))
            if not self._parece_oferta(title, context):
                continue
            ofertas.append(
                {
                    "title": title or self._recortar_cargo(context),
                    "content_text": context,
                    "date": None,
                    "fecha_cierre": None,
                    "url": urljoin(source_url, href),
                    "pdf_links": [urljoin(source_url, href)] if ".pdf" in href.lower() else [],
                }
            )
        return ofertas

    def _extract_pdf_links_from_html(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html or "", "html.parser")
        return self._extract_pdf_links_from_node(soup, base_url)

    def _extract_pdf_links_from_node(self, node: BeautifulSoup | Any, base_url: str) -> list[str]:
        pdfs: list[str] = []
        for anchor in node.select("a[href]"):
            href = anchor.get("href", "")
            text = clean_text(anchor.get_text(" ", strip=True)).lower()
            if ".pdf" in href.lower() or "bases" in text:
                pdfs.append(urljoin(base_url, href))
        return self._deduplicate_urls(pdfs)

    # Regex que detecta el inicio del bloque de boilerplate (widgets de terceros)
    _BOILERPLATE_RE = re.compile(
        r"\d+\s+personas han compartido su experiencia postulando"
        r"|ver experiencias completas",
        re.IGNORECASE,
    )
    # Cabeceras que marcan el inicio de la sección de requisitos en el contenido
    _REQ_MARKER_RE = re.compile(
        r"(?:documentos necesarios|requisitos(?: principales)?|"
        r"perfil requerido|lo que necesitas|"
        r"para postular[,\s]+(?:debes|necesitas|env[íi]a)|"
        r"antecedentes\s+requeridos|"
        r"curriculum vitae\b)",
        re.IGNORECASE,
    )

    def _limpiar_boilerplate(self, text: str) -> str:
        """Elimina bloques de texto boilerplate (widgets, sidebars) del contenido."""
        m = self._BOILERPLATE_RE.search(text)
        if m:
            text = text[: m.start()].strip()
        return text

    def _separar_descripcion_requisitos(self, content_text: str) -> tuple[str | None, str | None]:
        """Divide el contenido en descripción introductoria y sección de requisitos."""
        if not content_text:
            return None, None
        m = self._REQ_MARKER_RE.search(content_text)
        # Sólo separar si hay al menos 80 chars de descripción antes del marcador
        if m and m.start() >= 80:
            descripcion = content_text[: m.start()].strip() or None
            requisitos = content_text[m.start() :].strip() or None
            return descripcion, requisitos
        # Sin marcador claro: todo va a descripción
        return content_text or None, None

    # Palabras clave que preceden a la fecha de cierre
    _CIERRE_CONTEXT_RE = re.compile(
        r"(?:"
        r"cierre\s+de\s+postulaci[oó]n|"
        r"plazo\s+de\s+postulaci[oó]n|"
        r"fecha\s+(?:l[ií]mite|de\s+cierre|de\s+vencimiento)|"
        r"recepci[oó]n\s+de\s+antecedentes(?:\s+hasta)?|"
        r"postulaciones?\s+(?:abiertas?\s+)?hasta|"
        r"postular\s+hasta(?:\s+el)?|"
        r"hasta\s+el\s+(?:d[ií]a\s+)?|"
        r"v[eé]nce?(?:\s+el)?|"
        r"plazo\s+hasta"
        r")"
        r"[^\d\w]{0,25}",
        re.IGNORECASE,
    )
    # Fecha numérica: 20/04/2026 o 20-04-26
    _DATE_NUM_RE = re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}")
    # Fecha textual: "20 de abril de 2026" o "jueves 20 de junio"
    _DATE_TEXT_RE = re.compile(
        r"(?:lunes|martes|mi[ée]rcoles|jueves|viernes|s[áa]bado|domingo)?\s*"
        r"(\d{1,2})\s+de\s+"
        r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)"
        r"(?:\s+de\s+(\d{4}))?",
        re.IGNORECASE,
    )

    def _extraer_fecha_cierre(self, text: str | None):
        content = clean_text(text)
        if not content:
            return None

        # 1. Buscar fecha precedida de palabra clave de cierre
        for m_ctx in self._CIERRE_CONTEXT_RE.finditer(content):
            resto = content[m_ctx.end():]
            # Intentar fecha numérica primero
            m_num = self._DATE_NUM_RE.match(resto.lstrip())
            if m_num:
                fecha = parse_date(m_num.group())
                if fecha:
                    return fecha
            # Luego fecha textual
            m_txt = self._DATE_TEXT_RE.match(resto.lstrip())
            if m_txt:
                fecha = parse_date(m_txt.group())
                if fecha:
                    return fecha

        # 2. Buscar cualquier fecha textual en el contenido (fallback)
        for m_txt in self._DATE_TEXT_RE.finditer(content):
            fecha = parse_date(m_txt.group())
            if fecha:
                return fecha

        return None

    def _extraer_jornada(self, text: str | None) -> str | None:
        content = clean_text(text)
        if not content:
            return None
        match = re.search(r"\b(\d{1,2})\s*horas\b", content, re.IGNORECASE)
        if match:
            return f"{match.group(1)} horas"
        if "media jornada" in normalize_key(content):
            return "media jornada"
        if "jornada completa" in normalize_key(content):
            return "jornada completa"
        return None

    def _inferir_area_profesional(self, cargo: str | None) -> str | None:
        key = normalize_key(cargo)
        if not key:
            return None
        area_map = {
            "salud": ["medic", "enfermer", "matron", "salud", "odontolog", "quimico"],
            "derecho": ["abogad", "jurid", "legal"],
            "ingenieria": ["ingenier", "arquitect", "informatic", "programador"],
            "educacion": ["docente", "profesor", "educador", "pedagog"],
            "administracion": ["analista", "administr", "gestion", "rrhh"],
            "social": ["social", "psicolog", "terapeuta", "sociolog"],
        }
        for area, keywords in area_map.items():
            if any(keyword in key for keyword in keywords):
                return area
        return "administracion"

    def _inferir_ciudad(self, nombre_institucion: str | None) -> str | None:
        text = clean_text(nombre_institucion)
        if not text:
            return None
        text = re.sub(r"^Municipalidad de\s+", "", text, flags=re.IGNORECASE).strip()
        return text or None

    # Palabras que en el TÍTULO indican fuertemente un aviso de empleo
    _TITLE_JOB_KEYWORDS = (
        "concurso publico",
        "llamado a concurso",
        "proceso de seleccion",
        "cargo",
        "vacante",
        "honorario",
        "contrata",
        "planta",
        "postulacion",
        "seleccion de personal",
        "reclutamiento",
        "profesional",
        "tecnico",
        "auxiliar",
        "administrativo",
        "asistente",
        "inspector",
        "conductor",
        "chofer",
        "enfermero",
        "enfermera",
        "medico",
        "medica",
        "abogado",
        "abogada",
        "director",
        "directora",
        "jefe",
        "jefa",
        "coordinador",
        "coordinadora",
        "encargado",
        "encargada",
    )
    # Palabras en el título que DESCARTAN el post aunque tenga otras señales
    _TITLE_NEGATIVE_KEYWORDS = (
        "resultado",
        "ganador",
        "ganadora",
        "adjudicacion",
        "licitacion",
        "acta",
        "resolucion exenta",
        "decreto",
        "acuerdo",
        "nomina",
        "lista de",
        "informe",
        "rendicion",
        "invitacion a",
        "taller",
        "capacitacion",
        "seminario",
        "webinar",
    )

    def _parece_oferta(self, title: str, content: str) -> bool:
        if len(clean_text(f"{title} {content}")) < 8:
            return False
        key_title = normalize_key(title)
        # Descartar si el título tiene señales claras de no ser un aviso de empleo
        if any(neg in key_title for neg in self._TITLE_NEGATIVE_KEYWORDS):
            return False
        # Aceptar si el título menciona directamente un cargo o proceso de selección
        if any(kw in key_title for kw in self._TITLE_JOB_KEYWORDS):
            return True
        # Fallback: el contenido tiene múltiples señales (más estricto que antes)
        key_content = normalize_key(content)
        content_hits = sum(
            1 for kw in ("concurso", "cargo", "postulacion", "seleccion", "contrata", "planta")
            if kw in key_content
        )
        return content_hits >= 2

    def _recortar_cargo(self, text: str) -> str:
        cleaned = clean_text(text)
        if len(cleaned) <= 180:
            return cleaned
        return cleaned[:177].rstrip() + "..."

    def _deduplicate(self, offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        results: list[dict[str, Any]] = []
        for offer in offers:
            key = clean_text(offer.get("url") or "") or clean_text(offer.get("title"))
            if not key or key in seen:
                continue
            seen.add(key)
            results.append(offer)
        return results

    @staticmethod
    def _deduplicate_urls(urls: list[str]) -> list[str]:
        seen: set[str] = set()
        results: list[str] = []
        for url in urls:
            cleaned = clean_text(url)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            results.append(cleaned)
        return results

    @staticmethod
    def _html_to_text(html: str | None) -> str:
        soup = BeautifulSoup(html or "", "html.parser")
        return clean_text(soup.get_text(" ", strip=True))

    @staticmethod
    def _resolver_base_url(institucion: dict[str, Any]) -> str:
        url = clean_text(institucion.get("sitio_web") or institucion.get("url_empleo"))
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"No se pudo determinar base_url para {institucion.get('nombre')}")
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")

    @staticmethod
    def _slug(value: str | None) -> str:
        key = normalize_key(value)
        return re.sub(r"[^a-z0-9]+", "_", key).strip("_") or "wordpress"


def load_instituciones(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return payload.get("instituciones") if isinstance(payload, dict) else payload


def ejecutar(
    institucion: dict[str, Any],
    instituciones_catalogo: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
    max_results: int | None = None,
) -> dict[str, Any]:
    scraper = WordPressScraper(
        institucion=institucion,
        instituciones_catalogo=instituciones_catalogo,
        dry_run=dry_run,
        max_results=max_results,
    )
    return scraper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper generico para WordPress")
    parser.add_argument("--json", required=True, help="Ruta al JSON maestro de instituciones")
    parser.add_argument("--id", type=int, required=True, help="ID de la institucion a ejecutar")
    parser.add_argument("--dry-run", action="store_true", help="No guarda en PostgreSQL")
    parser.add_argument("--max", type=int, default=None, help="Limite de ofertas")
    args = parser.parse_args()

    instituciones = load_instituciones(args.json)
    objetivo = next((item for item in instituciones if item.get("id") == args.id), None)
    if not objetivo:
        raise SystemExit(f"No se encontro la institucion con id={args.id}")

    print(
        ejecutar(
            institucion=objetivo,
            instituciones_catalogo=instituciones,
            dry_run=args.dry_run,
            max_results=args.max,
        )
    )
