from __future__ import annotations

import argparse
import asyncio
import re
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.base import (
    LegacyBaseScraper as BaseScraper,  # EmpleosPublicosScraper usa la API legacy síncrona
    clean_text,
    normalize_key,
    normalize_region,
    normalize_tipo_contrato,
    parse_date,
    parse_renta,
    truncate,
)

BASE_URL = "https://www.empleospublicos.cl"
LIST_URL = f"{BASE_URL}/pub/convocatorias/convocatorias.aspx"
LIST_CARD_SELECTOR = "div.caja.row.primerEmpleo, div.caja.row"
DETAIL_LINK_SELECTOR = (
    'a.btnverficha[href], '
    'a[href*="convpostularavisoTrabajo"][href], '
    'a[href*="convFicha"][href]'
)
BUTTON_TEXTS = {
    "ver bases",
    "postula en linea",
}
BADGE_TEXTS = {
    "solo difusion",
    "solo difusion interna",
}
BAD_CARGO_TEXTS = BUTTON_TEXTS | BADGE_TEXTS
HEADING_TAGS = ("h2", "h3", "h4", "strong")
# Tier CRITICAL: la fuente más densa del país. Damos margen de timeout (la
# Servicio Civil suele ser lenta en horario peak) y un reintento extra.
DEFAULT_TIMEOUT = 20
DEFAULT_MAX_ATTEMPTS = 4


@dataclass(slots=True)
class PageRequest:
    method: str
    url: str
    data: dict[str, str] | None = None

    def signature(self) -> tuple[str, str, tuple[tuple[str, str], ...]]:
        payload = tuple(sorted((self.data or {}).items()))
        return self.method.upper(), self.url, payload


class AsyncRateLimiter:
    def __init__(self, delay: float) -> None:
        self.delay = max(delay, 0)
        self._lock = asyncio.Lock()
        self._next_slot = 0.0

    async def wait(self) -> None:
        # Reservar el slot con el lock, pero dormir fuera de él
        # Así los workers concurrentes se turnan sin bloquearse entre sí
        async with self._lock:
            now = asyncio.get_running_loop().time()
            self._next_slot = max(self._next_slot, now) + self.delay
            sleep_time = self._next_slot - now - self.delay
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


class EmpleosPublicosScraper(BaseScraper):
    """Scraper principal para convocatorias activas de empleospublicos.cl."""

    def __init__(
        self,
        instituciones: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        max_results: int | None = None,
        strict_institution_match: bool = False,
    ) -> None:
        self.strict_institution_match = strict_institution_match
        super().__init__(
            nombre="scraper.empleos_publicos",
            instituciones=instituciones,
            dry_run=dry_run,
            delay=0.3,
            timeout=DEFAULT_TIMEOUT,
            max_results=max_results,
        )
        self.scope_url_patterns = [
            "https://www.empleospublicos.cl/%",
            "https://empleospublicos.cl/%",
            "http://www.empleospublicos.cl/%",
            "http://empleospublicos.cl/%",
        ]
        self._rate_limiter = AsyncRateLimiter(self.delay)
        # 30 detalles concurrentes: empíricamente la API soporta ese paralelismo
        # sin throttle agresivo. El semáforo evita saturar el servidor.
        self._detail_semaphore = asyncio.Semaphore(30)


    def fetch_ofertas(self) -> list[dict[str, Any]]:
        return asyncio.run(self._fetch_ofertas_async())

    def parse_oferta(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        raw = dict(raw)
        raw["institucion_id"] = raw.get("institucion_id") or self.match_institucion_id(
            raw.get("institucion_nombre")
        )
        if self.strict_institution_match and not raw.get("institucion_id"):
            self.logger.warning(
                "evento=skip_oferta_sin_match scraper=%s institucion=%s cargo=%s",
                self.nombre,
                raw.get("institucion_nombre"),
                raw.get("cargo"),
            )
            return None
        return self.normalize_offer(raw)

    async def _fetch_ofertas_async(self) -> list[dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=10, ssl=False)
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
            "Referer": BASE_URL,
        }

        ofertas: list[dict[str, Any]] = []
        urls_vistas: set[str] = set()
        firmas_paginas: set[tuple[str, ...]] = set()
        requests_visitados: set[tuple[str, str, tuple[tuple[str, str], ...]]] = set()
        page_request: PageRequest | None = PageRequest(method="GET", url=LIST_URL)
        pagina = 1

        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
            while page_request:
                if self.max_results and len(ofertas) >= self.max_results:
                    break

                firma_request = page_request.signature()
                if firma_request in requests_visitados:
                    self.logger.warning(
                        "evento=paginacion_repetida scraper=%s pagina=%s url=%s",
                        self.nombre,
                        pagina,
                        page_request.url,
                    )
                    break

                self.logger.info(
                    "evento=pagina_inicio scraper=%s pagina=%s url=%s",
                    self.nombre,
                    pagina,
                    page_request.url,
                )
                html = await self._request_text(session, page_request)
                requests_visitados.add(firma_request)

                ofertas_pagina = self._parsear_listado(html)
                if not ofertas_pagina:
                    self.logger.info(
                        "evento=pagina_vacia scraper=%s pagina=%s",
                        self.nombre,
                        pagina,
                    )
                    break

                firma_pagina = tuple(
                    sorted(item.get("id_externo") or item["url_oferta"] for item in ofertas_pagina)
                )
                if firma_pagina in firmas_paginas:
                    self.logger.warning(
                        "evento=fin_paginacion scraper=%s pagina=%s motivo=pagina_repetida",
                        self.nombre,
                        pagina,
                    )
                    break
                firmas_paginas.add(firma_pagina)

                for oferta in ofertas_pagina:
                    if oferta["url_oferta"] in urls_vistas:
                        continue
                    urls_vistas.add(oferta["url_oferta"])
                    ofertas.append(oferta)
                    if self.max_results and len(ofertas) >= self.max_results:
                        break

                siguiente = self._detectar_siguiente_pagina(html, page_request.url)
                if siguiente is None:
                    break
                page_request = siguiente
                pagina += 1

            self.logger.info(
                "evento=listado_completo scraper=%s paginas=%s ofertas=%s",
                self.nombre,
                pagina,
                len(ofertas),
            )

            ofertas = ofertas[: self.max_results] if self.max_results else ofertas
            enriched = await self._enriquecer_ofertas(session, ofertas)
            return enriched

    async def _enriquecer_ofertas(
        self,
        session: aiohttp.ClientSession,
        ofertas: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        async def worker(oferta: dict[str, Any]) -> dict[str, Any]:
            async with self._detail_semaphore:
                try:
                    return await self._enriquecer_con_detalle(session, oferta)
                except Exception as exc:  # pragma: no cover - defensa runtime
                    self.logger.exception(
                        "evento=detalle_error scraper=%s url=%s error=%s",
                        self.nombre,
                        oferta.get("url_oferta"),
                        exc,
                    )
                    self.stats["errores"] += 1
                    return oferta

        tasks = [asyncio.create_task(worker(oferta)) for oferta in ofertas]
        resultados: list[dict[str, Any]] = []
        for task in asyncio.as_completed(tasks):
            resultados.append(await task)
        return resultados

    async def _request_text(
        self,
        session: aiohttp.ClientSession,
        request: PageRequest,
    ) -> str:
        last_error: Exception | None = None
        # 4 intentos (1 + 3 retries) con backoff exponencial corto. Antes
        # eran 3 intentos lo que hacía que ofertas se perdieran cuando el
        # Servicio Civil tarda en responder en horario peak.
        for attempt in range(1, DEFAULT_MAX_ATTEMPTS + 1):
            try:
                await self._rate_limiter.wait()
                headers = {
                    "User-Agent": next(self.user_agents),
                    "Cache-Control": "no-cache",
                }
                async with session.request(
                    request.method.upper(),
                    request.url,
                    data=request.data,
                    headers=headers,
                    allow_redirects=True,
                ) as response:
                    # 404/410 son terminales: no reintentamos.
                    if response.status in {404, 410}:
                        response.raise_for_status()
                    if response.status in {403, 429, 500, 502, 503, 504}:
                        response.raise_for_status()
                    return await response.text(encoding="utf-8", errors="ignore")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt >= DEFAULT_MAX_ATTEMPTS:
                    break
                backoff = min(8, 2 ** (attempt - 1))
                self.logger.warning(
                    "evento=request_retry scraper=%s url=%s intento=%s espera=%s error=%s",
                    self.nombre,
                    request.url,
                    attempt,
                    backoff,
                    exc,
                )
                await asyncio.sleep(backoff)
        assert last_error is not None
        raise last_error

    def _parsear_listado(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        ofertas: list[dict[str, Any]] = []
        urls_vistas: set[str] = set()

        for tarjeta in soup.select(LIST_CARD_SELECTOR):
            try:
                oferta = self._parsear_tarjeta(tarjeta)
            except Exception as exc:  # pragma: no cover - defensa runtime
                self.logger.warning(
                    "evento=tarjeta_error scraper=%s error=%s",
                    self.nombre,
                    exc,
                )
                continue
            if not oferta:
                continue
            if oferta["url_oferta"] in urls_vistas:
                continue
            urls_vistas.add(oferta["url_oferta"])
            ofertas.append(oferta)
        return ofertas

    def _parsear_tarjeta(self, tarjeta: Tag) -> dict[str, Any] | None:
        titulo_el = tarjeta.select_one("div#bx_titulos")
        cargo = clean_text(titulo_el.get_text(" ", strip=True) if titulo_el else "")

        # Si el div principal tiene un badge ("Solo Difusión") en vez del cargo real,
        # buscamos el título en otros elementos de la tarjeta.
        if self._es_cargo_basura(cargo):
            cargo = self._extraer_cargo_alternativo(tarjeta)
            if self._es_cargo_basura(cargo):
                return None

        link = self._seleccionar_link_detalle(tarjeta)
        if not link:
            return None

        detail_url = urljoin(LIST_URL, link.get("href", ""))
        resumen = tarjeta.select_one("div#bx_resumen")
        institucion = None
        descripcion = None
        plazos_texto = None

        if resumen:
            institucion_el = resumen.find("strong")
            plazos_el = resumen.find("em")
            institucion = self._limpiar_jerarquia_institucion(
                clean_text(institucion_el.get_text(" ", strip=True) if institucion_el else "")
            )
            plazos_texto = clean_text(plazos_el.get_text(" ", strip=True) if plazos_el else "")
            descripcion = self._extraer_resumen_sin_campos(resumen)

        fecha_publicacion, fecha_cierre = self._extraer_rango_fechas(
            plazos_texto or clean_text(tarjeta.get_text(" ", strip=True))
        )

        return {
            "id_externo": self._extraer_id_externo(detail_url),
            "cargo": truncate(cargo, 500),
            "institucion_nombre": truncate(institucion, 300),
            "descripcion": descripcion or None,
            "requisitos": None,
            "tipo_contrato": None,
            "region": None,
            "ciudad": None,
            "renta_bruta_min": None,
            "renta_bruta_max": None,
            "grado_eus": None,
            "jornada": None,
            "area_profesional": self._inferir_area_profesional(cargo),
            "fecha_publicacion": fecha_publicacion,
            "fecha_cierre": fecha_cierre,
            "url_oferta": detail_url,
            "url_bases": None,
            "estado": "activo",
        }

    async def _enriquecer_con_detalle(
        self,
        session: aiohttp.ClientSession,
        oferta: dict[str, Any],
    ) -> dict[str, Any]:
        html = await self._request_text(
            session,
            PageRequest(method="GET", url=oferta["url_oferta"]),
        )
        soup = BeautifulSoup(html, "html.parser")

        iframe = soup.select_one('iframe#ifFicha[src], iframe[src*="avisopizarronficha.aspx"][src]')
        if iframe and iframe.get("src"):
            iframe_url = urljoin(oferta["url_oferta"], iframe["src"])
            html = await self._request_text(session, PageRequest(method="GET", url=iframe_url))
            soup = BeautifulSoup(html, "html.parser")

        return self._parsear_detalle(soup, oferta)

    def _parsear_detalle(self, soup: BeautifulSoup, oferta: dict[str, Any]) -> dict[str, Any]:
        resultado = dict(oferta)
        metadata = self._extraer_metadata_detalle(soup)

        resultado["cargo"] = truncate(
            metadata.get("cargo") or resultado.get("cargo"),
            500,
        )
        resultado["institucion_nombre"] = truncate(
            metadata.get("institucion_nombre") or resultado.get("institucion_nombre"),
            300,
        )
        resultado["tipo_contrato"] = (
            metadata.get("tipo_contrato")
            or self._extraer_tipo_desde_texto(metadata.get("condiciones"))
            or resultado.get("tipo_contrato")
        )
        resultado["region"] = metadata.get("region") or resultado.get("region")
        resultado["ciudad"] = metadata.get("ciudad") or resultado.get("ciudad")
        resultado["jornada"] = metadata.get("jornada") or self._extraer_jornada(
            metadata.get("condiciones")
        )
        resultado["area_profesional"] = (
            metadata.get("area_profesional")
            or resultado.get("area_profesional")
            or self._inferir_area_profesional(resultado.get("cargo"))
        )

        descripcion = self._componer_descripcion(soup)
        if descripcion:
            resultado["descripcion"] = descripcion

        requisitos = self._componer_requisitos(soup)
        if requisitos:
            resultado["requisitos"] = requisitos

        renta_texto = self._extraer_renta_texto(soup, metadata)
        renta_min, renta_max, grado_eus = parse_renta(renta_texto)
        if renta_min is not None:
            resultado["renta_bruta_min"] = renta_min
            resultado["renta_bruta_max"] = renta_max
        if grado_eus:
            resultado["grado_eus"] = grado_eus

        fecha_publicacion, fecha_cierre = self._extraer_fechas_desde_calendario(soup)
        if fecha_publicacion:
            resultado["fecha_publicacion"] = fecha_publicacion
        if fecha_cierre:
            resultado["fecha_cierre"] = fecha_cierre

        resultado["url_bases"] = self._extraer_url_bases(soup, resultado["url_oferta"])
        resultado["institucion_id"] = resultado.get("institucion_id") or self.match_institucion_id(
            resultado.get("institucion_nombre")
        )
        return resultado

    def _detectar_siguiente_pagina(self, html: str, current_url: str) -> PageRequest | None:
        soup = BeautifulSoup(html, "html.parser")

        rel_next = soup.select_one('a[rel="next"][href]')
        if rel_next:
            return PageRequest(method="GET", url=urljoin(current_url, rel_next["href"]))

        _NEXT_TEXTS = {"siguiente", "proxima", "next", "sig", "adelante"}

        for anchor in soup.select(".pagination a[href], .pager a[href], .navPagina a[href], a[href]"):
            text = clean_text(anchor.get_text(" ", strip=True)).lower().rstrip(".")
            href = anchor.get("href", "")
            onclick = anchor.get("onclick", "")

            # Texto exacto o parcial de "siguiente"
            is_next_text = text in _NEXT_TEXTS or text in {">", ">>"} or any(
                t in text for t in _NEXT_TEXTS
            )

            if is_next_text and href and "__doPostBack" not in href:
                return PageRequest(method="GET", url=urljoin(current_url, href))

            payload = onclick or href
            if "__doPostBack" in payload and (is_next_text or text.isdigit()):
                request = self._construir_postback_request(soup, current_url, payload)
                if request:
                    return request
        return None

    def _construir_postback_request(
        self,
        soup: BeautifulSoup,
        current_url: str,
        payload: str,
    ) -> PageRequest | None:
        match = re.search(r"__doPostBack\('([^']*)','([^']*)'\)", payload)
        if not match:
            return None
        event_target, event_argument = match.groups()
        form = soup.find("form")
        if not form:
            return None

        data: dict[str, str] = {}
        for input_el in form.select("input[name]"):
            input_type = (input_el.get("type") or "").lower()
            if input_type not in {"hidden", ""}:
                continue
            data[input_el["name"]] = input_el.get("value", "")

        data["__EVENTTARGET"] = event_target
        data["__EVENTARGUMENT"] = event_argument
        action = form.get("action") or current_url
        return PageRequest(method="POST", url=urljoin(current_url, action), data=data)

    def _seleccionar_link_detalle(self, tarjeta: Tag) -> Tag | None:
        candidatos = tarjeta.select(DETAIL_LINK_SELECTOR)
        if candidatos:
            for link in candidatos:
                texto = clean_text(link.get_text(" ", strip=True)).lower()
                href = link.get("href", "")
                if "convpostularavisoTrabajo" in href or texto == "ver bases":
                    return link
            return candidatos[0]
        # Fallback: cualquier link al dominio dentro de la tarjeta
        for anchor in tarjeta.select("a[href]"):
            href = str(anchor.get("href", ""))
            if "empleospublicos.cl" in href or (href.startswith("/") and len(href) > 1):
                return anchor
        return None

    def _extraer_cargo_alternativo(self, tarjeta: Tag) -> str:
        """Intenta extraer el cargo real cuando div#bx_titulos contiene un badge en vez del título."""
        for selector in ("h2", "h3", "h4", ".card-title", "b"):
            for el in tarjeta.select(selector):
                text = clean_text(el.get_text(" ", strip=True))
                if text and not self._es_cargo_basura(text):
                    return text
        return ""

    def _extraer_resumen_sin_campos(self, resumen: Tag) -> str:
        cloned = BeautifulSoup(str(resumen), "html.parser")
        node = cloned.find()
        if not node:
            return ""
        for selector in ("strong", "em"):
            for child in node.select(selector):
                child.decompose()
        return clean_text(node.get_text(" ", strip=True))

    @staticmethod
    def _limpiar_jerarquia_institucion(texto: str | None) -> str | None:
        """Cuando el campo institución muestra una jerarquía separada por '/'
        (ej: 'Ministerio de Salud / Servicio de Salud X /'), devuelve el segmento
        más a la derecha, que es la entidad empleadora real."""
        if not texto:
            return None
        partes = [p.strip() for p in texto.split("/") if p.strip()]
        return partes[-1] if partes else None

    def _extraer_metadata_detalle(self, soup: BeautifulSoup) -> dict[str, Any]:
        meta_container = soup.select_one("#lblAvisoTrabajoDatos")
        meta = self._extraer_mapa_encabezados(meta_container)
        condiciones = self._texto_seccion_sin_heading(soup.select_one("#lblCondiciones"))
        return {
            "cargo": clean_text(meta.get("convocatoria") or meta.get("cargo")) or None,
            "institucion_nombre": self._limpiar_jerarquia_institucion(
                clean_text(meta.get("institucion") or meta.get("institucion / entidad"))
            ) or None,
            "area_profesional": clean_text(meta.get("area de trabajo")) or None,
            "tipo_contrato": normalize_tipo_contrato(meta.get("tipo de vacante")),
            "region": normalize_region(meta.get("region")),
            "ciudad": clean_text(meta.get("ciudad")) or None,
            "jornada": clean_text(meta.get("jornada")) or None,
            "condiciones": condiciones or None,
        }

    def _componer_descripcion(self, soup: BeautifulSoup) -> str | None:
        funciones = self._extraer_mapa_encabezados(soup.select_one("#lblFunciones"))
        bloques = [
            ("Objetivo del cargo", funciones.get("objetivo del cargo")),
            ("Funciones del cargo", funciones.get("funciones del cargo")),
        ]
        return self._unir_bloques(bloques)

    def _componer_requisitos(self, soup: BeautifulSoup) -> str | None:
        # Competencias: solo los nombres, sin las definiciones completas
        competencias = self._extraer_nombres_competencias(soup.select_one("#lblCompetencias"))
        bloques = [
            ("Formacion educacional", self._truncar_seccion(
                self._texto_seccion_sin_heading(soup.select_one("#lblFormacion")), 600
            )),
            ("Especializacion y capacitacion", self._truncar_seccion(
                self._texto_seccion_sin_heading(soup.select_one("#lblEspecializaciones")), 400
            )),
            ("Experiencia", self._truncar_seccion(
                self._texto_seccion_sin_heading(soup.select_one("#lblExperiencias")), 500
            )),
            ("Competencias requeridas", competencias),
            # Excluidos intencionalmente:
            # - "Requisitos generales": boilerplate legal idéntico en todos los cargos
            # - "Criterios": tabla del proceso evaluativo (etapas, puntajes, comisión)
        ]
        return self._unir_bloques(bloques)

    def _extraer_nombres_competencias(self, node: Tag | None) -> str | None:
        """Extrae solo los nombres de las competencias, sin sus definiciones."""
        if not node:
            return None
        nombres: list[str] = []
        for tag in node.find_all(HEADING_TAGS):
            name = clean_text(tag.get_text(" ", strip=True))
            # Los nombres de competencias son cortos; ignorar headings de sección
            if name and len(name) < 80 and normalize_key(name) not in {"competencias", "definicion"}:
                nombres.append(name)
        if nombres:
            return ", ".join(nombres)
        # Fallback: texto truncado si no hay headings
        return self._truncar_seccion(self._texto_seccion_sin_heading(node), 300)

    def _truncar_seccion(self, text: str | None, max_chars: int) -> str | None:
        """Trunca texto al límite indicado, cortando en punto o espacio."""
        if not text:
            return None
        text = clean_text(text)
        if len(text) <= max_chars:
            return text
        # Intentar cortar en el último punto dentro del límite
        candidato = text[:max_chars]
        ultimo_punto = candidato.rfind(".")
        if ultimo_punto > max_chars * 0.5:
            return text[: ultimo_punto + 1]
        return candidato.rstrip() + "…"

    def _extraer_fechas_desde_calendario(
        self,
        soup: BeautifulSoup,
    ) -> tuple[Any | None, Any | None]:
        for table in soup.select("table"):
            table_text = normalize_key(table.get_text(" ", strip=True))
            if "postulacion" not in table_text and "difusion" not in table_text:
                continue
            for row in table.select("tr"):
                cells = row.find_all(("td", "th"))
                if len(cells) < 2:
                    continue
                label = normalize_key(cells[0].get_text(" ", strip=True))
                value = clean_text(cells[1].get_text(" ", strip=True))
                if "postulacion" in label or "difusion" in label:
                    return self._extraer_rango_fechas(value)
        return None, None

    def _extraer_renta_texto(self, soup: BeautifulSoup, metadata: dict[str, Any]) -> str:
        partes = [
            self._texto_seccion_sin_heading(soup.select_one("#lblRenta")),
            metadata.get("condiciones"),
            self._texto_seccion_sin_heading(soup.select_one("#lblTexto")),
        ]
        return " | ".join(part for part in partes if part)

    def _extraer_url_bases(self, soup: BeautifulSoup, fallback_url: str) -> str | None:
        for anchor in soup.select("a[href]"):
            href = anchor.get("href", "")
            text = clean_text(anchor.get_text(" ", strip=True)).lower()
            if ".pdf" in href.lower() or "bases" in text:
                return urljoin(fallback_url, href)
        return fallback_url

    def _extraer_mapa_encabezados(self, container: Tag | None) -> dict[str, str]:
        if not container:
            return {}
        resultado: dict[str, str] = {}
        for heading in container.find_all(HEADING_TAGS):
            clave = normalize_key(heading.get_text(" ", strip=True))
            if not clave:
                continue
            valor = self._extraer_texto_despues_de_heading(heading)
            if valor:
                resultado[clave] = valor
        return resultado

    def _extraer_texto_despues_de_heading(self, heading: Tag) -> str:
        partes: list[str] = []
        for sibling in heading.next_siblings:
            if isinstance(sibling, Tag):
                if sibling.name in HEADING_TAGS:
                    break
                nested_heading = sibling.find(HEADING_TAGS)
                if nested_heading:
                    break
            if isinstance(sibling, NavigableString):
                text = clean_text(str(sibling))
            else:
                text = clean_text(sibling.get_text(" ", strip=True))
            if text:
                partes.append(text)
        return clean_text(" ".join(partes))

    def _texto_seccion_sin_heading(self, node: Tag | None) -> str:
        if not node:
            return ""
        mapped = self._extraer_mapa_encabezados(node)
        if mapped:
            return clean_text(" ".join(value for value in mapped.values() if value))
        return clean_text(node.get_text(" ", strip=True))

    def _extraer_rango_fechas(self, text: str | None) -> tuple[Any | None, Any | None]:
        content = clean_text(text)
        if not content:
            return None, None

        match = re.search(
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})\s*(?:-|–|—|a)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{4})",
            content,
        )
        if match:
            return parse_date(match.group(1)), parse_date(match.group(2))

        fechas = re.findall(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}", content)
        if len(fechas) >= 2:
            return parse_date(fechas[0]), parse_date(fechas[-1])
        if len(fechas) == 1:
            unica = parse_date(fechas[0])
            return unica, unica
        return None, None

    def _unir_bloques(self, bloques: Iterable[tuple[str, str | None]]) -> str | None:
        partes: list[str] = []
        for etiqueta, valor in bloques:
            texto = clean_text(valor)
            if texto:
                partes.append(f"{etiqueta}: {texto}")
        if not partes:
            return None
        return "\n\n".join(partes)

    def _es_cargo_basura(self, cargo: str | None) -> bool:
        value = clean_text(cargo).lower()
        return not value or value in BAD_CARGO_TEXTS

    def _extraer_tipo_desde_texto(self, text: str | None) -> str | None:
        key = normalize_key(text)
        if not key:
            return None
        if "reemplazo" in key or "suplencia" in key:
            return "reemplazo"
        return normalize_tipo_contrato(key)

    def _extraer_jornada(self, text: str | None) -> str | None:
        content = clean_text(text)
        if not content:
            return None
        match = re.search(r"\b(\d{1,2})\s*horas\b", content, re.IGNORECASE)
        if match:
            return f"{match.group(1)} horas"
        if "jornada completa" in normalize_key(content):
            return "jornada completa"
        return None

    def _inferir_area_profesional(self, cargo: str | None) -> str | None:
        key = normalize_key(cargo)
        if not key:
            return None
        area_map = {
            "salud": ["medic", "enfermer", "matron", "kinesio", "salud", "odontolog"],
            "derecho": ["abogad", "jurid", "legal", "fiscal"],
            "ingenieria": ["ingenier", "constructor", "arquitect", "informatic", "desarrollador"],
            "educacion": ["docente", "profesor", "educador", "pedagog"],
            "administracion": ["administr", "analista", "gestion", "rrhh", "recursos humanos"],
            "finanzas": ["contador", "auditor", "finanza", "tesorer"],
            "ciencias sociales": ["social", "psicolog", "sociolog", "terapeuta"],
        }
        for area, keywords in area_map.items():
            if any(keyword in key for keyword in keywords):
                return area
        return "administracion"

    def _extraer_id_externo(self, url: str) -> str | None:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        if query.get("i"):
            return query["i"][0]
        return None


def load_instituciones(path: str | Path | None = None) -> list[dict[str, Any]]:
    if path is None:
        path = Path(__file__).resolve().parents[1] / "repositorio_instituciones_publicas_chile.json"
    path = Path(path)
    if not path.exists():
        return []
    data = json_load(path)
    instituciones = data.get("instituciones") if isinstance(data, dict) else data
    return instituciones or []


def json_load(path: Path) -> Any:
    import json

    return json.loads(path.read_text(encoding="utf-8-sig"))


def ejecutar(
    dry_run: bool = False,
    max_results: int | None = None,
    instituciones: list[dict[str, Any]] | None = None,
    strict_institution_match: bool = False,
) -> dict[str, Any]:
    scraper = EmpleosPublicosScraper(
        instituciones=instituciones or load_instituciones(),
        dry_run=dry_run,
        max_results=max_results,
        strict_institution_match=strict_institution_match,
    )
    return scraper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper de empleospublicos.cl")
    parser.add_argument("--dry-run", action="store_true", help="No guarda en PostgreSQL")
    parser.add_argument("--max", type=int, default=None, help="Limite de ofertas a procesar")
    parser.add_argument(
        "--json",
        dest="json_path",
        default=None,
        help="Ruta alternativa del repositorio de instituciones",
    )
    args = parser.parse_args()

    started = time.time()
    result = ejecutar(
        dry_run=args.dry_run,
        max_results=args.max,
        instituciones=load_instituciones(args.json_path),
    )
    print(result)
    print(f"Duracion total: {round(time.time() - started, 2)}s")
