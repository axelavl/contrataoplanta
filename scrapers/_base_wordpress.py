"""
EmpleoEstado.cl — Base WordPress para municipios
Clase reutilizable: hereda y sobreescribe solo lo necesario.

Municipios que usan esta base:
    - Puente Alto   → muni_puente_alto.py
    - San Bernardo  → muni_san_bernardo.py
    - La Florida    → muni_la_florida.py
    - Temuco        → muni_temuco.py
    - (agregar más según se vaya escalando)

Uso heredado:
    class MuniSanBernardo(MuniWordPressBase):
        FUENTE_ID   = 21
        BASE_URL    = "https://www.sanbernardo.cl"
        URL_EMPLEO  = "https://www.sanbernardo.cl/concursos-publicos/"
        INSTITUCION = "Municipalidad de San Bernardo"
        CIUDAD      = "San Bernardo"
"""

import re
import time
import logging
import random
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import config
from db.database import (
    SessionLocal, upsert_oferta, marcar_ofertas_cerradas,
    registrar_log, normalizar_area, generar_id_estable, limpiar_texto
)

LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)


class MuniWordPressBase:
    """
    Scraper base para municipios con sitio WordPress.
    Subclasear y definir los atributos de clase.
    """

    # ── Atributos a definir en cada subclase ──────────────────
    FUENTE_ID:   int = None
    BASE_URL:    str = None
    URL_EMPLEO:  str = None       # URL principal de concursos
    URLS_EXTRA:  list = []        # URLs adicionales (bolsa, ofertas, etc.)
    INSTITUCION: str = None
    CIUDAD:      str = None
    REGION:      str = "Metropolitana de Santiago"
    SECTOR:      str = "Municipal"

    # Palabras clave que indican sección ACTIVA
    KW_ACTIVOS = [
        "en proceso", "vigente", "activo", "llamado a concurso",
        "concurso municipal", "concursos públicos", "concursos publicos",
        "postulacion", "postulación", "bases"
    ]
    # Palabras clave que indican sección CERRADA (parar de parsear)
    KW_CERRADOS = [
        "cerrado", "finalizado", "terminado", "adjudicado",
        "concursos anteriores", "histórico"
    ]

    def __init__(self):
        self.logger = self._configurar_logger()
        self.sesion = self._crear_sesion()

    def _configurar_logger(self) -> logging.Logger:
        """Configura logging consistente por scraper municipal."""
        logger = logging.getLogger(f"scraper.{self.__class__.__name__.lower()}")
        logger.setLevel(getattr(logging, config.LOG_LEVEL))

        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
            )
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            file_handler = logging.FileHandler(
                LOG_DIR / f"{self.__class__.__name__.lower()}.log",
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            logger.addHandler(file_handler)

        logger.propagate = False
        return logger

    # ── HTTP ──────────────────────────────────────────────────
    def _crear_sesion(self) -> requests.Session:
        s = requests.Session()
        s.trust_env = False
        retries = Retry(
            total=config.MAX_REINTENTOS,
            connect=config.MAX_REINTENTOS,
            read=config.MAX_REINTENTOS,
            status=config.MAX_REINTENTOS,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-CL,es;q=0.9",
            "Referer": self.BASE_URL,
        })
        return s

    def _get(self, url: str) -> str | None:
        """GET con reintentos y delay."""
        for intento in range(max(config.MAX_REINTENTOS, 1)):
            try:
                resp = self.sesion.get(url, timeout=config.TIMEOUT_REQUEST)
                resp.raise_for_status()
                resp.encoding = "utf-8"
                return resp.text
            except requests.exceptions.Timeout:
                self.logger.warning(f"  Timeout intento {intento+1}: {url}")
                time.sleep(2 ** intento)
            except requests.exceptions.RequestException as e:
                self.logger.error(f"  Error HTTP: {e}")
                return None
        return None

    # ── Parsing ───────────────────────────────────────────────
    def obtener_contenedor(self, soup: BeautifulSoup):
        """Localiza el div de contenido principal de WordPress."""
        selectores = [
            ("div", {"class": "entry-content"}),
            ("div", {"class": "jupiterx-post-content"}),
            ("div", {"class": re.compile(r"post.?content|entry.?content|page.?content", re.I)}),
            ("article", {}),
            ("main", {}),
        ]
        for tag, attrs in selectores:
            el = soup.find(tag, attrs)
            if el:
                return el
        return soup.body

    def parsear(self, html: str, url_fuente: str) -> list[dict]:
        """
        Parsea el HTML y devuelve lista de ofertas.
        Subclases pueden sobrescribir para lógica específica.
        """
        soup = BeautifulSoup(html, "html.parser")
        contenedor = self.obtener_contenedor(soup)
        if not contenedor:
            self.logger.warning("  Contenedor no encontrado")
            return []

        ofertas = []
        en_activo = True  # Asumir activo hasta ver sección cerrados

        for el in contenedor.descendants:
            if el.name is None:
                continue

            texto = self._limpiar(el.get_text())
            texto_l = texto.lower()

            # Detectar cambio de estado de sección
            if el.name in ["strong", "b", "h2", "h3", "h4"]:
                if any(kw in texto_l for kw in self.KW_CERRADOS):
                    en_activo = False
                    continue
                if any(kw in texto_l for kw in self.KW_ACTIVOS):
                    en_activo = True
                    continue

            if not en_activo:
                continue

            # Extraer desde párrafos y listas
            if el.name == "p":
                o = self._extraer_oferta(el, url_fuente)
                if o:
                    ofertas.append(o)
            elif el.name in ["ul", "ol"]:
                for li in el.find_all("li", recursive=False):
                    o = self._extraer_oferta(li, url_fuente)
                    if o:
                        ofertas.append(o)

        # Fallback si no se encontró nada estructurado
        if not ofertas:
            self.logger.warning("  Usando fallback por links")
            ofertas = self._fallback_links(contenedor, url_fuente)

        return self._deduplicar(ofertas)

    def _extraer_oferta(self, el, url_fuente: str) -> dict | None:
        """Extrae una oferta de un elemento <p> o <li>."""
        texto = self._limpiar(el.get_text())
        if not self._es_concurso(texto):
            return None

        cargo = self._extraer_cargo(texto)
        if not cargo:
            return None

        url = self._extraer_url(el)
        return self._construir_oferta(cargo, texto, url, url_fuente=url_fuente)

    def _es_concurso(self, texto: str) -> bool:
        """Determina si un texto describe una oferta laboral."""
        if len(texto) < 8:
            return False
        t = texto.lower()
        neg = [
            "noticia", "comunicado", "actividad", "ceremonia", "taller",
            "capacitación", "capacitacion", "licitación", "licitacion",
            "proveedor", "subvención", "subvencion", "cuenta pública",
            "participación ciudadana", "agenda", "operativo",
        ]
        if any(k in t for k in neg):
            return False

        hard_positive = [
            "concurso público", "concurso publico", "perfil del cargo",
            "requisitos del cargo", "funciones del cargo", "recepción de antecedentes",
            "recepcion de antecedentes", "bases del concurso", "postulaciones hasta",
            "honorarios", "contrata", "planta", "suplencia", "reemplazo",
        ]
        soft_positive = [
            "cargo", "vacante", "puesto", "llamado a postulación", "llamado a postulacion",
            "proceso de selección", "proceso de seleccion", "postular",
        ]
        hard_hits = sum(1 for k in hard_positive if k in t)
        soft_hits = sum(1 for k in soft_positive if k in t)
        if hard_hits >= 1:
            return True
        return soft_hits >= 2

    def _extraer_url(self, el) -> str | None:
        """Extrae la URL de bases/convocatoria de un elemento."""
        for a in el.find_all("a", href=True):
            href = a["href"]
            if not href or href.startswith("#"):
                continue
            # Priorizar PDFs y links con keywords
            if any(kw in href.lower() for kw in [".pdf", "bases", "wp-content"]):
                return href if href.startswith("http") else urljoin(self.BASE_URL, href)
            # Links externos o internos a documentos
            if href.startswith("http") and "mpuentealto" not in href:
                texto_a = self._limpiar(a.get_text()).lower()
                if any(kw in texto_a for kw in ["aquí", "aqui", "aqui", "ver", "descarg"]):
                    return href
        return None

    def _extraer_cargo(self, texto: str) -> str | None:
        """Extrae el nombre del cargo del texto."""
        # Patrón: "Cargo: XXX"
        m = re.search(r"[Cc]argo[:\s]+([^\n\.\(]{5,150})", texto)
        if m:
            return self._limpiar(m.group(1))

        # Texto antes de "Aquí", "Bases" o fecha
        limpiado = re.split(r"\bAqu[íi]\b|\bBases\b|\bPostul|\(\d", texto)[0]
        limpiado = self._limpiar(limpiado)

        # Remover prefijos comunes
        prefijos = [
            "Llamado a concurso", "Concurso público", "Concurso Público",
            "Se llama a concurso", "Proceso de selección", "Concurso para",
        ]
        for pfx in prefijos:
            if limpiado.lower().startswith(pfx.lower()):
                limpiado = self._limpiar(limpiado[len(pfx):].lstrip(" :—-"))

        if 5 <= len(limpiado) <= 300:
            return limpiado
        if len(texto) > 5:
            return texto[:200].strip()
        return None

    def _crear_identificador_estable(self, cargo: str, texto: str) -> str:
        """Genera un ID estable cuando el sitio no expone un detalle único."""
        return generar_id_estable(self.FUENTE_ID, self.INSTITUCION, cargo, texto)

    def _normalizar_url_oferta(self, url: str | None, url_fuente: str, cargo: str, texto: str) -> tuple[str, str]:
        """Genera una URL estable y no duplicada para el upsert."""
        id_externo = self._crear_identificador_estable(cargo, texto)
        url_final = url or url_fuente
        if not url_final or url_final == url_fuente:
            url_final = f"{url_fuente}#oferta-{id_externo}"
        return url_final, id_externo

    def _construir_oferta(self, cargo: str, texto: str, url: str | None, url_fuente: str) -> dict:
        """Construye el dict de oferta con todos los campos normalizados."""
        url_final, id_externo = self._normalizar_url_oferta(url, url_fuente, cargo, texto)
        return {
            "id_externo":        id_externo,
            "fuente_id":         self.FUENTE_ID,
            "url_original":      url_final,
            "cargo":             cargo[:500],
            "descripcion":       texto[:2000] if len(texto) > 30 else None,
            "institucion_nombre": self.INSTITUCION,
            "sector":            self.SECTOR,
            "area_profesional":  normalizar_area(cargo),
            "tipo_cargo":        self._inferir_tipo(texto),
            "nivel":             self._inferir_nivel(cargo),
            "region":            self.REGION,
            "ciudad":            self.CIUDAD,
            "renta_bruta_min":   self._extraer_renta(texto)[0],
            "renta_bruta_max":   self._extraer_renta(texto)[1],
            "renta_texto":       None,
            "fecha_publicacion": date.today(),
            "fecha_cierre":      self._extraer_fecha(texto),
            "requisitos_texto":  None,
        }

    def _fallback_links(self, contenedor, url_fuente: str) -> list[dict]:
        """Extrae concursos a partir de todos los links relevantes."""
        IGNORAR = {"#", "instagram", "twitter", "facebook", "youtube",
                   "tiktok", "transparencia", "mercadopublico", "leylobby",
                   "javascript:", "mailto:"}
        ofertas = []
        vistos = set()
        for a in contenedor.find_all("a", href=True):
            href = a["href"]
            if any(ig in href for ig in IGNORAR) or href in vistos:
                continue
            texto = self._limpiar(a.get_text())
            if len(texto) < 5:
                continue
            padre = a.find_parent(["p", "li"])
            ctx = self._limpiar(padre.get_text()) if padre else texto
            if not self._es_concurso(ctx):
                continue
            url = href if href.startswith("http") else urljoin(self.BASE_URL, href)
            vistos.add(href)
            cargo = self._extraer_cargo(ctx) or texto[:200]
            ofertas.append(self._construir_oferta(cargo, ctx, url, url_fuente))
        return ofertas

    # ── Helpers ───────────────────────────────────────────────
    @staticmethod
    def _limpiar(texto: str) -> str:
        return limpiar_texto(re.sub(r"\s+", " ", texto or " "))

    @staticmethod
    def _inferir_tipo(texto: str) -> str:
        t = texto.lower()
        if "planta" in t:     return "Planta"
        if "contrata" in t:   return "Contrata"
        if "honorario" in t:  return "Honorarios"
        if "reemplazo" in t:  return "Reemplazo"
        return "Contrata"

    @staticmethod
    def _inferir_nivel(cargo: str) -> str:
        c = cargo.lower()
        if any(w in c for w in ["director", "jefe", "encargado",
                                  "coordinador", "subdirector"]):
            return "Directivo"
        if any(w in c for w in ["médico", "abogado", "ingeniero",
                                  "psicólogo", "asistente social",
                                  "trabajador social", "arquitecto",
                                  "contador", "periodista", "educador",
                                  "matrón", "enfermero", "kinesiólogo"]):
            return "Profesional"
        if any(w in c for w in ["técnico", "paramédico", "tens"]):
            return "Técnico"
        if any(w in c for w in ["administrativo", "secretaria", "digitador"]):
            return "Administrativo"
        if any(w in c for w in ["auxiliar", "conductor", "chofer",
                                  "guardia", "jardinero"]):
            return "Auxiliar"
        return "Profesional"

    @staticmethod
    def _extraer_fecha(texto: str) -> date | None:
        MESES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
                  "julio":7,"agosto":8,"septiembre":9,"octubre":10,
                  "noviembre":11,"diciembre":12}
        m = re.findall(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b", texto)
        if m:
            try:
                d, mo, y = m[-1]
                return date(int(y), int(mo), int(d))
            except ValueError:
                pass
        m = re.search(
            r"\b(\d{1,2})\s+de\s+(" + "|".join(MESES) + r")\s+(?:de\s+)?(\d{4})\b",
            texto.lower()
        )
        if m:
            try:
                return date(int(m.group(3)), MESES[m.group(2)], int(m.group(1)))
            except ValueError:
                pass
        return None

    @staticmethod
    def _extraer_renta(texto: str) -> tuple:
        montos = list(re.finditer(r"\$?\s*(\d{1,3}(?:[.,]\d{3})+)", texto))
        limpios = []
        for match in montos:
            mo = match.group(1)
            try:
                n = int(mo.replace(".", "").replace(",", ""))
                ctx = texto[max(0, match.start() - 60):match.end() + 60].lower()
                if any(neg in ctx for neg in ["presupuesto", "monto total", "convenio", "proyecto", "anual", "global"]):
                    continue
                if 300_000 <= n < 15_000_000:
                    limpios.append(n)
            except ValueError:
                continue
        if not limpios: return None, None
        if len(limpios) == 1: return limpios[0], limpios[0]
        return min(limpios), max(limpios)

    @staticmethod
    def _deduplicar(ofertas: list) -> list:
        vistos, resultado = set(), []
        for o in ofertas:
            k = o["url_original"] + o["cargo"][:30]
            if k not in vistos:
                vistos.add(k)
                resultado.append(o)
        return resultado

    # ── Ejecutor ──────────────────────────────────────────────
    def ejecutar(self, dry_run: bool = False, verbose: bool = False) -> dict:
        inicio = time.time()
        self.logger.info("=" * 60)
        self.logger.info(f"INICIO - {self.INSTITUCION}")
        self.logger.info("=" * 60)

        db = SessionLocal()
        stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
        urls_activas = []
        todas_las_ofertas = []

        # Procesar URL principal + extras
        urls_a_procesar = [self.URL_EMPLEO] + self.URLS_EXTRA

        try:
            for url in urls_a_procesar:
                self.logger.info(f"  Procesando: {url}")
                html = self._get(url)
                if not html:
                    stats["errores"] += 1
                    continue

                ofertas = self.parsear(html, url)
                self.logger.info(f"  → {len(ofertas)} ofertas en {url}")
                todas_las_ofertas.extend(ofertas)

                if len(urls_a_procesar) > 1:
                    time.sleep(config.DELAY_ENTRE_REQUESTS)

            # Deduplicar entre todas las URLs
            todas_las_ofertas = self._deduplicar(todas_las_ofertas)
            self.logger.info(f"  Total (deduplicado): {len(todas_las_ofertas)}")

            for datos in todas_las_ofertas:
                urls_activas.append(datos["url_original"])

                if verbose or dry_run:
                    print(f"\n  {'─'*52}")
                    print(f"  Cargo:  {datos['cargo'][:65]}")
                    print(f"  Tipo:   {datos['tipo_cargo']} | Nivel: {datos['nivel']}")
                    print(f"  Área:   {datos['area_profesional']}")
                    print(f"  Cierre: {datos['fecha_cierre']} | Renta: {datos['renta_bruta_min']}")
                    print(f"  URL:    {datos['url_original'][:75]}")

                if not dry_run:
                    try:
                        nueva, actualizada = upsert_oferta(db, datos)
                        if nueva:
                            stats["nuevas"] += 1
                        elif actualizada:
                            stats["actualizadas"] += 1
                    except Exception as e:
                        stats["errores"] += 1
                        db.rollback()
                        self.logger.exception(
                            "  Error procesando oferta %s: %s",
                            datos.get("id_externo") or datos["url_original"],
                            e,
                        )
                        continue

            if not dry_run and urls_activas:
                stats["cerradas"] = marcar_ofertas_cerradas(
                    db, self.FUENTE_ID, sorted(urls_activas)
                )

        except Exception as e:
            if not dry_run:
                db.rollback()
            self.logger.exception(f"  Error: {e}")
            stats["errores"] += 1
            raise

        finally:
            dur = time.time() - inicio
            self.logger.info(f"  Nuevas: {stats['nuevas']} | "
                             f"Act: {stats['actualizadas']} | "
                             f"Cerradas: {stats['cerradas']} | "
                             f"{dur:.1f}s")
            if not dry_run:
                try:
                    db.rollback()
                    registrar_log(db, self.FUENTE_ID,
                                 "OK" if stats["errores"] == 0 else "PARCIAL",
                                 ofertas_nuevas=stats["nuevas"],
                                 ofertas_actualizadas=stats["actualizadas"],
                                 ofertas_cerradas=stats["cerradas"],
                                 paginas=len(urls_a_procesar),
                                 duracion=dur)
                except Exception:
                    self.logger.exception("  No se pudo registrar el log final")
            db.close()

        return stats
