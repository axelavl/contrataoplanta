"""
EmpleoEstado.cl — Scraper: Municipalidad de Puente Alto
URL: https://www.mpuentealto.cl/trabaje-con-nosotros/

Estructura verificada (Abril 2026):
- WordPress con página estática
- Contenido en div.entry-content (o .jupiterx-post-content)
- Concursos listados como:
    * Títulos en <strong> o <h3>
    * Links a bases en PDF con texto "Aquí" o nombre del cargo
    * Sección "CONCURSOS EN PROCESO" (activos) y "CONCURSOS CERRADOS"
- Sin paginación: toda la info en una sola página

Uso:
    python scrapers/muni_puente_alto.py
    python scrapers/muni_puente_alto.py --dry-run
    python scrapers/muni_puente_alto.py --verbose
"""

import sys
import re
import time
import logging
import argparse
import random
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import config
from db.database import (
    SessionLocal, upsert_oferta, marcar_ofertas_cerradas,
    registrar_log, normalizar_area, generar_id_estable
)

# ── Configuración ─────────────────────────────────────────────
LOG_DIR = Path(config.LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

FUENTE_ID       = 20          # ID en tabla fuentes para Muni Puente Alto
BASE_URL        = "https://www.mpuentealto.cl"
URL_PRINCIPAL   = f"{BASE_URL}/trabaje-con-nosotros/"
INSTITUCION     = "Municipalidad de Puente Alto"
REGION          = "Metropolitana de Santiago"
SECTOR          = "Municipal"

# Logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "muni_puente_alto.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("scraper.muni_puente_alto")

POSITIVE_HARD_SIGNALS = (
    "concurso público",
    "concurso publico",
    "perfil del cargo",
    "requisitos del cargo",
    "funciones del cargo",
    "recepción de antecedentes",
    "recepcion de antecedentes",
    "bases del concurso",
    "postulaciones hasta",
    "honorarios",
    "contrata",
    "planta",
    "suplencia",
    "reemplazo",
)
POSITIVE_SOFT_SIGNALS = (
    "cargo",
    "vacante",
    "puesto",
    "llamado a postulación",
    "llamado a postulacion",
    "proceso de selección",
    "proceso de seleccion",
    "postular",
)
NEGATIVE_SIGNALS = (
    "noticia",
    "comunicado",
    "actividad",
    "ceremonia",
    "taller",
    "capacitación",
    "capacitacion",
    "licitación",
    "licitacion",
    "proveedor",
    "subvención",
    "subvencion",
    "cuenta pública",
    "participación ciudadana",
    "agenda",
    "operativo",
)


# ── Sesión HTTP ───────────────────────────────────────────────
def crear_sesion() -> requests.Session:
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
        "Referer": BASE_URL,
    })
    return s


# ── Parser principal ──────────────────────────────────────────
def parsear_pagina(html: str) -> list[dict]:
    """
    Extrae todos los concursos activos de la página de Puente Alto.

    Estructura WordPress observada:
    ─────────────────────────────────
    <div class="entry-content"> (o .jupiterx-post-content)
      <strong>CONCURSOS EN PROCESO</strong>
        <p>Texto del concurso... <a href="...bases.pdf">Aquí</a></p>
        <p>Otro concurso... <a href="...bases.pdf">Aquí</a></p>
      <strong>CONCURSOS CERRADOS</strong>
        ... (ignorar)
    ─────────────────────────────────
    """
    soup = BeautifulSoup(html, "html.parser")

    # Encontrar el contenedor principal del contenido
    contenedor = (
        soup.find("div", class_="entry-content")
        or soup.find("div", class_="jupiterx-post-content")
        or soup.find("div", class_=re.compile(r"post.content|entry.content|page.content", re.I))
        or soup.find("main")
    )

    if not contenedor:
        logger.warning("No se encontró contenedor de contenido. Verificar selectores CSS.")
        return []

    ofertas = []
    en_seccion_activa = False
    seccion_actual = None

    # Iterar todos los elementos del contenedor
    for elemento in contenedor.descendants:
        if elemento.name is None:
            continue

        texto_elem = limpiar(elemento.get_text())

        # ── Detectar cambio de sección ──
        if elemento.name in ["strong", "h2", "h3", "h4", "b"]:
            texto_upper = texto_elem.upper()

            if any(kw in texto_upper for kw in [
                "EN PROCESO", "VIGENTE", "ACTIVO", "LLAMADO A CONCURSO",
                "CONCURSO MUNICIPAL", "MUNICIPALIDAD DE PUENTE ALTO"
            ]):
                en_seccion_activa = True
                seccion_actual = texto_elem
                logger.debug(f"  Sección activa detectada: '{texto_elem[:60]}'")
                continue

            if any(kw in texto_upper for kw in [
                "CERRADO", "FINALIZADO", "TERMINADO", "ADJUDICADO"
            ]):
                en_seccion_activa = False
                seccion_actual = "CERRADOS"
                logger.debug("  Sección cerrados detectada — ignorando resto")
                continue

        # ── Solo procesar si estamos en sección activa ──
        if not en_seccion_activa:
            continue

        # ── Buscar párrafos con información de concurso ──
        if elemento.name == "p":
            oferta = extraer_oferta_de_parrafo(elemento)
            if oferta:
                ofertas.append(oferta)

        # ── Buscar listas de concursos ──
        elif elemento.name in ["ul", "ol"]:
            for li in elemento.find_all("li", recursive=False):
                oferta = extraer_oferta_de_parrafo(li)
                if oferta:
                    ofertas.append(oferta)

    # Si no encontramos nada con la lógica de secciones,
    # intentar extracción directa de todos los links a PDF/bases
    if not ofertas:
        logger.warning("  Extracción por secciones fallida. Usando fallback por links.")
        ofertas = extraer_por_links_fallback(contenedor)

    # Deduplicar por URL
    vistos = set()
    resultado = []
    for o in ofertas:
        key = o["url_original"]
        if key not in vistos:
            vistos.add(key)
            resultado.append(o)

    logger.info(f"  Total ofertas extraídas: {len(resultado)}")
    return resultado


def extraer_oferta_de_parrafo(elemento) -> dict | None:
    """
    Extrae una oferta desde un elemento <p> o <li>.
    Busca: texto del cargo + link a bases.
    """
    texto_completo = limpiar(elemento.get_text())

    # Filtrar párrafos que no son concursos
    if len(texto_completo) < 8:
        return None
    if not es_publicacion_laboral(texto_completo):
        return None

    # Buscar link a bases o convocatoria
    links = elemento.find_all("a", href=True)
    url_bases = None
    texto_link = None

    for a in links:
        href = a.get("href", "")
        texto_a = limpiar(a.get_text())

        # Priorizar links a PDF de bases
        if href.endswith(".pdf") or "bases" in href.lower() or "aqui" in href.lower():
            url_bases = href if href.startswith("http") else urljoin(BASE_URL, href)
            texto_link = texto_a
            break

        # Cualquier link que no sea navegación interna
        if href.startswith("http") and "mpuentealto.cl" not in href:
            url_bases = href
            texto_link = texto_a
            break

        if href.startswith("http") and "mpuentealto.cl" in href and "/wp-content/" in href:
            url_bases = href
            texto_link = texto_a
            break

    # Si no hay link, usar la URL principal como fallback
    if not url_bases:
        url_bases = URL_PRINCIPAL

    # Extraer nombre del cargo
    cargo = extraer_cargo(texto_completo)
    if not cargo:
        return None

    # Extraer tipo de vínculo
    tipo_cargo = inferir_tipo(texto_completo)

    # Extraer fecha de cierre si aparece
    fecha_cierre = extraer_fecha(texto_completo)

    # Extraer renta si aparece
    renta_min, renta_max = extraer_renta(texto_completo)
    url_original, id_externo = normalizar_url_oferta(url_bases, cargo, texto_completo)

    return {
        "id_externo":        id_externo,
        "fuente_id":         FUENTE_ID,
        "url_original":      url_original,
        "cargo":             cargo[:500],
        "descripcion":       texto_completo[:2000] if len(texto_completo) > 30 else None,
        "institucion_nombre": INSTITUCION,
        "sector":            SECTOR,
        "area_profesional":  normalizar_area(cargo),
        "tipo_cargo":        tipo_cargo,
        "nivel":             inferir_nivel(cargo),
        "region":            REGION,
        "ciudad":            "Puente Alto",
        "renta_bruta_min":   renta_min,
        "renta_bruta_max":   renta_max,
        "renta_texto":       None,
        "fecha_publicacion": date.today(),
        "fecha_cierre":      fecha_cierre,
        "requisitos_texto":  None,
    }


def extraer_por_links_fallback(contenedor) -> list[dict]:
    """
    Fallback: extrae todos los links que parezcan concursos o bases,
    ignorando links de navegación.
    """
    ofertas = []
    links_vistos = set()

    for a in contenedor.find_all("a", href=True):
        href = a.get("href", "")
        texto = limpiar(a.get_text())

        # Filtrar links de navegación y vacíos
        if not href or href in links_vistos:
            continue
        if len(texto) < 5:
            continue
        if any(nav in href for nav in ["#", "instagram", "twitter", "facebook",
                                        "youtube", "tiktok", "transparencia",
                                        "mercadopublico", "leylobby"]):
            continue
        if any(nav in texto.lower() for nav in ["inicio", "home", "contacto",
                                                  "alcald", "municip", "estructura"]):
            continue

        if not es_publicacion_laboral(f"{href} {texto}"):
            continue

        url_completa = href if href.startswith("http") else urljoin(BASE_URL, href)
        links_vistos.add(href)

        cargo = extraer_cargo(texto) or texto[:200]

        # Buscar contexto del link (párrafo padre)
        padre = a.find_parent(["p", "li", "div"])
        texto_contexto = limpiar(padre.get_text()) if padre else texto
        if not es_publicacion_laboral(texto_contexto):
            continue

        url_original, id_externo = normalizar_url_oferta(url_completa, cargo, texto_contexto)

        ofertas.append({
            "id_externo":        id_externo,
            "fuente_id":         FUENTE_ID,
            "url_original":      url_original,
            "cargo":             cargo[:500],
            "descripcion":       texto_contexto[:2000],
            "institucion_nombre": INSTITUCION,
            "sector":            SECTOR,
            "area_profesional":  normalizar_area(cargo),
            "tipo_cargo":        inferir_tipo(texto_contexto),
            "nivel":             inferir_nivel(cargo),
            "region":            REGION,
            "ciudad":            "Puente Alto",
            "renta_bruta_min":   None,
            "renta_bruta_max":   None,
            "renta_texto":       None,
            "fecha_publicacion": date.today(),
            "fecha_cierre":      extraer_fecha(texto_contexto),
            "requisitos_texto":  None,
        })

    return ofertas


# ── Utilidades de extracción ──────────────────────────────────
def limpiar(texto: str) -> str:
    """Normaliza espacios y caracteres especiales."""
    if not texto:
        return ""
    # Reemplazar secuencias de espacios/tabs/newlines
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def es_publicacion_laboral(texto: str) -> bool:
    contenido = limpiar(texto).lower()
    if len(contenido) < 8:
        return False
    if any(neg in contenido for neg in NEGATIVE_SIGNALS):
        return False
    hard_hits = sum(1 for kw in POSITIVE_HARD_SIGNALS if kw in contenido)
    soft_hits = sum(1 for kw in POSITIVE_SOFT_SIGNALS if kw in contenido)
    if hard_hits >= 1:
        return True
    return soft_hits >= 2


def crear_identificador_estable(cargo: str, texto: str) -> str:
    """Genera un ID estable cuando no hay URL única por oferta."""
    return generar_id_estable(FUENTE_ID, INSTITUCION, cargo, texto)


def normalizar_url_oferta(url: str | None, cargo: str, texto: str) -> tuple[str, str]:
    """Crea una URL estable para el upsert cuando solo existe la página madre."""
    id_externo = crear_identificador_estable(cargo, texto)
    url_final = url or URL_PRINCIPAL
    if not url_final or url_final == URL_PRINCIPAL:
        url_final = f"{URL_PRINCIPAL}#oferta-{id_externo}"
    return url_final, id_externo


def extraer_cargo(texto: str) -> str | None:
    """
    Intenta extraer el nombre limpio del cargo desde el texto.
    Estrategias:
    1. Buscar patrón "cargo: XXXX"
    2. Buscar texto antes del primer link o fecha
    3. Usar el texto completo si es corto
    """
    if not texto:
        return None

    # Patrón: "Cargo:" o "cargo:"
    m = re.search(r"[Cc]argo[:\s]+([^\n\.\(]{5,100})", texto)
    if m:
        return limpiar(m.group(1))

    # Patrón: texto antes de "Aquí", "Bases", fecha o paréntesis
    limpiado = re.split(r"\bAqu[íi]\b|\bBases\b|\bPostul|\(\d", texto)[0]
    limpiado = limpiar(limpiado)

    # Remover frases de introducción comunes
    for prefijo in [
        "Llamado a concurso",
        "Concurso público",
        "Concurso Público",
        "Se llama a concurso",
        "Llamado a Concurso",
        "Concurso para",
        "Proceso de selección",
    ]:
        if limpiado.startswith(prefijo):
            limpiado = limpiado[len(prefijo):].strip(" :—-")

    # Aceptar si tiene longitud razonable
    if 5 <= len(limpiado) <= 300:
        return limpiado

    # Último recurso: truncar a 200 chars
    if len(texto) > 5:
        return texto[:200].strip()

    return None


def inferir_tipo(texto: str) -> str:
    """Determina el tipo de vínculo laboral."""
    t = texto.lower()
    if "planta" in t:     return "Planta"
    if "contrata" in t:   return "Contrata"
    if "honorario" in t:  return "Honorarios"
    if "reemplazo" in t:  return "Reemplazo"
    if "código del trabajo" in t or "codigo del trabajo" in t: return "Código del Trabajo"
    return "Contrata"  # Default más común en municipios


def inferir_nivel(cargo: str) -> str:
    """Infiere el nivel jerárquico desde el nombre del cargo."""
    c = cargo.lower()
    if any(w in c for w in ["director", "jefe", "encargado", "coordinador",
                              "subdirector", "gerente", "superintendente"]):
        return "Directivo"
    if any(w in c for w in ["médico", "abogado", "ingeniero", "psicólogo",
                              "asistente social", "trabajador social",
                              "arquitecto", "contador", "periodista",
                              "educador", "profesor", "nutricionista",
                              "matrón", "enfermero", "kinesiólogo"]):
        return "Profesional"
    if any(w in c for w in ["técnico", "paramédico", "tens"]):
        return "Técnico"
    if any(w in c for w in ["administrativo", "secretaria", "digitador",
                              "asistente administrativo"]):
        return "Administrativo"
    if any(w in c for w in ["auxiliar", "conductor", "chofer", "portero",
                              "jardinero", "guardia"]):
        return "Auxiliar"
    return "Profesional"  # Default


def extraer_fecha(texto: str) -> date | None:
    """Extrae fechas de cierre del texto."""
    # Patrones: dd/mm/yyyy, dd-mm-yyyy, dd de mes de yyyy
    MESES = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
    }

    # Formato numérico
    m = re.findall(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b", texto)
    if m:
        try:
            d, mo, y = m[-1]
            return date(int(y), int(mo), int(d))
        except ValueError:
            pass

    # Formato texto: "15 de marzo de 2026"
    m = re.search(
        r"\b(\d{1,2})\s+de\s+(" + "|".join(MESES.keys()) + r")\s+(?:de\s+)?(\d{4})\b",
        texto.lower()
    )
    if m:
        try:
            return date(int(m.group(3)), MESES[m.group(2)], int(m.group(1)))
        except ValueError:
            pass

    return None


def extraer_renta(texto: str) -> tuple[int | None, int | None]:
    """Extrae renta desde texto libre."""
    montos = list(re.finditer(r"\$?\s*(\d{1,3}(?:[.,]\d{3})+)", texto))
    limpios = []
    for match in montos:
        try:
            m = match.group(1)
            n = int(m.replace(".", "").replace(",", ""))
            contexto = texto[max(0, match.start() - 60):match.end() + 60].lower()
            if any(neg in contexto for neg in ["presupuesto", "monto total", "convenio", "proyecto", "anual", "global"]):
                continue
            if 300_000 <= n < 15_000_000:
                limpios.append(n)
        except ValueError:
            continue

    if not limpios: return None, None
    if len(limpios) == 1: return limpios[0], limpios[0]
    return min(limpios), max(limpios)


# ── Ejecutor principal ────────────────────────────────────────
def ejecutar(dry_run: bool = False, verbose: bool = False):
    """
    Ejecuta el scraper de la Municipalidad de Puente Alto.

    Args:
        dry_run: Si True, imprime las ofertas sin guardar en BD.
        verbose: Si True, muestra detalle de cada oferta extraída.
    """
    inicio = time.time()
    logger.info("=" * 60)
    logger.info(f"INICIO - Scraper {INSTITUCION}")
    logger.info(f"  URL: {URL_PRINCIPAL}")
    logger.info(f"  dry_run={dry_run} | verbose={verbose}")
    logger.info("=" * 60)

    sesion = crear_sesion()
    db = SessionLocal()
    stats = {"nuevas": 0, "actualizadas": 0, "cerradas": 0, "errores": 0}
    urls_activas = []

    try:
        # ── Descargar página ──
        logger.info("  Descargando página principal...")
        resp = sesion.get(URL_PRINCIPAL, timeout=config.TIMEOUT_REQUEST)
        resp.raise_for_status()
        resp.encoding = "utf-8"

        # ── Parsear ──
        ofertas = parsear_pagina(resp.text)

        if not ofertas:
            logger.warning("  ⚠️  No se encontraron ofertas. Verificar HTML del sitio.")
            return stats

        # ── Procesar cada oferta ──
        for i, datos in enumerate(ofertas, 1):
            urls_activas.append(datos["url_original"])

            if verbose or dry_run:
                print(f"\n  [{i}] {'─'*50}")
                print(f"       Cargo:     {datos['cargo'][:70]}")
                print(f"       Tipo:      {datos['tipo_cargo']}")
                print(f"       Área:      {datos['area_profesional']}")
                print(f"       Nivel:     {datos['nivel']}")
                print(f"       Cierre:    {datos['fecha_cierre']}")
                print(f"       Renta:     {datos['renta_bruta_min']}")
                print(f"       URL:       {datos['url_original'][:80]}")

            if not dry_run:
                try:
                    nueva, actualizada = upsert_oferta(db, datos)
                    if nueva:
                        stats["nuevas"] += 1
                        logger.debug(f"    [NEW] {datos['cargo'][:60]}")
                    elif actualizada:
                        stats["actualizadas"] += 1
                except Exception as e:
                    db.rollback()
                    stats["errores"] += 1
                    logger.exception(
                        "    Error procesando oferta %s: %s",
                        datos.get("id_externo") or datos["url_original"],
                        e,
                    )
                    continue

        # ── Marcar cerradas ──
        if not dry_run and urls_activas:
            cerradas = marcar_ofertas_cerradas(db, FUENTE_ID, sorted(urls_activas))
            stats["cerradas"] = cerradas
            if cerradas > 0:
                logger.info(f"  -> {cerradas} ofertas marcadas como cerradas")

    except requests.exceptions.Timeout:
        if not dry_run:
            db.rollback()
        logger.error(f"  Timeout al conectar con {URL_PRINCIPAL}")
        stats["errores"] += 1
        return stats

    except requests.exceptions.RequestException as e:
        if not dry_run:
            db.rollback()
        logger.error(f"  Error HTTP: {e}")
        stats["errores"] += 1
        return stats

    except Exception as e:
        if not dry_run:
            db.rollback()
        logger.exception(f"  Error inesperado: {e}")
        stats["errores"] += 1
        raise

    finally:
        duracion = time.time() - inicio

        logger.info("-" * 60)
        logger.info("RESUMEN")
        logger.info(f"  Ofertas encontradas:  {len(ofertas) if 'ofertas' in dir() else 0}")
        logger.info(f"  Nuevas en BD:         {stats['nuevas']}")
        logger.info(f"  Actualizadas en BD:   {stats['actualizadas']}")
        logger.info(f"  Cerradas en BD:       {stats['cerradas']}")
        logger.info(f"  Errores:              {stats['errores']}")
        logger.info(f"  Duración:             {duracion:.1f} seg")
        logger.info("=" * 60)

        if not dry_run:
            try:
                db.rollback()
                registrar_log(
                    db, FUENTE_ID, "OK" if stats["errores"] == 0 else "PARCIAL",
                    ofertas_nuevas=stats["nuevas"],
                    ofertas_actualizadas=stats["actualizadas"],
                    ofertas_cerradas=stats["cerradas"],
                    paginas=1,
                    duracion=duracion
                )
            except Exception:
                logger.exception("  No se pudo registrar el log final")
        db.close()

    return stats


# ── CLI ───────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    os.makedirs(config.LOG_DIR, exist_ok=True)

    parser = argparse.ArgumentParser(
        description=f"Scraper de {INSTITUCION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python scrapers/muni_puente_alto.py               # Ejecución normal
  python scrapers/muni_puente_alto.py --dry-run     # Sin guardar en BD
  python scrapers/muni_puente_alto.py --dry-run -v  # Sin BD + detalle
        """
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="No escribe en la base de datos, solo muestra lo que extraería"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Mostrar detalle de cada oferta extraída"
    )
    args = parser.parse_args()

    ejecutar(dry_run=args.dry_run, verbose=args.verbose)
