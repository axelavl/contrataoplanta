"""Perfil desde un CV y afinidad con las ofertas vigentes.

Qué hace
--------
1. Lee un CV (PDF o DOCX) EN MEMORIA y extrae un perfil: área(s), títulos,
   años de experiencia, cargos previos, capacitaciones, región y palabras.
2. Ordena ofertas por afinidad con ese perfil y explica SIEMPRE por qué calza
   cada una (y qué le falta).

Decisiones de diseño
--------------------
* Sin almacenar: todo trabaja sobre bytes en memoria; el CV no se escribe en
  la base ni se registra en logs. Procesarlo sí es tratamiento de datos
  personales (ley 21.719): el endpoint exige consentimiento explícito.
* Sin porcentaje falso: se entrega una banda (alta/media/baja) y la lista de
  coincidencias y reparos, no un "87 %" que sugiere una precisión inexistente.
* Solo reglas: predecible, depurable y sin costo por consulta (no llama a un
  LLM). Si más adelante se agregan embeddings, se suman como otra señal en
  `afinidad` sin rehacer el resto.
"""
from __future__ import annotations

import io
import re
import unicodedata
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable
from xml.etree import ElementTree

MAX_BYTES_CV = 5 * 1024 * 1024        # un CV real pesa mucho menos
MAX_PAGINAS_PDF = 10
MAX_CHARS_CV = 60_000
# Tope del XML descomprimido de un DOCX (defensa ante "zip bombs").
MAX_BYTES_DOCX_XML = 20 * 1024 * 1024


class CVInvalido(ValueError):
    """El archivo no se pudo leer como CV (formato, tamaño o contenido)."""


# ── Taxonomía de áreas (raíces normalizadas: sin tildes ni ñ) ────────────────
_AREAS_RAW: dict[str, tuple[str, ...]] = {
    "Salud": ("enfermer", "medic", "kinesiolog", "matron", "odontolog",
              "nutricion", "tecnologo medico", "farmac", "psicolog", "tens",
              "paramedic", "obstetr", "fonoaudiolog", "terapeuta ocupacional",
              "salud publica", "epidemiolog"),
    # "educacion" a secas NO dispara el área: aparece en la fórmula legal
    # "institución de educación reconocida" de casi cualquier oferta.
    "Educación": ("profesor", "docente", "pedagog", "academic", "educador",
                  "parvular", "curriculum", "didactic", "escolar"),
    "Ingeniería y Construcción": ("ingenier", "constructor", "arquitect",
                                  "obras", "topograf", "electric", "mecanic",
                                  "industrial", "prevencion de riesgos"),
    "Informática": ("informatic", "computacion", "software", "desarrollador",
                    "programador", "sistemas", "analista de datos",
                    "ciberseguridad", "redes", "soporte ti", "telematic"),
    "Administración y Finanzas": ("administracion", "contador", "contabilidad",
                                  "finanzas", "auditor", "presupuest",
                                  "adquisicion", "abastecimiento", "tesorer",
                                  "recursos humanos", "gestion de personas"),
    "Jurídica": ("abogad", "derecho", "juridic", "fiscal", "legal", "notarial"),
    "Social y Comunitaria": ("trabajador social", "trabajo social",
                             "asistente social", "sociolog", "comunitari",
                             "desarrollo social"),
    "Comunicaciones": ("periodis", "comunicacion", "publicidad", "disenador",
                       "diseno grafico", "audiovisual", "marketing"),
}

_NIVEL_DIRECTIVO = ("director", "jefe de", "jefa de", "jefatura", "gerente",
                    "coordinador", "coordinadora", "encargado", "encargada",
                    "decano")
_NIVEL_PROFESIONAL = ("profesional", "ingenier", "analista", "abogad",
                      "psicolog", "enfermer", "medic", "docente", "academic",
                      "licenciad", "titulad")
_NIVEL_TECNICO = ("tecnico", "asistente", "auxiliar", "administrativo",
                  "secretari", "operador", "tens")

_REGIONES = (
    "Arica y Parinacota", "Tarapacá", "Antofagasta", "Atacama", "Coquimbo",
    "Valparaíso", "Metropolitana", "O'Higgins", "Maule", "Ñuble",
    "Biobío", "La Araucanía", "Los Ríos", "Los Lagos", "Aysén", "Magallanes",
)
_CIUDAD_REGION = {
    "santiago": "Metropolitana", "providencia": "Metropolitana",
    "nunoa": "Metropolitana", "las condes": "Metropolitana",
    "maipu": "Metropolitana", "puente alto": "Metropolitana",
    "la florida": "Metropolitana",
    "valparaiso": "Valparaíso", "vina del mar": "Valparaíso",
    "quilpue": "Valparaíso", "san antonio": "Valparaíso",
    "concepcion": "Biobío", "talcahuano": "Biobío", "los angeles": "Biobío",
    "temuco": "La Araucanía", "valdivia": "Los Ríos", "osorno": "Los Lagos",
    "puerto montt": "Los Lagos", "coyhaique": "Aysén",
    "punta arenas": "Magallanes", "iquique": "Tarapacá",
    "arica": "Arica y Parinacota", "antofagasta": "Antofagasta",
    "calama": "Antofagasta", "copiapo": "Atacama", "la serena": "Coquimbo",
    "coquimbo": "Coquimbo", "rancagua": "O'Higgins", "talca": "Maule",
    "curico": "Maule", "chillan": "Ñuble",
}

_RE_ANIOS = re.compile(
    r"(\d{1,2})\s*\+?\s*(?:a[nñ]os?|years?)\s+de\s+"
    r"(?:experiencia|ejercicio|trayectoria)", re.I)
_RE_RANGO_ANIOS = re.compile(
    r"\b((?:19|20)\d{2})\s*[-–—a]\s*(?:(?:19|20)\d{2}|actualidad|presente|hoy)\b",
    re.I)
_RE_TITULO = re.compile(
    r"\b(t[ií]tulo\s+(?:profesional\s+)?(?:de\s+)?|licenciad[oa]\s+en\s+|"
    r"ingenier[oa]\s+(?:civil\s+)?(?:en\s+)?|profesor[a]?\s+de\s+|"
    r"m[aá]ster\s+en\s+|mag[ií]ster\s+en\s+|doctor[a]?\s+en\s+|"
    r"t[eé]cnico\s+(?:de\s+nivel\s+superior\s+)?(?:en\s+)?)"
    r"([A-ZÁÉÍÓÚÑ][^\n.,;]{3,60})", re.I)
_RE_CARGO_PREVIO = re.compile(
    r"(?:19|20)\d{2}\s*[-–—aA]\s*(?:(?:19|20)\d{2}|actualidad|presente|hoy)"
    r"\s*[:.\-–]\s*([^,\n;|]{4,60})", re.I)
_RE_CAPACITACION = re.compile(
    r"((?:diplomad[oa]|curso|capacitaci[oó]n|certificaci[oó]n|certificad[oa]|"
    r"post[ií]tulo|especializaci[oó]n)s?\s+"
    r"(?:universitarios?\s+)?(?:de\s+formaci[oó]n\s+)?(?:en|de|sobre)\s+"
    r"[^\n.;,|]{4,70})", re.I)
_RE_ANIOS_EXIGIDOS = re.compile(
    r"(\d{1,2})\s*a[nñ]os?(?:\s+de)?\s+(?:experiencia|ejercicio)", re.I)
_RE_TOKEN = re.compile(r"[a-z]{4,}")

_PALABRAS_RUIDO = {
    "para", "como", "esta", "este", "desde", "hasta", "entre", "sobre", "otros",
    "curriculum", "vitae", "chile", "email", "correo", "telefono", "celular",
    "direccion", "nombre", "fecha", "experiencia", "laboral", "laborales",
    "educacion", "estudios", "referencias", "actualidad", "presente",
    "profesional", "profesionales", "titulo", "grado", "institucion",
    "superior", "reconocida", "reconocido", "vigente", "legislacion",
    "validado", "cargo", "cargos", "universidad", "universitaria", "cumplir",
    "deseable", "conocimiento", "conocimientos", "manejo", "nivel", "anos",
    "region", "comuna", "servicio", "publico", "publica", "chilena",
    "chileno", "gobierno", "trabajo", "funciones", "area", "unidad",
    "departamento", "contrata", "planta", "honorarios", "jornada", "completa",
    "regional", "nacional", "comunal", "municipal", "programa", "apoyo",
    "encargado", "encargada", "asistente", "profesionales",
}


_RE_DIACRITICOS = re.compile(r"[\u0300-\u036f]")
_RE_ESPACIOS = re.compile(r"\s+")


def _norm(s: str | None) -> str:
    """Minúsculas, sin tildes ni ñ (ñ → n), espacios colapsados.

    Con regex en vez de recorrer carácter a carácter: se llama sobre los
    requisitos de miles de ofertas por request y era el 80 % del tiempo."""
    s = _RE_DIACRITICOS.sub("", unicodedata.normalize("NFD", (s or "").lower()))
    return _RE_ESPACIOS.sub(" ", s).strip()


_AREAS = {area: tuple(_norm(c) for c in claves)
          for area, claves in _AREAS_RAW.items()}
# Vocabulario de profesiones: solo estas raíces cuentan como "profesión
# exigida", para no confundir adjetivos del cargo con la profesión del CV.
_VOCAB_PROFESION = tuple(sorted(
    {c for claves in _AREAS.values() for c in claves if len(c) >= 6},
    key=len, reverse=True))


def _tokens(texto_norm: str) -> set[str]:
    return {w for w in _RE_TOKEN.findall(texto_norm) if w not in _PALABRAS_RUIDO}


# ── Lectura del CV (en memoria) ──────────────────────────────────────────────
def _leer_pdf(datos: bytes) -> str:
    import pdfplumber
    partes: list[str] = []
    try:
        with pdfplumber.open(io.BytesIO(datos)) as pdf:
            for pagina in pdf.pages[:MAX_PAGINAS_PDF]:
                partes.append(pagina.extract_text() or "")
    except Exception as exc:
        raise CVInvalido("No pudimos leer el PDF.") from exc
    return "\n".join(partes)


def _leer_docx(datos: bytes) -> str:
    """Texto de word/document.xml sin dependencias extra (python-docx)."""
    try:
        with zipfile.ZipFile(io.BytesIO(datos)) as z:
            info = z.getinfo("word/document.xml")
            if info.file_size > MAX_BYTES_DOCX_XML:
                raise CVInvalido("El documento es demasiado grande.")
            xml = z.read(info)
    except CVInvalido:
        raise
    except Exception as exc:
        raise CVInvalido("No pudimos leer el DOCX.") from exc
    try:
        raiz = ElementTree.fromstring(xml)
    except ElementTree.ParseError as exc:
        raise CVInvalido("No pudimos leer el DOCX.") from exc
    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    parrafos = []
    for p in raiz.iter(f"{ns}p"):
        parrafos.append("".join(t.text or "" for t in p.iter(f"{ns}t")))
    return "\n".join(parrafos)


def leer_cv(datos: bytes) -> str:
    """Extrae el texto de un CV PDF o DOCX según su firma (no su extensión)."""
    if not datos:
        raise CVInvalido("El archivo está vacío.")
    if len(datos) > MAX_BYTES_CV:
        raise CVInvalido("El archivo supera los 5 MB.")
    if datos[:5] == b"%PDF-":
        texto = _leer_pdf(datos)
    elif datos[:4] == b"PK\x03\x04":
        texto = _leer_docx(datos)
    else:
        raise CVInvalido("Formato no soportado: sube un PDF o un DOCX.")
    texto = texto[:MAX_CHARS_CV]
    if len(texto.strip()) < 80:
        raise CVInvalido(
            "No encontramos texto en el archivo. Si es un PDF escaneado "
            "(una imagen), expórtalo desde Word o Google Docs y vuelve a subirlo.")
    return texto


# ── Perfil ───────────────────────────────────────────────────────────────────
@dataclass
class Perfil:
    areas: list[str] = field(default_factory=list)
    titulos: list[str] = field(default_factory=list)
    anios_experiencia: int | None = None
    nivel: str | None = None
    region: str | None = None
    cargos_previos: list[str] = field(default_factory=list)
    capacitaciones: list[str] = field(default_factory=list)
    palabras: set[str] = field(default_factory=set)

    def a_dict(self) -> dict[str, Any]:
        """Lo que se devuelve al usuario (sin la bolsa de palabras)."""
        return {
            "areas": self.areas,
            "titulos": self.titulos,
            "anios_experiencia": self.anios_experiencia,
            "nivel": self.nivel,
            "region": self.region,
            "cargos_previos": self.cargos_previos[:4],
            "capacitaciones": self.capacitaciones[:6],
        }


def _detectar_areas(t: str) -> list[str]:
    puntajes = {a: sum(t.count(c) for c in claves) for a, claves in _AREAS.items()}
    return [a for a, n in sorted(puntajes.items(), key=lambda x: -x[1]) if n][:3]


def _detectar_nivel(t: str) -> str | None:
    for nivel, claves in (("Directivo", _NIVEL_DIRECTIVO),
                          ("Profesional", _NIVEL_PROFESIONAL),
                          ("Técnico", _NIVEL_TECNICO)):
        if any(c in t for c in claves):
            return nivel
    return None


def _detectar_region(t: str) -> str | None:
    for r in _REGIONES:
        if _norm(r) in t:
            return r
    for ciudad, region in _CIUDAD_REGION.items():
        if re.search(rf"\b{re.escape(ciudad)}\b", t):
            return region
    return None


def _detectar_anios(texto: str, hoy: date | None = None) -> int | None:
    if m := _RE_ANIOS.search(texto):
        v = int(m.group(1))
        if 0 < v <= 50:
            return v
    inicios = [int(m.group(1)) for m in _RE_RANGO_ANIOS.finditer(texto)]
    if inicios:
        span = (hoy or date.today()).year - min(inicios)
        if 0 < span <= 50:
            return span
    return None


def _unicos(valores: Iterable[str], minimo: int, tope: int) -> list[str]:
    salida: list[str] = []
    vistos: list[str] = []
    for v in valores:
        v = re.sub(r"\s+", " ", v).strip(" .,;:-–")
        clave = _norm(v)
        if len(v) < minimo or any(clave in x or x in clave for x in vistos):
            continue
        salida.append(v)
        vistos.append(clave)
        if len(salida) >= tope:
            break
    return salida


def perfil_desde_texto(texto: str, hoy: date | None = None) -> Perfil:
    t = _norm(texto)
    return Perfil(
        areas=_detectar_areas(t),
        titulos=_unicos((m.group(1) + m.group(2) for m in _RE_TITULO.finditer(texto)),
                        8, 4),
        anios_experiencia=_detectar_anios(texto, hoy),
        nivel=_detectar_nivel(t),
        region=_detectar_region(t),
        cargos_previos=_unicos((m.group(1) for m in _RE_CARGO_PREVIO.finditer(texto)),
                               5, 8),
        capacitaciones=_unicos((m.group(1) for m in _RE_CAPACITACION.finditer(texto)),
                               10, 10),
        palabras=_tokens(t),
    )


def perfil_desde_archivo(datos: bytes) -> Perfil:
    """Bytes del CV → Perfil. No persiste nada."""
    return perfil_desde_texto(leer_cv(datos))


# ── Afinidad ─────────────────────────────────────────────────────────────────
@dataclass
class Resultado:
    oferta: dict[str, Any]
    puntaje: int
    banda: str                       # "alta" | "media" | "baja"
    razones: list[str] = field(default_factory=list)
    reparos: list[str] = field(default_factory=list)
    #: Hubo al menos una señal de fondo (área, profesión, cargo previo o
    #: palabras del cargo). Nivel, región y experiencia solos no bastan.
    nucleo: bool = False


def _formacion_exigida(req: str) -> str:
    """Parte de los requisitos que habla de formación/título."""
    m = re.search(r"(?:formacion|titulo|profesion)[^|]{0,220}", req)
    return m.group(0) if m else req[:220]


def _raices_profesion(titulos: list[str]) -> list[str]:
    raices: list[str] = []
    for t in titulos:
        texto = _norm(t)
        for clave in _VOCAB_PROFESION:
            if clave in texto and clave not in raices:
                raices.append(clave)
    return raices[:6]


def afinidad(perfil: Perfil, oferta: dict[str, Any],
             raices: list[str] | None = None) -> Resultado:
    """Puntúa una oferta contra el perfil y explica el porqué."""
    razones: list[str] = []
    reparos: list[str] = []
    puntaje = 0
    nucleo = False

    cargo = _norm(oferta.get("cargo"))
    req = _norm(oferta.get("requisitos"))[:4000]
    tokens_cargo = _tokens(cargo)

    # Área: se infiere del cargo y de la formación exigida, no de la
    # descripción completa (el texto institucional contamina).
    areas_of: set[str] = set()
    if oferta.get("area_profesional"):
        areas_of.add(oferta["area_profesional"])
    base_area = f"{cargo} {_formacion_exigida(req)}"
    for a, claves in _AREAS.items():
        if any(c in base_area for c in claves):
            areas_of.add(a)
    comunes = [a for a in perfil.areas if a in areas_of]
    if comunes:
        nucleo = True
        puntaje += 40
        razones.append(f"Coincide tu área: {comunes[0]}")
    elif perfil.areas and areas_of:
        reparos.append("El área del cargo no coincide con la de tu CV")

    nivel_of = oferta.get("nivel")
    if perfil.nivel and nivel_of and _norm(perfil.nivel) == _norm(nivel_of):
        puntaje += 10
        razones.append(f"Nivel {nivel_of}, como tu trayectoria")

    region_of = oferta.get("region")
    if perfil.region and region_of:
        if _norm(perfil.region) in _norm(region_of):
            puntaje += 15
            razones.append(f"Está en tu región ({region_of})")
        else:
            reparos.append(f"Se ubica en {region_of}")

    solapa = tokens_cargo & perfil.palabras
    if solapa:
        nucleo = True
        puntaje += min(15, 5 * len(solapa))
        razones.append("Tu CV menciona: " + ", ".join(sorted(solapa)[:3]))

    # Profesión exigida en las bases que el CV declara como título.
    raices = _raices_profesion(perfil.titulos) if raices is None else raices
    if raices and req:
        hit = next((r for r in raices if r in req), None)
        if hit:
            nucleo = True
            puntaje += 30
            razones.append("Las bases piden tu profesión")

    for previo in perfil.cargos_previos:
        if tokens_cargo & _tokens(_norm(previo)):
            nucleo = True
            puntaje += 20
            razones.append(f"Ya te desempeñaste como {previo}")
            break

    if req:
        tokens_req = _tokens(req)
        if tokens_req and len(tokens_req & perfil.palabras) / len(tokens_req) >= 0.25:
            puntaje += 10
            razones.append("Tu perfil cubre parte de los requisitos publicados")
        if m := _RE_ANIOS_EXIGIDOS.search(req):
            exigidos = int(m.group(1))
            if 0 < exigidos <= 30:
                plural = "s" if exigidos > 1 else ""
                if perfil.anios_experiencia is None:
                    reparos.append(f"Piden {exigidos} año{plural} de experiencia "
                                   "(no pudimos deducirla de tu CV)")
                elif perfil.anios_experiencia < exigidos:
                    reparos.append(f"Piden {exigidos} año{plural} de experiencia y "
                                   f"detectamos ~{perfil.anios_experiencia}")
                else:
                    puntaje += 10
                    razones.append(f"Cumples la experiencia pedida ({exigidos} año{plural})")
    else:
        reparos.append("La oferta no publica requisitos en texto; revisa las bases")

    banda = "alta" if puntaje >= 60 else "media" if puntaje >= 35 else "baja"
    return Resultado(oferta=oferta, puntaje=puntaje, banda=banda,
                     razones=razones, reparos=reparos, nucleo=nucleo)


def rankear(perfil: Perfil, ofertas: Iterable[dict[str, Any]],
            top: int = 20, minimo: int = 25) -> list[Resultado]:
    """Ofertas ordenadas por afinidad; descarta las que no alcanzan `minimo`."""
    raices = _raices_profesion(perfil.titulos)
    res = [r for r in (afinidad(perfil, o, raices) for o in ofertas)
           if r.nucleo and r.puntaje >= minimo]
    res.sort(key=lambda r: (-r.puntaje, str(r.oferta.get("fecha_cierre") or "9999")))
    return res[:top]
