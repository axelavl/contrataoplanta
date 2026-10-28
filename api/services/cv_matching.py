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
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable
from xml.etree import ElementTree

from classification import funciones as FUN
from classification import profesiones as PROF

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
    r"\b(?:\d{1,2}/)?((?:19|20)\d{2})\s*[-–—a]\s*"
    r"(?:(?:\d{1,2}/)?(?:19|20)\d{2}|actualidad|presente|hoy|a la fecha)\b",
    re.I)
# Cargo con el rango de fechas al FINAL de la línea (formato de CV muy común):
# "Profesional de Desarrollo Organizacional 04/2022 - 10/2023".
_RE_CARGO_LINEA = re.compile(
    r"^\s*(?P<cargo>[^\n]{5,90}?)\s+(?:\d{1,2}/)?(?:19|20)\d{2}\s*[-–—]\s*"
    r"(?:(?:\d{1,2}/)?(?:19|20)\d{2}|presente|actualidad|hoy|a la fecha)\s*$",
    re.I | re.M)
_RE_SECCION_EDUCACION = re.compile(
    r"^\s*(?:educaci[oó]n|formaci[oó]n(?: acad[eé]mica)?|estudios|antecedentes acad[eé]micos)\s*:?\s*$",
    re.I | re.M)
# Encabezados de sección típicos de un CV (no cualquier línea en mayúsculas:
# "UNIVERSIDAD DE CHILE" no cierra la sección EDUCACIÓN).
_RE_SECCION = re.compile(
    r"^\s*(?:experiencia|especializaci|habilidades|competencias|sistemas|software|"
    r"herramientas|idiomas|cursos|capacitaci|certificaci|publicaciones|otras actividades|"
    r"proyectos|referencias|perfil|resumen|logros|voluntariado|intereses)[^\n]{0,40}$",
    re.I | re.M)
_RE_ANIO_FINAL = re.compile(r"\s*[(\[]?\b(?:19|20)\d{2}\b[)\]]?\s*$")
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
    r"((?:diplomad[oa]|diploma|curso|capacitaci[oó]n|certificaci[oó]n|certificad[oa]|"
    r"post[ií]tulo|especializaci[oó]n)s?\s+"
    r"(?:universitarios?\s+)?(?:de\s+formaci[oó]n\s+)?(?:en|de|sobre)\s+"
    r"[^\n.;,|]{4,70})", re.I)
# "3 años de experiencia", "1 a 5 años de experiencia" (→ 1),
# "experiencia mínima de 3 años", "desde 3 años de experiencia".
_RE_ANIOS_EXIGIDOS = (
    re.compile(r"(\d{1,2})\s*(?:a\s*\d{1,2}\s*)?anos?(?:\s+de)?\s+(?:experiencia|ejercicio)"),
    re.compile(r"experiencia\s+(?:\w+\s+){0,3}?(?:de\s+)?(\d{1,2})\s*anos?"),
)
_RE_ESPECIALIDAD = re.compile(
    r"(?:con\s+)?especialidad(?:es)?\s+(?:en\s+)?((?:[a-z]+\s?){1,4}?)(?=\s+o\s|\s+y\s|,|\(|\.|$)")
_RE_VENTANA_FORMACION = re.compile(r"titul|carrera|formacion|estudios|profesion|egresad")
_RE_VENTANA_CURSOS = re.compile(
    r"capacitaci|especializaci|cursos?\b|diplomad|postitulo|post-titulo|magister|deseable")
_RE_FIN_FRASE = re.compile(r"[a-z]{5,}\.\s")
_RE_UNIVERSIDAD = re.compile(
    r"(?:universidad|instituto profesional|centro de formacion tecnica|liceo|colegio|escuela)"
    r"(?:\s+\w+){0,4}")
_RE_TOKEN = re.compile(r"[a-z]{5,}")
_VACIAS = {"de", "del", "la", "las", "el", "los", "en", "y", "e", "o", "u", "a", "al",
           "para", "con", "por", "sobre", "uso", "su", "sus"}
#: Frases que aparecen en casi cualquier aviso del Estado: no prueban nada.
_BIGRAMAS_GENERICOS = {
    "politicas publicas", "entidades publicas", "sector publico", "servicios publicos",
    "administracion publica", "gestion publica", "instituciones publicas",
    "organismos publicos", "estado chile", "educacion superior",
}


def _bigramas(texto_norm: str) -> set[str]:
    """Pares de palabras con contenido consecutivas ("control gestion")."""
    palabras = [w for w in re.findall(r"[a-z]{3,}", texto_norm) if w not in _VACIAS]
    return {f"{a} {b}" for a, b in zip(palabras, palabras[1:])} - _BIGRAMAS_GENERICOS


_RE_CARGO_TECNICO = re.compile(
    r"\btecnic[oa]s?\b|\badministrativ[oa]s?\b|\bauxiliar\w*|\basistente\b|"
    r"\bgrado (?:1[5-9]|2\d)\b")
_RE_CARGO_PROFESIONAL = re.compile(r"\bprofesional\b|\bjef\w*|\bencargad\w*|\banalista\b|\bcoordinador\w*")

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
    # genéricos de avisos y CV que no dicen nada del calce
    "tecnico", "tecnica", "tecnicos", "clinica", "clinico", "horas", "turno",
    "reemplazo", "reemplazos", "datos", "diurno", "servicios", "santiago",
    "metropolitana", "instituto", "curso", "cursos", "diplomado", "certificado",
    "manejo", "usuario", "office", "excel", "equipo", "trabajar", "personas",
    "mayor", "menor", "minimo", "minima", "excluyente", "requisito", "requisitos",
    "titulo", "licenciado", "licenciada", "ingeniero", "ingeniera",
}
# Nombres de lugares tampoco son coincidencias de perfil.
_PALABRAS_RUIDO |= {w for r in (
    "arica parinacota tarapaca antofagasta atacama coquimbo valparaiso "
    "ohiggins higgins maule nuble biobio araucania lagos aysen magallanes "
    "providencia nunoa condes maipu puente florida vina quilpue antonio "
    "concepcion talcahuano angeles temuco valdivia osorno puerto montt "
    "coyhaique punta arenas iquique calama copiapo serena rancagua talca "
    "curico chillan granja pintana renca colina").split() for w in [r]}


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
    profesiones: list[str] = field(default_factory=list)   # claves de classification.profesiones
    funciones: list[str] = field(default_factory=list)     # claves de classification.funciones
    areas: list[str] = field(default_factory=list)
    titulos: list[str] = field(default_factory=list)
    anios_experiencia: int | None = None
    nivel: str | None = None
    region: str | None = None
    cargos_previos: list[str] = field(default_factory=list)
    capacitaciones: list[str] = field(default_factory=list)
    palabras: set[str] = field(default_factory=set)
    texto_norm: str = ""

    def a_dict(self) -> dict[str, Any]:
        """Lo que se devuelve al usuario (sin texto ni bolsa de palabras)."""
        return {
            "profesiones": [PROF.POR_CLAVE[c].etiqueta for c in self.profesiones],
            "funciones": [FUN.POR_CLAVE[c].etiqueta for c in self.funciones],
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


def _detectar_region(texto: str) -> str | None:
    """Región del postulante. Se ignoran los nombres de lugar dentro de
    instituciones ("Universidad de Concepción", línea por línea para no
    comerse la ciudad de la línea siguiente) y se privilegia el encabezado
    del CV, donde suele ir la dirección."""
    t = " \n ".join(_RE_UNIVERSIDAD.sub(" ", _norm(l)) for l in texto.splitlines())
    for zona in (t[:500], t):
        for r in _REGIONES:
            if _norm(r) in zona:
                return r
        for ciudad, region in _CIUDAD_REGION.items():
            if re.search(rf"\b{re.escape(ciudad)}\b", zona):
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


def _detectar_profesiones(texto: str, titulos: list[str],
                          cargos: list[str]) -> list[str]:
    """Profesiones del postulante. Se toma la PRIMERA profesión de cada
    título, cargo previo y línea del encabezado: así "Enfermera supervisora
    de TENS" cuenta como enfermera, no como TENS. Si nada calza, se usa lo
    que aparezca en el resto del CV."""
    # Encabezado: primeras líneas, sin las de experiencia fechada.
    lineas = [l for l in texto.splitlines()
              if l.strip() and not re.search(r"(?:19|20)\d{2}", l)][:3]

    def _de(frases: Iterable[str]) -> list[str]:
        claves: list[str] = []
        for frase in frases:
            c = PROF.principal(_norm(frase))
            if c and c not in claves:
                claves.append(c)
        return claves

    # El título manda: una ingeniera comercial que trabajó de administrativa
    # sigue siendo ingeniera comercial (lo afín se cubre por `campo`).
    claves = _de([*titulos, *lineas]) or _de(cargos) or PROF.todas(_norm(texto))[:2]
    return claves[:3]


def _titulos_de_educacion(texto: str) -> list[str]:
    """Líneas de la sección EDUCACIÓN que nombran una profesión
    ("Administrador Público 2022", "Psicóloga, U. de Chile")."""
    m = _RE_SECCION_EDUCACION.search(texto)
    if not m:
        return []
    resto = texto[m.end():]
    fin = _RE_SECCION.search(resto)
    bloque = resto[: fin.start()] if fin else resto[:1500]
    titulos = []
    for linea in bloque.splitlines():
        linea = linea.strip()
        if 4 <= len(linea) <= 90 and PROF.principal(_norm(linea)):
            titulos.append(_RE_ANIO_FINAL.sub("", linea))
    return titulos


def _titulos_por_patron(texto: str) -> list[str]:
    """`_RE_TITULO` sin falsos positivos del tipo "para optar al título de
    Investigador Policial" (la carrera de otros, no la del postulante)."""
    salida = []
    for m in _RE_TITULO.finditer(texto):
        previo = texto[max(0, m.start() - 20):m.start()].lower()
        if "optar" in previo:
            continue
        salida.append(_RE_ANIO_FINAL.sub("", m.group(1) + m.group(2)))
    return salida


def perfil_desde_texto(texto: str, hoy: date | None = None) -> Perfil:
    t = _norm(texto)
    titulos = _unicos([*_titulos_de_educacion(texto), *_titulos_por_patron(texto)], 8, 4)
    cargos = _unicos([*(m.group(1) for m in _RE_CARGO_PREVIO.finditer(texto)),
                      *(m.group("cargo") for m in _RE_CARGO_LINEA.finditer(texto))], 5, 8)
    profesiones = _detectar_profesiones(texto, titulos, cargos)
    areas = list(dict.fromkeys(PROF.POR_CLAVE[c].area for c in profesiones)) \
        or _detectar_areas(t)
    return Perfil(
        profesiones=profesiones,
        funciones=FUN.del_cv(t),
        areas=areas[:3],
        titulos=titulos,
        anios_experiencia=_detectar_anios(texto, hoy),
        nivel=PROF.POR_CLAVE[profesiones[0]].nivel if profesiones else None,
        region=_detectar_region(texto),
        cargos_previos=cargos,
        capacitaciones=_unicos((_RE_ANIO_FINAL.sub("", m.group(1))
                                for m in _RE_CAPACITACION.finditer(texto)), 10, 10),
        palabras=_tokens(t),
        texto_norm=t,
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
    #: Hubo al menos una señal de fondo (profesión, cargo previo o palabras
    #: del cargo). Región y experiencia solas no bastan para sugerir.
    nucleo: bool = False


def _ventanas(req: str, patron: re.Pattern) -> str:
    """Fragmentos de `req` que siguen a cada coincidencia de `patron`
    (hasta fin de frase o 260 caracteres)."""
    partes = []
    for m in patron.finditer(req):
        ini = m.start()
        fin = ini + 260
        if f := _RE_FIN_FRASE.search(req, ini, fin):
            fin = f.end()
        partes.append(req[ini:fin])
    return " ".join(partes)


def _formacion_exigida(req: str) -> str:
    """Fragmentos de los requisitos que hablan de título/carrera/formación."""
    partes = []
    for m in _RE_VENTANA_FORMACION.finditer(req):
        ini = m.start()
        fin = ini + 260
        if f := _RE_FIN_FRASE.search(req, ini, fin):
            fin = f.end()
        partes.append(req[ini:fin])
    return " ".join(partes)


def _anios_exigidos(req: str) -> int | None:
    for patron in _RE_ANIOS_EXIGIDOS:
        if m := patron.search(req):
            v = int(m.group(1))
            if 0 < v <= 30:
                return v
    return None


@dataclass(frozen=True)
class _AnalisisOferta:
    profs_cargo: tuple[str, ...]
    profs_req: tuple[str, ...]
    anios_exigidos: int | None
    especialidades: tuple[str, ...]
    funciones_cargo: tuple[str, ...] = ()
    tokens_cursos: frozenset[str] = frozenset()


#: Lo que no depende del CV se calcula una vez por oferta. Clave: id + hash
#: del texto (si el scraper actualiza la oferta, cambia el hash). Solo se
#: guardan tuplas cortas, nunca el texto: ~200 B por oferta.
_CACHE_OFERTAS: OrderedDict[tuple, _AnalisisOferta] = OrderedDict()
_CACHE_MAX = 20_000


def _analizar_oferta(oferta: dict[str, Any], cargo: str, req: str) -> _AnalisisOferta:
    clave = (oferta.get("id"), hash(cargo), hash(req))
    if (hit := _CACHE_OFERTAS.get(clave)) is not None:
        _CACHE_OFERTAS.move_to_end(clave)
        return hit
    a = _AnalisisOferta(
        profs_cargo=tuple(PROF.todas(cargo)),
        profs_req=tuple(PROF.todas(_formacion_exigida(req))) if req else (),
        anios_exigidos=_anios_exigidos(req) if req else None,
        especialidades=tuple(e.strip() for e in _RE_ESPECIALIDAD.findall(req) if e.strip()),
        funciones_cargo=tuple(FUN.del_cargo(cargo)),
        tokens_cursos=frozenset(_bigramas(_ventanas(req, _RE_VENTANA_CURSOS))) if req else frozenset(),
    )
    _CACHE_OFERTAS[clave] = a
    if len(_CACHE_OFERTAS) > _CACHE_MAX:
        _CACHE_OFERTAS.popitem(last=False)
    return a


def _etiquetas(claves: Iterable[str]) -> str:
    return ", ".join(PROF.POR_CLAVE[c].etiqueta for c in claves)


def afinidad(perfil: Perfil, oferta: dict[str, Any]) -> Resultado:
    """Puntúa una oferta contra el perfil y explica el porqué."""
    razones: list[str] = []
    reparos: list[str] = []
    puntaje = 0
    nucleo = False
    solo_afin = False

    cargo = _norm(oferta.get("cargo"))
    req = _norm(oferta.get("requisitos"))[:4000]
    tokens_cargo = _tokens(cargo)

    # 1) Profesión: la señal principal. Se busca en el cargo y en lo que las
    #    bases dicen de título/carrera. (No se usa ofertas.area_profesional:
    #    viene mal clasificado en muchas ofertas.)
    analisis = _analizar_oferta(oferta, cargo, req)
    profs_cargo = analisis.profs_cargo
    profs_of = list(dict.fromkeys(analisis.profs_cargo + analisis.profs_req))
    propias = set(perfil.profesiones)
    # Si el CARGO nombra una profesión de campo regulado (salud, educación,
    # derecho, social) que el postulante no tiene, no hay calce posible por
    # más que las bases mencionen otras carreras ("Psicólogo/a de
    # Reclutamiento" no es para un administrador público).
    regulada_ajena = [c for c in profs_cargo
                      if PROF.POR_CLAVE[c].campo in PROF.CAMPOS_ESTRICTOS and c not in propias]
    if regulada_ajena and propias:
        profs_of = [c for c in profs_of if c in propias] or profs_of
    if regulada_ajena and propias and not set(profs_of) & propias:
        reparos.append(f"El cargo es para {_etiquetas(regulada_ajena[:1])}")
        return Resultado(oferta=oferta, puntaje=0, banda="baja",
                         razones=razones, reparos=reparos, nucleo=False)
    if propias and profs_of:
        exactas = [c for c in profs_of if c in propias]
        if exactas:
            nucleo = True
            if exactas[0] in profs_cargo:
                puntaje += 60
                razones.append(f"El cargo es de tu profesión ({_etiquetas(exactas[:1])})")
            else:
                puntaje += 50
                razones.append(f"Las bases aceptan tu formación ({_etiquetas(exactas[:1])})")
        else:
            afines = [c for c in profs_of
                      if PROF.POR_CLAVE[c].campo not in PROF.CAMPOS_ESTRICTOS
                      and any(PROF.POR_CLAVE[c].campo == PROF.POR_CLAVE[p].campo
                              and PROF.puede_ejercer(p, c) for p in propias)]
            if afines:
                nucleo = True
                solo_afin = True
                puntaje += 30
                razones.append(f"Cargo afín a tu formación ({_etiquetas(afines[:1])})")
            else:
                reparos.append(f"Piden otra formación: {_etiquetas(profs_of[:2])}")

    # 1b) Área de trabajo: muchas ofertas no piden una profesión sino una
    #     función ("Analista de Datos", "Unidad de Personal"). Si el CV muestra
    #     experiencia en esa función, es señal de fondo — salvo que la oferta
    #     pida OTRA profesión que el postulante no tiene.
    otra_profesion = bool(propias and profs_of and not nucleo)
    funciones = [f for f in analisis.funciones_cargo if f in perfil.funciones]
    if funciones and not otra_profesion:
        nucleo = True
        puntaje += 35
        razones.append(f"El cargo es de tu área de trabajo ({FUN.POR_CLAVE[funciones[0]].etiqueta})")

    # 2) Cargo ya ejercido: evidencia directa de idoneidad.
    for previo in perfil.cargos_previos:
        previo_n = _norm(previo)
        comunes = tokens_cargo & _tokens(previo_n)
        misma_area = bool(set(FUN.del_cargo(previo_n)) & set(analisis.funciones_cargo))
        if (len(comunes) >= 2 or misma_area) and (nucleo or not profs_of):
            nucleo = True
            puntaje += 15
            razones.append(f"Ya te desempeñaste como {previo}")
            break

    # 3) Palabras del cargo presentes en el CV (sin lugares ni genéricos).
    solapa = tokens_cargo & perfil.palabras
    if solapa and (nucleo or (not profs_of and len(solapa) >= 2)):
        nucleo = True
        puntaje += min(10, 5 * len(solapa))
        razones.append("Tu CV menciona: " + ", ".join(sorted(solapa)[:3]))

    # 4) Región.
    region_of = oferta.get("region")
    if perfil.region and region_of:
        if _norm(perfil.region) in _norm(region_of):
            puntaje += 15
            razones.append(f"Está en tu región ({region_of})")
        else:
            puntaje -= 5
            reparos.append(f"Se ubica en {region_of}")

    if req:
        tokens_req = _tokens(req)
        # 5) Cursos y certificaciones del CV que las bases mencionan.
        # Solo contra lo que las bases dicen de cursos/capacitación, y por
        # frase ("control de gestión", "gestión de proyectos"), no por palabra
        # suelta: "públicas" o "gestión" calzaban con casi cualquier aviso.
        cursos = [c for c in perfil.capacitaciones
                  if _bigramas(_norm(c)) & analisis.tokens_cursos]
        if cursos:
            puntaje += min(10, 5 * len(cursos))
            razones.append("Las bases valoran lo que acreditas: " + "; ".join(cursos[:2]))
        # 6) Experiencia exigida.
        exigidos = analisis.anios_exigidos
        if exigidos:
            plural = "s" if exigidos > 1 else ""
            if perfil.anios_experiencia is None:
                reparos.append(f"Piden {exigidos} año{plural} de experiencia "
                               "(no pudimos deducirla de tu CV)")
            elif perfil.anios_experiencia < exigidos:
                puntaje -= 10
                reparos.append(f"Piden {exigidos} año{plural} de experiencia y "
                               f"detectamos ~{perfil.anios_experiencia}")
            else:
                puntaje += 10
                razones.append(f"Cumples la experiencia pedida ({exigidos} año{plural})")
        # 7) Especialidad exigida (p. ej. médicos) que el CV no muestra.
        especialidades = analisis.especialidades
        if especialidades and not any(
                e.split()[-1] in perfil.texto_norm and "especiali" in perfil.texto_norm
                for e in especialidades):
            puntaje -= 15
            reparos.append("Exigen especialidad en " + " o ".join(especialidades[:3])
                           + "; no la vemos en tu CV")
    else:
        reparos.append("La oferta no publica requisitos en texto; revisa las bases")

    # Cargo de nivel técnico/administrativo para un perfil profesional:
    # se muestra (puede interesar) pero baja y se avisa.
    if perfil.nivel == "Profesional" and _RE_CARGO_TECNICO.search(cargo) \
            and not _RE_CARGO_PROFESIONAL.search(cargo):
        puntaje -= 25
        solo_afin = True
        reparos.append("Es un cargo de nivel técnico o administrativo")

    banda = "alta" if puntaje >= 70 else "media" if puntaje >= 45 else "baja"
    if solo_afin and banda == "alta":
        banda = "media"          # afín no es tu profesión exacta
    return Resultado(oferta=oferta, puntaje=puntaje, banda=banda,
                     razones=razones, reparos=reparos, nucleo=nucleo)


def rankear(perfil: Perfil, ofertas: Iterable[dict[str, Any]],
            top: int = 30, minimo: int = 30) -> list[Resultado]:
    """Ofertas ordenadas por afinidad; descarta las que no alcanzan `minimo`
    o no tienen ninguna señal de fondo."""
    res = [r for r in (afinidad(perfil, o) for o in ofertas)
           if r.nucleo and r.puntaje >= minimo]
    res.sort(key=lambda r: (-r.puntaje, str(r.oferta.get("fecha_cierre") or "9999")))
    return res[:top]
