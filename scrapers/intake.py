"""
EmpleoEstado.cl / contrataoplanta.cl — Capa de intake unificada para scrapers.

Centraliza decisiones que antes vivían dispersas en cada scraper:

1. Detección de basura (noticias, comunicados, resultados, nóminas, actas,
   adjudicaciones, contenido institucional general, eventos, licitaciones,
   concursos no laborales).
2. Validación de vigencia: descarte por fecha_cierre vencida, por antigüedad
   anómala (un proceso de 6+ meses abierto es altamente sospechoso), por
   señales explícitas de cierre en el texto.
3. Validación de remuneraciones: descarta montos absurdamente altos para el
   sector público chileno y montos que parecen presupuesto/anual mal parseado.
4. Validación de campos mínimos: cargo + URL + (institución o región) son
   obligatorios; si faltan, la oferta se descarta o se marca needs_review.

Cualquier scraper —antes de persistir— debe pasar por
``intake_validate_offer(offer)``. El resultado indica si la oferta entra,
se descarta (con motivo), o se acepta marcada para revisión manual.

Diseño:
- No hace I/O (red ni BD). Es puro validador en memoria.
- No depende del pipeline pydantic completo (RawPage / ClassificationResult)
  — lo invoca cualquier scraper, incluidos los legacy que producen dicts.
- Reutiliza la lógica de salary_extractor cuando hay texto disponible.

Uso típico:
    from scrapers.intake import intake_validate_offer
    decision = intake_validate_offer(offer_dict)
    if decision.discard:
        logger.info("descarte motivo=%s", decision.motivo_descarte)
        continue
    if decision.needs_review:
        offer_dict["needs_review"] = True
    save(offer_dict)
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from classification.policy import (
    INTERNAL_ONLY_PATTERNS,
    NEGATIVE_PATTERNS,
    NEGATIVE_URL_PARTS,
    RULESET_VERSION,
    classify_offer_candidate,
)

# ─────────────────────────── Listas negativas ─────────────────────────
# Frases que, si aparecen en cargo/título/descripcion, descartan la oferta.
# Ojo: deben ser frases discriminantes; "cerrado" como sustring de "cerrador
# de operaciones" no debe gatillar — usamos límites de palabra.

_GARBAGE_PHRASES: tuple[str, ...] = NEGATIVE_PATTERNS

_GARBAGE_RE = re.compile("|".join(_GARBAGE_PHRASES), re.IGNORECASE)

# Palabras que, en URL o ruta, son indicador fuerte de que NO es una oferta.
_GARBAGE_URL_PARTS: tuple[str, ...] = NEGATIVE_URL_PARTS

# Frases que indican explícitamente que un cargo es solo "difusión interna"
# y por tanto no se publica (el postulante externo no puede acceder).
_INTERNAL_ONLY_PHRASES: tuple[str, ...] = INTERNAL_ONLY_PATTERNS
_INTERNAL_ONLY_RE = re.compile("|".join(_INTERNAL_ONLY_PHRASES), re.IGNORECASE)


# ─────────────────────────── Vigencia / antigüedad ────────────────────
# Reglas empíricas para empleo público chileno:
# - La inmensa mayoría de procesos cierra en 7–60 días.
# - Procesos > 90 días son inusuales pero existen (concursos de planta).
# - Procesos > 180 días son altamente sospechosos: típicamente la fecha
#   nunca se actualizó, o el aviso quedó "fantasma" en el sitio.
# - Procesos > 365 días son casi seguro basura.
#
# Política aplicada:
#   antigüedad <= 90 días  → válido
#   91–180 días            → necesita validación (needs_review = True)
#   181–365 días           → descartar salvo que tenga fecha_cierre futura
#                            verificada por el scraper
#   > 365 días             → descartar siempre

ANTIGUEDAD_OK_DIAS: int = 90
ANTIGUEDAD_REVISION_DIAS: int = 180
ANTIGUEDAD_DESCARTE_DIAS: int = 365


# ─────────────────────────── Remuneración ─────────────────────────────
# El sector público chileno tiene techos razonables:
# - Cargos estándar: hasta ~$8.000.000 mensuales bruto.
# - ADP / directivos top: hasta ~$12.000.000 bruto en casos puntuales.
# - Cualquier cifra > $15.000.000 mensual es prácticamente imposible
#   y casi siempre es: presupuesto anual, monto de proyecto, o error
#   de parsing (puntos vs comas, suma de varios bonos, etc.).
#
# Estos umbrales coinciden con extraction/salary_extractor.py para que
# el comportamiento sea consistente cuando un scraper bypassa el pipeline.

RENTA_MAX_CONFIABLE: int = 10_000_000
RENTA_MAX_SOSPECHOSA: int = 15_000_000
RENTA_MIN_RAZONABLE: int = 250_000  # < $250k mensual no es empleo público real


# ─────────────── Fecha desde la URL / cierre futuro inverosímil ────────
# Muchos sitios WordPress municipales codifican la fecha de publicación en la
# ruta: /2019/11/13/titulo/. Es la señal más fiable de antigüedad cuando el
# scraper no extrajo fecha_publicacion del contenido.
_URL_DATE_RE = re.compile(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)")
_URL_YM_RE = re.compile(r"/(20\d{2})/(\d{1,2})(?:/|$)")

# Un cierre a más de ~5 meses casi nunca es real en empleo público chileno:
# suele ser una fecha inventada (se tomó "15 de noviembre" de una noticia vieja
# y se le puso el año actual/siguiente). No se confía en ella.
DIAS_CIERRE_FUTURO_MAX: int = 150

# Títulos que NO son cargos: actas, decretos, resultados, etiquetas de
# navegación y titulares de noticias. Si el CARGO es esto, la fila es basura.
_NON_JOB_TITLE_PATTERNS: tuple[str, ...] = (
    r"\bacta\b", r"\blistado final\b", r"\bseleccionad", r"\bganador",
    r"\badjudicaci", r"\bn[oó]mina\b", r"\bdeliberaci", r"\bdecreto\b",
    r"\bpr[oó]rroga\b", r"\bampliaci[oó]n de plazo", r"\bproyectos admisibles\b",
    r"\banexos?\b", r"\bficha de postulaci", r"\bver decreto\b", r"\bver m[aá]s\b",
    r"\btrabaja con nosotros\b", r"\bpostula aqu[ií]\b", r"^\s*cargos?\s*$",
    r"\bbases administrativas\b", r"\bhomenaje", r"\bconmemora", r"\baniversario\b",
    r"^\s*inicio\s*$", r"^\s*acceder\s*$", r"\bdescargar\b", r"\bver llamado\b",
    r"\bver todos\b", r"^\s*bases\s*$", r"\breitera\b", r"\brecuerda\b",
    r"\binforma sobre\b", r"publicaci[o]n .{0,20}?en diario",
    r"^\s*concursos?\s*$", r"\bbolsa de empleo", r"^\s*postulaciones( y concursos)?\s*$",
    r"\bpostulaciones escuela", r"^\s*oportunidades( laborales)?\s*$",
    r"^\s*vacantes\s*$", r"^\s*empleos\s*$", r"^\s*ofertas( laborales| de empleo)?\s*$",
    r"^\s*trabaja con nosotros\s*$", r"^\s*postula\s*$",
)
_NON_JOB_TITLE_RE = re.compile("|".join(_NON_JOB_TITLE_PATTERNS), re.IGNORECASE)

# Señales fuertes de que el título SÍ es un empleo. Sirven para anular el
# descarte por ruta de URL (muchas municipalidades publican sus concursos bajo
# /noticias/, que de otro modo se tomaría como contenido no laboral).
_JOB_SIGNAL_PATTERNS: tuple[str, ...] = (
    r"concurso p[uú]blico", r"llamado a concurso", r"oferta laboral",
    r"\bvacante", r"\bconvocatoria", r"bases de(?:l)? concurso",
    r"postular al cargo", r"\bcargo de\b", r"perfil de(?:l)? cargo",
    r"se requiere", r"se necesita", r"proceso de selecci[oó]n",
)
_JOB_SIGNAL_RE = re.compile("|".join(_JOB_SIGNAL_PATTERNS), re.IGNORECASE)


# ─────────────────────────── Resultado ────────────────────────────────

@dataclass(slots=True)
class IntakeDecision:
    """Resultado de evaluar una oferta antes de persistirla."""

    discard: bool = False
    needs_review: bool = False
    motivo_descarte: str | None = None
    review_reasons: list[str] = field(default_factory=list)
    salary_sanitized: bool = False
    notes: list[str] = field(default_factory=list)

    def add_review(self, reason: str) -> None:
        if reason not in self.review_reasons:
            self.review_reasons.append(reason)
        self.needs_review = True

    def reject(self, motivo: str) -> "IntakeDecision":
        self.discard = True
        self.motivo_descarte = motivo
        return self


# ─────────────────────────── Helpers internos ─────────────────────────

def _norm(text: Any) -> str:
    if not text:
        return ""
    s = str(text)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()


def _coerce_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def _today() -> date:
    return datetime.now(timezone.utc).date()


def fecha_desde_url(url: Any) -> date | None:
    """Extrae la fecha de publicación embebida en la ruta (WordPress: /AAAA/MM/DD/)."""
    sx = str(url or "")
    m = _URL_DATE_RE.search(sx)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    m = _URL_YM_RE.search(sx)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            pass
    return None


def titulo_es_no_laboral(cargo: Any) -> bool:
    """True si el título es un acta/decreto/resultado/etiqueta de navegación
    o titular de noticia, en vez de un cargo real."""
    sx = str(cargo or "").strip()
    if not sx:
        return False
    return bool(_NON_JOB_TITLE_RE.search(sx))


def titulo_es_empleo(cargo: Any) -> bool:
    """True si el título tiene señales fuertes de ser una oferta de empleo.
    Se usa para no descartar por ruta de URL avisos reales publicados en
    secciones tipo /noticias/."""
    return bool(_JOB_SIGNAL_RE.search(str(cargo or "")))


_MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
_FECHA_LARGA_RE = re.compile(r"\b(\d{1,2})\s+de\s+([a-z]+)\s+(?:de(?:l)?\s+)?(\d{4})")
_FECHA_NUM_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")
# Frases que anuncian explícitamente el cierre, seguidas de una fecha.
_CIERRE_HINT_RE = re.compile(
    r"(?:hasta el|durar[a]? hasta el|durar[a]? hasta|plazo[^.]{0,40}?\bal\b|"
    r"postul\w*[^.]{0,40}?hasta el|recepci[o]n[^.]{0,40}?hasta el|"
    r"cierre[^.]{0,25}?\bel\b|vence el|t[e]rmino[^.]{0,25}?\bel\b)\s*"
    r"(\d{1,2})\s+de\s+([a-z]+)(?:\s+de(?:l)?\s+(\d{4}))?",
)


def _fecha_es(dia: str, mes: str, anio: str | None, ref_anio: int) -> "date | None":
    m = _MESES_ES.get(mes)
    if not m:
        return None
    try:
        y = int(anio) if anio else ref_anio
        return date(y, m, int(dia))
    except (ValueError, TypeError):
        return None


def _todas_las_fechas(texto_norm: str, ref_anio: int) -> list[date]:
    fechas: list[date] = []
    for d, mes, y in _FECHA_LARGA_RE.findall(texto_norm):
        f = _fecha_es(d, mes, y, ref_anio)
        if f:
            fechas.append(f)
    for d, mo, y in _FECHA_NUM_RE.findall(texto_norm):
        try:
            fechas.append(date(int(y), int(mo), int(d)))
        except ValueError:
            continue
    return fechas


def fecha_cierre_desde_texto(texto: Any, *, today: "date | None" = None) -> "date | None":
    """Extrae una fecha de cierre embebida en el texto del aviso.

    1) Frase explícita de cierre ("...durará hasta el 19 de marzo del 2026",
       "plazo de postulación ... al DD de MMMM", "postular hasta el ...").
    2) Si no hay frase explícita pero TODAS las fechas del proceso ya pasaron,
       devuelve la última (el proceso terminó → el aviso está vencido).
    Devuelve None si no hay señal fiable.
    """
    t = _norm(texto)
    if not t:
        return None
    today = today or _today()
    m = _CIERRE_HINT_RE.search(t)
    if m:
        f = _fecha_es(m.group(1), m.group(2), m.group(3), today.year)
        if f:
            return f
    fechas = _todas_las_fechas(t, today.year)
    if fechas and max(fechas) < today:
        return max(fechas)
    return None


# ─────────────────────────── Reglas individuales ──────────────────────

def is_garbage_text(text: str | None) -> bool:
    """True si el texto coincide con frases-marca de noticias/resultados/etc.

    No descarta por palabras genéricas como 'concurso' (el aviso real las
    contiene); descarta sólo por combinaciones discriminantes.
    """
    if not text:
        return False
    return bool(_GARBAGE_RE.search(text))


def is_garbage_url(url: str | None) -> bool:
    """True si la ruta de la URL es típica de contenido no-laboral."""
    if not url:
        return False
    lower = url.lower()
    return any(part in lower for part in _GARBAGE_URL_PARTS)


def is_internal_only(text: str | None) -> bool:
    """Detecta avisos marcados como 'solo difusión interna'."""
    if not text:
        return False
    return bool(_INTERNAL_ONLY_RE.search(text))


def assess_salary(
    renta_min: Any,
    renta_max: Any,
    contexto: str | None = None,
) -> tuple[int | None, int | None, str | None]:
    """Sanitiza un par (min, max) de renta en CLP.

    Devuelve ``(min_safe, max_safe, motivo)``. Si motivo no es None, la
    renta se considera no confiable y debe limpiarse del registro final.

    Reglas:
    - max > $15MM ⇒ descartar (probable presupuesto / anual mal parseado).
    - max > $10MM sin contexto que mencione 'renta'/'sueldo'/'remuneracion'
      ⇒ marcar como no confiable.
    - min < $250k ⇒ tratar el valor como ruido (cifra suelta sin contexto).
    """
    def _to_int(v: Any) -> int | None:
        if v in (None, ""):
            return None
        try:
            n = int(re.sub(r"[^\d]", "", str(v)))
        except ValueError:
            return None
        return n if n > 0 else None

    lo = _to_int(renta_min)
    hi = _to_int(renta_max)

    if lo is None and hi is None:
        return None, None, None

    # Nivelar: si sólo hay uno, ambos toman ese valor.
    if lo is None:
        lo = hi
    if hi is None:
        hi = lo
    if lo is not None and hi is not None and lo > hi:
        lo, hi = hi, lo

    if hi is not None and hi >= RENTA_MAX_SOSPECHOSA:
        return None, None, "renta_descarte_monto_inverosimil"

    if lo is not None and lo < RENTA_MIN_RAZONABLE:
        # No es necesariamente erróneo (puede ser un cargo a honorarios
        # parcial), pero baja confianza: dejamos sólo el extremo confiable.
        if hi is not None and hi >= RENTA_MIN_RAZONABLE:
            lo = hi  # quedarnos sólo con el monto mayor
        else:
            return None, None, "renta_descarte_monto_irrelevante"

    if hi is not None and hi >= RENTA_MAX_CONFIABLE:
        ctx = _norm(contexto)
        positive_ctx = any(
            token in ctx
            for token in (
                "renta bruta",
                "remuneracion",
                "renta liquida",
                "sueldo",
                "honorarios mensuales",
                "monto mensual",
                "renta ofrecida",
                "renta mensual",
            )
        )
        if not positive_ctx:
            return None, None, "renta_no_confiable_sin_contexto"

    return lo, hi, None


def assess_vigencia(
    fecha_publicacion: Any,
    fecha_cierre: Any,
    today: date | None = None,
    *,
    motivo_cierre_vencido: str = "fecha_cierre_vencida",
    motivo_publicacion_180_sin_cierre: str = "publicacion_excede_180_dias_sin_cierre",
) -> tuple[bool, str | None, bool]:
    """Devuelve (descartar, motivo, needs_review) según vigencia del aviso.

    - fecha_cierre en el pasado → descarte.
    - fecha_publicacion muy antigua sin fecha_cierre futura conocida →
      descarte o revisión según umbrales.
    - sin fechas → no descarta (otros chequeos lo manejan).
    """
    today = today or _today()
    cierre = _coerce_date(fecha_cierre)
    publi = _coerce_date(fecha_publicacion)

    if cierre and cierre < today:
        return True, motivo_cierre_vencido, False

    # Una publicación de más de 365 días invalida CUALQUIER fecha de cierre:
    # un cierre futuro sobre un post tan viejo está inventado. (La fecha puede
    # venir de la URL /AAAA/MM/DD/ vía fecha_desde_url en el caller.)
    if publi and (today - publi).days > ANTIGUEDAD_DESCARTE_DIAS:
        return True, "publicacion_excede_365_dias", False

    cierre_sospechoso = False
    if cierre and cierre >= today:
        if (cierre - today).days > DIAS_CIERRE_FUTURO_MAX:
            # Cierre sospechosamente lejano: probable fecha inventada. No se
            # confía; se evalúa por antigüedad de publicación más abajo.
            cierre_sospechoso = True
        else:
            # Cierre futuro plausible → confiamos en él aunque la publicación
            # sea antigua (procesos largos legítimos).
            return False, None, False

    if publi:
        edad = (today - publi).days
        if edad > ANTIGUEDAD_DESCARTE_DIAS:
            return True, "publicacion_excede_365_dias", False
        if edad > ANTIGUEDAD_REVISION_DIAS:
            return True, motivo_publicacion_180_sin_cierre, False
        if edad > ANTIGUEDAD_OK_DIAS:
            return False, None, True

    if cierre_sospechoso:
        # Cierre lejano no confiable y sin publicación que lo respalde →
        # no se descarta en duro (puede ser real), pero se marca para revisión.
        return False, None, True
    return False, None, False


def assess_minimum_fields(offer: dict[str, Any]) -> tuple[bool, str | None]:
    """Cargo + URL son indispensables. Sin ellos la oferta no aporta."""
    cargo = (offer.get("cargo") or offer.get("titulo") or "").strip()
    url = (offer.get("url_oferta") or offer.get("url_original") or "").strip()
    if not cargo:
        return True, "sin_cargo"
    if not url:
        return True, "sin_url"
    if len(cargo) < 5:
        return True, "cargo_demasiado_corto"
    return False, None


# ─────────────────────────── API pública ──────────────────────────────

def intake_validate_offer(
    offer: dict[str, Any],
    *,
    extra_text: str | None = None,
    today: date | None = None,
) -> IntakeDecision:
    """Pipeline rápido de validación que cualquier scraper debe invocar
    antes de persistir.

    Parámetros:
    - ``offer``: dict con al menos ``cargo`` y ``url_oferta``/``url_original``.
      Se inspeccionan también: ``descripcion``, ``requisitos``, ``renta_texto``,
      ``renta_bruta_min``, ``renta_bruta_max``, ``fecha_publicacion``,
      ``fecha_cierre``.
    - ``extra_text``: texto adicional (HTML completo, PDF) si el scraper lo
      tiene a mano. Mejora la precisión de las heurísticas.

    Mutaciones que hace sobre ``offer`` (in-place):
    - Si la renta es no confiable, deja ``renta_bruta_min`` y ``renta_bruta_max``
      en None y agrega ``renta_validation_status`` con el motivo.
    - Si el aviso necesita revisión, agrega ``needs_review = True`` y
      ``review_reasons`` (lista).
    """
    decision = IntakeDecision()

    # 1. Campos mínimos
    discard, motivo = assess_minimum_fields(offer)
    if discard:
        return decision.reject(motivo)

    cargo = offer.get("cargo") or offer.get("titulo") or ""
    descripcion = offer.get("descripcion") or ""
    requisitos = offer.get("requisitos") or ""
    renta_texto = offer.get("renta_texto") or ""
    url = offer.get("url_oferta") or offer.get("url_original") or ""
    blob_partes = [cargo, descripcion, requisitos, renta_texto]
    if extra_text:
        blob_partes.append(extra_text)
    blob = " \n ".join(str(p) for p in blob_partes if p)

    policy_eval = classify_offer_candidate(
        title=str(cargo),
        content_text=str(descripcion),
        url=str(url),
        extra_text=blob,
    )
    offer["policy_ruleset_version"] = RULESET_VERSION
    offer["policy_score"] = policy_eval.score
    offer["policy_reason_codes"] = list(policy_eval.reason_codes)

    # 2. Difusión interna → descarte (no es publicable a externos)
    if is_internal_only(cargo) or is_internal_only(blob):
        return decision.reject("solo_difusion_interna")

    # 3a. URLs de archivo histórico del sitio (concursos viejos rearchivados):
    #     señal fuerte de contenido vencido, sin importar el título.
    url_low = str(url).lower()
    if any(p in url_low for p in ("oldweb", "web2010", "/web2009", "/archivo/")):
        return decision.reject("url_archivo_viejo")

    # 3b. Basura por URL — salvo que el título sea claramente un empleo
    #     (muchas municipalidades publican concursos bajo /noticias/).
    if is_garbage_url(url) and not titulo_es_empleo(cargo):
        return decision.reject("url_no_laboral")

    # 4. Basura por texto (combinación de frase + ausencia de señales pos.)
    if is_garbage_text(cargo):
        return decision.reject("cargo_es_noticia_o_resultado")

    if titulo_es_no_laboral(cargo):
        return decision.reject("cargo_no_laboral_acta_decreto_etiqueta")

    if is_garbage_text(blob):
        # El blob completo puede contener "noticias", "resultados", etc. por
        # contexto institucional, pero aún así corresponder a un cargo válido
        # (ej: "Periodista"). Para evitar falsos negativos, no descartamos en
        # este punto y exigimos revisión manual.
        decision.add_review("texto_contiene_indicadores_no_laborales")

    # 5. Vigencia / antigüedad
    plataforma = _norm(offer.get("plataforma_empleo") or offer.get("plataforma") or "")
    url_norm = _norm(url)
    is_wordpress_offer = (
        "wordpress" in plataforma
        or "/wp-content/" in url_norm
        or "/wp-json/" in url_norm
    )
    motivo_cierre = "wordpress_expired_deadline" if is_wordpress_offer else "fecha_cierre_vencida"
    motivo_180 = (
        "wordpress_old_without_deadline"
        if is_wordpress_offer
        else "publicacion_excede_180_dias_sin_cierre"
    )
    # Si no hay fecha_cierre estructurada, intentar extraerla del propio texto
    # del aviso ("...durará hasta el 19 de marzo del 2026"). Muchos municipales
    # la traen solo en la descripción y por eso nunca expiraban.
    fecha_cierre_efectiva = offer.get("fecha_cierre")
    _cc = _coerce_date(fecha_cierre_efectiva)
    _hoy = today or _today()
    # Re-derivar del texto si no hay cierre, o si el guardado es sospechosamente
    # lejano (típico año mal parseado: "19 de marzo del 2026" guardado como 2027).
    _cierre_sospechoso = _cc is not None and (_cc - _hoy).days > DIAS_CIERRE_FUTURO_MAX
    if _cc is None or _cierre_sospechoso:
        fc_txt = fecha_cierre_desde_texto(
            " ".join(str(p) for p in (cargo, descripcion, requisitos) if p),
            today=today,
        )
        if fc_txt is not None:
            fecha_cierre_efectiva = fc_txt
            offer.setdefault("fecha_cierre_desde_texto", fc_txt.isoformat())

    descartar, motivo_v, review_v = assess_vigencia(
        offer.get("fecha_publicacion") or fecha_desde_url(url),
        fecha_cierre_efectiva,
        today=today,
        motivo_cierre_vencido=motivo_cierre,
        motivo_publicacion_180_sin_cierre=motivo_180,
    )
    if descartar:
        return decision.reject(motivo_v or "vigencia_invalida")
    if review_v:
        decision.add_review("publicacion_antigua_sin_cierre")

    # 6. Remuneración
    lo, hi, motivo_r = assess_salary(
        offer.get("renta_bruta_min"),
        offer.get("renta_bruta_max"),
        contexto=" ".join(filter(None, [renta_texto, descripcion])),
    )
    if motivo_r:
        offer["renta_bruta_min"] = None
        offer["renta_bruta_max"] = None
        offer["renta_validation_status"] = motivo_r
        decision.salary_sanitized = True
        if motivo_r == "renta_descarte_monto_inverosimil":
            decision.add_review("renta_descartada_monto_inverosimil")
        else:
            decision.notes.append(motivo_r)
    else:
        if lo is not None:
            offer["renta_bruta_min"] = lo
        if hi is not None:
            offer["renta_bruta_max"] = hi

    # Reflejar review en el offer (para que persistencia lo sepa)
    if decision.needs_review:
        offer["needs_review"] = True
        offer["review_reasons"] = list(decision.review_reasons)

    return decision


__all__ = [
    "ANTIGUEDAD_DESCARTE_DIAS",
    "ANTIGUEDAD_OK_DIAS",
    "ANTIGUEDAD_REVISION_DIAS",
    "IntakeDecision",
    "RENTA_MAX_CONFIABLE",
    "RENTA_MAX_SOSPECHOSA",
    "RENTA_MIN_RAZONABLE",
    "assess_minimum_fields",
    "assess_salary",
    "assess_vigencia",
    "intake_validate_offer",
    "is_garbage_text",
    "is_garbage_url",
    "is_internal_only",
]
