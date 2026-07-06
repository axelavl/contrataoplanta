"""
EmpleoEstado.cl — Capa de base de datos
Manejo de conexiones, modelos y operaciones de escritura.
"""

import hashlib
import json
import logging
import unicodedata
from datetime import datetime, date
from typing import Optional

from sqlalchemy import (
    create_engine, text, Column, Integer, String, Text,
    Boolean, BigInteger, Date, DateTime, Numeric, ForeignKey, ARRAY
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

from db.config import get_database_config

logger = logging.getLogger(__name__)

# ── Engine y sesión ──────────────────────────────────────────────────────────
# DSN derivado de `db.config` (misma fuente que usan api/main.py y
# scrapers/base.py). No usar `config.DB_URL` de `config.py` — aunque
# coincide hoy, mantener un único punto de resolución evita drift.
engine = create_engine(
    get_database_config().to_sqlalchemy_url(),
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,       # verifica conexión antes de usar
    echo=False,
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db() -> Session:
    """Context manager para sesiones."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Utilidades ───────────────────────────────────────────────────────────────
def url_a_hash(url: str) -> str:
    """Genera SHA256 de una URL para detección de duplicados."""
    return hashlib.sha256(url.strip().lower().encode()).hexdigest()


def limpiar_texto(texto: str | None) -> str:
    """Normaliza espacios para comparaciones y IDs estables."""
    if not texto:
        return ""
    return " ".join(str(texto).replace("\xa0", " ").split()).strip()


def generar_id_estable(*partes, largo: int = 20) -> str:
    """Genera un ID hash estable a partir de varias piezas de texto."""
    base = "||".join(limpiar_texto(parte) for parte in partes if parte is not None)
    digest = hashlib.sha1(base.lower().encode()).hexdigest()
    return digest[:largo]


def _plegar_para_clave(texto: str | None) -> str:
    """Minúsculas, sin tildes y sin puntuación, para comparar entre portales."""
    base = limpiar_texto(texto)
    if not base:
        return ""
    desc = unicodedata.normalize("NFD", base)
    sin_tildes = "".join(c for c in desc if unicodedata.category(c) != "Mn")
    solo_alnum = "".join(c if c.isalnum() or c.isspace() else " " for c in sin_tildes)
    return " ".join(solo_alnum.lower().split())


def clave_dedup_difusa(institucion, cargo, fecha_cierre, region) -> str:
    """Clave estable para detectar la MISMA oferta llegada desde portales distintos
    (Plan Parte 1.8). Combina institución + cargo + fecha de cierre + región,
    todo plegado (sin tildes, sin puntuación, minúsculas) para que variaciones
    de formato no rompan el match. No reemplaza al ``url_hash`` exacto: es una
    segunda llave de fusión.
    """
    if hasattr(fecha_cierre, "isoformat"):
        fecha = fecha_cierre.isoformat()
    else:
        fecha = limpiar_texto(str(fecha_cierre)) if fecha_cierre else ""
    return generar_id_estable(
        _plegar_para_clave(institucion),
        _plegar_para_clave(cargo),
        fecha,
        _plegar_para_clave(region),
        largo=40,
    )


def truncar_texto(valor, max_len: int) -> str | None:
    """Recorta strings a la longitud máxima definida por el schema."""
    if valor is None:
        return None
    texto = limpiar_texto(valor)
    if not texto:
        return None
    return texto[:max_len]


def _sin_caracteres_nul(valor):
    """Quita el byte NUL (0x00) y otros controles C0 (salvo \\t\\n\\r) de un str.

    PostgreSQL rechaza cadenas con NUL ('A string literal cannot contain NUL
    characters'); el texto extraído de PDFs/DOCX a veces los trae y hacía fallar
    el INSERT completo (perdiendo la oferta). Se limpia toda cadena antes de
    persistir.
    """
    if not isinstance(valor, str):
        return valor
    def _malo(c: str) -> bool:
        o = ord(c)
        return (o < 32 and c not in "\t\n\r") or o == 127
    if not any(_malo(c) for c in valor):
        return valor
    return "".join(c for c in valor if not _malo(c))


def normalizar_datos_oferta(datos: dict) -> dict:
    """Ajusta largos de campos y sanea caracteres NUL/control para evitar errores
    de VARCHAR y de literales inválidos en PostgreSQL."""
    normalizados = {k: _sin_caracteres_nul(v) for k, v in datos.items()}
    limites = {
        "id_externo": 200,
        "cargo": 500,
        "institucion_nombre": 300,
        "sector": 80,
        "area_profesional": 100,
        "tipo_cargo": 50,
        "nivel": 80,
        "region": 80,
        "ciudad": 80,
        "renta_texto": 200,
        "modalidad": 50,
        "jornada": 100,
        "email_postulacion": 200,
        "email_consultas": 200,
    }
    for campo, limite in limites.items():
        normalizados[campo] = truncar_texto(normalizados.get(campo), limite)
    return normalizados


# ── Operaciones principales ──────────────────────────────────────────────────
def upsert_oferta(db: Session, datos: dict) -> tuple[bool, bool]:
    """
    Inserta o actualiza una oferta.

    Aplica el intake transversal antes de tocar la BD: descarta noticias,
    resultados, vencidos por antigüedad y montos absurdos. Si el intake
    descarta, retorna ``(False, False)`` y la fila no se escribe.

    Retorna: (es_nueva: bool, fue_actualizada: bool)
    """
    # Mapping a las claves que el intake espera (url_oferta, fecha_*).
    # No mutamos ``datos`` para no afectar al caller; pasamos una vista.
    from scrapers.intake import intake_validate_offer

    intake_view = dict(datos)
    intake_view.setdefault("url_oferta", datos.get("url_original"))
    decision = intake_validate_offer(intake_view)
    if decision.discard:
        logger.info(
            "intake_descarte url=%s motivo=%s cargo=%s",
            datos.get("url_original"),
            decision.motivo_descarte,
            (datos.get("cargo") or "")[:80],
        )
        return False, False

    # Aplicamos las correcciones que el intake haya hecho (renta saneada).
    if intake_view.get("renta_validation_status"):
        datos = dict(datos)
        datos["renta_bruta_min"] = intake_view.get("renta_bruta_min")
        datos["renta_bruta_max"] = intake_view.get("renta_bruta_max")

    datos = normalizar_datos_oferta(datos)
    datos.setdefault("institucion_id", None)  # FK a instituciones; NULL si no aplica
    datos.setdefault("url_bases", None)
    datos.setdefault("modalidad", None)
    datos.setdefault("horas_semanales", None)
    datos.setdefault("jornada", None)
    datos.setdefault("numero_vacantes", None)
    datos.setdefault("email_postulacion", None)
    datos.setdefault("email_consultas", None)
    url_hash = url_a_hash(datos["url_original"])

    # Parte 1.8 — llave difusa para detectar la misma oferta entre portales.
    dedup_hash = clave_dedup_difusa(
        datos.get("institucion_nombre"), datos.get("cargo"),
        datos.get("fecha_cierre"), datos.get("region"),
    )
    # Parte 1.5 — tabla de renta multi-región reconstruida (NULL si no parseable).
    from extraction.renta_regional import parse_renta_regional
    _filas_renta = parse_renta_regional(datos.get("renta_texto") or datos.get("descripcion"))
    renta_regional_json = (
        json.dumps([f.as_dict() for f in _filas_renta], ensure_ascii=False)
        if _filas_renta else None
    )

    # Tipo de renta: la renta informada debe ser BRUTA; si la fuente solo trae
    # LÍQUIDA debe quedar señalado (en renta_tipo y dentro de renta_texto). Se
    # respeta el valor que el scraper ya haya determinado (p.ej. PDI / TC).
    from extraction.renta_tipo import anotar_renta_texto, resolver_tipo_renta
    renta_tipo = datos.get("renta_tipo") or resolver_tipo_renta(
        datos.get("renta_texto"), datos.get("descripcion"), datos.get("cargo")
    )
    datos = dict(datos)
    datos["renta_tipo"] = renta_tipo
    datos["renta_texto"] = anotar_renta_texto(datos.get("renta_texto"), renta_tipo)

    params_extra = {"dedup_hash": dedup_hash, "renta_regional": renta_regional_json}

    try:
        # ¿Existe?
        row = db.execute(
            text("SELECT id, fecha_cierre FROM ofertas WHERE url_hash = :h"),
            {"h": url_hash}
        ).fetchone()

        if row is None:
            # Parte 1.8 — antes de insertar, ¿llegó la misma oferta desde otro
            # portal? Solo fusionamos cuando hay fecha de cierre (señal fuerte
            # que evita unir avisos sin plazo del mismo cargo). Conservamos la
            # fila original (enlace ya establecido) y la refrescamos.
            if datos.get("fecha_cierre"):
                gemela = db.execute(
                    text("SELECT id FROM ofertas WHERE dedup_hash = :d AND url_hash <> :h LIMIT 1"),
                    {"d": dedup_hash, "h": url_hash},
                ).fetchone()
                if gemela is not None:
                    db.execute(text("""
                        UPDATE ofertas SET
                            activa         = TRUE,
                            actualizada_en = NOW(),
                            renta_regional = COALESCE(CAST(:renta_regional AS JSONB), renta_regional)
                        WHERE id = :id
                    """), {"id": gemela.id, "renta_regional": renta_regional_json})
                    db.commit()
                    return False, True

            # INSERT
            db.execute(text("""
                INSERT INTO ofertas (
                    id_externo, fuente_id, url_original, url_hash, dedup_hash,
                    cargo, descripcion,
                    institucion_id, institucion_nombre, sector, area_profesional,
                    tipo_cargo, nivel,
                    region, ciudad,
                    renta_bruta_min, renta_bruta_max, renta_texto, renta_tipo, renta_regional,
                    fecha_publicacion, fecha_cierre,
                    requisitos_texto, url_bases, modalidad, horas_semanales,
                    jornada, numero_vacantes,
                    email_postulacion, email_consultas,
                    activa, es_nueva, detectada_en
                ) VALUES (
                    :id_externo,
                    -- fuente_id defensivo: si la fuente no existe en `fuentes`,
                    -- queda NULL en vez de reventar todo el INSERT con
                    -- ForeignKeyViolation (ofertas_fuente_id_fkey). Mismo patrón
                    -- que institucion_id abajo. Recupera fuentes cuyo id del
                    -- catálogo no está poblado en la tabla `fuentes` de prod.
                    (SELECT id FROM fuentes WHERE id = :fuente_id),
                    :url_original, :url_hash, :dedup_hash,
                    :cargo, :descripcion,
                    (SELECT id FROM instituciones WHERE id = :institucion_id), :institucion_nombre, :sector, :area_profesional,
                    :tipo_cargo, :nivel,
                    :region, :ciudad,
                    :renta_bruta_min, :renta_bruta_max, :renta_texto, :renta_tipo, CAST(:renta_regional AS JSONB),
                    :fecha_publicacion, :fecha_cierre,
                    :requisitos_texto, :url_bases, :modalidad, :horas_semanales,
                    :jornada, :numero_vacantes,
                    :email_postulacion, :email_consultas,
                    TRUE, TRUE, NOW()
                )
            """), {**datos, "url_hash": url_hash, **params_extra})
            db.commit()
            return True, False

        else:
            # UPDATE: solo actualiza campos que podrían cambiar
            db.execute(text("""
                UPDATE ofertas SET
                    id_externo          = COALESCE(NULLIF(:id_externo, ''), id_externo),
                    cargo               = COALESCE(NULLIF(:cargo, ''), cargo),
                    descripcion         = COALESCE(NULLIF(:descripcion, ''), descripcion),
                    institucion_nombre  = COALESCE(NULLIF(:institucion_nombre, ''), institucion_nombre),
                    sector              = COALESCE(NULLIF(:sector, ''), sector),
                    area_profesional    = COALESCE(NULLIF(:area_profesional, ''), area_profesional),
                    tipo_cargo          = COALESCE(NULLIF(:tipo_cargo, ''), tipo_cargo),
                    nivel               = COALESCE(NULLIF(:nivel, ''), nivel),
                    region              = COALESCE(NULLIF(:region, ''), region),
                    ciudad              = COALESCE(NULLIF(:ciudad, ''), ciudad),
                    renta_bruta_min     = COALESCE(:renta_bruta_min, renta_bruta_min),
                    renta_bruta_max     = COALESCE(:renta_bruta_max, renta_bruta_max),
                    renta_texto         = COALESCE(NULLIF(:renta_texto, ''), renta_texto),
                    renta_tipo          = COALESCE(NULLIF(:renta_tipo, ''), renta_tipo),
                    fecha_publicacion   = COALESCE(:fecha_publicacion, fecha_publicacion),
                    fecha_cierre        = COALESCE(:fecha_cierre, fecha_cierre),
                    requisitos_texto    = COALESCE(NULLIF(:requisitos_texto, ''), requisitos_texto),
                    url_bases           = COALESCE(NULLIF(:url_bases, ''), url_bases),
                    modalidad           = COALESCE(NULLIF(:modalidad, ''), modalidad),
                    horas_semanales     = COALESCE(:horas_semanales, horas_semanales),
                    jornada             = COALESCE(NULLIF(:jornada, ''), jornada),
                    numero_vacantes     = COALESCE(:numero_vacantes, numero_vacantes),
                    email_postulacion   = COALESCE(NULLIF(:email_postulacion, ''), email_postulacion),
                    email_consultas     = COALESCE(NULLIF(:email_consultas, ''), email_consultas),
                    dedup_hash          = COALESCE(:dedup_hash, dedup_hash),
                    renta_regional      = COALESCE(CAST(:renta_regional AS JSONB), renta_regional),
                    activa              = TRUE,
                    actualizada_en      = NOW()
                WHERE url_hash = :h
            """), {
                "id_externo": datos.get("id_externo"),
                "cargo": datos.get("cargo"),
                "descripcion": datos.get("descripcion"),
                "institucion_nombre": datos.get("institucion_nombre"),
                "sector": datos.get("sector"),
                "area_profesional": datos.get("area_profesional"),
                "tipo_cargo": datos.get("tipo_cargo"),
                "nivel": datos.get("nivel"),
                "region": datos.get("region"),
                "ciudad": datos.get("ciudad"),
                "renta_bruta_min": datos.get("renta_bruta_min"),
                "renta_bruta_max": datos.get("renta_bruta_max"),
                "renta_texto": datos.get("renta_texto"),
                "renta_tipo": datos.get("renta_tipo"),
                "fecha_publicacion": datos.get("fecha_publicacion"),
                "fecha_cierre": datos.get("fecha_cierre"),
                "requisitos_texto": datos.get("requisitos_texto"),
                "url_bases": datos.get("url_bases"),
                "modalidad": datos.get("modalidad"),
                "horas_semanales": datos.get("horas_semanales"),
                "jornada": datos.get("jornada"),
                "numero_vacantes": datos.get("numero_vacantes"),
                "email_postulacion": datos.get("email_postulacion"),
                "email_consultas": datos.get("email_consultas"),
                "dedup_hash": dedup_hash,
                "renta_regional": renta_regional_json,
                "h": url_hash
            })
            db.commit()
            return False, True
    except Exception:
        db.rollback()
        raise


def marcar_ofertas_cerradas(db: Session, fuente_id: int, urls_activas: list[str]) -> int:
    """
    Marca como inactivas las ofertas de una fuente que ya no aparecen en el listado.
    Además registra la primera vez que el scraper dejó de verlas en
    `fecha_cierre_detectada` (solo si aún está NULL, para no pisarla si
    reaparecen y vuelven a desaparecer).
    Retorna la cantidad de ofertas cerradas.

    Defensa: si la sesión llega aquí con la transacción en estado abortado
    (visto en logs/empleos_publicos.log: psycopg2.errors.InFailedSqlTransaction
    sobre esta misma query), hacemos rollback antes de ejecutar — el rollback
    es no-op en sesión sana y rescata sesiones rotas. Sin esto, todo el run
    aborta con la cleanup en cascada.
    """
    if not urls_activas:
        return 0

    hashes_activos = [url_a_hash(u) for u in urls_activas]
    # Dominio común de las URLs vistas: el cierre se acota a ofertas de ESE
    # dominio para no tocar ofertas de la misma institución que vinieron de otra
    # fuente (p.ej. la misma muni publica en su sitio Y en empleospublicos.cl).
    from urllib.parse import urlparse as _urlparse
    _hosts = {
        _urlparse(u).netloc.lower().removeprefix("www.")
        for u in urls_activas if u
    }
    _dom = next(iter(_hosts)) if len(_hosts) == 1 else ""

    # Rollback defensivo previo: en sesión sana es no-op; en sesión con
    # transacción abortada por un upsert previo, evita que el UPDATE
    # de cleanup haga cascada con InFailedSqlTransaction.
    try:
        db.rollback()
    except Exception:
        pass

    try:
        result = db.execute(text("""
            UPDATE ofertas
            SET activa                 = FALSE,
                actualizada_en         = NOW(),
                fecha_cierre_detectada = COALESCE(fecha_cierre_detectada, NOW())
            WHERE institucion_id = :fid
              AND activa = TRUE
              AND url_hash != ALL(:hashes)
              AND (:dom = '' OR url_oferta ILIKE :domlike)
        """), {"fid": fuente_id, "hashes": hashes_activos, "dom": _dom, "domlike": f"%{_dom}%"})
        db.commit()
        return result.rowcount
    except Exception:
        db.rollback()
        raise


def registrar_log(
    db: Session,
    fuente_id: int,
    estado: str,
    ofertas_nuevas: int = 0,
    ofertas_actualizadas: int = 0,
    ofertas_cerradas: int = 0,
    paginas: int = 0,
    duracion: float = 0,
    error: str = None
) -> None:
    """Registra el resultado de una ejecución del scraper."""
    try:
        db.execute(text("""
        INSERT INTO logs_scraping (
            fuente_id, finalizado_en, duracion_seg, estado,
            ofertas_nuevas, ofertas_actualizadas, ofertas_cerradas,
            paginas_visitadas, error_mensaje
        ) VALUES (
            :fid, NOW(), :dur, :estado,
            :nuevas, :actualizadas, :cerradas,
            :paginas, :error
        )
    """), {
        "fid": fuente_id, "dur": round(duracion, 2),
        "estado": estado, "nuevas": ofertas_nuevas,
        "actualizadas": ofertas_actualizadas, "cerradas": ofertas_cerradas,
        "paginas": paginas, "error": error
    })

        # Actualizar última ejecución en tabla fuentes
        db.execute(text("""
            UPDATE fuentes SET
                ultima_ejecucion = NOW(),
                ultima_exitosa   = CASE WHEN :estado = 'OK' THEN NOW() ELSE ultima_exitosa END,
                total_ofertas    = (SELECT COUNT(*) FROM ofertas WHERE fuente_id = :fid AND activa = TRUE)
            WHERE id = :fid
        """), {"estado": estado, "fid": fuente_id})
        db.commit()
    except Exception as exc:
        # No es fatal: el scraping y el upsert ya ocurrieron. El registro de log
        # es auxiliar y puede fallar por causas benignas en algunos despliegues
        # (tabla logs_scraping ausente, o FK fuente_id→fuentes sin fila para los
        # scrapers de nuevo estándar que pasan institucion_id). Se hace rollback
        # para dejar la sesión limpia y se traga el error con un warning conciso,
        # en vez de propagar un traceback que aborta/ensucia toda la corrida.
        db.rollback()
        logger.warning("registrar_log omitido (%s): %s",
                       type(exc).__name__, str(exc).splitlines()[0][:160])


def normalizar_region(texto: str) -> str:
    """Normaliza nombres de regiones al estándar del sistema."""
    MAPA = {
        "metropolitana":        "Metropolitana de Santiago",
        "rm":                   "Metropolitana de Santiago",
        "santiago":             "Metropolitana de Santiago",
        "arica":                "Arica y Parinacota",
        "tarapaca":             "Tarapacá",
        "antofagasta":          "Antofagasta",
        "atacama":              "Atacama",
        "coquimbo":             "Coquimbo",
        "valparaiso":           "Valparaíso",
        "ohiggins":             "O'Higgins",
        "libertador":           "O'Higgins",
        "maule":                "Maule",
        "nuble":                "Ñuble",
        "biobio":               "Biobío",
        "araucania":            "La Araucanía",
        "los rios":             "Los Ríos",
        "los lagos":            "Los Lagos",
        "aysen":                "Aysén",
        "magallanes":           "Magallanes",
    }
    if not texto:
        return None
    key = texto.lower().strip()
    key = key.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
    for k, v in MAPA.items():
        if k in key:
            return v
    return texto.strip().title()


def normalizar_tipo_cargo(texto: str) -> str:
    """Normaliza el tipo de vínculo laboral."""
    if not texto:
        return None
    t = texto.lower()
    if "planta"    in t: return "Planta"
    if "contrata"  in t: return "Contrata"
    if "honorario" in t: return "Honorarios"
    if "adp"       in t or "alta direcci" in t: return "ADP"
    if "codigo del trabajo" in t or "código del trabajo" in t: return "Código del Trabajo"
    return texto.strip().title()


def normalizar_area(cargo: str) -> str:
    """Infiere el área profesional desde el nombre del cargo."""
    c = cargo.lower()
    if any(w in c for w in ["abogad", "juridic", "jurídic", "legal", "fiscal"]): return "Derecho"
    if any(w in c for w in ["médic", "medic", "enfermer", "kinesiol", "matron", "salud", "psiquiatr", "farmac"]): return "Salud"
    if any(w in c for w in ["ingenier", "técnic", "tecnolog"]): return "Ingeniería"
    if any(w in c for w in ["trabajador social", "asistente social", "social"]): return "Ciencias Sociales"
    if any(w in c for w in ["psicolog"]): return "Psicología"
    if any(w in c for w in ["contador", "contabilidad", "auditor", "finanz"]): return "Finanzas"
    if any(w in c for w in ["econom"]): return "Economía"
    if any(w in c for w in ["sistem", "informát", "computaci", "software", "datos", "ti ", " ti,", "tecnología inform"]): return "TI"
    if any(w in c for w in ["architect", "diseñ"]): return "Arquitectura/Diseño"
    if any(w in c for w in ["educad", "docent", "profesor", "pedagog"]): return "Educación"
    if any(w in c for w in ["comunicacion", "comunicación", "periodist", "relacione"]): return "Comunicaciones"
    if any(w in c for w in ["administr", "gestión", "gestión de personas", "rrhh", "recursos humanos"]): return "Administración"
    if any(w in c for w in ["agrón", "agron", "veterinar", "forestal", "agropecuar"]): return "Agropecuario/Forestal"
    if any(w in c for w in ["geolog", "geógraf", "ambiental", "ambient"]): return "Medioambiente"
    if any(w in c for w in ["fiscaliz", "inspector", "inspector"]): return "Fiscalización"
    return "Administración"  # categoría por defecto
