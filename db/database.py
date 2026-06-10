"""
EmpleoEstado.cl — Capa de base de datos
Manejo de conexiones, modelos y operaciones de escritura.
"""

import hashlib
import logging
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


def truncar_texto(valor, max_len: int) -> str | None:
    """Recorta strings a la longitud máxima definida por el schema."""
    if valor is None:
        return None
    texto = limpiar_texto(valor)
    if not texto:
        return None
    return texto[:max_len]


def normalizar_datos_oferta(datos: dict) -> dict:
    """Ajusta largos de campos para evitar errores de VARCHAR."""
    normalizados = dict(datos)
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
    url_hash = url_a_hash(datos["url_original"])
    try:
        # ¿Existe?
        row = db.execute(
            text("SELECT id, fecha_cierre FROM ofertas WHERE url_hash = :h"),
            {"h": url_hash}
        ).fetchone()

        if row is None:
            # INSERT
            db.execute(text("""
                INSERT INTO ofertas (
                    id_externo, fuente_id, url_original, url_hash,
                    cargo, descripcion,
                    institucion_nombre, sector, area_profesional,
                    tipo_cargo, nivel,
                    region, ciudad,
                    renta_bruta_min, renta_bruta_max, renta_texto,
                    fecha_publicacion, fecha_cierre,
                    requisitos_texto,
                    activa, es_nueva, detectada_en
                ) VALUES (
                    :id_externo, :fuente_id, :url_original, :url_hash,
                    :cargo, :descripcion,
                    :institucion_nombre, :sector, :area_profesional,
                    :tipo_cargo, :nivel,
                    :region, :ciudad,
                    :renta_bruta_min, :renta_bruta_max, :renta_texto,
                    :fecha_publicacion, :fecha_cierre,
                    :requisitos_texto,
                    TRUE, TRUE, NOW()
                )
            """), {**datos, "url_hash": url_hash})
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
                    fecha_publicacion   = COALESCE(:fecha_publicacion, fecha_publicacion),
                    fecha_cierre        = COALESCE(:fecha_cierre, fecha_cierre),
                    requisitos_texto    = COALESCE(NULLIF(:requisitos_texto, ''), requisitos_texto),
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
                "fecha_publicacion": datos.get("fecha_publicacion"),
                "fecha_cierre": datos.get("fecha_cierre"),
                "requisitos_texto": datos.get("requisitos_texto"),
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
            WHERE fuente_id = :fid
              AND activa = TRUE
              AND url_hash != ALL(:hashes)
        """), {"fid": fuente_id, "hashes": hashes_activos})
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
    except Exception:
        db.rollback()
        raise


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
