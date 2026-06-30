"""
ContrataOPlanta.cl — Carga manual de ofertas desde publicaciones de LinkedIn u
otros canales informales.

Recibe el texto de la publicación (copiar/pegar de LinkedIn) y opcionalmente
un PDF de perfil de cargo. Auto-detecta: institución, cargo, fecha de cierre,
URL de postulación, modalidad, renta, requisitos, etc.

Uso:
    # Solo texto (interactivo)
    python scrapers/carga_manual.py

    # Con PDF adjunto
    python scrapers/carga_manual.py --pdf perfil_cargo.pdf

    # No interactivo: texto desde archivo
    python scrapers/carga_manual.py --texto publicacion.txt --pdf perfil.pdf

    # Dry-run (no persiste, solo muestra lo detectado)
    python scrapers/carga_manual.py --dry-run

    # Exportar a JSON en vez de BD
    python scrapers/carga_manual.py --dry-run --export salida.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

# ── Integración con el proyecto ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from config import config
    from db.database import (
        SessionLocal,
        generar_id_estable,
        limpiar_texto,
        normalizar_area,
        normalizar_region,
        normalizar_tipo_cargo,
        registrar_log,
        upsert_oferta,
    )
    STANDALONE = False
except (ImportError, RuntimeError):
    STANDALONE = True
    SessionLocal = None  # type: ignore[assignment]

    class _Cfg:
        LOG_DIR = "logs"
        LOG_LEVEL = "INFO"
    config = _Cfg()  # type: ignore[assignment]

    def generar_id_estable(*partes: Any, largo: int = 20) -> str:  # type: ignore[misc]
        base = "||".join(str(p) for p in partes if p).lower()
        return hashlib.sha1(base.encode()).hexdigest()[:largo]

    def limpiar_texto(t: str | None) -> str:  # type: ignore[misc]
        return re.sub(r"\s+", " ", t or "").strip()

    def normalizar_area(cargo: str) -> str | None:  # type: ignore[misc]
        return None

    def normalizar_region(r: str) -> str | None:  # type: ignore[misc]
        return (r or "").strip() or None

    def normalizar_tipo_cargo(t: str) -> str | None:  # type: ignore[misc]
        return (t or "").strip().title() or None

    def registrar_log(*a: Any, **k: Any) -> None:  # type: ignore[misc]
        return None

    def upsert_oferta(*a: Any, **k: Any) -> tuple[bool, bool]:  # type: ignore[misc]
        return False, False


logger = logging.getLogger("carga_manual")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
    "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
    "octubre": 10, "noviembre": 11, "diciembre": 12,
}

REPO_PATH = Path(__file__).resolve().parent.parent / "repositorio_instituciones_publicas_chile.json"


# ══════════════════════════════════════════════════════════════════════════════
# Catálogo de instituciones
# ══════════════════════════════════════════════════════════════════════════════
def _cargar_catalogo() -> list[dict]:
    if not REPO_PATH.exists():
        return []
    data = json.loads(REPO_PATH.read_text(encoding="utf-8-sig"))
    return data.get("instituciones") if isinstance(data, dict) else data


def buscar_institucion(texto: str, catalogo: list[dict] | None = None) -> dict | None:
    """Busca la institución en el catálogo por coincidencia de nombre."""
    if catalogo is None:
        catalogo = _cargar_catalogo()
    texto_norm = texto.lower().strip()
    # búsqueda exacta
    for inst in catalogo:
        if inst["nombre"].lower() == texto_norm:
            return inst
    # búsqueda parcial
    for inst in catalogo:
        if texto_norm in inst["nombre"].lower() or inst["nombre"].lower() in texto_norm:
            return inst
    # búsqueda por sigla
    for inst in catalogo:
        if inst.get("sigla", "").lower() == texto_norm:
            return inst
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Parseo de fecha en español
# ══════════════════════════════════════════════════════════════════════════════
def _parsear_fecha(texto: str) -> date | None:
    if not texto:
        return None
    # "27 de junio del 2026" / "27 de JUNIO de 2026"
    m = re.search(
        r"(\d{1,2})\s+(?:de\s+)?(\w+)\s+(?:de(?:l)?\s+)?(\d{4})",
        texto, re.I,
    )
    if m:
        mes = MESES.get(m.group(2).lower())
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                pass
    # dd/mm/yyyy o dd-mm-yyyy
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", texto)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Extracción desde texto de publicación (LinkedIn, RRSS, email)
# ══════════════════════════════════════════════════════════════════════════════
def extraer_de_publicacion(texto: str) -> dict[str, Any]:
    """Extrae campos de una publicación de LinkedIn o similar."""
    resultado: dict[str, Any] = {}

    # Institución
    # Patrones: "Municipalidad de X requiere", "X necesita contratar",
    # "en [Institución]", nombre propio al inicio
    m = re.search(
        r"((?:Municipalidad|Corporación|Servicio|Hospital|Gobierno Regional|"
        r"Ministerio|Subsecretaría|Superintendencia|Dirección|SEREMI|"
        r"Contraloría|Defensoría|Fiscalía|Junta Nacional|Instituto|"
        r"Universidad|Fondo de Solidaridad|Secretaría|Consejo|Tribunal|"
        r"Fundación|Empresa|Centro)\s+(?:de\s+|del\s+|Municipal\s+de\s+)?"
        r"[\w\sáéíóúñÁÉÍÓÚÑ\-']+?)(?:\s+(?:requiere|necesita|busca|"
        r"convoca|invita|llama|informa|se encuentra))",
        texto, re.I,
    )
    if m:
        resultado["institucion_nombre"] = limpiar_texto(m.group(1))
    else:
        # fallback: primera línea que contenga un nombre institucional
        for linea in texto.split("\n")[:5]:
            for palabra in ("Municipalidad", "Corporación", "Servicio",
                            "Hospital", "Ministerio", "Universidad",
                            "Gobierno Regional", "Superintendencia"):
                if palabra.lower() in linea.lower():
                    resultado["institucion_nombre"] = limpiar_texto(linea.split(".")[0].split("–")[0].strip())
                    break
            if "institucion_nombre" in resultado:
                break

    # Cargo — entre comillas o después de "cargo de"
    m = re.search(
        r'(?:cargo\s+(?:de\s+)?)?"([^"]{5,200})"',
        texto, re.I,
    )
    if m:
        resultado["cargo"] = limpiar_texto(m.group(1))
    else:
        m = re.search(
            r'(?:contratar|cargo\s+de|puesto\s+de|perfil\s+de)\s+[""«]?'
            r'([^""»\n]{5,200}?)[""»]?\s*(?:\(|–|-|$)',
            texto, re.I,
        )
        if m:
            resultado["cargo"] = limpiar_texto(m.group(1))

    # Fecha de cierre
    m = re.search(
        r"(?:postulaciones?\s+(?:abiertas?\s+)?hasta|plazo\s+(?:hasta|de\s+postulación)"
        r"|cierre\s+(?:de\s+)?postulación|fecha\s+(?:de\s+)?cierre|hasta\s+el)\s+"
        r"(?:el\s+)?(\d{1,2}\s+(?:de\s+)?\w+\s+(?:de(?:l)?\s+)?\d{4})",
        texto, re.I,
    )
    if m:
        resultado["fecha_cierre"] = _parsear_fecha(m.group(1))

    # URL de postulación
    urls = re.findall(r"https?://\S+", texto)
    if urls:
        resultado["url_postulacion"] = urls[0].rstrip(".,;:)")

    # Dirección/unidad (entre paréntesis después del cargo)
    m = re.search(r"\(([^)]{5,100})\)", texto)
    if m:
        contenido = m.group(1)
        if any(w in contenido.lower() for w in ("dirección", "departamento",
               "unidad", "programa", "división", "sección", "secretaría")):
            resultado["dependencia"] = limpiar_texto(contenido)

    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# Extracción desde PDF de perfil de cargo
# ══════════════════════════════════════════════════════════════════════════════
def extraer_de_pdf(ruta: str | Path) -> dict[str, Any]:
    """Extrae campos de un PDF de perfil de cargo."""
    try:
        import pdfplumber
    except ImportError:
        try:
            from PyPDF2 import PdfReader
            reader = PdfReader(str(ruta))
            texto = "\n".join(page.extract_text() or "" for page in reader.pages)
        except ImportError:
            logger.warning("Sin pdfplumber ni PyPDF2: no se puede leer el PDF")
            return {}
    else:
        with pdfplumber.open(str(ruta)) as pdf:
            texto = "\n".join(page.extract_text() or "" for page in pdf.pages)

    if not texto.strip():
        return {}

    resultado: dict[str, Any] = {}

    # Tablas clave-valor del perfil de cargo
    patrones_kv = {
        "cargo": r"(?:Nombre\s+del\s+cargo|Cargo)\s*[:\|]\s*(.+)",
        "dependencia": r"(?:Dependencia|Unidad/Programa)\s*[:\|]\s*(.+)",
        "modalidad": r"(?:Modalidad|Tipo\s+de\s+contrato|Calidad\s+jurídica)\s*[:\|]\s*(.+)",
        "horario": r"(?:Horario|Jornada)\s*[:\|]\s*(.+)",
        "vacantes": r"(?:N[°º]\s*(?:de\s+)?vacantes?|Vacantes?)\s*[:\|]\s*(\d+)",
        "renta_texto": r"(?:Remuneración\s+(?:bruta|líquida)|Renta|Sueldo)\s*[:\|]\s*(.+)",
        "inicio_funciones": r"(?:Inicio\s+de\s+funciones|Fecha\s+de\s+inicio)\s*[:\|]\s*(.+)",
    }
    for campo, patron in patrones_kv.items():
        m = re.search(patron, texto, re.I)
        if m:
            resultado[campo] = limpiar_texto(m.group(1))

    # Requisitos — sección completa
    m = re.search(
        r"(?:REQUISITOS\s+(?:MÍNIMOS|ESPECÍFICOS)|V\.\-?\s*REQUISITOS)"
        r"\s*(?:PARA\s+EL\s+CARGO)?\s*\n([\s\S]*?)(?=\n\s*(?:[IVX]+\.\-|COMPETENCIAS|FUNCIONES|DOCUMENTOS|$))",
        texto, re.I,
    )
    if m:
        resultado["requisitos_texto"] = limpiar_texto(m.group(1))[:2000]

    # Descripción/funciones
    m = re.search(
        r"(?:FUNCIONES\s+PRINCIPALES|OBJETIVOS\s+DEL\s+CARGO|III\.\-?\s*OBJETIVOS)"
        r"\s*\n([\s\S]*?)(?=\n\s*(?:[IVX]+\.\-|REQUISITOS|COMPETENCIAS|DOCUMENTOS|$))",
        texto, re.I,
    )
    if m:
        resultado["descripcion"] = limpiar_texto(m.group(1))[:2000]

    # Renta numérica
    if resultado.get("renta_texto"):
        m_renta = re.search(r"\$\s*([\d.]+(?:\.\d{3})*)", resultado["renta_texto"])
        if m_renta:
            try:
                resultado["renta_bruta"] = int(m_renta.group(1).replace(".", ""))
            except ValueError:
                pass

    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# Construcción de la oferta normalizada
# ══════════════════════════════════════════════════════════════════════════════
def construir_oferta(pub: dict, pdf: dict, catalogo: list[dict]) -> dict[str, Any]:
    """Combina datos de publicación y PDF en una oferta lista para BD."""

    # Prioridad: PDF tiene datos más estructurados, publicación tiene fecha/URL
    cargo = pdf.get("cargo") or pub.get("cargo") or ""
    inst_nombre = pub.get("institucion_nombre") or ""

    # Buscar en catálogo
    inst = buscar_institucion(inst_nombre, catalogo)
    fuente_id = inst["id"] if inst else None
    region = inst.get("region") if inst else None
    sector = inst.get("sector") if inst else "Municipal"

    # URL: preferir la de postulación, si no el sitio de empleo de la institución
    url = pub.get("url_postulacion") or (inst.get("url_empleo") if inst else None) or ""

    # Renta
    renta_bruta = pdf.get("renta_bruta")
    renta_texto = pdf.get("renta_texto")

    # Modalidad
    modalidad = normalizar_tipo_cargo(pdf.get("modalidad") or "")

    # Fecha cierre
    fecha_cierre = pub.get("fecha_cierre")

    oferta = {
        "id_externo": generar_id_estable(fuente_id or inst_nombre, cargo, url),
        "fuente_id": fuente_id,
        "institucion_nombre": inst["nombre"] if inst else inst_nombre,
        "institucion_id": fuente_id,
        "cargo": cargo[:500],
        "sector": sector,
        "area_profesional": normalizar_area(cargo),
        "tipo_cargo": modalidad or "Honorarios",
        "nivel": _inferir_nivel(cargo),
        "region": normalizar_region(region) if region else None,
        "ciudad": None,
        "renta_bruta_min": renta_bruta,
        "renta_bruta_max": renta_bruta,
        "renta_texto": renta_texto,
        "modalidad": modalidad,
        "horas_semanales": _extraer_horas(pdf.get("horario") or ""),
        "fecha_publicacion": date.today(),
        "fecha_cierre": fecha_cierre,
        "url_original": url,
        "url_bases": None,
        "descripcion": pdf.get("descripcion"),
        "requisitos_texto": pdf.get("requisitos_texto"),
        "vacantes": _safe_int(pdf.get("vacantes")),
        "fuente_tipo": "carga_manual",
    }
    return oferta


def _inferir_nivel(cargo: str) -> str:
    c = (cargo or "").lower()
    if any(w in c for w in ("director", "jefe", "jefa", "coordinador", "gerente")):
        return "Directivo"
    if any(w in c for w in ("profesional", "ingenier", "abogad", "médic",
                            "psicólog", "analista", "psiquiatr")):
        return "Profesional"
    if any(w in c for w in ("técnic", "administrativ", "auxiliar", "secretari")):
        return "Técnico"
    return "Profesional"


def _extraer_horas(texto: str) -> int | None:
    m = re.search(r"(\d{2})\s*horas?\s*semanal", texto, re.I)
    return int(m.group(1)) if m else None


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Presentación y confirmación
# ══════════════════════════════════════════════════════════════════════════════
def mostrar_oferta(oferta: dict, editable: bool = True) -> dict:
    """Muestra la oferta detectada y permite corregir campos."""
    campos_principales = [
        ("institucion_nombre", "Institución"),
        ("cargo", "Cargo"),
        ("tipo_cargo", "Modalidad"),
        ("region", "Región"),
        ("renta_texto", "Renta"),
        ("fecha_cierre", "Fecha cierre"),
        ("url_original", "URL postulación"),
        ("descripcion", "Descripción"),
        ("requisitos_texto", "Requisitos"),
        ("horas_semanales", "Horas/semana"),
        ("vacantes", "Vacantes"),
    ]

    print("\n" + "=" * 60)
    print("  OFERTA DETECTADA")
    print("=" * 60)
    for i, (campo, label) in enumerate(campos_principales, 1):
        valor = oferta.get(campo)
        if isinstance(valor, str) and len(valor) > 80:
            valor = valor[:77] + "..."
        print(f"  {i:2}. {label:20} {valor or '—'}")
    print("=" * 60)

    if not editable:
        return oferta

    print("\nPara corregir, escribe el número y el nuevo valor (ej: '2 Psicólogo Clínico')")
    print("Enter vacío para confirmar, 'q' para cancelar.\n")

    while True:
        linea = input("> ").strip()
        if not linea:
            break
        if linea.lower() == "q":
            print("Cancelado.")
            sys.exit(0)
        m = re.match(r"(\d+)\s+(.+)", linea)
        if m:
            idx = int(m.group(1)) - 1
            if 0 <= idx < len(campos_principales):
                campo = campos_principales[idx][0]
                valor = m.group(2).strip()
                if campo == "fecha_cierre":
                    parsed = _parsear_fecha(valor)
                    if parsed:
                        oferta[campo] = parsed
                        print(f"  ✓ {campos_principales[idx][1]} → {parsed}")
                    else:
                        print(f"  ✗ No se pudo parsear la fecha '{valor}'")
                elif campo in ("horas_semanales", "vacantes"):
                    oferta[campo] = _safe_int(valor)
                    print(f"  ✓ {campos_principales[idx][1]} → {oferta[campo]}")
                else:
                    oferta[campo] = valor
                    print(f"  ✓ {campos_principales[idx][1]} → {valor}")
            else:
                print(f"  ✗ Número fuera de rango (1-{len(campos_principales)})")
        else:
            print("  Formato: número valor (ej: '2 Nuevo cargo')")

    return oferta


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def ejecutar(texto: str, pdf_path: str | None = None,
             dry_run: bool = False, export: str | None = None,
             no_interactivo: bool = False) -> dict:
    """Punto de entrada principal."""
    catalogo = _cargar_catalogo()

    # 1. Extraer de publicación
    pub = extraer_de_publicacion(texto)
    logger.info("Publicación: %s", {k: v for k, v in pub.items() if v})

    # 2. Extraer de PDF (si existe)
    pdf = {}
    if pdf_path:
        pdf = extraer_de_pdf(pdf_path)
        logger.info("PDF: %s", {k: (v[:60] + "...") if isinstance(v, str) and len(v) > 60 else v
                                 for k, v in pdf.items() if v})

    # 3. Construir oferta
    oferta = construir_oferta(pub, pdf, catalogo)

    # 4. Mostrar y confirmar
    oferta = mostrar_oferta(oferta, editable=not no_interactivo)

    # 5. Exportar o persistir
    if export:
        serializable = dict(oferta)
        for k in ("fecha_publicacion", "fecha_cierre"):
            if isinstance(serializable.get(k), date):
                serializable[k] = serializable[k].isoformat()
        Path(export).write_text(
            json.dumps(serializable, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nExportado a {export}")
        return {"status": "exportado", "archivo": export}

    if dry_run:
        print("\n[dry-run] No se persistió nada.")
        return {"status": "dry_run", "oferta": oferta}

    if STANDALONE:
        logger.warning("Sin módulos del proyecto: no se puede persistir.")
        return {"status": "standalone", "oferta": oferta}

    # Persistir
    db = SessionLocal()
    try:
        nueva, actualizada = upsert_oferta(db, oferta)
        if nueva:
            print("\n✓ Oferta INSERTADA en BD.")
        elif actualizada:
            print("\n✓ Oferta ACTUALIZADA en BD.")
        else:
            print("\n— Oferta ya existente o descartada por intake.")
        registrar_log(db, oferta.get("fuente_id") or 0, "OK",
                      ofertas_nuevas=1 if nueva else 0,
                      ofertas_actualizadas=1 if actualizada else 0)
        return {"status": "OK", "nueva": nueva, "actualizada": actualizada}
    except Exception as exc:
        db.rollback()
        logger.exception("Error al persistir: %s", exc)
        return {"status": "ERROR", "error": str(exc)}
    finally:
        db.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Carga manual de ofertas desde publicaciones de LinkedIn/RRSS")
    p.add_argument("--texto", type=str, default=None,
                   help="Archivo con el texto de la publicación (si no, se pide por stdin)")
    p.add_argument("--pdf", type=str, default=None,
                   help="PDF de perfil de cargo adjunto")
    p.add_argument("--dry-run", action="store_true",
                   help="No persistir, solo mostrar lo detectado")
    p.add_argument("--export", type=str, default=None,
                   help="Exportar oferta a archivo JSON en vez de BD")
    p.add_argument("--no-interactivo", action="store_true",
                   help="No pedir confirmación (para scripts)")
    args = p.parse_args()

    if args.texto:
        texto = Path(args.texto).read_text(encoding="utf-8")
    else:
        print("Pega el texto de la publicación (LinkedIn, email, etc.).")
        print("Termina con una línea vacía:\n")
        lineas = []
        while True:
            try:
                linea = input()
            except EOFError:
                break
            if linea == "":
                break
            lineas.append(linea)
        texto = "\n".join(lineas)

    if not texto.strip():
        print("Error: texto vacío.")
        sys.exit(1)

    ejecutar(
        texto=texto,
        pdf_path=args.pdf,
        dry_run=args.dry_run,
        export=args.export,
        no_interactivo=args.no_interactivo,
    )
