"""Pipeline de RRSS: avisos destacados → imágenes + copy → cola de aprobación.

Flujo (semi-automático, modo "buzón de salida"):

  1. Consulta los avisos marcados ``nivel='destacado'`` que siguen vigentes.
  2. Por cada uno renderiza la imagen para cada plataforma activa
     (Instagram = square 1080x1080, LinkedIn = horizontal 1200x630),
     reutilizando el motor existente ``api.services.og_image.render_offer_card``.
  3. Redacta el copy por plataforma con enlace UTM y hashtags
     (``social.copy_writer``).
  4. Escribe todo en ``social/cola/<fecha>/`` con un ``manifest.json`` y
     copia la UI de revisión ``revisar.html``. Nada se publica solo: tú
     apruebas en la UI y publicas.

Uso:

    python -m social.generar_cola                 # corre contra la BD (destacados vigentes)
    python -m social.generar_cola --max 5
    python -m social.generar_cola --demo          # sin BD: datos de ejemplo (para probar el visual)
    python -m social.generar_cola --solo instagram

Pensado para correr en una tarea programada (systemd timer / cron) un par de
veces por semana. Ver social/README.md.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

# Permite ejecutar como script suelto (python social/generar_cola.py) además
# de como módulo (python -m social.generar_cola).
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from social import config as cfg
    from social import copy_writer
else:
    from . import config as cfg
    from . import copy_writer

logger = logging.getLogger("social.generar_cola")


# ── Datos de ejemplo (modo --demo, sin BD) ───────────────────────────────
_DEMO: list[dict[str, Any]] = [
    {
        "id": 1001, "cargo": "Profesional de Apoyo en Gestión de Personas",
        "institucion": "Servicio de Salud Metropolitano Sur", "sigla": "SSMS",
        "region": "Metropolitana", "ciudad": "Santiago", "tipo_contrato": "contrata",
        "renta_bruta_min": 1200000, "renta_bruta_max": 1500000, "grado_eus": "12",
        "fecha_cierre": "2026-06-30", "url_oferta": "https://empleospublicos.cl/x1",
    },
    {
        "id": 1002, "cargo": "Ingeniero/a en Prevención de Riesgos",
        "institucion": "Municipalidad de Valparaíso", "sigla": "MV",
        "region": "Valparaíso", "ciudad": "Valparaíso", "tipo_contrato": "planta",
        "renta_bruta_min": 1650000, "renta_bruta_max": None, "grado_eus": "10",
        "fecha_cierre": "2026-06-25", "url_oferta": "https://empleospublicos.cl/x2",
    },
    {
        "id": 1003, "cargo": "Analista de Presupuesto",
        "institucion": "Subsecretaría de Hacienda", "sigla": "SH",
        "region": "Metropolitana", "ciudad": "Santiago", "tipo_contrato": "contrata",
        "renta_bruta_min": 2100000, "renta_bruta_max": 2400000, "grado_eus": "8",
        "fecha_cierre": "2026-07-10", "url_oferta": "https://empleospublicos.cl/x3",
    },
    {
        "id": 1004, "cargo": "Trabajador/a Social",
        "institucion": "Servicio Nacional de la Mujer y la Equidad de Género",
        "sigla": "SernamEG", "region": "Biobío", "ciudad": "Concepción",
        "tipo_contrato": "honorarios", "renta_bruta_min": 980000, "renta_bruta_max": None,
        "grado_eus": None, "fecha_cierre": "2026-06-24", "url_oferta": "https://empleospublicos.cl/x4",
    },
]


def _dias_restantes(fecha_cierre: Any) -> int | None:
    if not fecha_cierre:
        return None
    fc = fecha_cierre
    if isinstance(fc, str):
        try:
            fc = datetime.fromisoformat(fc).date()
        except ValueError:
            return None
    if isinstance(fc, datetime):
        fc = fc.date()
    if isinstance(fc, date):
        return (fc - date.today()).days
    return None


def cargar_destacados(max_n: int, demo: bool) -> list[dict[str, Any]]:
    """Devuelve avisos destacados vigentes (o los de ejemplo en modo demo)."""
    if demo:
        ofertas = [dict(o) for o in _DEMO[:max_n]]
        for o in ofertas:
            o["dias_restantes"] = _dias_restantes(o.get("fecha_cierre"))
        return ofertas

    # Modo real: consulta la BD reutilizando el SQL canónico del API.
    import psycopg2
    import psycopg2.extras
    from db.config import DB_CONFIG
    from api.services.sql import ofertas_select_sql, ofertas_base_sql
    from api.services.seo import serialize_offer

    sql = f"""
    WITH base AS (
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        WHERE COALESCE(o.nivel, '') = 'destacado'
          AND o.activa IS TRUE
          AND (o.fecha_cierre IS NULL OR o.fecha_cierre >= CURRENT_DATE)
          AND (
                %(ventana)s IS NULL
                OR o.fecha_cierre IS NULL
                OR o.fecha_cierre <= CURRENT_DATE + (%(ventana)s || ' days')::interval
              )
    )
    SELECT * FROM base
    ORDER BY fecha_cierre ASC NULLS LAST
    LIMIT %(limite)s
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {
                "ventana": cfg.VENTANA_CIERRE_DIAS,
                "limite": max_n,
            })
            filas = cur.fetchall()
    finally:
        conn.close()
    return [serialize_offer(dict(f)) for f in filas]


def _render(of: dict[str, Any], formato: str):
    from api.services.og_image import render_offer_card
    return render_offer_card(of, fmt=formato)  # type: ignore[arg-type]


def generar(max_n: int, demo: bool, solo: str | None) -> Path:
    plataformas = {
        k: v for k, v in cfg.PLATAFORMAS.items()
        if not solo or k == solo
    }
    if not plataformas:
        raise SystemExit(f"Plataforma desconocida: {solo}")

    ofertas = cargar_destacados(max_n, demo)
    logger.info("Avisos destacados a procesar: %d", len(ofertas))
    if not ofertas:
        logger.warning("No hay avisos destacados vigentes. Marca avisos como 'destacado' en el panel admin.")

    sello = date.today().isoformat()
    dest = cfg.COLA_DIR / sello
    dest.mkdir(parents=True, exist_ok=True)

    posts: list[dict[str, Any]] = []
    for of in ofertas:
        oid = of.get("id")
        carpeta_oferta = dest / f"oferta-{oid}"
        carpeta_oferta.mkdir(exist_ok=True)
        entrada: dict[str, Any] = {
            "oferta_id": oid,
            "cargo": of.get("cargo"),
            "institucion": of.get("institucion"),
            "fecha_cierre": str(of.get("fecha_cierre") or ""),
            "dias_restantes": of.get("dias_restantes"),
            "url_oferta": of.get("url_oferta"),
            "plataformas": {},
        }
        for plat, meta in plataformas.items():
            png = _render(of, meta["formato"])
            img_rel = f"oferta-{oid}/{plat}.png"
            (dest / img_rel).write_bytes(png)
            entrada["plataformas"][plat] = {
                "etiqueta": meta["etiqueta"],
                "formato": meta["formato"],
                "imagen": img_rel,
                "copy": copy_writer.construir_copy(of, plat),
                "enlace": copy_writer.enlace_utm(of, plat),
                "estado": "pendiente",  # pendiente | aprobado | rechazado
            }
        posts.append(entrada)

    manifest = {
        "generado_en": datetime.now().isoformat(timespec="seconds"),
        "campania": cfg.UTM_CAMPAIGN,
        "total": len(posts),
        "demo": demo,
        "posts": posts,
    }
    manifest_json = json.dumps(manifest, ensure_ascii=False, indent=2)
    (dest / "manifest.json").write_text(manifest_json, encoding="utf-8")

    # UI de revisión self-contained: inyecta el manifest en la plantilla para
    # que abra con doble clic (file://) sin necesidad de servidor.
    tpl = cfg.ROOT / "revisar_template.html"
    if tpl.exists():
        html = tpl.read_text(encoding="utf-8").replace("/*__MANIFEST__*/ null", manifest_json)
        (dest / "revisar.html").write_text(html, encoding="utf-8")

    logger.info("Cola generada en: %s (%d posts)", dest, len(posts))
    return dest


def ejecutar(dry_run: bool = False, max: int | None = None,
             demo: bool = False, solo: str | None = None) -> Path | None:
    """Entry function (convención del repo: ejecutar())."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    n = max or cfg.MAX_POR_CORRIDA
    if dry_run:
        ofertas = cargar_destacados(n, demo)
        logger.info("[dry-run] %d avisos destacados; no se escribe nada.", len(ofertas))
        for o in ofertas:
            logger.info("  · %s — %s (cierra %s)", o.get("cargo"), o.get("institucion"), o.get("fecha_cierre"))
        return None
    return generar(n, demo, solo)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Genera la cola de posts RRSS de avisos destacados.")
    p.add_argument("--max", type=int, default=None, help="Máximo de avisos a procesar.")
    p.add_argument("--demo", action="store_true", help="Usa datos de ejemplo (sin BD).")
    p.add_argument("--solo", choices=list(cfg.PLATAFORMAS), default=None, help="Generar solo una plataforma.")
    p.add_argument("--dry-run", action="store_true", help="Lista sin escribir.")
    args = p.parse_args(argv)
    ejecutar(dry_run=args.dry_run, max=args.max, demo=args.demo, solo=args.solo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
