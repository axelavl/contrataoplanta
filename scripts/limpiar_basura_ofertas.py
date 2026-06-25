"""
Desactiva ofertas ACTIVAS ya almacenadas cuyo título es basura según los
criterios reforzados de intake (navegación, noticias, programas/eventos,
fragmentos de documento, títulos genéricos sin cargo y avisos viejos por año).

Reutiliza la MISMA lógica de scrapers/intake.py (no duplica patrones), así que
queda sincronizado con el filtro que evita que vuelvan a entrar a futuro.

Uso:
    python scripts/limpiar_basura_ofertas.py            # preview (no escribe)
    python scripts/limpiar_basura_ofertas.py --apply    # desactiva (activa=FALSE)
    python scripts/limpiar_basura_ofertas.py --apply --borrar   # DELETE físico
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.config import DB_CONFIG


def _connect():
    """Conecta usando psycopg2 (ya incluido en requirements.txt)."""
    import psycopg2  # type: ignore
    return psycopg2.connect(**dict(DB_CONFIG))
from scrapers.intake import (
    titulo_es_no_laboral, titulo_es_noticia, titulo_anio_pasado,
    titulo_es_empleo, titulo_es_generico, is_garbage_text, is_garbage_url,
)


def es_basura(cargo: str, url: str) -> str | None:
    """Devuelve el motivo si el título es basura; None si parece válido."""
    if titulo_es_no_laboral(cargo):
        return "no_laboral/generico/nav/doc/programa"
    if titulo_es_noticia(cargo):
        return "noticia_municipal"
    if titulo_anio_pasado(cargo):
        return "anio_pasado"
    if is_garbage_text(cargo):
        return "garbage_text"
    if is_garbage_url(url) and not titulo_es_empleo(cargo):
        return "url_no_laboral"
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Limpia ofertas basura ya guardadas")
    ap.add_argument("--apply", action="store_true", help="Ejecuta los cambios (sin esto, solo preview)")
    ap.add_argument("--borrar", action="store_true", help="DELETE físico en vez de activa=FALSE")
    ap.add_argument("--marcar-revision", action="store_true", help="Además, marca needs_review=TRUE los títulos genéricos sin cargo (concursos reales con encabezado pobre)")
    ap.add_argument("--limit", type=int, default=0, help="Tope de filas a revisar (0 = todas)")
    ap.add_argument("--reactivar-falsos", action="store_true", help="Reactiva ofertas desactivadas recientemente que YA NO son basura con los criterios actuales (revierte falsos positivos)")
    ap.add_argument("--horas", type=int, default=6, help="Ventana (horas) para --reactivar-falsos (default 6)")
    a = ap.parse_args()

    conn = _connect()
    cur = conn.cursor()

    if a.reactivar_falsos:
        cur.execute(
            "SELECT id, cargo, COALESCE(url_oferta, url_original), institucion_nombre "
            "FROM ofertas WHERE activa = FALSE "
            "AND fecha_cierre_detectada >= NOW() - make_interval(hours => %s)",
            (int(a.horas),))
        recientes = cur.fetchall()
        reactivar = [(oid, cargo, inst) for oid, cargo, url, inst in recientes
                     if es_basura(cargo or "", url or "") is None]
        print(f"Desactivadas en las últimas {a.horas}h: {len(recientes)}")
        print(f"Ya NO son basura (a reactivar): {len(reactivar)}")
        for oid, cargo, inst in reactivar[:40]:
            print(f"  + {(inst or '')[:25]:25} | {(cargo or '')[:60]}")
        if not a.apply:
            print("\n(preview) Nada se modificó. Repite con --apply para reactivar.")
            return 0
        if reactivar:
            cur.execute(
                "UPDATE ofertas SET activa = TRUE, fecha_cierre_detectada = NULL, "
                "actualizada_en = NOW() WHERE id = ANY(%s)", ([r[0] for r in reactivar],))
            conn.commit()
            print(f"\nReactivadas: {cur.rowcount} ofertas.")
        cur.close(); conn.close()
        return 0

    q = "SELECT id, cargo, COALESCE(url_oferta, url_original), institucion_nombre FROM ofertas WHERE activa = TRUE"
    if a.limit:
        q += f" LIMIT {int(a.limit)}"
    cur.execute(q)
    filas = cur.fetchall()

    a_borrar: list[int] = []
    a_revisar: list[int] = []
    por_motivo: dict[str, int] = {}
    muestras: list[str] = []
    for oid, cargo, url, inst in filas:
        motivo = es_basura(cargo or "", url or "")
        if motivo:
            a_borrar.append(oid)
            por_motivo[motivo] = por_motivo.get(motivo, 0) + 1
            if len(muestras) < 40:
                muestras.append(f"  [{motivo}] {(inst or '')[:25]:25} | {(cargo or '')[:60]}")
        elif titulo_es_generico(cargo or ""):
            a_revisar.append(oid)

    print(f"Ofertas activas revisadas: {len(filas)}")
    print(f"Detectadas como basura:    {len(a_borrar)}")
    print(f"Genéricas para revisión:   {len(a_revisar)}")
    print("Por motivo:", por_motivo)
    print("Muestra:")
    print("\n".join(muestras))

    if not a.apply:
        print("\n(preview) Nada se modificó. Repite con --apply para aplicar.")
        return 0
    if not a_borrar and not (a.marcar_revision and a_revisar):
        print("Nada que limpiar.")
        return 0

    if a.borrar:
        cur.execute("DELETE FROM ofertas WHERE id = ANY(%s)", (a_borrar,))
    else:
        cur.execute(
            "UPDATE ofertas SET activa = FALSE, actualizada_en = NOW(), "
            "fecha_cierre_detectada = COALESCE(fecha_cierre_detectada, NOW()) "
            "WHERE id = ANY(%s)", (a_borrar,))
    desactivadas = cur.rowcount
    revisadas = 0
    if a.marcar_revision and a_revisar:
        cur.execute(
            "UPDATE ofertas SET needs_review = TRUE, actualizada_en = NOW() "
            "WHERE id = ANY(%s)", (a_revisar,))
        revisadas = cur.rowcount
    conn.commit()
    print(f"\nAplicado: {desactivadas} ofertas {'borradas' if a.borrar else 'desactivadas'}"
          + (f"; {revisadas} marcadas para revisión." if a.marcar_revision else "."))
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
