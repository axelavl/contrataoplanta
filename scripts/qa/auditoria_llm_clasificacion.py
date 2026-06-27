"""Auditoría por muestreo de la clasificación de ofertas, con dictamen LLM.

Complementa al auditor estadístico ``scrapers.evaluation.classification_quality_audit``
(que detecta anomalías GRATIS con reglas) agregando una capa de **dictamen**:
toma una muestra acotada y le pregunta a Claude si cada caso es REALMENTE una
oferta o no. Sirve para medir, con datos, dos cosas:

1. **Falsos positivos** — ofertas publicadas que en realidad son noticias,
   comunicados, resultados de concursos cerrados, etc. (se muestrean ofertas
   activas al azar desde la BD).
2. **Falsos negativos** — páginas que el pipeline rechazó pero que sí eran
   ofertas reales (se adjudican las anomalías ``reject_with_only_positive_signals``
   y ``publish_with_negative_url`` de un reporte previo del auditor estadístico).

Diseño barato: el LLM **sólo** ve la muestra/anomalías, nunca todas las páginas.
El costo se reporta al final.

Ejemplos:

    # Falsos positivos: 200 ofertas publicadas al azar (necesita ANTHROPIC_API_KEY)
    python scripts/qa/auditoria_llm_clasificacion.py --from-db --max 200 \\
        --output reports/auditoria_llm.json

    # Estimar costo y ver la muestra SIN llamar a la API
    python scripts/qa/auditoria_llm_clasificacion.py --from-db --max 200 --dry-run

    # Adjudicar las anomalías de un reporte del auditor estadístico
    python scripts/qa/auditoria_llm_clasificacion.py \\
        --anomalias reports/classification_audit.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from classification.anthropic_client import (  # noqa: E402
    MODELO_DEFAULT,
    PRECIOS_USD_POR_1M,
)

# ── SQL de muestreo (filtro de "activa" autocontenido, sin placeholders) ──
_SQL_PUBLICADAS = """
    SELECT o.id, o.cargo, o.descripcion, o.institucion_nombre,
           o.url_oferta, o.area_profesional, o.region, o.fecha_cierre
    FROM ofertas o
    WHERE COALESCE(o.activa, TRUE) = TRUE
      AND (o.fecha_cierre IS NULL
           OR o.fecha_cierre >= (NOW() AT TIME ZONE 'America/Santiago')::date)
    ORDER BY RANDOM()
    LIMIT %s
"""


def muestrear_publicadas(conn, n: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(_SQL_PUBLICADAS, (int(n),))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _prompt_desde_publicada(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": row.get("cargo"),
        "institucion": row.get("institucion_nombre"),
        "region": row.get("region"),
        "fecha_cierre": str(row.get("fecha_cierre")) if row.get("fecha_cierre") else None,
        "url": row.get("url_oferta"),
        "content_preview": row.get("descripcion") or "",
    }


def cargar_anomalias(path: Path) -> dict[str, list[dict[str, Any]]]:
    """Lee un reporte de ClassificationQualityAudit y devuelve las muestras de
    las dos anomalías relevantes para falsos positivos/negativos."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, list[dict[str, Any]]] = {
        "publish_with_negative_url": [],
        "reject_with_only_positive_signals": [],
    }
    for bucket in payload.get("anomalies") or []:
        nombre = bucket.get("name")
        if nombre in out:
            out[nombre] = list(bucket.get("sample_records") or [])
    return out


def _prompt_desde_anomalia(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": rec.get("cargo"),
        "institucion": rec.get("institucion_nombre"),
        "url": rec.get("url_oferta"),
        "content_preview": rec.get("descripcion_excerpt") or "",
    }


# ── Estimación de costo para --dry-run (sin llamar a la API) ──────────────

def _estimar_tokens(prompt: dict[str, Any]) -> int:
    """~4 caracteres por token; suma del contexto enviado."""
    chars = sum(len(str(v)) for v in prompt.values() if v)
    return chars // 4 + 350  # + system prompt aprox.


# ── Núcleo ────────────────────────────────────────────────────────────────

def _adjudicar(client, items: list[dict[str, Any]], esperado_es_oferta: bool) -> list[dict[str, Any]]:
    """Corre el dictamen sobre cada item. ``esperado_es_oferta`` es lo que el
    pipeline asumió (publicadas → True; rechazadas → False). Un veredicto
    distinto del esperado marca el item como sospechoso."""
    resultados: list[dict[str, Any]] = []
    for it in items:
        prompt = it["_prompt"]
        try:
            veredicto = client.classify_content(prompt)
        except Exception as exc:  # noqa: BLE001 — un fallo no debe abortar la corrida
            resultados.append({**it["_meta"], "error": str(exc)})
            continue
        es_oferta = bool(veredicto.get("is_job_posting"))
        resultados.append({
            **it["_meta"],
            "veredicto_es_oferta": es_oferta,
            "content_type": veredicto.get("content_type"),
            "confidence": veredicto.get("confidence"),
            "reason": veredicto.get("reason"),
            "sospechoso": es_oferta != esperado_es_oferta,
        })
    return resultados


def _meta_publicada(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "fuente": "publicada",
        "oferta_id": row.get("id"),
        "cargo": row.get("cargo"),
        "url": row.get("url_oferta"),
    }


def _meta_anomalia(rec: dict[str, Any], anomalia: str) -> dict[str, Any]:
    return {
        "fuente": f"anomalia:{anomalia}",
        "oferta_id": rec.get("oferta_id"),
        "cargo": rec.get("cargo"),
        "url": rec.get("url_oferta"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Auditoría por muestreo con dictamen LLM de la clasificación de ofertas.",
    )
    parser.add_argument("--from-db", action="store_true",
                        help="Muestrea ofertas publicadas activas desde la BD (falsos positivos).")
    parser.add_argument("--max", type=int, default=100,
                        help="Tamaño de la muestra de ofertas publicadas (default 100).")
    parser.add_argument("--anomalias", help="Ruta a un reporte JSON de ClassificationQualityAudit "
                                            "para adjudicar sus anomalías (falsos positivos/negativos).")
    parser.add_argument("--model", default=MODELO_DEFAULT,
                        help=f"Modelo de Claude (default {MODELO_DEFAULT}).")
    parser.add_argument("--dry-run", action="store_true",
                        help="No llama a la API: muestra la muestra y estima el costo.")
    parser.add_argument("--output", help="Ruta de salida JSON. Si se omite, imprime a stdout.")
    args = parser.parse_args(argv)

    if not args.from_db and not args.anomalias:
        raise SystemExit("Indica al menos una fuente: --from-db y/o --anomalias PATH")

    # Arma los items a evaluar (cada uno con su prompt y metadata + lo esperado).
    grupos: list[tuple[str, bool, list[dict[str, Any]]]] = []  # (nombre, esperado_es_oferta, items)

    if args.from_db:
        try:
            import psycopg2  # noqa: F401
            from db.config import DB_CONFIG
        except Exception as exc:  # pragma: no cover
            raise SystemExit(f"--from-db no disponible: {exc}")
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            rows = muestrear_publicadas(conn, args.max)
        finally:
            conn.close()
        items = [{"_prompt": _prompt_desde_publicada(r), "_meta": _meta_publicada(r)} for r in rows]
        grupos.append(("publicadas", True, items))

    if args.anomalias:
        anomalias = cargar_anomalias(Path(args.anomalias))
        # publish_with_negative_url → esperado=publicada(oferta); buscamos FALSOS POSITIVOS
        fp = [
            {"_prompt": _prompt_desde_anomalia(r), "_meta": _meta_anomalia(r, "publish_with_negative_url")}
            for r in anomalias["publish_with_negative_url"]
        ]
        if fp:
            grupos.append(("anomalias_publish", True, fp))
        # reject_with_only_positive_signals → esperado=rechazada(no oferta); buscamos FALSOS NEGATIVOS
        fn = [
            {"_prompt": _prompt_desde_anomalia(r), "_meta": _meta_anomalia(r, "reject_with_only_positive_signals")}
            for r in anomalias["reject_with_only_positive_signals"]
        ]
        if fn:
            grupos.append(("anomalias_reject", False, fn))

    total_items = sum(len(items) for _, _, items in grupos)
    if total_items == 0:
        raise SystemExit("La muestra quedó vacía (¿BD sin ofertas activas? ¿reporte sin anomalías?).")

    # ── Dry-run: estimar y salir ─────────────────────────────────────────
    if args.dry_run:
        precio_in, precio_out = PRECIOS_USD_POR_1M.get(args.model, (0.0, 0.0))
        in_tok = sum(_estimar_tokens(it["_prompt"]) for _, _, items in grupos for it in items)
        out_tok = total_items * 120
        costo = in_tok / 1_000_000 * precio_in + out_tok / 1_000_000 * precio_out
        reporte = {
            "dry_run": True,
            "model": args.model,
            "items_a_evaluar": total_items,
            "por_grupo": {nombre: len(items) for nombre, _, items in grupos},
            "input_tokens_aprox": in_tok,
            "output_tokens_aprox": out_tok,
            "costo_estimado_usd": round(costo, 4),
            "costo_estimado_usd_batch_50pct": round(costo / 2, 4),
            "nota": "Estimación grosera (~4 chars/token). Con Batch API el costo baja ~50%.",
        }
        _emitir(reporte, args.output)
        return 0

    # ── Corrida real ─────────────────────────────────────────────────────
    from classification.anthropic_client import AnthropicLLMClient
    client = AnthropicLLMClient(model=args.model)

    salida_grupos: dict[str, Any] = {}
    sospechosos: list[dict[str, Any]] = []
    for nombre, esperado, items in grupos:
        resultados = _adjudicar(client, items, esperado_es_oferta=esperado)
        evaluados = [r for r in resultados if "veredicto_es_oferta" in r]
        n_sosp = sum(1 for r in evaluados if r.get("sospechoso"))
        salida_grupos[nombre] = {
            "esperado_es_oferta": esperado,
            "evaluados": len(evaluados),
            "errores_api": len(resultados) - len(evaluados),
            "sospechosos": n_sosp,
            "tasa_sospechosos": round(n_sosp / len(evaluados), 4) if evaluados else None,
        }
        sospechosos.extend(r for r in evaluados if r.get("sospechoso"))

    reporte = {
        "dry_run": False,
        "resumen_por_grupo": salida_grupos,
        "sospechosos": sorted(sospechosos, key=lambda r: (r.get("confidence") or 0)),
        "uso_api": client.usage_summary(),
    }
    _emitir(reporte, args.output)

    # Resumen legible a stderr para no contaminar el JSON de stdout.
    print("\n── Resumen de auditoría ──", file=sys.stderr)
    for nombre, data in salida_grupos.items():
        tasa = data["tasa_sospechosos"]
        print(f"  {nombre}: {data['sospechosos']}/{data['evaluados']} sospechosos"
              f" ({'' if tasa is None else f'{tasa:.1%}'})", file=sys.stderr)
    u = client.usage_summary()
    print(f"  Costo estimado: US${u['estimated_cost_usd']} "
          f"({u['calls']} llamadas, {u['input_tokens']}+{u['output_tokens']} tokens)", file=sys.stderr)
    return 0


def _emitir(reporte: dict[str, Any], output: str | None) -> None:
    texto = json.dumps(reporte, ensure_ascii=False, indent=2, default=str)
    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(texto, encoding="utf-8")
        print(f"reporte escrito en {output}", file=sys.stderr)
    else:
        print(texto)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
