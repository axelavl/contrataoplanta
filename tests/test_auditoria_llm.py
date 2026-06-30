"""Tests de los helpers puros de la auditoría LLM y del cliente Anthropic.

No tocan la API ni la BD: cubren parseo de respuesta, tolerancia a JSON
sucio, carga de anomalías y construcción de prompts.
"""

from __future__ import annotations

import json

from classification.anthropic_client import _coerce_response, _extraer_json
from scripts.qa.auditoria_llm_clasificacion import (
    cargar_anomalias,
    _prompt_desde_anomalia,
    _prompt_desde_publicada,
)


def test_extraer_json_limpio():
    assert _extraer_json('{"is_job_posting": true}') == {"is_job_posting": True}


def test_extraer_json_con_basura_alrededor():
    txt = 'Claro:\n```json\n{"is_job_posting": false, "confidence": 0.9}\n```\nfin'
    assert _extraer_json(txt)["is_job_posting"] is False


def test_extraer_json_invalido_levanta():
    import pytest

    with pytest.raises(ValueError):
        _extraer_json("no hay json acá")


def test_coerce_normaliza_tipos_y_rangos():
    out = _coerce_response(
        {"is_job_posting": "sí?", "content_type": "inventado", "confidence": "2.0", "reason": "x"},
        fallback_title="Cargo X",
    )
    assert out["is_job_posting"] is True            # truthy -> True
    assert out["content_type"] == "job_posting"     # inválido -> derivado de is_job
    assert out["confidence"] == 1.0                 # clamp a [0,1]
    assert out["likely_job_title"] == "Cargo X"


def test_coerce_confidence_no_numerica_cae_a_medio():
    out = _coerce_response({"is_job_posting": False, "reason": "ruido"}, fallback_title=None)
    assert out["confidence"] == 0.5
    assert out["content_type"] == "other"


def test_cargar_anomalias_extrae_los_dos_buckets(tmp_path):
    rep = {
        "anomalies": [
            {"name": "publish_with_negative_url", "sample_records": [{"oferta_id": 1}]},
            {"name": "reject_with_only_positive_signals", "sample_records": [{"oferta_id": 2}]},
            {"name": "otra_cosa", "sample_records": [{"oferta_id": 3}]},
        ]
    }
    p = tmp_path / "audit.json"
    p.write_text(json.dumps(rep), encoding="utf-8")
    out = cargar_anomalias(p)
    assert [r["oferta_id"] for r in out["publish_with_negative_url"]] == [1]
    assert [r["oferta_id"] for r in out["reject_with_only_positive_signals"]] == [2]


def test_prompt_desde_publicada_mapea_campos():
    row = {"cargo": "Analista", "descripcion": "funciones...", "institucion_nombre": "Min X",
           "url_oferta": "http://x", "region": "RM", "fecha_cierre": None}
    p = _prompt_desde_publicada(row)
    assert p["title"] == "Analista"
    assert p["content_preview"] == "funciones..."
    assert p["fecha_cierre"] is None


def test_prompt_desde_anomalia_usa_excerpt():
    rec = {"cargo": "Cargo", "url_oferta": "http://x", "descripcion_excerpt": "texto"}
    p = _prompt_desde_anomalia(rec)
    assert p["content_preview"] == "texto"
