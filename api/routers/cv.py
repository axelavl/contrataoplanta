"""`POST /api/cv/match`: sube tu CV y recibe ofertas vigentes afines.

Privacidad (ley 21.719): el CV se procesa en memoria durante la request y no
se guarda en la base ni se escribe en logs. Se exige consentimiento explícito
(`consentimiento=true`, casilla no premarcada en el frontend).

Costo: solo reglas (api/services/cv_matching.py), sin LLM ni llamadas
externas. Rate limit propio para que nadie use el parser de PDF para cargar
el servidor.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from starlette.concurrency import run_in_threadpool

from api.deps import check_public_rate_limit, client_ip
from api.services.cv_matching import (
    MAX_BYTES_CV,
    CVInvalido,
    perfil_desde_archivo,
    rankear,
)
from api.services.db import execute_fetch_all
from api.services.sql import ACTIVE_OFFER_SQL, ofertas_base_sql

logger = logging.getLogger("api.routers.cv")

router = APIRouter(tags=["cv"])

#: 5 CV cada 10 min por IP: de sobra para una persona que ajusta su CV.
CV_RATE_WINDOW = 600
CV_RATE_MAX = 5
#: Tope de ofertas a puntuar (todas las vigentes caben holgadamente).
MAX_OFERTAS = 8000

_SQL_OFERTAS = f"""
    SELECT
        o.id,
        COALESCE(o.cargo, '') AS cargo,
        COALESCE(NULLIF(TRIM(o.institucion_nombre), ''), i.nombre, '') AS institucion,
        o.area_profesional,
        o.nivel,
        COALESCE(o.region, i.region) AS region,
        LEFT(COALESCE(NULLIF(o.requisitos, ''), o.requisitos_texto, ''), 4000) AS requisitos,
        o.fecha_cierre,
        COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) AS tipo_contrato,
        o.renta_bruta_min,
        o.renta_bruta_max
    {ofertas_base_sql()}
    WHERE {ACTIVE_OFFER_SQL}
    LIMIT {MAX_OFERTAS}
"""

_CAMPOS_SALIDA = ("id", "cargo", "institucion", "region", "fecha_cierre",
                  "tipo_contrato", "renta_bruta_min", "renta_bruta_max")


def _serializar(oferta: dict[str, Any]) -> dict[str, Any]:
    salida = {k: oferta.get(k) for k in _CAMPOS_SALIDA}
    if isinstance(salida["fecha_cierre"], (date, datetime)):
        salida["fecha_cierre"] = salida["fecha_cierre"].isoformat()
    return salida


@router.post("/api/cv/match")
async def match_cv(
    request: Request,
    archivo: UploadFile = File(...),
    consentimiento: str = Form(""),
) -> dict[str, Any]:
    if consentimiento.strip().lower() not in ("true", "1", "si", "sí", "on"):
        raise HTTPException(400, "Debes aceptar el procesamiento de tu CV para continuar.")

    check_public_rate_limit(client_ip(request), "cv_match",
                            window_seg=CV_RATE_WINDOW, max_hits=CV_RATE_MAX)

    datos = await archivo.read(MAX_BYTES_CV + 1)
    await archivo.close()
    try:
        perfil = await run_in_threadpool(perfil_desde_archivo, datos)
    except CVInvalido as exc:
        raise HTTPException(422, str(exc)) from None
    finally:
        del datos

    try:
        ofertas = await run_in_threadpool(execute_fetch_all, _SQL_OFERTAS, [])
    except Exception as exc:
        logger.warning("cv_match: no se pudieron leer ofertas: %s", type(exc).__name__)
        raise HTTPException(503, "No pudimos consultar las ofertas. Intenta en unos minutos.") from None

    resultados = await run_in_threadpool(rankear, perfil, ofertas)
    # Solo métricas agregadas: nada del contenido del CV.
    logger.info("cv_match: %d ofertas evaluadas, %d sugeridas",
                len(ofertas), len(resultados))
    return {
        "perfil": perfil.a_dict(),
        "evaluadas": len(ofertas),
        "resultados": [
            {"oferta": _serializar(r.oferta), "banda": r.banda,
             "razones": r.razones, "reparos": r.reparos}
            for r in resultados
        ],
    }
