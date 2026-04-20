"""Builders de SQL y constantes para consultas de ofertas.

Extraído de `api/main.py`. Pure string-building — no ejecuta SQL,
sólo devuelve strings y listas de parámetros que el caller pasa a
`execute_fetch_*` de `api/services/db.py`.

Contenido:

- `OFFER_STATUS_SQL`: expresión CASE que normaliza el estado de una
  oferta (``active`` / ``closing_today`` / ``upcoming`` / ``closed`` /
  ``unknown``) desde columnas legadas heterogéneas.
- `ACTIVE_OFFER_SQL`: filtro `WHERE` para ofertas vigentes
  (active + closing_today).
- `STATUS_LEGACY_MAP`: traducción del estado canónico a los valores
  legacy en español que el frontend aún consume.
- `ofertas_base_sql()` y `ofertas_select_sql()`: `FROM` + `SELECT` de
  ofertas enriquecidas con la tabla `instituciones`.
- `build_ofertas_filters(...)`: construye el `WHERE` + params list
  para los filtros del buscador.
"""
from __future__ import annotations

from typing import Any


OFFER_STATUS_SQL = (
    "CASE "
    "WHEN COALESCE(o.activa, TRUE) = FALSE THEN 'closed' "
    "WHEN LOWER(COALESCE(NULLIF(o.estado, ''), '')) IN "
    "('cerrada', 'cerrado', 'cerrada_manual', 'vencido', 'finalizada', 'closed', 'expired') THEN 'closed' "
    "WHEN COALESCE(o.fecha_inicio, o.fecha_publicacion) IS NOT NULL "
    "  AND COALESCE(o.fecha_inicio, o.fecha_publicacion) > CURRENT_DATE THEN 'upcoming' "
    "WHEN o.fecha_cierre IS NOT NULL AND o.fecha_cierre < CURRENT_DATE THEN 'closed' "
    "WHEN o.fecha_cierre = CURRENT_DATE THEN 'closing_today' "
    "WHEN o.fecha_cierre IS NULL OR o.fecha_cierre > CURRENT_DATE THEN 'active' "
    "ELSE 'unknown' "
    "END"
)

ACTIVE_OFFER_SQL = f"{OFFER_STATUS_SQL} IN ('active', 'closing_today')"

STATUS_LEGACY_MAP = {
    "active": "activo",
    "closing_today": "activo",
    "upcoming": "proximo",
    "closed": "cerrado",
    "unknown": "desconocido",
}


def ofertas_base_sql() -> str:
    return """
    FROM ofertas o
    LEFT JOIN instituciones i ON i.id = o.institucion_id
    """


def ofertas_select_sql() -> str:
    return f"""
    SELECT
        o.id,
        o.institucion_id,
        -- Prioridad: nombre tal como aparece en la oferta oficial (o.institucion_nombre)
        -- sobre el match del catálogo (i.nombre). El match por institucion_id se
        -- hace por heurística sobre nombres y puede asignar el portal madre o el
        -- ministerio superior (ej. "Superintendencia de Salud") cuando la vacante
        -- real pertenece a un hospital/servicio específico (ej. "Hospital Base
        -- San José de Osorno"). El texto extraído por el scraper desde la oferta
        -- es más fiel a lo que el usuario debe ver.
        COALESCE(NULLIF(TRIM(o.institucion_nombre), ''), i.nombre, 'Sin institución') AS institucion,
        COALESCE(i.sigla, i.nombre_corto) AS sigla,
        COALESCE(o.cargo, 'Sin cargo') AS cargo,
        COALESCE(o.descripcion, '') AS descripcion,
        COALESCE(o.requisitos, o.requisitos_texto, '') AS requisitos,
        COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) AS tipo_contrato,
        COALESCE(o.region, i.region) AS region,
        o.ciudad,
        COALESCE(i.sector, o.sector, i.tipo) AS sector,
        o.renta_bruta_min,
        o.renta_bruta_max,
        o.grado_eus,
        COALESCE(
            o.jornada,
            CASE
                WHEN o.horas_semanales IS NOT NULL THEN o.horas_semanales::text || ' hrs / semana'
                ELSE NULL
            END
        ) AS jornada,
        o.area_profesional,
        o.fecha_publicacion,
        o.fecha_inicio,
        o.fecha_cierre,
        COALESCE(o.url_oferta, o.url_original) AS url_oferta,
        COALESCE(o.url_bases, o.url_original, o.url_oferta) AS url_bases,
        o.url_oferta_valida,
        o.url_bases_valida,
        o.url_valida_chequeada_en,
        {OFFER_STATUS_SQL} AS estado,
        COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) AS fecha_scraped,
        COALESCE(o.fecha_actualizado, o.actualizada_en, o.creada_en) AS fecha_actualizado,
        i.plataforma_empleo AS plataforma,
        i.url_empleo AS institucion_url_empleo
    """


def build_ofertas_filters(
    q: str | None = None,
    region: str | None = None,
    sector: str | None = None,
    tipo: str | None = None,
    institucion_id: int | None = None,
    area_profesional: str | None = None,
    renta_min: int | None = None,
    ciudad: str | None = None,
    comunas: str | None = None,
    cierra_pronto: bool = False,
    nuevas: bool = False,
    solo_activas: bool = True,
    closed_only: bool = False,
) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []

    if solo_activas:
        where.append(ACTIVE_OFFER_SQL)
    if closed_only:
        where.append(f"{OFFER_STATUS_SQL} = 'closed'")

    if q:
        where.append(
            "("
            "to_tsvector('spanish', coalesce(o.cargo, '') || ' ' || coalesce(i.nombre, '') || ' ' || coalesce(o.descripcion, '')) @@ plainto_tsquery('spanish', %s) "
            "OR o.cargo ILIKE %s "
            "OR COALESCE(i.nombre, o.institucion_nombre, '') ILIKE %s "
            "OR COALESCE(o.descripcion, '') ILIKE %s"
            ")"
        )
        like = f"%{q}%"
        params.extend([q, like, like, like])

    if region:
        where.append("COALESCE(o.region, i.region, '') ILIKE %s")
        params.append(f"%{region}%")

    if sector:
        where.append("COALESCE(i.sector, o.sector, i.tipo, '') ILIKE %s")
        params.append(f"%{sector}%")

    if tipo:
        tipos = [item.strip() for item in tipo.split(",") if item.strip()]
        if len(tipos) == 1:
            where.append("COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) ILIKE %s")
            params.append(f"%{tipos[0]}%")
        elif tipos:
            clauses = []
            for item in tipos:
                clauses.append("COALESCE(NULLIF(o.tipo_contrato, ''), NULLIF(o.tipo_cargo, '')) ILIKE %s")
                params.append(f"%{item}%")
            where.append("(" + " OR ".join(clauses) + ")")

    if institucion_id is not None:
        where.append("o.institucion_id = %s")
        params.append(institucion_id)

    if area_profesional:
        where.append("o.area_profesional ILIKE %s")
        params.append(f"%{area_profesional}%")

    if renta_min is not None:
        where.append("(o.renta_bruta_min >= %s OR o.renta_bruta_max >= %s)")
        params.extend([renta_min, renta_min])

    if comunas:
        lista_comunas = [item.strip() for item in comunas.split(",") if item.strip()]
        if lista_comunas:
            clauses = []
            for item in lista_comunas:
                clauses.append("o.ciudad ILIKE %s")
                params.append(f"%{item}%")
            where.append("(" + " OR ".join(clauses) + ")")
    elif ciudad:
        where.append("o.ciudad ILIKE %s")
        params.append(f"%{ciudad}%")

    if cierra_pronto:
        where.append("o.fecha_cierre BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '5 days'")

    if nuevas:
        where.append("COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) >= NOW() - INTERVAL '48 hours'")

    return (" WHERE " + " AND ".join(where)) if where else "", params
