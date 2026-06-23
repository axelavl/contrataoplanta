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

# Normalización sin extensiones: permite que búsquedas como "contraloria" o
# "educacion" encuentren instituciones/cargos con tildes. Se usa translate()
# para no depender de la extensión PostgreSQL unaccent en producción.
_ACCENT_FROM = "ÁÀÂÄÃÅáàâäãåÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÖÕóòôöõÚÙÛÜúùûüÑñÇç"
_ACCENT_TO = "AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc"


def _norm_sql(expr: str) -> str:
    """SQL expression lower/trim/sin tildes para comparaciones LIKE."""
    return f"lower(translate(({expr})::text, '{_ACCENT_FROM}', '{_ACCENT_TO}'))"


def _norm_like(value: str) -> str:
    """Normaliza el término en Python para usarlo con _norm_sql(... ) LIKE %s."""
    src = str.maketrans(_ACCENT_FROM, _ACCENT_TO)
    return f"%{value.translate(src).lower()}%"


def _clean_institution_sql(expr: str) -> str:
    """Elimina sufijos/segmentos visibles tipo '— Personal Civil' del nombre.

    Se aplica sólo al nombre mostrado/buscado; no cambia IDs ni datos persistidos.
    Cubre variantes: 'Armada - Personal Civil', 'Armada, Personal Civil',
    'Carabineros Personal Civil' y espacios duplicados resultantes.
    """
    without_marker = (
        "regexp_replace("
        f"({expr})::text, "
        "'\\s*[,;:/|—–-]?\\s*personal\\s+civil\\s*', "
        "' ', 'gi')"
    )
    collapsed = f"regexp_replace({without_marker}, '\\s{{2,}}', ' ', 'g')"
    return f"NULLIF(TRIM({collapsed}), '')"


def ofertas_base_sql() -> str:
    return """
    FROM ofertas o
    LEFT JOIN instituciones i ON i.id = o.institucion_id
    """


def ofertas_select_sql() -> str:
    institucion_visible = _clean_institution_sql(
        "COALESCE(NULLIF(TRIM(o.institucion_nombre), ''), i.nombre, 'Sin institución')"
    )
    return f"""
    SELECT
        o.id,
        o.institucion_id,
        -- Prioridad: nombre tal como aparece en la oferta oficial (o.institucion_nombre)
        -- sobre el match del catálogo (i.nombre). Antes de exponerlo se limpia
        -- el marcador operacional 'Personal Civil' para que el usuario vea sólo
        -- la institución: 'Armada de Chile', 'Carabineros de Chile', etc.
        COALESCE({institucion_visible}, 'Sin institución') AS institucion,
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
        o.renta_regional,
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
        o.email_postulacion,
        o.email_consultas,
        o.numero_vacantes,
        o.calidad_juridica,
        o.estamento,
        o.lugar_desempenio,
        {OFFER_STATUS_SQL} AS estado,
        COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) AS fecha_scraped,
        COALESCE(o.fecha_actualizado, o.actualizada_en, o.creada_en) AS fecha_actualizado,
        i.plataforma_empleo AS plataforma,
        i.url_empleo AS institucion_url_empleo
    """


def _normalizar_ids(valor: Any) -> list[int]:
    """Acepta int, lista de ints o CSV ('12,34') y devuelve [12, 34].

    Ignora valores no numéricos en silencio. Soporta el filtro
    multi-institución sin romper el contrato anterior (un solo id).
    """
    if valor is None:
        return []
    crudos: list[Any]
    if isinstance(valor, (list, tuple, set)):
        crudos = list(valor)
    elif isinstance(valor, str):
        crudos = valor.split(",")
    else:
        crudos = [valor]
    ids: list[int] = []
    for item in crudos:
        try:
            ids.append(int(str(item).strip()))
        except (TypeError, ValueError):
            continue
    # Dedup preservando orden.
    return list(dict.fromkeys(ids))


# Lexemas por familia profesional. Debe mantenerse en sintonía con
# `FAMILIAS` de web/integracion/profesiones.js (mismas raíces, en ASCII;
# el match usa unaccent() así que no hace falta duplicar variantes con tilde).
FAMILIAS_ROOTS: dict[str, list[str]] = {
    "salud": ["enfermer", "matron", "kinesi", "medic", "tens", "nutricion",
              "clinic", "paramedic", "odontolog", "tecnologo medic", "salud"],
    "educacion": ["profesor", "educador", "parvul", "docente", "pedag",
                  "asistente de la educacion"],
    "juridico": ["abogad", "fiscalizador", "procurador", "juridic", "litig",
                 "legal", "derecho"],
    "ingenieria": ["ingenier", "obra", "proyecto", "construccion"],
    "admin": ["administrativ", "analista", "recursos humanos", "rrhh",
              "secretari", "gestion", "vinculacion"],
    "psicosocial": ["psicolog", "trabajador social", "trabajadora social",
                    "asistente social", "psicosocial"],
    "finanzas": ["contad", "auditor", "tributari", "financ", "finanz",
                 "contabil", "presupuest"],
    "ti": ["informatic", "desarrollador", "programador", "sistemas",
           "soporte ti", "ciberseg", "datos"],
}


def build_ofertas_filters(
    q: str | None = None,
    region: str | None = None,
    sector: str | None = None,
    tipo: str | None = None,
    institucion_id: int | str | list | None = None,
    area_profesional: str | None = None,
    profesion: str | None = None,
    nivel: str | None = None,
    renta_min: int | None = None,
    renta_max: int | None = None,
    ciudad: str | None = None,
    comunas: str | None = None,
    cierra_pronto: bool = False,
    nuevas: bool = False,
    solo_con_correo: bool = False,
    solo_activas: bool = True,
    closed_only: bool = False,
) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []

    if solo_activas:
        where.append(ACTIVE_OFFER_SQL)
    if closed_only:
        where.append(f"{OFFER_STATUS_SQL} = 'closed'")

    if solo_con_correo:
        # "Postular por correo": ofertas con email de contacto capturado en las
        # columnas nuevas, o con un correo presente en descripción/requisitos
        # (cubre filas anteriores a que el scraper repoblara las columnas).
        _email_re = r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}'
        where.append(
            "(o.email_postulacion IS NOT NULL OR o.email_consultas IS NOT NULL "
            "OR COALESCE(o.descripcion, '') ~* %s "
            "OR COALESCE(o.requisitos, o.requisitos_texto, '') ~* %s)"
        )
        params.extend([_email_re, _email_re])

    if q:
        norm_like = _norm_like(q)
        norm_cargo_sql = _norm_sql("COALESCE(o.cargo, '')")
        norm_inst_sql = _norm_sql(_clean_institution_sql("COALESCE(i.nombre, o.institucion_nombre, '')"))
        norm_sigla_sql = _norm_sql("COALESCE(i.sigla, i.nombre_corto, '')")
        where.append(
            "("
            # Búsqueda por FRASE: phraseto_tsquery exige que los lexemas aparezcan
            # ADYACENTES, no sueltos. Así "administrador público" matchea el cargo
            # "Administrador Público" y no cualquier aviso que tenga "administrador"
            # en un lado y "público" en otro (antes era plainto_tsquery, que los
            # unía con AND disperso y traía ruido). Los ILIKE/LIKE de abajo ya
            # exigen la frase completa como substring, así que todo el bloque es
            # coherente con búsqueda por frase.
            "to_tsvector('spanish', coalesce(o.cargo, '') || ' ' || coalesce(i.nombre, '') || ' ' || coalesce(o.descripcion, '')) @@ phraseto_tsquery('spanish', %s) "
            "OR o.cargo ILIKE %s "
            "OR COALESCE(i.nombre, o.institucion_nombre, '') ILIKE %s "
            "OR COALESCE(o.descripcion, '') ILIKE %s "
            f"OR {norm_cargo_sql} LIKE %s "
            f"OR {norm_inst_sql} LIKE %s "
            f"OR {norm_sigla_sql} LIKE %s "
            ")"
        )
        like = f"%{q}%"
        params.extend([q, like, like, like, norm_like, norm_like, norm_like])

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

    ids_institucion = _normalizar_ids(institucion_id)
    if len(ids_institucion) == 1:
        where.append("o.institucion_id = %s")
        params.append(ids_institucion[0])
    elif ids_institucion:
        placeholders = ", ".join(["%s"] * len(ids_institucion))
        where.append(f"o.institucion_id IN ({placeholders})")
        params.extend(ids_institucion)

    if area_profesional:
        where.append("o.area_profesional ILIKE %s")
        params.append(f"%{area_profesional}%")

    # Familia profesional (chips del frontend). Expande a los lexemas de la
    # familia y matchea contra el cargo (accent-insensitive). El campo
    # o.area_profesional NO es confiable, por eso filtramos por cargo.
    if profesion:
        roots = FAMILIAS_ROOTS.get(profesion.strip().lower())
        if roots:
            # Sin unaccent (no está en producción): usamos translate() vía _norm_sql.
            norm_cargo = _norm_sql("COALESCE(o.cargo, '')")
            clauses = []
            for root in roots:
                clauses.append(f"{norm_cargo} LIKE %s")
                params.append(_norm_like(root))
            where.append("(" + " OR ".join(clauses) + ")")

    if nivel:
        # Nivel/estamento (Directivo, Profesional, Técnico, Administrativo...).
        # Acepta varios separados por coma. Tolerante a tildes SIN unaccent.
        niveles = [item.strip() for item in nivel.split(",") if item.strip()]
        norm_nivel = _norm_sql("COALESCE(o.nivel, '')")
        if niveles:
            clauses = []
            for item in niveles:
                clauses.append(f"{norm_nivel} LIKE %s")
                params.append(_norm_like(item))
            where.append("(" + " OR ".join(clauses) + ")")

    if renta_min is not None:
        where.append("(o.renta_bruta_min >= %s OR o.renta_bruta_max >= %s)")
        params.extend([renta_min, renta_min])

    if renta_max is not None:
        # La oferta cae dentro del rango si su piso de renta no supera el tope
        # pedido. Usamos COALESCE para no descartar ofertas que sólo publican máximo.
        where.append("COALESCE(o.renta_bruta_min, o.renta_bruta_max) <= %s")
        params.append(renta_max)

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
        # "Cierra pronto" = cierra hoy o mañana (issue #242).
        where.append("o.fecha_cierre BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '1 day'")

    if nuevas:
        # "Nuevas" = añadidas en las últimas 24 horas (issue #242).
        where.append("COALESCE(o.fecha_scraped, o.detectada_en, o.actualizada_en, o.creada_en) >= NOW() - INTERVAL '24 hours'")

    return (" WHERE " + " AND ".join(where)) if where else "", params
