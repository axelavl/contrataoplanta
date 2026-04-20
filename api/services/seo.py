"""SSR, meta tags, JSON-LD y sitemap para el frontend.

Extraído de `api/main.py`. Consume `api.services.db` (DB) y
`api.services.formatters` (helpers puros). Define mapas de landings
(región/sector) hardcoded, helpers de manipulación HTML, y los
builders de JobPosting/ItemList JSON-LD con escape OWASP.

Nada acá accede a FastAPI request objects — las funciones reciben
dicts y devuelven strings. Eso las hace testables sin TestClient.
"""
from __future__ import annotations

import html
import json
import re
from typing import Any

from api.deps import DEFAULT_OG_IMAGE, SITE_URL, WEB_INDEX_PATH
from api.services.db import execute_fetch_all, execute_fetch_one
from api.services.formatters import (
    _descripcion_a_parrafos_html,
    _escape_attr,
    _format_fecha_larga,
    _format_renta_bruta,
    _slugify,
    _truncate_text,
    dias_restantes,
    resolve_institucion_sitio_web,
)
from api.services.sql import (
    ACTIVE_OFFER_SQL,
    STATUS_LEGACY_MAP,
    ofertas_base_sql,
    ofertas_select_sql,
)


# URLs estáticas que siempre figuran en el sitemap. Las rutas dinámicas
# (ofertas activas) se añaden desde la DB en el endpoint /sitemap.xml.
_STATIC_SITEMAP_URLS: tuple[tuple[str, str, str], ...] = (
    ("/", "1.0", "hourly"),
    ("/faq.html", "0.5", "monthly"),
    ("/guia-busqueda-empleo-publico.html", "0.7", "weekly"),
    ("/guia-postulacion-empleos-publicos.html", "0.7", "weekly"),
    ("/guia-preparacion-cv-sector-publico.html", "0.7", "weekly"),
    ("/guia-seguimiento-postulacion-publica.html", "0.7", "weekly"),
    ("/ruta-ingreso-empleo-publico.html", "0.7", "weekly"),
    ("/glosario-laboral-publico.html", "0.6", "monthly"),
    ("/formacion-sector-publico-chile.html", "0.6", "monthly"),
    ("/regimenes-laborales-sector-publico-chile.html", "0.6", "monthly"),
    ("/editorial.html", "0.4", "monthly"),
    ("/estadisticas.html", "0.5", "weekly"),
    ("/terminos.html", "0.3", "yearly"),
    ("/privacidad.html", "0.3", "yearly"),
    ("/descargo.html", "0.3", "yearly"),
)


# ── Landings SEO: region + sector ─────────────────────────────────────────
# Mapas estáticos para no depender de la API externa DPA en hot path.
# Los slugs se usan en `/empleos/region/{slug}` y `/empleos/sector/{slug}`.
# El `nombre_db` debe coincidir con lo que guardan los scrapers en la
# columna `ofertas.region` / `ofertas.sector` — si alguno difiere,
# agregar a `aliases` para que el query WHERE IN capture las variantes.

_LANDING_REGIONES: tuple[dict[str, Any], ...] = (
    {"slug": "arica-y-parinacota", "nombre": "Arica y Parinacota",
     "aliases": ("Arica y Parinacota", "Arica")},
    {"slug": "tarapaca", "nombre": "Tarapacá",
     "aliases": ("Tarapacá", "Tarapaca")},
    {"slug": "antofagasta", "nombre": "Antofagasta",
     "aliases": ("Antofagasta",)},
    {"slug": "atacama", "nombre": "Atacama",
     "aliases": ("Atacama",)},
    {"slug": "coquimbo", "nombre": "Coquimbo",
     "aliases": ("Coquimbo",)},
    {"slug": "valparaiso", "nombre": "Valparaíso",
     "aliases": ("Valparaíso", "Valparaiso")},
    {"slug": "metropolitana", "nombre": "Metropolitana de Santiago",
     "aliases": ("Metropolitana de Santiago", "Metropolitana",
                 "Región Metropolitana", "RM")},
    {"slug": "ohiggins", "nombre": "O'Higgins",
     "aliases": ("O'Higgins", "OHiggins", "Libertador General Bernardo O'Higgins")},
    {"slug": "maule", "nombre": "Maule",
     "aliases": ("Maule", "Del Maule")},
    {"slug": "nuble", "nombre": "Ñuble",
     "aliases": ("Ñuble", "Nuble")},
    {"slug": "biobio", "nombre": "Biobío",
     "aliases": ("Biobío", "Biobio", "Bío-Bío", "Bio-Bio")},
    {"slug": "araucania", "nombre": "La Araucanía",
     "aliases": ("La Araucanía", "Araucanía", "La Araucania", "Araucania")},
    {"slug": "los-rios", "nombre": "Los Ríos",
     "aliases": ("Los Ríos", "Los Rios")},
    {"slug": "los-lagos", "nombre": "Los Lagos",
     "aliases": ("Los Lagos",)},
    {"slug": "aysen", "nombre": "Aysén",
     "aliases": ("Aysén", "Aysen", "Aysén del General Carlos Ibáñez del Campo")},
    {"slug": "magallanes", "nombre": "Magallanes",
     "aliases": ("Magallanes", "Magallanes y de la Antártica Chilena")},
)

_LANDING_SECTORES: tuple[dict[str, Any], ...] = (
    {"slug": "municipal", "nombre": "Municipal",
     "aliases": ("Municipal", "Municipalidad")},
    {"slug": "ejecutivo", "nombre": "Ejecutivo Central",
     "aliases": ("Ejecutivo Central", "Ejecutivo")},
    {"slug": "salud", "nombre": "Salud Pública",
     "aliases": ("Salud Pública", "Salud", "Salud Publica")},
    {"slug": "educacion", "nombre": "Educación Superior",
     "aliases": ("Educación Superior", "Educación", "Educacion Superior", "Educacion")},
    {"slug": "gobierno-regional", "nombre": "Gobierno Regional",
     "aliases": ("Gobierno Regional",)},
    {"slug": "judicial", "nombre": "Judicial",
     "aliases": ("Judicial", "Poder Judicial")},
    {"slug": "ffaa-orden", "nombre": "FF.AA. y Orden",
     "aliases": ("FF.AA. y Orden", "FFAA", "FF.AA.", "Orden")},
    {"slug": "empresa-estado", "nombre": "Empresa del Estado",
     "aliases": ("Empresa del Estado", "Empresa Pública", "Empresa Publica")},
)

_REGION_BY_SLUG = {r["slug"]: r for r in _LANDING_REGIONES}
_SECTOR_BY_SLUG = {s["slug"]: s for s in _LANDING_SECTORES}


def _find_landing(tipo: str, slug: str) -> dict[str, Any] | None:
    if tipo == "region":
        return _REGION_BY_SLUG.get(slug)
    if tipo == "sector":
        return _SECTOR_BY_SLUG.get(slug)
    return None


def serialize_offer(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["dias_restantes"] = dias_restantes(data.get("fecha_cierre"))
    estado = str(data.get("estado") or "unknown").strip().lower()
    data["estado_normalizado"] = estado
    data["estado_legacy"] = STATUS_LEGACY_MAP.get(estado, "desconocido")
    # Expone el sitio web real de la institución (desde el catálogo JSON), para
    # que el frontend pueda resolver el logo correcto aunque la oferta venga
    # intermediada por Empleos Públicos u otros portales.
    data["institucion_sitio_web"] = resolve_institucion_sitio_web(
        data.get("institucion"), data.get("institucion_id")
    )
    return data


def _set_title(html_doc: str, title: str) -> str:
    safe = html.escape(title)
    if re.search(r"<title>.*?</title>", html_doc, flags=re.IGNORECASE | re.DOTALL):
        return re.sub(r"<title>.*?</title>", f"<title>{safe}</title>", html_doc, count=1, flags=re.IGNORECASE | re.DOTALL)
    return html_doc.replace("</head>", f"<title>{safe}</title>\n</head>", 1)


def _set_meta(html_doc: str, key: str, content: str, *, attr: str = "name") -> str:
    pattern = re.compile(
        rf'<meta\s+[^>]*{attr}\s*=\s*["\']{re.escape(key)}["\'][^>]*>',
        flags=re.IGNORECASE,
    )
    tag = f'<meta {attr}="{_escape_attr(key)}" content="{_escape_attr(content)}">'
    if pattern.search(html_doc):
        return pattern.sub(tag, html_doc, count=1)
    return html_doc.replace("</head>", f"{tag}\n</head>", 1)


def _set_canonical(html_doc: str, href: str) -> str:
    pattern = re.compile(r'<link\s+[^>]*rel\s*=\s*["\']canonical["\'][^>]*>', flags=re.IGNORECASE)
    tag = f'<link rel="canonical" href="{_escape_attr(href)}">'
    if pattern.search(html_doc):
        return pattern.sub(tag, html_doc, count=1)
    return html_doc.replace("</head>", f"{tag}\n</head>", 1)


def _inject_offer_path_bootstrap(html_doc: str, oferta_id: int | None) -> str:
    """Inyecta un `<meta>` con el id de la oferta para el SSR de /oferta/{id}.

    Antes esto inyectaba un ``<script>`` inline con
    ``window.__OFERTA_PATH_ID__ = ...`` + ``history.replaceState(...)``,
    pero CSP con ``script-src 'self'`` (sin ``'unsafe-inline'``) lo
    bloquearía. Ahora se emite un tag que ``web/app.js`` lee al cargar
    y se encarga de replicar el comportamiento (setear la global y
    empujar ``?oferta=ID`` a la URL).
    """
    if not oferta_id:
        return html_doc
    marker = 'name="x-oferta-id"'
    if marker in html_doc:
        return html_doc
    tag = f'<meta name="x-oferta-id" content="{int(oferta_id)}">'
    return html_doc.replace("</head>", f"{tag}\n</head>", 1)


def fetch_offer_for_meta(oferta_id: int) -> dict[str, Any] | None:
    sql = f"""
    WITH base AS (
        {ofertas_select_sql()}
        {ofertas_base_sql()}
        WHERE o.id = %s
    )
    SELECT * FROM base
    """
    row = execute_fetch_one(sql, [oferta_id])
    if not row:
        return None
    return serialize_offer(row)


def build_job_posting_jsonld(
    oferta: dict[str, Any], canonical_url: str
) -> str | None:
    """Genera el payload JSON-LD ``JobPosting`` para una oferta.

    Retorna ``None`` si faltan campos mínimos (cargo, institución, url_oferta
    absoluta, fecha_cierre, al menos ciudad o región). Reglas en
    ``web/JSONLD_VALIDACION.md``. El objetivo es que Google muestre el
    resultado enriquecido de empleo (rich result) en la SERP.
    """
    cargo = (oferta.get("cargo") or "").strip()
    institucion = (oferta.get("institucion") or "").strip()
    url_oferta = (oferta.get("url_oferta") or "").strip()
    fecha_cierre = oferta.get("fecha_cierre")
    ciudad = (oferta.get("ciudad") or "").strip()
    region = (oferta.get("region") or "").strip()

    if not cargo or not institucion:
        return None
    if not url_oferta or not url_oferta.startswith(("http://", "https://")):
        return None
    if fecha_cierre is None:
        return None
    if not ciudad and not region:
        return None

    def _iso(value: Any) -> str | None:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()[:10]
            except Exception:
                return None
        return str(value)[:10] or None

    valid_through = _iso(fecha_cierre)
    if not valid_through:
        return None
    date_posted = _iso(oferta.get("fecha_publicacion")) or valid_through

    address: dict[str, Any] = {"@type": "PostalAddress", "addressCountry": "CL"}
    if region:
        address["addressRegion"] = region
    if ciudad:
        address["addressLocality"] = ciudad

    data: dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": cargo,
        "description": (oferta.get("descripcion") or cargo).strip()[:4000],
        "datePosted": date_posted,
        "validThrough": valid_through,
        "url": url_oferta,
        "identifier": {
            "@type": "PropertyValue",
            "name": "estadoemplea",
            "value": str(oferta.get("id") or ""),
        },
        "mainEntityOfPage": canonical_url,
        "hiringOrganization": {
            "@type": "Organization",
            "name": institucion,
        },
        "jobLocation": {"@type": "Place", "address": address},
        "directApply": False,
    }

    tipo = (oferta.get("tipo_contrato") or "").strip().upper()
    if tipo:
        # Mapeo conservador al vocabulario de schema.org — si no reconocemos
        # el tipo, lo omitimos (en vez de inventar un valor inválido).
        mapeo = {
            "PLANTA": "FULL_TIME",
            "CONTRATA": "FULL_TIME",
            "CODIGO_TRABAJO": "FULL_TIME",
            "CÓDIGO_TRABAJO": "FULL_TIME",
            "HONORARIOS": "CONTRACTOR",
            "REEMPLAZO": "TEMPORARY",
        }
        if tipo in mapeo:
            data["employmentType"] = mapeo[tipo]

    jornada = (oferta.get("jornada") or "").strip()
    if jornada:
        data["workHours"] = jornada

    rmin = oferta.get("renta_bruta_min")
    rmax = oferta.get("renta_bruta_max")
    if isinstance(rmin, int) and rmin > 0:
        value: dict[str, Any] = {
            "@type": "QuantitativeValue",
            "unitText": "MONTH",
        }
        if isinstance(rmax, int) and rmax > rmin:
            value["minValue"] = rmin
            value["maxValue"] = rmax
        else:
            value["value"] = rmin
        data["baseSalary"] = {
            "@type": "MonetaryAmount",
            "currency": "CLP",
            "value": value,
        }

    # JSON embebido en <script type="application/ld+json">: escapar
    # `<`, `>`, `&` y `'` a sus secuencias unicode para que el parser
    # JSON los lea igual y el parser HTML no pueda terminar el tag
    # (vector `</script>` dentro de un campo de texto). OWASP JSON-in-HTML.
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return (
        payload
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def build_offer_ssr_html(oferta: dict[str, Any]) -> str:
    """Genera el bloque HTML visible para crawlers de una oferta.

    Antes, `/oferta/{id}-{slug}` devolvía el SPA vacío con meta tags y
    JSON-LD. Google sabía *de qué* era la página pero no veía contenido
    en el body — el cargo, la institución y la descripción se inyectaban
    con JS al abrir el modal. Con este bloque, el crawler recibe el
    contenido real en el HTML inicial; el CSS en `web/styles/index.css`
    lo oculta cuando JS está activo (regla ``html.js-nav .oferta-ssr``).
    """
    cargo = (oferta.get("cargo") or "").strip() or "Oferta pública"
    institucion = (oferta.get("institucion") or "").strip()
    region = (oferta.get("region") or "").strip()
    ciudad = (oferta.get("ciudad") or "").strip()
    tipo = (oferta.get("tipo_contrato") or "").strip()
    jornada = (oferta.get("jornada") or "").strip()
    renta = _format_renta_bruta(oferta) or ""
    fecha_pub = _format_fecha_larga(oferta.get("fecha_publicacion")) or ""
    fecha_cie = _format_fecha_larga(oferta.get("fecha_cierre")) or ""
    descripcion_html = _descripcion_a_parrafos_html(oferta.get("descripcion") or "")
    url_oferta = (oferta.get("url_oferta") or "").strip()

    partes: list[str] = [
        '<article class="oferta-ssr" data-oferta-ssr="true" aria-labelledby="ssr-oferta-titulo">',
        '  <header class="oferta-ssr-header">',
        '    <p class="oferta-ssr-kicker">Oferta pública</p>',
        f'    <h1 id="ssr-oferta-titulo">{html.escape(cargo)}</h1>',
    ]
    if institucion:
        partes.append(
            f'    <p class="oferta-ssr-institucion">{html.escape(institucion)}</p>'
        )
    partes.append("  </header>")

    meta_items: list[tuple[str, str]] = []
    ubicacion = " · ".join(x for x in (region, ciudad) if x)
    if ubicacion:
        meta_items.append(("Ubicación", ubicacion))
    if tipo:
        meta_items.append(("Tipo de contrato", tipo.capitalize()))
    if jornada:
        meta_items.append(("Jornada", jornada))
    if renta:
        meta_items.append(("Renta bruta", renta))
    if fecha_pub:
        meta_items.append(("Publicación", fecha_pub))
    if fecha_cie:
        meta_items.append(("Cierre de postulaciones", fecha_cie))
    if meta_items:
        partes.append('  <dl class="oferta-ssr-meta">')
        for label, value in meta_items:
            partes.append(
                f"    <dt>{html.escape(label)}</dt>"
                f"<dd>{html.escape(value)}</dd>"
            )
        partes.append("  </dl>")

    if descripcion_html:
        partes.append('  <section class="oferta-ssr-descripcion">')
        partes.append("    <h2>Descripción de la oferta</h2>")
        partes.append(f"    {descripcion_html}")
        partes.append("  </section>")

    if url_oferta.startswith(("http://", "https://")):
        partes.append(
            f'  <p class="oferta-ssr-cta"><a href="{html.escape(url_oferta)}"'
            ' rel="nofollow noopener" target="_blank">Ir a postular en el sitio oficial →</a></p>'
        )
    partes.append("</article>")
    return "\n".join(partes)


# ── Landings SEO (region / sector) — SSR + meta + sitemap ─────────────────


def fetch_landing_ofertas(
    tipo: str, aliases: tuple[str, ...], limite: int = 30
) -> list[dict[str, Any]]:
    """Top N ofertas activas que matchean cualquiera de los aliases."""
    if tipo not in {"region", "sector"}:
        return []
    columna = "region" if tipo == "region" else "sector"
    rows = execute_fetch_all(
        f"""
        SELECT
            o.id,
            o.cargo,
            COALESCE(NULLIF(o.institucion_nombre, ''), 'Institución pública') AS institucion,
            o.ciudad,
            o.region,
            o.fecha_cierre,
            COALESCE(o.actualizada_en, o.fecha_scraped, o.detectada_en, o.creada_en) AS lastmod
        FROM ofertas o
        WHERE {ACTIVE_OFFER_SQL}
          AND o.{columna} IN %s
        ORDER BY o.fecha_cierre ASC NULLS LAST, o.id DESC
        LIMIT %s
        """,
        [tuple(aliases), limite],
    )
    return rows


def fetch_landing_total(tipo: str, aliases: tuple[str, ...]) -> int:
    if tipo not in {"region", "sector"}:
        return 0
    columna = "region" if tipo == "region" else "sector"
    row = execute_fetch_one(
        f"""
        SELECT COUNT(*) AS total
        FROM ofertas o
        WHERE {ACTIVE_OFFER_SQL}
          AND o.{columna} IN %s
        """,
        [tuple(aliases)],
    )
    return int(row["total"]) if row else 0


def build_landing_meta(
    tipo: str, nombre: str, total: int, canonical_url: str
) -> dict[str, str]:
    """Meta tags de la landing (title, description, og, twitter)."""
    tipo_humano = {"region": "la región de", "sector": "el sector"}.get(tipo, "")
    title = f"Empleos públicos en {nombre} — estadoemplea"
    if total > 0:
        description = (
            f"{total} ofertas activas del sector público chileno en {tipo_humano} "
            f"{nombre}. Filtra por institución, cargo y renta. Actualizado a diario."
        )
    else:
        description = (
            f"Empleos públicos en {tipo_humano} {nombre}. Suscríbete a alertas "
            f"para recibir las nuevas ofertas apenas se publiquen."
        )
    return {
        "title": _truncate_text(title, 90),
        "description": _truncate_text(description, 200),
        "og_image": DEFAULT_OG_IMAGE,
        "canonical": canonical_url,
    }


def build_landing_ssr_html(
    tipo: str,
    nombre: str,
    slug: str,
    total: int,
    ofertas: list[dict[str, Any]],
) -> str:
    """Bloque HTML visible con listado y enlaces cruzados."""
    tipo_humano = {"region": "región", "sector": "sector"}.get(tipo, tipo)
    partes: list[str] = [
        '<article class="landing-ssr" data-landing-ssr="true" '
        f'data-tipo="{html.escape(tipo)}" aria-labelledby="ssr-landing-titulo">',
        '  <header class="landing-ssr-header">',
        f'    <p class="landing-ssr-kicker">Empleos públicos · {html.escape(tipo_humano)}</p>',
        f'    <h1 id="ssr-landing-titulo">Empleos públicos en {html.escape(nombre)}</h1>',
        f'    <p class="landing-ssr-resumen">'
        f'<strong>{total}</strong> oferta{"s" if total != 1 else ""} activa{"s" if total != 1 else ""} '
        f'en {html.escape(tipo_humano)} {html.escape(nombre)}.</p>',
        "  </header>",
    ]

    if ofertas:
        partes.append('  <section class="landing-ssr-lista">')
        partes.append("    <h2>Ofertas vigentes</h2>")
        partes.append("    <ol>")
        for o in ofertas:
            slug_cargo = _slugify(o.get("cargo") or "")
            href = f"/oferta/{o['id']}" + (f"-{slug_cargo}" if slug_cargo else "")
            cargo = html.escape((o.get("cargo") or "Oferta pública").strip())
            institucion = html.escape((o.get("institucion") or "").strip())
            ciudad = html.escape((o.get("ciudad") or "").strip())
            cierre = _format_fecha_larga(o.get("fecha_cierre")) or "Sin fecha de cierre"
            loc_str = f" · {ciudad}" if ciudad else ""
            partes.append(
                f'      <li><a href="{href}"><strong>{cargo}</strong></a>'
                f' — {institucion}{loc_str} · '
                f'<span class="landing-ssr-cierre">{html.escape(cierre)}</span></li>'
            )
        partes.append("    </ol>")
        # Buscador con filtro pre-aplicado
        query_param = "region" if tipo == "region" else "sector"
        query_value = ofertas[0].get(tipo if tipo == "region" else None) or nombre
        # Preferir el nombre canónico (más URL-safe)
        query_value = nombre
        partes.append(
            f'    <p class="landing-ssr-cta"><a href="/?{query_param}={html.escape(query_value)}">'
            f'Ver todas las ofertas en el buscador →</a></p>'
        )
        partes.append("  </section>")
    else:
        partes.append('  <section class="landing-ssr-vacio">')
        partes.append(
            f"    <p>Hoy no hay ofertas activas en {html.escape(tipo_humano)} "
            f"{html.escape(nombre)}. Suscríbete a una alerta y recibiremos las nuevas "
            f"por email apenas se publiquen.</p>"
        )
        partes.append('    <p><a href="/#alertas">Crear alerta gratuita</a></p>')
        partes.append("  </section>")

    # Enlaces cruzados al otro eje (sector si estás en region, region si
    # estás en sector) para dar descubrimiento interno y autoridad SEO.
    partes.append('  <nav class="landing-ssr-cruce" aria-label="Otras landings">')
    if tipo == "region":
        partes.append("    <h2>Empleos por sector</h2>")
        partes.append("    <ul>")
        for s in _LANDING_SECTORES:
            partes.append(
                f'      <li><a href="/empleos/sector/{s["slug"]}">Sector {html.escape(s["nombre"])}</a></li>'
            )
        partes.append("    </ul>")
    else:
        partes.append("    <h2>Empleos por región</h2>")
        partes.append("    <ul>")
        for r in _LANDING_REGIONES:
            partes.append(
                f'      <li><a href="/empleos/region/{r["slug"]}">Región de {html.escape(r["nombre"])}</a></li>'
            )
        partes.append("    </ul>")
    partes.append("  </nav>")
    partes.append("</article>")
    return "\n".join(partes)


def build_landing_itemlist_jsonld(
    ofertas: list[dict[str, Any]], canonical_url: str
) -> str | None:
    """JSON-LD ItemList que referencia las ofertas vigentes de la landing."""
    if not ofertas:
        return None
    items: list[dict[str, Any]] = []
    for idx, o in enumerate(ofertas, start=1):
        slug_cargo = _slugify(o.get("cargo") or "")
        href = f"{SITE_URL}/oferta/{o['id']}"
        if slug_cargo:
            href += f"-{slug_cargo}"
        items.append({
            "@type": "ListItem",
            "position": idx,
            "url": href,
            "name": (o.get("cargo") or "").strip() or "Oferta pública",
        })
    data = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "url": canonical_url,
        "numberOfItems": len(items),
        "itemListElement": items,
    }
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return (
        payload
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def build_offer_meta(oferta: dict[str, Any] | None, canonical_url: str) -> dict[str, str]:
    if not oferta:
        return {
            "title": "estadoemplea.cl — Empleos públicos vigentes en Chile",
            "description": "Encuentra empleos públicos en Chile, filtra por institución y revisa oportunidades del sector público.",
            "og_image": DEFAULT_OG_IMAGE,
            "canonical": canonical_url,
        }

    cargo = (oferta.get("cargo") or "Oferta laboral").strip()
    institucion = (oferta.get("institucion") or "Institución pública").strip()
    ciudad = (oferta.get("ciudad") or "").strip()
    region = (oferta.get("region") or "").strip()
    tipo = (oferta.get("tipo_contrato") or "").strip()
    cierre = _format_fecha_larga(oferta.get("fecha_cierre"))
    estado = (oferta.get("estado") or "").strip()
    renta = _format_renta_bruta(oferta)

    title = _truncate_text(f"{cargo} – {institucion}", 90)
    desc_parts = []
    if region:
        desc_parts.append(region)
    if ciudad and ciudad.lower() not in region.lower():
        desc_parts.append(ciudad)
    if tipo:
        desc_parts.append(tipo.capitalize())
    if renta:
        desc_parts.append(renta)
    if cierre:
        desc_parts.append(f"Cierre: {cierre}")
    elif estado:
        desc_parts.append(f"Estado: {estado}")
    description = _truncate_text(" · ".join(desc_parts) or "Revisa requisitos, renta y plazos de postulación.", 200)
    oferta_id = oferta.get("id")
    image_url = f"{SITE_URL}/api/og/{oferta_id}.png" if oferta_id else DEFAULT_OG_IMAGE

    return {
        "title": title,
        "description": description,
        "og_image": image_url,
        "canonical": canonical_url,
    }


def render_index_with_meta(
    meta: dict[str, str],
    *,
    oferta_id_for_bootstrap: int | None = None,
    oferta: dict[str, Any] | None = None,
    landing_html: str | None = None,
    landing_jsonld: str | None = None,
) -> str:
    html_doc = WEB_INDEX_PATH.read_text(encoding="utf-8")
    html_doc = _set_title(html_doc, meta["title"])
    html_doc = _set_meta(html_doc, "description", meta["description"], attr="name")
    html_doc = _set_meta(html_doc, "og:title", meta["title"], attr="property")
    html_doc = _set_meta(html_doc, "og:description", meta["description"], attr="property")
    html_doc = _set_meta(html_doc, "og:url", meta["canonical"], attr="property")
    html_doc = _set_meta(html_doc, "og:image", meta["og_image"], attr="property")
    # Hints explícitos: algunos crawlers (WhatsApp, Slack) fallan a summary
    # pequeño si no encuentran dimensiones declaradas.
    html_doc = _set_meta(html_doc, "og:image:width", "1200", attr="property")
    html_doc = _set_meta(html_doc, "og:image:height", "630", attr="property")
    html_doc = _set_meta(html_doc, "og:image:alt", meta["title"], attr="property")
    html_doc = _set_meta(html_doc, "og:type", "website", attr="property")
    html_doc = _set_meta(html_doc, "twitter:card", "summary_large_image", attr="name")
    html_doc = _set_meta(html_doc, "twitter:title", meta["title"], attr="name")
    html_doc = _set_meta(html_doc, "twitter:description", meta["description"], attr="name")
    html_doc = _set_meta(html_doc, "twitter:image", meta["og_image"], attr="name")
    html_doc = _set_meta(html_doc, "twitter:image:alt", meta["title"], attr="name")
    html_doc = _set_canonical(html_doc, meta["canonical"])
    html_doc = _inject_offer_path_bootstrap(html_doc, oferta_id_for_bootstrap)
    if oferta:
        # JSON-LD JobPosting server-side: antes sólo se generaba con JS en
        # el cliente, y Google no lo indexaba de forma fiable. Se inyecta
        # como <script type="application/ld+json"> dentro de <head>.
        jsonld = build_job_posting_jsonld(oferta, meta["canonical"])
        if jsonld:
            tag = (
                '<script type="application/ld+json" data-jobposting="ssr">'
                f"{jsonld}"
                "</script>"
            )
            html_doc = html_doc.replace("</head>", f"{tag}\n</head>", 1)
        # Contenido visible para crawlers. El placeholder está en
        # web/index.html dentro de <main>; si no existe (fallback), se
        # inyecta después del <main ...> opening tag como último recurso.
        ssr_html = build_offer_ssr_html(oferta)
        placeholder = "<!-- SSR_OFFER_SLOT_START --><!-- SSR_OFFER_SLOT_END -->"
        if placeholder in html_doc:
            html_doc = html_doc.replace(placeholder, ssr_html, 1)
        else:
            html_doc = re.sub(
                r'(<main[^>]*id="contenido-principal"[^>]*>)',
                rf"\1\n{ssr_html}",
                html_doc,
                count=1,
            )
    # Landings SEO (region / sector): reusan el mismo slot del SSR de oferta.
    # Son mutuamente excluyentes — una URL es /oferta/... o /empleos/... —
    # así que no hay colisión.
    if landing_html:
        if landing_jsonld:
            tag = (
                '<script type="application/ld+json" data-itemlist="ssr">'
                f"{landing_jsonld}"
                "</script>"
            )
            html_doc = html_doc.replace("</head>", f"{tag}\n</head>", 1)
        placeholder = "<!-- SSR_OFFER_SLOT_START --><!-- SSR_OFFER_SLOT_END -->"
        if placeholder in html_doc:
            html_doc = html_doc.replace(placeholder, landing_html, 1)
        else:
            html_doc = re.sub(
                r'(<main[^>]*id="contenido-principal"[^>]*>)',
                rf"\1\n{landing_html}",
                html_doc,
                count=1,
            )
    return html_doc


_OFFER_PATH_RE = re.compile(r"^(?P<id>\d+)(?:-(?P<slug>[a-z0-9-]*))?/?$")


_INSTITUCION_PATH_RE = re.compile(r"^(?P<id>\d+)(?:-(?P<slug>[a-z0-9-]*))?/?$")


def fetch_institucion_para_landing(inst_id: int) -> dict[str, Any] | None:
    """Datos de una institución para construir la landing."""
    row = execute_fetch_one(
        """
        SELECT id, nombre,
               COALESCE(NULLIF(TRIM(sigla), ''), '') AS sigla,
               COALESCE(NULLIF(TRIM(sector), ''), '') AS sector,
               COALESCE(NULLIF(TRIM(region), ''), '') AS region
        FROM instituciones
        WHERE id = %s
        """,
        [inst_id],
    )
    return row


def fetch_institucion_ofertas(inst_id: int, limite: int = 30) -> list[dict[str, Any]]:
    return execute_fetch_all(
        f"""
        SELECT
            o.id, o.cargo,
            COALESCE(NULLIF(o.institucion_nombre, ''), 'Institución pública') AS institucion,
            o.ciudad, o.region,
            o.fecha_cierre,
            COALESCE(o.actualizada_en, o.fecha_scraped, o.detectada_en, o.creada_en) AS lastmod
        FROM ofertas o
        WHERE {ACTIVE_OFFER_SQL}
          AND o.institucion_id = %s
        ORDER BY o.fecha_cierre ASC NULLS LAST, o.id DESC
        LIMIT %s
        """,
        [inst_id, limite],
    )


def fetch_institucion_total(inst_id: int) -> int:
    row = execute_fetch_one(
        f"""
        SELECT COUNT(*) AS total
        FROM ofertas o
        WHERE {ACTIVE_OFFER_SQL}
          AND o.institucion_id = %s
        """,
        [inst_id],
    )
    return int(row["total"]) if row else 0


def build_institucion_meta(
    inst: dict[str, Any], total: int, canonical_url: str
) -> dict[str, str]:
    nombre = inst.get("nombre") or "Institución pública"
    title = f"Empleos públicos en {nombre} — estadoemplea"
    if total > 0:
        description = (
            f"{total} oferta{'s' if total != 1 else ''} activa{'s' if total != 1 else ''} "
            f"del sector público chileno en {nombre}. "
            "Filtra por cargo, región y renta; postula directamente al sitio oficial."
        )
    else:
        description = (
            f"Empleos públicos en {nombre}. Suscríbete a alertas y recibe las "
            "nuevas ofertas apenas se publiquen."
        )
    return {
        "title": _truncate_text(title, 90),
        "description": _truncate_text(description, 200),
        "og_image": DEFAULT_OG_IMAGE,
        "canonical": canonical_url,
    }


def build_institucion_ssr_html(
    inst: dict[str, Any],
    total: int,
    ofertas: list[dict[str, Any]],
) -> str:
    nombre = inst.get("nombre") or "Institución pública"
    sigla = inst.get("sigla") or ""
    sector = inst.get("sector") or ""
    region_inst = inst.get("region") or ""
    partes: list[str] = [
        '<article class="landing-ssr" data-landing-ssr="true" '
        'data-tipo="institucion" aria-labelledby="ssr-landing-titulo">',
        '  <header class="landing-ssr-header">',
        '    <p class="landing-ssr-kicker">Empleos públicos · Institución</p>',
        f'    <h1 id="ssr-landing-titulo">{html.escape(nombre)}'
        + (f' <span class="landing-ssr-sigla">({html.escape(sigla)})</span>' if sigla else "")
        + "</h1>",
    ]
    # Sub-resumen con contexto de la institución (sector + region si están)
    sub_parts: list[str] = []
    if sector:
        sub_parts.append(f"Sector {html.escape(sector)}")
    if region_inst:
        sub_parts.append(f"Región {html.escape(region_inst)}")
    if sub_parts:
        partes.append(f'    <p class="landing-ssr-sub">{" · ".join(sub_parts)}</p>')
    partes.append(
        f'    <p class="landing-ssr-resumen"><strong>{total}</strong> '
        f'oferta{"s" if total != 1 else ""} activa{"s" if total != 1 else ""} '
        f'en {html.escape(nombre)}.</p>'
    )
    partes.append("  </header>")

    if ofertas:
        partes.append('  <section class="landing-ssr-lista">')
        partes.append("    <h2>Ofertas vigentes</h2>")
        partes.append("    <ol>")
        for o in ofertas:
            slug_cargo = _slugify(o.get("cargo") or "")
            href = f"/oferta/{o['id']}" + (f"-{slug_cargo}" if slug_cargo else "")
            cargo = html.escape((o.get("cargo") or "Oferta pública").strip())
            ciudad = html.escape((o.get("ciudad") or "").strip())
            region = html.escape((o.get("region") or "").strip())
            loc = " · ".join(x for x in (region, ciudad) if x)
            cierre = _format_fecha_larga(o.get("fecha_cierre")) or "Sin fecha de cierre"
            partes.append(
                f'      <li><a href="{href}"><strong>{cargo}</strong></a>'
                + (f" — {loc}" if loc else "")
                + f' · <span class="landing-ssr-cierre">{html.escape(cierre)}</span>'
                + "</li>"
            )
        partes.append("    </ol>")
        partes.append(
            f'    <p class="landing-ssr-cta"><a href="/?institucion={inst["id"]}">'
            "Ver todas las ofertas en el buscador →</a></p>"
        )
        partes.append("  </section>")
    else:
        partes.append('  <section class="landing-ssr-vacio">')
        partes.append(
            f"    <p>Hoy no hay ofertas activas en {html.escape(nombre)}. Suscríbete a "
            "una alerta y recibe las nuevas ofertas por email apenas se publiquen.</p>"
        )
        partes.append('    <p><a href="/#alertas">Crear alerta gratuita</a></p>')
        partes.append("  </section>")

    # Cross-links al sector y región propios + "todas las instituciones"
    partes.append('  <nav class="landing-ssr-cruce" aria-label="Relacionados">')
    partes.append("    <h2>También te puede interesar</h2>")
    partes.append("    <ul>")
    # Región propia de la institución (si la conocemos)
    if region_inst:
        for reg in _LANDING_REGIONES:
            if region_inst in reg["aliases"]:
                partes.append(
                    f'      <li><a href="/empleos/region/{reg["slug"]}">'
                    f'Empleos en {html.escape(reg["nombre"])}</a></li>'
                )
                break
    # Sector propio (si matchea alguno del mapa)
    if sector:
        for sec in _LANDING_SECTORES:
            if sector in sec["aliases"]:
                partes.append(
                    f'      <li><a href="/empleos/sector/{sec["slug"]}">'
                    f'Empleos en el sector {html.escape(sec["nombre"])}</a></li>'
                )
                break
    partes.append('      <li><a href="/">Ver todas las ofertas</a></li>')
    partes.append("    </ul>")
    partes.append("  </nav>")
    partes.append("</article>")
    return "\n".join(partes)
