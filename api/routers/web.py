"""Endpoints SSR y legacy redirects (renderizan HTML para navegadores/crawlers).

Rutas incluidas:

- `/` — home (HTML con meta default, o JSON si Accept = application/json).
- `/health` — healthcheck simple (usa DB, devuelve 503 si no responde).
- `/sitemap.xml` — sitemap dinámico con ofertas activas + landings +
  URLs estáticas.
- `/index.html` y `/web/index.html` — legacy redirects al canónico.
- `/oferta/{id}-{slug}` — detalle de oferta con SSR completo + JSON-LD
  JobPosting. Redirige 301 al slug canónico.
- `/empleos/region/{slug}` · `/empleos/sector/{slug}` · `/empleos/institucion/{id}-{slug}`
  — landings SEO con SSR + ItemList JSON-LD.
- `/share/oferta/{id}` — redirect 308 al canónico (para short links).

Todos los endpoints leen de la DB via `api.services.db` y construyen
la respuesta con `api.services.seo`. Nada de lógica inline — el router
sólo orquesta.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from urllib.parse import urlencode
import html

from api.deps import DEFAULT_OG_IMAGE, SITE_URL, WEB_INDEX_PATH
from api.services.db import DB_CONFIG, execute_fetch_all, execute_fetch_one
from api.services.formatters import _slugify
from api.services.sql import ACTIVE_OFFER_SQL
from api.services.seo import (
    _INSTITUCION_PATH_RE,
    _LANDING_REGIONES,
    _LANDING_SECTORES,
    _OFFER_PATH_RE,
    _STATIC_SITEMAP_URLS,
    _find_landing,
    build_institucion_meta,
    build_institucion_ssr_html,
    build_landing_itemlist_jsonld,
    build_landing_meta,
    build_landing_ssr_html,
    build_offer_meta,
    fetch_institucion_ofertas,
    fetch_institucion_para_landing,
    fetch_institucion_total,
    fetch_landing_ofertas,
    fetch_landing_total,
    fetch_offer_for_meta,
    render_index_with_meta,
)

logger = logging.getLogger("api.routers.web")

router = APIRouter(tags=["web"])


@router.get("/web/index.html", response_class=HTMLResponse, include_in_schema=False)
def web_index(oferta: int | None = Query(None, ge=1)) -> HTMLResponse:
    canonical = f"{SITE_URL}/web/index.html"
    if oferta:
        canonical = f"{SITE_URL}/oferta/{oferta}"
    oferta_data = fetch_offer_for_meta(oferta) if oferta else None
    meta = build_offer_meta(oferta_data, canonical_url=canonical)
    html_doc = render_index_with_meta(meta, oferta_id_for_bootstrap=oferta)
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=60, stale-while-revalidate=600"},
    )


@router.get("/oferta/{path:path}", response_class=HTMLResponse, include_in_schema=False)
def web_offer(path: str) -> Response:
    """Sirve la SPA con meta tags + JSON-LD inyectados para una oferta.

    URLs aceptadas:
        /oferta/42                           → 301 a /oferta/42-slug-canonico
        /oferta/42-slug-obsoleto             → 301 a /oferta/42-slug-canonico
        /oferta/42-slug-canonico             → 200 con HTML pre-renderizado

    El slug se deriva del ``cargo`` y es sólo cosmético para SEO y para
    humanos que lean el URL. El ``id`` es la identidad real.
    """
    match = _OFFER_PATH_RE.match(path.strip())
    if not match:
        raise HTTPException(status_code=404, detail="Oferta no encontrada")
    oferta_id = int(match.group("id"))
    slug_actual = match.group("slug") or ""

    oferta_data = fetch_offer_for_meta(oferta_id)
    if not oferta_data:
        raise HTTPException(status_code=404, detail="Oferta no encontrada")

    slug_canonico = _slugify(oferta_data.get("cargo") or "")
    path_canonico = f"/oferta/{oferta_id}"
    if slug_canonico:
        path_canonico += f"-{slug_canonico}"

    if slug_actual != slug_canonico:
        # 301 permanente: el canónico para Google es la versión con slug.
        return RedirectResponse(url=path_canonico, status_code=301)

    canonical = f"{SITE_URL}{path_canonico}"
    meta = build_offer_meta(oferta_data, canonical_url=canonical)
    html_doc = render_index_with_meta(
        meta,
        oferta_id_for_bootstrap=oferta_id,
        oferta=oferta_data,
    )
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=120, stale-while-revalidate=900"},
    )


@router.get("/share/oferta/{oferta_id}", include_in_schema=False)
def web_offer_share(oferta_id: int) -> RedirectResponse:
    # Delegamos al canonical builder: primero va a /oferta/{id} (sin slug),
    # que a su vez responde 301 al slug canónico. Así mantenemos una sola
    # fuente de verdad.
    return RedirectResponse(url=f"/oferta/{oferta_id}", status_code=308)


def _render_landing(tipo: str, slug: str) -> Response:
    """Endpoint compartido para /empleos/region/{slug} y /empleos/sector/{slug}."""
    landing = _find_landing(tipo, slug)
    if not landing:
        raise HTTPException(status_code=404, detail=f"{tipo.capitalize()} no encontrada")

    aliases = tuple(landing["aliases"])
    try:
        total = fetch_landing_total(tipo, aliases)
        ofertas = fetch_landing_ofertas(tipo, aliases, limite=30) if total else []
    except Exception:
        logger.exception("Error armando landing %s/%s", tipo, slug)
        total, ofertas = 0, []

    canonical = f"{SITE_URL}/empleos/{tipo}/{slug}"
    meta = build_landing_meta(tipo, landing["nombre"], total, canonical)
    landing_html = build_landing_ssr_html(
        tipo, landing["nombre"], slug, total, ofertas
    )
    landing_jsonld = build_landing_itemlist_jsonld(ofertas, canonical)

    html_doc = render_index_with_meta(
        meta,
        landing_html=landing_html,
        landing_jsonld=landing_jsonld,
    )
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=300, stale-while-revalidate=1800"},
    )


@router.get("/empleos/region/{slug}", response_class=HTMLResponse, include_in_schema=False)
def web_landing_region(slug: str) -> Response:
    return _render_landing("region", slug)


@router.get("/empleos/sector/{slug}", response_class=HTMLResponse, include_in_schema=False)
def web_landing_sector(slug: str) -> Response:
    return _render_landing("sector", slug)


# ── Landing por institución: /empleos/institucion/{id}-{slug} ─────────────
# Mismo patrón que /oferta/{id}-{slug}: el id es canonical, el slug es
# cosmético para SEO y legibilidad. Si el slug no matchea el canónico
# derivado del nombre, responde 301. Si la institución no existe o no
# tiene ofertas (activa en `instituciones` o id ausente), 404.

@router.get(
    "/empleos/institucion/{path:path}",
    response_class=HTMLResponse,
    include_in_schema=False,
)
def web_landing_institucion(path: str) -> Response:
    """Landing SEO por institución. URL canónica `/empleos/institucion/{id}-{slug}`.

    `/empleos/institucion/42` (sin slug) → 301 al canónico.
    `/empleos/institucion/42-slug-viejo` → 301 si el slug no matchea el
    derivado del nombre.
    """
    match = _INSTITUCION_PATH_RE.match(path.strip())
    if not match:
        raise HTTPException(status_code=404, detail="Institución no encontrada")
    inst_id = int(match.group("id"))
    slug_actual = match.group("slug") or ""

    inst = fetch_institucion_para_landing(inst_id)
    if not inst:
        raise HTTPException(status_code=404, detail="Institución no encontrada")

    slug_canonico = _slugify(inst.get("nombre") or "")
    path_canonico = f"/empleos/institucion/{inst_id}"
    if slug_canonico:
        path_canonico += f"-{slug_canonico}"
    if slug_actual != slug_canonico:
        return RedirectResponse(url=path_canonico, status_code=301)

    try:
        total = fetch_institucion_total(inst_id)
        ofertas = fetch_institucion_ofertas(inst_id, limite=30) if total else []
    except Exception:
        logger.exception("Error armando landing institucion/%s", inst_id)
        total, ofertas = 0, []

    canonical = f"{SITE_URL}{path_canonico}"
    meta = build_institucion_meta(inst, total, canonical)
    landing_html = build_institucion_ssr_html(inst, total, ofertas)
    landing_jsonld = build_landing_itemlist_jsonld(ofertas, canonical)

    html_doc = render_index_with_meta(
        meta,
        landing_html=landing_html,
        landing_jsonld=landing_jsonld,
    )
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=300, stale-while-revalidate=1800"},
    )


@router.get("/index.html", include_in_schema=False)
def legacy_index_redirect(request: Request) -> RedirectResponse:
    query = f"?{urlencode(list(request.query_params.multi_items()))}" if request.query_params else ""
    return RedirectResponse(url=f"/web/index.html{query}", status_code=308)


@router.get("/health", response_model=None)
def health() -> dict[str, Any] | JSONResponse:
    try:
        row = execute_fetch_one("SELECT NOW() AS ts")
        return {"status": "ok", "db": str(row["ts"]) if row else None}
    except Exception as exc:  # pragma: no cover
        logger.warning("Healthcheck sin DB: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "detail": "database_unavailable"},
        )


@router.get("/sitemap.xml", include_in_schema=False)
def sitemap_xml() -> Response:
    """Sitemap dinámico con URLs estáticas + una entrada por oferta activa.

    Las URLs siempre apuntan a ``SITE_URL`` (frontend en Cloudflare Pages),
    aunque el sitemap se sirva desde el backend en Railway. Google/Bing
    aceptan sitemaps cross-host siempre que ambos dominios estén
    verificados en Search Console.

    Tope de 45 000 URLs (dentro del límite oficial de 50 000). Si algún
    día hay más ofertas activas, se parte en un sitemap-index paginado.
    """
    try:
        rows = execute_fetch_all(
            f"""
            SELECT
                o.id,
                o.cargo,
                COALESCE(o.actualizada_en, o.fecha_scraped, o.detectada_en, o.creada_en) AS lastmod
            FROM ofertas o
            WHERE {ACTIVE_OFFER_SQL}
            ORDER BY o.id DESC
            LIMIT 45000
            """
        )
    except Exception:
        logger.exception("No se pudo leer ofertas para sitemap; devolviendo solo estáticas")
        rows = []

    hoy = date.today().isoformat()
    partes: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path, priority, changefreq in _STATIC_SITEMAP_URLS:
        partes.append(
            f"  <url><loc>{html.escape(SITE_URL + path)}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>{changefreq}</changefreq>"
            f"<priority>{priority}</priority></url>"
        )
    # Landings SEO (16 regiones + 8 sectores).
    for reg in _LANDING_REGIONES:
        partes.append(
            f"  <url><loc>{html.escape(SITE_URL)}/empleos/region/{reg['slug']}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.8</priority></url>"
        )
    for sec in _LANDING_SECTORES:
        partes.append(
            f"  <url><loc>{html.escape(SITE_URL)}/empleos/sector/{sec['slug']}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.8</priority></url>"
        )
    # Landings por institución: sólo las que tienen ≥1 oferta activa hoy.
    # Evita indexar landings vacías que Google podría marcar como thin.
    try:
        inst_rows = execute_fetch_all(
            f"""
            SELECT i.id, i.nombre
            FROM instituciones i
            WHERE EXISTS (
                SELECT 1 FROM ofertas o
                WHERE o.institucion_id = i.id
                  AND {ACTIVE_OFFER_SQL}
            )
            ORDER BY i.nombre
            """
        )
    except Exception:
        logger.exception("No se pudo leer instituciones para sitemap; saltando.")
        inst_rows = []
    for inst in inst_rows:
        slug = _slugify(inst.get("nombre") or "")
        loc = f"{SITE_URL}/empleos/institucion/{inst['id']}"
        if slug:
            loc += f"-{slug}"
        partes.append(
            f"  <url><loc>{html.escape(loc)}</loc>"
            f"<lastmod>{hoy}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.6</priority></url>"
        )
    for row in rows:
        slug = _slugify(row.get("cargo"))
        loc = f"{SITE_URL}/oferta/{row['id']}" + (f"-{slug}" if slug else "")
        raw_lastmod = row.get("lastmod")
        if raw_lastmod is None:
            lastmod_str = hoy
        elif hasattr(raw_lastmod, "date"):
            lastmod_str = raw_lastmod.date().isoformat()
        else:
            lastmod_str = raw_lastmod.isoformat()
        partes.append(
            f"  <url><loc>{html.escape(loc)}</loc>"
            f"<lastmod>{lastmod_str}</lastmod>"
            f"<changefreq>daily</changefreq>"
            f"<priority>0.7</priority></url>"
        )
    partes.append("</urlset>")
    return Response(
        content="\n".join(partes),
        media_type="application/xml",
        headers={"Cache-Control": "public, max-age=3600, stale-while-revalidate=86400"},
    )


@router.get("/")
def web_root(request: Request, oferta: int | None = Query(None, ge=1)) -> Response:
    accept_types = [
        item.split(";", 1)[0].strip().lower()
        for item in request.headers.get("accept", "").split(",")
        if item.strip()
    ]
    accepts_html = any(item in {"text/html", "application/xhtml+xml"} for item in accept_types)
    accepts_json = any(item == "application/json" or item.endswith("+json") for item in accept_types)
    accepts_any = "*/*" in accept_types
    if accepts_json or (accepts_any and not accepts_html):
        return JSONResponse(
            {
                "nombre": "contrata o planta .cl - API",
                "version": "3.0.0",
                "docs": "/docs",
                "db_host": DB_CONFIG["host"],
            }
        )
    if oferta:
        return RedirectResponse(url=f"/oferta/{oferta}", status_code=308)
    meta = build_offer_meta(None, canonical_url=f"{SITE_URL}/")
    html_doc = render_index_with_meta(meta)
    return HTMLResponse(
        content=html_doc,
        status_code=200,
        headers={"Cache-Control": "public, max-age=60, stale-while-revalidate=600"},
    )

