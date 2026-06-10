from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from html import unescape
from types import MethodType
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

os.environ.setdefault("DB_PASSWORD", "dummy")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp  # noqa: E402

from scrapers.evaluation.source_evaluator import SourceEvaluator  # noqa: E402
from scrapers.intake import intake_validate_offer  # noqa: E402
from scrapers.plataformas.generic_site import GenericSiteScraper  # noqa: E402
from scrapers.plataformas.hiringroom import HiringRoomScraper  # noqa: E402
from scrapers.plataformas.playwright_scraper import PlaywrightScraper  # noqa: E402


CATALOG_PATH = Path("repositorio_instituciones_publicas_chile.json")
FAMILY_IDS = {
    "wordpress": 387,   # Municipalidad de Independencia
    "hiringroom": 392,  # Municipalidad de La Reina
    "playwright": 145,  # Banco Central de Chile
}


@dataclass
class FamilySmokeResult:
    family: str
    institucion_id: int
    institucion_nombre: str
    url_empleo: str | None
    offers_total: int
    offers_with_min_fields: int
    offers_valid_now: int
    offers_needs_review: int
    offers_discarded_by_intake: int
    intake_reason_counts: dict[str, int]
    evaluation_reason_code: str | None
    evaluation_validity_status: str | None
    evaluation_decision: str | None
    evaluation_profile: str | None
    sample_urls: list[str]
    notes: list[str]


def _load_catalog() -> list[dict[str, Any]]:
    payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    return payload["instituciones"] if isinstance(payload, dict) else payload


def _find_source(catalog: list[dict[str, Any]], source_id: int) -> dict[str, Any]:
    for src in catalog:
        if int(src.get("id") or -1) == source_id:
            return src
    raise KeyError(f"No existe id={source_id} en catálogo")


def _has_min_fields(offer: dict[str, Any]) -> bool:
    cargo = str(offer.get("cargo") or "").strip()
    url = str(offer.get("url_oferta") or offer.get("url") or "").strip()
    institucion = str(offer.get("institucion_nombre") or "").strip()
    region = str(offer.get("region") or "").strip()
    return bool(cargo and url and (institucion or region))


def _is_valid_now(offer: dict[str, Any]) -> bool:
    cierre = offer.get("fecha_cierre")
    if cierre is None:
        return False
    if hasattr(cierre, "date"):
        cierre = cierre.date()
    if isinstance(cierre, datetime):
        cierre = cierre.date()
    if not isinstance(cierre, date):
        return False
    return cierre >= datetime.utcnow().date()


def _to_offer_dict(offer: Any) -> dict[str, Any]:
    if isinstance(offer, dict):
        return offer
    data = asdict(offer)
    data["url_oferta"] = data.get("url")
    return data


async def _evaluate_source(source: dict[str, Any]) -> dict[str, Any]:
    evaluator = SourceEvaluator()
    try:
        result = await evaluator.evaluate(source)
        return {
            "reason_code": result.reason_code.value if result.reason_code else None,
            "validity_status": result.validity_status.value if result.validity_status else None,
            "decision": result.decision.value if result.decision else None,
            "profile": result.profile_name,
        }
    except Exception as exc:
        return {
            "reason_code": "evaluation_error",
            "validity_status": "unknown",
            "decision": "manual_review",
            "profile": f"error:{type(exc).__name__}",
        }


async def _run_hiringroom(source: dict[str, Any], max_offers: int) -> list[dict[str, Any]]:
    scraper = HiringRoomScraper(fuente_id=source.get("id"), institucion=source)
    scraper._candidate_urls = MethodType(
        lambda self, *args, **kwargs: GenericSiteScraper._candidate_urls(
            self,
            max_urls=self.max_candidate_urls,
            preferred_url=None,
        ),
        scraper,
    )
    async with scraper:
        offers = await scraper.descubrir_ofertas()
    return [_to_offer_dict(o) for o in offers[:max_offers]]


async def _run_playwright(source: dict[str, Any], max_offers: int) -> tuple[list[dict[str, Any]], list[str]]:
    scraper = PlaywrightScraper(fuente_id=source.get("id"), institucion=source)
    notes: list[str] = []
    async with scraper:
        offers = await scraper.descubrir_ofertas()
    state = scraper.signals_json.get("playwright", {})
    if state.get("status") != "ok":
        notes.append(f"playwright_status={state.get('status')}")
        if state.get("reason_code"):
            notes.append(f"playwright_reason={state.get('reason_code')}")
        if state.get("error"):
            notes.append(f"playwright_error={state.get('error')}")
    return ([_to_offer_dict(o) for o in offers[:max_offers]], notes)


async def _run_wordpress(source: dict[str, Any], max_offers: int) -> tuple[list[dict[str, Any]], list[str]]:
    employment_url = str(source.get("url_empleo") or "")
    parsed = urlparse(employment_url)
    if not parsed.scheme or not parsed.netloc:
        return [], [f"wordpress_invalid_url={employment_url}"]
    origin = f"{parsed.scheme}://{parsed.netloc}"
    api_url = f"{origin}/wp-json/wp/v2/posts?per_page={max_offers}&orderby=date&order=desc"
    notes: list[str] = []
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(api_url) as resp:
                if resp.status != 200:
                    return [], [f"wordpress_api_status={resp.status}", f"wordpress_api_url={api_url}"]
                payload = await resp.json()
    except Exception as exc:
        return [], [f"wordpress_fetch_error={type(exc).__name__}: {exc}"]

    offers: list[dict[str, Any]] = []
    for post in payload:
        title = unescape(str((post.get("title") or {}).get("rendered") or "")).strip()
        content = unescape(str((post.get("content") or {}).get("rendered") or "")).strip()
        offer_url = str(post.get("link") or "").strip()
        fecha_pub = str(post.get("date") or "")[:10] or None
        if not title or not offer_url:
            continue
        offers.append(
            {
                "cargo": title,
                "descripcion": content,
                "url_oferta": offer_url,
                "institucion_nombre": source.get("nombre"),
                "region": source.get("region"),
                "fecha_publicacion": fecha_pub,
            }
        )
    if not offers:
        notes.append("wordpress_api_no_posts")
    return offers, notes


def _summarize_family(
    family: str,
    source: dict[str, Any],
    offers: list[dict[str, Any]],
    evaluation: dict[str, Any],
    extra_notes: list[str] | None = None,
) -> FamilySmokeResult:
    min_ok = 0
    vigentes = 0
    review = 0
    discarded = 0
    reason_counts: dict[str, int] = {}

    for offer in offers:
        if _has_min_fields(offer):
            min_ok += 1
        if _is_valid_now(offer):
            vigentes += 1
        decision = intake_validate_offer(offer)
        if decision.discard:
            discarded += 1
            reason = decision.motivo_descarte or "motivo_desconocido"
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if decision.needs_review:
            review += 1

    sample_urls = []
    for offer in offers[:5]:
        url = offer.get("url_oferta") or offer.get("url")
        if url:
            sample_urls.append(str(url))

    return FamilySmokeResult(
        family=family,
        institucion_id=int(source.get("id") or -1),
        institucion_nombre=str(source.get("nombre") or ""),
        url_empleo=source.get("url_empleo"),
        offers_total=len(offers),
        offers_with_min_fields=min_ok,
        offers_valid_now=vigentes,
        offers_needs_review=review,
        offers_discarded_by_intake=discarded,
        intake_reason_counts=dict(sorted(reason_counts.items())),
        evaluation_reason_code=evaluation.get("reason_code"),
        evaluation_validity_status=evaluation.get("validity_status"),
        evaluation_decision=evaluation.get("decision"),
        evaluation_profile=evaluation.get("profile"),
        sample_urls=sample_urls,
        notes=extra_notes or [],
    )


async def run_smoke(output_path: Path, max_offers: int = 25) -> list[FamilySmokeResult]:
    catalog = _load_catalog()
    results: list[FamilySmokeResult] = []

    wp_source = _find_source(catalog, FAMILY_IDS["wordpress"])
    wp_eval = await _evaluate_source(wp_source)
    wp_offers, wp_notes = await _run_wordpress(wp_source, max_offers=max_offers)
    results.append(_summarize_family("WordPress", wp_source, wp_offers, wp_eval, wp_notes))

    hr_source = _find_source(catalog, FAMILY_IDS["hiringroom"])
    hr_eval = await _evaluate_source(hr_source)
    try:
        hr_offers = await _run_hiringroom(hr_source, max_offers=max_offers)
        hr_notes: list[str] = []
    except Exception as exc:
        hr_offers = []
        hr_notes = [f"hiringroom_error={type(exc).__name__}: {exc}"]
    results.append(_summarize_family("ATS HiringRoom", hr_source, hr_offers, hr_eval, hr_notes))

    pw_source = _find_source(catalog, FAMILY_IDS["playwright"])
    pw_eval = await _evaluate_source(pw_source)
    try:
        pw_offers, pw_notes = await _run_playwright(pw_source, max_offers=max_offers)
    except Exception as exc:
        pw_offers, pw_notes = [], [f"playwright_error={type(exc).__name__}: {exc}"]
    results.append(_summarize_family("Playwright", pw_source, pw_offers, pw_eval, pw_notes))

    payload = {
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "max_offers_per_family": max_offers,
        "results": [asdict(r) for r in results],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke real por familias (WordPress, HiringRoom, Playwright).")
    parser.add_argument("--output", default="reports/smoke_real_familias.json", help="Ruta JSON de salida.")
    parser.add_argument("--max-offers", type=int, default=25, help="Máximo de ofertas a validar por familia.")
    args = parser.parse_args()

    results = asyncio.run(run_smoke(Path(args.output), max_offers=args.max_offers))

    print("SMOKE REAL POR FAMILIAS")
    for row in results:
        print(
            f"- {row.family}: ofertas={row.offers_total} min_ok={row.offers_with_min_fields} "
            f"vigentes={row.offers_valid_now} reason_code={row.evaluation_reason_code} "
            f"validity={row.evaluation_validity_status}"
        )
        if row.intake_reason_counts:
            print(f"  intake_reason_counts={row.intake_reason_counts}")
        if row.notes:
            print(f"  notes={row.notes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
