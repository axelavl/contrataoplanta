"""Recon técnico para https://postulaciones.investigaciones.cl/.

Objetivo: descubrir la fuente real de datos (XHR/fetch/GraphQL/JSON inline)
SIN inventar endpoints.

Uso:
  python -m scrapers.plataformas.pdi_postulaciones_recon --out research/pdi/recon_run

Requiere playwright instalado localmente:
  pip install playwright
  playwright install chromium
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

TARGET_URL = "https://postulaciones.investigaciones.cl/"

KEYWORDS = (
    "oferta",
    "vacante",
    "cargo",
    "postul",
    "concurso",
    "trabaja",
    "recluta",
)


@dataclass
class NetworkEvent:
    ts: str
    method: str
    url: str
    resource_type: str
    status: int | None
    ok: bool | None
    request_headers: dict[str, str]
    response_headers: dict[str, str]
    post_data: str | None
    response_text_sample: str | None


def _ts() -> str:
    return datetime.now(UTC).isoformat()


def _looks_relevant(url: str, body: str | None, ctype: str | None) -> bool:
    haystack = f"{url}\n{(body or '')}\n{(ctype or '')}".lower()
    return any(k in haystack for k in KEYWORDS)


def _guess_payload_kind(response_text: str) -> str:
    text = response_text.strip()
    if not text:
        return "empty"
    if text.startswith("{") or text.startswith("["):
        return "json"
    if "<html" in text.lower():
        return "html"
    return "text"


def _safe_all_headers(message: Any) -> dict[str, str]:
    """Obtiene headers completos (incluyendo cookies/autorización cuando aplique).

    Playwright expone `headers` como una vista sanitizada para uso general.
    Para preservar contexto replayable de tráfico se prioriza `all_headers()`.
    """
    try:
        raw_headers = message.all_headers()
    except Exception:
        raw_headers = None

    if isinstance(raw_headers, dict) and raw_headers:
        return {str(k).lower(): str(v) for k, v in raw_headers.items()}

    try:
        return {str(k).lower(): str(v) for k, v in message.headers.items()}
    except Exception:
        return {}


def run_recon(output_dir: Path, headless: bool = True, wait_ms: int = 15000) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    output_dir.mkdir(parents=True, exist_ok=True)
    events: list[NetworkEvent] = []
    json_candidates: list[dict[str, Any]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        def on_response(resp):
            req = resp.request
            try:
                status = resp.status
            except Exception:
                status = None
            try:
                ok = resp.ok
            except Exception:
                ok = None
            req_headers = _safe_all_headers(req)
            resp_headers = _safe_all_headers(resp)
            ctype = resp_headers.get("content-type", "")
            post_data = req.post_data

            text_sample = None
            is_textual = any(x in ctype for x in ("json", "text", "javascript", "graphql", "html"))
            if is_textual:
                try:
                    text_sample = resp.text()[:1500]
                except Exception:
                    text_sample = None

            ev = NetworkEvent(
                ts=_ts(),
                method=req.method,
                url=req.url,
                resource_type=req.resource_type,
                status=status,
                ok=ok,
                request_headers=req_headers,
                response_headers=resp_headers,
                post_data=post_data,
                response_text_sample=text_sample,
            )
            events.append(ev)

            if _looks_relevant(req.url, text_sample, ctype):
                parsed = urlparse(req.url)
                item = {
                    "ts": ev.ts,
                    "method": req.method,
                    "url": req.url,
                    "path": parsed.path,
                    "query": parse_qs(parsed.query),
                    "status": status,
                    "content_type": ctype,
                    "payload_kind": _guess_payload_kind(text_sample or ""),
                    "has_bearer": "authorization" in req_headers,
                    "has_csrf": any("csrf" in k for k in req_headers),
                    "post_data_sample": (post_data or "")[:400],
                    "response_sample": (text_sample or "")[:600],
                }
                json_candidates.append(item)

        page.on("response", on_response)

        page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(wait_ms)

        # Intenta navegar/interactuar sólo con selectores obvios, sin romper si no existen.
        for selector in ["text=Ingresar", "text=Postular", "text=Perfil de Cargo", "text=Cargos disponibles"]:
            try:
                node = page.locator(selector).first
                if node.count() > 0:
                    node.click(timeout=1500)
                    page.wait_for_timeout(1500)
            except Exception:
                pass

        page.wait_for_timeout(3000)

        # Extrae JSON inline potencial (window.__*, script[type='application/json']).
        inline_json: dict[str, Any] = {}
        try:
            scripts = page.locator("script").all_text_contents()
            for idx, raw in enumerate(scripts):
                s = (raw or "").strip()
                if not s:
                    continue
                if re.search(r"window\.__|__INITIAL|__NEXT_DATA__|preloadedState", s):
                    inline_json[f"script_{idx}"] = s[:2000]
        except Exception:
            pass

        html = page.content()
        (output_dir / "page.html").write_text(html, encoding="utf-8")
        (output_dir / "network_events.jsonl").write_text(
            "\n".join(json.dumps(asdict(e), ensure_ascii=False) for e in events),
            encoding="utf-8",
        )
        (output_dir / "candidates.json").write_text(
            json.dumps(json_candidates, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "inline_json_signals.json").write_text(
            json.dumps(inline_json, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        browser.close()

    return {
        "target": TARGET_URL,
        "captured_events": len(events),
        "candidate_calls": len(json_candidates),
        "out_dir": str(output_dir),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="research/pdi/recon_run", help="Directorio de salida")
    parser.add_argument("--headed", action="store_true", help="Ejecuta navegador visible")
    parser.add_argument("--wait-ms", type=int, default=15000)
    args = parser.parse_args()

    result = run_recon(Path(args.out), headless=not args.headed, wait_ms=args.wait_ms)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
