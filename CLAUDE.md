# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Aggregator of Chilean public-sector job listings (domains: `contrataoplanta.cl`, previously `empleoestado.cl`). Three loosely coupled pieces share one PostgreSQL database:

1. **Scrapers** (`scrapers/`) — harvest offers from the central portal `empleospublicos.cl` plus hundreds of individual institutional sites.
2. **FastAPI backend** (`api/main.py`, ASGI app `api.main:app`) — exposes `/api/...` to the frontend and runs auxiliary services (Meilisearch indexing, email alerts via Resend, OG image generation, BCN law lookups).
3. **Static HTML frontend** (`web/index.html` and siblings, plus legacy copies at repo root) — consumes the API via `fetchApi('/api/...')` with fallback to `https://api.contrataoplanta.cl`.

Spanish is the working language for code comments, log messages, DB identifiers, and docs. Preserve it when editing — do not translate to English.

## Runtime

- Python **3.11+** (`.python-version` pins `3.11`).
- PostgreSQL **14+**, default DB name `empleospublicos`.
- Optional: Meilisearch (search), Resend (email), Playwright (JS-heavy scrapers).

Install: `pip install -r requirements.txt`. Schema: `psql -f db/schema.sql`. Migrations live in `db/migrations/` and are numbered (`001_*.sql`, `002_*.sql`, `003_*.sql`) — apply in order.

## Common commands

```bash
# API (dev)
uvicorn api.main:app --reload --port 8000
# API (prod, matches systemd unit)
uvicorn api.main:app --host 127.0.0.1 --port 8000 --workers 2

# Scrapers — NEW orchestrator (classifies sources via source_status.py)
python scrapers/run_all.py                          # only status=active
python scrapers/run_all.py --include-experimental   # add experimental sources
python scrapers/run_all.py --only-kind wordpress --max 10
python scrapers/run_all.py --dry-run --max 3 --skip-empleos-publicos
python scrapers/run_all.py --list-only              # just show classification
python scrapers/run_all.py --id 157                 # single institution by catalog ID

# Scrapers — LEGACY orchestrator (schedule-driven via fuentes.frecuencia_hrs)
python run_scrapers.py [--todos|--fuente N|--dry-run|--listar]

# Single scraper module directly (most expose ejecutar())
python scrapers/empleos_publicos.py --dry-run --max 3 --con-detalle

# URL health re-check (runs automatically at end of run_scrapers.py)
python validate_offer_urls.py --workers 20 --max-edad-h 24

# Tests
pytest                                  # full suite
pytest tests/test_job_pipeline.py       # one file
pytest tests/test_extraction.py::TestClassName::test_name   # one test
```

`tests/conftest.py` sets a fake `DATABASE_URL` so pure-helper tests import without a live DB. Tests that hit the DB (`test_db_persistence.py`, `test_db_helpers.py`) need a running Postgres reachable at that DSN.

## Two environment-variable conventions (do not unify without care)

Two different DB config styles coexist and both are live:

- **`config.py`** — reads `DATABASE_URL` (single DSN) for SQLAlchemy. Used by `run_scrapers.py` and `db/database.py`.
- **`DB_CONFIG` dict** — reads split `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` for psycopg2. Used by `api/main.py`, `scrapers/base.py`, and most individual scrapers.

`.env.example` documents the split-var style (primary for the API + new scrapers). The legacy orchestrator path still needs `DATABASE_URL`. When adding a new component, prefer the split `DB_*` style to match `api/main.py` and `scrapers/base.py`.

Other env vars in play: `SITE_URL`, `CORS_ALLOW_ORIGINS` (CSV; falls back to a hard-coded safe list in `api/main.py`), `RESEND_API_KEY` / `EMAIL_FROM`, `MEILISEARCH_URL` / `MEILISEARCH_API_KEY`, `UMAMI_SCRIPT_URL` / `UMAMI_WEBSITE_ID`, `LOG_DIR` (default `logs/`).

## Scraper architecture

The codebase is mid-migration from per-scraper ad-hoc extraction to a shared pipeline. Both live side-by-side — pick the right one for the task.

**Source catalog + classification.** `repositorio_instituciones_publicas_chile.json` at the repo root is the master list of ~640 institutions. `scrapers/source_status.py` classifies each one into a `SourceStatus` (`active`, `experimental`, `manual_review`, `js_required`, `blocked`, `broken`, `no_data`, `disabled`) and a `ScraperKind` (`empleos_publicos`, `wordpress`, `generic`, `custom_trabajando`, `custom_hiringroom`, `custom_buk`, `custom_playwright`, `custom_policia`, `custom_ffaa`, `skip`). `scrapers/source_overrides.json` forces status/kind for specific IDs — overrides win over automatic classification. Only `active` kinds in `RUNNABLE_KINDS` run by default.

**Orchestration.** `scrapers/run_all.py` (the production entry, invoked by `deploy/systemd/contrataoplanta-scrapers.service`) loads the catalog, enriches each entry with `classify_source(...)`, filters by status/kind flags, and dispatches to the module in `PLATFORM_MODULES`. It also runs the big `EmpleosPublicosScraper` batch unless `--skip-empleos-publicos`.

**Extraction pipeline** (`scrapers/job_pipeline.py` → `JobExtractionPipeline.run`):

1. Scraper produces a `RawPage` (`models/raw_page.py`) — HTML text, tables, PDF attachment texts, discovery metadata.
2. `classification/content_classifier.py` + `classification/rule_engine.py` apply rule-based classification with `rule_trace`. Ambiguous cases (`0.55 ≤ score < 0.80`) fall through to `classification/llm_fallback_classifier.py`. See `docs/CLASSIFICATION_RULES.md` for thresholds and positive/negative signals.
3. Extraction fans out across `extraction/*.py` (dates, field extractors, attachment parser, salary, email, requirements). PDF attachments are parsed only when the filename is relevant (`bases`, `perfil`, `concurso`, `tdr`, `convocatoria`); OCR is last-resort. See `docs/EXTRACTION_STRATEGY.md`.
4. `validation/expiry_validator.py` + `validation/job_validator.py` reject expired or incomplete postings. `validation/quality_scoring.py` computes `overall_quality_score`; low scores flip `needs_review=true`.
5. `normalization/job_normalizer.py` emits a `JobPosting` (`models/job_posting.py`) ready to persist.

Plain-file scrapers in `scrapers/` (`banco_central.py`, `codelco.py`, `tvn.py`, `poder_judicial.py`, etc.) still carry their own ad-hoc extraction; the stated direction (`docs/SCRAPERS_REVIEW.md`) is to move their intelligence into the shared pipeline and leave them responsible only for discovery/fetch. When touching one of these, prefer delegating to `JobExtractionPipeline` over adding more per-scraper heuristics.

**Persistence.** `db/database.py` provides SQLAlchemy session (`SessionLocal`) plus helpers `url_a_hash`, `generar_id_estable`, `normalizar_datos_oferta` (enforces VARCHAR length limits — call it before inserting). Most scrapers write via raw psycopg2 from `scrapers/base.py` using `DB_CONFIG`. Deduplication uses SHA256 of the offer URL.

**Post-scrape URL validation.** `validate_offer_urls.py` hits `url_oferta` / `url_bases` on active rows and updates `url_oferta_valida`, `url_bases_valida`, `url_valida_chequeada_en`. The frontend uses these flags to gate the "Ver bases" / "Postular" buttons. `run_scrapers.py` calls this automatically when non-dry-run; `scrapers/run_all.py` does not — run it manually after a `run_all.py` pass if URL validity matters.

## API surface

`api/main.py` is intentionally monolithic (~1500 lines) — one `app = FastAPI(...)` plus route handlers, an in-file psycopg2 helper `get_cursor()`, and pydantic response models. `api/main_rebuilt.py` is a WIP rewrite; the production entry is `api.main:app`. `api/services/` hosts integrations (`regiones`, `leyes` — BCN law lookup, `mailcheck`, `email_alerts` — Resend, `meilisearch_svc`, `og_image`).

Health/smoke-test endpoints: `GET /health`, `GET /api/ofertas?pagina=1&por_pagina=50&orden=cierre`, `GET /api/estadisticas`, `GET /docs` (Swagger).

CORS origins come from `CORS_ALLOW_ORIGINS` (CSV). If unset, `DEFAULT_ALLOW_ORIGINS` in `api/main.py` ships with Netlify/Pages preview domains and both production hosts.

## Frontend + SEO

Static HTML is duplicated between the repo root (older copies) and `web/` (current). `web/index.html` is the live single-page app; `web/nav-mobile.js` and `web/rich-text.js` are the only hand-written JS files. `web/JSONLD_VALIDACION.md` governs which fields are required before a `JobPosting` JSON-LD block is emitted — if a field is missing from the API payload, the block is omitted rather than emitted invalid. When changing the API offer contract, re-read that doc.

## Deploy

`deploy/deploy.sh` is the production update script (runs as `contrata` user at `/opt/contrataoplanta`): `git pull`, `pip install`, `systemctl restart contrataoplanta-api`, check with `journalctl -u contrataoplanta-api`. systemd units live in `deploy/systemd/`: `contrataoplanta-api.service` (uvicorn, long-lived) and `contrataoplanta-scrapers.service` + `.timer` (oneshot, runs `scrapers/run_all.py --mode production`). Nginx config at `deploy/nginx/contrataoplanta.conf`.

## Conventions

- Spanish-language code is intentional — DB columns (`ofertas`, `fuentes`, `cargo`, `institucion_nombre`, `fecha_cierre`, `activa`), function names (`parsear_listado`, `ejecutar`, `debe_ejecutar`), and log lines. Match the surrounding style.
- New scrapers expose an `ejecutar(dry_run=False, max=None, ...)` entry function so both orchestrators can call them dynamically.
- To add a runnable source, prefer an override in `scrapers/source_overrides.json` over editing the master JSON catalog.
- `logs/` is auto-created; don't commit it. `__pycache__/`, `.env`, `*.log` are already gitignored.
