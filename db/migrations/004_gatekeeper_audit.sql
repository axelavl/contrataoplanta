ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases TEXT;

CREATE TABLE IF NOT EXISTS source_evaluations (
    id                      BIGSERIAL PRIMARY KEY,
    source_id               INTEGER REFERENCES fuentes(id),
    institucion_id          INTEGER REFERENCES instituciones(id),
    source_url              TEXT NOT NULL,
    availability            VARCHAR(50) NOT NULL,
    http_status             INTEGER,
    page_type               VARCHAR(50) NOT NULL,
    job_relevance           VARCHAR(50) NOT NULL,
    open_calls_status       VARCHAR(50) NOT NULL,
    validity_status         VARCHAR(50) NOT NULL,
    recommended_extractor   VARCHAR(80),
    decision                VARCHAR(50) NOT NULL,
    reason_code             VARCHAR(80),
    reason_detail           TEXT,
    confidence              NUMERIC(6,4) NOT NULL,
    retry_policy            VARCHAR(30) NOT NULL,
    signals_json            JSONB NOT NULL DEFAULT '{}'::jsonb,
    evaluated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    profile_name            VARCHAR(80)
);

CREATE INDEX IF NOT EXISTS idx_source_evaluations_source ON source_evaluations(source_id);
CREATE INDEX IF NOT EXISTS idx_source_evaluations_inst ON source_evaluations(institucion_id);
CREATE INDEX IF NOT EXISTS idx_source_evaluations_decision ON source_evaluations(decision, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_source_evaluations_reason ON source_evaluations(reason_code, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS offer_quality_events (
    id                  BIGSERIAL PRIMARY KEY,
    oferta_id           INTEGER REFERENCES ofertas(id) ON DELETE CASCADE,
    fuente_id           INTEGER REFERENCES fuentes(id),
    institucion_id      INTEGER REFERENCES instituciones(id),
    url_oferta          TEXT,
    decision            VARCHAR(30) NOT NULL,
    primary_reason_code VARCHAR(80),
    reason_codes        JSONB NOT NULL DEFAULT '[]'::jsonb,
    reason_detail       TEXT,
    quality_score       NUMERIC(6,4) NOT NULL DEFAULT 0,
    signals_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_offer_quality_events_inst ON offer_quality_events(institucion_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_offer_quality_events_reason ON offer_quality_events(primary_reason_code, created_at DESC);

CREATE TABLE IF NOT EXISTS catalog_integrity_events (
    id              BIGSERIAL PRIMARY KEY,
    institucion_id  INTEGER REFERENCES instituciones(id),
    event_type      VARCHAR(80) NOT NULL,
    detail          TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_catalog_integrity_events_inst ON catalog_integrity_events(institucion_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_catalog_integrity_events_type ON catalog_integrity_events(event_type, created_at DESC);
