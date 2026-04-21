CREATE TABLE IF NOT EXISTS generic_site_path_audit (
    institucion_id      INTEGER PRIMARY KEY REFERENCES instituciones(id),
    fuente_id           INTEGER REFERENCES fuentes(id),
    last_success_url    TEXT NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_generic_site_path_audit_fuente ON generic_site_path_audit(fuente_id);
CREATE INDEX IF NOT EXISTS idx_generic_site_path_audit_updated ON generic_site_path_audit(updated_at DESC);
