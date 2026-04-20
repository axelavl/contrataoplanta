-- ============================================================
--  contrataoplanta.cl — Schema de Base de Datos
--  PostgreSQL 14+
--  Version: 2.0 (post-audit 2026-04-15)
--
--  Todas las sentencias son idempotentes. Se puede ejecutar
--  varias veces sobre una base existente sin romper datos.
-- ============================================================

-- ──────────────────────────────────────────────────────────
--  Extensiones
-- ──────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ──────────────────────────────────────────────────────────
--  TABLA: fuentes
--  Cada portal scrapeado (empleospublicos, ADP, muni X...)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fuentes (
    id                SERIAL PRIMARY KEY,
    nombre            VARCHAR(200) NOT NULL,
    url_base          TEXT NOT NULL,
    sector            VARCHAR(80),
    tipo_plataforma   VARCHAR(40),        -- 'empleospublicos' | 'wordpress' | 'html' | 'adp' | 'pjud'
    activa            BOOLEAN DEFAULT TRUE,
    frecuencia_hrs    INTEGER DEFAULT 12,
    ultima_ejecucion  TIMESTAMPTZ,
    ultima_exitosa    TIMESTAMPTZ,
    total_ofertas     INTEGER DEFAULT 0,
    precision_7d      NUMERIC(5,2),       -- tasa de precisión últimos 7 días
    creada_en         TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE fuentes ADD COLUMN IF NOT EXISTS tipo_plataforma VARCHAR(40);
ALTER TABLE fuentes ADD COLUMN IF NOT EXISTS precision_7d NUMERIC(5,2);

-- ──────────────────────────────────────────────────────────
--  TABLA: instituciones
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS instituciones (
    id            SERIAL PRIMARY KEY,
    nombre        VARCHAR(300) NOT NULL,
    nombre_corto  VARCHAR(80),
    nombre_norm   VARCHAR(300),           -- nombre normalizado para fuzzy match
    sector        VARCHAR(80),
    tipo          VARCHAR(80),
    region        VARCHAR(80),
    url_empleo    TEXT,
    fuente_id     INTEGER REFERENCES fuentes(id),
    color_hex     VARCHAR(7) DEFAULT '#1F4E79',
    creada_en     TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE instituciones ADD COLUMN IF NOT EXISTS nombre_norm VARCHAR(300);
CREATE INDEX IF NOT EXISTS idx_instituciones_norm ON instituciones(nombre_norm);
CREATE INDEX IF NOT EXISTS idx_instituciones_sector ON instituciones(sector);

-- ──────────────────────────────────────────────────────────
--  TABLA: ofertas — núcleo del sistema
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas (
    id                      SERIAL PRIMARY KEY,
    codigo                  VARCHAR(30) UNIQUE,        -- EE-YYY-SEC-NNNNNN

    -- Identificación
    id_externo              VARCHAR(200),
    fuente_id               INTEGER REFERENCES fuentes(id),
    url_original            TEXT,
    url_oferta              TEXT,
    url_hash                VARCHAR(64) UNIQUE,        -- SHA256 de url canónica
    contenido_hash          VARCHAR(64),               -- SHA256 del contenido, para detectar cambios

    -- Datos principales
    cargo                   VARCHAR(500) NOT NULL,
    descripcion             TEXT,
    institucion_id          INTEGER REFERENCES instituciones(id),
    institucion_nombre      VARCHAR(300) NOT NULL,
    sector                  VARCHAR(80),
    area_profesional        VARCHAR(100),
    tipo_cargo              VARCHAR(50),
    nivel                   VARCHAR(80),

    -- Ubicación
    region                  VARCHAR(80),
    ciudad                  VARCHAR(80),

    -- Condiciones
    renta_bruta_min         BIGINT,
    renta_bruta_max         BIGINT,
    renta_texto             VARCHAR(200),
    grado_eus               VARCHAR(20),
    url_bases               TEXT,
    horas_semanales         INTEGER,
    modalidad               VARCHAR(50),

    -- Plazos
    fecha_publicacion       DATE,
    fecha_cierre            DATE,
    fecha_inicio            DATE,

    -- Requisitos
    requisitos_texto        TEXT,
    experiencia_anos        INTEGER,

    -- Estado
    activa                  BOOLEAN DEFAULT TRUE,
    es_nueva                BOOLEAN DEFAULT TRUE,
    vistas                  INTEGER DEFAULT 0,

    -- Auditoría
    creada_en               TIMESTAMPTZ DEFAULT NOW(),
    actualizada_en          TIMESTAMPTZ DEFAULT NOW(),
    detectada_en            TIMESTAMPTZ DEFAULT NOW(),
    fecha_cierre_detectada  TIMESTAMPTZ,
    ultima_vista_en         TIMESTAMPTZ DEFAULT NOW()
);

-- Columnas agregadas post-creación (idempotente)
ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_hash VARCHAR(64);
ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS contenido_hash VARCHAR(64);
ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS grado_eus VARCHAR(20);
ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS url_bases TEXT;
ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS ultima_vista_en TIMESTAMPTZ DEFAULT NOW();

-- Índice único para deduplicación
CREATE UNIQUE INDEX IF NOT EXISTS uq_ofertas_url_hash ON ofertas(url_hash)
    WHERE url_hash IS NOT NULL;

-- ──────────────────────────────────────────────────────────
--  CONSTRAINTS DE INTEGRIDAD (post-audit 4.2)
-- ──────────────────────────────────────────────────────────

-- tipo_cargo debe ser uno de los valores válidos (o NULL)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_tipo_cargo'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_tipo_cargo
            CHECK (
                tipo_cargo IS NULL OR
                tipo_cargo IN ('planta','contrata','honorarios','reemplazo','codigo_trabajo')
            );
    END IF;
END $$;

-- fecha_cierre no puede ser anterior a fecha_publicacion
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_fechas'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_fechas
            CHECK (
                fecha_cierre IS NULL OR
                fecha_publicacion IS NULL OR
                fecha_cierre >= fecha_publicacion
            );
    END IF;
END $$;

-- cargo no puede ser vacío
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_cargo_no_vacio'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_cargo_no_vacio
            CHECK (length(trim(cargo)) > 0);
    END IF;
END $$;

-- Calidad de datos (ver db/migrations/005_data_quality_constraints.sql).
-- Rentas: 300 000 a 20 000 000 CLP/mes (grados EUS van hasta ~4.8M;
-- techo holgado para cargos excepcionales).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_renta_min_rango') THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_renta_min_rango
            CHECK (renta_bruta_min IS NULL OR renta_bruta_min BETWEEN 300000 AND 20000000);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_renta_max_rango') THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_renta_max_rango
            CHECK (renta_bruta_max IS NULL OR renta_bruta_max BETWEEN 300000 AND 20000000);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_renta_min_leq_max') THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_renta_min_leq_max
            CHECK (
                renta_bruta_min IS NULL OR renta_bruta_max IS NULL OR
                renta_bruta_min <= renta_bruta_max
            );
    END IF;
END $$;

-- Fechas dentro de ventana razonable (2020 a +3 años).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_fecha_publicacion_rango') THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_fecha_publicacion_rango
            CHECK (
                fecha_publicacion IS NULL OR
                fecha_publicacion BETWEEN DATE '2020-01-01' AND CURRENT_DATE + INTERVAL '1 year'
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_fecha_cierre_rango') THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_fecha_cierre_rango
            CHECK (
                fecha_cierre IS NULL OR
                fecha_cierre BETWEEN DATE '2020-01-01' AND CURRENT_DATE + INTERVAL '3 years'
            );
    END IF;
END $$;

-- Jornada razonable.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_horas_semanales_rango') THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_horas_semanales_rango
            CHECK (horas_semanales IS NULL OR horas_semanales BETWEEN 1 AND 88);
    END IF;
END $$;

-- ──────────────────────────────────────────────────────────
--  ÍNDICES DE PERFORMANCE (post-audit 2.7)
-- ──────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ofertas_activa          ON ofertas(activa);
CREATE INDEX IF NOT EXISTS idx_ofertas_sector          ON ofertas(sector);
CREATE INDEX IF NOT EXISTS idx_ofertas_region          ON ofertas(region);
CREATE INDEX IF NOT EXISTS idx_ofertas_tipo_cargo      ON ofertas(tipo_cargo);
CREATE INDEX IF NOT EXISTS idx_ofertas_area            ON ofertas(area_profesional);
CREATE INDEX IF NOT EXISTS idx_ofertas_cierre          ON ofertas(fecha_cierre);
CREATE INDEX IF NOT EXISTS idx_ofertas_publicacion     ON ofertas(fecha_publicacion DESC);
CREATE INDEX IF NOT EXISTS idx_ofertas_fuente          ON ofertas(fuente_id);
CREATE INDEX IF NOT EXISTS idx_ofertas_institucion     ON ofertas(institucion_id);
CREATE INDEX IF NOT EXISTS idx_ofertas_nueva           ON ofertas(es_nueva) WHERE es_nueva = TRUE;
CREATE INDEX IF NOT EXISTS idx_ofertas_scraped         ON ofertas(detectada_en DESC);

-- Índice parcial: solo ofertas activas, que son las que se consultan 99% del tiempo
CREATE INDEX IF NOT EXISTS idx_ofertas_activas_cierre
    ON ofertas(fecha_cierre ASC NULLS LAST)
    WHERE activa = TRUE;

-- Full-text search en español con unaccent (post-audit 2.8)
CREATE INDEX IF NOT EXISTS idx_ofertas_fts ON ofertas
    USING GIN (
        to_tsvector(
            'spanish',
            unaccent(
                coalesce(cargo, '') || ' ' ||
                coalesce(institucion_nombre, '') || ' ' ||
                coalesce(descripcion, '')
            )
        )
    );

-- ──────────────────────────────────────────────────────────
--  TABLA: scraper_runs — reporte de cada corrida (4.3)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS scraper_runs (
    id                    SERIAL PRIMARY KEY,
    ejecutado_en          TIMESTAMPTZ DEFAULT NOW(),
    duracion_segundos     INTEGER,
    total_instituciones   INTEGER DEFAULT 0,
    total_encontradas     INTEGER DEFAULT 0,
    total_nuevas          INTEGER DEFAULT 0,
    total_actualizadas    INTEGER DEFAULT 0,
    total_cerradas        INTEGER DEFAULT 0,
    total_vencidas        INTEGER DEFAULT 0,
    total_descartadas     INTEGER DEFAULT 0,
    total_errores         INTEGER DEFAULT 0,
    tasa_precision        NUMERIC(5,2),
    detalle               JSONB
);

CREATE INDEX IF NOT EXISTS idx_scraper_runs_ejecutado ON scraper_runs(ejecutado_en DESC);

-- ──────────────────────────────────────────────────────────
--  TABLA: scraper_descartes — falsos positivos (paso 7)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS scraper_descartes (
    id                 SERIAL PRIMARY KEY,
    institucion_id     INTEGER REFERENCES instituciones(id),
    institucion_nombre VARCHAR(300),
    titulo             TEXT,
    url                TEXT,
    motivo             VARCHAR(50) NOT NULL,
    keyword_detectada  VARCHAR(100),
    fecha_descarte     TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT chk_descarte_motivo
        CHECK (motivo IN (
            'keyword_negativa',
            'sin_keywords',
            'vencida',
            'duplicada',
            'fecha_invalida',
            'contenido_vacio'
        ))
);

CREATE INDEX IF NOT EXISTS idx_descartes_fecha ON scraper_descartes(fecha_descarte DESC);
CREATE INDEX IF NOT EXISTS idx_descartes_motivo ON scraper_descartes(motivo);
CREATE INDEX IF NOT EXISTS idx_descartes_institucion ON scraper_descartes(institucion_id);

-- ──────────────────────────────────────────────────────────
--  TABLA: source_evaluations - auditoria del gatekeeper
-- ──────────────────────────────────────────────────────────
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

-- ──────────────────────────────────────────────────────────
--  TABLA: offer_quality_events - validacion post-extraccion
-- ──────────────────────────────────────────────────────────
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

-- ──────────────────────────────────────────────────────────
--  TABLA: catalog_integrity_events - catalogo vs ofertas
-- ──────────────────────────────────────────────────────────
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

-- ──────────────────────────────────────────────────────────
--  TABLA: logs_scraping — se mantiene por compatibilidad
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logs_scraping (
    id                   SERIAL PRIMARY KEY,
    fuente_id            INTEGER REFERENCES fuentes(id),
    iniciado_en          TIMESTAMPTZ DEFAULT NOW(),
    finalizado_en        TIMESTAMPTZ,
    duracion_seg         NUMERIC(8,2),
    estado               VARCHAR(20),
    ofertas_nuevas       INTEGER DEFAULT 0,
    ofertas_actualizadas INTEGER DEFAULT 0,
    ofertas_cerradas     INTEGER DEFAULT 0,
    paginas_visitadas    INTEGER DEFAULT 0,
    error_mensaje        TEXT,
    detalle              JSONB
);

-- ──────────────────────────────────────────────────────────
--  TABLA: usuarios + alertas — para POST /api/alertas
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS usuarios (
    id                  SERIAL PRIMARY KEY,
    email               VARCHAR(200) UNIQUE NOT NULL,
    nombre              VARCHAR(200),
    activo              BOOLEAN DEFAULT TRUE,
    verificado          BOOLEAN DEFAULT FALSE,
    token_verificacion  VARCHAR(100),
    creado_en           TIMESTAMPTZ DEFAULT NOW(),
    ultimo_acceso       TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS alertas (
    id              SERIAL PRIMARY KEY,
    usuario_id      INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
    email           VARCHAR(200) NOT NULL,
    region          VARCHAR(80),
    sector          VARCHAR(80),
    tipo_cargo      VARCHAR(50),
    palabras_clave  TEXT,
    activa          BOOLEAN DEFAULT TRUE,
    creada_en       TIMESTAMPTZ DEFAULT NOW(),
    ultima_envio    TIMESTAMPTZ,
    total_enviados  INTEGER DEFAULT 0,
    CONSTRAINT chk_alertas_email_formato
        CHECK (email ~* '^[^@]+@[^@]+\.[^@]+$')
);

CREATE INDEX IF NOT EXISTS idx_alertas_email ON alertas(email);
CREATE INDEX IF NOT EXISTS idx_alertas_activa ON alertas(activa) WHERE activa = TRUE;

CREATE TABLE IF NOT EXISTS alertas_enviadas (
    id          SERIAL PRIMARY KEY,
    alerta_id   INTEGER REFERENCES alertas(id) ON DELETE CASCADE,
    usuario_id  INTEGER REFERENCES usuarios(id),
    oferta_id   INTEGER REFERENCES ofertas(id),
    enviada_en  TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────
--  TABLA: grados_eus — referencia de grados EUS/EMS
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS grados_eus (
    grado         INTEGER PRIMARY KEY,
    escala        VARCHAR(10) NOT NULL,     -- 'EUS' | 'EMS'
    renta_bruta   BIGINT NOT NULL,
    vigente_desde DATE NOT NULL
);

-- Valores referenciales (Escala Única Sueldos, 2026 aprox — revisar anualmente)
INSERT INTO grados_eus (grado, escala, renta_bruta, vigente_desde) VALUES
    (1,  'EUS', 4800000, '2026-01-01'),
    (2,  'EUS', 4200000, '2026-01-01'),
    (3,  'EUS', 3700000, '2026-01-01'),
    (4,  'EUS', 3300000, '2026-01-01'),
    (5,  'EUS', 2900000, '2026-01-01'),
    (6,  'EUS', 2600000, '2026-01-01'),
    (7,  'EUS', 2300000, '2026-01-01'),
    (8,  'EUS', 2050000, '2026-01-01'),
    (9,  'EUS', 1850000, '2026-01-01'),
    (10, 'EUS', 1650000, '2026-01-01'),
    (11, 'EUS', 1450000, '2026-01-01'),
    (12, 'EUS', 1280000, '2026-01-01'),
    (13, 'EUS', 1120000, '2026-01-01'),
    (14, 'EUS',  980000, '2026-01-01'),
    (15, 'EUS',  860000, '2026-01-01')
ON CONFLICT (grado) DO NOTHING;

-- ──────────────────────────────────────────────────────────
--  ENUMERADOR PROPIO: código EE-YYY-SEC-NNNNNN (conservado)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas_contador (
    anio    CHAR(3) NOT NULL,
    sector  CHAR(3) NOT NULL,
    ultimo  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (anio, sector)
);

CREATE OR REPLACE FUNCTION asignar_codigo_oferta() RETURNS TRIGGER AS $$
DECLARE
    v_anio        CHAR(3);
    v_sector_raw  TEXT;
    v_sector_norm TEXT;
    v_sector      CHAR(3);
    v_correlativo INTEGER;
BEGIN
    IF NEW.codigo IS NOT NULL THEN
        RETURN NEW;
    END IF;

    v_anio := TO_CHAR(NOW(), 'YYY');

    v_sector_raw := COALESCE(NEW.sector, '');
    IF v_sector_raw = '' AND NEW.fuente_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM fuentes WHERE id = NEW.fuente_id;
    END IF;
    IF (v_sector_raw IS NULL OR v_sector_raw = '') AND NEW.institucion_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM instituciones WHERE id = NEW.institucion_id;
    END IF;

    v_sector_norm := unaccent(lower(COALESCE(v_sector_raw, '')));

    v_sector := CASE
        WHEN v_sector_norm LIKE '%judic%'              THEN 'JUD'
        WHEN v_sector_norm LIKE '%municip%'            THEN 'MUN'
        WHEN v_sector_norm LIKE '%salud%'              THEN 'SAL'
        WHEN v_sector_norm LIKE '%universidad%'
          OR v_sector_norm LIKE '%educa%'              THEN 'EDU'
        WHEN v_sector_norm LIKE '%gobierno%regional%'  THEN 'GOR'
        WHEN v_sector_norm LIKE '%empresa%'            THEN 'EMP'
        WHEN v_sector_norm LIKE '%ff.aa%'
          OR v_sector_norm LIKE '%fuerza%armad%'
          OR v_sector_norm LIKE '%orden%'              THEN 'FFA'
        WHEN v_sector_norm LIKE '%legislat%'           THEN 'LEG'
        WHEN v_sector_norm LIKE '%ejecut%'             THEN 'EJE'
        WHEN v_sector_norm LIKE '%adp%'
          OR v_sector_norm LIKE '%alta direcc%'        THEN 'ADP'
        WHEN v_sector_norm LIKE '%auton%'              THEN 'AUT'
        ELSE 'OTR'
    END;

    INSERT INTO ofertas_contador (anio, sector, ultimo)
    VALUES (v_anio, v_sector, 1)
    ON CONFLICT (anio, sector)
    DO UPDATE SET ultimo = ofertas_contador.ultimo + 1
    RETURNING ultimo INTO v_correlativo;

    NEW.codigo := 'EE-' || v_anio || '-' || v_sector || '-' || LPAD(v_correlativo::text, 6, '0');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_asignar_codigo_oferta ON ofertas;
CREATE TRIGGER tr_asignar_codigo_oferta
    BEFORE INSERT ON ofertas
    FOR EACH ROW
    EXECUTE FUNCTION asignar_codigo_oferta();

-- ──────────────────────────────────────────────────────────
--  SNAPSHOTS E HISTÓRICO (se conservan, abreviados)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    oferta_id           INTEGER NOT NULL REFERENCES ofertas(id) ON DELETE CASCADE,
    codigo              VARCHAR(30),
    capturado_en        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    origen              VARCHAR(10) NOT NULL,
    fuente_id           INTEGER,
    cargo               VARCHAR(500),
    descripcion         TEXT,
    institucion_nombre  VARCHAR(300),
    sector              VARCHAR(80),
    area_profesional    VARCHAR(100),
    tipo_cargo          VARCHAR(50),
    nivel               VARCHAR(80),
    region              VARCHAR(80),
    ciudad              VARCHAR(80),
    renta_bruta_min     BIGINT,
    renta_bruta_max     BIGINT,
    renta_texto         VARCHAR(200),
    fecha_publicacion   DATE,
    fecha_cierre        DATE,
    activa              BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_snapshots_oferta ON ofertas_snapshots(oferta_id, capturado_en DESC);

CREATE OR REPLACE FUNCTION snapshot_oferta() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ofertas_snapshots (
        oferta_id, codigo, capturado_en, origen,
        fuente_id, cargo, descripcion, institucion_nombre, sector,
        area_profesional, tipo_cargo, nivel, region, ciudad,
        renta_bruta_min, renta_bruta_max, renta_texto,
        fecha_publicacion, fecha_cierre, activa
    ) VALUES (
        NEW.id, NEW.codigo, NOW(), TG_OP,
        NEW.fuente_id, NEW.cargo, NEW.descripcion, NEW.institucion_nombre, NEW.sector,
        NEW.area_profesional, NEW.tipo_cargo, NEW.nivel, NEW.region, NEW.ciudad,
        NEW.renta_bruta_min, NEW.renta_bruta_max, NEW.renta_texto,
        NEW.fecha_publicacion, NEW.fecha_cierre, NEW.activa
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_snapshot_oferta ON ofertas;
CREATE TRIGGER tr_snapshot_oferta
    AFTER INSERT OR UPDATE ON ofertas
    FOR EACH ROW
    EXECUTE FUNCTION snapshot_oferta();

-- ──────────────────────────────────────────────────────────
--  VISTAS (el API consume estas, no las tablas directas)
-- ──────────────────────────────────────────────────────────

-- Mapea activa BOOLEAN → estado VARCHAR {'activo','cerrado','vencido'}
-- (decisión documentada en AUDIT_REPORT sección 5)
CREATE OR REPLACE VIEW v_ofertas_estado AS
SELECT
    o.*,
    i.nombre_corto,
    i.color_hex,
    i.region AS institucion_region,
    CASE
        WHEN NOT o.activa THEN 'cerrado'
        WHEN o.fecha_cierre IS NOT NULL AND o.fecha_cierre < CURRENT_DATE THEN 'vencido'
        ELSE 'activo'
    END AS estado,
    GREATEST(0, COALESCE((o.fecha_cierre - CURRENT_DATE)::int, 0)) AS dias_restantes,
    CASE
        WHEN o.fecha_cierre IS NULL THEN 'sin_plazo'
        WHEN (o.fecha_cierre - CURRENT_DATE) <= 2 THEN 'urgente'
        WHEN (o.fecha_cierre - CURRENT_DATE) <= 5 THEN 'pronto'
        ELSE 'vigente'
    END AS semaforo
FROM ofertas o
LEFT JOIN instituciones i ON o.institucion_id = i.id;

-- Vista auxiliar: solo ofertas vigentes (activo) para listados públicos
CREATE OR REPLACE VIEW v_ofertas_vigentes AS
SELECT *
FROM v_ofertas_estado
WHERE estado = 'activo'
ORDER BY fecha_cierre ASC NULLS LAST, detectada_en DESC;

-- ──────────────────────────────────────────────────────────
--  DATOS INICIALES: fuentes
-- ──────────────────────────────────────────────────────────
INSERT INTO fuentes (nombre, url_base, sector, tipo_plataforma, frecuencia_hrs) VALUES
    ('Portal Empleos Públicos - Servicio Civil', 'https://www.empleospublicos.cl', 'Ejecutivo', 'empleospublicos', 6),
    ('Alta Dirección Pública (ADP)',             'https://adp.serviciocivil.cl',   'ADP',       'adp',             24),
    ('Poder Judicial',                           'https://www.pjud.cl',            'Judicial',  'pjud',            24),
    ('Banco Central de Chile',                   'https://empleos.bcentral.cl',    'Autónomo',  'html',            24),
    ('Contraloría General de la República',      'https://www.contraloria.cl',     'Autónomo',  'wordpress',       48),
    ('Ministerio Público - Fiscalía',            'https://www.fiscaliadechile.cl', 'Autónomo',  'html',            48),
    ('Dirección del Trabajo',                    'https://www.dt.gob.cl',          'Ejecutivo', 'wordpress',       48)
ON CONFLICT DO NOTHING;

-- ──────────────────────────────────────────────────────────
--  MANTENIMIENTO: función para cierre diario de vencidas (4.4)
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION cerrar_ofertas_vencidas()
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE ofertas
    SET activa = FALSE,
        actualizada_en = NOW(),
        fecha_cierre_detectada = NOW()
    WHERE activa = TRUE
      AND fecha_cierre IS NOT NULL
      AND fecha_cierre < CURRENT_DATE;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
--  FIN DEL SCHEMA
-- ============================================================
