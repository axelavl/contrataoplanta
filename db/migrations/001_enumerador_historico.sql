-- ============================================================
--  Migration 001 — Enumerador propio + histórico de ofertas
--  EmpleoEstado.cl
--
--  Agrega:
--    1. Columna `codigo` con formato EE-YYY-SEC-NNNNNN
--       (estable para siempre, asignada en BEFORE INSERT)
--    2. Columna `fecha_cierre_detectada` (cuándo el scraper
--       dejó de ver la oferta)
--    3. Tabla `ofertas_snapshots` — una fila por cada vez
--       que el scraper toca la oferta (AFTER INSERT/UPDATE)
--    4. Tabla `ofertas_historico` — versión previa archivada
--       cada vez que un campo significativo cambia (BEFORE UPDATE)
--
--  Idempotente: se puede correr varias veces sin efectos extra.
-- ============================================================

BEGIN;

-- ──────────────────────────────────────────────────────────
-- 1. Nuevas columnas en ofertas
-- ──────────────────────────────────────────────────────────
ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS codigo                 VARCHAR(30);
ALTER TABLE ofertas ADD COLUMN IF NOT EXISTS fecha_cierre_detectada TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ofertas_codigo_key'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT ofertas_codigo_key UNIQUE (codigo);
    END IF;
END $$;

-- ──────────────────────────────────────────────────────────
-- 2. Contador atómico por (año, sector) para el correlativo
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas_contador (
    anio    CHAR(3)  NOT NULL,
    sector  CHAR(3)  NOT NULL,
    ultimo  INTEGER  NOT NULL DEFAULT 0,
    PRIMARY KEY (anio, sector)
);

-- ──────────────────────────────────────────────────────────
-- 3. Función y trigger que asigna `codigo` en BEFORE INSERT
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION asignar_codigo_oferta() RETURNS TRIGGER AS $$
DECLARE
    v_anio        CHAR(3);
    v_sector_raw  TEXT;
    v_sector_norm TEXT;
    v_sector      CHAR(3);
    v_correlativo INTEGER;
BEGIN
    -- Estable forever: si la fila ya trae código, respetarlo.
    IF NEW.codigo IS NOT NULL THEN
        RETURN NEW;
    END IF;

    -- Año en 3 dígitos: 2026 → '026', 2027 → '027'
    v_anio := TO_CHAR(NOW(), 'YYY');

    -- Fallbacks en cascada para obtener el sector:
    --   1) NEW.sector            (raramente pasado por el scraper)
    --   2) fuentes.sector        via NEW.fuente_id
    --   3) instituciones.sector  via NEW.institucion_id  ← el scraper real
    v_sector_raw := COALESCE(NEW.sector, '');
    IF v_sector_raw = '' AND NEW.fuente_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM fuentes WHERE id = NEW.fuente_id;
    END IF;
    IF (v_sector_raw IS NULL OR v_sector_raw = '') AND NEW.institucion_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM instituciones WHERE id = NEW.institucion_id;
    END IF;

    v_sector_norm := unaccent(lower(COALESCE(v_sector_raw, '')));

    -- Normalizar a código de 3 letras. Orden: más específicos primero.
    v_sector := CASE
        WHEN v_sector_norm LIKE '%judic%'                             THEN 'JUD'
        WHEN v_sector_norm LIKE '%municip%'                           THEN 'MUN'
        WHEN v_sector_norm LIKE '%salud%'                             THEN 'SAL'
        WHEN v_sector_norm LIKE '%universidad%'
          OR v_sector_norm LIKE '%educa%'                             THEN 'EDU'
        WHEN v_sector_norm LIKE '%gobierno%regional%'                 THEN 'GOR'
        WHEN v_sector_norm LIKE '%empresa%'                           THEN 'EMP'
        WHEN v_sector_norm LIKE '%ff.aa%'
          OR v_sector_norm LIKE '%fuerza%armad%'
          OR v_sector_norm LIKE '%orden%'                             THEN 'FFA'
        WHEN v_sector_norm LIKE '%legislat%'                          THEN 'LEG'
        WHEN v_sector_norm LIKE '%ejecut%'                            THEN 'EJE'
        WHEN v_sector_norm LIKE '%adp%'
          OR v_sector_norm LIKE '%alta direcc%'                       THEN 'ADP'
        WHEN v_sector_norm LIKE '%auton%'                             THEN 'AUT'
        ELSE 'OTR'
    END;

    -- Próximo correlativo atómico por (año, sector)
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
-- 4. Tabla ofertas_snapshots: una fila por cada observación
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    oferta_id           INTEGER NOT NULL REFERENCES ofertas(id) ON DELETE CASCADE,
    codigo              VARCHAR(30),
    capturado_en        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    origen              VARCHAR(10) NOT NULL,   -- 'INSERT' | 'UPDATE'

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

CREATE INDEX IF NOT EXISTS idx_snapshots_oferta
    ON ofertas_snapshots(oferta_id, capturado_en DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_codigo
    ON ofertas_snapshots(codigo);

CREATE OR REPLACE FUNCTION snapshot_oferta() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ofertas_snapshots (
        oferta_id, codigo, capturado_en, origen,
        fuente_id, cargo, descripcion, institucion_nombre, sector,
        area_profesional, tipo_cargo, nivel,
        region, ciudad,
        renta_bruta_min, renta_bruta_max, renta_texto,
        fecha_publicacion, fecha_cierre, activa
    ) VALUES (
        NEW.id, NEW.codigo, NOW(), TG_OP,
        NEW.fuente_id, NEW.cargo, NEW.descripcion, NEW.institucion_nombre, NEW.sector,
        NEW.area_profesional, NEW.tipo_cargo, NEW.nivel,
        NEW.region, NEW.ciudad,
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
-- 5. Tabla ofertas_historico: versión previa en cada UPDATE
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas_historico (
    id                  BIGSERIAL PRIMARY KEY,
    oferta_id           INTEGER NOT NULL REFERENCES ofertas(id) ON DELETE CASCADE,
    codigo              VARCHAR(30),
    versionado_en       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

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

CREATE INDEX IF NOT EXISTS idx_historico_oferta
    ON ofertas_historico(oferta_id, versionado_en DESC);

CREATE OR REPLACE FUNCTION archivar_version_oferta() RETURNS TRIGGER AS $$
BEGIN
    -- Solo archiva si hubo cambio real en un campo significativo
    IF (OLD.cargo              IS DISTINCT FROM NEW.cargo)
       OR (OLD.descripcion        IS DISTINCT FROM NEW.descripcion)
       OR (OLD.institucion_nombre IS DISTINCT FROM NEW.institucion_nombre)
       OR (OLD.sector             IS DISTINCT FROM NEW.sector)
       OR (OLD.area_profesional   IS DISTINCT FROM NEW.area_profesional)
       OR (OLD.tipo_cargo         IS DISTINCT FROM NEW.tipo_cargo)
       OR (OLD.nivel              IS DISTINCT FROM NEW.nivel)
       OR (OLD.region             IS DISTINCT FROM NEW.region)
       OR (OLD.ciudad             IS DISTINCT FROM NEW.ciudad)
       OR (OLD.renta_bruta_min    IS DISTINCT FROM NEW.renta_bruta_min)
       OR (OLD.renta_bruta_max    IS DISTINCT FROM NEW.renta_bruta_max)
       OR (OLD.renta_texto        IS DISTINCT FROM NEW.renta_texto)
       OR (OLD.fecha_publicacion  IS DISTINCT FROM NEW.fecha_publicacion)
       OR (OLD.fecha_cierre       IS DISTINCT FROM NEW.fecha_cierre)
       OR (OLD.activa             IS DISTINCT FROM NEW.activa)
    THEN
        INSERT INTO ofertas_historico (
            oferta_id, codigo, versionado_en,
            fuente_id, cargo, descripcion, institucion_nombre, sector,
            area_profesional, tipo_cargo, nivel,
            region, ciudad,
            renta_bruta_min, renta_bruta_max, renta_texto,
            fecha_publicacion, fecha_cierre, activa
        ) VALUES (
            OLD.id, OLD.codigo, NOW(),
            OLD.fuente_id, OLD.cargo, OLD.descripcion, OLD.institucion_nombre, OLD.sector,
            OLD.area_profesional, OLD.tipo_cargo, OLD.nivel,
            OLD.region, OLD.ciudad,
            OLD.renta_bruta_min, OLD.renta_bruta_max, OLD.renta_texto,
            OLD.fecha_publicacion, OLD.fecha_cierre, OLD.activa
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_archivar_version_oferta ON ofertas;
CREATE TRIGGER tr_archivar_version_oferta
    BEFORE UPDATE ON ofertas
    FOR EACH ROW
    EXECUTE FUNCTION archivar_version_oferta();

COMMIT;
