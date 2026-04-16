-- ============================================================
--  EmpleoEstado.cl — Schema de Base de Datos
--  PostgreSQL 14+
-- ============================================================

-- Extensiones útiles
CREATE EXTENSION IF NOT EXISTS unaccent;  -- para búsqueda sin tildes

-- ──────────────────────────────────────────────────────────
--  TABLA: fuentes
--  Registro de cada portal scrapeado
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fuentes (
    id              SERIAL PRIMARY KEY,
    nombre          VARCHAR(200) NOT NULL,
    url_base        TEXT NOT NULL,
    sector          VARCHAR(80),       -- Ejecutivo, Municipal, Judicial, etc.
    activa          BOOLEAN DEFAULT TRUE,
    frecuencia_hrs  INTEGER DEFAULT 12, -- cada cuántas horas ejecutar
    ultima_ejecucion TIMESTAMPTZ,
    ultima_exitosa   TIMESTAMPTZ,
    total_ofertas    INTEGER DEFAULT 0,
    creada_en       TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────
--  TABLA: instituciones
--  Catálogo de instituciones públicas
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS instituciones (
    id              SERIAL PRIMARY KEY,
    nombre          VARCHAR(300) NOT NULL,
    nombre_corto    VARCHAR(80),        -- sigla o acrónimo: PDI, SII, etc.
    sector          VARCHAR(80),
    tipo            VARCHAR(80),        -- Ministerio, Servicio, Municipio, etc.
    region          VARCHAR(80),
    url_empleo      TEXT,
    fuente_id       INTEGER REFERENCES fuentes(id),
    color_hex       VARCHAR(7) DEFAULT '#1F4E79',
    creada_en       TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────
--  TABLA: ofertas
--  Corazón del sistema: cada oferta laboral
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas (
    id                  SERIAL PRIMARY KEY,

    -- Enumerador propio (formato EE-YYY-SEC-NNNNNN, asignado por trigger)
    codigo              VARCHAR(30) UNIQUE,

    -- Identificación externa (para detectar duplicados)
    id_externo          VARCHAR(200),      -- ID en el sistema origen
    fuente_id           INTEGER REFERENCES fuentes(id),
    url_original        TEXT,              -- URL de la oferta en el sitio oficial (legado)
    url_oferta          TEXT UNIQUE,       -- URL canónica usada por el nuevo scraper
    url_hash            VARCHAR(64) UNIQUE, -- SHA256 de url_original (índice rápido)

    -- Datos principales
    cargo               VARCHAR(500) NOT NULL,
    descripcion         TEXT,
    institucion_id      INTEGER REFERENCES instituciones(id),
    institucion_nombre  VARCHAR(300),      -- desnormalizado para rapidez
    sector              VARCHAR(80),
    area_profesional    VARCHAR(100),
    tipo_cargo          VARCHAR(50),       -- Planta, Contrata, Honorarios, ADP
    nivel               VARCHAR(80),       -- Directivo, Profesional, Técnico, etc.

    -- Ubicación
    region              VARCHAR(80),
    ciudad              VARCHAR(80),

    -- Condiciones laborales
    renta_bruta_min     BIGINT,            -- en pesos CLP
    renta_bruta_max     BIGINT,
    renta_texto         VARCHAR(200),      -- cuando viene como texto libre
    horas_semanales     INTEGER,
    modalidad           VARCHAR(50),       -- Presencial, Teletrabajo, Mixta

    -- Plazos
    fecha_publicacion   DATE,
    fecha_cierre        DATE,
    fecha_inicio        DATE,

    -- Requisitos (texto libre del scraper)
    requisitos_texto    TEXT,
    experiencia_anos    INTEGER,

    -- Estado
    activa              BOOLEAN DEFAULT TRUE,
    es_nueva            BOOLEAN DEFAULT TRUE,  -- flag primeras 24hrs
    vistas              INTEGER DEFAULT 0,

    -- Auditoría
    creada_en              TIMESTAMPTZ DEFAULT NOW(),
    actualizada_en         TIMESTAMPTZ DEFAULT NOW(),
    detectada_en           TIMESTAMPTZ DEFAULT NOW(),  -- cuándo la encontró el scraper
    fecha_cierre_detectada TIMESTAMPTZ                 -- cuándo el scraper dejó de verla
);

-- Índices para performance de búsqueda
CREATE INDEX IF NOT EXISTS idx_ofertas_sector     ON ofertas(sector);
CREATE INDEX IF NOT EXISTS idx_ofertas_region     ON ofertas(region);
CREATE INDEX IF NOT EXISTS idx_ofertas_tipo       ON ofertas(tipo_cargo);
CREATE INDEX IF NOT EXISTS idx_ofertas_area       ON ofertas(area_profesional);
CREATE INDEX IF NOT EXISTS idx_ofertas_cierre     ON ofertas(fecha_cierre);
CREATE INDEX IF NOT EXISTS idx_ofertas_activa     ON ofertas(activa);
CREATE INDEX IF NOT EXISTS idx_ofertas_fuente     ON ofertas(fuente_id);
CREATE INDEX IF NOT EXISTS idx_ofertas_nueva      ON ofertas(es_nueva);

-- Índice de búsqueda de texto completo (español con unaccent)
CREATE INDEX IF NOT EXISTS idx_ofertas_fts ON ofertas
    USING GIN (to_tsvector('spanish', coalesce(cargo,'') || ' ' || coalesce(institucion_nombre,'') || ' ' || coalesce(descripcion,'')));

-- ──────────────────────────────────────────────────────────
--  TABLA: logs_scraping
--  Registro de cada ejecución del scraper
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logs_scraping (
    id              SERIAL PRIMARY KEY,
    fuente_id       INTEGER REFERENCES fuentes(id),
    iniciado_en     TIMESTAMPTZ DEFAULT NOW(),
    finalizado_en   TIMESTAMPTZ,
    duracion_seg    NUMERIC(8,2),
    estado          VARCHAR(20),    -- OK, ERROR, PARCIAL
    ofertas_nuevas  INTEGER DEFAULT 0,
    ofertas_actualizadas INTEGER DEFAULT 0,
    ofertas_cerradas INTEGER DEFAULT 0,
    paginas_visitadas INTEGER DEFAULT 0,
    error_mensaje   TEXT,
    detalle         JSONB           -- info adicional en formato JSON
);

-- ──────────────────────────────────────────────────────────
--  TABLA: usuarios
--  Suscriptores de alertas (fase MVP)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS usuarios (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(200) UNIQUE NOT NULL,
    nombre          VARCHAR(200),
    activo          BOOLEAN DEFAULT TRUE,
    verificado      BOOLEAN DEFAULT FALSE,
    token_verificacion VARCHAR(100),

    -- Preferencias de alerta
    alerta_sector   VARCHAR(80)[],      -- array de sectores de interés
    alerta_region   VARCHAR(80)[],
    alerta_area     VARCHAR(100)[],
    alerta_tipo     VARCHAR(50)[],      -- Planta, Contrata, etc.
    frecuencia_alerta VARCHAR(20) DEFAULT 'diaria',  -- diaria, instantanea

    creado_en       TIMESTAMPTZ DEFAULT NOW(),
    ultimo_acceso   TIMESTAMPTZ
);

-- ──────────────────────────────────────────────────────────
--  TABLA: alertas_enviadas
--  Historial de emails enviados
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alertas_enviadas (
    id          SERIAL PRIMARY KEY,
    usuario_id  INTEGER REFERENCES usuarios(id),
    oferta_id   INTEGER REFERENCES ofertas(id),
    enviada_en  TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────
--  ENUMERADOR PROPIO: código EE-YYY-SEC-NNNNNN
--  Estable forever: se asigna en BEFORE INSERT y no cambia
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ofertas_contador (
    anio    CHAR(3)  NOT NULL,
    sector  CHAR(3)  NOT NULL,
    ultimo  INTEGER  NOT NULL DEFAULT 0,
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

    v_anio := TO_CHAR(NOW(), 'YYY');   -- 2026 → '026'

    -- Fallbacks en cascada para obtener el sector:
    --   1) NEW.sector directo
    --   2) fuentes.sector       via NEW.fuente_id
    --   3) instituciones.sector via NEW.institucion_id
    v_sector_raw := COALESCE(NEW.sector, '');
    IF v_sector_raw = '' AND NEW.fuente_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM fuentes WHERE id = NEW.fuente_id;
    END IF;
    IF (v_sector_raw IS NULL OR v_sector_raw = '') AND NEW.institucion_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM instituciones WHERE id = NEW.institucion_id;
    END IF;

    v_sector_norm := unaccent(lower(COALESCE(v_sector_raw, '')));

    -- Normalizar a código de 3 letras. Orden: más específicos primero.
    --   JUD Judicial  | MUN Municipal  | SAL Salud Pública
    --   EDU Universidad/Educación | GOR Gobierno Regional
    --   EMP Empresa del Estado    | FFA FF.AA. y Orden
    --   LEG Legislativo | EJE Ejecutivo Central
    --   ADP Alta Dirección Pública | AUT Autónomo/Regulador
    --   OTR (fallback)
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
--  TABLA: ofertas_snapshots
--  Una fila por cada vez que el scraper toca la oferta
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

CREATE INDEX IF NOT EXISTS idx_snapshots_oferta ON ofertas_snapshots(oferta_id, capturado_en DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_codigo ON ofertas_snapshots(codigo);

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
--  TABLA: ofertas_historico
--  Versión previa archivada cada vez que cambia un campo
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

CREATE INDEX IF NOT EXISTS idx_historico_oferta ON ofertas_historico(oferta_id, versionado_en DESC);

CREATE OR REPLACE FUNCTION archivar_version_oferta() RETURNS TRIGGER AS $$
BEGIN
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

-- ──────────────────────────────────────────────────────────
--  DATOS INICIALES: fuentes
-- ──────────────────────────────────────────────────────────
INSERT INTO fuentes (nombre, url_base, sector, frecuencia_hrs) VALUES
('Portal Empleos Públicos - Servicio Civil', 'https://www.empleospublicos.cl', 'Ejecutivo', 12),
('Alta Dirección Pública (ADP)',             'https://adp.serviciocivil.cl',   'ADP',       24),
('Poder Judicial',                           'https://www.pjud.cl',            'Judicial',  24),
('Banco Central de Chile',                   'https://empleos.bcentral.cl',    'Autónomo',  24),
('Contraloría General de la República',      'https://www.contraloria.cl',     'Autónomo',  48),
('Ministerio Público - Fiscalía',            'https://www.fiscaliadechile.cl', 'Autónomo',  48),
('Dirección del Trabajo',                    'https://www.dt.gob.cl',          'Ejecutivo', 48)
ON CONFLICT DO NOTHING;

-- Vista útil: ofertas vigentes con días restantes
CREATE OR REPLACE VIEW v_ofertas_vigentes AS
SELECT
    o.*,
    i.nombre_corto,
    i.color_hex,
    (o.fecha_cierre - CURRENT_DATE) AS dias_restantes,
    CASE
        WHEN (o.fecha_cierre - CURRENT_DATE) <= 2  THEN 'urgente'
        WHEN (o.fecha_cierre - CURRENT_DATE) <= 5  THEN 'pronto'
        ELSE 'vigente'
    END AS estado_plazo
FROM ofertas o
LEFT JOIN instituciones i ON o.institucion_id = i.id
WHERE o.activa = TRUE
  AND (o.fecha_cierre IS NULL OR o.fecha_cierre >= CURRENT_DATE)
ORDER BY CASE WHEN o.fecha_cierre IS NULL AND o.fecha_inicio IS NULL THEN 1 ELSE 0 END ASC, o.fecha_cierre ASC NULLS LAST;
