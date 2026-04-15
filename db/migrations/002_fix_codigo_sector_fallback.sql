-- ============================================================
--  Migration 002 — Fix: fallback de sector a `instituciones`
--
--  El scraper `scrapers/base.py` hace INSERT en `ofertas`
--  pasando `institucion_id` pero NO `sector` ni `fuente_id`,
--  así que el trigger original de la migration 001 asignaba
--  siempre 'OTR' a las ofertas de los scrapers reales.
--
--  Esta migration reemplaza la función `asignar_codigo_oferta`
--  con una versión que agrega un tercer fallback:
--    1. NEW.sector           (directo)
--    2. fuentes.sector       vía NEW.fuente_id
--    3. instituciones.sector vía NEW.institucion_id   ← NUEVO
-- ============================================================

BEGIN;

CREATE OR REPLACE FUNCTION asignar_codigo_oferta() RETURNS TRIGGER AS $$
DECLARE
    v_anio        CHAR(3);
    v_sector_raw  TEXT;
    v_sector      CHAR(3);
    v_correlativo INTEGER;
BEGIN
    IF NEW.codigo IS NOT NULL THEN
        RETURN NEW;
    END IF;

    v_anio := TO_CHAR(NOW(), 'YYY');

    -- 1) Sector directo en el propio NEW
    v_sector_raw := COALESCE(NEW.sector, '');

    -- 2) Sector desde `fuentes` vía fuente_id
    IF v_sector_raw = '' AND NEW.fuente_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM fuentes WHERE id = NEW.fuente_id;
    END IF;

    -- 3) Sector desde `instituciones` vía institucion_id
    IF (v_sector_raw IS NULL OR v_sector_raw = '') AND NEW.institucion_id IS NOT NULL THEN
        SELECT sector INTO v_sector_raw FROM instituciones WHERE id = NEW.institucion_id;
    END IF;

    v_sector := CASE
        WHEN unaccent(lower(COALESCE(v_sector_raw,''))) LIKE '%ejecut%'      THEN 'EJE'
        WHEN unaccent(lower(COALESCE(v_sector_raw,''))) LIKE '%municip%'     THEN 'MUN'
        WHEN unaccent(lower(COALESCE(v_sector_raw,''))) LIKE '%judic%'       THEN 'JUD'
        WHEN unaccent(lower(COALESCE(v_sector_raw,''))) LIKE '%adp%'
          OR unaccent(lower(COALESCE(v_sector_raw,''))) LIKE '%alta direcc%' THEN 'ADP'
        WHEN unaccent(lower(COALESCE(v_sector_raw,''))) LIKE '%auton%'       THEN 'AUT'
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

COMMIT;
