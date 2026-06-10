-- ============================================================
--  Migration 003 — Mapeo completo de sectores
--
--  La primera corrida del scraper reveló que el catálogo real
--  de 700 instituciones usa 10 sectores, no los 5 que había en
--  la migration 001. Las ofertas de Salud Pública, Educación,
--  Gobierno Regional, etc. estaban cayendo en 'OTR'.
--
--  Este fix agrega los 6 sectores faltantes:
--    SAL  — Salud Pública
--    EDU  — Universidad/Educación
--    GOR  — Gobierno Regional
--    EMP  — Empresa del Estado
--    FFA  — FF.AA. y Orden
--    LEG  — Legislativo
--
--  El orden del CASE importa: patrones más específicos primero.
-- ============================================================

BEGIN;

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

    -- Normalizar a código de 3 letras.
    -- Orden importa: primero los más específicos.
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

COMMIT;
