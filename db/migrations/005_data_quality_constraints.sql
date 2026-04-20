-- ============================================================
--  Migration 005 — CHECK constraints para calidad de datos
--  contrataoplanta.cl
--
--  La auditoría detectó que no había cotas sobre rentas ni fechas:
--    - Sueldos absurdos (9 999 999 999 por error de OCR, 1 peso por
--      parseo de UF, etc.) se persistían silenciosamente.
--    - Fechas de publicación o cierre fuera de rango razonable
--      (1970-01-01, 2099-12-31) pasaban igual.
--
--  Se añaden constraints con NOT VALID para no fallar si hay datos
--  históricos fuera de rango; los nuevos INSERT/UPDATE sí se validan.
--  Cuando la data se limpie, correr:
--    ALTER TABLE ofertas VALIDATE CONSTRAINT <nombre>;
--
--  Valores:
--    - rentas: 300 000 a 20 000 000 CLP (sueldos públicos chilenos;
--      el grado EUS 1 está en ~4.8M, techo holgado para cargos
--      excepcionales; 300k bajo el mínimo legal por si hay media
--      jornada u honorarios parciales).
--    - fechas: entre 2020-01-01 y current + 3 años (cualquier otra
--      cosa es error de OCR o parseo).
--
--  Idempotente: todos los ADD CONSTRAINT usan IF NOT EXISTS via
--  DO $$ BEGIN IF NOT EXISTS (...) THEN ... END IF; END $$.
-- ============================================================

-- Renta mínima razonable
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_renta_min_rango'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_renta_min_rango
            CHECK (
                renta_bruta_min IS NULL OR
                renta_bruta_min BETWEEN 300000 AND 20000000
            ) NOT VALID;
    END IF;
END $$;

-- Renta máxima razonable
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_renta_max_rango'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_renta_max_rango
            CHECK (
                renta_bruta_max IS NULL OR
                renta_bruta_max BETWEEN 300000 AND 20000000
            ) NOT VALID;
    END IF;
END $$;

-- Coherencia min <= max
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_renta_min_leq_max'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_renta_min_leq_max
            CHECK (
                renta_bruta_min IS NULL OR
                renta_bruta_max IS NULL OR
                renta_bruta_min <= renta_bruta_max
            ) NOT VALID;
    END IF;
END $$;

-- Fecha de publicación en ventana razonable
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_fecha_publicacion_rango'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_fecha_publicacion_rango
            CHECK (
                fecha_publicacion IS NULL OR
                fecha_publicacion BETWEEN DATE '2020-01-01'
                                     AND CURRENT_DATE + INTERVAL '1 year'
            ) NOT VALID;
    END IF;
END $$;

-- Fecha de cierre en ventana razonable
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_fecha_cierre_rango'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_fecha_cierre_rango
            CHECK (
                fecha_cierre IS NULL OR
                fecha_cierre BETWEEN DATE '2020-01-01'
                                AND CURRENT_DATE + INTERVAL '3 years'
            ) NOT VALID;
    END IF;
END $$;

-- Horas semanales razonables
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ofertas_horas_semanales_rango'
    ) THEN
        ALTER TABLE ofertas ADD CONSTRAINT chk_ofertas_horas_semanales_rango
            CHECK (
                horas_semanales IS NULL OR
                horas_semanales BETWEEN 1 AND 88
            ) NOT VALID;
    END IF;
END $$;
