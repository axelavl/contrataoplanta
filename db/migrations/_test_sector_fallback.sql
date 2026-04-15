-- Test del fallback de sector a instituciones.sector
-- Simula el INSERT real que hace scrapers/base.py
BEGIN;

-- Crear una institución de prueba con cada sector representativo
INSERT INTO instituciones (id, nombre, sector, region)
VALUES
    (9001, 'Corte de Apelaciones de Prueba', 'Judicial',  'Metropolitana de Santiago'),
    (9002, 'Municipalidad de Prueba',        'Municipal', 'Biobío'),
    (9003, 'Ministerio de Prueba',           'Ejecutivo', 'Valparaíso'),
    (9004, 'Servicio Autónomo de Prueba',    'Autónomo',  'Antofagasta');

-- INSERT estilo base.py: pasa institucion_id pero NO sector/fuente_id
INSERT INTO ofertas (institucion_id, cargo, url_oferta)
VALUES
    (9001, 'Oficial 1º de Prueba',   'https://test/jud'),
    (9002, 'Fiscalizador de Prueba', 'https://test/mun'),
    (9003, 'Analista de Prueba',     'https://test/eje'),
    (9004, 'Especialista de Prueba', 'https://test/aut');

SELECT '→ fallback instituciones' AS check, institucion_id, codigo
FROM ofertas
WHERE url_oferta LIKE 'https://test/%'
ORDER BY institucion_id;

-- Una oferta SIN institucion_id → debe caer en OTR
INSERT INTO ofertas (cargo, url_oferta)
VALUES ('Cargo Huérfano', 'https://test/otr');

SELECT '→ sin institucion_id' AS check, codigo
FROM ofertas WHERE url_oferta = 'https://test/otr';

-- Estado del contador después del test
SELECT '→ contador' AS check, anio || '/' || sector || '=' || ultimo AS valor
FROM ofertas_contador ORDER BY sector;

ROLLBACK;
