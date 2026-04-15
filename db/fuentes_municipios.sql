-- ============================================================
--  EmpleoEstado.cl — Fuentes Municipales (Top 10 verificadas)
--  Ejecutar: psql -U empleoestado_user -d empleoestado -f db/fuentes_municipios.sql
-- ============================================================

INSERT INTO fuentes (id, nombre, url_base, sector, frecuencia_hrs) VALUES
(20, 'Municipalidad de Puente Alto',  'https://www.mpuentealto.cl',               'Municipal', 24),
(21, 'Municipalidad de San Bernardo', 'https://www.sanbernardo.cl',               'Municipal', 24),
(22, 'Municipalidad de La Florida',   'https://www.laflorida.cl',                 'Municipal', 24),
(23, 'Municipalidad de Temuco',       'https://www.temuco.cl',                    'Municipal', 24),
(24, 'Municipalidad de Maipú',        'https://www.municipalidadmaipu.cl',        'Municipal', 24),
(25, 'Municipalidad de Santiago',     'https://www.munistgo.cl',                  'Municipal', 12),
(26, 'Municipalidad de Antofagasta',  'https://www.municipalidaddeantofagasta.cl','Municipal', 24),
(27, 'Municipalidad de Viña del Mar', 'https://www.munivina.cl',                  'Municipal', 24),
(28, 'Municipalidad de Las Condes',   'https://lascondes.omil.cl',                'Municipal', 24),
(29, 'Municipalidad de Valparaíso',   'https://municipalidaddevalparaiso.cl',     'Municipal', 24)
ON CONFLICT (id) DO UPDATE SET
    nombre        = EXCLUDED.nombre,
    url_base      = EXCLUDED.url_base,
    sector        = EXCLUDED.sector,
    frecuencia_hrs = EXCLUDED.frecuencia_hrs;

-- Verificar
SELECT id, nombre, sector, frecuencia_hrs FROM fuentes ORDER BY id;
