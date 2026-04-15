-- Test exhaustivo: toma una institución real de cada sector
-- y verifica el PREFIJO del código asignado (el correlativo
-- incrementa desde el contador existente, no importa el número)
BEGIN;

-- Una oferta por cada sector real del catálogo
WITH una_por_sector AS (
    SELECT DISTINCT ON (sector) id, sector, nombre
    FROM instituciones
    WHERE sector IS NOT NULL
    ORDER BY sector, id
)
INSERT INTO ofertas (institucion_id, cargo, url_oferta)
SELECT id, 'Cargo de Prueba ' || sector, 'https://test/' || id
FROM una_por_sector;

-- Verificar el código asignado para cada sector
SELECT i.sector, o.codigo, LEFT(i.nombre, 45) AS institucion
FROM ofertas o
JOIN instituciones i ON i.id = o.institucion_id
WHERE o.url_oferta LIKE 'https://test/%'
ORDER BY i.sector;

-- Validar que ningún sector del catálogo produjo OTR
SELECT 'FALLOS' AS check, i.sector, o.codigo
FROM ofertas o
JOIN instituciones i ON i.id = o.institucion_id
WHERE o.url_oferta LIKE 'https://test/%'
  AND o.codigo LIKE '%-OTR-%';

ROLLBACK;
