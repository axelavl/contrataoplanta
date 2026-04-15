-- Test end-to-end del enumerador + snapshots + histórico
-- Se ejecuta dentro de una transacción y hace ROLLBACK al final.

BEGIN;

-- ── Test 1: INSERT de una oferta del Poder Judicial (sector JUD) ──
INSERT INTO ofertas (fuente_id, url_original, url_hash, cargo, sector, region, fecha_cierre)
VALUES (3, 'https://www.pjud.cl/test/1', 'testhash001', 'Abogado de Prueba', 'Judicial', 'Metropolitana de Santiago', '2026-05-30');

SELECT 'T1 codigo asignado' AS check, codigo FROM ofertas WHERE url_hash = 'testhash001';

-- ── Test 2: INSERT de una oferta ejecutiva (sector EJE) ──
INSERT INTO ofertas (fuente_id, url_original, url_hash, cargo, sector, region)
VALUES (1, 'https://empleospublicos.cl/test/2', 'testhash002', 'Profesional TI', 'Ejecutivo', 'Valparaíso');

SELECT 'T2 codigo asignado' AS check, codigo FROM ofertas WHERE url_hash = 'testhash002';

-- ── Test 3: Segundo JUD para verificar correlativo ──
INSERT INTO ofertas (fuente_id, url_original, url_hash, cargo, sector, region)
VALUES (3, 'https://www.pjud.cl/test/3', 'testhash003', 'Secretario Judicial', 'Judicial', 'Biobío');

SELECT 'T3 correlativo JUD' AS check, codigo FROM ofertas WHERE url_hash = 'testhash003';

-- ── Test 4: UPDATE del Test 1 cambiando el cargo → debe crear historico ──
UPDATE ofertas SET cargo = 'Abogado Senior de Prueba' WHERE url_hash = 'testhash001';

SELECT 'T4 historico filas' AS check, COUNT(*)::text AS value FROM ofertas_historico WHERE codigo = (SELECT codigo FROM ofertas WHERE url_hash = 'testhash001');

-- ── Test 5: snapshots por cada INSERT/UPDATE ──
SELECT 'T5 snapshots totales' AS check, COUNT(*)::text AS value FROM ofertas_snapshots WHERE codigo LIKE 'EE-%';

-- Timeline del oferta 1 (debe tener 2 snapshots: INSERT + UPDATE)
SELECT 'T5b timeline oferta1' AS check, origen, codigo, cargo
FROM ofertas_snapshots
WHERE codigo = (SELECT codigo FROM ofertas WHERE url_hash = 'testhash001')
ORDER BY capturado_en;

-- ── Test 6: marcar_ofertas_cerradas simulado (pasa fecha_cierre_detectada) ──
UPDATE ofertas
SET activa = FALSE,
    actualizada_en = NOW(),
    fecha_cierre_detectada = COALESCE(fecha_cierre_detectada, NOW())
WHERE url_hash = 'testhash002';

SELECT 'T6 fecha_cierre_detectada' AS check, (fecha_cierre_detectada IS NOT NULL)::text AS value FROM ofertas WHERE url_hash = 'testhash002';

-- ── Test 7: estado del contador ──
SELECT 'T7 contador' AS check, anio || '/' || sector || '=' || ultimo AS value FROM ofertas_contador ORDER BY sector;

ROLLBACK;
