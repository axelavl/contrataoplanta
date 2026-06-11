-- Limpieza one-off: ofertas de baja calidad de la Armada (institución 158)
-- generadas por el FfaaScraper genérico, antes de activar el scraper dedicado
-- scrapers/armada.py.
--
-- Contexto: el scraper genérico tomaba concursos ya cerrados como vigentes y
-- guardaba registros sin cierre/región/tipo. El scraper dedicado solo importa
-- los "postulacion-abierta" con datos estandarizados. Su limpieza automática
-- (marcar_ofertas_cerradas) se acota al dominio de las ofertas vistas en la
-- corrida, así que este script cubre cualquier residuo (incluido el dominio
-- de transparencia armada.cl) de una sola vez.
--
-- Uso (revisa SIEMPRE el SELECT antes de ejecutar el UPDATE):
--   psql "$DATABASE_URL" -f db/limpieza_armada_158.sql
--
-- Por defecto DESACTIVA (activa = FALSE), que es el soft-delete del sistema:
-- el frontend filtra por activa = TRUE, así que dejan de mostrarse. Si quieres
-- borrado físico, usa el DELETE comentado al final.

BEGIN;

-- 1) Previsualización: cuántas y cuáles se verían afectadas.
SELECT id, cargo, region, fecha_cierre, activa, url_oferta
FROM ofertas
WHERE (institucion_id = 158
       OR url_oferta ILIKE '%admisionarmada.cl%'
       OR url_oferta ILIKE '%//armada.cl%'
       OR url_oferta ILIKE '%.armada.cl%')
  AND activa = TRUE
ORDER BY fecha_cierre NULLS FIRST, id;

-- 2) Desactivación (soft-delete). Marca también la fecha de cierre detectada.
UPDATE ofertas
SET activa                 = FALSE,
    actualizada_en         = NOW(),
    fecha_cierre_detectada = COALESCE(fecha_cierre_detectada, NOW())
WHERE (institucion_id = 158
       OR url_oferta ILIKE '%admisionarmada.cl%'
       OR url_oferta ILIKE '%//armada.cl%'
       OR url_oferta ILIKE '%.armada.cl%')
  AND activa = TRUE;

-- Revisa el recuento devuelto por el UPDATE. Si es correcto:
COMMIT;
-- Si algo no cuadra:  ROLLBACK;

-- ── Alternativa: borrado físico (irreversible). Descomentar si lo prefieres ──
-- DELETE FROM ofertas
-- WHERE (institucion_id = 158
--        OR url_oferta ILIKE '%admisionarmada.cl%'
--        OR url_oferta ILIKE '%//armada.cl%'
--        OR url_oferta ILIKE '%.armada.cl%')
--   AND activa = FALSE;   -- borra solo las ya desactivadas, por seguridad
