-- Limpieza one-off: páginas de Ley de Transparencia indexadas como ofertas
-- (auditoría empresas del estado, 2026-06-11).
--
-- Contexto: el scraper genérico (generic_site) clasificó como ofertas las
-- secciones de transparencia de Empresa Portuaria Coquimbo (institución 287):
-- "Directorio y Personal Planta" y el índice "Transparencia" (ids 5251, 5252),
-- ambas con tipo_contrato "planta" por la keyword suelta. La política de
-- clasificación (classification/policy.py, ruleset 2026.06.11) ya excluye
-- estos casos hacia adelante; este script desactiva los residuos existentes.
--
-- Uso (revisa SIEMPRE el SELECT antes de ejecutar el UPDATE):
--   psql "$DATABASE_URL" -f db/limpieza_transparencia_empresas.sql
--
-- Por defecto DESACTIVA (activa = FALSE), el soft-delete del sistema:
-- el frontend filtra por activa = TRUE. Para borrado físico usa el DELETE
-- comentado al final.

BEGIN;

-- 1) Previsualización: cuántas y cuáles se verían afectadas.
--    Nota: se excluyen explícitamente rutas legítimas de empleo bajo
--    /transparencia/ (trabaje-con-nosotros, concursos).
SELECT id, institucion_id, cargo, tipo_contrato, activa, url_oferta
FROM ofertas
WHERE activa = TRUE
  AND (
        id IN (5251, 5252)
        OR (
            (url_oferta ILIKE '%/transparencia/%'
             OR url_oferta ILIKE '%directorio-personal%'
             OR url_oferta ILIKE '%personal-planta%'
             OR url_oferta ILIKE '%estados-financieros%'
             OR url_oferta ILIKE '%marco-normativo%'
             OR url_oferta ILIKE '%estructura-organica%'
             OR url_oferta ILIKE '%memorias-anuales%')
            AND url_oferta NOT ILIKE '%trabaje-con-nosotros%'
            AND url_oferta NOT ILIKE '%trabaja-con-nosotros%'
            AND url_oferta NOT ILIKE '%concurso%'
        )
      )
ORDER BY institucion_id, id;

-- 2) Desactivación (previsualización revisada 2026-06-11: ids 5251, 5252, 919).
UPDATE ofertas
SET activa = FALSE
WHERE activa = TRUE
  AND (
        id IN (5251, 5252)
        OR (
            (url_oferta ILIKE '%/transparencia/%'
             OR url_oferta ILIKE '%directorio-personal%'
             OR url_oferta ILIKE '%personal-planta%'
             OR url_oferta ILIKE '%estados-financieros%'
             OR url_oferta ILIKE '%marco-normativo%'
             OR url_oferta ILIKE '%estructura-organica%'
             OR url_oferta ILIKE '%memorias-anuales%')
            AND url_oferta NOT ILIKE '%trabaje-con-nosotros%'
            AND url_oferta NOT ILIKE '%trabaja-con-nosotros%'
            AND url_oferta NOT ILIKE '%concurso%'
        )
      );

COMMIT;

-- 3) Borrado físico (opcional, NO recomendado salvo certeza total):
-- DELETE FROM ofertas WHERE id IN (5251, 5252);
