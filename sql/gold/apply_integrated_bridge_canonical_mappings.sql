SET client_encoding = 'UTF8';

BEGIN;

DO $preflight$
BEGIN
    IF current_database() <> 'health_datalake' THEN
        RAISE EXCEPTION
            'Ejecución cancelada: base detectada %, esperada health_datalake',
            current_database();
    END IF;

    IF to_regprocedure('gold.ensure_integrated_indicator_source_bridge()') IS NULL THEN
        RAISE EXCEPTION
            'No existe gold.ensure_integrated_indicator_source_bridge(); despliegue primero la función revisada.';
    END IF;
END
$preflight$;

-- Inserta mappings faltantes y actualiza los atributos canónicos existentes.
SELECT *
FROM gold.ensure_integrated_indicator_source_bridge();

DO $validation$
DECLARE
    total_count INTEGER;
    artifact_count INTEGER;
    duplicate_count INTEGER;
BEGIN
    SELECT COUNT(*),
           COUNT(*) FILTER (
               WHERE mapping_notes LIKE '%Ã%'
                  OR mapping_notes LIKE '%??%'
           )
    INTO total_count, artifact_count
    FROM gold.bridge_health_indicator_source b
    JOIN gold.dim_source s ON s.source_key = b.source_key
    WHERE s.source_code IN ('PLACES', 'BRFSS');

    SELECT COUNT(*)
    INTO duplicate_count
    FROM (
        SELECT b.source_key, b.indicator_key,
               b.health_measure_key, b.variable_key
        FROM gold.bridge_health_indicator_source b
        JOIN gold.dim_source s ON s.source_key = b.source_key
        WHERE s.source_code IN ('PLACES', 'BRFSS')
        GROUP BY b.source_key, b.indicator_key,
                 b.health_measure_key, b.variable_key
        HAVING COUNT(*) > 1
    ) duplicates;

    IF total_count <> 26 OR artifact_count <> 0 OR duplicate_count <> 0 THEN
        RAISE EXCEPTION
            'Validación posterior fallida: mappings %, artefactos %, duplicados %',
            total_count, artifact_count, duplicate_count;
    END IF;
END
$validation$;

COMMIT;
