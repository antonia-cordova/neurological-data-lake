SET client_encoding = 'UTF8';

-- Despliegue puntual de la función de mapeos integrados.
-- Ejecutar únicamente en health_datalake; no recrea tablas ni elimina datos.
BEGIN;

DO $preflight$
DECLARE
    current_db TEXT;
    indicator_count INTEGER;
    measure_count INTEGER;
    variable_count INTEGER;
BEGIN
    SELECT current_database() INTO current_db;
    IF current_db <> 'health_datalake' THEN
        RAISE EXCEPTION 'Despliegue cancelado: base detectada %, esperada health_datalake', current_db;
    END IF;

    IF to_regclass('gold.bridge_health_indicator_source') IS NULL
       OR to_regclass('gold.dim_health_integrated_indicator') IS NULL
       OR to_regclass('gold.dim_health_measure') IS NULL
       OR to_regclass('gold.dim_brfss_variable') IS NULL
       OR to_regclass('gold.dim_source') IS NULL THEN
        RAISE EXCEPTION 'Despliegue cancelado: faltan tablas/dimensiones Gold requeridas';
    END IF;

    SELECT COUNT(*) INTO indicator_count
    FROM gold.dim_health_integrated_indicator;
    SELECT COUNT(*) INTO measure_count
    FROM gold.dim_health_measure;
    SELECT COUNT(*) INTO variable_count
    FROM gold.dim_brfss_variable;

    IF indicator_count <> 15 OR measure_count <> 15 OR variable_count <> 20 THEN
        RAISE EXCEPTION
            'Despliegue cancelado: dimensiones inesperadas (indicadores %, medidas %, variables BRFSS %)',
            indicator_count, measure_count, variable_count;
    END IF;
END
$preflight$;

CREATE OR REPLACE FUNCTION gold.ensure_integrated_indicator_source_bridge()
RETURNS TABLE (places_mappings INTEGER, brfss_mappings INTEGER)
LANGUAGE plpgsql
AS $function$
DECLARE
    places_count INTEGER;
    brfss_count INTEGER;
BEGIN
    WITH mapping_values(measure_id, indicator_code, relation_type, comparability, mapping_notes) AS (
        VALUES
        ('ARTHRITIS','ARTHRITIS','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa. La comparación requiere controlar año, población, geografía y tipo de prevalencia.'),
        ('CHD','CORONARY_HEART_DISEASE','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con enfermedad coronaria. Verificar metodología, población, año y tipo de prevalencia.'),
        ('COGNITION','COGNITION','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con discapacidad cognitiva. Verificar metodología, población, año y tipo de prevalencia.'),
        ('DEPRESSION','DEPRESSION','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con depresión. Verificar metodología, población, año y tipo de prevalencia.'),
        ('DIABETES','DIABETES','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con diabetes diagnosticada. Verificar metodología, población, año y tipo de prevalencia.'),
        ('DISABILITY','DISABILITY','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con discapacidad. Verificar definición operacional, población, año y tipo de prevalencia.'),
        ('GHLTH','GENERAL_HEALTH','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con salud general autopercibida. Verificar categorías, metodología, población y año.'),
        ('HEARING','HEARING','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con discapacidad auditiva. Verificar metodología, población, año y tipo de prevalencia.'),
        ('INDEPLIVE','INDEPLIVE','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con discapacidad para la vida independiente. Verificar definición operacional y población.'),
        ('MHLTH','MENTAL_HEALTH','PARTIAL','NOT_DIRECTLY_COMPARABLE','Relación parcial: PLACES mide malestar mental frecuente, no la totalidad del constructo de salud mental.'),
        ('MOBILITY','MOBILITY','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con discapacidad de movilidad. Verificar metodología, población, año y tipo de prevalencia.'),
        ('PHLTH','PHYSICAL_HEALTH','PARTIAL','NOT_DIRECTLY_COMPARABLE','Relación parcial: PLACES mide malestar físico frecuente, no la totalidad del constructo de salud física.'),
        ('SELFCARE','SELFCARE','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con discapacidad para el autocuidado. Verificar definición operacional y población.'),
        ('STROKE','STROKE','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con accidente cerebrovascular. Verificar metodología, población, año y tipo de prevalencia.'),
        ('VISION','VISION','DIRECT','COMPARABLE_WITH_CAVEATS','Correspondencia directa con discapacidad visual. Verificar metodología, población, año y tipo de prevalencia.')
    ),
    resolved AS (
        SELECT ii.indicator_key, src.source_key, hm.health_measure_key,
               mv.relation_type, mv.comparability, mv.mapping_notes
        FROM mapping_values mv
        JOIN gold.dim_health_measure hm ON hm.measure_id = mv.measure_id
        JOIN gold.dim_health_integrated_indicator ii ON ii.indicator_code = mv.indicator_code
        JOIN gold.dim_source src ON src.source_code = 'PLACES' AND src.dataset_name = 'places_2025'
    )
    INSERT INTO gold.bridge_health_indicator_source (
        indicator_key, source_key, health_measure_key, variable_key,
        relation_type, comparability, mapping_notes
    )
    SELECT r.indicator_key, r.source_key, r.health_measure_key, NULL,
           r.relation_type, r.comparability, r.mapping_notes
    FROM resolved r
    ON CONFLICT (indicator_key, source_key, health_measure_key)
        WHERE health_measure_key IS NOT NULL
    DO UPDATE SET
        relation_type = EXCLUDED.relation_type,
        comparability = EXCLUDED.comparability,
        mapping_notes = EXCLUDED.mapping_notes;

    WITH mapping_values(indicator_code, variable_code, relation_type, comparability, mapping_notes) AS (
        VALUES
        ('ARTHRITIS','HAVARTH4','CONCEPTUAL','COMPARABLE_WITH_CAVEATS','Diagnóstico de artritis autodeclarado; revisar codificación.'),
        ('CORONARY_HEART_DISEASE','CVDCRHD4','CONCEPTUAL','COMPARABLE_WITH_CAVEATS','Antecedente autodeclarado de enfermedad coronaria.'),
        ('DEPRESSION','ADDEPEV3','CONCEPTUAL','COMPARABLE_WITH_CAVEATS','Diagnóstico de trastorno depresivo autodeclarado.'),
        ('DIABETES','DIABETE4','CONCEPTUAL','COMPARABLE_WITH_CAVEATS','Distinguir diabetes, diabetes gestacional y prediabetes.'),
        ('GENERAL_HEALTH','GENHLTH','CONCEPTUAL','COMPARABLE_WITH_CAVEATS','Respuesta ordinal de salud general percibida.'),
        ('MENTAL_HEALTH','MENTHLTH','PARTIAL','NOT_DIRECTLY_COMPARABLE','Número de días de mala salud mental; no equivale directamente a prevalencia de distress frecuente.'),
        ('PHYSICAL_HEALTH','PHYSHLTH','PARTIAL','NOT_DIRECTLY_COMPARABLE','Número de días de mala salud física; no equivale directamente a prevalencia de distress frecuente.'),
        ('STROKE','CVDSTRK3','CONCEPTUAL','COMPARABLE_WITH_CAVEATS','Antecedente autodeclarado de accidente cerebrovascular.'),
        ('MOBILITY','DIFFWALK','PARTIAL','NOT_DIRECTLY_COMPARABLE','Dificultad para caminar o subir escaleras.'),
        ('DISABILITY','DIFFWALK','PARTIAL','NOT_DIRECTLY_COMPARABLE','Una dimensión funcional de la discapacidad.'),
        ('DISABILITY','DIFFALON','PARTIAL','NOT_DIRECTLY_COMPARABLE','Dificultad para realizar actividades y gestiones de forma independiente.')
    ),
    resolved AS (
        SELECT ii.indicator_key, src.source_key, v.variable_key,
               mv.relation_type, mv.comparability, mv.mapping_notes
        FROM mapping_values mv
        JOIN gold.dim_health_integrated_indicator ii ON ii.indicator_code = mv.indicator_code
        JOIN gold.dim_brfss_variable v ON v.variable_code = mv.variable_code
        JOIN gold.dim_source src ON src.source_code = 'BRFSS' AND src.dataset_name = 'brfss_2023'
    )
    INSERT INTO gold.bridge_health_indicator_source (
        indicator_key, source_key, health_measure_key, variable_key,
        relation_type, comparability, mapping_notes
    )
    SELECT r.indicator_key, r.source_key, NULL, r.variable_key,
           r.relation_type, r.comparability, r.mapping_notes
    FROM resolved r
    ON CONFLICT (indicator_key, source_key, variable_key)
        WHERE variable_key IS NOT NULL
    DO UPDATE SET
        relation_type = EXCLUDED.relation_type,
        comparability = EXCLUDED.comparability,
        mapping_notes = EXCLUDED.mapping_notes;

    SELECT COUNT(*) INTO places_count
    FROM gold.bridge_health_indicator_source b
    JOIN gold.dim_source s ON s.source_key = b.source_key
    WHERE s.source_code = 'PLACES'
      AND b.health_measure_key IS NOT NULL AND b.variable_key IS NULL;

    SELECT COUNT(*) INTO brfss_count
    FROM gold.bridge_health_indicator_source b
    JOIN gold.dim_source s ON s.source_key = b.source_key
    WHERE s.source_code = 'BRFSS'
      AND b.variable_key IS NOT NULL AND b.health_measure_key IS NULL;

    IF places_count <> 15 OR brfss_count <> 11 THEN
        RAISE EXCEPTION
            'Bridge integrado incompleto: PLACES %/15, BRFSS %/11',
            places_count, brfss_count;
    END IF;

    RETURN QUERY SELECT places_count, brfss_count;
END;
$function$;

COMMIT;
