-- GOLD BRFSS 2023 - ANALISIS DESCRIPTIVO
-- Muestra: 2.800 personas, 52 jurisdicciones, 20 variables.
-- IMPORTANTE: resultados descriptivos NO ponderados de la muestra.
-- En psql: \set source_run_id 17
--             \i sql/gold/analysis_brfss.sql

-- 1. CONTROL DE COBERTURA Y TRAZABILIDAD
SELECT f.source_run_id, s.source_code, s.dataset_name,
       COUNT(DISTINCT (f.jurisdiction_key, f.sequence_number)) AS respondents,
       COUNT(DISTINCT f.jurisdiction_key) AS jurisdictions,
       COUNT(DISTINCT f.variable_key) AS variables,
       COUNT(*) AS facts
FROM gold.fact_brfss_response f
JOIN gold.dim_source s ON s.source_key = f.source_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
GROUP BY f.source_run_id, s.source_code, s.dataset_name;

-- 2. PERFIL DEMOGRAFICO: edad, sexo y grupo racial/etnico
-- _SEX: 1 masculino, 2 femenino. _AGEG5YR: códigos 1-13
SELECT v.variable_code, f.raw_value AS response_code,
       COUNT(*) AS respondents,
       ROUND(100.0 * COUNT(*) /
         NULLIF(SUM(COUNT(*)) OVER (PARTITION BY v.variable_code), 0), 2)
         AS percent_of_non_null_responses
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND v.variable_code IN ('_AGEG5YR', '_SEX', '_RACEGR3')
  AND NULLIF(BTRIM(f.raw_value), '') IS NOT NULL
GROUP BY v.variable_code, f.raw_value
ORDER BY v.variable_code, respondents DESC, response_code;

-- 3. AUTOPERCEPCION DE SALUD GENERAL (GENHLTH)
-- 1 excelente, 2 muy buena, 3 buena, 4 regular, 5 mala, 7 no sabe, 9 rechazo
SELECT f.raw_value AS genhlth_code,
       CASE f.raw_value
         WHEN '1' THEN 'Excellent' WHEN '2' THEN 'Very good'
         WHEN '3' THEN 'Good' WHEN '4' THEN 'Fair'
         WHEN '5' THEN 'Poor' WHEN '7' THEN 'Don''t know'
         WHEN '9' THEN 'Refused' ELSE 'Other / missing code'
       END AS health_category,
       COUNT(*) AS respondents,
       ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2)
         AS percent_of_non_null_responses
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND v.variable_code = 'GENHLTH'
  AND NULLIF(BTRIM(f.raw_value), '') IS NOT NULL
GROUP BY f.raw_value
ORDER BY f.raw_value;

-- 4. DIAS DE MALA SALUD FISICA, MENTAL Y LIMITACION DE ACTIVIDADES
-- numeric_value solo contiene valores válidos 1-30 para estas variables
SELECT v.variable_code, v.variable_description,
       COUNT(*) AS total_records, COUNT(f.numeric_value) AS valid_day_observations,
       ROUND(AVG(f.numeric_value), 2) AS mean_days,
       PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.numeric_value) AS median_days,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.numeric_value) AS p75_days,
       MIN(f.numeric_value) AS min_days, MAX(f.numeric_value) AS max_days
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND v.variable_code IN ('PHYSHLTH', 'MENTHLTH', 'POORHLTH')
GROUP BY v.variable_code, v.variable_description
ORDER BY v.variable_code;

-- 5. CONDICIONES CRONICAS Y LIMITACIONES FUNCIONALES
-- Para estas variables binarias: 1 = Sí, 2 = No.
-- El denominador excluye códigos especiales y valores nulos
SELECT v.variable_code, v.variable_description,
       COUNT(*) FILTER (WHERE f.raw_value IN ('1','2')) AS valid_yes_no,
       COUNT(*) FILTER (WHERE f.raw_value = '1') AS yes_count,
       COUNT(*) FILTER (WHERE f.raw_value = '2') AS no_count,
       ROUND(100.0 * COUNT(*) FILTER (WHERE f.raw_value = '1') /
         NULLIF(COUNT(*) FILTER (WHERE f.raw_value IN ('1','2')), 0), 2)
         AS percent_yes_among_valid
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND v.variable_code IN ('DIABETE4','CVDCRHD4','CVDSTRK3','HAVARTH4',
                          'ADDEPEV3','DIFFWALK','DIFFALON')
GROUP BY v.variable_code, v.variable_description
ORDER BY v.variable_code;

-- 6. DISTRIBUCION DEL INDICE DE MASA CORPORAL (BMI)
-- El ETL ya convirtió _BMI5 a escala convencional
SELECT COUNT(*) AS total_records, COUNT(f.numeric_value) AS valid_bmi_observations,
       ROUND(AVG(f.numeric_value), 2) AS mean_bmi,
       PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.numeric_value) AS median_bmi,
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.numeric_value) AS p25_bmi,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.numeric_value) AS p75_bmi,
       MIN(f.numeric_value) AS min_bmi, MAX(f.numeric_value) AS max_bmi
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND v.variable_code = '_BMI5';

-- 7. HABITOS: TABAQUISMO, ALCOHOL Y ACTIVIDAD FISICA.
-- Se muestran los códigos originales para evitar recodificaciones no documentadas en esta consulta
SELECT v.variable_code, v.variable_description, f.raw_value AS response_code,
       COUNT(*) AS respondents,
       ROUND(100.0 * COUNT(*) /
         NULLIF(SUM(COUNT(*)) OVER (PARTITION BY v.variable_code), 0), 2)
         AS percent_of_non_null_responses
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND v.variable_code IN ('_SMOKER3','DRNKANY6','_TOTINDA')
  AND NULLIF(BTRIM(f.raw_value), '') IS NOT NULL
GROUP BY v.variable_code, v.variable_description, f.raw_value
ORDER BY v.variable_code, respondents DESC, response_code;

-- 8. COMPARACION DESCRIPTIVA POR JURISDICCION.
-- Porcentaje de respuestas afirmativas entre respuestas válidas 1/2.
-- No es una estimación estatal oficial ni ponderada.
SELECT j.state_code, j.jurisdiction_name, v.variable_code,
       COUNT(*) FILTER (WHERE f.raw_value IN ('1','2')) AS valid_responses,
       COUNT(*) FILTER (WHERE f.raw_value = '1') AS yes_responses,
       ROUND(100.0 * COUNT(*) FILTER (WHERE f.raw_value = '1') /
         NULLIF(COUNT(*) FILTER (WHERE f.raw_value IN ('1','2')), 0), 2)
         AS percent_yes_in_sample
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
JOIN gold.dim_brfss_jurisdiction j ON j.jurisdiction_key = f.jurisdiction_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND v.variable_code IN ('DIABETE4','CVDCRHD4','CVDSTRK3')
GROUP BY j.state_code, j.jurisdiction_name, v.variable_code
ORDER BY v.variable_code, percent_yes_in_sample DESC NULLS LAST;

-- 9. COMPLETITUD POR VARIABLE.
SELECT v.variable_code, COUNT(*) AS total_observations,
       COUNT(*) FILTER (WHERE NULLIF(BTRIM(f.raw_value), '') IS NOT NULL)
         AS populated_observations,
       COUNT(*) FILTER (WHERE NULLIF(BTRIM(f.raw_value), '') IS NULL)
         AS missing_observations,
       ROUND(100.0 * COUNT(*) FILTER (
         WHERE NULLIF(BTRIM(f.raw_value), '') IS NULL) /
         NULLIF(COUNT(*), 0), 2) AS percent_missing
FROM gold.fact_brfss_response f
JOIN gold.dim_brfss_variable v ON v.variable_key = f.variable_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
GROUP BY v.variable_code
ORDER BY percent_missing DESC, v.variable_code;
