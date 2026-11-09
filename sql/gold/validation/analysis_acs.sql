-- GOLD ACS 2024 - ANALISIS DESCRIPTIVO (DP05)
-- En psql: \set source_run_id 13
--             \i sql/gold/analysis_acs.sql
--
-- CRITERIOS DE INTERPRETACION:
-- - 113 indicadores en dimensión; 108 hechos; 5 encabezados sin hecho.
-- - PERCENTAGE = porcentaje numérico.
-- - COUNT_VALUE = conteo numérico, NO porcentaje.
-- - NOT_AVAILABLE = marcador (X), no equivale a cero.
-- - NULL = valor vacío/no informado.

-- 1. CONTROL DE COBERTURA: indicadores, encabezados y hechos.
SELECT d.source_run_id,
       COUNT(*) AS indicators_in_dimension,
       COUNT(*) FILTER (WHERE d.is_section) AS section_headers,
       COUNT(*) FILTER (WHERE NOT d.is_section) AS indicators_with_estimate,
       COUNT(f.indicator_key) AS fact_rows
FROM gold.dim_acs_indicator d
LEFT JOIN gold.fact_acs_indicator f
  ON f.indicator_key = d.indicator_key AND f.source_run_id = d.source_run_id
WHERE d.source_run_id = :'source_run_id'::BIGINT
GROUP BY d.source_run_id;

-- 2. DISTRIBUCION DE TIPOS EN LA COLUMNA PERCENT.
SELECT percent_value_type, COUNT(*) AS indicators,
       COUNT(*) FILTER (WHERE percent_numeric IS NOT NULL) AS numeric_percent_values,
       COUNT(*) FILTER (WHERE percent_count_numeric IS NOT NULL) AS numeric_count_values
FROM gold.fact_acs_indicator
WHERE source_run_id = :'source_run_id'::BIGINT
GROUP BY percent_value_type
ORDER BY percent_value_type;

-- 3. ESTRUCTURA JERARQUICA DE INDICADORES.
-- La jerarquía se reconstruyó desde la indentación original.
SELECT d.ordinal_position, d.source_row_id, d.indentation_level, d.is_section,
       d.label_original, d.label_normalized,
       parent.label_normalized AS parent_label
FROM gold.dim_acs_indicator d
LEFT JOIN gold.dim_acs_indicator parent
  ON parent.indicator_key = d.parent_indicator_key
 AND parent.source_run_id = d.source_run_id
WHERE d.source_run_id = :'source_run_id'::BIGINT
ORDER BY d.ordinal_position;

-- 4. INDICADORES CON PORCENTAJES DISPONIBLES Y SUS MARGENES DE ERROR.
-- Solo PERCENTAGE; no mezcla conteos con porcentajes.
SELECT d.ordinal_position, d.label_normalized AS indicator,
       f.estimate_numeric AS estimate,
       f.margin_of_error_numeric AS estimate_margin_of_error,
       f.percent_numeric AS percentage,
       f.percent_margin_of_error_numeric AS percentage_margin_of_error
FROM gold.fact_acs_indicator f
JOIN gold.dim_acs_indicator d
  ON d.indicator_key = f.indicator_key AND d.source_run_id = f.source_run_id
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND f.percent_value_type = 'PERCENTAGE'
ORDER BY d.ordinal_position;

-- 5. COMPOSICION POR SEXO: Male y Female.
SELECT d.label_normalized AS indicator,
       f.estimate_numeric AS population_estimate,
       f.margin_of_error_numeric AS estimate_margin_of_error,
       f.percent_numeric AS percentage,
       f.percent_margin_of_error_numeric AS percentage_margin_of_error
FROM gold.fact_acs_indicator f
JOIN gold.dim_acs_indicator d
  ON d.indicator_key = f.indicator_key AND d.source_run_id = f.source_run_id
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND d.label_normalized IN ('Male','Female')
ORDER BY d.ordinal_position;

-- 6. DISTRIBUCION POR EDAD.
-- Se conservan las etiquetas y el orden originales de ACS.
SELECT d.ordinal_position, d.label_normalized AS age_indicator,
       f.estimate_numeric AS estimate,
       f.margin_of_error_numeric AS margin_of_error,
       f.percent_numeric AS percentage,
       f.percent_margin_of_error_numeric AS percent_margin_of_error,
       f.percent_value_type
FROM gold.fact_acs_indicator f
JOIN gold.dim_acs_indicator d
  ON d.indicator_key = f.indicator_key AND d.source_run_id = f.source_run_id
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND (d.label_normalized ILIKE '%years%'
       OR d.label_normalized ILIKE '%year%'
       OR d.label_normalized ILIKE '%median age%')
ORDER BY d.ordinal_position;

-- 7. COMPOSICION RACIAL Y ETNICA.
-- No sumar categorías: las definiciones/denominadores pueden diferir.
SELECT d.ordinal_position, d.label_normalized AS indicator,
       f.estimate_numeric AS estimate,
       f.margin_of_error_numeric AS margin_of_error,
       f.percent_numeric AS percentage,
       f.percent_margin_of_error_numeric AS percent_margin_of_error,
       f.percent_value_type
FROM gold.fact_acs_indicator f
JOIN gold.dim_acs_indicator d
  ON d.indicator_key = f.indicator_key AND d.source_run_id = f.source_run_id
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND (d.label_normalized ILIKE '%White%'
       OR d.label_normalized ILIKE '%Black%'
       OR d.label_normalized ILIKE '%American Indian%'
       OR d.label_normalized ILIKE '%Asian%'
       OR d.label_normalized ILIKE '%Native Hawaiian%'
       OR d.label_normalized ILIKE '%Some other race%'
       OR d.label_normalized ILIKE '%Two or more races%'
       OR d.label_normalized ILIKE '%Hispanic%'
       OR d.label_normalized ILIKE '%Latino%')
ORDER BY d.ordinal_position;

-- 8. HOGARES Y VIVIENDA.
SELECT d.ordinal_position, d.label_normalized AS indicator,
       f.estimate_numeric AS estimate,
       f.margin_of_error_numeric AS margin_of_error,
       f.percent_numeric AS percentage,
       f.percent_count_numeric AS percent_column_count_value,
       f.percent_value_type
FROM gold.fact_acs_indicator f
JOIN gold.dim_acs_indicator d
  ON d.indicator_key = f.indicator_key AND d.source_run_id = f.source_run_id
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND (d.label_normalized ILIKE '%household%'
       OR d.label_normalized ILIKE '%housing unit%'
       OR d.label_normalized ILIKE '%occupied%'
       OR d.label_normalized ILIKE '%vacant%'
       OR d.label_normalized ILIKE '%persons per household%')
ORDER BY d.ordinal_position;

-- 9. MARGEN DE ERROR RELATIVO PARA PORCENTAJES.
-- Razón descriptiva MOE/estimación; no reemplaza intervalos de confianza
-- ni un análisis completo del diseño muestral ACS.
SELECT d.ordinal_position, d.label_normalized AS indicator,
       f.percent_numeric AS percentage,
       f.percent_margin_of_error_numeric AS percent_margin_of_error,
       ROUND(100.0 * f.percent_margin_of_error_numeric /
         NULLIF(ABS(f.percent_numeric), 0), 2) AS relative_percent_moe
FROM gold.fact_acs_indicator f
JOIN gold.dim_acs_indicator d
  ON d.indicator_key = f.indicator_key AND d.source_run_id = f.source_run_id
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND f.percent_value_type = 'PERCENTAGE'
  AND f.percent_numeric IS NOT NULL
  AND f.percent_margin_of_error_numeric IS NOT NULL
  AND f.percent_numeric <> 0
ORDER BY relative_percent_moe DESC, d.ordinal_position;

-- 10. REGISTROS CUYO CAMPO PERCENT NO ES UN PORCENTAJE.
-- Permite revisar COUNT_VALUE, NOT_AVAILABLE y NULL por indicador.
SELECT d.ordinal_position, d.label_normalized AS indicator,
       f.estimate_raw, f.percent_raw, f.percent_count_numeric,
       f.percent_value_type, f.percent_margin_of_error_raw
FROM gold.fact_acs_indicator f
JOIN gold.dim_acs_indicator d
  ON d.indicator_key = f.indicator_key AND d.source_run_id = f.source_run_id
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND f.percent_value_type <> 'PERCENTAGE'
ORDER BY d.ordinal_position;