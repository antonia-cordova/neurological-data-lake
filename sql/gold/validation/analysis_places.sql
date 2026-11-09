-- ============================================================
-- ANALISIS 1
-- RANKING TERRITORIAL DE DIABETES
--
-- Pregunta:
-- ¿Qué geografías presentan mayor prevalencia de diabetes?
--
-- Indicador: DIABETES
-- Tipo: Age-adjusted prevalence
-- ============================================================

SELECT
    g.state_abbr,
    g.location_id,
    g.location_name,
    m.measure_id,
    f.data_value AS diabetes_prevalence
FROM gold.fact_area_health f
JOIN gold.dim_geography g
    ON f.geography_key = g.geography_key
JOIN gold.dim_health_measure m
    ON f.health_measure_key = m.health_measure_key
JOIN gold.dim_prevalence_type p
    ON f.prevalence_type_key = p.prevalence_type_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
  AND m.measure_id = 'DIABETES'
  AND p.data_value_type = 'Age-adjusted prevalence'
ORDER BY f.data_value DESC, g.location_name
LIMIT 15;


-- ============================================================
-- ANALISIS 2
-- SALUD CARDIOVASCULAR
--
-- Pregunta:
-- ¿Qué geografías presentan mayor prevalencia de enfermedad
-- coronaria y accidente cerebrovascular?
--
-- Indicadores: CHD y STROKE
-- Tipo: Age-adjusted prevalence
-- Ranking independiente por indicador.
-- ============================================================

WITH ranked AS (
    SELECT
        g.state_abbr,
        g.location_id,
        g.location_name,
        m.measure_id,
        m.measure,
        f.data_value,
        ROW_NUMBER() OVER (
            PARTITION BY m.measure_id
            ORDER BY f.data_value DESC, g.location_name
        ) AS ranking
    FROM gold.fact_area_health f
    JOIN gold.dim_geography g
        ON f.geography_key = g.geography_key
    JOIN gold.dim_health_measure m
        ON f.health_measure_key = m.health_measure_key
    JOIN gold.dim_prevalence_type p
        ON f.prevalence_type_key = p.prevalence_type_key
    WHERE f.source_run_id = :'source_run_id'::BIGINT
      AND m.measure_id IN ('CHD', 'STROKE')
      AND p.data_value_type = 'Age-adjusted prevalence'
)
SELECT
    measure_id,
    measure,
    ranking,
    state_abbr,
    location_id,
    location_name,
    data_value AS prevalence
FROM ranked
WHERE ranking <= 10
ORDER BY measure_id, ranking;


-- ============================================================
-- ANALISIS 3
-- SALUD MENTAL
--
-- Pregunta:
-- ¿Qué territorios presentan mayores prevalencias de depresión
-- y malestar mental frecuente?
--
-- Indicadores: DEPRESSION y MHLTH
-- Tipo: Age-adjusted prevalence
-- ============================================================

WITH ranked AS (
    SELECT
        g.state_abbr,
        g.location_id,
        g.location_name,
        m.measure_id,
        m.measure,
        f.data_value,
        ROW_NUMBER() OVER (
            PARTITION BY m.measure_id
            ORDER BY f.data_value DESC, g.location_name
        ) AS ranking
    FROM gold.fact_area_health f
    JOIN gold.dim_geography g
        ON f.geography_key = g.geography_key
    JOIN gold.dim_health_measure m
        ON f.health_measure_key = m.health_measure_key
    JOIN gold.dim_prevalence_type p
        ON f.prevalence_type_key = p.prevalence_type_key
    WHERE f.source_run_id = :'source_run_id'::BIGINT
      AND m.measure_id IN ('DEPRESSION', 'MHLTH')
      AND p.data_value_type = 'Age-adjusted prevalence'
)
SELECT
    measure_id,
    measure,
    ranking,
    state_abbr,
    location_id,
    location_name,
    data_value AS prevalence
FROM ranked
WHERE ranking <= 10
ORDER BY measure_id, ranking;


-- ============================================================
-- ANALISIS 4
-- DISCAPACIDAD Y LIMITACIONES FUNCIONALES
--
-- Pregunta:
-- ¿Qué geografías presentan mayores prevalencias de
-- discapacidad y limitaciones funcionales?
--
-- Indicadores:
-- DISABILITY, MOBILITY, SELFCARE, INDEPLIVE, HEARING,
-- VISION y COGNITION.
--
-- Tipo: Age-adjusted prevalence
-- Ranking independiente por indicador.
-- ============================================================

WITH ranked AS (
    SELECT
        g.state_abbr,
        g.location_id,
        g.location_name,
        m.measure_id,
        m.measure,
        f.data_value,
        ROW_NUMBER() OVER (
            PARTITION BY m.measure_id
            ORDER BY f.data_value DESC, g.location_name
        ) AS ranking
    FROM gold.fact_area_health f
    JOIN gold.dim_geography g
        ON f.geography_key = g.geography_key
    JOIN gold.dim_health_measure m
        ON f.health_measure_key = m.health_measure_key
    JOIN gold.dim_prevalence_type p
        ON f.prevalence_type_key = p.prevalence_type_key
    WHERE f.source_run_id = :'source_run_id'::BIGINT
      AND m.measure_id IN (
          'DISABILITY',
          'MOBILITY',
          'SELFCARE',
          'INDEPLIVE',
          'HEARING',
          'VISION',
          'COGNITION'
      )
      AND p.data_value_type = 'Age-adjusted prevalence'
)
SELECT
    measure_id,
    measure,
    ranking,
    state_abbr,
    location_id,
    location_name,
    data_value AS prevalence
FROM ranked
WHERE ranking <= 10
ORDER BY measure_id, ranking;


-- ============================================================
-- ANALISIS 5
-- COMPARACION DE PREVALENCIA CRUDA Y AJUSTADA
--
-- Pregunta:
-- ¿Cómo cambian los indicadores al comparar prevalencias
-- crudas y ajustadas por edad?
--
-- Diferencia:
-- Crude prevalence - Age-adjusted prevalence
--
-- Una diferencia positiva indica que la prevalencia cruda
-- es superior a la ajustada.
-- ============================================================

SELECT
    g.state_abbr,
    g.location_id,
    g.location_name,
    m.measure_id,
    m.measure,
    MAX(
        CASE
            WHEN p.data_value_type = 'Crude prevalence'
            THEN f.data_value
        END
    ) AS crude_prevalence,
    MAX(
        CASE
            WHEN p.data_value_type = 'Age-adjusted prevalence'
            THEN f.data_value
        END
    ) AS age_adjusted_prevalence,
    ROUND(
        MAX(
            CASE
                WHEN p.data_value_type = 'Crude prevalence'
                THEN f.data_value
            END
        )
        -
        MAX(
            CASE
                WHEN p.data_value_type = 'Age-adjusted prevalence'
                THEN f.data_value
            END
        ),
        2
    ) AS difference_crude_adjusted
FROM gold.fact_area_health f
JOIN gold.dim_geography g
    ON f.geography_key = g.geography_key
JOIN gold.dim_health_measure m
    ON f.health_measure_key = m.health_measure_key
JOIN gold.dim_prevalence_type p
    ON f.prevalence_type_key = p.prevalence_type_key
WHERE f.source_run_id = :'source_run_id'::BIGINT
GROUP BY
    g.state_abbr,
    g.location_id,
    g.location_name,
    m.measure_id,
    m.measure
ORDER BY
    ABS(
        MAX(
            CASE
                WHEN p.data_value_type = 'Crude prevalence'
                THEN f.data_value
            END
        )
        -
        MAX(
            CASE
                WHEN p.data_value_type = 'Age-adjusted prevalence'
                THEN f.data_value
            END
        )
    ) DESC;


-- ============================================================
-- ANALISIS 6
-- PRIORIZACION EXPLORATORIA DE GEOGRAFIAS
--
-- Pregunta:
-- ¿Qué geografías presentan valores elevados en varios
-- indicadores de salud simultaneamente?
--
-- Metodologia:
-- 1. Utiliza prevalencias ajustadas por edad.
-- 2. Calcula el percentil 75 de cada indicador entre las
--    83 geografias.
-- 3. Cuenta cuantos indicadores superan su propio P75.
-- 4. Ordena las geografias por cantidad de indicadores elevados.
--
-- Indicadores:
-- DIABETES, CHD, STROKE, GHLTH, MHLTH, PHLTH y DISABILITY.
-- ============================================================

WITH indicator_values AS (
    SELECT
        g.state_abbr,
        g.location_id,
        g.location_name,
        m.measure_id,
        f.data_value
    FROM gold.fact_area_health f
    JOIN gold.dim_geography g
        ON f.geography_key = g.geography_key
    JOIN gold.dim_health_measure m
        ON f.health_measure_key = m.health_measure_key
    JOIN gold.dim_prevalence_type p
        ON f.prevalence_type_key = p.prevalence_type_key
    WHERE f.source_run_id = :'source_run_id'::BIGINT
      AND m.measure_id IN (
          'DIABETES',
          'CHD',
          'STROKE',
          'GHLTH',
          'MHLTH',
          'PHLTH',
          'DISABILITY'
      )
      AND p.data_value_type = 'Age-adjusted prevalence'
      AND f.data_value IS NOT NULL
),
indicator_thresholds AS (
    SELECT
        measure_id,
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY data_value) AS percentile_75
    FROM indicator_values
    GROUP BY measure_id
),
classified AS (
    SELECT
        v.state_abbr,
        v.location_id,
        v.location_name,
        v.measure_id,
        v.data_value,
        t.percentile_75,
        CASE
            WHEN v.data_value > t.percentile_75 THEN 1
            ELSE 0
        END AS elevated_indicator
    FROM indicator_values v
    JOIN indicator_thresholds t
        ON v.measure_id = t.measure_id
)
SELECT
    state_abbr,
    location_id,
    location_name,
    COUNT(*) AS indicators_evaluated,
    SUM(elevated_indicator) AS elevated_indicators,
    ROUND(
        100.0 * SUM(elevated_indicator) / COUNT(*),
        2
    ) AS percentage_of_elevated_indicators
FROM classified
GROUP BY
    state_abbr,
    location_id,
    location_name
ORDER BY
    elevated_indicators DESC,
    percentage_of_elevated_indicators DESC,
    location_name
LIMIT 20;

-- ============================================================
-- 7. MULTIDIMENSIONAL TERRITORIAL HEALTH PROFILE
-- ============================================================
-- Objective:
-- Compare seven age-adjusted health indicators across the
-- eight geographies identified above the 75th percentile
-- for all seven selected indicators.
--
-- Source: PLACES
-- Expected result: 56 rows (8 geographies x 7 indicators)
-- ============================================================

WITH selected_geographies AS (
    SELECT
        g.geography_key,
        g.state_abbr,
        g.location_id,
        g.location_name
    FROM gold.dim_geography g
    WHERE (g.state_abbr, g.location_id) IN (
        ('VA', '51027'),
        ('FL', '12013'),
        ('VA', '51089'),
        ('MS', '28053'),
        ('GA', '13193'),
        ('AR', '05099'),
        ('LA', '22099'),
        ('AL', '01133')
    )
)

SELECT
    g.state_abbr,
    g.location_name,
    m.measure_id,
    f.data_value AS age_adjusted_prevalence

FROM selected_geographies g

JOIN gold.fact_area_health f
    ON f.geography_key = g.geography_key

JOIN gold.dim_health_measure m
    ON f.health_measure_key = m.health_measure_key

JOIN gold.dim_prevalence_type p
    ON f.prevalence_type_key = p.prevalence_type_key

WHERE f.source_run_id = :'source_run_id'::BIGINT  AND m.measure_id IN (
      'DIABETES',
      'CHD',
      'STROKE',
      'GHLTH',
      'MHLTH',
      'PHLTH',
      'DISABILITY'
  )
  AND p.data_value_type = 'Age-adjusted prevalence'

ORDER BY
    g.state_abbr,
    g.location_name,
    m.measure_id;