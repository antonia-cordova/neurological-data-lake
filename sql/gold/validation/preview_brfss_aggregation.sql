
WITH parsed AS (
    SELECT
        f.source_run_id,
        f.jurisdiction_key,
        g.geography_key,
        g.state_abbr,
        g.location_name AS state_name,
        v.variable_code,
        b.indicator_key,
        i.indicator_code,
        CASE
            WHEN BTRIM(f.raw_value) ~ '^[0-9]+(\.[0-9]+)?$'
            THEN BTRIM(f.raw_value)::numeric
            ELSE NULL
        END AS response_value
    FROM gold.fact_brfss_response f
    JOIN gold.dim_brfss_variable v
        ON v.variable_key = f.variable_key
    JOIN gold.bridge_health_indicator_source b
        ON b.variable_key = v.variable_key
       AND b.source_key = f.source_key
    JOIN gold.dim_health_integrated_indicator i
        ON i.indicator_key = b.indicator_key
    JOIN gold.bridge_brfss_geography bg
        ON bg.jurisdiction_key = f.jurisdiction_key
    JOIN gold.dim_geography g
        ON g.geography_key = bg.geography_key
    WHERE f.source_run_id = 17
      AND f.source_key = (
          SELECT source_key
          FROM gold.dim_source
          WHERE source_code = 'BRFSS'
      )
      AND g.geography_level = 'STATE'
),
classified AS (
    SELECT
        *,
        CASE
            WHEN variable_code IN (
                'HAVARTH4', 'CVDCRHD4', 'ADDEPEV3',
                'CVDSTRK3', 'DIFFWALK', 'DIFFALON'
            )
                THEN response_value IN (1, 2)

            WHEN variable_code = 'DIABETE4'
                THEN response_value IN (1, 3)

            WHEN variable_code = 'GENHLTH'
                THEN response_value IN (1, 2, 3, 4, 5)

            WHEN variable_code IN ('MENTHLTH', 'PHYSHLTH')
                THEN response_value BETWEEN 1 AND 30
                     OR response_value = 88

            ELSE FALSE
        END AS is_valid,

        CASE
            WHEN variable_code IN (
                'HAVARTH4', 'CVDCRHD4', 'ADDEPEV3',
                'CVDSTRK3', 'DIFFWALK', 'DIFFALON'
            )
                THEN response_value = 1

            WHEN variable_code = 'DIABETE4'
                THEN response_value = 1

            WHEN variable_code = 'GENHLTH'
                THEN response_value IN (4, 5)

            WHEN variable_code IN ('MENTHLTH', 'PHYSHLTH')
                THEN response_value BETWEEN 14 AND 30

            ELSE FALSE
        END AS is_positive
    FROM parsed
)
SELECT
    indicator_code,
    variable_code,
    state_abbr,
    state_name,
    COUNT(*) FILTER (WHERE is_positive) AS numerator,
    COUNT(*) FILTER (WHERE is_valid) AS denominator,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_positive)
        / NULLIF(COUNT(*) FILTER (WHERE is_valid), 0),
        2
    ) AS sample_percentage
FROM classified
GROUP BY
    indicator_code,
    variable_code,
    state_abbr,
    state_name
ORDER BY
    indicator_code,
    state_abbr,
    variable_code;
