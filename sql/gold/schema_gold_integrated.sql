-- ============================================================
-- GOLD INTEGRATED
-- Esquema definitivo para instalaciones nuevas.
-- No contiene instrucciones ALTER ni depende de migraciones.
-- Las correspondencias del bridge se cargan desde el ETL integrado,
-- una vez pobladas las dimensiones PLACES y BRFSS.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ------------------------------------------------------------
-- 1. DIMENSION: INDICADORES INTEGRADOS (15 indicadores)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_health_integrated_indicator (
    indicator_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    indicator_code TEXT NOT NULL UNIQUE,
    indicator_name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO gold.dim_health_integrated_indicator (
    indicator_code,
    indicator_name,
    category,
    unit,
    description
)
VALUES
    ('ARTHRITIS', 'Artritis', 'CHRONIC_CONDITIONS', 'PERCENTAGE',
     'Indicadores relacionados con artritis en población adulta.'),
    ('CORONARY_HEART_DISEASE', 'Enfermedad coronaria', 'CARDIOVASCULAR', 'PERCENTAGE',
     'Indicadores relacionados con enfermedad coronaria.'),
    ('DEPRESSION', 'Depresión', 'MENTAL_HEALTH', 'PERCENTAGE_OR_DAYS',
     'Indicadores relacionados con diagnóstico o salud mental.'),
    ('DIABETES', 'Diabetes', 'CHRONIC_CONDITIONS', 'PERCENTAGE',
     'Indicadores relacionados con diabetes.'),
    ('GENERAL_HEALTH', 'Salud general percibida', 'GENERAL_HEALTH', 'PERCENTAGE_OR_CATEGORY',
     'Indicadores de autopercepción del estado general de salud.'),
    ('MENTAL_HEALTH', 'Salud mental', 'MENTAL_HEALTH', 'PERCENTAGE_OR_DAYS',
     'Indicadores relacionados con días de mala salud mental.'),
    ('PHYSICAL_HEALTH', 'Salud física', 'PHYSICAL_HEALTH', 'PERCENTAGE_OR_DAYS',
     'Indicadores relacionados con días de mala salud física.'),
    ('STROKE', 'Accidente cerebrovascular', 'CARDIOVASCULAR', 'PERCENTAGE',
     'Indicadores relacionados con antecedentes de accidente cerebrovascular.'),
    ('MOBILITY', 'Movilidad', 'FUNCTIONAL_DISABILITY', 'PERCENTAGE',
     'Indicadores relacionados con limitaciones de movilidad.'),
    ('DISABILITY', 'Discapacidad', 'FUNCTIONAL_DISABILITY', 'PERCENTAGE',
     'Indicadores relacionados con limitaciones funcionales.'),
    ('COGNITION', 'Discapacidad cognitiva', 'FUNCTIONAL_DISABILITY', 'PERCENTAGE',
     'Discapacidad cognitiva entre adultos.'),
    ('HEARING', 'Discapacidad auditiva', 'FUNCTIONAL_DISABILITY', 'PERCENTAGE',
     'Discapacidad auditiva entre adultos.'),
    ('INDEPLIVE', 'Discapacidad para la vida independiente', 'FUNCTIONAL_DISABILITY', 'PERCENTAGE',
     'Dificultad para vivir independientemente entre adultos.'),
    ('SELFCARE', 'Discapacidad para el autocuidado', 'FUNCTIONAL_DISABILITY', 'PERCENTAGE',
     'Dificultad para el autocuidado entre adultos.'),
    ('VISION', 'Discapacidad visual', 'FUNCTIONAL_DISABILITY', 'PERCENTAGE',
     'Discapacidad visual entre adultos.')
ON CONFLICT (indicator_code) DO UPDATE SET
    indicator_name = EXCLUDED.indicator_name,
    category = EXCLUDED.category,
    unit = EXCLUDED.unit,
    description = EXCLUDED.description;

-- ------------------------------------------------------------
-- 2. BRIDGE: INDICADOR Y VARIABLE/MEDIDA DE ORIGEN
-- Las filas se insertan en etl_gold_integrated.py después de
-- ejecutar los ETL Gold base, pues dependen de sus dimensiones.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.bridge_health_indicator_source (
    mapping_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    indicator_key BIGINT NOT NULL,
    source_key BIGINT NOT NULL,
    health_measure_key BIGINT,
    variable_key BIGINT,
    relation_type TEXT NOT NULL,
    comparability TEXT NOT NULL,
    mapping_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT bridge_integrated_indicator_fk
        FOREIGN KEY (indicator_key)
        REFERENCES gold.dim_health_integrated_indicator(indicator_key),
    CONSTRAINT bridge_integrated_source_fk
        FOREIGN KEY (source_key)
        REFERENCES gold.dim_source(source_key),
    CONSTRAINT bridge_integrated_places_measure_fk
        FOREIGN KEY (health_measure_key)
        REFERENCES gold.dim_health_measure(health_measure_key),
    CONSTRAINT bridge_integrated_brfss_variable_fk
        FOREIGN KEY (variable_key)
        REFERENCES gold.dim_brfss_variable(variable_key),
    CONSTRAINT bridge_integrated_origin_check
        CHECK (
            (health_measure_key IS NOT NULL AND variable_key IS NULL)
            OR
            (health_measure_key IS NULL AND variable_key IS NOT NULL)
        ),
    CONSTRAINT bridge_integrated_relation_check
        CHECK (relation_type IN ('DIRECT', 'CONCEPTUAL', 'PARTIAL')),
    CONSTRAINT bridge_integrated_comparability_check
        CHECK (
            comparability IN (
                'COMPARABLE_WITH_CAVEATS',
                'NOT_DIRECTLY_COMPARABLE',
                'CONTEXTUAL_ONLY'
            )
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_integrated_places_mapping
    ON gold.bridge_health_indicator_source (
        indicator_key, source_key, health_measure_key
    )
    WHERE health_measure_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_integrated_brfss_mapping
    ON gold.bridge_health_indicator_source (
        indicator_key, source_key, variable_key
    )
    WHERE variable_key IS NOT NULL;

-- ------------------------------------------------------------
-- 3. FACT: OBSERVACIONES SANITARIAS INTEGRADAS
-- Estructura final equivalente al resultado consolidado de V004
-- y V005, creada directamente desde cero.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_health_integrated (
    integrated_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    indicator_key BIGINT NOT NULL,
    geography_key BIGINT NOT NULL,
    source_key BIGINT NOT NULL,
    source_run_id BIGINT NOT NULL,
    reference_year SMALLINT,
    geography_level TEXT NOT NULL,
    health_measure_key BIGINT,
    variable_key BIGINT,
    value_numeric NUMERIC,
    value_raw TEXT,
    unit TEXT NOT NULL,
    prevalence_type_key BIGINT,
    sample_size INTEGER,
    numerator INTEGER,
    denominator INTEGER,
    confidence_interval_lower NUMERIC,
    confidence_interval_upper NUMERIC,
    methodology TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT integrated_indicator_fk
        FOREIGN KEY (indicator_key)
        REFERENCES gold.dim_health_integrated_indicator(indicator_key),
    CONSTRAINT integrated_geography_fk
        FOREIGN KEY (geography_key)
        REFERENCES gold.dim_geography(geography_key),
    CONSTRAINT integrated_source_fk
        FOREIGN KEY (source_key)
        REFERENCES gold.dim_source(source_key),
    CONSTRAINT integrated_run_fk
        FOREIGN KEY (source_run_id)
        REFERENCES public.etl_runs(run_id),
    CONSTRAINT integrated_measure_fk
        FOREIGN KEY (health_measure_key)
        REFERENCES gold.dim_health_measure(health_measure_key),
    CONSTRAINT integrated_variable_fk
        FOREIGN KEY (variable_key)
        REFERENCES gold.dim_brfss_variable(variable_key),
    CONSTRAINT integrated_prevalence_fk
        FOREIGN KEY (prevalence_type_key)
        REFERENCES gold.dim_prevalence_type(prevalence_type_key),
    CONSTRAINT integrated_origin_check
        CHECK (
            (health_measure_key IS NOT NULL AND variable_key IS NULL)
            OR
            (health_measure_key IS NULL AND variable_key IS NOT NULL)
        ),
    CONSTRAINT integrated_geography_level_check
        CHECK (geography_level IN ('COUNTY', 'STATE')),
    CONSTRAINT integrated_reference_year_check
        CHECK (reference_year IS NULL OR reference_year BETWEEN 1900 AND 2100),
    CONSTRAINT integrated_sample_size_check
        CHECK (sample_size IS NULL OR sample_size >= 0),
    CONSTRAINT integrated_confidence_interval_check
        CHECK (
            confidence_interval_lower IS NULL
            OR confidence_interval_upper IS NULL
            OR confidence_interval_lower <= confidence_interval_upper
        ),
    CONSTRAINT chk_integrated_numerator_nonnegative
        CHECK (numerator IS NULL OR numerator >= 0),
    CONSTRAINT chk_integrated_denominator_positive
        CHECK (denominator IS NULL OR denominator > 0),
    CONSTRAINT chk_integrated_numerator_denominator
        CHECK (
            numerator IS NULL
            OR denominator IS NULL
            OR numerator <= denominator
        )
);

CREATE INDEX IF NOT EXISTS idx_integrated_fact_indicator
    ON gold.fact_health_integrated(indicator_key);
CREATE INDEX IF NOT EXISTS idx_integrated_fact_geography
    ON gold.fact_health_integrated(geography_key);
CREATE INDEX IF NOT EXISTS idx_integrated_fact_source
    ON gold.fact_health_integrated(source_key);
CREATE INDEX IF NOT EXISTS idx_integrated_fact_run
    ON gold.fact_health_integrated(source_run_id);
CREATE INDEX IF NOT EXISTS idx_integrated_fact_year
    ON gold.fact_health_integrated(reference_year);

CREATE UNIQUE INDEX IF NOT EXISTS uq_integrated_fact_places
    ON gold.fact_health_integrated (
        source_run_id, geography_key, indicator_key,
        health_measure_key, prevalence_type_key
    )
    WHERE health_measure_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_integrated_fact_brfss
    ON gold.fact_health_integrated (
        source_run_id, geography_key, indicator_key, variable_key
    )
    WHERE variable_key IS NOT NULL;

COMMENT ON TABLE gold.dim_health_integrated_indicator IS
    'Catálogo común de indicadores sanitarios integrados desde fuentes heterogéneas.';
COMMENT ON TABLE gold.bridge_health_indicator_source IS
    'Correspondencia entre indicadores integrados y medidas PLACES o variables BRFSS.';
COMMENT ON TABLE gold.fact_health_integrated IS
    'Hechos sanitarios integrados con procedencia, geografía y ejecución Silver trazables.';
COMMENT ON COLUMN gold.fact_health_integrated.numerator IS
    'Numerador descriptivo de la agregación BRFSS cuando corresponda.';
COMMENT ON COLUMN gold.fact_health_integrated.denominator IS
    'Denominador descriptivo de la agregación BRFSS cuando corresponda.';
