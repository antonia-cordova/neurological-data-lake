BEGIN;

-- ============================================================
-- GOLD ACS 2024
-- ACS nacional y ACS County.
-- Conserva jerarquías, valores originales y valores numéricos.
-- ============================================================

-- ============================================================
-- 1. ACS NACIONAL: DIMENSIÓN DE INDICADORES
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_acs_indicator (
    indicator_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    source_run_id BIGINT NOT NULL,
    source_row_id BIGINT NOT NULL,

    label_original TEXT NOT NULL,
    label_normalized TEXT NOT NULL,

    indentation_level INTEGER NOT NULL DEFAULT 0,
    ordinal_position INTEGER NOT NULL,

    is_section BOOLEAN NOT NULL DEFAULT FALSE,
    parent_indicator_key BIGINT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT dim_acs_indicator_run_fk
        FOREIGN KEY (source_run_id)
        REFERENCES public.etl_runs(run_id)
        ON DELETE RESTRICT,

    CONSTRAINT dim_acs_indicator_parent_fk
        FOREIGN KEY (parent_indicator_key)
        REFERENCES gold.dim_acs_indicator(indicator_key)
        ON DELETE RESTRICT,

    CONSTRAINT dim_acs_indicator_source_row_unique
        UNIQUE (source_run_id, source_row_id),

    CONSTRAINT dim_acs_indicator_ordinal_unique
        UNIQUE (source_run_id, ordinal_position),

    CONSTRAINT dim_acs_indicator_indent_check
        CHECK (indentation_level >= 0),

    CONSTRAINT dim_acs_indicator_ordinal_check
        CHECK (ordinal_position >= 0)
);

-- ============================================================
-- 2. ACS NACIONAL: TABLA DE HECHOS
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_acs_indicator (
    source_run_id BIGINT NOT NULL,
    source_key BIGINT NOT NULL,
    indicator_key BIGINT NOT NULL,

    estimate_raw TEXT,
    estimate_numeric NUMERIC,

    margin_of_error_raw TEXT,
    margin_of_error_numeric NUMERIC,

    percent_raw TEXT,
    percent_numeric NUMERIC,
    percent_count_numeric NUMERIC,
    percent_value_type TEXT NOT NULL,

    percent_margin_of_error_raw TEXT,
    percent_margin_of_error_numeric NUMERIC,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fact_acs_indicator_pk
        PRIMARY KEY (source_run_id, indicator_key),

    CONSTRAINT fact_acs_indicator_run_fk
        FOREIGN KEY (source_run_id)
        REFERENCES public.etl_runs(run_id)
        ON DELETE RESTRICT,

    CONSTRAINT fact_acs_indicator_source_fk
        FOREIGN KEY (source_key)
        REFERENCES gold.dim_source(source_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_acs_indicator_dimension_fk
        FOREIGN KEY (indicator_key)
        REFERENCES gold.dim_acs_indicator(indicator_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_acs_indicator_percent_type_check
        CHECK (
            percent_value_type IN (
                'PERCENTAGE',
                'COUNT_VALUE',
                'NOT_AVAILABLE',
                'NULL'
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_dim_acs_indicator_parent
    ON gold.dim_acs_indicator(parent_indicator_key);

CREATE INDEX IF NOT EXISTS idx_dim_acs_indicator_run
    ON gold.dim_acs_indicator(source_run_id);

CREATE INDEX IF NOT EXISTS idx_fact_acs_indicator_indicator
    ON gold.fact_acs_indicator(indicator_key);

CREATE INDEX IF NOT EXISTS idx_fact_acs_indicator_source_run
    ON gold.fact_acs_indicator(source_run_id);

-- ============================================================
-- 3. ACS COUNTY: DIMENSIÓN DE INDICADORES DP05
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_acs_county_indicator (
    indicator_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    variable_code TEXT NOT NULL UNIQUE,
    indicator_label TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT dim_acs_county_indicator_code_check
        CHECK (variable_code ~ '^DP05_[0-9]{4}$')
);

-- Catálogo de indicadores utilizados por ACS County DP05.
INSERT INTO gold.dim_acs_county_indicator (
    variable_code,
    indicator_label
)
VALUES
    ('DP05_0001', 'Total population'),
    ('DP05_0002', 'Male population'),
    ('DP05_0003', 'Female population'),
    ('DP05_0018', 'Median age')
ON CONFLICT (variable_code) DO UPDATE
SET indicator_label = EXCLUDED.indicator_label;

-- ============================================================
-- 4. ACS COUNTY: TABLA DE HECHOS
--
-- Grano: una observación por ejecución, geografía e indicador.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_acs_county_indicator (
    source_run_id BIGINT NOT NULL,
    source_key BIGINT NOT NULL,
    geography_key BIGINT NOT NULL,
    indicator_key BIGINT NOT NULL,

    estimate_raw TEXT,
    estimate_numeric NUMERIC,

    margin_of_error_raw TEXT,
    margin_of_error_numeric NUMERIC,

    percent_raw TEXT,
    percent_numeric NUMERIC,

    percent_margin_of_error_raw TEXT,
    percent_margin_of_error_numeric NUMERIC,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fact_acs_county_indicator_pk
        PRIMARY KEY (
            source_run_id,
            geography_key,
            indicator_key
        ),

    CONSTRAINT fact_acs_county_indicator_run_fk
        FOREIGN KEY (source_run_id)
        REFERENCES public.etl_runs(run_id)
        ON DELETE RESTRICT,

    CONSTRAINT fact_acs_county_indicator_source_fk
        FOREIGN KEY (source_key)
        REFERENCES gold.dim_source(source_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_acs_county_indicator_geography_fk
        FOREIGN KEY (geography_key)
        REFERENCES gold.dim_geography(geography_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_acs_county_indicator_dimension_fk
        FOREIGN KEY (indicator_key)
        REFERENCES gold.dim_acs_county_indicator(indicator_key)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_fact_acs_county_geography
    ON gold.fact_acs_county_indicator(geography_key);

CREATE INDEX IF NOT EXISTS idx_fact_acs_county_indicator
    ON gold.fact_acs_county_indicator(indicator_key);

CREATE INDEX IF NOT EXISTS idx_fact_acs_county_source_run
    ON gold.fact_acs_county_indicator(source_run_id);

-- ============================================================
-- 5. REGISTRO DE FUENTES
-- ============================================================

INSERT INTO gold.dim_source (
    source_code,
    dataset_name,
    description
)
VALUES
(
    'ACS',
    'acs_2024',
    'American Community Survey 2024, Data Profile DP05.'
),
(
    'ACS_COUNTY',
    'acs_2024_county_dp05',
    'American Community Survey 2024, Data Profile DP05 at county level.'
)
ON CONFLICT (source_code) DO NOTHING;

-- ============================================================
-- 6. DOCUMENTACIÓN
-- ============================================================

COMMENT ON TABLE gold.dim_acs_indicator IS
    'Catálogo de indicadores ACS nacional con jerarquía y orden original.';

COMMENT ON TABLE gold.fact_acs_indicator IS
    'Hechos estadísticos ACS nacional. Conserva valores originales y valores numéricos interpretados.';

COMMENT ON COLUMN gold.fact_acs_indicator.percent_value_type IS
    'Clasificación del campo percent: porcentaje, conteo, no disponible o nulo.';

COMMENT ON TABLE gold.dim_acs_county_indicator IS
    'Catálogo de indicadores DP05 utilizados para ACS County 2024.';

COMMENT ON TABLE gold.fact_acs_county_indicator IS
    'Hechos estadísticos ACS County por ejecución, geografía e indicador. Conserva estimaciones, márgenes de error y porcentajes.';

COMMENT ON COLUMN gold.fact_acs_county_indicator.geography_key IS
    'Referencia a la geografía COUNTY de gold.dim_geography.';

COMMIT;