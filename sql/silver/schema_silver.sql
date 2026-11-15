CREATE TABLE IF NOT EXISTS public.etl_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    target_table TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'succeeded', 'failed', 'partial')),
    source_rows BIGINT CHECK (source_rows IS NULL OR source_rows >= 0),
    loaded_rows BIGINT NOT NULL DEFAULT 0 CHECK (loaded_rows >= 0),
    error_message TEXT,
    CONSTRAINT etl_runs_finished_after_started
        CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_dataset_started
    ON public.etl_runs (dataset_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_runs_status
    ON public.etl_runs (status);

-- ============================================================
-- PLACES 2023
-- 11 physical columns
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_places_2023 (
    state_abbr TEXT,
    location_name TEXT,
    location_id TEXT NOT NULL,
    measure TEXT,
    measure_id TEXT NOT NULL,
    data_value_type TEXT NOT NULL,
    data_value NUMERIC,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    reference_year SMALLINT NOT NULL,
    run_id BIGINT NOT NULL
        REFERENCES public.etl_runs(run_id),

    CONSTRAINT silver_places_2023_reference_year_check
        CHECK (reference_year BETWEEN 1900 AND 2100),

    CONSTRAINT silver_places_2023_pkey
        PRIMARY KEY (location_id, measure_id, data_value_type)
);

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_location
    ON public.silver_places_2023 (location_id);

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_measure
    ON public.silver_places_2023 (measure_id);

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_run_id
    ON public.silver_places_2023 (run_id);

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_reference_year
    ON public.silver_places_2023 (reference_year);


-- ============================================================
-- BRFSS 2023
-- Respondent identity separated from analytical responses.
-- silver_respondent  : one row per sampled respondent.
-- silver_brfss_2023  : one response row per respondent (1:1).
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_respondent (
    respondent_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sequence_number TEXT NOT NULL,
    state_fips VARCHAR(2) NOT NULL,
    run_id BIGINT NOT NULL
        REFERENCES public.etl_runs(run_id),

    CONSTRAINT ck_silver_respondent_state_fips
        CHECK (state_fips ~ '^[0-9]{2}$'),
    CONSTRAINT uq_silver_respondent_state_sequence
        UNIQUE (state_fips, sequence_number)
);

CREATE INDEX IF NOT EXISTS ix_silver_respondent_run_id
    ON public.silver_respondent (run_id);

CREATE TABLE IF NOT EXISTS public.silver_brfss_2023 (
    respondent_id BIGINT PRIMARY KEY
        REFERENCES public.silver_respondent(respondent_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    record_data JSONB NOT NULL,
    CONSTRAINT ck_silver_brfss_record_data_object
        CHECK (jsonb_typeof(record_data) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_silver_brfss_2023_record_data
    ON public.silver_brfss_2023 USING GIN (record_data);

-- ============================================================
-- ACS 2024
-- 7 physical columns
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_acs_2024 (
    acs_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    label_grouping TEXT NOT NULL,
    estimate TEXT,
    margin_of_error TEXT,
    percent TEXT,
    percent_margin_of_error TEXT,
    run_id BIGINT NOT NULL
        REFERENCES public.etl_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_acs_2024_label
    ON public.silver_acs_2024 (label_grouping);

CREATE INDEX IF NOT EXISTS idx_silver_acs_2024_run_id
    ON public.silver_acs_2024 (run_id);

    
-- ============================================================
-- ACS 2024 5-Year DP05 - County Level
-- One current record per county and variable.
-- ETL  is preserved through run_id.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_acs_county_2024 (
    acs_county_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_fips TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    geography_id TEXT NOT NULL,
    geography_name TEXT NOT NULL,
    variable_code TEXT NOT NULL,
    estimate TEXT,
    margin_of_error TEXT,
    percent TEXT,
    percent_margin_of_error TEXT,
    run_id BIGINT NOT NULL
        REFERENCES public.etl_runs(run_id),

    CONSTRAINT ck_acs_county_state_fips
        CHECK (state_fips ~ '^[0-9]{2}$'),
    CONSTRAINT ck_acs_county_county_fips
        CHECK (county_fips ~ '^[0-9]{3}$'),
    CONSTRAINT ck_acs_county_geography_id
        CHECK (geography_id ~ '^[0-9]{5}$'),
    CONSTRAINT ck_acs_county_fips_composition
        CHECK (geography_id = state_fips || county_fips),
    CONSTRAINT ck_acs_county_variable_code
        CHECK (
            variable_code IN (
                'DP05_0001',
                'DP05_0002',
                'DP05_0003',
                'DP05_0018'
            )
        ),
    CONSTRAINT uq_acs_county_geo_variable
        UNIQUE (geography_id, variable_code)
);

CREATE INDEX IF NOT EXISTS ix_silver_acs_county_run
    ON public.silver_acs_county_2024 (run_id);

CREATE INDEX IF NOT EXISTS ix_silver_acs_county_variable
    ON public.silver_acs_county_2024 (variable_code);

COMMENT ON TABLE public.silver_acs_county_2024 IS
    'ACS 2024 5-Year Data Profiles DP05 at county level. One current record per county and variable, with ETL run-level lineage.';

COMMENT ON COLUMN public.silver_acs_county_2024.geography_id IS
    'Five-digit Census FIPS code composed of state FIPS and county FIPS.';

COMMENT ON COLUMN public.silver_acs_county_2024.variable_code IS
    'ACS DP05 variable identifier. Four selected demographic indicators.';

COMMENT ON COLUMN public.silver_acs_county_2024.run_id IS
    'ETL execution identifier referencing public.etl_runs.';

-- ============================================================
-- SOURCE CATALOG
-- Reference table: excluded from analytical Silver cell budget.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_source (
    source_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code TEXT NOT NULL,
    source_name TEXT NOT NULL,
    provider TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    release_year SMALLINT NOT NULL,
    reference_period_start SMALLINT,
    reference_period_end SMALLINT,

    CONSTRAINT uq_silver_source_source_code
        UNIQUE (source_code),
    CONSTRAINT ck_silver_source_release_year
        CHECK (release_year BETWEEN 1900 AND 2100),
    CONSTRAINT ck_silver_source_reference_period
        CHECK (
            (reference_period_start IS NULL AND reference_period_end IS NULL)
            OR (
                reference_period_start IS NOT NULL
                AND reference_period_end IS NOT NULL
                AND reference_period_start BETWEEN 1900 AND 2100
                AND reference_period_end BETWEEN reference_period_start AND 2100
            )
        )
);

CREATE INDEX IF NOT EXISTS ix_silver_source_dataset_name
    ON public.silver_source (dataset_name);

CREATE INDEX IF NOT EXISTS ix_silver_source_release_year
    ON public.silver_source (release_year);


-- ============================================================
-- HEALTH INDICATOR CATALOG
-- One record per source variable/measure.
-- Reference table: excluded from analytical Silver cell budget.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_health_indicator (
    indicator_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    indicator_category TEXT NOT NULL,
    unit TEXT,
    value_type TEXT,
    source_id BIGINT NOT NULL
        REFERENCES public.silver_source(source_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    CONSTRAINT ck_silver_health_indicator_category
        CHECK (
            indicator_category IN (
                'health',
                'demographic',
                'socioeconomic',
                'health_risk'
            )
        ),
    CONSTRAINT ck_silver_health_indicator_source_code
        CHECK (BTRIM(source_code) <> ''),
    CONSTRAINT ck_silver_health_indicator_name
        CHECK (BTRIM(indicator_name) <> ''),
    CONSTRAINT uq_silver_health_indicator_source_code
        UNIQUE (source_id, source_code)
);

CREATE INDEX IF NOT EXISTS ix_silver_health_indicator_source_id
    ON public.silver_health_indicator (source_id);

CREATE INDEX IF NOT EXISTS ix_silver_health_indicator_category
    ON public.silver_health_indicator (indicator_category);


-- ============================================================
-- DOCUMENTATION
-- ============================================================

COMMENT ON TABLE public.etl_runs IS
    'Audit log for dataset ETL executions. One row per execution.';

COMMENT ON TABLE public.silver_places_2023 IS
    'Compact PLACES Silver dataset filtered to reference year 2023, with ETL run-level lineage.';

COMMENT ON COLUMN public.silver_places_2023.reference_year IS
    'Observation reference year preserved from the original Bronze Year column.';

COMMENT ON TABLE public.silver_respondent IS
    'BRFSS 2023 respondent identity dimension. One row per sampled respondent, with state FIPS and ETL lineage.';

COMMENT ON COLUMN public.silver_respondent.sequence_number IS
    'Original BRFSS SEQNO identifier, preserved as text.';

COMMENT ON COLUMN public.silver_respondent.state_fips IS
    'Two-digit state FIPS derived from BRFSS _STATE, preserving leading zeros.';

COMMENT ON COLUMN public.silver_respondent.run_id IS
    'ETL execution that most recently loaded this respondent.';

COMMENT ON TABLE public.silver_brfss_2023 IS
    'BRFSS 2023 analytical responses. One row per silver_respondent, linked by respondent_id; selected variables stored in JSONB.';

COMMENT ON TABLE public.silver_acs_2024 IS
    'ACS 2024 DP05 five-year estimates. Text values preserve source markers.';

-- ============================================================
-- GEOGRAPHIC REFERENCE DIMENSIONS
-- Reference dimensions; excluded from analytical Silver cell budget.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_geography (
    geography_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    geography_level VARCHAR(10) NOT NULL,
    geography_code VARCHAR(5) NOT NULL,
    geography_name VARCHAR(150) NOT NULL,
    parent_geography_id BIGINT REFERENCES public.silver_geography(geography_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    CONSTRAINT ck_silver_geography_level
        CHECK (geography_level IN ('STATE', 'COUNTY')),
    CONSTRAINT ck_silver_geography_code_length
        CHECK (
            (geography_level = 'STATE' AND geography_code ~ '^[0-9]{2}$')
            OR (geography_level = 'COUNTY' AND geography_code ~ '^[0-9]{5}$')
        ),
    CONSTRAINT ck_silver_geography_name CHECK (BTRIM(geography_name) <> ''),
    CONSTRAINT ck_silver_geography_latitude
        CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    CONSTRAINT ck_silver_geography_longitude
        CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    CONSTRAINT ck_silver_geography_parent
        CHECK (
            (geography_level = 'STATE' AND parent_geography_id IS NULL)
            OR (geography_level = 'COUNTY' AND parent_geography_id IS NOT NULL)
        ),
    CONSTRAINT uq_silver_geography_level_code UNIQUE (geography_level, geography_code)
);

CREATE INDEX IF NOT EXISTS ix_silver_geography_parent
    ON public.silver_geography (parent_geography_id);
CREATE INDEX IF NOT EXISTS ix_silver_geography_level
    ON public.silver_geography (geography_level);

CREATE TABLE IF NOT EXISTS public.silver_county (
    geography_id BIGINT PRIMARY KEY
        REFERENCES public.silver_geography(geography_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    state_fips VARCHAR(2) NOT NULL,
    county_fips VARCHAR(3) NOT NULL,
    state_abbr VARCHAR(2) NOT NULL,
    CONSTRAINT ck_silver_county_state_fips CHECK (state_fips ~ '^[0-9]{2}$'),
    CONSTRAINT ck_silver_county_county_fips CHECK (county_fips ~ '^[0-9]{3}$'),
    CONSTRAINT ck_silver_county_state_abbr CHECK (state_abbr ~ '^[A-Z]{2}$'),
    CONSTRAINT uq_silver_county_fips UNIQUE (state_fips, county_fips)
);

CREATE INDEX IF NOT EXISTS ix_silver_county_state_fips
    ON public.silver_county (state_fips);

COMMENT ON TABLE public.silver_geography IS
    'Geographic reference catalogue for state and county FIPS units. One row per geography level and normalized FIPS code.';
COMMENT ON COLUMN public.silver_geography.geography_code IS
    'FIPS stored as text to preserve leading zeros: 2 digits for STATE and 5 digits for COUNTY.';
COMMENT ON COLUMN public.silver_geography.parent_geography_id IS
    'Self-reference to the parent geography. NULL for states; county rows reference their state.';
COMMENT ON TABLE public.silver_county IS
    'County-specific attributes linked one-to-one to silver_geography through geography_id.';
COMMENT ON COLUMN public.silver_county.geography_id IS
    'Primary key and foreign key to the corresponding COUNTY row in silver_geography.';


COMMENT ON TABLE public.silver_source IS
    'Silver source catalog. Release year identifies the published dataset edition; optional reference period identifies the observation period.';
COMMENT ON COLUMN public.silver_source.release_year IS
    'Publication/release year of the dataset edition, not necessarily the observation year.';
COMMENT ON TABLE public.silver_health_indicator IS
    'Source-level indicator catalog. One record per source and original variable/measure code; independent from Gold dimensions.';
COMMENT ON COLUMN public.silver_health_indicator.source_code IS
    'Original variable or measure identifier from the source, without estimate/MOE suffixes.';
COMMENT ON COLUMN public.silver_health_indicator.indicator_category IS
    'Controlled classification: health, demographic, socioeconomic, or health_risk.';

