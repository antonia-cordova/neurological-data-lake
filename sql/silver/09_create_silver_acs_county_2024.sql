
-- ============================================================
-- ACS 2024 5-Year DP05 - County Level
-- One current record per county and variable.
-- ETL lineage is preserved through run_id.
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

    -- Geographic integrity
    CONSTRAINT ck_acs_county_state_fips
        CHECK (state_fips ~ '^[0-9]{2}$'),

    CONSTRAINT ck_acs_county_county_fips
        CHECK (county_fips ~ '^[0-9]{3}$'),

    CONSTRAINT ck_acs_county_geography_id
        CHECK (geography_id ~ '^[0-9]{5}$'),

    CONSTRAINT ck_acs_county_fips_composition
        CHECK (geography_id = state_fips || county_fips),

    -- Allowed ACS DP05 variables
    CONSTRAINT ck_acs_county_variable_code
        CHECK (
            variable_code IN (
                'DP05_0001',
                'DP05_0002',
                'DP05_0003',
                'DP05_0018'
            )
        ),

    -- Business uniqueness: one variable per county
    CONSTRAINT uq_acs_county_geo_variable
        UNIQUE (geography_id, variable_code)
);

-- Indexes
CREATE INDEX IF NOT EXISTS ix_silver_acs_county_run
    ON public.silver_acs_county_2024 (run_id);

CREATE INDEX IF NOT EXISTS ix_silver_acs_county_variable
    ON public.silver_acs_county_2024 (variable_code);

-- Documentation
COMMENT ON TABLE public.silver_acs_county_2024 IS
    'ACS 2024 5-Year Data Profiles DP05 at county level. One current record per county and variable, with ETL run-level lineage.';

COMMENT ON COLUMN public.silver_acs_county_2024.geography_id IS
    'Five-digit Census FIPS code composed of state FIPS and county FIPS.';

COMMENT ON COLUMN public.silver_acs_county_2024.variable_code IS
    'ACS DP05 variable identifier. Four selected demographic indicators.';

COMMENT ON COLUMN public.silver_acs_county_2024.run_id IS
    'ETL execution identifier referencing public.etl_runs.';
