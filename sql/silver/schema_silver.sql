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
-- 10 physical columns (9 data + 1 lineage)
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
    run_id BIGINT NOT NULL
        REFERENCES public.etl_runs(run_id),

    CONSTRAINT silver_places_2023_pkey
        PRIMARY KEY (location_id, measure_id, data_value_type)
);

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_location
    ON public.silver_places_2023 (location_id);

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_measure
    ON public.silver_places_2023 (measure_id);

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_run_id
    ON public.silver_places_2023 (run_id);


-- ============================================================
-- BRFSS 2023
-- 5 physical columns
-- ============================================================

CREATE TABLE IF NOT EXISTS public.silver_brfss_2023 (
    respondent_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_code TEXT NOT NULL,
    sequence_number TEXT NOT NULL,
    record_data JSONB NOT NULL,
    run_id BIGINT NOT NULL
        REFERENCES public.etl_runs(run_id),

    CONSTRAINT silver_brfss_2023_state_sequence_key
        UNIQUE (state_code, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_silver_brfss_2023_state
    ON public.silver_brfss_2023 (state_code);

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
-- DOCUMENTATION
-- ============================================================

COMMENT ON TABLE public.etl_runs IS
    'Audit log for dataset ETL executions. One row per execution.';

COMMENT ON TABLE public.silver_places_2023 IS
    'Compact PLACES 2023 Silver dataset with ETL run-level lineage.';

COMMENT ON TABLE public.silver_brfss_2023 IS
    'BRFSS 2023 sample. Selected variables stored in JSONB.';

COMMENT ON TABLE public.silver_acs_2024 IS
    'ACS 2024 DP05 five-year estimates. Text values preserve source markers.';