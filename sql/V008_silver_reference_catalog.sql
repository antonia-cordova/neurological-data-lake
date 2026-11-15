-- Silver reference catalogs. Safe to apply repeatedly; does not alter analytical tables.
CREATE TABLE IF NOT EXISTS public.silver_source (
    source_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    provider TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    release_year SMALLINT NOT NULL CHECK (release_year BETWEEN 1900 AND 2100),
    reference_period_start SMALLINT CHECK (reference_period_start BETWEEN 1900 AND 2100),
    reference_period_end SMALLINT CHECK (reference_period_end BETWEEN 1900 AND 2100),
    CONSTRAINT ck_silver_source_period
        CHECK (
            reference_period_start IS NULL OR reference_period_end IS NULL
            OR reference_period_start <= reference_period_end
        )
);

CREATE TABLE IF NOT EXISTS public.silver_health_indicator (
    indicator_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    indicator_category TEXT NOT NULL
        CHECK (indicator_category IN ('health', 'demographic', 'socioeconomic', 'health_risk')),
    unit TEXT NOT NULL,
    value_type TEXT NOT NULL,
    source_id BIGINT NOT NULL
        REFERENCES public.silver_source(source_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT uq_silver_health_indicator_source_code
        UNIQUE (source_id, source_code),
    CONSTRAINT ck_silver_health_indicator_code
        CHECK (BTRIM(source_code) <> ''),
    CONSTRAINT ck_silver_health_indicator_name
        CHECK (BTRIM(indicator_name) <> '')
);

CREATE INDEX IF NOT EXISTS ix_silver_health_indicator_source
    ON public.silver_health_indicator(source_id);
CREATE INDEX IF NOT EXISTS ix_silver_health_indicator_category
    ON public.silver_health_indicator(indicator_category);

COMMENT ON TABLE public.silver_source IS
    'Reference catalog of original data sources used by the Silver layer. Release year is distinct from observation/reference period.';
COMMENT ON TABLE public.silver_health_indicator IS
    'Reference catalog of selected source variables and measures. It is independent of Gold dimensions and excluded from analytical cell budgeting.';
COMMENT ON COLUMN public.silver_health_indicator.source_code IS
    'Original variable or measure identifier from the source dataset; ACS profile suffixes are normalized to the base DP05 code.';
COMMENT ON COLUMN public.silver_health_indicator.value_type IS
    'Semantic type of values represented by the source variable, not a physical database type.';
