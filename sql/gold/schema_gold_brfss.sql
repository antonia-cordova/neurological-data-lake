BEGIN;

-- ============================================================
-- GOLD BRFSS 2023
-- Grano: una respuesta por persona, variable y ejecución ETL.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Dimensión de variables BRFSS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_brfss_variable (
    variable_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    variable_code TEXT NOT NULL UNIQUE,
    variable_description TEXT,
    value_type TEXT NOT NULL
        CHECK (value_type IN ('CATEGORICAL', 'NUMERIC')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 2. Catálogo de códigos de respuesta
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_brfss_response_code (
    response_code_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    variable_key BIGINT NOT NULL,
    response_code TEXT NOT NULL,
    response_label TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT dim_brfss_response_variable_fk
        FOREIGN KEY (variable_key)
        REFERENCES gold.dim_brfss_variable(variable_key)
        ON DELETE RESTRICT,

    CONSTRAINT dim_brfss_response_code_unique
        UNIQUE (variable_key, response_code)
);

CREATE INDEX IF NOT EXISTS idx_dim_brfss_response_variable
    ON gold.dim_brfss_response_code(variable_key);

-- ------------------------------------------------------------
-- 3. Dimensión de jurisdicciones BRFSS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_brfss_jurisdiction (
    jurisdiction_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_code TEXT NOT NULL UNIQUE,
    jurisdiction_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 4. Hechos de respuestas BRFSS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_brfss_response (
    source_run_id BIGINT NOT NULL,
    source_key BIGINT NOT NULL,
    jurisdiction_key BIGINT NOT NULL,
    variable_key BIGINT NOT NULL,

    sequence_number TEXT NOT NULL,
    raw_value TEXT,
    numeric_value NUMERIC,
    response_code_key BIGINT,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fact_brfss_response_pk
        PRIMARY KEY (
            source_run_id,
            jurisdiction_key,
            sequence_number,
            variable_key
        ),

    CONSTRAINT fact_brfss_response_run_fk
        FOREIGN KEY (source_run_id)
        REFERENCES public.etl_runs(run_id)
        ON DELETE RESTRICT,

    CONSTRAINT fact_brfss_response_source_fk
        FOREIGN KEY (source_key)
        REFERENCES gold.dim_source(source_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_brfss_response_jurisdiction_fk
        FOREIGN KEY (jurisdiction_key)
        REFERENCES gold.dim_brfss_jurisdiction(jurisdiction_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_brfss_response_variable_fk
        FOREIGN KEY (variable_key)
        REFERENCES gold.dim_brfss_variable(variable_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_brfss_response_code_fk
        FOREIGN KEY (response_code_key)
        REFERENCES gold.dim_brfss_response_code(response_code_key)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_fact_brfss_variable
    ON gold.fact_brfss_response(variable_key);

CREATE INDEX IF NOT EXISTS idx_fact_brfss_jurisdiction
    ON gold.fact_brfss_response(jurisdiction_key);

CREATE INDEX IF NOT EXISTS idx_fact_brfss_source_run
    ON gold.fact_brfss_response(source_run_id);

CREATE INDEX IF NOT EXISTS idx_fact_brfss_sequence
    ON gold.fact_brfss_response(sequence_number);

-- ------------------------------------------------------------
-- 5. Puente geográfico BRFSS - GOLD
--
-- Una jurisdicción BRFSS se vincula con una geografía STATE
-- mediante su código FIPS estatal.
--
-- Las correspondencias se insertan desde el ETL BRFSS,
-- una vez cargadas las jurisdicciones.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.bridge_brfss_geography (
    jurisdiction_key BIGINT PRIMARY KEY,
    geography_key BIGINT NOT NULL,
    state_code TEXT NOT NULL,
    state_fips VARCHAR(2) NOT NULL,
    mapping_method TEXT NOT NULL DEFAULT 'STATE_FIPS',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT bridge_brfss_geography_jurisdiction_fk
        FOREIGN KEY (jurisdiction_key)
        REFERENCES gold.dim_brfss_jurisdiction(jurisdiction_key)
        ON DELETE RESTRICT,

    CONSTRAINT bridge_brfss_geography_geography_fk
        FOREIGN KEY (geography_key)
        REFERENCES gold.dim_geography(geography_key)
        ON DELETE RESTRICT,

    CONSTRAINT bridge_brfss_geography_code_check
        CHECK (state_fips ~ '^[0-9]{2}$'),

    CONSTRAINT bridge_brfss_geography_method_check
        CHECK (mapping_method = 'STATE_FIPS'),

    CONSTRAINT bridge_brfss_geography_state_code_unique
        UNIQUE (state_code),

    CONSTRAINT bridge_brfss_geography_state_fips_unique
        UNIQUE (state_fips),

    CONSTRAINT bridge_brfss_geography_geography_unique
        UNIQUE (geography_key)
);

CREATE INDEX IF NOT EXISTS idx_bridge_brfss_geography_geography
    ON gold.bridge_brfss_geography(geography_key);

CREATE INDEX IF NOT EXISTS idx_bridge_brfss_geography_state_fips
    ON gold.bridge_brfss_geography(state_fips);

-- ------------------------------------------------------------
-- 6. Registrar BRFSS en el catálogo compartido de fuentes
-- ------------------------------------------------------------

INSERT INTO gold.dim_source (
    source_code,
    dataset_name,
    description
)
VALUES (
    'BRFSS',
    'brfss_2023',
    'Behavioral Risk Factor Surveillance System 2023.'
)
ON CONFLICT (source_code) DO NOTHING;

-- ------------------------------------------------------------
-- 7. Documentación del modelo
-- ------------------------------------------------------------

COMMENT ON TABLE gold.bridge_brfss_geography IS
    'Relaciona las jurisdicciones BRFSS con geografías estatales GOLD mediante FIPS.';

COMMENT ON COLUMN gold.bridge_brfss_geography.state_fips IS
    'Código FIPS estatal de dos dígitos utilizado para la correspondencia geográfica.';

COMMENT ON COLUMN gold.bridge_brfss_geography.mapping_method IS
    'Método de correspondencia geográfica. Para este modelo: STATE_FIPS.';

COMMIT;