BEGIN;

-- Execution order: GENERAL -> ACS -> BRFSS -> INTEGRATED.
-- Geografia: 52 jurisdicciones + condados
-- ============================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ------------------------------------------------------------
-- 1. DIMENSION: SOURCE
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_source (
    source_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code TEXT NOT NULL UNIQUE,
    dataset_name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 2. DIMENSION: GEOGRAPHY
-- Nivel STATE: 52 jurisdicciones.
-- Nivel COUNTY: condados identificados por FIPS de 5 digitos.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_geography (
    geography_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id TEXT NOT NULL UNIQUE,
    state_abbr TEXT,
    location_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,

    geography_level TEXT NOT NULL,
    state_fips VARCHAR(2),
    county_fips VARCHAR(3),
    parent_geography_key BIGINT,
    country_code VARCHAR(2) DEFAULT 'US',

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT dim_geography_level_check
        CHECK (geography_level IN ('STATE', 'COUNTY')),

    CONSTRAINT dim_geography_state_fips_check
        CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),

    CONSTRAINT dim_geography_county_fips_check
        CHECK (county_fips IS NULL OR county_fips ~ '^[0-9]{3}$'),

    CONSTRAINT dim_geography_country_check
        CHECK (country_code IS NULL OR country_code ~ '^[A-Z]{2}$'),

    CONSTRAINT dim_geography_coordinates_check
        CHECK (
            (latitude IS NULL OR latitude BETWEEN -90 AND 90)
            AND
            (longitude IS NULL OR longitude BETWEEN -180 AND 180)
        ),

    CONSTRAINT dim_geography_parent_fk
        FOREIGN KEY (parent_geography_key)
        REFERENCES gold.dim_geography(geography_key)
        ON DELETE RESTRICT,

    CONSTRAINT dim_geography_state_structure_check
        CHECK (
            geography_level <> 'STATE'
            OR (
                state_fips IS NOT NULL
                AND county_fips IS NULL
                AND parent_geography_key IS NULL
            )
        ),

    CONSTRAINT dim_geography_county_structure_check
        CHECK (
            geography_level <> 'COUNTY'
            OR (
                state_fips IS NOT NULL
                AND county_fips IS NOT NULL
                AND parent_geography_key IS NOT NULL
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_dim_geography_level
    ON gold.dim_geography(geography_level);

CREATE INDEX IF NOT EXISTS idx_dim_geography_state_fips
    ON gold.dim_geography(state_fips);

CREATE INDEX IF NOT EXISTS idx_dim_geography_parent
    ON gold.dim_geography(parent_geography_key);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_geography_county_fips
    ON gold.dim_geography(state_fips, county_fips)
    WHERE geography_level = 'COUNTY';

-- ------------------------------------------------------------
-- 3. CATALOGO DE 52 JURISDICCIONES
-- Se conservan las 52 como nivel STATE por compatibilidad con BRFSS y con la correspondencia FIPS del proyecto
-- ------------------------------------------------------------

WITH state_catalog(state_fips, state_abbr, state_name) AS (
    VALUES
        ('01','AL','Alabama'),
        ('02','AK','Alaska'),
        ('04','AZ','Arizona'),
        ('05','AR','Arkansas'),
        ('06','CA','California'),
        ('08','CO','Colorado'),
        ('09','CT','Connecticut'),
        ('10','DE','Delaware'),
        ('11','DC','District of Columbia'),
        ('12','FL','Florida'),
        ('13','GA','Georgia'),
        ('15','HI','Hawaii'),
        ('16','ID','Idaho'),
        ('17','IL','Illinois'),
        ('18','IN','Indiana'),
        ('19','IA','Iowa'),
        ('20','KS','Kansas'),
        ('22','LA','Louisiana'),
        ('23','ME','Maine'),
        ('24','MD','Maryland'),
        ('25','MA','Massachusetts'),
        ('26','MI','Michigan'),
        ('27','MN','Minnesota'),
        ('28','MS','Mississippi'),
        ('29','MO','Missouri'),
        ('30','MT','Montana'),
        ('31','NE','Nebraska'),
        ('32','NV','Nevada'),
        ('33','NH','New Hampshire'),
        ('34','NJ','New Jersey'),
        ('35','NM','New Mexico'),
        ('36','NY','New York'),
        ('37','NC','North Carolina'),
        ('38','ND','North Dakota'),
        ('39','OH','Ohio'),
        ('40','OK','Oklahoma'),
        ('41','OR','Oregon'),
        ('44','RI','Rhode Island'),
        ('45','SC','South Carolina'),
        ('46','SD','South Dakota'),
        ('47','TN','Tennessee'),
        ('48','TX','Texas'),
        ('49','UT','Utah'),
        ('50','VT','Vermont'),
        ('51','VA','Virginia'),
        ('53','WA','Washington'),
        ('54','WV','West Virginia'),
        ('55','WI','Wisconsin'),
        ('56','WY','Wyoming'),
        ('66','GU','Guam'),
        ('72','PR','Puerto Rico'),
        ('78','VI','U.S. Virgin Islands')
)
INSERT INTO gold.dim_geography (
    location_id,
    state_abbr,
    location_name,
    geography_level,
    state_fips,
    county_fips,
    parent_geography_key,
    country_code
)
SELECT
    state_fips,
    state_abbr,
    state_name,
    'STATE',
    state_fips,
    NULL,
    NULL,
    'US'
FROM state_catalog
ON CONFLICT (location_id) DO NOTHING;

-- validar que el catalogo contiene exactamente 52 jurisdicciones
DO $$
DECLARE
    total_states INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO total_states
    FROM gold.dim_geography
    WHERE geography_level = 'STATE';

    IF total_states <> 52 THEN
        RAISE EXCEPTION
            'Catalogo geografico invalido: se esperaban 52 jurisdicciones y existen %',
            total_states;
    END IF;
END $$;

-- ------------------------------------------------------------
-- 4. DIMENSION: HEALTH MEASURE
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_health_measure (
    health_measure_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    measure_id TEXT NOT NULL UNIQUE,
    measure TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 5. DIMENSION: PREVALENCE TYPE
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_prevalence_type (
    prevalence_type_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data_value_type TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 6. FACT: AREA HEALTH
-- source_run_id + geography_key
-- + health_measure_key + prevalence_type_key
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_area_health (
    source_run_id BIGINT NOT NULL,
    source_key BIGINT NOT NULL,
    geography_key BIGINT NOT NULL,
    health_measure_key BIGINT NOT NULL,
    prevalence_type_key BIGINT NOT NULL,

    reference_year SMALLINT NOT NULL,
    data_value NUMERIC NOT NULL,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fact_area_health_pk
        PRIMARY KEY (
            source_run_id,
            geography_key,
            health_measure_key,
            prevalence_type_key
        ),

    CONSTRAINT fact_area_health_run_fk
        FOREIGN KEY (source_run_id)
        REFERENCES public.etl_runs(run_id)
        ON DELETE RESTRICT,

    CONSTRAINT fact_area_health_source_fk
        FOREIGN KEY (source_key)
        REFERENCES gold.dim_source(source_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_area_health_geography_fk
        FOREIGN KEY (geography_key)
        REFERENCES gold.dim_geography(geography_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_area_health_measure_fk
        FOREIGN KEY (health_measure_key)
        REFERENCES gold.dim_health_measure(health_measure_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_area_health_prevalence_fk
        FOREIGN KEY (prevalence_type_key)
        REFERENCES gold.dim_prevalence_type(prevalence_type_key)
        ON DELETE RESTRICT,

    CONSTRAINT fact_area_health_value_check
        CHECK (data_value >= 0 AND data_value <= 100),

    CONSTRAINT fact_area_health_reference_year_check
        CHECK (reference_year BETWEEN 1900 AND 2100)
);

-- ------------------------------------------------------------
-- 7. INDICES FACT AREA HEALTH
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_fact_area_health_geography
    ON gold.fact_area_health(geography_key);

CREATE INDEX IF NOT EXISTS idx_fact_area_health_measure
    ON gold.fact_area_health(health_measure_key);

CREATE INDEX IF NOT EXISTS idx_fact_area_health_prevalence
    ON gold.fact_area_health(prevalence_type_key);

CREATE INDEX IF NOT EXISTS idx_fact_area_health_source_run
    ON gold.fact_area_health(source_run_id);

CREATE INDEX IF NOT EXISTS idx_fact_area_health_reference_year
    ON gold.fact_area_health(reference_year);

CREATE INDEX IF NOT EXISTS idx_fact_area_health_source
    ON gold.fact_area_health(source_key);

-- ------------------------------------------------------------
-- 8. CATALOGOS INICIALES
-- ------------------------------------------------------------

INSERT INTO gold.dim_source (
    source_code,
    dataset_name,
    description
)
VALUES (
    'PLACES',
    'places_2025',
    'PLACES 2025: indicadores de salud publica por geografia.'
)
ON CONFLICT (source_code) DO NOTHING;

INSERT INTO gold.dim_prevalence_type (data_value_type)
VALUES
    ('Age-adjusted prevalence'),
    ('Crude prevalence')
ON CONFLICT (data_value_type) DO NOTHING;

-- ------------------------------------------------------------
-- 9. DOCUMENTACION
-- ------------------------------------------------------------

COMMENT ON TABLE gold.dim_geography IS
    'Dimension geografica jerarquica de Estados, jurisdicciones y condados.';

COMMENT ON COLUMN gold.dim_geography.location_id IS
    'Identificador geografico de origen conservado como texto.';

COMMENT ON COLUMN gold.dim_geography.geography_level IS
    'Nivel geografico: STATE o COUNTY.';

COMMENT ON COLUMN gold.dim_geography.state_fips IS
    'Codigo FIPS estatal de dos digitos.';

COMMENT ON COLUMN gold.dim_geography.county_fips IS
    'Codigo FIPS de condado de tres digitos.';

COMMENT ON COLUMN gold.dim_geography.parent_geography_key IS
    'Referencia al estado o jurisdiccion padre del condado.';

COMMENT ON TABLE gold.fact_area_health IS
    'Hechos territoriales de salud. Conserva versiones por ejecucion ETL.';

COMMENT ON COLUMN gold.fact_area_health.source_run_id IS
    'Identificador de la ejecucion ETL que origino la version de los datos.';

COMMENT ON COLUMN gold.fact_area_health.loaded_at IS
    'Fecha y hora en que el registro fue incorporado a Gold.';


-- ============================================================
-- 02. GOLD ACS: nacional y county
-- ============================================================

-- ============================================================
-- GOLD ACS 2024
-- ACS nacional y ACS County.
-- ============================================================

-- ============================================================
-- 02.1. ACS NACIONAL: DIMENSIÓN DE INDICADORES
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
-- 02.2. ACS NACIONAL: TABLA DE HECHOS
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
-- 02.3. ACS COUNTY: DIMENSIÓN DE INDICADORES DP05
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
-- 02.4. ACS COUNTY: TABLA DE HECHOS
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
-- 02.5. REGISTRO DE FUENTES
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
-- 02.6. DOCUMENTACIÓN
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


-- ============================================================
-- 03. GOLD BRFSS: variables, respuestas, jurisdicciones y bridge geográfico
-- Grano: una respuesta por persona, variable y ejecución ETL.
-- ============================================================

-- ------------------------------------------------------------
-- 03.1. Dimensión de variables BRFSS
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
-- 03.2. Catálogo de códigos de respuesta
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
-- 03.3. Dimensión de jurisdicciones BRFSS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_brfss_jurisdiction (
    jurisdiction_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_code TEXT NOT NULL UNIQUE,
    jurisdiction_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 03.4. Hechos de respuestas BRFSS
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
-- 03.5. Puente geográfico BRFSS - GOLD
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
-- 03.6. Registrar BRFSS en el catálogo compartido de fuentes
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
-- 03.7. Documentación del modelo
-- ------------------------------------------------------------

COMMENT ON TABLE gold.bridge_brfss_geography IS
    'Relaciona las jurisdicciones BRFSS con geografías estatales GOLD mediante FIPS.';

COMMENT ON COLUMN gold.bridge_brfss_geography.state_fips IS
    'Código FIPS estatal de dos dígitos utilizado para la correspondencia geográfica.';

COMMENT ON COLUMN gold.bridge_brfss_geography.mapping_method IS
    'Método de correspondencia geográfica. Para este modelo: STATE_FIPS.';


-- ============================================================
-- 04. GOLD INTEGRATED: indicadores, bridge y hechos integrados

-- Esquema definitivo para instalaciones nuevas.
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

COMMIT;

COMMENT ON COLUMN gold.fact_area_health.reference_year IS
    'Observation reference year propagated from Silver; for PLACES 2023 this is 2023.';
