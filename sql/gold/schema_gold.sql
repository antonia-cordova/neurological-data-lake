BEGIN;

-- ============================================================
-- Fuentes: PLACES, BRFSS, ACS y ACS_COUNTY
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
        CHECK (data_value >= 0 AND data_value <= 100)
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

COMMIT;