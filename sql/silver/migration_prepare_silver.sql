-- PREPARACIÓN ÚNICA PARA LA MIGRACIÓN A SILVER COMPACTA
-- Ejecutar una sola vez antes de los nuevos ETL.
-- Mueve las tablas completas actuales fuera de public (Silver).
-- No elimina los datos históricos.

BEGIN;

CREATE SCHEMA IF NOT EXISTS silver_archive;

DO $$
BEGIN
    IF to_regclass('public.silver_places_2023') IS NOT NULL
       AND to_regclass('silver_archive.silver_places_2023') IS NULL THEN
        ALTER TABLE public.silver_places_2023 SET SCHEMA silver_archive;
    END IF;

    IF to_regclass('public.silver_places_2025') IS NOT NULL
       AND to_regclass('silver_archive.silver_places_2025') IS NULL THEN
        ALTER TABLE public.silver_places_2025 SET SCHEMA silver_archive;
    END IF;

    IF to_regclass('public.silver_brfss_2023') IS NOT NULL
       AND to_regclass('silver_archive.silver_brfss_2023') IS NULL THEN
        ALTER TABLE public.silver_brfss_2023 SET SCHEMA silver_archive;
    END IF;
END $$;

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
    CONSTRAINT silver_places_2023_pkey
        PRIMARY KEY (location_id, measure_id, data_value_type)
);

CREATE TABLE IF NOT EXISTS public.silver_brfss_2023 (
    respondent_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_code TEXT NOT NULL,
    sequence_number TEXT NOT NULL,
    record_data JSONB NOT NULL,
    run_id BIGINT NOT NULL REFERENCES public.etl_runs(run_id),
    CONSTRAINT silver_brfss_2023_state_sequence_key
        UNIQUE (state_code, sequence_number)
);

COMMIT;
