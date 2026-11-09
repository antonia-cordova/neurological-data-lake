BEGIN;

-- Recibe el identificador como parámetro de psql.
-- La configuración dura únicamente durante esta transacción.
SELECT set_config(
    'app.places_source_run_id',
    :'places_source_run_id',
    true
);

-- ------------------------------------------------------------
-- 1. VALIDAR TABLA, EJECUCIÓN Y CONSISTENCIA DE LOS REGISTROS
-- ------------------------------------------------------------

DO $$
DECLARE
    v_run_id BIGINT;
    v_status TEXT;
    v_dataset TEXT;
    v_target TEXT;
    v_loaded_rows BIGINT;
    v_silver_rows BIGINT;
    v_existing_conflicts BIGINT;
BEGIN
    v_run_id := current_setting(
        'app.places_source_run_id'
    )::BIGINT;

    IF to_regclass('public.silver_places_2023') IS NULL THEN
        RAISE EXCEPTION
            'No existe public.silver_places_2023.';
    END IF;

    SELECT
        status,
        dataset_name,
        target_table,
        loaded_rows
    INTO
        v_status,
        v_dataset,
        v_target,
        v_loaded_rows
    FROM public.etl_runs
    WHERE run_id = v_run_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'No existe el run_id % en public.etl_runs.',
            v_run_id;
    END IF;

    IF v_status <> 'succeeded'
       OR v_dataset <> 'places_2025'
       OR v_target <> 'silver_places_2023' THEN
        RAISE EXCEPTION
            'El run_id % no corresponde a una ejecución PLACES exitosa.',
            v_run_id;
    END IF;

    SELECT COUNT(*)
    INTO v_silver_rows
    FROM public.silver_places_2023;

    IF v_silver_rows = 0 THEN
        RAISE EXCEPTION
            'La tabla Silver PLACES está vacía.';
    END IF;

    IF v_loaded_rows <> v_silver_rows THEN
        RAISE EXCEPTION
            'Inconsistencia: etl_runs.loaded_rows=%, Silver rows=%.',
            v_loaded_rows,
            v_silver_rows;
    END IF;

    -- Si la columna ya existe por una ejecución anterior
    -- o parcial, no sobrescribir identificadores diferentes.
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'silver_places_2023'
          AND column_name = 'run_id'
    ) THEN
        EXECUTE format(
            'SELECT COUNT(*) FROM public.silver_places_2023
             WHERE run_id IS NOT NULL AND run_id <> %L',
            v_run_id
        )
        INTO v_existing_conflicts;

        IF v_existing_conflicts > 0 THEN
            RAISE EXCEPTION
                'Hay % registros con run_id diferente al solicitado (%).',
                v_existing_conflicts,
                v_run_id;
        END IF;
    END IF;

    RAISE NOTICE
        'Validación correcta: run_id=%, registros=%.',
        v_run_id,
        v_silver_rows;
END $$;

-- ------------------------------------------------------------
-- 2. AGREGAR LA COLUMNA DE TRAZABILIDAD
-- ------------------------------------------------------------

ALTER TABLE public.silver_places_2023
ADD COLUMN IF NOT EXISTS run_id BIGINT;

-- ------------------------------------------------------------
-- 3. ASIGNAR EL RUN_ID A LOS REGISTROS EXISTENTES
-- ------------------------------------------------------------

UPDATE public.silver_places_2023
SET run_id = current_setting(
    'app.places_source_run_id'
)::BIGINT
WHERE run_id IS NULL;

-- ------------------------------------------------------------
-- 4. EXIGIR RUN_ID EN TODOS LOS REGISTROS
-- ------------------------------------------------------------

ALTER TABLE public.silver_places_2023
ALTER COLUMN run_id SET NOT NULL;

-- ------------------------------------------------------------
-- 5. INCORPORAR CLAVE FORÁNEA
-- ------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'silver_places_2023_run_id_fkey'
          AND conrelid = 'public.silver_places_2023'::regclass
    ) THEN
        ALTER TABLE public.silver_places_2023
        ADD CONSTRAINT silver_places_2023_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES public.etl_runs(run_id);
    END IF;
END $$;

-- ------------------------------------------------------------
-- 6. ÍNDICE Y DOCUMENTACIÓN
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_silver_places_2023_run_id
ON public.silver_places_2023 (run_id);

COMMENT ON COLUMN public.silver_places_2023.run_id IS
    'Identificador de la ejecución ETL que produjo el registro.';

COMMIT;
