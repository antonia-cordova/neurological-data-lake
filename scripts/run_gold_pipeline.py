from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import text

from scripts.gold.db_connection import create_gold_engine

ROOT = Path(__file__).resolve().parents[1]

DATABASE_ALLOWED = "health_datalake"

# Bloqueo de sesión PostgreSQL compartido por todas las instancias del orquestador.
# El bloqueo permanece adquirido mientras se ejecutan también los ETL hijos.
GOLD_PIPELINE_LOCK_KEY = 78_115_004_452_421

GEOGRAPHY_DATASET = "geography_catalog"
GEOGRAPHY_TARGET = "silver_geography+silver_county"

GEOGRAPHY_EXPECTED = {
    "silver_states": 49,
    "silver_counties": 83,
    "silver_county_catalog": 83,
    "gold_states": 52,
    "gold_counties": 83,
    "geography_loaded_rows": 215,
}

SOURCE_CONFIG = {
    "PLACES": {
        "dataset": "places_2025",
        "table": "silver_places_2023",
        "expected_rows": 2490,
        "module": "scripts.gold.etl_gold_places",
    },
    "BRFSS": {
        "dataset": "brfss_2023",
        "table": "silver_respondent+silver_brfss_2023",
        "expected_rows": 2800,
        "module": "scripts.gold.etl_gold_brfss",
    },
    "ACS nacional": {
        "dataset": "acs_2024",
        "table": "silver_acs_2024",
        "expected_rows": 113,
        "module": "scripts.gold.etl_gold_acs",
    },
    "ACS County": {
        "dataset": "acs_2024_county_dp05",
        "table": "silver_acs_county_2024",
        "expected_rows": 332,
        "module": "scripts.gold.etl_gold_acs_county",
    },
}

EXPECTED_GOLD = {
    "PLACES": {
        "facts": 2490,
    },
    "BRFSS": {
        "facts": 56000,
    },
    "ACS nacional": {
        "dimensions": 113,
        "facts": 108,
    },
    "ACS County": {
        "facts": 332,
    },
    "Integración PLACES": {
        "facts": 2490,
    },
}


def get_source_key(conn, source_code: str, dataset_name: str) -> int:
    source_key = conn.execute(
        text("""
            SELECT source_key
            FROM gold.dim_source
            WHERE source_code = :source_code
              AND dataset_name = :dataset_name
        """),
        {
            "source_code": source_code,
            "dataset_name": dataset_name,
        },
    ).scalar_one_or_none()

    if source_key is None:
        raise RuntimeError(
            f"No existe la fuente {source_code}/{dataset_name} en Gold."
        )

    return int(source_key)


def count_rows(conn, query: str, params: dict) -> int:
    return int(conn.execute(text(query), params).scalar_one())

def inspect_gold_stage(
    conn,
    name: str,
    run_id: int,
) -> bool:
    """
    Devuelve True si la etapa está completa y validada.
    Devuelve False si está vacía.
    Detiene el pipeline si encuentra una carga parcial o inconsistente.
    """

    if name == "PLACES":
        source_key = get_source_key(
            conn, "PLACES", "places_2025"
        )

        count = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.fact_area_health
            WHERE source_run_id = :run_id
              AND source_key = :source_key
            """,
            {"run_id": run_id, "source_key": source_key},
        )

        duplicates = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT geography_key, health_measure_key,
                       prevalence_type_key
                FROM gold.fact_area_health
                WHERE source_run_id = :run_id
                  AND source_key = :source_key
                GROUP BY geography_key, health_measure_key,
                         prevalence_type_key
                HAVING COUNT(*) > 1
            ) duplicated
            """,
            {"run_id": run_id, "source_key": source_key},
        )

        invalid = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.fact_area_health fact
            LEFT JOIN gold.dim_geography geography
                ON geography.geography_key = fact.geography_key
            LEFT JOIN gold.dim_health_measure measure
                ON measure.health_measure_key = fact.health_measure_key
            LEFT JOIN gold.dim_prevalence_type prevalence
                ON prevalence.prevalence_type_key = fact.prevalence_type_key
            WHERE fact.source_run_id = :run_id
            AND (
                    fact.source_key <> :source_key
                    OR geography.geography_key IS NULL
                    OR measure.health_measure_key IS NULL
                    OR prevalence.prevalence_type_key IS NULL
                    OR fact.data_value IS NULL
                    OR fact.data_value < 0
                    OR fact.data_value > 100
                    OR fact.reference_year IS NULL
                    OR fact.reference_year <> 2023
            )
            """,
            {"run_id": run_id, "source_key": source_key},
        )

        expected = EXPECTED_GOLD[name]["facts"]

    elif name == "BRFSS":
        source_key = get_source_key(
        conn, "BRFSS", "brfss_2023"
        )
        count = count_rows(
        conn,
        """
        SELECT COUNT(*)
        FROM gold.fact_brfss_response
        WHERE source_run_id = :run_id
        AND source_key = :source_key
        """,
        {
            "run_id": run_id,
            "source_key": source_key,
        },
    )

        duplicates = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT sequence_number, variable_key, jurisdiction_key
                FROM gold.fact_brfss_response
                WHERE source_run_id = :run_id
                  AND source_key = :source_key
                GROUP BY sequence_number, variable_key, jurisdiction_key
                HAVING COUNT(*) > 1
            ) duplicated
            """,
            {
                "run_id": run_id,
                "source_key": source_key,
            },
        )

        invalid = count_rows(
        conn,
        """
        SELECT COUNT(*)
        FROM gold.fact_brfss_response fact
        LEFT JOIN gold.dim_brfss_variable variable
            ON variable.variable_key = fact.variable_key
        LEFT JOIN gold.dim_brfss_jurisdiction jurisdiction
            ON jurisdiction.jurisdiction_key = fact.jurisdiction_key
        WHERE fact.source_run_id = :run_id
        AND (
        fact.source_key <> :source_key
        OR variable.variable_key IS NULL
        OR jurisdiction.jurisdiction_key IS NULL
        )
        """,
        {
            "run_id": run_id,
            "source_key": source_key,
        },
    )
        expected = EXPECTED_GOLD[name]["facts"]


    elif name == "ACS nacional":
        source_key = get_source_key(
        conn, "ACS", "acs_2024"
        )
        dimensions = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.dim_acs_indicator
            WHERE source_run_id = :run_id
            """,
            {"run_id": run_id},
        )

        count = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.fact_acs_indicator
            WHERE source_run_id = :run_id
            AND source_key = :source_key
            """,
            {"run_id": run_id,
            "source_key": source_key,},
        )

        duplicate_dimensions = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT source_row_id
                FROM gold.dim_acs_indicator
                WHERE source_run_id = :run_id
                GROUP BY source_row_id
                HAVING COUNT(*) > 1
            ) duplicated
            """,
            {"run_id": run_id},
        )

        duplicate_facts = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT indicator_key
                FROM gold.fact_acs_indicator
                WHERE source_run_id = :run_id
                AND source_key = :source_key
                GROUP BY indicator_key
                HAVING COUNT(*) > 1
            ) duplicated
            """,
            {
                "run_id": run_id,
                "source_key": source_key,
            },
        )
        invalid = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.fact_acs_indicator fact
            LEFT JOIN gold.dim_acs_indicator indicator
                ON indicator.indicator_key = fact.indicator_key
            WHERE fact.source_run_id = :run_id
            AND (
                    fact.source_key <> :source_key
                    OR indicator.indicator_key IS NULL
                    OR indicator.source_run_id <> fact.source_run_id
            ) 
            """,
            {"run_id": run_id,
            "source_key": source_key,},
        )

        expected_dimensions = EXPECTED_GOLD[name]["dimensions"]
        expected = EXPECTED_GOLD[name]["facts"]

        if dimensions == 0 and count == 0 and invalid == 0:
            return False

        if (
            dimensions != expected_dimensions
            or count != expected
            or duplicate_dimensions != 0
            or duplicate_facts != 0
            or invalid != 0
        ):
            raise RuntimeError(
                "ACS nacional presenta una carga parcial o inconsistente: "
                f"dimensiones={dimensions}/{expected_dimensions}, "
                f"hechos={count}/{expected}, "
                f"duplicados_dimensiones={duplicate_dimensions}, "
                f"duplicados_hechos={duplicate_facts}, "
                f"hechos_invalidos={invalid}."
            )

        return True

    elif name == "ACS County":
        source_key = get_source_key(
        conn, "ACS_COUNTY", "acs_2024_county_dp05"
        )
        count = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.fact_acs_county_indicator
            WHERE source_run_id = :run_id
            AND source_key = :source_key
            """,
            {"run_id": run_id,
            "source_key": source_key,},
        )

        duplicates = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT geography_key, indicator_key
                FROM gold.fact_acs_county_indicator
                WHERE source_run_id = :run_id
                AND source_key = :source_key
                GROUP BY geography_key, indicator_key
                HAVING COUNT(*) > 1
            ) duplicated
            """,
            {
                "run_id": run_id,
                "source_key": source_key,
            },
        )

        invalid = count_rows(
        conn,
        """
        SELECT COUNT(*)
        FROM gold.fact_acs_county_indicator fact
        LEFT JOIN gold.dim_geography geography
            ON geography.geography_key = fact.geography_key
        LEFT JOIN gold.dim_acs_county_indicator indicator
            ON indicator.indicator_key = fact.indicator_key
        WHERE fact.source_run_id = :run_id
        AND (
                fact.source_key <> :source_key
                OR geography.geography_key IS NULL
                OR geography.geography_level <> 'COUNTY'
                OR indicator.indicator_key IS NULL
        )
        """,
        {
            "run_id": run_id,
            "source_key": source_key,
        },
    )

        expected = EXPECTED_GOLD[name]["facts"]

    elif name == "Integración PLACES":
        source_key = get_source_key(
            conn, "PLACES", "places_2025"
        )

        count = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.fact_health_integrated
            WHERE source_run_id = :run_id
              AND source_key = :source_key
            """,
            {"run_id": run_id, "source_key": source_key},
        )

        duplicates = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT geography_key, indicator_key,
                       health_measure_key, prevalence_type_key
                FROM gold.fact_health_integrated
                WHERE source_run_id = :run_id
                  AND source_key = :source_key
                GROUP BY geography_key, indicator_key,
                         health_measure_key, prevalence_type_key
                HAVING COUNT(*) > 1
            ) duplicated
            """,
            {"run_id": run_id, "source_key": source_key},
        )

        invalid = count_rows(
            conn,
            """
            SELECT COUNT(*)
            FROM gold.fact_health_integrated fact
            LEFT JOIN gold.bridge_health_indicator_source bridge
                ON bridge.indicator_key = fact.indicator_key
            AND bridge.source_key = fact.source_key
            AND bridge.health_measure_key = fact.health_measure_key
            AND bridge.variable_key IS NULL
            LEFT JOIN gold.dim_geography geography
                ON geography.geography_key = fact.geography_key
            LEFT JOIN gold.dim_prevalence_type prevalence
                ON prevalence.prevalence_type_key = fact.prevalence_type_key
            WHERE fact.source_run_id = :run_id
            AND (
                    fact.source_key <> :source_key
                    OR bridge.mapping_key IS NULL
                    OR geography.geography_key IS NULL
                    OR fact.value_numeric IS NULL
                    OR fact.reference_year IS NULL
                    OR fact.reference_year <> 2023
                    OR fact.prevalence_type_key IS NULL
                    OR prevalence.prevalence_type_key IS NULL
                    OR geography.geography_level NOT IN ('COUNTY', 'STATE')
            )
            """,
            {"run_id": run_id, "source_key": source_key},
            )

        if count == 0 and invalid == 0:
            return False
                # ------------------------------------------------------------
        # VALIDACIÓN DE EQUIVALENCIA BIDIRECCIONAL PLACES - INTEGRACIÓN
        # Comprueba que ambas tablas contengan exactamente los mismos
        # registros para el run_id y source_key seleccionados.
        # ------------------------------------------------------------

        equivalence = conn.execute(
            text("""
                WITH base AS (
                    SELECT
                        bridge.indicator_key,
                        fact.geography_key,
                        fact.source_key,
                        fact.source_run_id,
                        fact.reference_year,
                        fact.health_measure_key,
                        fact.prevalence_type_key,
                        fact.data_value AS value_numeric
                    FROM gold.fact_area_health fact
                    JOIN gold.bridge_health_indicator_source bridge
                        ON bridge.source_key = fact.source_key
                       AND bridge.health_measure_key =
                           fact.health_measure_key
                       AND bridge.variable_key IS NULL
                    WHERE fact.source_run_id = :run_id
                      AND fact.source_key = :source_key
                ),
                integrated AS (
                    SELECT
                        indicator_key,
                        geography_key,
                        source_key,
                        source_run_id,
                        reference_year,
                        health_measure_key,
                        prevalence_type_key,
                        value_numeric
                    FROM gold.fact_health_integrated
                    WHERE source_run_id = :run_id
                      AND source_key = :source_key
                ),
                missing_integrated AS (
                    SELECT * FROM base
                    EXCEPT ALL
                    SELECT * FROM integrated
                ),
                unexpected_integrated AS (
                    SELECT * FROM integrated
                    EXCEPT ALL
                    SELECT * FROM base
                )
                SELECT
                    (
                        SELECT COUNT(*)
                        FROM missing_integrated
                    ) AS missing_integrated,
                    (
                        SELECT COUNT(*)
                        FROM unexpected_integrated
                    ) AS unexpected_integrated
            """),
            {
                "run_id": run_id,
                "source_key": source_key,
            },
        ).mappings().one()

        if (
            equivalence["missing_integrated"] != 0
            or equivalence["unexpected_integrated"] != 0
        ):
            raise RuntimeError(
                "Los hechos integrados no coinciden exactamente "
                "con los hechos territoriales PLACES: "
                f"{dict(equivalence)}"
            )
        
        expected = EXPECTED_GOLD[name]["facts"]

    else:
        raise ValueError(f"Etapa desconocida: {name}")

    if count == 0 and invalid == 0:
     return False 

    if (
        count != expected
        or duplicates != 0
        or invalid != 0
    ):
        raise RuntimeError(
            f"{name}: carga Gold parcial o inconsistente. "
            f"hechos={count}/{expected}, "
            f"duplicados={duplicates}, inválidos={invalid}."
        )

    return True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("run_gold_pipeline")


def select_latest_silver_run(conn, name: str, config: dict) -> int:
    """Selecciona la última ejecución Silver exitosa y valida su registro."""

    run = conn.execute(
        text("""
            SELECT run_id, loaded_rows
            FROM public.etl_runs
            WHERE dataset_name = :dataset_name
              AND target_table = :target_table
              AND status = 'succeeded'
              AND finished_at IS NOT NULL
            ORDER BY finished_at DESC, run_id DESC
            LIMIT 1
        """),
        {
            "dataset_name": config["dataset"],
            "target_table": config["table"],
        },
    ).mappings().one_or_none()

    if run is None:
        raise RuntimeError(
            f"No existe una ejecución Silver exitosa para {name}."
        )

    run_id = int(run["run_id"])
    loaded_rows = int(run["loaded_rows"])

    if loaded_rows != config["expected_rows"]:
        raise RuntimeError(
            f"{name}: run_id={run_id} registra {loaded_rows} filas; "
            f"se esperaban {config['expected_rows']}."
        )

    logger.info(
        "%s: run_id=%s | filas=%s",
        name,
        run_id,
        loaded_rows,
    )

    return run_id


def select_latest_geography_run() -> int:
    """Obtiene la ejecución Silver geográfica válida de esta reconstrucción."""
    engine = create_gold_engine()
    try:
        with engine.connect() as conn:
            database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()
            if database != DATABASE_ALLOWED:
                raise RuntimeError(
                    f"Pipeline geográfico bloqueado: base detectada {database}."
                )
            run = conn.execute(
                text("""
                    SELECT run_id, loaded_rows
                    FROM public.etl_runs
                    WHERE dataset_name = :dataset_name
                      AND target_table = :target_table
                      AND status = 'succeeded'
                      AND finished_at IS NOT NULL
                    ORDER BY finished_at DESC, run_id DESC
                    LIMIT 1
                """),
                {
                    "dataset_name": GEOGRAPHY_DATASET,
                    "target_table": GEOGRAPHY_TARGET,
                },
            ).mappings().one_or_none()
            if run is None:
                raise RuntimeError(
                    "No existe una ejecución Silver exitosa para "
                    "el catálogo geográfico."
                )
            run_id = int(run["run_id"])
            loaded_rows = int(run["loaded_rows"])
            if loaded_rows != GEOGRAPHY_EXPECTED["geography_loaded_rows"]:
                raise RuntimeError(
                    "La ejecución Silver geográfica no cumple el volumen "
                    f"esperado: run_id={run_id}, loaded_rows={loaded_rows}, "
                    f"esperado={GEOGRAPHY_EXPECTED['geography_loaded_rows']}."
                )
            logger.info(
                "Ejecución Silver geográfica seleccionada | run_id=%s | filas=%s",
                run_id,
                loaded_rows,
            )
            return run_id
    finally:
        engine.dispose()


def select_source_runs() -> dict[str, int]:
    """Selecciona una única versión Silver para cada fuente."""

    engine = create_gold_engine()

    try:
        with engine.connect() as conn:
            database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if database != DATABASE_ALLOWED:
                raise RuntimeError(
                    f"Pipeline bloqueado: base detectada {database}."
                )

            runs = {}

            for name, config in SOURCE_CONFIG.items():
                runs[name] = select_latest_silver_run(
                    conn,
                    name,
                    config,
                )

            return runs

    finally:
        engine.dispose()


def execute_stage(
    name: str,
    module: str,
    arguments: list[str],
) -> None:
    """Ejecuta una etapa como módulo Python."""

    command = [
        sys.executable,
        "-m",
        module,
        *arguments,
    ]

    logger.info("Iniciando etapa: %s", name)
    logger.info("Módulo: %s", module)

    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline detenido: etapa '{name}' terminó "
            f"con código {result.returncode}."
        )

    logger.info("Etapa completada: %s", name)


def ensure_integrated_mappings() -> None:
    """Registra y valida los 15 mapeos PLACES y 11 BRFSS del esquema Gold."""
    engine = create_gold_engine()
    try:
        with engine.begin() as conn:
            database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()
            if database != DATABASE_ALLOWED:
                raise RuntimeError(
                    f"Registro de mapeos bloqueado: base detectada {database}."
                )

            result = conn.execute(
                text(
                    "SELECT places_mappings, brfss_mappings "
                    "FROM gold.ensure_integrated_indicator_source_bridge()"
                )
            ).mappings().one()

            places_count = int(result["places_mappings"])
            brfss_count = int(result["brfss_mappings"])
            if (places_count, brfss_count) != (15, 11):
                raise RuntimeError(
                    "Cantidades inesperadas de mapeos: "
                    f"PLACES={places_count}/15, BRFSS={brfss_count}/11."
                )
            logger.info(
                "Bridge integrado validado | PLACES=%s | BRFSS=%s",
                places_count,
                brfss_count,
            )
    finally:
        engine.dispose()


def stage_is_complete(name: str, run_id: int) -> bool:
    """Comprueba el estado de una etapa sin modificar la base de datos."""

    engine = create_gold_engine()

    try:
        with engine.connect() as conn:
            database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if database != DATABASE_ALLOWED:
                raise RuntimeError(
                    f"Base de datos no autorizada: {database}."
                )

            return inspect_gold_stage(conn, name, run_id)

    finally:
        engine.dispose()


def run_stage_with_resume(
    name: str,
    module: str,
    run_id: int,
    arguments: list[str],
) -> None:
    """Omite una etapa completa o ejecuta una etapa vacía."""

    logger.info("=" * 65)
    logger.info("Evaluando etapa: %s | Silver run_id=%s", name, run_id)

    if stage_is_complete(name, run_id):
        logger.info(
            "Etapa omitida: %s ya está completa y validada.",
            name,
        )
        return

    logger.info("Etapa pendiente: %s. Se iniciará su ejecución.", name)

    execute_stage(name, module, arguments)

    # No se considera completada hasta validar los resultados persistidos.
    if not stage_is_complete(name, run_id):
        raise RuntimeError(
            f"La etapa {name} terminó sin producir los registros esperados."
        )

    logger.info("Etapa validada correctamente: %s", name)


def rebuild_gold_for_runs(runs: dict[str, int]) -> None:
    """
    Elimina los resultados Gold de las versiones Silver seleccionadas.

    Preserva catálogos, dimensiones compartidas y versiones históricas
    correspondientes a otros source_run_id.
    """
    engine = create_gold_engine()

    deletions = [
        ("fact_health_integrated", runs["PLACES"]),
        ("fact_area_health", runs["PLACES"]),
        ("fact_brfss_response", runs["BRFSS"]),
        ("fact_acs_county_indicator", runs["ACS County"]),
        ("fact_acs_indicator", runs["ACS nacional"]),
        ("dim_acs_indicator", runs["ACS nacional"]),
    ]

    try:
        with engine.begin() as conn:
            database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if database != DATABASE_ALLOWED:
                raise RuntimeError(
                    f"Reconstrucci?n cancelada. "
                    f"Base no autorizada: {database}."
                )

            logger.warning(
                "Iniciando reconstrucci?n Gold selectiva. "
                "Versiones Silver: %s",
                runs,
            )

            for table, run_id in deletions:
                result = conn.execute(
                    text(
                        f"DELETE FROM gold.{table} "
                        "WHERE source_run_id = :run_id"
                    ),
                    {"run_id": run_id},
                )

                logger.info(
                    "Reconstrucci?n: %s | run_id=%s | eliminados=%s",
                    table,
                    run_id,
                    result.rowcount,
                )

        logger.info(
            "Limpieza Gold confirmada. "
            "Los catálogos y otras versiones históricas se preservaron."
        )

    finally:
        engine.dispose()



@contextmanager
def acquire_global_pipeline_lock():
    """Mantiene un advisory lock PostgreSQL durante todo el pipeline."""
    engine = create_gold_engine()
    conn = None
    acquired = False

    try:
        conn = engine.connect()
        database = conn.execute(
            text("SELECT current_database()")
        ).scalar_one()

        if database != DATABASE_ALLOWED:
            raise RuntimeError(
                f"Pipeline bloqueado: base detectada {database}."
            )

        acquired = bool(
            conn.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": GOLD_PIPELINE_LOCK_KEY},
            ).scalar_one()
        )
        conn.commit()

        if not acquired:
            raise RuntimeError(
                "Ya existe otra instancia del pipeline Gold en ejecución. "
                "No se iniciará una ejecución concurrente."
            )

        logger.info("Bloqueo global Gold adquirido.")
        yield

    finally:
        if conn is not None:
            if acquired:
                try:
                    conn.execute(
                        text("SELECT pg_advisory_unlock(:lock_key)"),
                        {"lock_key": GOLD_PIPELINE_LOCK_KEY},
                    )
                    conn.commit()
                    logger.info("Bloqueo global Gold liberado.")
                except Exception:
                    logger.exception(
                        "No fue posible liberar explícitamente el bloqueo Gold; "
                        "se invalidará la conexión para liberar el lock de sesión."
                    )
                    conn.invalidate()
            conn.close()
        engine.dispose()


def validate_geography_preflight() -> int:
    """Valida Silver/Gold Geography sin insertar, actualizar ni borrar datos."""
    engine = create_gold_engine()

    try:
        with engine.connect() as conn:
            database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()
            if database != DATABASE_ALLOWED:
                raise RuntimeError(
                    f"Preflight geográfico bloqueado: base detectada {database}."
                )

            run = conn.execute(
                text("""
                    SELECT run_id, loaded_rows
                    FROM public.etl_runs
                    WHERE dataset_name = :dataset_name
                      AND target_table = :target_table
                      AND status = 'succeeded'
                      AND finished_at IS NOT NULL
                    ORDER BY finished_at DESC, run_id DESC
                    LIMIT 1
                """),
                {
                    "dataset_name": GEOGRAPHY_DATASET,
                    "target_table": GEOGRAPHY_TARGET,
                },
            ).mappings().one_or_none()

            if run is None:
                raise RuntimeError(
                    "No existe una ejecución Silver exitosa para geography_catalog."
                )

            geography_run_id = int(run["run_id"])
            loaded_rows = int(run["loaded_rows"])
            if loaded_rows != GEOGRAPHY_EXPECTED["geography_loaded_rows"]:
                raise RuntimeError(
                    "La ejecución Silver de geografía no coincide con el "
                    f"resultado validado: run_id={geography_run_id}, "
                    f"loaded_rows={loaded_rows}, esperado="
                    f"{GEOGRAPHY_EXPECTED['geography_loaded_rows']}."
                )

            counts = conn.execute(
                text("""
                    SELECT
                        (SELECT COUNT(*) FROM public.silver_geography
                         WHERE geography_level = 'STATE') AS silver_states,
                        (SELECT COUNT(*) FROM public.silver_geography
                         WHERE geography_level = 'COUNTY') AS silver_counties,
                        (SELECT COUNT(*) FROM public.silver_county) AS silver_county_catalog,
                        (SELECT COUNT(*) FROM gold.dim_geography
                         WHERE geography_level = 'STATE') AS gold_states,
                        (SELECT COUNT(*) FROM gold.dim_geography
                         WHERE geography_level = 'COUNTY') AS gold_counties
                """)
            ).mappings().one()

            actual = {key: int(value) for key, value in counts.items()}
            expected_keys = (
                "silver_states", "silver_counties", "silver_county_catalog",
                "gold_states", "gold_counties",
            )
            mismatched_counts = {
                key: {"actual": actual[key], "expected": GEOGRAPHY_EXPECTED[key]}
                for key in expected_keys
                if actual[key] != GEOGRAPHY_EXPECTED[key]
            }
            if mismatched_counts:
                raise RuntimeError(
                    f"Conteos geográficos inconsistentes: {mismatched_counts}"
                )

            silver_state_mismatches = count_rows(
                conn,
                """
                SELECT COUNT(*)
                FROM public.silver_geography sg
                LEFT JOIN gold.dim_geography gg
                  ON gg.geography_level = 'STATE'
                 AND gg.location_id = sg.geography_code
                WHERE sg.geography_level = 'STATE'
                  AND (
                    gg.geography_key IS NULL
                    OR gg.state_fips IS DISTINCT FROM sg.geography_code
                    OR gg.location_name IS DISTINCT FROM sg.geography_name
                    OR gg.country_code IS DISTINCT FROM 'US'
                    OR gg.parent_geography_key IS NOT NULL
                  )
                """,
                {},
            )

            county_mismatches = count_rows(
                conn,
                """
                SELECT COUNT(*)
                FROM public.silver_geography sg
                LEFT JOIN public.silver_county sc
                    ON sc.geography_id = sg.geography_id
                LEFT JOIN public.silver_geography sparent
                  ON sparent.geography_id = sg.parent_geography_id
                LEFT JOIN gold.dim_geography gg
                  ON gg.geography_level = 'COUNTY'
                 AND gg.location_id = sg.geography_code
                LEFT JOIN gold.dim_geography gparent
                  ON gparent.geography_key = gg.parent_geography_key
                WHERE sg.geography_level = 'COUNTY'
                  AND (
                    sparent.geography_id IS NULL
                    OR gg.geography_key IS NULL
                    OR sc.geography_id IS NULL
                    OR gg.state_fips IS DISTINCT FROM sc.state_fips
                    OR gg.county_fips IS DISTINCT FROM sc.county_fips
                    OR gg.state_abbr IS DISTINCT FROM sc.state_abbr
                    OR gg.location_name IS DISTINCT FROM sg.geography_name
                    OR gg.country_code IS DISTINCT FROM 'US'
                    OR gparent.geography_key IS NULL
                    OR gparent.geography_level <> 'STATE'
                    OR gparent.location_id IS DISTINCT FROM sparent.geography_code
                    OR sg.geography_code IS DISTINCT FROM
                    (sc.state_fips || sc.county_fips)
                    OR sc.state_fips IS DISTINCT FROM
                    sparent.geography_code
                    OR gg.location_id IS DISTINCT FROM
                    sg.geography_code
                  )
                """,
                {},
            )

            extra_jurisdictions = count_rows(
                conn,
                """
                WITH expected AS (
                    SELECT *
                    FROM (VALUES
                        ('66', 'Guam'),
                        ('72', 'Puerto Rico'),
                        ('78', 'U.S. Virgin Islands')
                    ) AS v(state_fips, location_name)
                )
                SELECT COUNT(*)
                FROM expected e
                LEFT JOIN gold.dim_geography g
                ON g.geography_level = 'STATE'
                AND g.state_fips = e.state_fips
                WHERE g.geography_key IS NULL
                OR g.location_id IS DISTINCT FROM e.state_fips
                OR g.location_name IS DISTINCT FROM e.location_name
                OR g.country_code IS DISTINCT FROM 'US'
                """,
                {},
            )

            if extra_jurisdictions != 0:
                raise RuntimeError(
                    "El catálogo Gold no contiene correctamente "
                    "las tres jurisdicciones adicionales."
                )

            silver_county_orphans = count_rows(
                conn,
                """
                SELECT COUNT(*)
                FROM public.silver_county sc
                LEFT JOIN public.silver_geography sg
                  ON sg.geography_id = sc.geography_id
                 AND sg.geography_level = 'COUNTY'
                WHERE sg.geography_id IS NULL
                """,
                {},
            )

            if any((silver_state_mismatches, county_mismatches, silver_county_orphans)):
                raise RuntimeError(
                    "La geografía Gold no coincide con el catálogo Silver: "
                    f"estados_inconsistentes={silver_state_mismatches}, "
                    f"condados_inconsistentes={county_mismatches}, "
                    f"registros_silver_county_sin_geografia={silver_county_orphans}."
                )

            logger.info(
                "Preflight geográfico aprobado | geography_run_id=%s | "
                "Silver: 49 estados/83 condados | Gold: 52 jurisdicciones/83 condados | "
                "sin diferencias.",
                geography_run_id,
            )
            return geography_run_id

    finally:
        engine.dispose()


def run_preflight_only() -> None:
    """Ejecuta únicamente validaciones de solo lectura; nunca inicia ETL."""
    with acquire_global_pipeline_lock():
        geography_run_id = validate_geography_preflight()
        logger.info(
            "Preflight geográfico aprobado | geography_run_id=%s",
            geography_run_id,
        )

        runs = select_source_runs()
        logger.info("Ejecuciones Silver seleccionadas: %s", runs)

        for name, run_id in runs.items():
            is_complete = stage_is_complete(name, run_id)
            logger.info(
                "Estado Gold | etapa=%s | silver_run_id=%s | %s",
                name,
                run_id,
                "completa y validada" if is_complete else "pendiente",
            )

        integrated_complete = stage_is_complete(
            "Integración PLACES", runs["PLACES"]
        )
        logger.info(
            "Estado Gold | etapa=Integración PLACES | silver_run_id=%s | %s",
            runs["PLACES"],
            "completa y validada" if integrated_complete else "pendiente",
        )
        logger.info(
            "Preflight-only finalizado. No se ejecutaron ETL ni operaciones DML."
        )


def run_pipeline(rebuild: bool = False) -> None:
    """Ejecuta Gold en orden controlado, bajo bloqueo global y con preflight."""
    with acquire_global_pipeline_lock():
        # Gold necesita primero los 83 condados procedentes de Silver.
        # El run_id se obtiene de etl_runs; nunca queda fijado en el código.
        geography_run_id = select_latest_geography_run()
        execute_stage(
            name="Geografía",
            module="scripts.gold.etl_gold_geography",
            arguments=["--source-run-id", str(geography_run_id)],
        )

        validated_geography_run_id = validate_geography_preflight()
        if validated_geography_run_id != geography_run_id:
            raise RuntimeError(
                "El preflight validó una ejecución geográfica distinta de la "
                "seleccionada para la carga: "
                f"seleccionada={geography_run_id}, "
                f"validada={validated_geography_run_id}."
            )
        logger.info(
            "Dependencia geográfica cargada y validada; "
            "Silver geography run_id=%s.",
            validated_geography_run_id,
        )

        runs = select_source_runs()
        logger.info("Versiones Silver seleccionadas: %s", runs)

        # El preflight se realiza antes de cualquier operación destructiva.
        if rebuild:
            rebuild_gold_for_runs(runs)

        execution_order = (
            "PLACES",
            "BRFSS",
            "ACS nacional",
            "ACS County",
        )

        for name in execution_order:
            config = SOURCE_CONFIG[name]
            run_stage_with_resume(
                name=name,
                module=config["module"],
                run_id=runs[name],
                arguments=[
                    "--source-run-id",
                    str(runs[name]),
                    "--orchestrated",
                ] if name in ("PLACES", "ACS nacional") else [
                    "--source-run-id",
                    str(runs[name]),
                ],
            )

        # Registrar mapeos una vez cargadas las dimensiones fuente.
        ensure_integrated_mappings()

        run_stage_with_resume(
            name="Integración PLACES",
            module="scripts.gold.etl_gold_integrated",
            run_id=runs["PLACES"],
            arguments=["--places-run-id", str(runs["PLACES"])],
        )

        logger.info(
            "Pipeline Gold completado. Geografía validada y todas las etapas verificadas."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Ejecuta los ETL Gold en orden controlado, con preflight geográfico "
            "y bloqueo global de concurrencia."
        )
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Ejecuta validaciones geográficas, selecciona ejecuciones Silver "
            "e inspecciona Gold sin ejecutar ETL ni modificar datos."
        ),
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help=(
            "Reconstruye selectivamente las versiones Gold seleccionadas. "
            "Valida primero la geografía y preserva catálogos y versiones históricas."
        ),
    )

    args = parser.parse_args()

    if args.preflight_only and args.rebuild:
        parser.error("--preflight-only y --rebuild son opciones incompatibles.")

    try:
        if args.preflight_only:
            run_preflight_only()
        else:
            run_pipeline(rebuild=args.rebuild)
        return 0
    except Exception:
        logger.exception("Falló el pipeline Gold.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())