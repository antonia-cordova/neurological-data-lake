from __future__ import annotations
import argparse
import logging
import subprocess
import sys
from pathlib import Path
from sqlalchemy import text
from scripts.etl_gold_places import create_db_engine

ROOT = Path(__file__).resolve().parents[1]

DATABASE_ALLOWED = "health_datalake"

SOURCE_CONFIG = {
    "PLACES": {
        "dataset": "places_2025",
        "table": "silver_places_2023",
        "expected_rows": 2490,
        "module": "scripts.etl_gold_places",
    },
    "BRFSS": {
        "dataset": "brfss_2023",
        "table": "silver_brfss_2023",
        "expected_rows": 2800,
        "module": "scripts.etl_gold_brfss",
    },
    "ACS nacional": {
        "dataset": "acs_2024",
        "table": "silver_acs_2024",
        "expected_rows": 113,
        "module": "scripts.etl_gold_acs",
    },
    "ACS County": {
        "dataset": "acs_2024_county_dp05",
        "table": "silver_acs_county_2024",
        "expected_rows": 332,
        "module": "scripts.etl_gold_acs_county",
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
    "IntegraciÃ³n PLACES": {
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
    Devuelve True si la etapa estÃ¡ completa y validada.
    Devuelve False si estÃ¡ vacÃ­a.
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

    elif name == "IntegraciÃ³n PLACES":
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
        # VALIDACIÃ“N DE EQUIVALENCIA BIDIRECCIONAL PLACES - INTEGRACIÃ“N
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
            f"duplicados={duplicates}, invÃ¡lidos={invalid}."
        )

    return True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("run_gold_pipeline")


def select_latest_silver_run(conn, name: str, config: dict) -> int:
    """Selecciona la Ãºltima ejecuciÃ³n Silver exitosa y valida su registro."""

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
            f"No existe una ejecuciÃ³n Silver exitosa para {name}."
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


def select_source_runs() -> dict[str, int]:
    """Selecciona una Ãºnica versiÃ³n Silver para cada fuente."""

    engine = create_db_engine()

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
    """Ejecuta una etapa como mÃ³dulo Python."""

    command = [
        sys.executable,
        "-m",
        module,
        *arguments,
    ]

    logger.info("Iniciando etapa: %s", name)
    logger.info("MÃ³dulo: %s", module)

    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline detenido: etapa '{name}' terminÃ³ "
            f"con cÃ³digo {result.returncode}."
        )

    logger.info("Etapa completada: %s", name)


def stage_is_complete(name: str, run_id: int) -> bool:
    """Comprueba el estado de una etapa sin modificar la base de datos."""

    engine = create_db_engine()

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
    """Omite una etapa completa o ejecuta una etapa vacÃ­a."""

    logger.info("=" * 65)
    logger.info("Evaluando etapa: %s | Silver run_id=%s", name, run_id)

    if stage_is_complete(name, run_id):
        logger.info(
            "Etapa omitida: %s ya estÃ¡ completa y validada.",
            name,
        )
        return

    logger.info("Etapa pendiente: %s. Se iniciarÃ¡ su ejecuciÃ³n.", name)

    execute_stage(name, module, arguments)

    # No se considera completada hasta validar los resultados persistidos.
    if not stage_is_complete(name, run_id):
        raise RuntimeError(
            f"La etapa {name} terminÃ³ sin producir los registros esperados."
        )

    logger.info("Etapa validada correctamente: %s", name)


def run_pipeline() -> None:
    """Ejecuta o reanuda Gold con versiones Silver seleccionadas una sola vez."""

    runs = select_source_runs()

    logger.info("Versiones Silver seleccionadas: %s", runs)

    for name, config in SOURCE_CONFIG.items():
        run_stage_with_resume(
            name=name,
            module=config["module"],
            run_id=runs[name],
            arguments=[
                "--source-run-id",
                str(runs[name]),
            ],
        )

    run_stage_with_resume(
        name="IntegraciÃ³n PLACES",
        module="scripts.etl_gold_integrated",
        run_id=runs["PLACES"],
        arguments=[
            "--places-run-id",
            str(runs["PLACES"]),
        ],
    )

    logger.info(
        "Pipeline Gold completado y todas las etapas validadas."
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Ejecuta los ETL Gold con versiones Silver "
            "seleccionadas de forma centralizada."
        )
    )
    parser.parse_args()

    try:
        run_pipeline()
        return 0

    except Exception:
        logger.exception("FallÃ³ el pipeline Gold.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())