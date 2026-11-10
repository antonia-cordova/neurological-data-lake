from __future__ import annotations
import argparse
import logging
import os
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Connection, Engine

from scripts.gold.silver_run_validator import resolve_single_silver_run_id

ROOT = Path(__file__).resolve().parents[1]

SOURCE_CODE = "PLACES"
SOURCE_TABLE = "public.silver_places_2023"
DATASET_NAME = "places_2025"
EXPECTED_SILVER_ROWS = 2490
EXPECTED_BASE_ROWS = 2490
EXPECTED_INTEGRATED_ROWS = 2490
EXPECTED_MAPPING_ROWS = 15

# Mapeo explÃ­cito de las medidas PLACES a la dimensiÃ³n integrada.
# MHLTH y PHLTH son relaciones parciales: miden dimensiones especÃ­ficas,
# no la totalidad de salud mental o salud fÃ­sica, respectivamente.
PLACES_INDICATOR_MAPPING: tuple[dict[str, str], ...] = (
    {
        "measure_id": "ARTHRITIS",
        "indicator_code": "ARTHRITIS",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa. La comparaciÃ³n requiere controlar aÃ±o, poblaciÃ³n, geografÃ­a y tipo de prevalencia.",
    },
    {
        "measure_id": "CHD",
        "indicator_code": "CORONARY_HEART_DISEASE",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con enfermedad coronaria. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "COGNITION",
        "indicator_code": "COGNITION",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con discapacidad cognitiva. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "DEPRESSION",
        "indicator_code": "DEPRESSION",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con depresiÃ³n. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "DIABETES",
        "indicator_code": "DIABETES",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con diabetes diagnosticada. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "DISABILITY",
        "indicator_code": "DISABILITY",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con discapacidad. Verificar definiciÃ³n operacional, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "GHLTH",
        "indicator_code": "GENERAL_HEALTH",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con salud general autopercibida. Verificar categorÃ­as, metodologÃ­a, poblaciÃ³n y aÃ±o.",
    },
    {
        "measure_id": "HEARING",
        "indicator_code": "HEARING",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con discapacidad auditiva. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "INDEPLIVE",
        "indicator_code": "INDEPLIVE",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con discapacidad para la vida independiente. Verificar definiciÃ³n operacional y poblaciÃ³n.",
    },
    {
        "measure_id": "MHLTH",
        "indicator_code": "MENTAL_HEALTH",
        "relation_type": "PARTIAL",
        "comparability": "NOT_DIRECTLY_COMPARABLE",
        "mapping_notes": "RelaciÃ³n parcial: PLACES mide malestar mental frecuente, no la totalidad del constructo de salud mental.",
    },
    {
        "measure_id": "MOBILITY",
        "indicator_code": "MOBILITY",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con discapacidad de movilidad. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "PHLTH",
        "indicator_code": "PHYSICAL_HEALTH",
        "relation_type": "PARTIAL",
        "comparability": "NOT_DIRECTLY_COMPARABLE",
        "mapping_notes": "RelaciÃ³n parcial: PLACES mide malestar fÃ­sico frecuente, no la totalidad del constructo de salud fÃ­sica.",
    },
    {
        "measure_id": "SELFCARE",
        "indicator_code": "SELFCARE",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con discapacidad para el autocuidado. Verificar definiciÃ³n operacional y poblaciÃ³n.",
    },
    {
        "measure_id": "STROKE",
        "indicator_code": "STROKE",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con accidente cerebrovascular. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
    {
        "measure_id": "VISION",
        "indicator_code": "VISION",
        "relation_type": "DIRECT",
        "comparability": "COMPARABLE_WITH_CAVEATS",
        "mapping_notes": "Correspondencia directa con discapacidad visual. Verificar metodologÃ­a, poblaciÃ³n, aÃ±o y tipo de prevalencia.",
    },
)

MAPPING_VALUES_SQL = ",\n".join(
    f"(:measure_id_{i}, :indicator_code_{i}, :relation_type_{i}, "
    f":comparability_{i}, :mapping_notes_{i})"
    for i in range(len(PLACES_INDICATOR_MAPPING))
)

MAPPING_PARAMS: dict[str, str] = {
    f"{field}_{i}": mapping[field]
    for i, mapping in enumerate(PLACES_INDICATOR_MAPPING)
    for field in (
        "measure_id",
        "indicator_code",
        "relation_type",
        "comparability",
        "mapping_notes",
    )
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("etl_gold_integrated")


def create_db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    database = os.getenv("POSTGRES_DB")
    if database != "health_datalake":
        raise RuntimeError(
            "ETL integrado bloqueado: solo se permite health_datalake."
        )

    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Faltan variables: {', '.join(missing)}")

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=database,
    )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )

    try:
        with engine.connect() as conn:
            actual_database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if actual_database != "health_datalake":
                raise RuntimeError(
                    f"ConexiÃ³n rechazada: base detectada {actual_database}."
                )
    except Exception:
        engine.dispose()
        raise

    logger.info("ConexiÃ³n PostgreSQL validada: %s", database)
    return engine


def resolve_and_validate_places_run(
    conn: Connection,
    requested_run_id: int | None = None,
) -> tuple[int, int]:
    run_id = resolve_single_silver_run_id(
        conn,
        SOURCE_TABLE,
        requested_run_id=requested_run_id,
        expected_rows=EXPECTED_SILVER_ROWS,
        dataset_name=DATASET_NAME,
        target_table="silver_places_2023",
    )

    run = conn.execute(
        text(
            """
            SELECT dataset_name, target_table, loaded_rows, status, finished_at
            FROM public.etl_runs
            WHERE run_id = :run_id
            """
        ),
        {"run_id": run_id},
    ).mappings().one_or_none()

    if run is None:
        raise ValueError(f"No existe etl_runs.run_id={run_id} para PLACES.")

    if (
        run["dataset_name"] != DATASET_NAME
        or run["target_table"] != "silver_places_2023"
        or run["status"] != "succeeded"
        or run["finished_at"] is None
        or run["loaded_rows"] != EXPECTED_SILVER_ROWS
    ):
        raise ValueError(
            f"EjecuciÃ³n Silver PLACES invÃ¡lida: run_id={run_id}."
        )

    source_key = conn.execute(
        text(
            """
            SELECT source_key
            FROM gold.dim_source
            WHERE source_code = :source_code
              AND dataset_name = :dataset_name
            """
        ),
        {"source_code": SOURCE_CODE, "dataset_name": DATASET_NAME},
    ).scalar_one_or_none()

    if source_key is None:
        raise ValueError(
            "No existe PLACES/places_2025 en gold.dim_source."
        )

    return run_id, int(source_key)


def validate_base_places_facts(
    conn: Connection,
    run_id: int,
    source_key: int,
) -> None:
    base_count = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM gold.fact_area_health
            WHERE source_run_id = :run_id
              AND source_key = :source_key
            """
        ),
        {"run_id": run_id, "source_key": source_key},
    ).scalar_one()

    if base_count != EXPECTED_BASE_ROWS:
        raise ValueError(
            "Gold PLACES incompleto para "
            f"run_id={run_id}: {base_count}; "
            f"esperado={EXPECTED_BASE_ROWS}."
        )

    base_validation = conn.execute(
    text("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT fact.geography_key) AS geographies,
            COUNT(DISTINCT fact.health_measure_key) AS measures,
            COUNT(DISTINCT fact.prevalence_type_key) AS prevalence_types,

            COUNT(*) FILTER (
                WHERE fact.data_value IS NULL
                   OR fact.data_value < 0
                   OR fact.data_value > 100
            ) AS invalid_values,

            COUNT(*) FILTER (
                WHERE geography.geography_key IS NULL
                   OR measure.health_measure_key IS NULL
                   OR prevalence.prevalence_type_key IS NULL
            ) AS invalid_dimensions

        FROM gold.fact_area_health fact

        LEFT JOIN gold.dim_geography geography
            ON geography.geography_key = fact.geography_key

        LEFT JOIN gold.dim_health_measure measure
            ON measure.health_measure_key = fact.health_measure_key

        LEFT JOIN gold.dim_prevalence_type prevalence
            ON prevalence.prevalence_type_key = fact.prevalence_type_key

        WHERE fact.source_run_id = :run_id
          AND fact.source_key = :source_key
    """),
    {
        "run_id": run_id,
        "source_key": source_key,
    },
).mappings().one()

    expected_base = {
        "total_rows": 2490,
        "geographies": 83,
        "measures": 15,
        "prevalence_types": 2,
        "invalid_values": 0,
        "invalid_dimensions": 0,
    }

    if any(
        base_validation[key] != expected
        for key, expected in expected_base.items()
    ):
        raise ValueError(
            "ValidaciÃ³n estructural de hechos PLACES incorrecta: "
            f"{dict(base_validation)}"
        )
    
    measure_count = conn.execute(
        text(
            """
            SELECT COUNT(DISTINCT measure_id)
            FROM public.silver_places_2023
            """
        )
    ).scalar_one()

    if measure_count != EXPECTED_MAPPING_ROWS:
        raise ValueError(
            "Cantidad inesperada de medidas PLACES en Silver: "
            f"{measure_count}; esperado={EXPECTED_MAPPING_ROWS}."
        )

    gold_measure_count = conn.execute(
        text("SELECT COUNT(*) FROM gold.dim_health_measure")
    ).scalar_one()

    if gold_measure_count != EXPECTED_MAPPING_ROWS:
        raise ValueError(
            "gold.dim_health_measure no estÃ¡ completa: "
            f"{gold_measure_count}; esperado={EXPECTED_MAPPING_ROWS}. "
            "Ejecute y valide primero etl_gold_places.py."
        )


def ensure_places_mappings(conn: Connection, source_key: int) -> None:
    mapping_source = f"""
        SELECT
            v.measure_id,
            v.indicator_code,
            v.relation_type,
            v.comparability,
            v.mapping_notes,
            hm.health_measure_key,
            ii.indicator_key
        FROM (VALUES {MAPPING_VALUES_SQL}) AS v(
            measure_id,
            indicator_code,
            relation_type,
            comparability,
            mapping_notes
        )
        JOIN gold.dim_health_measure hm
          ON hm.measure_id = v.measure_id
        JOIN gold.dim_health_integrated_indicator ii
          ON ii.indicator_code = v.indicator_code
    """

    resolved_count = conn.execute(
        text(f"SELECT COUNT(*) FROM ({mapping_source}) resolved"),
        MAPPING_PARAMS,
    ).scalar_one()

    if resolved_count != EXPECTED_MAPPING_ROWS:
        raise ValueError(
            "No se pudieron resolver todas las correspondencias PLACES: "
            f"{resolved_count}/{EXPECTED_MAPPING_ROWS}. "
            "Revise dim_health_measure y dim_health_integrated_indicator."
        )

    # Inserta solo las correspondencias ausentes. Las existentes se validan
    # posteriormente; no se sobrescriben silenciosamente.
    conn.execute(
        text(
            f"""
            INSERT INTO gold.bridge_health_indicator_source (
                indicator_key,
                source_key,
                health_measure_key,
                variable_key,
                relation_type,
                comparability,
                mapping_notes
            )
            SELECT
                resolved.indicator_key,
                :source_key,
                resolved.health_measure_key,
                NULL,
                resolved.relation_type,
                resolved.comparability,
                resolved.mapping_notes
            FROM ({mapping_source}) resolved
            WHERE NOT EXISTS (
                SELECT 1
                FROM gold.bridge_health_indicator_source existing
                WHERE existing.indicator_key = resolved.indicator_key
                  AND existing.source_key = :source_key
                  AND existing.health_measure_key = resolved.health_measure_key
            )
            """
        ),
        {**MAPPING_PARAMS, "source_key": source_key},
    )

    # Deben existir exactamente los 15 mapeos previstos y sus atributos
    # deben coincidir con la definiciÃ³n versionada en este ETL.
    mismatches = conn.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM ({mapping_source}) resolved
            LEFT JOIN gold.bridge_health_indicator_source existing
              ON existing.indicator_key = resolved.indicator_key
             AND existing.source_key = :source_key
             AND existing.health_measure_key = resolved.health_measure_key
            WHERE existing.mapping_key IS NULL
               OR existing.variable_key IS NOT NULL
               OR existing.relation_type <> resolved.relation_type
               OR existing.comparability <> resolved.comparability
               OR existing.mapping_notes IS DISTINCT FROM resolved.mapping_notes
            """
        ),
        {**MAPPING_PARAMS, "source_key": source_key},
    ).scalar_one()

    total_places_mappings = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM gold.bridge_health_indicator_source
            WHERE source_key = :source_key
              AND health_measure_key IS NOT NULL
              AND variable_key IS NULL
            """
        ),
        {"source_key": source_key},
    ).scalar_one()

    if mismatches != 0 or total_places_mappings != EXPECTED_MAPPING_ROWS:
        raise ValueError(
            "El bridge PLACES no coincide con el mapeo esperado: "
            f"discrepancias={mismatches}, "
            f"total={total_places_mappings}, "
            f"esperado={EXPECTED_MAPPING_ROWS}."
        )

    logger.info(
        "Bridge PLACES validado | source_key=%s | mappings=%s",
        source_key,
        total_places_mappings,
    )


def load_integrated_places_facts(
    conn: Connection,
    run_id: int,
    source_key: int,
) -> int:
    existing = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM gold.fact_health_integrated
            WHERE source_run_id = :run_id
              AND source_key = :source_key
            """
        ),
        {"run_id": run_id, "source_key": source_key},
    ).scalar_one()

    if existing:
        raise ValueError(
            "Gold integrado ya contiene "
            f"{existing} filas para PLACES run_id={run_id}; "
            "se rechaza reprocesar la misma versiÃ³n."
        )

    conn.execute(
        text(
            """
            INSERT INTO gold.fact_health_integrated (
                indicator_key,
                geography_key,
                source_key,
                source_run_id,
                reference_year,
                geography_level,
                health_measure_key,
                variable_key,
                value_numeric,
                value_raw,
                unit,
                prevalence_type_key,
                sample_size,
                numerator,
                denominator,
                confidence_interval_lower,
                confidence_interval_upper,
                methodology
            )
            SELECT
                bridge.indicator_key,
                fact.geography_key,
                fact.source_key,
                fact.source_run_id,
                NULL::smallint,
                geography.geography_level,
                fact.health_measure_key,
                NULL,
                fact.data_value,
                NULL,
                'PERCENTAGE',
                fact.prevalence_type_key,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                'CDC PLACES: estimaciÃ³n territorial copiada sin recalcular '
                'desde gold.fact_area_health. El valor corresponde a '
                'prevalencia; la interpretaciÃ³n debe considerar el tipo '
                'de prevalencia y la metodologÃ­a de origen.'
            FROM gold.fact_area_health fact
            JOIN gold.dim_health_measure measure
              ON measure.health_measure_key = fact.health_measure_key
            JOIN gold.bridge_health_indicator_source bridge
              ON bridge.source_key = fact.source_key
             AND bridge.health_measure_key = fact.health_measure_key
             AND bridge.variable_key IS NULL
            JOIN gold.dim_geography geography
              ON geography.geography_key = fact.geography_key
            WHERE fact.source_run_id = :run_id
              AND fact.source_key = :source_key
            """
        ),
        {"run_id": run_id, "source_key": source_key},
    )

    persisted = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM gold.fact_health_integrated
            WHERE source_run_id = :run_id
              AND source_key = :source_key
            """
        ),
        {"run_id": run_id, "source_key": source_key},
    ).scalar_one()

    if persisted != EXPECTED_INTEGRATED_ROWS:
        raise RuntimeError(
            "IntegraciÃ³n PLACES incompleta: "
            f"{persisted}; esperado={EXPECTED_INTEGRATED_ROWS}."
        )

    # ValidaciÃ³n adicional del grano: cada fila integrada debe tener
    # indicador, medida, geografÃ­a y tipo de prevalencia.
    invalid_rows = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM gold.fact_health_integrated integrated
            LEFT JOIN gold.bridge_health_indicator_source bridge
              ON bridge.indicator_key = integrated.indicator_key
             AND bridge.source_key = integrated.source_key
             AND bridge.health_measure_key = integrated.health_measure_key
             AND bridge.variable_key IS NULL
            WHERE integrated.source_run_id = :run_id
              AND integrated.source_key = :source_key
              AND (
                    bridge.mapping_key IS NULL
                    OR integrated.value_numeric IS NULL
                    OR integrated.geography_level NOT IN ('COUNTY', 'STATE')
                    OR integrated.prevalence_type_key IS NULL
                  )
            """
        ),
        {"run_id": run_id, "source_key": source_key},
    ).scalar_one()

    if invalid_rows:
        raise RuntimeError(
            "Se detectaron hechos integrados PLACES invÃ¡lidos: "
            f"{invalid_rows}."
        )                            

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
                   AND bridge.health_measure_key = fact.health_measure_key
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
                (SELECT COUNT(*) FROM missing_integrated)
                    AS missing_integrated,
                (SELECT COUNT(*) FROM unexpected_integrated)
                    AS unexpected_integrated
        """),
        {"run_id": run_id, "source_key": source_key},
    ).mappings().one()

    if (
        equivalence["missing_integrated"] != 0
        or equivalence["unexpected_integrated"] != 0
    ):
        raise RuntimeError(
            "Los hechos integrados no coinciden con los hechos "
            "territoriales PLACES: "
            f"{dict(equivalence)}"
        )
    logger.info(
        "IntegraciÃ³n PLACES finalizada | source_run_id=%s | filas=%s",
        run_id,
        persisted,
    )
    return int(persisted)


def run_etl(places_run_id: int | None = None) -> None:
    engine = create_db_engine()

    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(741, 2027)"))

            run_id, source_key = resolve_and_validate_places_run(
                conn,
                places_run_id,
            )

            validate_base_places_facts(conn, run_id, source_key)
            ensure_places_mappings(conn, source_key)
            load_integrated_places_facts(conn, run_id, source_key)

            logger.info(
                "ETL integrado PLACES completado | run_id=%s | source_key=%s",
                run_id,
                source_key,
            )

    except Exception:
        logger.exception("FallÃ³ la integraciÃ³n Gold PLACES.")
        raise
    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Integra exclusivamente los hechos territoriales PLACES "
            "en gold.fact_health_integrated."
        )
    )
    parser.add_argument(
        "--places-run-id",
        type=int,
        default=None,
        help="run_id de Silver PLACES. Si se omite, se resuelve automÃ¡ticamente.",
    )

    args = parser.parse_args()

    try:
        run_etl(places_run_id=args.places_run_id)
        return 0
    except Exception:
        logger.exception("FallÃ³ la integraciÃ³n Gold PLACES.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())