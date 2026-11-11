from __future__ import annotations

import argparse
import logging
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

from scripts.gold.db_connection import create_gold_engine
from scripts.gold.silver_run_validator import resolve_single_silver_run_id
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]

SOURCE_TABLE = "silver_acs_county_2024"
DATASET_NAME = "acs_2024_county_dp05"
TARGET_TABLE = "gold.fact_acs_county_indicator"

EXPECTED_SOURCE_ROWS = 332
EXPECTED_GEOGRAPHIES = 83
EXPECTED_INDICATORS = 4
EXPECTED_FACT_ROWS = 332

VARIABLE_CODES = {
    "DP05_0001",
    "DP05_0002",
    "DP05_0003",
    "DP05_0018",
}

INSERT_BATCH_SIZE = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("etl_gold_acs_county")


def resolve_source_run_id(
    conn,
    requested_run_id: int | None = None,
) -> int:
    """Selecciona y valida una ejecución Silver de ACS County."""

    run_id = resolve_single_silver_run_id(
        conn,
        SOURCE_TABLE,
        requested_run_id=requested_run_id,
        expected_rows=EXPECTED_SOURCE_ROWS,
        dataset_name=DATASET_NAME,
        target_table=SOURCE_TABLE,
    )

    logger.info(
        "Run ID ACS County seleccionado y validado | run_id=%s",
        run_id,
    )

    return run_id


def validate_source_run(conn, source_run_id: int) -> dict:
    run = conn.execute(
        text("""
            SELECT
                run_id,
                dataset_name,
                target_table,
                source_rows,
                loaded_rows,
                status
            FROM public.etl_runs
            WHERE run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).mappings().one_or_none()

    if run is None:
        raise ValueError(
            f"No existe la ejecución run_id={source_run_id}."
        )

    if run["dataset_name"] != DATASET_NAME:
        raise ValueError(
            f"Dataset inesperado: {run['dataset_name']}."
        )

    if run["target_table"] != SOURCE_TABLE:
        raise ValueError(
            f"Tabla de origen inesperada: {run['target_table']}."
        )

    if run["status"] != "succeeded":
        raise ValueError(
            "La ejecución de Silver no terminó correctamente."
        )

    if run["loaded_rows"] != EXPECTED_SOURCE_ROWS:
        raise ValueError(
            f"Se esperaban {EXPECTED_SOURCE_ROWS} filas; "
            f"etl_runs informa {run['loaded_rows']}."
        )

    logger.info(
        "Ejecución Silver validada | run_id=%s | dataset=%s",
        source_run_id,
        run["dataset_name"],
    )

    return dict(run)


def parse_numeric(raw_value: str | None) -> Decimal | None:
    """
    Convierte valores ACS a Decimal.
    Los valores originales permanecen en las columnas raw.
    Los valores ausentes o códigos especiales no se convierten a cero.
    """
    if raw_value is None:
        return None

    value = str(raw_value).strip()

    if value == "":
        return None

    if value in {
        "*****",
        "(X)",
        "-555555555",
        "-666666666",
        "-888888888",
        "-999999999",
    }:
        return None

    # Acepta enteros/decimales y separadores de miles correctamente
    # agrupados. No elimina comas indiscriminadamente: "1,2" no es 12.
    numeric_pattern = r"-?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?"

    if not re.fullmatch(numeric_pattern, value):
        raise ValueError(
            f"Formato numérico ACS no reconocido: {raw_value!r}"
        )

    try:
        return Decimal(value.replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError(
            f"No se pudo convertir el valor: {raw_value!r}"
        ) from exc


def read_and_validate_silver(
    conn,
    source_run_id: int,
) -> list[dict]:

    conn.execute(text("""
        LOCK TABLE public.silver_acs_county_2024 IN SHARE MODE
    """))

    lineage = conn.execute(
        text("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(run_id) AS rows_with_run_id,
                COUNT(DISTINCT run_id) AS distinct_runs,
                MIN(run_id) AS min_run_id,
                MAX(run_id) AS max_run_id,
                COUNT(DISTINCT geography_id) AS geographies,
                COUNT(DISTINCT variable_code) AS variables
            FROM public.silver_acs_county_2024
            WHERE run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).mappings().one()

    if (
        lineage["total_rows"] != EXPECTED_SOURCE_ROWS
        or lineage["rows_with_run_id"] != EXPECTED_SOURCE_ROWS
        or lineage["distinct_runs"] != 1
        or lineage["min_run_id"] != source_run_id
        or lineage["max_run_id"] != source_run_id
        or lineage["geographies"] != EXPECTED_GEOGRAPHIES
        or lineage["variables"] != EXPECTED_INDICATORS
    ):
        raise ValueError(
            f"Trazabilidad o volumen Silver incorrecto: {dict(lineage)}"
        )

    records = conn.execute(
        text("""
            SELECT
                s.acs_county_id,
                s.state_fips,
                s.county_fips,
                s.geography_id,
                s.geography_name,
                s.variable_code,
                s.estimate,
                s.margin_of_error,
                s.percent,
                s.percent_margin_of_error,
                s.run_id,
                g.geography_key,
                st.geography_key AS parent_state_key
            FROM public.silver_acs_county_2024 s
            LEFT JOIN gold.dim_geography g
                ON g.location_id = s.geography_id
               AND g.geography_level = 'COUNTY'
               AND g.country_code = 'US'
               AND g.state_fips = s.state_fips
               AND g.county_fips = s.county_fips
            LEFT JOIN gold.dim_geography st
                ON st.geography_key = g.parent_geography_key
               AND st.geography_level = 'STATE'
               AND st.country_code = 'US'
               AND st.state_fips = s.state_fips
            WHERE s.run_id = :run_id
            ORDER BY s.geography_id, s.variable_code
        """),
        {"run_id": source_run_id},
    ).mappings().all()

    if len(records) != EXPECTED_SOURCE_ROWS:
        raise ValueError(
            f"Se leyeron {len(records)} filas; "
            f"se esperaban {EXPECTED_SOURCE_ROWS}."
        )

    missing_geographies = [
        row["geography_id"]
        for row in records
        if row["geography_key"] is None
        or row["parent_state_key"] is None
    ]

    if missing_geographies:
        raise ValueError(
            "Geografías sin correspondencia COUNTY o sin estado padre "
            "FIPS válido en gold.dim_geography: "
            f"{sorted(set(missing_geographies))}"
        )

    unique_pairs = {
        (row["geography_id"], row["variable_code"])
        for row in records
    }

    if len(unique_pairs) != EXPECTED_FACT_ROWS:
        raise ValueError(
            "Existen combinaciones duplicadas de geografía e indicador."
        )

    actual_variables = {
        row["variable_code"] for row in records
    }

    if actual_variables != VARIABLE_CODES:
        raise ValueError(
            f"Variables inesperadas: {sorted(actual_variables)}"
        )

    counts_per_geography = {}

    for row in records:
        geography_id = row["geography_id"]
        counts_per_geography[geography_id] = (
            counts_per_geography.get(geography_id, 0) + 1
        )

    invalid_counts = {
        geography: count
        for geography, count in counts_per_geography.items()
        if count != EXPECTED_INDICATORS
    }

    if (
        len(counts_per_geography) != EXPECTED_GEOGRAPHIES
        or invalid_counts
    ):
        raise ValueError(
            "Distribución de indicadores por condado incorrecta: "
            f"{invalid_counts}"
        )

    if any(row["estimate"] is None for row in records):
        raise ValueError(
            "Se encontraron estimaciones nulas en Silver."
        )

    logger.info(
        "Silver validado | filas=%s | condados=%s | variables=%s",
        len(records),
        len(counts_per_geography),
        len(actual_variables),
    )

    return [dict(row) for row in records]


def load_facts(
    conn,
    records: list[dict],
    source_run_id: int,
) -> int:

    source_key = conn.execute(
        text("""
            SELECT source_key
            FROM gold.dim_source
            WHERE source_code = 'ACS_COUNTY'
              AND dataset_name = :dataset_name
        """),
        {"dataset_name": DATASET_NAME},
    ).scalar_one_or_none()

    if source_key is None:
        raise ValueError(
            "No se encontró ACS_COUNTY en gold.dim_source."
        )

    indicator_rows = conn.execute(
        text("""
            SELECT indicator_key, variable_code
            FROM gold.dim_acs_county_indicator
        """)
    ).mappings().all()

    indicator_map = {
        row["variable_code"]: row["indicator_key"]
        for row in indicator_rows
    }

    if set(indicator_map) != VARIABLE_CODES:
        raise ValueError(
            "La dimensión de indicadores ACS County no contiene "
            "exactamente las cuatro variables esperadas."
        )

    existing = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_acs_county_indicator
            WHERE source_run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).scalar_one()

    if existing:
        raise RuntimeError(
            f"Ya existen {existing} hechos para run_id={source_run_id}. "
            "Se evita duplicar o sobrescribir una carga histórica."
        )

    insert_stmt = text("""
        INSERT INTO gold.fact_acs_county_indicator (
            source_run_id,
            source_key,
            geography_key,
            indicator_key,
            estimate_raw,
            estimate_numeric,
            margin_of_error_raw,
            margin_of_error_numeric,
            percent_raw,
            percent_numeric,
            percent_margin_of_error_raw,
            percent_margin_of_error_numeric
        )
        VALUES (
            :source_run_id,
            :source_key,
            :geography_key,
            :indicator_key,
            :estimate_raw,
            :estimate_numeric,
            :margin_of_error_raw,
            :margin_of_error_numeric,
            :percent_raw,
            :percent_numeric,
            :percent_margin_of_error_raw,
            :percent_margin_of_error_numeric
        )
    """)

    batch = []
    loaded = 0

    for row in records:
        variable_code = row["variable_code"]

        # DP05_0001PE no representa un porcentaje válido:
        # no se publica ni como porcentaje ni como MOE porcentual.
        if variable_code == "DP05_0001":
            percent_raw = None
            percent_numeric = None
            percent_moe_raw = None
            percent_moe_numeric = None
        else:
            percent_raw = row["percent"]
            percent_numeric = parse_numeric(row["percent"])
            percent_moe_raw = row["percent_margin_of_error"]
            percent_moe_numeric = parse_numeric(
                row["percent_margin_of_error"]
            )

        batch.append({
            "source_run_id": source_run_id,
            "source_key": source_key,
            "geography_key": row["geography_key"],
            "indicator_key": indicator_map[variable_code],
            "estimate_raw": row["estimate"],
            "estimate_numeric": parse_numeric(row["estimate"]),
            "margin_of_error_raw": row["margin_of_error"],
            "margin_of_error_numeric": parse_numeric(
                row["margin_of_error"]
            ),
            "percent_raw": percent_raw,
            "percent_numeric": percent_numeric,
            "percent_margin_of_error_raw": percent_moe_raw,
            "percent_margin_of_error_numeric": percent_moe_numeric,
        })

        if len(batch) >= INSERT_BATCH_SIZE:
            conn.execute(insert_stmt, batch)
            loaded += len(batch)
            batch.clear()

    if batch:
        conn.execute(insert_stmt, batch)
        loaded += len(batch)

    persisted = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_acs_county_indicator
            WHERE source_run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).scalar_one()

    if loaded != EXPECTED_FACT_ROWS or persisted != EXPECTED_FACT_ROWS:
        raise RuntimeError(
            f"Validación Gold fallida: insertados={loaded}, "
            f"persistidos={persisted}, esperados={EXPECTED_FACT_ROWS}."
        )

    return loaded


def run_etl(source_run_id: int | None = None) -> None:
    engine = create_gold_engine()

    try:
        with engine.begin() as conn:
            source_run_id = resolve_source_run_id(conn, source_run_id)
            source_run = validate_source_run(
                conn,
                source_run_id,
            )

            records = read_and_validate_silver(
                conn,
                source_run_id,
            )

            loaded = load_facts(
                conn,
                records,
                source_run_id,
            )

            logger.info(
                "ETL Gold ACS County completado | "
                "run_id=%s | origen=%s | hechos=%s",
                source_run_id,
                source_run["dataset_name"],
                loaded,
            )

    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Integra ACS County DP05 desde Silver hacia Gold."
        )
    )

    parser.add_argument(
        "--source-run-id",
        type=int,
        default=None,
        help=(
            "run_id de Silver ACS County. Si se omite, "
            "se selecciona automáticamente la versión vigente."
        ),
    )

    args = parser.parse_args()

    try:
        run_etl(args.source_run_id)
        return 0
    except Exception:
        logger.exception("Falló el ETL Gold ACS County.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
