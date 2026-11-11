from __future__ import annotations
import argparse
import hashlib
import logging
import math
import os
import re
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from scripts.silver.silver_budget import (
    enforce_silver_budget,
    lock_silver_budget,
)

ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = ROOT / "data" / "bronze" / "places_2025"
TARGET_TABLE = "silver_places_2023"
TARGET_YEAR = 2023
DATASET_NAME = "places_2025"
SAMPLE_LOCATIONS = 83
SAMPLE_SEED = "neurological-data-lake-places-2023-v1"
INSERT_BATCH_SIZE = 1_000
SELECTED_MEASURE_IDS = frozenset({
    "COGNITION", "STROKE", "DISABILITY", "MOBILITY", "SELFCARE",
    "INDEPLIVE", "HEARING", "VISION", "DEPRESSION", "MHLTH",
    "PHLTH", "ARTHRITIS", "CHD", "DIABETES", "GHLTH",
})

TARGET_COLUMNS = [
    "state_abbr",
    "location_name",
    "location_id",
    "measure",
    "measure_id",
    "data_value_type",
    "data_value",
    "latitude",
    "longitude",
    "run_id",
]

GEO_PATTERN = re.compile(
    r"^\s*POINT\s*\(\s*([-+]?\d+(?:\.\d+)?)\s+"
    r"([-+]?\d+(?:\.\d+)?)\s*\)\s*$",
    re.IGNORECASE,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger("etl_places")



def create_db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    allowed_database = "health_datalake"
    allowed_host = "localhost"
    allowed_port = 5432

    database = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST")
    port_raw = os.getenv("POSTGRES_PORT")

    if database != allowed_database:
        raise RuntimeError(
            f"Conexión rechazada: base no permitida: {database!r}"
        )

    if host != allowed_host:
        raise RuntimeError(
            f"Conexión rechazada: host no permitido: {host!r}"
        )

    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        raise RuntimeError(
            f"Puerto PostgreSQL inválido: {port_raw!r}"
        )

    if port != allowed_port:
        raise RuntimeError(
            f"Conexión rechazada: solo se permite el puerto "
            f"{allowed_port}. Configurado: {port}"
        )

    required = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]

    missing = [
        key for key in required if not os.getenv(key)
    ]

    if missing:
        raise RuntimeError(
            f"Faltan variables PostgreSQL: {', '.join(missing)}"
        )

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=allowed_host,
        port=allowed_port,
        database=allowed_database,
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

            if actual_database != allowed_database:
                raise RuntimeError(
                    f"Conexión rechazada: PostgreSQL respondió "
                    f"con la base {actual_database!r}"
                )

        return engine

    except Exception:
        engine.dispose()
        raise


def find_source_file() -> Path:
    files = sorted(BRONZE_DIR.glob("*.csv"))
    if len(files) != 1:
        raise FileNotFoundError(
            f"Se esperaba un único CSV en {BRONZE_DIR}; se encontraron {len(files)}." )
    return files[0]


def allocate_proportionally(counts: dict[str, int], total: int) -> dict[str, int]:
    if total < len(counts):
        raise ValueError("No hay presupuesto para representar todas las jurisdicciones.")
    n = sum(counts.values())
    remaining = total - len(counts)
    quotas = {state: remaining * count / n for state, count in counts.items()}
    allocation = {state: 1 + math.floor(quota) for state, quota in quotas.items()}
    left = total - sum(allocation.values())
    order = sorted(
        counts,
        key=lambda s: (quotas[s] - math.floor(quotas[s]), counts[s], s),
        reverse=True,)

    for state in order[:left]:
        allocation[state] += 1

    for state, count in counts.items():
        allocation[state] = min(allocation[state], count)

    if sum(allocation.values()) != total:
        raise RuntimeError("No fue posible asignar exactamente 83 ubicaciones.")
    return allocation


def parse_geo(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    extracted = series.astype("string").str.extract(GEO_PATTERN)
    malformed = series.notna() & extracted[0].isna()

    if malformed.any():
        raise ValueError(f"Geolocation inválida: {series[malformed].head(5).tolist()}")
    longitude = pd.to_numeric(extracted[0], errors="coerce")
    latitude = pd.to_numeric(extracted[1], errors="coerce")

    if (
        (longitude.notna() & ~longitude.between(-180, 180)).any()
        or (latitude.notna() & ~latitude.between(-90, 90)).any()):
        raise ValueError("Se encontraron coordenadas fuera de rango.")
    return latitude, longitude


def transform_and_sample(source: Path, run_id: int) -> tuple[pd.DataFrame, int, int]:

    df = pd.read_csv(
        source, dtype="string", keep_default_na=False, na_values=[""],
        usecols=lambda c: c in {
            "Year", "StateAbbr", "LocationName", "Measure", "Data_Value_Type",
            "Data_Value", "LocationID", "MeasureId", "DataValueTypeID",
            "Geolocation",
        },
        )

    source_rows = len(df)
    required = {
        "Year", "StateAbbr", "LocationName", "Measure", "Data_Value_Type",
        "Data_Value", "LocationID", "MeasureId", "DataValueTypeID", "Geolocation",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Faltan columnas Bronze: {sorted(missing)}")


    year = pd.to_numeric(df["Year"], errors="coerce")
    df = df.loc[year.eq(TARGET_YEAR)].copy()
    year_rows = len(df)
    df = df.loc[df["MeasureId"].isin(SELECTED_MEASURE_IDS)].copy()
    # La ubicación nacional es un agregado y no se mezcla con unidades estatales.
    df = df.loc[df["StateAbbr"].ne("US")].copy()

    if df.empty:
        raise ValueError("No hay datos PLACES 2023 para las medidas seleccionadas.")


    counts = (
        df[["StateAbbr", "LocationID"]].drop_duplicates()
        .groupby("StateAbbr")["LocationID"].nunique().astype(int).to_dict()
    )

    allocation = allocate_proportionally(counts, SAMPLE_LOCATIONS)

    locations = df[["StateAbbr", "LocationID"]].drop_duplicates().copy()

    locations["_hash"] = [
        hashlib.sha256(f"{SAMPLE_SEED}|{state}|{loc}".encode()).hexdigest()

        for state, loc in zip(locations["StateAbbr"], locations["LocationID"])
    ]

    chosen_parts = []

    for state, quota in allocation.items():
        part = locations.loc[locations["StateAbbr"].eq(state)].sort_values("_hash").head(quota)
        chosen_parts.append(part)
    chosen = pd.concat(chosen_parts, ignore_index=True)

    if len(chosen) != SAMPLE_LOCATIONS:
        raise RuntimeError(f"Se seleccionaron {len(chosen)} ubicaciones; se esperaban 83.")


    selected = df.merge(
        chosen[["StateAbbr", "LocationID"]],
        on=["StateAbbr", "LocationID"],
        how="inner",
        validate="many_to_one",
    )

    if selected.groupby("LocationID").size().ne(30).any():
        raise ValueError("Alguna ubicación seleccionada no tiene exactamente 30 registros.")

    if selected["MeasureId"].nunique() != 15 or selected["DataValueTypeID"].nunique() != 2:
        raise ValueError("La muestra no conserva las 15 medidas y los 2 tipos de valor.")

    if len(selected) != SAMPLE_LOCATIONS * 30:
        raise ValueError(
            f"Se esperaban {SAMPLE_LOCATIONS * 30} filas; se obtuvieron {len(selected)}."
        )


    latitude, longitude = parse_geo(selected["Geolocation"])

    result = pd.DataFrame({
        "state_abbr": selected["StateAbbr"],
        "location_name": selected["LocationName"],
        "location_id": selected["LocationID"],
        "measure": selected["Measure"],
        "measure_id": selected["MeasureId"],
        "data_value_type": selected["Data_Value_Type"],
        "data_value": pd.Series(index=selected.index, dtype="float64"),
        "latitude": latitude,
        "longitude": longitude,
    })

    # PLACES entrega Data_Value con coma decimal (por ejemplo, "29,9").
    # Los campos vacíos se conservan como NULL; los demás deben convertirse.
    data_value_raw = selected["Data_Value"].astype("string").str.strip()
    data_value_normalized = data_value_raw.str.replace(",", ".", regex=False)
    data_value_numeric = pd.to_numeric(data_value_normalized, errors="coerce")

    invalid_numeric = (
        data_value_raw.notna()
        & data_value_raw.ne("")
        & data_value_numeric.isna()
    )
    if invalid_numeric.any():
        examples = data_value_raw.loc[invalid_numeric].head(10).tolist()
        raise ValueError(
            "Se encontraron valores numéricos PLACES no interpretables "
            f"en Data_Value: {examples}"
        )

    result["data_value"] = data_value_numeric
    result["run_id"] = run_id  # solo auditoría durante el proceso; no se persiste como columna
    return result, source_rows, year_rows


def validate_target_schema(engine: Engine) -> None:

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table_name
            ORDER BY ordinal_position
        """), {"table_name": TARGET_TABLE}).mappings().all()

    expected = {
    "state_abbr": ("text", "YES"),
    "location_name": ("text", "YES"),
    "location_id": ("text", "NO"),
    "measure": ("text", "YES"),
    "measure_id": ("text", "NO"),
    "data_value_type": ("text", "NO"),
    "data_value": ("numeric", "YES"),
    "latitude": ("double precision", "YES"),
    "longitude": ("double precision", "YES"),
    "run_id": ("bigint", "NO"),
    }

    actual = {r["column_name"]: (r["data_type"], r["is_nullable"]) for r in rows}

    if actual != expected:
        raise RuntimeError(
            "Esquema PLACES no corresponde a la versión compacta. "
             "Ejecuta sql/silver/schema_silver.sql en una base de datos nueva.\n"
            f"Esperado: {expected}\nActual: {actual}"
        )


def run_etl(replace: bool = False) -> None:
    source = find_source_file()
    engine = create_db_engine()

    run_id = None
    source_rows = loaded_rows = year_rows = 0

    try:
        validate_target_schema(engine)

        with engine.begin() as conn:
            run_id = conn.execute(text("""
                INSERT INTO public.etl_runs (
                    dataset_name,
                    target_table,
                    started_at,
                    status
                )
                VALUES (
                    :dataset_name,
                    :target_table,
                    clock_timestamp(),
                    'running'
                )
                RETURNING run_id
            """), {
                "dataset_name": DATASET_NAME,
                "target_table": TARGET_TABLE,
            }).scalar_one()

        transformed, source_rows, year_rows = transform_and_sample(
            source, int(run_id)
        )

        logger.info(
            "PLACES preparado | origen=%s | año %s=%s | ubicaciones=%s | filas=%s",
            source_rows,
            TARGET_YEAR,
            year_rows,
            transformed["location_id"].nunique(),
            len(transformed),
        )

        with engine.begin() as conn:

            lock_silver_budget(conn)

            existing_rows = int(conn.execute(text("""
                SELECT COUNT(*)
                FROM public.silver_places_2023
            """)).scalar_one())

            if existing_rows > 0 and not replace:
                raise RuntimeError(
                    f"La tabla {TARGET_TABLE} ya contiene {existing_rows} registros. "
                    "No se modificaron los datos existentes. "
                    "Para reemplazarlos, ejecuta el ETL con --replace."
                )

            if existing_rows > 0 and replace:
                logger.warning(
                    "Se reemplazarán %s registros existentes de PLACES.",
                    existing_rows,
                )

                conn.execute(text("""
                    DELETE FROM public.silver_places_2023
                """))

            payload = transformed[TARGET_COLUMNS].to_dict(orient="records")

            insert_stmt = text("""
            INSERT INTO public.silver_places_2023
                (
                    state_abbr,
                    location_name,
                    location_id,
                    measure,
                    measure_id,
                    data_value_type,
                    data_value,
                    latitude,
                    longitude,
                    run_id
                )
            VALUES
                (
                    :state_abbr,
                    :location_name,
                    :location_id,
                    :measure,
                    :measure_id,
                    :data_value_type,
                    :data_value,
                    :latitude,
                    :longitude,
                    :run_id
                )
                """)

            for start in range(0, len(payload), INSERT_BATCH_SIZE):
                batch = payload[start:start + INSERT_BATCH_SIZE]
                conn.execute(insert_stmt, batch)
                loaded_rows += len(batch)

            if loaded_rows != 2_490:
                raise RuntimeError(
                    f"Se esperaban 2.490 filas; se cargaron {loaded_rows}."
                )

            persisted = int(conn.execute(text("""
                SELECT COUNT(*)
                FROM public.silver_places_2023
            """)).scalar_one())

            if persisted != loaded_rows:
                raise RuntimeError(
                    f"Persistidos={persisted}; cargados={loaded_rows}."
                )

            # Validar trazabilidad de todos los registros Silver
            lineage = conn.execute(text("""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(run_id) AS rows_with_run_id,
                    COUNT(DISTINCT run_id) AS distinct_runs,
                    MIN(run_id) AS min_run_id,
                    MAX(run_id) AS max_run_id
                FROM public.silver_places_2023
            """)).mappings().one()

            if (
                lineage["total_rows"] != loaded_rows
                or lineage["rows_with_run_id"] != loaded_rows
                or lineage["distinct_runs"] != 1
                or lineage["min_run_id"] != run_id
                or lineage["max_run_id"] != run_id
            ):
                raise RuntimeError(
                    "Trazabilidad PLACES inconsistente. "
                    f"Esperado run_id={run_id}; "
                    f"resultado={dict(lineage)}"
                )

            logger.info(
                "Trazabilidad Silver validada | run_id=%s | filas=%s",
                run_id,
                lineage["rows_with_run_id"],
            )

            total_cells = enforce_silver_budget(conn)


            conn.execute(text("""
                UPDATE public.etl_runs
                SET status='succeeded',
                    finished_at=GREATEST(clock_timestamp(), started_at),
                    source_rows=:source_rows,
                    loaded_rows=:loaded_rows
                WHERE run_id=:run_id
            """), {
                "source_rows": source_rows,
                "loaded_rows": loaded_rows,
                "run_id": run_id,
            })

            logger.info(
                "Presupuesto Silver validado: %s / 100000 celdas",
                total_cells,
            )

        logger.info(
            "ETL PLACES finalizado | run_id=%s | cargados=%s",
            run_id,
            loaded_rows,
        )

    except Exception as exc:
        logger.exception("Falló el ETL PLACES.")

        if run_id is not None:
            try:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE public.etl_runs
                        SET status='failed',
                            finished_at=GREATEST(clock_timestamp(), started_at),
                            source_rows=:source_rows,
                            loaded_rows=:loaded_rows,
                            error_message=:error
                        WHERE run_id=:run_id
                    """), {
                        "source_rows": source_rows,
                        "loaded_rows": loaded_rows,
                        "error": str(exc)[:4000],
                        "run_id": run_id,
                    })

            except Exception:
                logger.exception("No se pudo actualizar etl_runs.")

            

        raise

    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL PLACES 2023 hacia Silver."
    )

    parser.add_argument(
        "--replace",
        action="store_true",
        help="Reemplaza los registros existentes en silver_places_2023.",
    )

    args = parser.parse_args()
    run_etl(replace=args.replace)


if __name__ == "__main__":
    main()