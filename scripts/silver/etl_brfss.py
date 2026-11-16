from __future__ import annotations
import argparse
import hashlib
import heapq
import json
import logging
import math
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyreadstat
from sqlalchemy import text
from sqlalchemy.engine import Engine
from scripts.bronze.parquet_io import (
    dataset_path,
    iter_parquet_frames,
    parquet_dataset,
    selected_frequency,
    validate_dataset_manifest,
)
from scripts.silver.silver_budget import (
    enforce_silver_budget,
    lock_silver_budget,
)

from scripts.database_connection import create_database_engine

ROOT = Path(__file__).resolve().parents[2]
LANDING_SOURCE_DIR = ROOT / "data" / "landing" / "brfss_2023"
LEGACY_SOURCE_DIR = ROOT / "data" / "bronze" / "brfss_2023"
SOURCE_DIR = (
    LANDING_SOURCE_DIR
    if (LANDING_SOURCE_DIR / "LLCP2023.XPT").is_file()
    else LEGACY_SOURCE_DIR
)
XPT_FILE = SOURCE_DIR / "LLCP2023.XPT"
CODEBOOK_FILE = SOURCE_DIR / "USCODE23_LLCP_021924.HTML"

DATASET_NAME = "brfss_2023"
TARGET_TABLE = "silver_respondent+silver_brfss_2023"
CHUNK_SIZE = 10_000
SAMPLE_SIZE = 2_800
SAMPLE_SEED = "neurological-data-lake-brfss-2023-v1"
INSERT_BATCH_SIZE = 500
EXPECTED_SOURCE_ROWS = 433_323
EXPECTED_VARIABLES = 350

BRFSS_VARIABLES = [
    "_AGEG5YR", "_SEX", "_RACEGR3", "EDUCA", "INCOME3",
    "GENHLTH", "PHYSHLTH", "MENTHLTH", "POORHLTH", "DIABETE4",
    "CVDCRHD4", "CVDSTRK3", "HAVARTH4", "ADDEPEV3", "DIFFWALK",
    "DIFFALON", "_BMI5", "_SMOKER3", "DRNKANY6", "_TOTINDA",
]
REQUIRED_SOURCE_COLUMNS = {"_STATE", "SEQNO", *BRFSS_VARIABLES}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("etl_brfss")


def validate_source_files() -> None:
    if not XPT_FILE.is_file():
        raise FileNotFoundError(f"No se encontró el archivo XPT: {XPT_FILE}")
    if not CODEBOOK_FILE.is_file():
        raise FileNotFoundError(f"No se encontró el codebook: {CODEBOOK_FILE}")


def validate_target_schema(engine: Engine) -> None:
    query = text("""
        SELECT column_name, data_type, character_maximum_length,
               is_nullable, is_identity
        FROM information_schema.columns
        WHERE table_schema = 'silver' AND table_name = :table_name
        ORDER BY ordinal_position
    """)
    expected_schemas = {
        "silver_respondent": {
            "respondent_id": ("bigint", None, "NO", "YES"),
            "sequence_number": ("text", None, "NO", "NO"),
            "state_fips": ("character varying", 2, "NO", "NO"),
            "run_id": ("bigint", None, "NO", "NO"),
        },
        "silver_brfss_2023": {
            "respondent_id": ("bigint", None, "NO", "NO"),
            "record_data": ("jsonb", None, "NO", "NO"),
        },
    }
    with engine.connect() as conn:
        for table_name, expected in expected_schemas.items():
            rows = conn.execute(query, {"table_name": table_name}).mappings().all()
            actual = {
                row["column_name"]: (
                    row["data_type"], row["character_maximum_length"],
                    row["is_nullable"], row["is_identity"],
                )
                for row in rows
            }
            if actual != expected:
                raise RuntimeError(
                    f"Esquema inesperado en silver.{table_name}. "
                    "Ejecuta sql/silver/schema_silver.sql en una base nueva.\n"
                    f"Esperado: {expected}\nActual: {actual}"
                )

        constraints = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE tc.constraint_type = 'PRIMARY KEY') AS pk_count,
                COUNT(*) FILTER (WHERE tc.constraint_type = 'FOREIGN KEY') AS fk_count
            FROM information_schema.table_constraints tc
            WHERE tc.constraint_schema = 'silver'
              AND tc.table_name = 'silver_brfss_2023'
        """)).mappings().one()
        if constraints["pk_count"] != 1 or constraints["fk_count"] != 1:
            raise RuntimeError(
                "silver_brfss_2023 debe tener una PK y una FK hacia silver_respondent."
            )


def create_etl_run(engine: Engine) -> int:
    with engine.begin() as conn:
        return int(conn.execute(text("""
            INSERT INTO datalake_control.etl_runs
                (dataset_name, target_table, started_at, status, source_rows, loaded_rows)
            VALUES (:dataset_name, :target_table, clock_timestamp(), 'running', 0, 0)
            RETURNING run_id
        """), {"dataset_name": DATASET_NAME, "target_table": TARGET_TABLE}).scalar_one())


def update_etl_run(
    engine: Engine, run_id: int, status: str, source_rows: int,
    loaded_rows: int, error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE datalake_control.etl_runs
            SET finished_at = GREATEST(clock_timestamp(), started_at), status = :status,
                source_rows = :source_rows, loaded_rows = :loaded_rows,
                error_message = :error_message
            WHERE run_id = :run_id
        """), {
            "run_id": run_id, "status": status, "source_rows": source_rows,
            "loaded_rows": loaded_rows, "error_message": error_message,
        })


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    result = pd.isna(value)
    return bool(result) if isinstance(result, (bool, np.bool_)) else False


def json_safe(value: Any) -> Any:
    if is_missing(value):
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    raise TypeError(f"Tipo no serializable en JSONB: {type(value).__name__}")


def normalize_identifier(value: Any, column: str) -> str:
    if is_missing(value):
        raise ValueError(f"{column} contiene un identificador nulo.")
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError(f"{column} contiene un identificador no entero: {value!r}")
        return str(int(value))
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{column} contiene un identificador vacío.")
    return normalized


def normalize_state_fips(value: Any) -> str:
    """Normaliza BRFSS _STATE como FIPS de dos dígitos."""
    normalized = normalize_identifier(value, "_STATE")
    if not normalized.isdigit() or len(normalized) > 2:
        raise ValueError(f"_STATE no es un código FIPS estatal válido: {normalized!r}")
    return normalized.zfill(2)


def read_chunks():
    if selected_frequency() is not None:
        validate_dataset_manifest("brfss_2023")
        parquet_dir = dataset_path("brfss_2023")
        schema = parquet_dataset(parquet_dir).schema
        source_columns = [
            name for name in schema.names
            if name not in {
                "source_record_id",
                "source_file",
                "simulated_received_date",
                "ingestion_sequence",
                "ingestion_batch_id",
                "ingestion_scenario",
                "source_row_hash",
            }
        ]
        if len(source_columns) != EXPECTED_VARIABLES:
            raise ValueError(
                "El Bronze Parquet BRFSS no conserva las 350 variables originales: "
                f"se encontraron {len(source_columns)}."
            )
        for chunk in iter_parquet_frames(
            parquet_dir,
            batch_size=CHUNK_SIZE,
            columns=source_columns,
        ):
            yield chunk, None
        return

    def read_xport_with_encoding(file_path, **kwargs):
        return pyreadstat.read_xport(
            file_path, encoding="windows-1252", **kwargs
        )

    yield from pyreadstat.read_file_in_chunks(
        read_xport_with_encoding,
        str(XPT_FILE),
        chunksize=CHUNK_SIZE,
        output_format="pandas",
    )


def allocate_proportionally(counts: dict[str, int], total: int) -> dict[str, int]:
    """Largest-remainder allocation, with at least one record per state."""
    if total < len(counts):
        raise ValueError("El tamaño de muestra no alcanza para representar todos los estados.")
    n = sum(counts.values())
    allocation = {state: 1 for state in counts}
    remaining = total - len(counts)
    quotas = {state: remaining * count / n for state, count in counts.items()}
    floors = {state: int(math.floor(q)) for state, q in quotas.items()}
    for state, value in floors.items():
        allocation[state] += value
    left = total - sum(allocation.values())
    order = sorted(counts, key=lambda s: (quotas[s] - floors[s], counts[s], s), reverse=True)
    for state in order[:left]:
        allocation[state] += 1
    for state, count in counts.items():
        allocation[state] = min(allocation[state], count)
    if sum(allocation.values()) != total:
        raise RuntimeError("No fue posible asignar exactamente el tamaño de muestra.")
    return allocation


def sample_source() -> tuple[pd.DataFrame, int]:
    """Dos pasadas: cuenta estados y luego conserva los hashes más bajos por estrato."""
    state_counts: dict[str, int] = {}
    source_rows = 0

    for chunk, _metadata in read_chunks():
        source_rows += len(chunk)
        if len(chunk.columns) != EXPECTED_VARIABLES:
            raise ValueError(
                f"Se esperaban {EXPECTED_VARIABLES} variables; se encontraron {len(chunk.columns)}."
            )
        missing = REQUIRED_SOURCE_COLUMNS - set(chunk.columns)
        if missing:
            raise ValueError(f"Faltan variables en XPT: {sorted(missing)}")
        # Normalizar _STATE con la misma función usada en la segunda pasada.
        # pyreadstat puede entregar el código como float (p. ej. 1.0);
        # comparar "1.0" con "1" dejaba todos los estratos sin coincidencias.
        # Se conserva la normalización histórica para no alterar los hashes
        # ni la composición determinista de la muestra existente.
        states = chunk["_STATE"].map(
            lambda value: normalize_identifier(value, "_STATE")
        )
        for state, count in states.value_counts().items():
            state_counts[state] = state_counts.get(state, 0) + int(count)

    if source_rows != EXPECTED_SOURCE_ROWS:
        raise ValueError(
            f"Filas de origen inesperadas: {source_rows}; se esperaban {EXPECTED_SOURCE_ROWS}."
        )
    allocation = allocate_proportionally(state_counts, SAMPLE_SIZE)
    logger.info("Estratos: %s | muestra: %s", len(state_counts), SAMPLE_SIZE)

    selected_frames = []
    for chunk, _metadata in read_chunks():
        cols = ["_STATE", "SEQNO", *BRFSS_VARIABLES]
        small = chunk[cols].copy()
        small["_STATE"] = small["_STATE"].map(
            lambda value: normalize_identifier(value, "_STATE")
        )
        small["SEQNO"] = small["SEQNO"].map(lambda v: normalize_identifier(v, "SEQNO"))
        small["_sample_hash"] = [
            hashlib.sha256(
                f"{SAMPLE_SEED}|{state}|{seq}".encode("utf-8")
            ).hexdigest()
            for state, seq in zip(small["_STATE"], small["SEQNO"])
        ]
        for state, quota in allocation.items():
            part = small.loc[small["_STATE"].eq(state)]
            if not part.empty:
                selected_frames.append(part.sort_values("_sample_hash").head(quota))

    candidates = pd.concat(selected_frames, ignore_index=True)
    # La selección de cada estado se vuelve a limitar tras combinar bloques
    pieces = []
    for state, quota in allocation.items():
        part = candidates.loc[candidates["_STATE"].eq(state)]
        pieces.append(part.sort_values("_sample_hash").head(quota))
    sampled = pd.concat(pieces, ignore_index=True)
    if len(sampled) != SAMPLE_SIZE:
        raise RuntimeError(f"Muestra obtenida={len(sampled)}; esperada={SAMPLE_SIZE}.")
    if sampled.duplicated(["_STATE", "SEQNO"]).any():
        raise ValueError("Se detectaron claves duplicadas en la muestra BRFSS.")
    return sampled.drop(columns=["_sample_hash"]), source_rows


def load_data(
    engine: Engine, sampled: pd.DataFrame, run_id: int,
    source_rows: int, replace: bool,
) -> tuple[int, int]:
    respondents = []
    response_payload = []

    for row in sampled.to_dict(orient="records"):
        state_fips = normalize_state_fips(row.pop("_STATE"))
        sequence_number = normalize_identifier(row.pop("SEQNO"), "SEQNO")
        record_data = {name: json_safe(row[name]) for name in BRFSS_VARIABLES}

        respondents.append({
            "state_fips": state_fips,
            "sequence_number": sequence_number,
            "run_id": run_id,
        })
        response_payload.append({
            "state_fips": state_fips,
            "sequence_number": sequence_number,
            "record_data": record_data,
        })

    insert_respondent = text("""
        INSERT INTO silver.silver_respondent
            (state_fips, sequence_number, run_id)
        VALUES
            (:state_fips, :sequence_number, :run_id)
    """)

    insert_responses = text("""
        INSERT INTO silver.silver_brfss_2023 (respondent_id, record_data)
        SELECT respondent.respondent_id, payload.record_data
        FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS payload(
            state_fips TEXT,
            sequence_number TEXT,
            record_data JSONB
        )
        INNER JOIN silver.silver_respondent respondent
            ON respondent.state_fips = payload.state_fips
           AND respondent.sequence_number = payload.sequence_number
           AND respondent.run_id = :run_id
    """)

    with engine.begin() as conn:
        lock_silver_budget(conn)

        existing_respondents = int(conn.execute(text(
            "SELECT COUNT(*) FROM silver.silver_respondent"
        )).scalar_one())
        existing_responses = int(conn.execute(text(
            "SELECT COUNT(*) FROM silver.silver_brfss_2023"
        )).scalar_one())

        if (existing_respondents or existing_responses) and not replace:
            raise RuntimeError(
                "Las tablas BRFSS ya contienen datos "
                f"(respondents={existing_respondents}, responses={existing_responses}). "
                "Usa --replace para reemplazar ambas tablas."
            )

        if replace:
            # Primero la tabla dependiente; luego la entidad principal.
            conn.execute(text("DELETE FROM silver.silver_brfss_2023"))
            conn.execute(text("DELETE FROM silver.silver_respondent"))

        for start in range(0, len(respondents), INSERT_BATCH_SIZE):
            conn.execute(
                insert_respondent,
                respondents[start:start + INSERT_BATCH_SIZE],
            )

        for start in range(0, len(response_payload), INSERT_BATCH_SIZE):
            batch = response_payload[start:start + INSERT_BATCH_SIZE]
            conn.execute(insert_responses, {
                "payload": json.dumps(
                    batch, ensure_ascii=False, allow_nan=False,
                    separators=(",", ":"),
                ),
                "run_id": run_id,
            })

        respondent_count = int(conn.execute(text("""
            SELECT COUNT(*) FROM silver.silver_respondent
            WHERE run_id = :run_id
        """), {"run_id": run_id}).scalar_one())
        response_count = int(conn.execute(text("""
            SELECT COUNT(*)
            FROM silver.silver_brfss_2023 response
            INNER JOIN silver.silver_respondent respondent
                ON respondent.respondent_id = response.respondent_id
            WHERE respondent.run_id = :run_id
        """), {"run_id": run_id}).scalar_one())

        if respondent_count != SAMPLE_SIZE or response_count != SAMPLE_SIZE:
            raise RuntimeError(
                "Conteos BRFSS inconsistentes: "
                f"respondents={respondent_count}, responses={response_count}, "
                f"esperados={SAMPLE_SIZE}."
            )
        invalid = int(conn.execute(text("""
            SELECT COUNT(*)
            FROM silver.silver_brfss_2023 response
            INNER JOIN silver.silver_respondent respondent
                ON respondent.respondent_id = response.respondent_id
            WHERE respondent.run_id = :run_id
              AND (
                  jsonb_typeof(response.record_data) <> 'object'
                  OR (SELECT COUNT(*) FROM jsonb_object_keys(response.record_data)) <> 20
              )
        """), {"run_id": run_id}).scalar_one())
        if invalid:
            raise RuntimeError(f"Se detectaron {invalid} respuestas BRFSS inválidas.")

        unmatched = int(conn.execute(text("""
            SELECT COUNT(*)
            FROM silver.silver_respondent respondent
            LEFT JOIN silver.silver_brfss_2023 response
                ON response.respondent_id = respondent.respondent_id
            WHERE respondent.run_id = :run_id
              AND response.respondent_id IS NULL
        """), {"run_id": run_id}).scalar_one())
        if unmatched:
            raise RuntimeError(
                f"Se detectaron {unmatched} encuestados sin respuesta asociada."
            )

        total_cells = enforce_silver_budget(conn)
        logger.info(
            "Presupuesto Silver validado: %s / %s celdas",
            total_cells, 100_000,
        )

        # El estado succeeded se confirma en la misma transacción que ambas tablas.
        conn.execute(text("""
            UPDATE datalake_control.etl_runs
            SET finished_at = GREATEST(clock_timestamp(), started_at),
                status = 'succeeded',
                source_rows = :source_rows,
                loaded_rows = :loaded_rows,
                error_message = NULL
            WHERE run_id = :run_id
        """), {
            "run_id": run_id,
            "source_rows": source_rows,
            "loaded_rows": SAMPLE_SIZE,
        })

    return len(sampled), response_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Carga muestra estratificada BRFSS 2023 (20 variables) "
            "y valida presupuesto Silver."
        )
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Reemplaza la tabla dentro de la transacción.",
    )
    args = parser.parse_args()

    validate_source_files()
    engine = create_database_engine()
    run_id = None
    source_rows = loaded_rows = 0
    try:
        validate_target_schema(engine)
        sampled, source_rows = sample_source()
        logger.info(
            "Muestra preparada | origen=%s | muestra=%s | variables=%s",
            source_rows, len(sampled), len(BRFSS_VARIABLES),
        )
        run_id = create_etl_run(engine)
        loaded_rows, _ = load_data(engine, sampled, run_id, source_rows, args.replace)
        logger.info(
            "ETL BRFSS completado | run_id=%s | origen=%s | muestra=%s",
            run_id, source_rows, loaded_rows,
        )
        return 0
    except Exception as exc:
        logger.exception("Falló el ETL BRFSS.")
        if run_id is not None:
            try:
                update_etl_run(engine, run_id, "failed", source_rows, loaded_rows, str(exc)[:4000])
            except Exception:
                logger.exception("No fue posible actualizar etl_runs.")
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())