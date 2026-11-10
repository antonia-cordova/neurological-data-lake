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
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from scripts.silver.silver_budget import (
    enforce_silver_budget,
    lock_silver_budget,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "data" / "bronze" / "brfss_2023"
XPT_FILE = SOURCE_DIR / "LLCP2023.XPT"
CODEBOOK_FILE = SOURCE_DIR / "USCODE23_LLCP_021924.HTML"

DATASET_NAME = "brfss_2023"
TARGET_TABLE = "silver_brfss_2023"
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



def create_db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    allowed_database = "health_datalake"
    database = os.getenv("POSTGRES_DB")

    if database != allowed_database:
        raise RuntimeError(
            f"Conexión rechazada: solo se permite '{allowed_database}'. "
            f"Base configurada: {database!r}"
        )

    required = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]

    if missing:
        raise RuntimeError(
            f"Faltan variables de conexión: {', '.join(missing)}"
        )

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

            if actual_database != allowed_database:
                raise RuntimeError(
                    f"Conexión rechazada: PostgreSQL respondió "
                    f"con la base '{actual_database}'."
                )

        return engine

    except Exception:
        engine.dispose()
        raise


def validate_source_files() -> None:
    if not XPT_FILE.is_file():
        raise FileNotFoundError(f"No se encontró el archivo XPT: {XPT_FILE}")
    if not CODEBOOK_FILE.is_file():
        raise FileNotFoundError(f"No se encontró el codebook: {CODEBOOK_FILE}")


def validate_target_schema(engine: Engine) -> None:
    query = text("""
        SELECT column_name, data_type, is_nullable, is_identity
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :table_name
        ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"table_name": TARGET_TABLE}).mappings().all()
    expected = {
        "respondent_id": ("bigint", "NO", "YES"),
        "state_code": ("text", "NO", "NO"),
        "sequence_number": ("text", "NO", "NO"),
        "record_data": ("jsonb", "NO", "NO"),
        "run_id": ("bigint", "NO", "NO"),
    }
    actual = {
        r["column_name"]: (r["data_type"], r["is_nullable"], r["is_identity"])
        for r in rows
    }
    if actual != expected:
        raise RuntimeError(
            f"Esquema inesperado en public.{TARGET_TABLE}. "
            f"Ejecuta sql/silver/schema_silver.sql en una base de datos nueva..\n"
            f"Esperado: {expected}\nActual: {actual}"
        )


def create_etl_run(engine: Engine) -> int:
    with engine.begin() as conn:
        return int(conn.execute(text("""
            INSERT INTO public.etl_runs
                (dataset_name, target_table, started_at, status, source_rows, loaded_rows)
            VALUES (:dataset_name, :target_table, NOW(), 'running', 0, 0)
            RETURNING run_id
        """), {"dataset_name": DATASET_NAME, "target_table": TARGET_TABLE}).scalar_one())


def update_etl_run(
    engine: Engine, run_id: int, status: str, source_rows: int,
    loaded_rows: int, error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE public.etl_runs
            SET finished_at = NOW(), status = :status,
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


def read_chunks():
    def read_xport_with_encoding(file_path, **kwargs):
        return pyreadstat.read_xport(
            file_path, encoding="windows-1252", **kwargs
        )
    return pyreadstat.read_file_in_chunks(
        read_xport_with_encoding, str(XPT_FILE),
        chunksize=CHUNK_SIZE, output_format="pandas",
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
        small["_STATE"] = small["_STATE"].map(lambda v: normalize_identifier(v, "_STATE"))
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


def load_data(engine: Engine, sampled: pd.DataFrame, run_id: int, replace: bool) -> tuple[int, int]:
    records = []
    for row in sampled.to_dict(orient="records"):
        state = normalize_identifier(row.pop("_STATE"), "_STATE")
        seq = normalize_identifier(row.pop("SEQNO"), "SEQNO")
        record_data = {name: json_safe(row[name]) for name in BRFSS_VARIABLES}
        records.append({
            "state_code": state,
            "sequence_number": seq,
            "record_data": json.dumps(
                record_data, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ),
            "run_id": run_id,
        })

    insert_stmt = text("""
        INSERT INTO public.silver_brfss_2023
            (state_code, sequence_number, record_data, run_id)
        VALUES
            (:state_code, :sequence_number, CAST(:record_data AS jsonb), :run_id)
    """)

    with engine.begin() as conn:
        lock_silver_budget(conn)
        existing = int(conn.execute(
            text("SELECT COUNT(*) FROM public.silver_brfss_2023")
        ).scalar_one())
        if existing and not replace:
            raise RuntimeError(
                f"silver_brfss_2023 ya contiene {existing} filas. Usa --replace."
            )
        if replace:
            conn.execute(text("DELETE FROM public.silver_brfss_2023"))

        for start in range(0, len(records), INSERT_BATCH_SIZE):
            conn.execute(insert_stmt, records[start:start + INSERT_BATCH_SIZE])

        persisted = int(conn.execute(text("""
            SELECT COUNT(*) FROM public.silver_brfss_2023 WHERE run_id = :run_id
        """), {"run_id": run_id}).scalar_one())
        if persisted != SAMPLE_SIZE:
            raise RuntimeError(f"Se esperaban {SAMPLE_SIZE} filas; se persistieron {persisted}.")

        invalid = int(conn.execute(text("""
            SELECT COUNT(*) FROM public.silver_brfss_2023
            WHERE run_id = :run_id
              AND (
                jsonb_typeof(record_data) <> 'object'
                OR (SELECT COUNT(*) FROM jsonb_object_keys(record_data)) <> 20
              )
        """), {"run_id": run_id}).scalar_one())
        if invalid:
            raise RuntimeError(f"Se detectaron {invalid} registros BRFSS inválidos.")

        total_cells = enforce_silver_budget(conn)
        logger.info("Presupuesto Silver validado: %s / 100000 celdas", total_cells)

    return len(sampled), persisted


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga muestra estratificada BRFSS 2023 (20 variables) y valida presupuesto Silver."
    )
    parser.add_argument("--replace", action="store_true", help="Reemplaza la tabla dentro de la transacción.")
    args = parser.parse_args()

    validate_source_files()
    engine = create_db_engine()
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
        loaded_rows, _ = load_data(engine, sampled, run_id, args.replace)
        update_etl_run(engine, run_id, "succeeded", source_rows, loaded_rows)
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
