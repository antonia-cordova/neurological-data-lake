from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from silver_budget import enforce_silver_budget, lock_silver_budget

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "data" / "bronze" / "acs_2024"

DATASET_NAME = "acs_2024"
TARGET_TABLE = "silver_acs_2024"
EXPECTED_SOURCE_COLUMNS = [
    "Label (Grouping)",
    "United States!!Estimate",
    "United States!!Margin of Error",
    "United States!!Percent",
    "United States!!Percent Margin of Error",
]
EXPECTED_SOURCE_ROWS = 113
INSERT_BATCH_SIZE = 500

COLUMN_MAPPING = {
    "Label (Grouping)": "label_grouping",
    "United States!!Estimate": "estimate",
    "United States!!Margin of Error": "margin_of_error",
    "United States!!Percent": "percent",
    "United States!!Percent Margin of Error": "percent_margin_of_error",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("etl_acs")


def locate_source_file(explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = ROOT / path
        if not path.is_file():
            raise FileNotFoundError(f"No se encontró el CSV ACS: {path}")
        return path

    candidates = sorted(SOURCE_DIR.glob("ACSDP*Y2024.DP05*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No se encontró ACSDP*Y2024.DP05*.csv en {SOURCE_DIR}"
        )
    if len(candidates) > 1:
        names = "\n".join(f" - {p.name}" for p in candidates)
        raise RuntimeError(
            "Se encontraron varios CSV ACS DP05 2024. Indica el archivo "
            "correcto mediante --source:\n" + names
        )
    return candidates[0]


def create_db_engine() -> Engine:
    load_dotenv(ROOT / ".env")
    required = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}")

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
    )
    return create_engine(url, pool_pre_ping=True)


def validate_source_file(source_file: Path) -> pd.DataFrame:
    # Se conservan como texto los símbolos ACS: *****, (X), ±, comas y %.
    df = pd.read_csv(
        source_file,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )

    actual_columns = list(df.columns)
    if actual_columns != EXPECTED_SOURCE_COLUMNS:
        raise ValueError(
            "Las columnas del CSV no coinciden con el formato esperado.\n"
            f"Esperadas: {EXPECTED_SOURCE_COLUMNS}\n"
            f"Encontradas: {actual_columns}"
        )

    if len(df) != EXPECTED_SOURCE_ROWS:
        raise ValueError(
            f"Cantidad de filas inesperada: {len(df)}; "
            f"se esperaban {EXPECTED_SOURCE_ROWS}."
        )

    # Se conserva literalmente la etiqueta, incluidos posibles espacios
    # iniciales que pueden representar jerarquía visual en el documento ACS.
    if df["Label (Grouping)"].str.strip().eq("").any():
        bad_rows = df.index[df["Label (Grouping)"].str.strip().eq("")].tolist()[:10]
        raise ValueError(f"Etiquetas vacías. Índices de ejemplo: {bad_rows}")

    return df


def validate_target_schema(engine: Engine) -> None:
    query = text("""
        SELECT column_name, data_type, is_nullable, is_identity
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table_name
        ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"table_name": TARGET_TABLE}).mappings().all()

    expected = {
        "acs_row_id": ("bigint", "NO", "YES"),
        "label_grouping": ("text", "NO", "NO"),
        "estimate": ("text", "YES", "NO"),
        "margin_of_error": ("text", "YES", "NO"),
        "percent": ("text", "YES", "NO"),
        "percent_margin_of_error": ("text", "YES", "NO"),
        "run_id": ("bigint", "NO", "NO"),
    }
    actual = {
        row["column_name"]: (
            row["data_type"], row["is_nullable"], row["is_identity"]
        )
        for row in rows
    }
    if actual != expected:
        raise RuntimeError(
            f"Esquema inesperado en public.{TARGET_TABLE}.\n"
            f"Esperado: {expected}\nActual: {actual}"
        )


def create_etl_run(engine: Engine) -> int:
    stmt = text("""
        INSERT INTO public.etl_runs
            (dataset_name, target_table, started_at, status, source_rows, loaded_rows)
        VALUES
            (:dataset_name, :target_table, NOW(), 'running', 0, 0)
        RETURNING run_id
    """)
    with engine.begin() as conn:
        run_id = conn.execute(
            stmt,
            {"dataset_name": DATASET_NAME, "target_table": TARGET_TABLE},
        ).scalar_one()
    return int(run_id)


def update_etl_run(
    engine: Engine,
    run_id: int,
    status: str,
    source_rows: int,
    loaded_rows: int,
    error_message: str | None = None,
) -> None:
    stmt = text("""
        UPDATE public.etl_runs
        SET finished_at = NOW(),
            status = :status,
            source_rows = :source_rows,
            loaded_rows = :loaded_rows,
            error_message = :error_message
        WHERE run_id = :run_id
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "run_id": run_id,
            "status": status,
            "source_rows": source_rows,
            "loaded_rows": loaded_rows,
            "error_message": error_message,
        })


def prepare_records(df: pd.DataFrame, run_id: int) -> list[dict]:
    records = []
    for source_row in df.to_dict(orient="records"):
        record = {}
        for source_column, target_column in COLUMN_MAPPING.items():
            value = source_row[source_column]
            record[target_column] = None if value == "" else value
        record["run_id"] = run_id
        records.append(record)
    return records


def load_data(
    engine: Engine,
    source_df: pd.DataFrame,
    run_id: int,
    replace: bool,
) -> tuple[int, int]:
    source_rows = len(source_df)
    loaded_rows = 0
    records = prepare_records(source_df, run_id)

    insert_stmt = text("""
        INSERT INTO public.silver_acs_2024
            (label_grouping, estimate, margin_of_error, percent,
             percent_margin_of_error, run_id)
        VALUES
            (:label_grouping, :estimate, :margin_of_error, :percent,
             :percent_margin_of_error, :run_id)
    """)

    with engine.begin() as conn:
        lock_silver_budget(conn)
        existing = int(
            conn.execute(text("SELECT COUNT(*) FROM public.silver_acs_2024")).scalar_one()
        )
        if existing and not replace:
            raise RuntimeError(
                f"silver_acs_2024 ya contiene {existing} registros. "
                "Usa --replace solo si deseas reemplazar toda la tabla."
            )
        if replace:
            logger.warning("Se reemplazarán los registros existentes de silver_acs_2024.")
            conn.execute(text("DELETE FROM public.silver_acs_2024"))

        for start in range(0, len(records), INSERT_BATCH_SIZE):
            batch = records[start:start + INSERT_BATCH_SIZE]
            conn.execute(insert_stmt, batch)
            loaded_rows += len(batch)
            logger.info(
                "Lote %s | leídos: %s | cargados: %s",
                start // INSERT_BATCH_SIZE + 1, source_rows, loaded_rows,
            )

        if loaded_rows != source_rows:
            raise RuntimeError(f"Leídos={source_rows}, cargados={loaded_rows}.")

        persisted = int(conn.execute(
            text("""
                SELECT COUNT(*) FROM public.silver_acs_2024
                WHERE run_id = :run_id
            """),
            {"run_id": run_id},
        ).scalar_one())
        if persisted != loaded_rows:
            raise RuntimeError(
                f"Persistidos para run_id={run_id}: {persisted}; cargados: {loaded_rows}."
            )

        invalid_labels = int(conn.execute(
            text("""
                SELECT COUNT(*) FROM public.silver_acs_2024
                WHERE run_id = :run_id
                  AND (label_grouping IS NULL OR BTRIM(label_grouping) = '')
            """),
            {"run_id": run_id},
        ).scalar_one())
        if invalid_labels:
            raise RuntimeError(f"Se detectaron {invalid_labels} etiquetas vacías o nulas.")

        total_cells = enforce_silver_budget(conn)
        logger.info("Presupuesto Silver validado: %s / 100000 celdas", total_cells)

    return source_rows, loaded_rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga ACS 2024 DP05 y valida el presupuesto global de 100.000 celdas Silver."
    )
    parser.add_argument(
        "--source",
        help="Ruta del CSV. Si se omite, se busca ACSDP*Y2024.DP05*.csv en data/bronze/acs_2024.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Reemplaza los registros existentes dentro de la transacción de carga.",
    )
    args = parser.parse_args()

    engine = None
    run_id = None
    source_rows = 0
    loaded_rows = 0

    try:
        source_file = locate_source_file(args.source)
        try:
            display_path = source_file.relative_to(ROOT)
        except ValueError:
            display_path = source_file
        logger.info("Archivo Bronze: %s", display_path)

        source_df = validate_source_file(source_file)
        source_rows = len(source_df)
        logger.info(
            "Archivo validado | filas=%s | columnas=%s",
            source_rows, len(source_df.columns),
        )

        engine = create_db_engine()
        validate_target_schema(engine)

        with engine.connect() as conn:
            existing = int(
                conn.execute(text("SELECT COUNT(*) FROM public.silver_acs_2024")).scalar_one()
            )
        if existing and not args.replace:
            raise RuntimeError(
                f"silver_acs_2024 ya contiene {existing} registros. "
                "Revisa la tabla o ejecuta con --replace para recargarla."
            )

        run_id = create_etl_run(engine)
        logger.info("Ejecución ETL iniciada. run_id=%s", run_id)

        source_rows, loaded_rows = load_data(
            engine, source_df, run_id, args.replace
        )
        update_etl_run(engine, run_id, "succeeded", source_rows, loaded_rows)

        logger.info(
            "ETL finalizado correctamente | run_id=%s | origen=%s | cargados=%s",
            run_id, source_rows, loaded_rows,
        )
        return 0

    except Exception as exc:
        logger.exception("Falló el ETL de ACS 2024.")
        if engine is not None and run_id is not None:
            try:
                update_etl_run(
                    engine, run_id, "failed", source_rows, loaded_rows, str(exc)[:4000]
                )
            except Exception:
                logger.exception("No fue posible actualizar la auditoría ETL.")
        return 1

    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
