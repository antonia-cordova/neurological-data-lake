from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg
from psycopg import sql
from dotenv import load_dotenv

from scripts.database_config import SCENARIOS, database_for_scenario, get_target_database
from scripts.bronze.parquet_io import validate_dataset_manifest

ROOT = Path(__file__).resolve().parents[1]

ALLOWED_HOST = "localhost"
ALLOWED_PORT = 5432

SCHEMA_FILES = [
    ROOT / "sql" / "silver" / "schema_silver.sql",
    ROOT / "sql" / "gold" / "schema_gold.sql",
]

SILVER_TABLES = [
    "silver_places_2023",
    "silver_respondent",
    "silver_brfss_2023",
    "silver_acs_2024",
    "silver_acs_county_2024",
    "silver_source",
    "silver_health_indicator",
    "silver_geography",
    "silver_county",
]
AUDIT_TABLES = ["etl_runs"]
GOLD_TABLES = [
    "dim_source",
    "dim_geography",
    "dim_health_measure",
    "dim_prevalence_type",
    "fact_area_health",
    "dim_acs_indicator",
    "fact_acs_indicator",
    "dim_acs_county_indicator",
    "fact_acs_county_indicator",
    "dim_brfss_variable",
    "dim_brfss_response_code",
    "dim_brfss_jurisdiction",
    "fact_brfss_response",
    "bridge_brfss_geography",
    "dim_health_integrated_indicator",
    "bridge_health_indicator_source",
    "fact_health_integrated",
]

EXPECTED_TABLES = {
    *(("silver", name) for name in SILVER_TABLES),
    ("datalake_control", "etl_runs"),
    ("datalake_control", "ingestion_batches"),
    ("datalake_control", "scenario_metadata"),
    *(("gold", name) for name in GOLD_TABLES),
}

EXPECTED_TABLE_COUNT = 29


def get_admin_connection():
    load_dotenv(ROOT / ".env", override=False)
    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    if host != ALLOWED_HOST or port != ALLOWED_PORT:
        raise RuntimeError(
            f"Destino rechazado: solo {ALLOWED_HOST}:{ALLOWED_PORT}; "
            f"se recibió {host}:{port}."
        )
    return psycopg.connect(
        host=host, port=port, dbname="postgres",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5, autocommit=True,
    )


def get_connection():
    load_dotenv(ROOT / ".env", override=False)
    database = get_target_database()
    return psycopg.connect(
        host=ALLOWED_HOST, port=ALLOWED_PORT, dbname=database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"], connect_timeout=5,
    )


def ensure_database_exists(database: str) -> None:
    with get_admin_connection() as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (database,)
        ).fetchone()
        if exists:
            raise RuntimeError(
                f"La base {database} ya existe. El inicializador no la "
                "sobrescribe, reinicia ni reutiliza."
            )
        conn.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
        )
        print(f"[CREATE DATABASE] {database}")


def get_existing_tables(conn) -> set[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
           WHERE table_schema IN (
                'public',
                'silver',
                'gold',
                'datalake_control'
            )
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        )
        return {(row[0], row[1]) for row in cur.fetchall()}


def create_scenario_metadata(conn, scenario: str, frequency: str, quality_level: str) -> None:
    scenario_root = ROOT / "data" / "bronze" / frequency
    manifest_path = scenario_root / "metadata" / "bronze_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"No existe el manifiesto Bronze del escenario: {manifest_path}")
    import json
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("contract_version") != 1 or manifest.get("frequency") != frequency:
        raise RuntimeError("El manifiesto Bronze no corresponde al escenario solicitado.")
    expected_frequency = SCENARIOS[scenario]["frequency"]
    expected_step_days = {"daily": 1, "every_3_days": 3, "every_6_days": 6}[expected_frequency]
    expected_days = {"daily": 731, "every_3_days": 244, "every_6_days": 122}[expected_frequency]
    if manifest.get("calendar_start") != "2023-01-01" or manifest.get("calendar_end") != "2024-12-31":
        raise RuntimeError("El calendario Bronze no cubre el período contractual 2023-01-01 a 2024-12-31.")
    if int(manifest.get("calendar_step_days", -1)) != expected_step_days:
        raise RuntimeError("El paso de días del calendario Bronze no corresponde a la frecuencia.")
    if int(manifest.get("calendar_dates", -1)) != expected_days:
        raise RuntimeError(
            f"Calendario Bronze incorrecto para {scenario}: "
            f"{manifest.get('calendar_dates')} fechas; se esperaban {expected_days}."
        )

    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or not datasets:
        raise RuntimeError("El manifiesto Bronze no contiene datasets.")
    batch_rows = []
    for dataset_name, entry in datasets.items():
        validate_dataset_manifest(dataset_name)
        files = entry.get("parquet_files", [])
        if sum(int(item["rows"]) for item in files) != int(entry["source_rows"]):
            raise RuntimeError(f"Conteo de lotes no coincide para {dataset_name}.")
        for item in files:
            batch_date = item.get("simulated_received_date")
            if not batch_date:
                raise RuntimeError(
                    f"Falta simulated_received_date en {dataset_name}/{item.get('path')}."
                )
            batch_rows.append((
                dataset_name,
                frequency,
                batch_date,
                item["path"],
                int(item["rows"]),
                int(item["bytes"]),
                item["sha256"],
            ))

    conn.execute("CREATE SCHEMA IF NOT EXISTS datalake_control")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS datalake_control.scenario_metadata (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            scenario TEXT NOT NULL CHECK (scenario IN ('perfect','medium','poor')),
            frequency TEXT NOT NULL CHECK (frequency IN ('daily','every_3_days','every_6_days')),
            quality_level TEXT NOT NULL CHECK (quality_level IN ('perfect','medium','poor')),
            quality_status TEXT NOT NULL CHECK (quality_status IN ('pristine','degraded')),
            calendar_start DATE NOT NULL,
            calendar_end DATE NOT NULL,
            calendar_step_days INTEGER NOT NULL CHECK (calendar_step_days IN (1,3,6)),
            calendar_dates INTEGER NOT NULL CHECK (calendar_dates > 0),
            initialized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            degraded_at TIMESTAMPTZ NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS datalake_control.ingestion_batches (
            batch_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            dataset_name TEXT NOT NULL,
            frequency TEXT NOT NULL CHECK (frequency IN ('daily','every_3_days','every_6_days')),
            simulated_received_date DATE NOT NULL,
            parquet_path TEXT NOT NULL UNIQUE,
            record_count INTEGER NOT NULL CHECK (record_count > 0),
            file_bytes BIGINT NOT NULL CHECK (file_bytes > 0),
            sha256 CHAR(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$')
        )
    """)
    conn.execute(
        "INSERT INTO datalake_control.scenario_metadata "
        "(singleton, scenario, frequency, quality_level, quality_status, "
        "calendar_start, calendar_end, calendar_step_days, calendar_dates) "
        "VALUES (TRUE, %s, %s, %s, 'pristine', %s, %s, %s, %s)",
        (
            scenario, frequency, quality_level,
            manifest["calendar_start"], manifest["calendar_end"],
            int(manifest["calendar_step_days"]), int(manifest["calendar_dates"]),
        ),
    )
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO datalake_control.ingestion_batches "
            "(dataset_name, frequency, simulated_received_date, parquet_path, "
            "record_count, file_bytes, sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            batch_rows,
        )


def read_schema(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(f"No existe el archivo de esquema: {path}")

    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        # abos archivos se ejecutan dentro de una única transacción
        # administrada por este inicializador
        if line.strip().upper() in ("BEGIN;", "COMMIT;"):
            continue
        lines.append(line)

    return "\n".join(lines)


def validate_tables(conn) -> None:
    if len(EXPECTED_TABLES) != EXPECTED_TABLE_COUNT:
        raise RuntimeError(
            f"Inventario incorrecto: {len(EXPECTED_TABLES)} tablas únicas; "
            f"se esperaban {EXPECTED_TABLE_COUNT}."
        )

    actual = get_existing_tables(conn)
    missing = sorted(EXPECTED_TABLES - actual)
    unexpected = sorted(actual - EXPECTED_TABLES)

    if missing or unexpected:
        raise RuntimeError(
            "Validación estructural fallida.\n"
            f"Esperadas: {EXPECTED_TABLE_COUNT}\n"
            f"Encontradas: {len(actual)}\n"
            f"Faltantes: {missing}\n"
            f"Inesperadas: {unexpected}"
        )

    print("\n[OK] Validación estructural completada.")
    print(f"[OK] Tablas Silver: {len(SILVER_TABLES)}")
    print(f"[OK] Tablas Gold: {len(GOLD_TABLES)}")
    print(f"[OK] Tablas de auditoría: {len(AUDIT_TABLES)} (etl_runs)")
    print(f"[OK] Total de tablas verificadas: {EXPECTED_TABLE_COUNT}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Crea e estructura una de las tres bases del neurological-data-lake."
    )
    parser.add_argument("--scenario", choices=tuple(SCENARIOS), required=True)
    parser.add_argument("--test-mode", action="store_true")
    args = parser.parse_args()

    os.environ["DATALAKE_SCENARIO"] = args.scenario
    os.environ["DATALAKE_TEST_MODE"] = "1" if args.test_mode else "0"
    spec = SCENARIOS[args.scenario]
    os.environ["POSTGRES_DB"] = database_for_scenario(args.scenario, args.test_mode)
    os.environ["BRONZE_INGESTION_FREQUENCY"] = spec["frequency"]
    database = get_target_database()

    schemas = [(path, read_schema(path)) for path in SCHEMA_FILES]
    ensure_database_exists(database)
    conn = get_connection()
    try:
        existing_tables = get_existing_tables(conn)
        unexpected = sorted(existing_tables - EXPECTED_TABLES)
        if unexpected:
            raise RuntimeError(
                f"La base {database} contiene tablas inesperadas: {unexpected}."
            )
        if existing_tables:
            raise RuntimeError(
                f"La base {database} ya contiene tablas. No se sobrescribirá."
            )
        #  consultas de inspección abrieron una transacción implícita
        # Se confirma antes de iniciar la transacción atómica de los esquemas.
        conn.commit()
        with conn.transaction():
            with conn.cursor() as cur:
                for path, schema_text in schemas:
                    print(f"[APPLY] {path.relative_to(ROOT)}")
                    cur.execute(schema_text)
                    print(f"[OK] {path.name}")
            create_scenario_metadata(
                conn, args.scenario, spec["frequency"], spec["quality_level"]
            )
            validate_tables(conn)
        print(
            f"Inicialización completada: {database} | "
            f"frecuencia={spec['frequency']} | calidad={spec['quality_level']}"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
