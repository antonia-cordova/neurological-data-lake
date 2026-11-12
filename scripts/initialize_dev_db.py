from __future__ import annotations
import os
from pathlib import Path
import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]

ALLOWED_DATABASE = "health_datalake"
ALLOWED_HOST = "localhost"
ALLOWED_PORT = 5432

SCHEMA_FILES = [
    ROOT / "sql" / "silver" / "schema_silver.sql",

    ROOT / "sql" / "gold" / "schema_gold.sql",
]

EXPECTED_TABLES = [
    # SILVER
    ("public", "etl_runs"),
    ("public", "silver_places_2023"),
    ("public", "silver_brfss_2023"),
    ("public", "silver_acs_2024"),
    ("public", "silver_acs_county_2024"),

    # GOLD GENERAL
    ("gold", "dim_source"),
    ("gold", "dim_geography"),
    ("gold", "dim_health_measure"),
    ("gold", "dim_prevalence_type"),
    ("gold", "fact_area_health"),

    # GOLD ACS NACIONAL
    ("gold", "dim_acs_indicator"),
    ("gold", "fact_acs_indicator"),

    # GOLD ACS COUNTY
    ("gold", "dim_acs_county_indicator"),
    ("gold", "fact_acs_county_indicator"),

    # GOLD BRFSS
    ("gold", "dim_brfss_variable"),
    ("gold", "dim_brfss_response_code"),
    ("gold", "dim_brfss_jurisdiction"),
    ("gold", "fact_brfss_response"),
    ("gold", "bridge_brfss_geography"),

    # GOLD INTEGRATED
    ("gold", "dim_health_integrated_indicator"),
    ("gold", "bridge_health_indicator_source"),
    ("gold", "fact_health_integrated"),
]
EXPECTED_TABLE_COUNT = 22

def get_connection():
    if ALLOWED_DATABASE != "health_datalake":
        raise RuntimeError("Destino no autorizado.")

    load_dotenv(ROOT / ".env")

    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]

    if missing:
        raise RuntimeError(
            f"Faltan variables en .env: {', '.join(missing)}"
        )

    conn = psycopg.connect(
        host=ALLOWED_HOST,
        port=ALLOWED_PORT,
        dbname=ALLOWED_DATABASE,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )

    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        actual_database = cur.fetchone()[0]

    if actual_database != ALLOWED_DATABASE:
        conn.close()
        raise RuntimeError(
            f"Conexión rechazada: {actual_database}"
        )

    return conn


def execute_schema(conn, path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(
            f"No existe el archivo: {path}"
        )

    sql = path.read_text(encoding="utf-8")

    # Los esquemas ACS y BRFSS contienen sus propios
    # BEGIN/COMMIT. Se eliminan para que cada archivo
    # tenga una única transacción administrada aquí.
    lines = []

    for line in sql.splitlines():
        statement = line.strip().upper()

        if statement in ("BEGIN;", "COMMIT;"):
            continue

        lines.append(line)

    sql = "\n".join(lines)

    print(f"[APPLY] {path.relative_to(ROOT)}")

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(sql)

    print(f"[OK] {path.name}")


def validate_tables(conn) -> None:
    expected_tables = set(EXPECTED_TABLES)

    if len(expected_tables) != EXPECTED_TABLE_COUNT:
        raise RuntimeError(
            f"Configuración incorrecta: se declararon "
            f"{len(expected_tables)} tablas únicas; "
            f"se esperaban {EXPECTED_TABLE_COUNT}."
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                table_schema,
                table_name
            FROM information_schema.tables
            WHERE table_schema IN ('public', 'gold')
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        )

        actual_tables = {
            (row[0], row[1])
            for row in cur.fetchall()
        }

    missing_tables = sorted(expected_tables - actual_tables)
    unexpected_tables = sorted(actual_tables - expected_tables)

    if missing_tables or unexpected_tables:
        raise RuntimeError(
            "Validación de estructura fallida.\n"
            f"Tablas esperadas: {EXPECTED_TABLE_COUNT}\n"
            f"Tablas encontradas: {len(actual_tables)}\n"
            f"Tablas faltantes: {missing_tables}\n"
            f"Tablas inesperadas: {unexpected_tables}"
        )

    print("\n[OK] Validación estructural completada.")
    print(f"[OK] Tablas Silver: 5")
    print(f"[OK] Tablas Gold: 17")
    print(f"[OK] Total de tablas verificadas: {EXPECTED_TABLE_COUNT}")


def main() -> int:
    print(f"Destino autorizado: {ALLOWED_DATABASE}")
    print(f"Servidor autorizado: {ALLOWED_HOST}:{ALLOWED_PORT}")

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema IN ('public', 'gold')
                """
            )

            existing_tables = cur.fetchone()[0]

        if existing_tables > 0:
            raise RuntimeError(
                "La base de desarrollo no está vacía. "
                "No se ejecutará la inicialización."
            )

        # Cerrar la transacción de lectura antes de iniciar
        # las transacciones independientes de cada esquema.
        conn.commit()

        for path in SCHEMA_FILES:
            execute_schema(conn, path)

        # Validar las 22 tablas después de aplicar
        # todos los archivos de esquema.
        validate_tables(conn)

        print(
            f"\nInicialización finalizada correctamente "
            f"en {ALLOWED_DATABASE}."
        )

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())