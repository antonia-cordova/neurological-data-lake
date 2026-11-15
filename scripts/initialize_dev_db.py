from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg import sql
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]

# Guardas deliberadamente fijas: este inicializador no debe operar
# sobre la base de reproducción ni sobre las bases degradadas.
ALLOWED_DATABASE = "health_datalake"
ALLOWED_HOST = "localhost"
ALLOWED_PORT = 5432
BACKUP_FILE = ROOT / "backups" / "health_datalake_backup.dump"

# Fuentes únicas para la creación de estructuras.
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
    *(("public", name) for name in SILVER_TABLES + AUDIT_TABLES),
    *(("gold", name) for name in GOLD_TABLES),
}
EXPECTED_TABLE_COUNT = 27


def get_connection():
    load_dotenv(ROOT / ".env")

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "")

    if host != ALLOWED_HOST or port != ALLOWED_PORT:
        raise RuntimeError(
            f"Destino rechazado: solo se permite {ALLOWED_HOST}:{ALLOWED_PORT}; "
            f"se recibió {host}:{port}."
        )

    if database != ALLOWED_DATABASE:
        raise RuntimeError(
            f"Base rechazada: solo se permite {ALLOWED_DATABASE}; "
            f"POSTGRES_DB={database!r}."
        )

    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}")

    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )

    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user")
        actual_database, actual_user = cur.fetchone()

    if actual_database != ALLOWED_DATABASE:
        conn.close()
        raise RuntimeError(f"Conexión rechazada: base efectiva {actual_database!r}.")

    print(f"Usuario PostgreSQL: {actual_user}")
    return conn


def get_existing_tables(conn) -> set[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('public', 'gold')
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        )
        return {(row[0], row[1]) for row in cur.fetchall()}


def validate_backup() -> None:
    if not BACKUP_FILE.is_file():
        raise RuntimeError(
            "No se encontró el respaldo requerido: "
            f"{BACKUP_FILE}. No se reiniciará una base existente."
        )

    size = BACKUP_FILE.stat().st_size
    if size <= 0:
        raise RuntimeError(f"El respaldo está vacío: {BACKUP_FILE}")

    print(
        f"[OK] Respaldo local encontrado: {BACKUP_FILE.relative_to(ROOT)} "
        f"({size:,} bytes)."
    )
    print(
        "[INFO] Se asume que el archivo fue validado con pg_restore --list "
        "antes de ejecutar el inicializador."
    )


def read_schema(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(f"No existe el archivo de esquema: {path}")

    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        # Ambos archivos se ejecutan dentro de una única transacción
        # administrada por este inicializador.
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
    print(f"Destino autorizado: {ALLOWED_DATABASE}")
    print(f"Servidor autorizado: {ALLOWED_HOST}:{ALLOWED_PORT}")

    # Leer y comprobar los dos archivos antes de tocar la base.
    schemas = [(path, read_schema(path)) for path in SCHEMA_FILES]

    conn = get_connection()
    try:
        existing_tables = get_existing_tables(conn)

        if existing_tables:
            unexpected = sorted(existing_tables - EXPECTED_TABLES)
            if unexpected:
                raise RuntimeError(
                    "Reinicio cancelado: existen tablas fuera del inventario "
                    "esperado. No se eliminó ni modificó ningún objeto.\n"
                    f"Tablas inesperadas: {unexpected}"
                )

            validate_backup()
            print(f"Base objetivo: {ALLOWED_DATABASE}")
            print(f"Tablas existentes que se reconstruirán: {len(existing_tables)}")

            confirmation = input(
                "Para confirmar, escribe exactamente "
                "'RESET health_datalake': "
            ).strip()

            if confirmation != "RESET health_datalake":
                raise RuntimeError(
                    "Reinicio cancelado por el usuario. No se modificó la base."
                )
        else:
            print("[INFO] No hay tablas previas en public/gold.")

        # Cerrar la transacción implícita abierta por la consulta de
        # inspección. Así conn.transaction() abre una transacción real
        # (no un savepoint anidado) y confirma el bloque completo al salir.
        conn.commit()

        # Operación atómica: DROP + creación Silver + creación Gold +
        # validación se confirman juntas. Si cualquier paso falla,
        # PostgreSQL revierte la transacción y conserva el estado previo.
        with conn.transaction():
            with conn.cursor() as cur:
                for schema_name, table_name in sorted(existing_tables, reverse=True):
                    cur.execute(
                        sql.SQL("DROP TABLE {}.{} CASCADE").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                        )
                    )
                    print(f"[DROP] {schema_name}.{table_name}")

                for path, schema_text in schemas:
                    print(f"[APPLY] {path.relative_to(ROOT)}")
                    cur.execute(schema_text)
                    print(f"[OK] {path.name}")

            validate_tables(conn)

        print(
            f"\nInicialización finalizada correctamente en {ALLOWED_DATABASE}."
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
