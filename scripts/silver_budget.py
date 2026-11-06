from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

MAX_SILVER_CELLS = 100_000
ADVISORY_LOCK_KEY = 741_2026

# Conteo lógico: las claves de JSONB cuentan como valores individuales;
# la columna contenedora record_data no se cuenta por separado.
EXPECTED_SILVER_TABLES = {
    "silver_places_2023",
    "silver_brfss_2023",
    "silver_acs_2024",
}


class SilverBudgetExceeded(RuntimeError):
    pass


def lock_silver_budget(conn: Connection) -> None:
    """Serializa las publicaciones ETL para evitar sobrepasar el presupuesto."""
    conn.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": ADVISORY_LOCK_KEY},
    )


def calculate_silver_cells(conn: Connection) -> int:
    """Cuenta celdas lógicas de las tres tablas Silver administradas."""
    found = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          AND table_name LIKE 'silver_%'
    """)).scalars().all()

    unexpected = set(found) - EXPECTED_SILVER_TABLES
    missing = EXPECTED_SILVER_TABLES - set(found)
    if unexpected:
        raise RuntimeError(
            "Hay tablas Silver fuera del presupuesto configurado: "
            + ", ".join(sorted(unexpected))
        )
    if missing:
        raise RuntimeError(
            "Faltan tablas Silver requeridas para calcular el presupuesto: "
            + ", ".join(sorted(missing))
        )

    places = int(conn.execute(text("""
        SELECT COUNT(*) * 9 FROM public.silver_places_2023
    """)).scalar_one())

    brfss = int(conn.execute(text("""
        SELECT
            (
                SELECT COUNT(*) * 4
                FROM public.silver_brfss_2023
            )
            +
            (
                SELECT COUNT(*)
                FROM public.silver_brfss_2023 AS b
                CROSS JOIN LATERAL
                    jsonb_object_keys(b.record_data) AS k(key)
            )
    """)).scalar_one())

    acs = int(conn.execute(text("""
        SELECT COUNT(*) * 7 FROM public.silver_acs_2024
    """)).scalar_one())

    return places + brfss + acs


def enforce_silver_budget(conn: Connection) -> int:
    """Falla la transacción si el total Silver excede 100.000 celdas."""
    total = calculate_silver_cells(conn)
    if total > MAX_SILVER_CELLS:
        raise SilverBudgetExceeded(
            f"Presupuesto Silver excedido: {total:,} celdas; "
            f"máximo permitido: {MAX_SILVER_CELLS:,}."
        )
    return total
