from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Connection

MAX_SILVER_CELLS = 100_000
ADVISORY_LOCK_KEY = 741_2026
EXPECTED_SILVER_TABLES = {
    "silver_places_2023",
    "silver_brfss_2023",
    "silver_acs_2024",
    "silver_acs_county_2024",
}

class SilverBudgetExceeded(RuntimeError):
    pass


def lock_silver_budget(conn: Connection) -> None:
    conn.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": ADVISORY_LOCK_KEY})


def calculate_silver_cells(conn: Connection) -> int:
    found = conn.execute(text("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
          AND table_name LIKE 'silver_%'
    """)).scalars().all()
    unexpected = set(found) - EXPECTED_SILVER_TABLES
    missing = EXPECTED_SILVER_TABLES - set(found)
    if unexpected:
        raise RuntimeError("Tablas Silver fuera del presupuesto: " + ", ".join(sorted(unexpected)))
    if missing:
        raise RuntimeError("Faltan tablas Silver requeridas: " + ", ".join(sorted(missing)))

    places = int(conn.execute(text("SELECT COUNT(*) * 9 FROM public.silver_places_2023")).scalar_one())
    brfss = int(conn.execute(text("""
        SELECT (SELECT COUNT(*) * 4 FROM public.silver_brfss_2023)
             + (SELECT COUNT(*) FROM public.silver_brfss_2023 b
                CROSS JOIN LATERAL jsonb_object_keys(b.record_data) AS k(key))
    """)).scalar_one())
    acs_national = int(conn.execute(text("SELECT COUNT(*) * 7 FROM public.silver_acs_2024")).scalar_one())
    # Diez campos lógicos por fila; se excluye la PK identity.
    acs_county = int(conn.execute(text("SELECT COUNT(*) * 10 FROM public.silver_acs_county_2024")).scalar_one())
    return places + brfss + acs_national + acs_county


def enforce_silver_budget(conn: Connection) -> int:
    total = calculate_silver_cells(conn)
    if total > MAX_SILVER_CELLS:
        raise SilverBudgetExceeded(f"Presupuesto Silver excedido: {total:,}; máximo: {MAX_SILVER_CELLS:,}.")
    return total
