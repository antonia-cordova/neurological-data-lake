
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from scripts.database_config import get_target_database

ROOT = Path(__file__).resolve().parents[1]


def create_database_engine() -> Engine:
    """
    Crea y valida un SQLAlchemy Engine para la base autorizada
    por el escenario seleccionado.

    La autorización del escenario, base, host y puerto se delega
    en database_config.py.
    """

    load_dotenv(ROOT / ".env", override=False)

    # Resuelve y valida el destino autorizado.
    database = get_target_database()

    required = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    ]

    missing = [
        key for key in required
        if not os.getenv(key)
    ]

    if missing:
        raise RuntimeError(
            f"Faltan variables de conexión PostgreSQL: {', '.join(missing)}"
        )

    host = os.environ["POSTGRES_HOST"]

    try:
        port = int(os.environ["POSTGRES_PORT"])
    except ValueError as exc:
        raise RuntimeError(
            "POSTGRES_PORT debe ser un número entero."
        ) from exc

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=host,
        port=port,
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

            if actual_database != database:
                raise RuntimeError(
                    "Conexión rechazada: PostgreSQL respondió "
                    f"con la base '{actual_database}', "
                    f"pero se esperaba '{database}'."
                )

        return engine

    except Exception:
        engine.dispose()
        raise
