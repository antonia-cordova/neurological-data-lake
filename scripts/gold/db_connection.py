import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[2]

EXPECTED_DATABASE = "health_datalake"
EXPECTED_HOST = "localhost"
EXPECTED_PORT = 5432


def create_gold_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    required = [
        "POSTGRES_DB",
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
            f"Faltan variables de conexi?n: {', '.join(missing)}"
        )

    database = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST")

    try:
        port = int(os.environ["POSTGRES_PORT"])
    except ValueError as exc:
        raise RuntimeError(
            "POSTGRES_PORT debe ser un n?mero entero."
        ) from exc

    if database != EXPECTED_DATABASE:
        raise RuntimeError(
            f"Conexi?n bloqueada: base '{database}'. "
            f"Solo se permite '{EXPECTED_DATABASE}'."
        )

    if host != EXPECTED_HOST:
        raise RuntimeError(
            f"Conexi?n bloqueada: host '{host}'. "
            f"Solo se permite '{EXPECTED_HOST}'."
        )

    if port != EXPECTED_PORT:
        raise RuntimeError(
            f"Conexi?n bloqueada: puerto {port}. "
            f"Solo se permite {EXPECTED_PORT}."
        )

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

            if actual_database != EXPECTED_DATABASE:
                raise RuntimeError(
                    "Conexi?n rechazada: PostgreSQL respondi? "
                    f"con la base '{actual_database}'."
                )

        return engine

    except Exception:
        engine.dispose()
        raise
