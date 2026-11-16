from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]

SCENARIOS = {
    "perfect": {
        "database": "health_datalake",
        "frequency": "daily",
        "quality_level": "perfect",
    },
    "medium": {
        "database": "health_datalake_medium",
        "frequency": "every_3_days",
        "quality_level": "medium",
    },
    "poor": {
        "database": "health_datalake_poor",
        "frequency": "every_6_days",
        "quality_level": "poor",
    },
}
TEST_SUFFIX = "_test"


def is_test_mode() -> bool:
    return os.getenv("DATALAKE_TEST_MODE", "0").strip().lower() in {"1", "true", "yes"}


def database_for_scenario(scenario: str, test_mode: bool | None = None) -> str:
    if scenario not in SCENARIOS:
        raise ValueError(f"Escenario no válido: {scenario!r}. Valores: {tuple(SCENARIOS)}")
    test = is_test_mode() if test_mode is None else test_mode
    database = SCENARIOS[scenario]["database"]
    return f"{database}{TEST_SUFFIX}" if test else database


def resolve_scenario() -> str:
    load_dotenv(ROOT / ".env", override=False)
    scenario = os.getenv("DATALAKE_SCENARIO")
    database = os.getenv("POSTGRES_DB")
    test = is_test_mode()
    if scenario is None:
        for candidate in SCENARIOS:
            if database == database_for_scenario(candidate, test):
                scenario = candidate
                break
    if scenario not in SCENARIOS:
        raise RuntimeError(
            "DATALAKE_SCENARIO debe ser perfect, medium o poor; "
            f"valor recibido: {scenario!r}."
        )
    expected = database_for_scenario(scenario, test)
    if database != expected:
        raise RuntimeError(
            f"Base no autorizada para {scenario}: se esperaba {expected!r}, "
            f"se recibió {database!r}."
        )
    frequency = os.getenv("BRONZE_INGESTION_FREQUENCY")
    expected_frequency = SCENARIOS[scenario]["frequency"]
    if frequency and frequency != expected_frequency:
        raise RuntimeError(
            f"Frecuencia incompatible con {scenario}: se esperaba "
            f"{expected_frequency!r}, se recibió {frequency!r}."
        )
    host = os.getenv("POSTGRES_HOST", "localhost")
    try:
        port = int(os.getenv("POSTGRES_PORT", "5432"))
    except ValueError as exc:
        raise RuntimeError("POSTGRES_PORT debe ser un entero.") from exc
    if host != "localhost" or port != 5432:
        raise RuntimeError(
            f"Servidor PostgreSQL no autorizado: {host}:{port}; "
            "solo se permite localhost:5432."
        )
    return scenario


def get_target_database() -> str:
    load_dotenv(ROOT / ".env", override=False)
    scenario = resolve_scenario()
    return database_for_scenario(scenario)


def get_scenario_spec(scenario: str | None = None) -> dict[str, str]:
    selected = scenario or resolve_scenario()
    if selected not in SCENARIOS:
        raise ValueError(f"Escenario no válido: {selected!r}")
    spec = dict(SCENARIOS[selected])
    spec["scenario"] = selected
    spec["database"] = database_for_scenario(selected)
    return spec
