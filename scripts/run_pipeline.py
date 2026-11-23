from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv

from scripts.database_config import SCENARIOS, database_for_scenario

ROOT = Path(__file__).resolve().parents[1]
MASTER_PIPELINE_LOCK_KEY = 78_115_004_452_422
CALENDAR = {
    "daily": {"step": 1, "dates": 731},
    "every_3_days": {"step": 3, "dates": 244},
    "every_6_days": {"step": 6, "dates": 122},
}
BRONZE_DATASETS = {
    "acs_2024",
    "brfss_2023",
    "places_2025",
    "acs_2024_county_dp05",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("datalake_pipeline")


def load_configuration() -> dict[str, str]:
    load_dotenv(ROOT / ".env", override=False)
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}.")
    config = {key: os.environ[key] for key in required}
    if config["POSTGRES_HOST"] != "localhost" or int(config["POSTGRES_PORT"]) != 5432:
        raise RuntimeError("El orquestador solo permite PostgreSQL en localhost:5432.")
    return config


def selected_scenarios(scenario: str | None) -> list[str]:
    return [scenario] if scenario else list(SCENARIOS)


def admin_connection():
    import psycopg

    config = load_configuration()
    return psycopg.connect(
        host=config["POSTGRES_HOST"],
        port=int(config["POSTGRES_PORT"]),
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
        dbname="postgres",
        connect_timeout=5,
        autocommit=True,
    )


def validate_bronze_manifest(frequency: str) -> None:
    scenario_root = ROOT / "data" / "bronze" / frequency
    manifest_path = scenario_root / "metadata" / "bronze_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"La base ya existe, pero falta el manifiesto Bronze requerido: {manifest_path}. "
            "No se regenerará Bronze automáticamente."
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    calendar = CALENDAR[frequency]
    if (
        manifest.get("contract_version") != 1
        or manifest.get("frequency") != frequency
        or manifest.get("calendar_start") != "2023-01-01"
        or manifest.get("calendar_end") != "2024-12-31"
        or int(manifest.get("calendar_step_days", -1)) != calendar["step"]
        or int(manifest.get("calendar_dates", -1)) != calendar["dates"]
    ):
        raise RuntimeError(
            f"El manifiesto Bronze no corresponde a la frecuencia {frequency}."
        )

    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or set(datasets) != BRONZE_DATASETS:
        raise RuntimeError(
            f"Inventario Bronze incorrecto para {frequency}: "
            f"{sorted(datasets or {})}; esperado={sorted(BRONZE_DATASETS)}."
        )

    for dataset, entry in datasets.items():
        files = entry.get("parquet_files")
        if not isinstance(files, list) or not files:
            raise RuntimeError(f"Bronze {dataset}/{frequency} no contiene archivos Parquet.")
        total_rows = 0
        for item in files:
            relative = Path(str(item.get("path", "")))
            if relative.is_absolute() or ".." in relative.parts:
                raise RuntimeError(f"Ruta Parquet insegura en el manifiesto: {relative}.")
            target = scenario_root / relative
            if not target.is_file():
                raise FileNotFoundError(f"Falta un archivo Bronze declarado: {target}.")
            if target.stat().st_size != int(item.get("bytes", -1)):
                raise RuntimeError(f"Tamaño Bronze no coincide con el manifiesto: {target}.")
            total_rows += int(item.get("rows", -1))
        if (
            total_rows != int(entry.get("source_rows", -1))
            or total_rows != int(entry.get("parquet_rows", -1))
        ):
            raise RuntimeError(f"Conteos Bronze inconsistentes para {dataset}/{frequency}.")


def inspect_existing_database(
    database: str,
    scenario: str,
) -> str:
    import psycopg

    config = load_configuration()
    spec = SCENARIOS[scenario]
    calendar = CALENDAR[spec["frequency"]]
    with psycopg.connect(
        host=config["POSTGRES_HOST"],
        port=int(config["POSTGRES_PORT"]),
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
        dbname=database,
        connect_timeout=5,
    ) as conn:
        try:
            row = conn.execute(
                """
                SELECT scenario, frequency, quality_level, quality_status,
                       calendar_start, calendar_end, calendar_step_days, calendar_dates
                FROM datalake_control.scenario_metadata
                WHERE singleton=TRUE
                """
            ).fetchone()
        except psycopg.Error as exc:
            raise RuntimeError(
                f"La base existente {database} no contiene metadatos de escenario "
                "válidos. No se inicializará ni modificará."
            ) from exc

    expected = (
        scenario,
        spec["frequency"],
        spec["quality_level"],
        "2023-01-01",
        "2024-12-31",
        calendar["step"],
        calendar["dates"],
    )
    if row is None:
        raise RuntimeError(
            f"La base {database} existe, pero no tiene scenario_metadata. "
            "Se requiere inspección manual; no se reutilizará."
        )
    actual = (
        row[0], row[1], row[2],
        row[4].isoformat(), row[5].isoformat(),
        int(row[6]), int(row[7]),
    )
    if actual != expected:
        raise RuntimeError(
            f"Los metadatos de {database} no corresponden a {scenario}: "
            f"actual={actual}, esperado={expected}."
        )
    quality_status = row[3]
    allowed_statuses = {"pristine"} if scenario == "perfect" else {"pristine", "degraded"}
    if quality_status not in allowed_statuses:
        raise RuntimeError(
            f"Estado de calidad no permitido para {database}: {quality_status!r}."
        )
    validate_bronze_manifest(spec["frequency"])
    return str(quality_status)


def inspect_target_databases(
    scenarios: list[str],
    test_mode: bool,
) -> dict[str, dict[str, object]]:
    targets = {name: database_for_scenario(name, test_mode) for name in scenarios}
    if len(set(targets.values())) != len(targets):
        raise RuntimeError("La configuración produjo nombres de bases duplicados.")

    with admin_connection() as conn:
        existing = {
            row[0]
            for row in conn.execute("SELECT datname FROM pg_database").fetchall()
        }

    states: dict[str, dict[str, object]] = {}
    for scenario, database in targets.items():
        if database in existing:
            quality_status = inspect_existing_database(database, scenario)
            states[scenario] = {
                "database": database,
                "exists": True,
                "quality_status": quality_status,
            }
            logger.info(
                "Base existente reconocida | escenario=%s | base=%s | calidad=%s",
                scenario, database, quality_status,
            )
        else:
            states[scenario] = {
                "database": database,
                "exists": False,
                "quality_status": "pristine",
            }
            logger.info(
                "Base nueva disponible | escenario=%s | base=%s",
                scenario, database,
            )
    return states


@contextmanager
def acquire_master_pipeline_lock():
    import psycopg

    config = load_configuration()
    connection = psycopg.connect(
        host=config["POSTGRES_HOST"],
        port=int(config["POSTGRES_PORT"]),
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
        dbname="postgres",
        connect_timeout=5,
        autocommit=True,
    )
    acquired = False
    try:
        acquired = bool(
            connection.execute(
                "SELECT pg_try_advisory_lock(%s)", (MASTER_PIPELINE_LOCK_KEY,)
            ).fetchone()[0]
        )
        if not acquired:
            raise RuntimeError("Ya existe otra ejecución del orquestador maestro.")
        yield
    finally:
        if acquired:
            connection.execute(
                "SELECT pg_advisory_unlock(%s)", (MASTER_PIPELINE_LOCK_KEY,)
            )
        connection.close()


def validate_landing_sources(allow_legacy: bool = False) -> None:
    landing = ROOT / "data" / "landing"
    legacy = ROOT / "data" / "bronze"
    requirements = {
        "acs_2024": ("ACSDP*Y2024.DP05*.csv", "DP05.json"),
        "brfss_2023": ("LLCP2023.XPT", "USCODE23_LLCP_021924.HTML"),
        "places_2025": ("*.csv",),
        "acs_2024_county": (
            "acs_dp05_county_run_*Z.json",
            "acs_dp05_county_run_*.manifest.json",
        ),
    }
    for dataset, patterns in requirements.items():
        folder = landing / dataset
        for pattern in patterns:
            found = list(folder.glob(pattern)) if folder.exists() else []
            if not found and allow_legacy:
                legacy_folder = legacy / dataset
                found = list(legacy_folder.glob(pattern)) if legacy_folder.exists() else []
            if len(found) != 1:
                raise RuntimeError(
                    f"Landing incompleto para {dataset}: se requiere exactamente "
                    f"un archivo {pattern}; encontrados={len(found)}."
                )


def execute_stage(
    label: str,
    command: list[str],
    environment: dict[str, str],
) -> float:
    logger.info("=" * 72)
    logger.info("ETAPA: %s", label)
    logger.info("COMANDO: %s", " ".join(command))
    started = time.perf_counter()
    result = subprocess.run(command, cwd=ROOT, env=environment, check=False)
    duration = time.perf_counter() - started
    if result.returncode != 0:
        raise RuntimeError(
            f"La etapa {label} falló con código {result.returncode} "
            f"después de {duration:.2f} segundos. Se detuvo el pipeline."
        )
    logger.info("ETAPA COMPLETADA: %s | duración=%.2f segundos", label, duration)
    return duration


def read_quality_status(database: str) -> str:
    import psycopg

    config = load_configuration()
    with psycopg.connect(
        host=config["POSTGRES_HOST"],
        port=int(config["POSTGRES_PORT"]),
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
        dbname=database,
        connect_timeout=5,
    ) as conn:
        row = conn.execute(
            "SELECT quality_status FROM datalake_control.scenario_metadata "
            "WHERE singleton=TRUE"
        ).fetchone()
    if row is None:
        raise RuntimeError(f"No se encontraron metadatos de calidad en {database}.")
    return str(row[0])


def run_pipeline(
    scenarios: list[str],
    test_mode: bool,
    build_only: bool = False,
    ) -> None:
    with acquire_master_pipeline_lock():
        states = inspect_target_databases(scenarios, test_mode)
        base_environment = os.environ.copy()
        base_environment["DATALAKE_TEST_MODE"] = "1" if test_mode else "0"

        logger.info(
            "INICIO DEL PIPELINE INTEGRAL | escenarios=%s | test_mode=%s",
            scenarios, test_mode,
        )

        # Landing se prepara una sola vez, inmutable e idempotente
        execute_stage(
            "Landing",
            [sys.executable, "-m", "scripts.landing.prepare_landing"],
            base_environment,
        )
        validate_landing_sources()

        total_started = time.perf_counter()
        durations = []

        for scenario in scenarios:
            spec = SCENARIOS[scenario]
            state = states[scenario]
            database = str(state["database"])
            quality_status = str(state["quality_status"])
            environment = base_environment.copy()
            environment.update({
                "DATALAKE_SCENARIO": scenario,
                "DATALAKE_TEST_MODE": "1" if test_mode else "0",
                "POSTGRES_DB": database,
                "BRONZE_INGESTION_FREQUENCY": spec["frequency"],
            })

            logger.info(
                "ESCENARIO %s | base=%s | frecuencia=%s | calidad=%s",
                scenario.upper(), database, spec["frequency"], spec["quality_level"],
            )

            if not bool(state["exists"]):
                durations.append(execute_stage(
                    f"{scenario}/Bronze Simulator",
                    [
                        sys.executable, "-m", "scripts.bronze.bronze_simulator",
                        "--frequency", spec["frequency"],
                    ],
                    environment,
                ))
                durations.append(execute_stage(
                    f"{scenario}/Inicialización de base",
                    [
                        sys.executable, "-m", "scripts.initialize_database",
                        "--scenario", scenario,
                    ] + (["--test-mode"] if test_mode else []),
                    environment,
                ))
            else:
                logger.info(
                    "Bronze e inicialización omitidos: la base ya existe y sus "
                    "metadatos fueron validados."
                )

            
            if quality_status == "degraded":
                # Una base degradada debe estar totalmente construida.
                # No se permite completar Silver/Gold después de degradarla.

                # 1. Validar Silver: no ejecuta ETL.
                durations.append(
                    execute_stage(
                        f"{scenario}/Validación Silver (solo lectura)",
                        [
                            sys.executable,
                            "-m",
                            "scripts.run_silver_pipeline",
                            "--validate-only",
                        ],
                        environment,
                    )
                )

                # 2. Recuperar y verificar Ground Truth contra PostgreSQL.
                durations.append(
                    execute_stage(
                        f"{scenario}/Recuperación Ground Truth",
                        [
                            sys.executable,
                            "-m",
                            "scripts.quality.quality_generator",
                            "--level",
                            scenario,
                            "--recover-ground-truth",
                        ],
                        environment,
                    )
                )

                # 3. Validar Gold utilizando las excepciones verificadas.
                durations.append(
                    execute_stage(
                        f"{scenario}/Validación Gold (solo lectura)",
                        [
                            sys.executable,
                            "-m",
                            "scripts.run_gold_pipeline",
                            "--preflight-only",
                            "--require-complete",
                            "--allow-quality-degradation",
                        ],
                        environment,
                    )
                )

                logger.info(
                    "Escenario %s degradado: validación completa sin "
                    "reconstruir Silver, Gold ni repetir la degradación.",
                    scenario,
                )
                continue

            durations.append(execute_stage(
                f"{scenario}/Silver ETL",
                [sys.executable, "-m", "scripts.run_silver_pipeline"],
                environment,
            ))
            durations.append(execute_stage(
                f"{scenario}/Gold ETL",
                [sys.executable, "-m", "scripts.run_gold_pipeline"],
                environment,
            ))

            if scenario in ("medium", "poor") and not build_only:
                durations.append(execute_stage(
                    f"{scenario}/Quality Generator",
                    [
                        sys.executable, "-m", "scripts.quality.quality_generator",
                        "--level", scenario, "--execute",
                    ],
                    environment,
                ))
                # Verifica el CSV y garantiza que el JSONL puede reconstruirse
                # desde los cambios realmente persistidos.
                durations.append(execute_stage(
                    f"{scenario}/Validación Ground Truth",
                    [
                        sys.executable, "-m", "scripts.quality.quality_generator",
                        "--level", scenario, "--recover-ground-truth",
                    ],
                    environment,
                ))

            final_status = read_quality_status(database)
            expected_status = (
                "pristine"
                if scenario == "perfect" or build_only
                else "degraded"
            )
            if final_status != expected_status:
                raise RuntimeError(
                    f"Estado de calidad final incorrecto para {scenario}: "
                    f"{final_status}; esperado={expected_status}."
                )
            logger.info("ESCENARIO COMPLETADO Y VALIDADO: %s | base=%s", scenario, database)

        total = time.perf_counter() - total_started
        logger.info("=" * 72)
        logger.info("PIPELINE INTEGRAL COMPLETADO: %s", ", ".join(scenarios))
        logger.info("Bases utilizadas: %s", [states[x]["database"] for x in scenarios])
        logger.info("Duración total: %.2f segundos", total)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Construye o reanuda los tres datalakes: Perfect/daily, "
            "Medium/every_3_days y Poor/every_6_days."
        )
    )
    parser.add_argument(
        "--scenario", choices=tuple(SCENARIOS),
        help="Ejecuta un único escenario para diagnóstico.",
    )
    parser.add_argument(
        "--test-mode", action="store_true",
        help="Usa bases aisladas *_test; no toca las bases finales.",
    )
    parser.add_argument(
        "--preflight-only", action="store_true",
        help="Valida fuentes, manifiestos y estado de las bases sin modificar datos.",
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help=(
            "Construye Bronze, Silver y Gold sin ejecutar "
            "el Quality Generator. Deja las bases pristine."
        ),
    )
    args = parser.parse_args()
    scenarios = selected_scenarios(args.scenario)
    try:
        load_configuration()
        if args.preflight_only:
            validate_landing_sources(allow_legacy=True)
            states = inspect_target_databases(scenarios, args.test_mode)
            logger.info("Preflight aprobado | estado de bases: %s", states)
            return 0
        run_pipeline(
            scenarios,
            args.test_mode,
            build_only=args.build_only,
        )
        return 0
    except Exception as exc:
        logger.exception("PIPELINE MAESTRO DETENIDO: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
