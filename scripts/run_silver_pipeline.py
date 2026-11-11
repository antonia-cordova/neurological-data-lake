from __future__ import annotations
import argparse
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("silver_pipeline")


@dataclass(frozen=True)
class ETLJob:
    name: str
    module: str
    rebuild_args: tuple[str, ...]


# Orden de ejecución determinado por las dependencias entre tablas.
JOBS = {
    "acs": ETLJob(
        name="ACS nacional",
        module="scripts.silver.etl_acs",
        rebuild_args=("--replace",),
    ),
    "brfss": ETLJob(
        name="BRFSS",
        module="scripts.silver.etl_brfss",
        rebuild_args=("--replace",),
    ),
    "places": ETLJob(
        name="PLACES",
        module="scripts.silver.etl_places",
        rebuild_args=("--replace",),
    ),
    "acs_county": ETLJob(
        name="ACS County",
        module="scripts.silver.etl_acs_county",
        rebuild_args=("--replace-existing",),
    ),
}


def validate_target_configuration() -> None:
    """
    Impide ejecutar el pipeline si la configuración
    no corresponde a la base de reproducción.
    """
    load_dotenv(ROOT / ".env", override=False)

    expected = {
        "POSTGRES_DB": "health_datalake",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
    }

    for variable, expected_value in expected.items():
        actual_value = os.getenv(variable)

        if actual_value != expected_value:
            raise RuntimeError(
                f"Configuración PostgreSQL rechazada: "
                f"{variable} debe ser {expected_value!r}; "
                f"valor actual: {actual_value!r}."
            )

    required = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]

    missing = [
        variable
        for variable in required
        if not os.getenv(variable)
    ]

    if missing:
        raise RuntimeError(
            f"Faltan variables PostgreSQL: {', '.join(missing)}"
        )

    logger.info(
        "Configuración de reproducción validada: "
        "localhost:5432 / health_datalake"
    )


def execute_job(
    job: ETLJob,
    rebuild: bool = False,
) -> tuple[bool, float]:

    logger.info("=" * 65)
    logger.info("INICIANDO ETL: %s", job.name)
    logger.info("=" * 65)

    command = [
        sys.executable,
        "-m",
        job.module,
    ]

    if rebuild:
        command.extend(job.rebuild_args)
        logger.info(
            "Modo reconstrucción: %s",
            " ".join(job.rebuild_args),
        )

    started = time.perf_counter()

    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
        )

        duration = time.perf_counter() - started

        if result.returncode != 0:
            logger.error(
                "ETL %s fallido | código=%s | duración=%.2f segundos",
                job.name,
                result.returncode,
                duration,
            )
            return False, duration

        logger.info(
            "ETL %s completado | duración=%.2f segundos",
            job.name,
            duration,
        )

        return True, duration

    except Exception:
        duration = time.perf_counter() - started
        logger.exception("Error ejecutando ETL %s", job.name)
        return False, duration


def main() -> int:

    parser = argparse.ArgumentParser(
        description=(
            "Orquestador secuencial de la capa Silver "
            "del neurological-data-lake."
        )
    )

    parser.add_argument(
        "--only",
        choices=list(JOBS.keys()),
        help="Ejecuta solamente el ETL seleccionado.",
    )

    parser.add_argument(
        "--rebuild",
        action="store_true",
        help=(
            "Reconstruye las tablas existentes utilizando "
            "los argumentos de reemplazo de cada ETL."
        ),
    )

    args = parser.parse_args()

    try:
        validate_target_configuration()
    except Exception as exc:
        logger.error("Prevalidación fallida: %s", exc)
        return 1

    selected_jobs = (
        [JOBS[args.only]]
        if args.only
        else list(JOBS.values())
    )

    logger.info("=" * 65)
    logger.info("INICIO PIPELINE SILVER")
    logger.info("ETL seleccionados: %s", len(selected_jobs))
    logger.info(
        "Modo: %s",
        "RECONSTRUCCIÓN" if args.rebuild else "CARGA NORMAL",
    )
    logger.info("=" * 65)

    pipeline_started = time.perf_counter()
    results = []

    for job in selected_jobs:

        success, duration = execute_job(
            job,
            rebuild=args.rebuild,
        )

        results.append(
            {
                "name": job.name,
                "success": success,
                "duration": duration,
            }
        )

        if not success:
            logger.error(
                "Pipeline detenido. "
                "No se ejecutarán los siguientes ETL."
            )
            break

    total_duration = time.perf_counter() - pipeline_started

    logger.info("=" * 65)
    logger.info("RESUMEN PIPELINE SILVER")
    logger.info("=" * 65)

    for result in results:

        status = "OK" if result["success"] else "ERROR"

        logger.info(
            "%-15s | %-5s | %.2f segundos",
            result["name"],
            status,
            result["duration"],
        )

    successful = all(
        result["success"]
        for result in results
    )

    all_completed = len(results) == len(selected_jobs)

    logger.info(
        "ETL completados: %s/%s",
        len(results),
        len(selected_jobs),
    )

    logger.info(
        "Duración total: %.2f segundos",
        total_duration,
    )

    if successful and all_completed:
        logger.info(
            "PIPELINE SILVER COMPLETADO CORRECTAMENTE."
        )
        return 0

    logger.error(
        "PIPELINE SILVER FINALIZADO CON ERRORES."
    )

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
