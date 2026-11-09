
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


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


JOBS = {
    "acs": ETLJob("ACS nacional", "scripts.etl_acs"),
    "acs_county": ETLJob("ACS County", "scripts.etl_acs_county"),
    "brfss": ETLJob("BRFSS", "scripts.etl_brfss"),
    "places": ETLJob("PLACES", "scripts.etl_places"),
}


def execute_job(job: ETLJob) -> tuple[bool, float]:
    logger.info("=" * 65)
    logger.info("INICIANDO ETL: %s", job.name)
    logger.info("=" * 65)

    started = time.perf_counter()

    try:
        result = subprocess.run(
            [sys.executable, "-m", job.module],
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
        description="Orquestador secuencial de los ETL Silver."
    )

    parser.add_argument(
        "--only",
        choices=list(JOBS.keys()),
        help="Ejecuta solamente el ETL seleccionado.",
    )

    args = parser.parse_args()

    selected_jobs = (
        [JOBS[args.only]]
        if args.only
        else list(JOBS.values())
    )

    logger.info("=" * 65)
    logger.info("INICIO PIPELINE SILVER")
    logger.info("ETL seleccionados: %s", len(selected_jobs))
    logger.info("=" * 65)

    pipeline_started = time.perf_counter()
    results = []

    for job in selected_jobs:
        success, duration = execute_job(job)

        results.append(
            {
                "name": job.name,
                "success": success,
                "duration": duration,
            }
        )

        if not success:
            logger.error(
                "Pipeline detenido. No se ejecutarán los siguientes ETL."
            )
            break

    total_duration = time.perf_counter() - pipeline_started

    logger.info("=" * 65)
    logger.info("RESUMEN PIPELINE SILVER")

    for result in results:
        status = "OK" if result["success"] else "ERROR"

        logger.info(
            "%-15s | %-5s | %.2f segundos",
            result["name"],
            status,
            result["duration"],
        )

    successful = all(result["success"] for result in results)
    all_completed = len(results) == len(selected_jobs)

    logger.info("Duración total: %.2f segundos", total_duration)

    if successful and all_completed:
        logger.info("PIPELINE SILVER COMPLETADO CORRECTAMENTE.")
        return 0

    logger.error("PIPELINE SILVER FINALIZADO CON ERRORES.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
