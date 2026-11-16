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

from scripts.database_config import get_target_database
from scripts.silver.resume import classify_audited_stage

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_ROWS = {
    "acs": 113,
    "brfss": 2800,
    "places": 2490,
    "acs_county": 332,
    "geography": 215,
}
CATALOG_MIN_INDICATORS = 100

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
    dataset_name: str | None = None
    target_table: str | None = None


JOBS = {
    "acs": ETLJob("ACS nacional", "scripts.silver.etl_acs", "acs_2024", "silver_acs_2024"),
    "brfss": ETLJob(
        "BRFSS", "scripts.silver.etl_brfss", "brfss_2023",
        "silver_respondent+silver_brfss_2023",
    ),
    "places": ETLJob(
        "PLACES", "scripts.silver.etl_places", "places_2025", "silver_places_2023"
    ),
    "acs_county": ETLJob(
        "ACS County", "scripts.silver.etl_acs_county",
        "acs_2024_county_dp05", "silver_acs_county_2024",
    ),
    "catalogs": ETLJob("Catálogos de fuentes e indicadores", "scripts.silver.etl_health_catalog"),
    "geography": ETLJob(
        "Geography", "scripts.silver.etl_geography",
        "geography_catalog", "silver_geography+silver_county",
    ),
}


def validate_target_configuration() -> str:
    load_dotenv(ROOT / ".env", override=False)
    target_database = get_target_database()
    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [variable for variable in required if not os.getenv(variable)]
    if missing:
        raise RuntimeError(f"Faltan variables PostgreSQL: {', '.join(missing)}")
    logger.info(
        "Configuración PostgreSQL validada: localhost:5432 / %s",
        target_database,
    )
    return target_database


def connect_target():
    import psycopg

    load_dotenv(ROOT / ".env", override=False)
    database = validate_target_configuration()
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )


def latest_etl_run(conn, job: ETLJob):
    row = conn.execute(
        """
        SELECT run_id, status, loaded_rows, finished_at
        FROM public.etl_runs
        WHERE dataset_name = %s AND target_table = %s
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (job.dataset_name, job.target_table),
    ).fetchone()
    return row


def inspect_job_state(conn, key: str) -> str:
    """Devuelve complete/pending; detiene la ejecución ante cualquier estado parcial."""
    job = JOBS[key]

    if key == "catalogs":
        source_count = conn.execute(
            "SELECT COUNT(*) FROM public.silver_source"
        ).fetchone()[0]
        indicator_count = conn.execute(
            "SELECT COUNT(*) FROM public.silver_health_indicator"
        ).fetchone()[0]
        if source_count == 0 and indicator_count == 0:
            return "pending"
        if source_count == 4 and indicator_count >= CATALOG_MIN_INDICATORS:
            return "complete"
        raise RuntimeError(
            "Catálogos Silver parciales o inconsistentes: "
            f"fuentes={source_count}/4, indicadores={indicator_count} "
            f"(mínimo={CATALOG_MIN_INDICATORS})."
        )

    if key == "acs":
        counts = conn.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE run_id = (
                SELECT run_id FROM public.etl_runs
                WHERE dataset_name=%s AND target_table=%s
                ORDER BY run_id DESC LIMIT 1
            ))
            FROM public.silver_acs_2024
            """,
            (job.dataset_name, job.target_table),
        ).fetchone()
        total, run_rows = map(int, counts)

    elif key == "places":
        counts = conn.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE run_id = (
                SELECT run_id FROM public.etl_runs
                WHERE dataset_name=%s AND target_table=%s
                ORDER BY run_id DESC LIMIT 1
            ))
            FROM public.silver_places_2023
            """,
            (job.dataset_name, job.target_table),
        ).fetchone()
        total, run_rows = map(int, counts)

    elif key == "acs_county":
        counts = conn.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE run_id = (
                SELECT run_id FROM public.etl_runs
                WHERE dataset_name=%s AND target_table=%s
                ORDER BY run_id DESC LIMIT 1
            ))
            FROM public.silver_acs_county_2024
            """,
            (job.dataset_name, job.target_table),
        ).fetchone()
        total, run_rows = map(int, counts)

    elif key == "brfss":
        latest = latest_etl_run(conn, job)
        run_id = latest[0] if latest else None
        if run_id is None:
            respondents = int(conn.execute(
                "SELECT COUNT(*) FROM public.silver_respondent"
            ).fetchone()[0])
            responses = int(conn.execute(
                "SELECT COUNT(*) FROM public.silver_brfss_2023"
            ).fetchone()[0])
            if respondents == 0 and responses == 0:
                return "pending"
            raise RuntimeError(
                "BRFSS contiene datos, pero no existe una ejecución ETL auditable."
            )
        respondents, responses, invalid = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM public.silver_respondent WHERE run_id=%s),
                (SELECT COUNT(*) FROM public.silver_brfss_2023 r
                 JOIN public.silver_respondent p ON p.respondent_id=r.respondent_id
                 WHERE p.run_id=%s),
                (SELECT COUNT(*) FROM public.silver_brfss_2023 r
                 JOIN public.silver_respondent p ON p.respondent_id=r.respondent_id
                 WHERE p.run_id=%s AND
                   (jsonb_typeof(r.record_data) <> 'object'
                    OR (SELECT COUNT(*) FROM jsonb_object_keys(r.record_data)) <> 20))
            """,
            (run_id, run_id, run_id),
        ).fetchone()
        total_respondents = int(conn.execute(
            "SELECT COUNT(*) FROM public.silver_respondent"
        ).fetchone()[0])
        total_responses = int(conn.execute(
            "SELECT COUNT(*) FROM public.silver_brfss_2023"
        ).fetchone()[0])
        if latest[1] == "succeeded" and latest[3] is not None:
            if (
                int(latest[2]) == EXPECTED_ROWS[key]
                and int(respondents) == EXPECTED_ROWS[key]
                and int(responses) == EXPECTED_ROWS[key]
                and total_respondents == EXPECTED_ROWS[key]
                and total_responses == EXPECTED_ROWS[key]
                and int(invalid) == 0
            ):
                return "complete"
        raise RuntimeError(
            "BRFSS está incompleto o inconsistente: "
            f"última ejecución={latest}, respondents={respondents}/{EXPECTED_ROWS[key]}, "
            f"responses={responses}/{EXPECTED_ROWS[key]}, inválidas={invalid}."
        )

    elif key == "geography":
        latest = latest_etl_run(conn, job)
        states, counties, county_catalog = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM public.silver_geography WHERE geography_level='STATE'),
                (SELECT COUNT(*) FROM public.silver_geography WHERE geography_level='COUNTY'),
                (SELECT COUNT(*) FROM public.silver_county)
            """
        ).fetchone()
        states, counties, county_catalog = map(int, (states, counties, county_catalog))
        if latest is None:
            if states == counties == county_catalog == 0:
                return "pending"
            raise RuntimeError(
                "Geography contiene datos, pero no existe una ejecución ETL auditable."
            )
        if (
            latest[1] == "succeeded"
            and latest[3] is not None
            and int(latest[2]) == EXPECTED_ROWS[key]
            and (states, counties, county_catalog) == (49, 83, 83)
        ):
            return "complete"
        raise RuntimeError(
            "Geography está incompleta o inconsistente: "
            f"última ejecución={latest}, estados={states}/49, "
            f"condados={counties}/83, catálogo_condados={county_catalog}/83."
        )

    else:
        latest = latest_etl_run(conn, job)
        if latest is None:
            if total == 0 and run_rows == 0:
                return "pending"
            raise RuntimeError(
                f"{job.name} contiene datos, pero no existe una ejecución ETL auditable."
            )
        if (
            latest[1] == "succeeded"
            and latest[3] is not None
            and int(latest[2]) == EXPECTED_ROWS[key]
            and total == EXPECTED_ROWS[key]
            and run_rows == EXPECTED_ROWS[key]
        ):
            return "complete"
        raise RuntimeError(
            f"{job.name} está incompleto o inconsistente: última ejecución={latest}, "
            f"filas_totales={total}, filas_de_último_run={run_rows}, "
            f"esperadas={EXPECTED_ROWS[key]}."
        )

    latest = latest_etl_run(conn, job)
    return classify_audited_stage(
        latest,
        (total, run_rows),
        EXPECTED_ROWS[key],
        job.name,
    )


def execute_job(job: ETLJob) -> float:
    logger.info("=" * 65)
    logger.info("INICIANDO ETL: %s", job.name)
    command = [sys.executable, "-m", job.module]
    logger.info("COMANDO: %s", " ".join(command))
    started = time.perf_counter()
    result = subprocess.run(command, cwd=ROOT, check=False)
    duration = time.perf_counter() - started
    if result.returncode != 0:
        raise RuntimeError(
            f"ETL {job.name} falló con código {result.returncode} "
            f"después de {duration:.2f} segundos."
        )
    logger.info("ETL %s completado | duración=%.2f segundos", job.name, duration)
    return duration


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Orquestador Silver con reanudación validada. Omite ETL completos "
            "y se detiene ante cargas parciales o inconsistentes."
        )
    )
    parser.add_argument(
        "--only", choices=list(JOBS),
        help="Ejecuta o valida solamente la etapa seleccionada.",
    )
    parser.add_argument(
        "--validate-only", action="store_true",
        help="Solo valida las seis etapas; falla si alguna está pendiente.",
    )
    args = parser.parse_args()

    try:
        validate_target_configuration()
        selected = [args.only] if args.only else list(JOBS)
        durations = []
        for key in selected:
            job = JOBS[key]
            with connect_target() as conn:
                state = inspect_job_state(conn, key)
            if state == "complete":
                logger.info("ETL omitido: %s ya está completo y validado.", job.name)
                continue
            if args.validate_only:
                raise RuntimeError(
                    f"Validación-only: {job.name} está pendiente; no se ejecutó."
                )
            logger.info("ETL pendiente: %s. Se ejecutará sin reconstrucción.", job.name)
            durations.append(execute_job(job))
            with connect_target() as conn:
                final_state = inspect_job_state(conn, key)
            if final_state != "complete":
                raise RuntimeError(
                    f"{job.name} terminó, pero sus resultados no superaron la validación."
                )
            logger.info("ETL validado correctamente: %s.", job.name)

        logger.info(
            "PIPELINE SILVER COMPLETADO | etapas=%s | ejecuciones=%s | duración=%.2f s",
            len(selected), len(durations), sum(durations),
        )
        return 0
    except Exception as exc:
        logger.exception("PIPELINE SILVER DETENIDO: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
