from __future__ import annotations
from scripts.gold.db_connection import create_gold_engine
import argparse
from contextlib import contextmanager, nullcontext
import logging
import os
import re
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from scripts.gold.silver_run_validator import resolve_single_silver_run_id

ROOT = Path(__file__).resolve().parents[2]

SOURCE_TABLE = "silver_places_2023"
TARGET_TABLE = "gold.fact_area_health"

DATASET_NAME = "places_2025"
EXPECTED_ROWS = 2_490
EXPECTED_GEOGRAPHIES = 83
EXPECTED_MEASURES = 15
EXPECTED_PREVALENCE_TYPES = 2

# Sufijos administrativos que Gold conserva en el nombre canónico y que
# PLACES puede omitir. La comparación elimina únicamente un sufijo final
# de esta lista; nunca modifica ni actualiza gold.dim_geography.location_name.
GEOGRAPHY_NAME_SUFFIXES = (
    "County",
    "Parish",
    "Census Area",
    "Planning Region",
    "city",
)


def normalize_geography_name(value: str | None) -> str | None:
    """Normaliza diferencias conocidas de sufijos administrativos finales."""
    if value is None:
        return None

    normalized = " ".join(str(value).strip().split())
    suffix_pattern = r"\s+(?:" + "|".join(
        re.escape(suffix) for suffix in GEOGRAPHY_NAME_SUFFIXES
    ) + r")$"
    return re.sub(suffix_pattern, "", normalized, flags=re.IGNORECASE).strip().casefold()

COORDINATE_TOLERANCE = 0.000001
INSERT_BATCH_SIZE = 500

# Debe coincidir con el bloqueo global de scripts/run_gold_pipeline.py.
GOLD_PIPELINE_LOCK_KEY = 78_115_004_452_421

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("etl_gold_places")



def create_db_engine() -> Engine:
    return create_gold_engine()


def resolve_source_run_id(
    conn,
    requested_run_id: int | None = None,
) -> int:
    """Selecciona y valida una ejecución Silver de PLACES."""

    run_id = resolve_single_silver_run_id(
        conn,
        SOURCE_TABLE,
        requested_run_id=requested_run_id,
        expected_rows=EXPECTED_ROWS,
        dataset_name=DATASET_NAME,
        target_table=SOURCE_TABLE,
    )

    logger.info(
        "Run ID PLACES seleccionado y validado | run_id=%s",
        run_id,
    )

    return run_id


def validate_source_run(conn, source_run_id: int) -> dict:
    run = conn.execute(
        text("""
            SELECT
                run_id,
                dataset_name,
                target_table,
                source_rows,
                loaded_rows,
                status,
                finished_at
            FROM public.etl_runs
            WHERE run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).mappings().one_or_none()

    if run is None:
        raise ValueError(
            f"No existe la ejecución ETL run_id={source_run_id}."
        )

    if run["dataset_name"] != DATASET_NAME:
        raise ValueError(
            f"Dataset inesperado: {run['dataset_name']}."
        )

    if run["target_table"] != SOURCE_TABLE:
        raise ValueError(
            f"Tabla de origen inesperada: {run['target_table']}."
        )

    if run["status"] != "succeeded":
        raise ValueError(
            f"La ejecución {source_run_id} no terminó correctamente."
        )

    if run["finished_at"] is None:
        raise ValueError(
            f"La ejecución {source_run_id} no tiene finished_at; "
            "no se consumirá una ejecución Silver sin cierre confirmado."
        )

    return dict(run)


def read_and_validate_silver(conn, source_run: dict) -> list[dict]:
    expected_run_id = source_run["run_id"]

    # Evitar que otra ejecución ETL reemplace Silver
    # mientras Gold valida y consume los registros.
    conn.execute(text("""
        LOCK TABLE public.silver_places_2023 IN SHARE MODE
    """))

    # VALIDAR TRAZABILIDAD DE SILVER
    
    lineage = conn.execute(
        text("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(run_id) AS rows_with_run_id,
                COUNT(DISTINCT run_id) AS distinct_runs,
                MIN(run_id) AS min_run_id,
                MAX(run_id) AS max_run_id
            FROM public.silver_places_2023
            WHERE run_id = :run_id
        """),
        {"run_id": expected_run_id},
    ).mappings().one()

    if (
        lineage["total_rows"] != EXPECTED_ROWS
        or lineage["rows_with_run_id"] != EXPECTED_ROWS
        or lineage["distinct_runs"] != 1
        or lineage["min_run_id"] != expected_run_id
        or lineage["max_run_id"] != expected_run_id
    ):
        raise ValueError(
            "La trazabilidad de Silver no coincide con la ejecución "
            f"solicitada. Esperado run_id={expected_run_id}; "
            f"resultado={dict(lineage)}. "
            "No se cargará Gold."
        )

    # LEER REGISTROS SILVER

    records = conn.execute(
        text("""
            SELECT
                state_abbr,
                location_name,
                location_id,
                measure,
                measure_id,
                data_value_type,
                data_value,
                latitude,
                longitude,
                reference_year
            FROM public.silver_places_2023
            WHERE run_id = :run_id
            ORDER BY location_id, measure_id, data_value_type
        """),
        {"run_id": expected_run_id},
    ).mappings().all()

    if len(records) != EXPECTED_ROWS:
        raise ValueError(
            f"Silver contiene {len(records)} filas para run_id="
            f"{expected_run_id}; se esperaban {EXPECTED_ROWS}."
        )

    if len(records) != source_run["loaded_rows"]:
        raise ValueError(
            "El número de filas Silver no coincide con loaded_rows "
            f"del run_id={expected_run_id}."
        )

    years = {int(row["reference_year"]) for row in records if row["reference_year"] is not None}
    if len(years) != 1 or years != {2023} or any(row["reference_year"] is None for row in records):
        raise ValueError(
            "Silver PLACES debe contener exclusivamente reference_year=2023 "
            f"para run_id={expected_run_id}; años={sorted(years)}."
        )

    # VALIDAR DIMENSIONES

    geographies = {row["location_id"] for row in records}
    measures = {row["measure_id"] for row in records}
    prevalence_types = {row["data_value_type"] for row in records}

    if len(geographies) != EXPECTED_GEOGRAPHIES:
        raise ValueError(
            f"Geografías encontradas: {len(geographies)}; "
            f"esperadas: {EXPECTED_GEOGRAPHIES}."
        )

    if len(measures) != EXPECTED_MEASURES:
        raise ValueError(
            f"Indicadores encontrados: {len(measures)}; "
            f"esperados: {EXPECTED_MEASURES}."
        )

    if len(prevalence_types) != EXPECTED_PREVALENCE_TYPES:
        raise ValueError(
            f"Tipos de prevalencia encontrados: {len(prevalence_types)}; "
            f"esperados: {EXPECTED_PREVALENCE_TYPES}."
        )

    natural_keys = {
        (
            row["location_id"],
            row["measure_id"],
            row["data_value_type"],
        )
        for row in records
    }

    if len(natural_keys) != len(records):
        raise ValueError(
            "Silver contiene combinaciones geografía-indicador-tipo duplicadas."
        )

    for row in records:
        value = row["data_value"]

        if value is None:
            raise ValueError(
                "PLACES contiene un valor de prevalencia nulo. "
                "No se permite cargar valores nulos en Gold."
            )

        if not (0 <= value <= 100):
            raise ValueError(
                f"Prevalencia fuera de rango: {value}."
            )

    logger.info(
        "Trazabilidad Silver validada | run_id=%s | filas=%s",
        expected_run_id,
        lineage["rows_with_run_id"],
    )

    return [dict(row) for row in records]


def load_dimensions(conn, records: list[dict]) -> tuple[dict, dict, dict]:
    # --------------------------------------------------------
    # 1. Cargar y validar los estados previamente inicializados
    # --------------------------------------------------------
    state_rows = conn.execute(text("""
        SELECT
            geography_key,
            state_fips,
            state_abbr
        FROM gold.dim_geography
        WHERE geography_level = 'STATE'
          AND country_code = 'US'
    """)).mappings().all()

    states_by_fips = {
        str(row["state_fips"]).zfill(2): dict(row)
        for row in state_rows
    }

    if len(states_by_fips) != 52:
        raise ValueError(
            f"Se esperaban 52 estados/jurisdicciones en Gold; "
            f"se encontraron {len(states_by_fips)}."
        )

    # --------------------------------------------------------
    # 2. Construir geografías de condado desde PLACES
    # --------------------------------------------------------
    geographies = {}

    for row in records:
        raw_location_id = str(row["location_id"]).strip()

        if not raw_location_id.isdigit() or len(raw_location_id) > 5:
            raise ValueError(
                f"FIPS de condado inválido: {raw_location_id!r}."
            )

        county_fips = raw_location_id.zfill(5)
        state_fips = county_fips[:2]

        if len(county_fips) != 5:
            raise ValueError(f"FIPS inválido: {county_fips}.")

        parent = states_by_fips.get(state_fips)

        if parent is None:
            raise ValueError(
                f"No existe estado padre para FIPS {county_fips} "
                f"(state_fips={state_fips})."
            )

        if row["state_abbr"] != parent["state_abbr"]:
            raise ValueError(
                f"Inconsistencia geográfica para {county_fips}: "
                f"PLACES indica {row['state_abbr']}, pero Gold "
                f"asocia el FIPS {state_fips} a "
                f"{parent['state_abbr']}."
            )

        candidate = {
            "location_id": county_fips,
            "state_abbr": row["state_abbr"],
            "location_name": row["location_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "geography_level": "COUNTY",
            "state_fips": state_fips,
            "county_fips": county_fips[2:],
            "parent_geography_key": parent["geography_key"],
            "country_code": "US",
        }

        previous = geographies.get(county_fips)

        if previous is not None and previous != candidate:
            raise ValueError(
                f"Datos geográficos inconsistentes para {county_fips}."
            )

        geographies[county_fips] = candidate

    # --------------------------------------------------------
    # 3. Insertar condados con su estado padre
    # --------------------------------------------------------
    geography_insert = text("""
        INSERT INTO gold.dim_geography (
            location_id,
            state_abbr,
            location_name,
            latitude,
            longitude,
            geography_level,
            state_fips,
            county_fips,
            parent_geography_key,
            country_code
        )
        VALUES (
            :location_id,
            :state_abbr,
            :location_name,
            :latitude,
            :longitude,
            :geography_level,
            :state_fips,
            :county_fips,
            :parent_geography_key,
            :country_code
        )
        ON CONFLICT (location_id) DO NOTHING
    """)

    conn.execute(
        geography_insert,
        list(geographies.values()),
    )

    # --------------------------------------------------------
    # 4. Recuperar y validar las geografías existentes
    # --------------------------------------------------------
    geography_rows = conn.execute(text("""
        SELECT
            geography_key,
            location_id,
            state_abbr,
            location_name,
            latitude,
            longitude,
            geography_level,
            state_fips,
            county_fips,
            parent_geography_key,
            country_code
        FROM gold.dim_geography
        WHERE location_id = ANY(:location_ids)
    """), {
        "location_ids": list(geographies.keys())
    }).mappings().all()

    geography_map = {
        str(row["location_id"]): row["geography_key"]
        for row in geography_rows
    }

    if len(geography_map) != len(geographies):
        raise ValueError(
            "No se pudieron recuperar todas las geografías "
            "de PLACES después de su inserción."
        )

    for location_id, candidate in geographies.items():
        existing = next(
            row for row in geography_rows
            if str(row["location_id"]) == location_id
        )

        for column in candidate:
            existing_value = existing[column]
            candidate_value = candidate[column]

            if column == "location_name":
                if existing_value == candidate_value:
                    continue

                if (
                    normalize_geography_name(candidate_value)
                    == normalize_geography_name(existing_value)
                ):
                    logger.info(
                        "Nombre geográfico equivalente por sufijo administrativo | "
                        "FIPS=%s | Silver=%s | Gold=%s",
                        location_id,
                        candidate_value,
                        existing_value,
                    )
                    continue

                raise ValueError(
                    f"Inconsistencia en gold.dim_geography: location_name "
                    f"para FIPS {location_id}. Silver={candidate_value!r}; "
                    f"Gold={existing_value!r}. No se modificará la dimensión."
                )

            if column in {"latitude", "longitude"}:
                if (
                    existing_value is not None
                    and candidate_value is not None
                    and abs(float(existing_value) - float(candidate_value))
                    <= COORDINATE_TOLERANCE
                ):
                    continue

                if existing_value == candidate_value:
                    continue

                raise ValueError(
                    f"Inconsistencia en gold.dim_geography: {column} "
                    f"para FIPS {location_id}. Silver={candidate_value!r}; "
                    f"Gold={existing_value!r}; tolerancia={COORDINATE_TOLERANCE}. "
                    "No se modificará la dimensión existente."
                )

            if existing_value != candidate_value:
                raise ValueError(
                    f"Inconsistencia en gold.dim_geography: "
                    f"{column} para FIPS {location_id}. "
                    "No se modificará la dimensión existente."
                )

    # --------------------------------------------------------
    # 5. Cargar indicadores de salud
    # --------------------------------------------------------
    measures = {}

    for row in records:
        measure_id = row["measure_id"]
        measure = row["measure"]

        if measure_id in measures and measures[measure_id] != measure:
            raise ValueError(
                f"Descripción inconsistente para measure_id={measure_id}."
            )

        measures[measure_id] = measure

    conn.execute(text("""
        INSERT INTO gold.dim_health_measure (
            measure_id,
            measure
        )
        VALUES (
            :measure_id,
            :measure
        )
        ON CONFLICT (measure_id) DO NOTHING
    """), [
        {"measure_id": key, "measure": value}
        for key, value in measures.items()
    ])

    measure_rows = conn.execute(text("""
        SELECT health_measure_key, measure_id, measure
        FROM gold.dim_health_measure
    """)).mappings().all()

    measure_map = {
        row["measure_id"]: row["health_measure_key"]
        for row in measure_rows
    }

    for measure_id, measure in measures.items():
        existing = next(
            row for row in measure_rows
            if row["measure_id"] == measure_id
        )

        if existing["measure"] != measure:
            raise ValueError(
                f"Cambió la descripción del indicador {measure_id}."
            )

    # --------------------------------------------------------
    # 6. Cargar tipos de prevalencia
    # --------------------------------------------------------
    prevalence_values = {
        row["data_value_type"] for row in records
    }

    conn.execute(text("""
        INSERT INTO gold.dim_prevalence_type (data_value_type)
        VALUES (:data_value_type)
        ON CONFLICT (data_value_type) DO NOTHING
    """), [
        {"data_value_type": value}
        for value in prevalence_values
    ])

    prevalence_rows = conn.execute(text("""
        SELECT prevalence_type_key, data_value_type
        FROM gold.dim_prevalence_type
    """)).mappings().all()

    prevalence_map = {
        row["data_value_type"]: row["prevalence_type_key"]
        for row in prevalence_rows
    }

    return geography_map, measure_map, prevalence_map

def load_facts(
    conn,
    records: list[dict],
    source_run_id: int,
    geography_map: dict,
    measure_map: dict,
    prevalence_map: dict,
) -> int:

    # --------------------------------------------------------
    # 1. Validar la fuente
    # --------------------------------------------------------
    source = conn.execute(
        text("""
            SELECT source_key
            FROM gold.dim_source
            WHERE source_code = 'PLACES'
              AND dataset_name = :dataset_name
        """),
        {"dataset_name": DATASET_NAME},
    ).scalar_one_or_none()

    if source is None:
        raise ValueError(
            "No se encontró la fuente PLACES en gold.dim_source."
        )

    # --------------------------------------------------------
    # 2. Evitar reprocesar una ejecución ya cargada
    # --------------------------------------------------------
    existing = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_area_health
            WHERE source_run_id = :source_run_id
              AND source_key = :source_key
        """),
        {
            "source_run_id": source_run_id,
            "source_key": source,
        },
    ).scalar_one()

    if existing:
        raise RuntimeError(
            f"Ya existen {existing} hechos para "
            f"source_run_id={source_run_id}. "
            "No se permite duplicar ni sobrescribir "
            "una versión histórica."
        )

    # --------------------------------------------------------
    # 3. Construir y validar el payload
    # --------------------------------------------------------
    payload = []

    for row in records:
        location_id = str(row["location_id"]).strip().zfill(5)

        if location_id not in geography_map:
            raise ValueError(
                f"No existe geography_key para FIPS {location_id}."
            )

        if row["measure_id"] not in measure_map:
            raise ValueError(
                f"No existe health_measure_key para "
                f"measure_id={row['measure_id']}."
            )

        if row["data_value_type"] not in prevalence_map:
            raise ValueError(
                "No existe prevalence_type_key para "
                f"{row['data_value_type']}."
            )

        payload.append({
            "source_run_id": source_run_id,
            "source_key": source,
            "geography_key": geography_map[location_id],
            "health_measure_key": measure_map[row["measure_id"]],
            "prevalence_type_key": prevalence_map[
                row["data_value_type"]
            ],
            "data_value": row["data_value"],
            "reference_year": row["reference_year"],
        })

    if len(payload) != len(records):
        raise RuntimeError(
            f"Payload inconsistente: {len(payload)} elementos; "
            f"se esperaban {len(records)}."
        )

    # --------------------------------------------------------
    # 4. Insertar hechos por lotes
    # --------------------------------------------------------
    insert_stmt = text("""
        INSERT INTO gold.fact_area_health (
            source_run_id,
            source_key,
            geography_key,
            health_measure_key,
            prevalence_type_key,
            reference_year,
            data_value
        )
        VALUES (
            :source_run_id,
            :source_key,
            :geography_key,
            :health_measure_key,
            :prevalence_type_key,
            :reference_year,
            :data_value
        )
    """)

    loaded_rows = 0

    for start in range(0, len(payload), INSERT_BATCH_SIZE):
        batch = payload[start:start + INSERT_BATCH_SIZE]

        conn.execute(insert_stmt, batch)

        loaded_rows += len(batch)

        logger.info(
            "Lote %s | hechos cargados: %s / %s",
            start // INSERT_BATCH_SIZE + 1,
            loaded_rows,
            len(payload),
        )

    # --------------------------------------------------------
    # 5. Validar persistencia
    # --------------------------------------------------------
    persisted = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_area_health
            WHERE source_run_id = :source_run_id
              AND source_key = :source_key
        """),
        {
            "source_run_id": source_run_id,
            "source_key": source,
        },
    ).scalar_one()

    if persisted != loaded_rows:
        raise RuntimeError(
            f"Persistidos={persisted}; cargados={loaded_rows}."
        )

    if loaded_rows != EXPECTED_ROWS:
        raise RuntimeError(
            f"Se cargaron {loaded_rows} hechos; "
            f"se esperaban {EXPECTED_ROWS}."
        )

    logger.info(
        "Hechos PLACES validados | source_run_id=%s | hechos=%s",
        source_run_id,
        loaded_rows,
    )

    return loaded_rows

def validate_orchestrator_lock(engine: Engine) -> None:
    """Confirma que el proceso padre mantiene el bloqueo global compartido."""
    with engine.connect() as conn:
        held = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND granted
                      AND pid <> pg_backend_pid()
                      AND classid = (:lock_key >> 32)::oid
                      AND objid = (:lock_key & 4294967295)::oid
                      AND objsubid = 1
                )
            """),
            {"lock_key": GOLD_PIPELINE_LOCK_KEY},
        ).scalar_one()

    if not held:
        raise RuntimeError(
            "Se indicó --orchestrated, pero no se detectó el bloqueo global "
            "Gold del proceso padre. Se cancela para evitar una ejecución sin exclusión mutua."
        )


@contextmanager
def acquire_places_execution_lock():
    """Serializa PLACES independiente frente al pipeline Gold completo."""
    engine = create_db_engine()
    conn = None
    acquired = False

    try:
        conn = engine.connect()
        database = conn.execute(text("SELECT current_database()")).scalar_one()
        if database != "health_datalake":
            raise RuntimeError(
                f"ETL PLACES bloqueado: base de datos no autorizada ({database})."
            )

        acquired = bool(
            conn.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": GOLD_PIPELINE_LOCK_KEY},
            ).scalar_one()
        )
        conn.commit()

        if not acquired:
            raise RuntimeError(
                "Existe un pipeline Gold u otra ejecución PLACES en curso. "
                "No se iniciará una ejecución concurrente."
            )

        logger.info("Bloqueo compartido de ejecución Gold adquirido por PLACES.")
        yield
    finally:
        if conn is not None:
            if acquired:
                try:
                    conn.execute(
                        text("SELECT pg_advisory_unlock(:lock_key)"),
                        {"lock_key": GOLD_PIPELINE_LOCK_KEY},
                    )
                    conn.commit()
                    logger.info("Bloqueo compartido de ejecución Gold liberado.")
                except Exception:
                    logger.exception("No fue posible liberar el bloqueo; se invalidará la conexión.")
                    conn.invalidate()
            conn.close()
        engine.dispose()


def run_etl(
    source_run_id: int | None = None,
    orchestrated: bool = False,
) -> None:
    engine = create_db_engine()

    try:
        if orchestrated:
            validate_orchestrator_lock(engine)
            logger.info(
                "Bloqueo global del orquestador verificado; PLACES no adquiere un segundo lock."
            )
            lock_context = nullcontext()
        else:
            lock_context = acquire_places_execution_lock()

        with lock_context, engine.begin() as conn:

            source_run_id = resolve_source_run_id(
                conn,
                source_run_id
            )

            source_run = validate_source_run(
                conn,
                source_run_id
            )

            logger.info(
                "Ejecución de origen validada | run_id=%s | dataset=%s",
                source_run_id,
                source_run["dataset_name"],
            )

            records = read_and_validate_silver(conn, source_run)

            logger.info(
                "Silver validado | filas=%s | geografías=%s | indicadores=%s",
                len(records),
                EXPECTED_GEOGRAPHIES,
                EXPECTED_MEASURES,
            )

            geography_map, measure_map, prevalence_map = load_dimensions(
                conn,
                records,
            )

            loaded_rows = load_facts(
                conn,
                records,
                source_run_id,
                geography_map,
                measure_map,
                prevalence_map,
            )

            logger.info(
                "ETL Gold PLACES finalizado | source_run_id=%s | hechos=%s",
                source_run_id,
                loaded_rows,
            )

    except Exception:
        logger.exception("Falló el ETL Gold PLACES.")
        raise

    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga histórica de PLACES desde Silver hacia Gold."
    )

    parser.add_argument(
    "--source-run-id",
    type=int,
    default=None,
    help=(
        "run_id de Silver. Si se omite, "
        "se selecciona automáticamente la versión vigente."
    ),
)

    parser.add_argument(
        "--orchestrated",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    args = parser.parse_args()

    try:
        run_etl(args.source_run_id, orchestrated=args.orchestrated)
        return 0

    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
