from __future__ import annotations
import argparse
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from scripts.gold.silver_run_validator import resolve_single_silver_run_id

ROOT = Path(__file__).resolve().parents[1]

SOURCE_TABLE = "silver_places_2023"
TARGET_TABLE = "gold.fact_area_health"

DATASET_NAME = "places_2025"
EXPECTED_ROWS = 2_490
EXPECTED_GEOGRAPHIES = 83
EXPECTED_MEASURES = 15
EXPECTED_PREVALENCE_TYPES = 2

INSERT_BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("etl_gold_places")



def create_db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    allowed_database = "health_datalake"
    database = os.getenv("POSTGRES_DB")

    if database != allowed_database:
        raise RuntimeError(
            f"ConexiÃ³n rechazada: la base configurada es "
            f"'{database}', pero solo se permite '{allowed_database}'."
        )

    required = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [name for name in required if not os.getenv(name)]

    if missing:
        raise RuntimeError(
            f"Faltan variables de conexiÃ³n: {', '.join(missing)}"
        )

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=database,
    )

    engine = create_engine(url, pool_pre_ping=True)

    try:
        with engine.connect() as conn:
            current_database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if current_database != allowed_database:
                raise RuntimeError(
                    f"ConexiÃ³n rechazada: PostgreSQL respondiÃ³ "
                    f"con la base '{current_database}'."
                )

        return engine

    except Exception:
        engine.dispose()
        raise


def resolve_source_run_id(
    conn,
    requested_run_id: int | None = None,
) -> int:
    """Selecciona y valida una ejecuciÃ³n Silver de PLACES."""

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
                status
            FROM public.etl_runs
            WHERE run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).mappings().one_or_none()

    if run is None:
        raise ValueError(
            f"No existe la ejecuciÃ³n ETL run_id={source_run_id}."
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
            f"La ejecuciÃ³n {source_run_id} no terminÃ³ correctamente."
        )

    return dict(run)


def read_and_validate_silver(conn, source_run: dict) -> list[dict]:
    expected_run_id = source_run["run_id"]

    # Evitar que otra ejecuciÃ³n ETL reemplace Silver
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
            "La trazabilidad de Silver no coincide con la ejecuciÃ³n "
            f"solicitada. Esperado run_id={expected_run_id}; "
            f"resultado={dict(lineage)}. "
            "No se cargarÃ¡ Gold."
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
                longitude
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
            "El nÃºmero de filas Silver no coincide con loaded_rows "
            f"del run_id={expected_run_id}."
        )

    # VALIDAR DIMENSIONES

    geographies = {row["location_id"] for row in records}
    measures = {row["measure_id"] for row in records}
    prevalence_types = {row["data_value_type"] for row in records}

    if len(geographies) != EXPECTED_GEOGRAPHIES:
        raise ValueError(
            f"GeografÃ­as encontradas: {len(geographies)}; "
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
            "Silver contiene combinaciones geografÃ­a-indicador-tipo duplicadas."
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
    # 2. Construir geografÃ­as de condado desde PLACES
    # --------------------------------------------------------
    geographies = {}

    for row in records:
        raw_location_id = str(row["location_id"]).strip()

        if not raw_location_id.isdigit() or len(raw_location_id) > 5:
            raise ValueError(
                f"FIPS de condado invÃ¡lido: {raw_location_id!r}."
            )

        county_fips = raw_location_id.zfill(5)
        state_fips = county_fips[:2]

        if len(county_fips) != 5:
            raise ValueError(f"FIPS invÃ¡lido: {county_fips}.")

        parent = states_by_fips.get(state_fips)

        if parent is None:
            raise ValueError(
                f"No existe estado padre para FIPS {county_fips} "
                f"(state_fips={state_fips})."
            )

        if row["state_abbr"] != parent["state_abbr"]:
            raise ValueError(
                f"Inconsistencia geogrÃ¡fica para {county_fips}: "
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
                f"Datos geogrÃ¡ficos inconsistentes para {county_fips}."
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
    # 4. Recuperar y validar las geografÃ­as existentes
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
            "No se pudieron recuperar todas las geografÃ­as "
            "de PLACES despuÃ©s de su inserciÃ³n."
        )

    for location_id, candidate in geographies.items():
        existing = next(
            row for row in geography_rows
            if str(row["location_id"]) == location_id
        )

        for column in candidate:
            if existing[column] != candidate[column]:
                raise ValueError(
                    f"Inconsistencia en gold.dim_geography: "
                    f"{column} para FIPS {location_id}. "
                    "No se modificarÃ¡ la dimensiÃ³n existente."
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
                f"DescripciÃ³n inconsistente para measure_id={measure_id}."
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
                f"CambiÃ³ la descripciÃ³n del indicador {measure_id}."
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

    # Serializa las cargas Gold que utilizan este bloqueo.
    conn.execute(
        text("SELECT pg_advisory_xact_lock(741, 2026)")
    )

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
            "No se encontrÃ³ la fuente PLACES en gold.dim_source."
        )

    # --------------------------------------------------------
    # 2. Evitar reprocesar una ejecuciÃ³n ya cargada
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
            "una versiÃ³n histÃ³rica."
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
            data_value
        )
        VALUES (
            :source_run_id,
            :source_key,
            :geography_key,
            :health_measure_key,
            :prevalence_type_key,
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

def run_etl(source_run_id: int | None = None) -> None:
    engine = create_db_engine()

    try:
        with engine.begin() as conn:

            source_run_id = resolve_source_run_id(
                conn,
                source_run_id
            )

            source_run = validate_source_run(
                conn,
                source_run_id
            )

            logger.info(
                "EjecuciÃ³n de origen validada | run_id=%s | dataset=%s",
                source_run_id,
                source_run["dataset_name"],
            )

            records = read_and_validate_silver(conn, source_run)

            logger.info(
                "Silver validado | filas=%s | geografÃ­as=%s | indicadores=%s",
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
        logger.exception("FallÃ³ el ETL Gold PLACES.")
        raise

    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga histÃ³rica de PLACES desde Silver hacia Gold."
    )

    parser.add_argument(
    "--source-run-id",
    type=int,
    default=None,
    help=(
        "run_id de Silver. Si se omite, "
        "se selecciona automÃ¡ticamente la versiÃ³n vigente."
    ),
)

    args = parser.parse_args()

    try:
        run_etl(args.source_run_id)
        return 0

    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())