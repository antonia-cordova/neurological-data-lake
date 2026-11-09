from __future__ import annotations
import argparse
import logging
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy import bindparam
from scripts.gold.silver_run_validator import resolve_single_silver_run_id

ROOT = Path(__file__).resolve().parents[1]

SOURCE_TABLE = "silver_brfss_2023"
DATASET_NAME = "brfss_2023"

EXPECTED_RESPONDENTS = 2800
EXPECTED_VARIABLES = 20
EXPECTED_FACTS = EXPECTED_RESPONDENTS * EXPECTED_VARIABLES

INSERT_BATCH_SIZE = 1000

NUMERIC_VARIABLES = {
    "_BMI5",
    "PHYSHLTH",
    "MENTHLTH",
    "POORHLTH",
}

BMI_VARIABLE = "_BMI5"
DAY_VARIABLES = {"PHYSHLTH", "MENTHLTH", "POORHLTH"}

VARIABLES = {
    "_AGEG5YR": ("Grupo de edad en intervalos de cinco aÃ±os", "CATEGORICAL"),
    "_SEX": ("Sexo del encuestado", "CATEGORICAL"),
    "_RACEGR3": ("Grupo racial y Ã©tnico", "CATEGORICAL"),
    "EDUCA": ("Nivel educacional", "CATEGORICAL"),
    "INCOME3": ("CategorÃ­a de ingreso familiar anual", "CATEGORICAL"),
    "GENHLTH": ("AutopercepciÃ³n del estado general de salud", "CATEGORICAL"),
    "PHYSHLTH": ("DÃ­as de mala salud fÃ­sica durante los Ãºltimos 30 dÃ­as", "NUMERIC"),
    "MENTHLTH": ("DÃ­as de mala salud mental durante los Ãºltimos 30 dÃ­as", "NUMERIC"),
    "POORHLTH": ("DÃ­as en que la mala salud impidiÃ³ actividades habituales", "NUMERIC"),
    "DIABETE4": ("CondiciÃ³n de diabetes", "CATEGORICAL"),
    "CVDCRHD4": ("Antecedente de enfermedad coronaria", "CATEGORICAL"),
    "CVDSTRK3": ("Antecedente de accidente cerebrovascular", "CATEGORICAL"),
    "HAVARTH4": ("DiagnÃ³stico de artritis", "CATEGORICAL"),
    "ADDEPEV3": ("DiagnÃ³stico de trastorno depresivo", "CATEGORICAL"),
    "DIFFWALK": ("Dificultad para caminar o subir escaleras", "CATEGORICAL"),
    "DIFFALON": ("Dificultad para realizar actividades por sÃ­ solo", "CATEGORICAL"),
    "_BMI5": ("Ãndice de masa corporal, con dos decimales implÃ­citos", "NUMERIC"),
    "_SMOKER3": ("ClasificaciÃ³n del hÃ¡bito tabÃ¡quico", "CATEGORICAL"),
    "DRNKANY6": ("Consumo de alcohol en los Ãºltimos 30 dÃ­as", "CATEGORICAL"),
    "_TOTINDA": ("Actividad fÃ­sica o ejercicio en los Ãºltimos 30 dÃ­as", "CATEGORICAL"),
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("etl_gold_brfss")



def create_db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    database = os.getenv("POSTGRES_DB")
    if database != "health_datalake":
        raise RuntimeError(
            "ETL bloqueado: solo se permite health_datalake."
        )

    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            f"Faltan variables en .env: {', '.join(missing)}"
        )

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
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

            if actual_database != "health_datalake":
                raise RuntimeError(
                    f"ConexiÃ³n rechazada: base detectada {actual_database}."
                )
    except Exception:
        engine.dispose()
        raise

    logger.info("ConexiÃ³n PostgreSQL validada: %s", database)
    return engine


def resolve_source_run_id(
    conn,
    requested_run_id: int | None = None,
) -> int:
    """Selecciona y valida una ejecuciÃ³n Silver de BRFSS."""

    run_id = resolve_single_silver_run_id(
        conn,
        SOURCE_TABLE,
        requested_run_id=requested_run_id,
        expected_rows=EXPECTED_RESPONDENTS,
        dataset_name=DATASET_NAME,
        target_table=SOURCE_TABLE,
    )

    logger.info(
        "Run ID BRFSS seleccionado y validado | run_id=%s",
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
        raise ValueError(f"No existe run_id={source_run_id}.")

    if run["dataset_name"] != DATASET_NAME:
        raise ValueError("El dataset de la ejecuciÃ³n no es BRFSS 2023.")

    if run["target_table"] != SOURCE_TABLE:
        raise ValueError("La tabla de origen no corresponde a Silver BRFSS.")

    if run["status"] != "succeeded":
        raise ValueError("La ejecuciÃ³n de Silver no fue exitosa.")

    if run["loaded_rows"] != EXPECTED_RESPONDENTS:
        raise ValueError(
            f"Se esperaban {EXPECTED_RESPONDENTS} registros; "
            f"etl_runs informa {run['loaded_rows']}."
        )

    return dict(run)



def parse_numeric(variable: str, raw_value: str | None):
    if raw_value is None or raw_value.strip() == "":
        return None

    try:
        value = Decimal(raw_value.strip())
    except InvalidOperation as exc:
        raise ValueError(
            f"Valor no numÃ©rico en {variable}: {raw_value!r}"
        ) from exc

    if variable == BMI_VARIABLE:
        if not (Decimal("1") <= value <= Decimal("9999")):
            raise ValueError(
                f"_BMI5 fuera de rango: {raw_value}"
            )

        return value / Decimal("100")   

    if variable in DAY_VARIABLES:
        if Decimal("1") <= value <= Decimal("30"):
            return value

        if value == Decimal("88"):
            return Decimal("0")

        if value in (Decimal("77"), Decimal("99")):
            return None

        raise ValueError(
            f"CÃ³digo inesperado en variable de dÃ­as {variable}: {raw_value}"
        )

    return value



def read_and_validate_silver(conn, source_run: dict) -> list[dict]:
    run_id = source_run["run_id"]

    conn.execute(text("""
        LOCK TABLE public.silver_brfss_2023 IN SHARE MODE
    """))

    
    lineage = conn.execute(
        text("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(run_id) AS rows_with_run_id,
                COUNT(DISTINCT run_id) AS distinct_runs,
                MIN(run_id) AS min_run_id,
                MAX(run_id) AS max_run_id
            FROM public.silver_brfss_2023
            WHERE run_id = :run_id
        """),
        {"run_id": run_id},
    ).mappings().one()

    if (
        lineage["total_rows"] != EXPECTED_RESPONDENTS
        or lineage["rows_with_run_id"] != EXPECTED_RESPONDENTS
        or lineage["distinct_runs"] != 1
        or lineage["min_run_id"] != run_id
        or lineage["max_run_id"] != run_id
    ):
        raise ValueError(
            f"Trazabilidad Silver incorrecta para run_id={run_id}: "
            f"{dict(lineage)}"
        )

    records = conn.execute(
        text("""
            SELECT
                state_code,
                sequence_number,
                record_data,
                run_id
            FROM public.silver_brfss_2023
            WHERE run_id = :run_id
            ORDER BY state_code, sequence_number
        """),
        {"run_id": run_id},
    ).mappings().all()

    if len(records) != EXPECTED_RESPONDENTS:
        raise ValueError(
            f"Silver contiene {len(records)} registros; "
            f"se esperaban {EXPECTED_RESPONDENTS}."
        )

    natural_keys = set()
    for row in records:
        key = (row["state_code"], row["sequence_number"])

        if not all(key):
            raise ValueError("Se encontrÃ³ una clave natural vacÃ­a.")

        if key in natural_keys:
            raise ValueError(f"Clave natural duplicada: {key}")

        natural_keys.add(key)

        data = row["record_data"]

        if not isinstance(data, dict):
            raise ValueError("record_data no contiene un objeto JSON.")

        if set(data.keys()) != set(VARIABLES.keys()):
            raise ValueError(
                f"Las variables de {key} no coinciden con el catÃ¡logo."
            )

    logger.info(
        "Silver validado: %s personas y %s variables.",
        len(records),
        len(VARIABLES),
    )

    return [dict(row) for row in records]


def load_dimensions(conn, records: list[dict]):
    # Fuente compartida.
    source_key = conn.execute(
        text("""
            SELECT source_key
            FROM gold.dim_source
            WHERE source_code = 'BRFSS'
              AND dataset_name = :dataset
        """),
        {"dataset": DATASET_NAME},
    ).scalar_one_or_none()

    if source_key is None:
        raise ValueError("BRFSS no estÃ¡ registrado en gold.dim_source.")

    # Variables.
    conn.execute(
        text("""
            INSERT INTO gold.dim_brfss_variable (
                variable_code,
                variable_description,
                value_type
            )
            VALUES (
                :variable_code,
                :variable_description,
                :value_type
            )
            ON CONFLICT (variable_code) DO NOTHING
        """),
        [
            {
                "variable_code": code,
                "variable_description": description,
                "value_type": value_type,
            }
            for code, (description, value_type) in VARIABLES.items()
        ],
    )

    variable_rows = conn.execute(
        text("""
            SELECT variable_key, variable_code,
                   variable_description, value_type
            FROM gold.dim_brfss_variable
        """)
    ).mappings().all()

    variable_map = {
        row["variable_code"]: row["variable_key"]
        for row in variable_rows
    }

    for code, (description, value_type) in VARIABLES.items():
        existing = next(
            row for row in variable_rows
            if row["variable_code"] == code
        )

        if (
            existing["variable_description"] != description
            or existing["value_type"] != value_type
        ):
            raise ValueError(
                f"DefiniciÃ³n diferente para la variable {code}."
            )

    # Jurisdicciones observadas en Silver.
    states = sorted({row["state_code"] for row in records})

    conn.execute(
        text("""
            INSERT INTO gold.dim_brfss_jurisdiction (state_code)
            VALUES (:state_code)
            ON CONFLICT (state_code) DO NOTHING
        """),
        [{"state_code": state} for state in states],
    )

    jurisdiction_rows = conn.execute(
        text("""
            SELECT jurisdiction_key, state_code
            FROM gold.dim_brfss_jurisdiction
        """)
    ).mappings().all()

    jurisdiction_map = {
    row["state_code"]: row["jurisdiction_key"]
    for row in jurisdiction_rows
}

    # --------------------------------------------------------
    # Puente geogrÃ¡fico BRFSS -> Gold
    # Correspondencia mediante FIPS estatal.
    # --------------------------------------------------------

    if len(states) != 52:
        raise ValueError(
            f"Se esperaban 52 jurisdicciones BRFSS; "
            f"se encontraron {len(states)}."
        )

    # Validar que todos los cÃ³digos sean FIPS numÃ©ricos.
    invalid_states = [
        state for state in states
        if not str(state).strip().isdigit()
        or not (1 <= len(str(state).strip()) <= 2)
    ]

    if invalid_states:
        raise ValueError(
            f"CÃ³digos FIPS BRFSS invÃ¡lidos: {invalid_states}"
        )

    # Crear las correspondencias utilizando FIPS normalizados.
    bridge_insert = text("""
        INSERT INTO gold.bridge_brfss_geography (
            jurisdiction_key,
            geography_key,
            state_code,
            state_fips,
            mapping_method
        )
        SELECT
            j.jurisdiction_key,
            g.geography_key,
            j.state_code,
            g.state_fips,
            'STATE_FIPS'
        FROM gold.dim_brfss_jurisdiction j
        INNER JOIN gold.dim_geography g
            ON g.state_fips = LPAD(TRIM(j.state_code), 2, '0')
        AND g.geography_level = 'STATE'
        WHERE j.state_code IN :state_codes
        ON CONFLICT (jurisdiction_key) DO UPDATE
        SET
            geography_key = EXCLUDED.geography_key,
            state_code = EXCLUDED.state_code,
            state_fips = EXCLUDED.state_fips,
            mapping_method = EXCLUDED.mapping_method
    """).bindparams(
        bindparam("state_codes", expanding=True)
    )

    conn.execute(
        bridge_insert,
        {"state_codes": states}
    )

    # Validar cantidad y unicidad de correspondencias.
    bridge_validation = conn.execute(text("""
        SELECT
            COUNT(*) AS total_mappings,
            COUNT(DISTINCT jurisdiction_key) AS jurisdictions,
            COUNT(DISTINCT geography_key) AS geographies
        FROM gold.bridge_brfss_geography
    """)).mappings().one()

    if (
        bridge_validation["total_mappings"] != 52
        or bridge_validation["jurisdictions"] != 52
        or bridge_validation["geographies"] != 52
    ):
        raise ValueError(
            "Puente geogrÃ¡fico BRFSS invÃ¡lido: "
            f"{dict(bridge_validation)}"
        )

    # Verificar jurisdicciones sin correspondencia.
    missing = conn.execute(text("""
        SELECT COUNT(*)
        FROM gold.dim_brfss_jurisdiction j
        WHERE j.state_code IN :state_codes
        AND NOT EXISTS (
            SELECT 1
            FROM gold.bridge_brfss_geography b
            WHERE b.jurisdiction_key = j.jurisdiction_key
        )
    """).bindparams(
        bindparam("state_codes", expanding=True)
    ), {
        "state_codes": states
    }).scalar_one()

    if missing != 0:
        raise ValueError(
            f"Existen {missing} jurisdicciones BRFSS "
            "sin correspondencia geogrÃ¡fica."
        )

    logger.info(
        "Puente geogrÃ¡fico BRFSS validado | correspondencias=%s",
        bridge_validation["total_mappings"],
    )

    return source_key, variable_map, jurisdiction_map


def load_facts(
    conn,
    records: list[dict],
    source_run_id: int,
    source_key: int,
    variable_map: dict,
    jurisdiction_map: dict,
) -> int:

    # 1. BLOQUEO TRANSACCIONAL   
    conn.execute(text("""
        SELECT pg_advisory_xact_lock(741, 2026)
    """))

    # 2. VALIDAR QUE EL RUN NO EXISTA EN GOLD

    existing = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_brfss_response
            WHERE source_run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).scalar_one()

    if existing:
        raise RuntimeError(
            f"Ya existen {existing} hechos para run_id={source_run_id}. "
            "No se sobrescriben versiones histÃ³ricas."
        )

    # 3. PREPARAR TABLA TEMPORAL

    conn.execute(text("""
        DROP TABLE IF EXISTS pg_temp.tmp_brfss_gold_payload
    """))

    conn.execute(text("""
        CREATE TEMP TABLE tmp_brfss_gold_payload
        ON COMMIT DROP
        AS
        SELECT
            source_key,
            jurisdiction_key,
            variable_key,
            sequence_number,
            raw_value,
            numeric_value,
            response_code_key
        FROM gold.fact_brfss_response
        WHERE FALSE
    """))

    # 4. PREPARAR INSERCIÃ“N TEMPORAL

    temp_insert = text("""
        INSERT INTO tmp_brfss_gold_payload (
            source_key,
            jurisdiction_key,
            variable_key,
            sequence_number,
            raw_value,
            numeric_value,
            response_code_key
        )
        VALUES (
            :source_key,
            :jurisdiction_key,
            :variable_key,
            :sequence_number,
            :raw_value,
            :numeric_value,
            NULL
        )
    """)

    payload = []

    # 5. CONSTRUIR PAYLOAD DESDE SILVER

    for person in records:

        data = person["record_data"]

        for variable, raw in data.items():

            raw_value = None if raw is None else str(raw)

            numeric_value = (
                parse_numeric(variable, raw_value)
                if variable in NUMERIC_VARIABLES
                else None
            )

            payload.append({
                "source_key": source_key,
                "jurisdiction_key": jurisdiction_map[
                    person["state_code"]
                ],
                "variable_key": variable_map[variable],
                "sequence_number": person["sequence_number"],
                "raw_value": raw_value,
                "numeric_value": numeric_value,
            })

    if len(payload) != EXPECTED_FACTS:
        raise RuntimeError(
            f"Payload incorrecto: {len(payload)} hechos; "
            f"se esperaban {EXPECTED_FACTS}."
        )

    # 6. CARGAR PAYLOAD TEMPORAL POR LOTES

    for start in range(0, len(payload), INSERT_BATCH_SIZE):

        batch = payload[start:start + INSERT_BATCH_SIZE]

        conn.execute(temp_insert, batch)

    temp_count = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM tmp_brfss_gold_payload
        """)
    ).scalar_one()

    if temp_count != EXPECTED_FACTS:
        raise RuntimeError(
            f"Payload temporal incorrecto: {temp_count}; "
            f"esperados={EXPECTED_FACTS}."
        )
  
    # 9. INSERTAR NUEVA VERSIÃ“N EN GOLD

    conn.execute(
        text("""
            INSERT INTO gold.fact_brfss_response (
                source_run_id,
                source_key,
                jurisdiction_key,
                variable_key,
                sequence_number,
                raw_value,
                numeric_value,
                response_code_key
            )
            SELECT
                :source_run_id,
                source_key,
                jurisdiction_key,
                variable_key,
                sequence_number,
                raw_value,
                numeric_value,
                response_code_key
            FROM tmp_brfss_gold_payload
        """),
        {"source_run_id": source_run_id},
    )

    # 10. VALIDAR RESULTADO DE LA CARGA

    persisted = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_brfss_response
            WHERE source_run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).scalar_one()

    if persisted != EXPECTED_FACTS:
        raise RuntimeError(
            f"ValidaciÃ³n de hechos fallida: "
            f"persistidos={persisted}, "
            f"esperados={EXPECTED_FACTS}."
        )

    logger.info(
        "Hechos BRFSS insertados: %s / %s",
        persisted,
        EXPECTED_FACTS,
    )

    return persisted



def run_etl(source_run_id: int | None = None):
    engine = create_db_engine()

    try:
        with engine.begin() as conn:

            source_run_id = resolve_source_run_id(
                conn,
                source_run_id,
            )

            source_run = validate_source_run(
                conn,
                source_run_id,
            )

            records = read_and_validate_silver(
                conn,
                source_run,
            )

            source_key, variable_map, jurisdiction_map = load_dimensions(
                conn,
                records,
            )

            loaded = load_facts(
                conn,
                records,
                source_run_id,
                source_key,
                variable_map,
                jurisdiction_map,
            )

            logger.info(
                "ETL Gold BRFSS completado | run_id=%s | hechos=%s",
                source_run_id,
                loaded,
            )

    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga BRFSS desde Silver hacia Gold."
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
        logger.exception("FallÃ³ el ETL Gold BRFSS.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
