from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from scripts.silver.silver_budget import lock_silver_budget


# ============================================================
# CONFIGURATION
# ============================================================

from scripts.database_config import get_target_database

ROOT = Path(__file__).resolve().parents[2]

DATASET_NAME = "geography_catalog"
TARGET_TABLE = "silver_geography+silver_county"

EXPECTED_COUNTIES = 83

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("etl_geography")


# ============================================================
# DATABASE CONNECTION
# ============================================================

def create_db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    allowed_database = get_target_database()
    allowed_host = "localhost"
    allowed_port = 5432

    database = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST")
    port_raw = os.getenv("POSTGRES_PORT")

    if database != allowed_database:
        raise RuntimeError(
            f"Conexión rechazada: base no permitida: {database!r}"
        )

    if host != allowed_host:
        raise RuntimeError(
            f"Conexión rechazada: host no permitido: {host!r}"
        )

    try:
        port = int(port_raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Puerto PostgreSQL inválido: {port_raw!r}"
        ) from exc

    if port != allowed_port:
        raise RuntimeError(
            f"Conexión rechazada: solo se permite el puerto "
            f"{allowed_port}. Configurado: {port}"
        )

    required = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]

    missing = [
        key for key in required
        if not os.getenv(key)
    ]

    if missing:
        raise RuntimeError(
            f"Faltan variables PostgreSQL: {', '.join(missing)}"
        )

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=allowed_host,
        port=allowed_port,
        database=allowed_database,
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

            if actual_database != allowed_database:
                raise RuntimeError(
                    f"PostgreSQL respondió con la base "
                    f"{actual_database!r}"
                )

        return engine

    except Exception:
        engine.dispose()
        raise


# ============================================================
# TARGET SCHEMA VALIDATION
# ============================================================

def validate_target_tables(engine: Engine) -> None:

    required_tables = {
        "silver_geography",
        "silver_county",
    }

    with engine.connect() as conn:
        found = set(
            conn.execute(
                text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name IN (
                          'silver_geography',
                          'silver_county'
                      )
                """)
            ).scalars()
        )

    missing = required_tables - found

    if missing:
        raise RuntimeError(
            "Faltan tablas geográficas. "
            "Aplica primero sql/silver/schema_silver.sql actualizado: "
            + ", ".join(sorted(missing))
        )


# ============================================================
# ETL AUDIT
# ============================================================

def start_run(engine: Engine) -> int:

    with engine.begin() as conn:

        run_id = conn.execute(
            text("""
                INSERT INTO public.etl_runs (
                    dataset_name,
                    target_table,
                    started_at,
                    status,
                    source_rows,
                    loaded_rows
                )
                VALUES (
                    :dataset,
                    :target,
                    clock_timestamp(),
                    'running',
                    0,
                    0
                )
                RETURNING run_id
            """),
            {
                "dataset": DATASET_NAME,
                "target": TARGET_TABLE,
            },
        ).scalar_one()

    return int(run_id)


def finish_run(
    engine: Engine,
    run_id: int,
    status: str,
    source_rows: int,
    loaded_rows: int,
    error: str | None = None,
) -> None:

    with engine.begin() as conn:

        conn.execute(
            text("""
                UPDATE public.etl_runs
                SET
                    finished_at = GREATEST(
                        clock_timestamp(),
                        started_at
                    ),
                    status = :status,
                    source_rows = :source_rows,
                    loaded_rows = :loaded_rows,
                    error_message = :error
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "status": status,
                "source_rows": source_rows,
                "loaded_rows": loaded_rows,
                "error": error,
            },
        )


# ============================================================
# SOURCE DATA EXTRACTION AND VALIDATION
# ============================================================

def collect_geographies(
    engine: Engine,
) -> tuple[list[dict], list[dict], int]:
    """
    Construye el catálogo geográfico desde las tablas Silver existentes.

    COUNTY:
        FIPS y nombre: ACS County.
        Abreviatura estatal: PLACES.
        Coordenadas: PLACES.

    STATE:
        FIPS: primeros dos dígitos del FIPS de condado.
        Nombre: componente estatal del NAME de ACS.
    """

    with engine.connect() as conn:

        # ----------------------------------------------------
        # ACS COUNTY
        # ----------------------------------------------------

            acs_rows = conn.execute(
            text("""
                SELECT
                    geography_id,
                    state_fips,
                    county_fips,
                    MIN(geography_name) AS geography_name,
                    COUNT(*) AS variable_rows,
                    COUNT(DISTINCT geography_name) AS name_variants
                FROM public.silver_acs_county_2024
                GROUP BY
                    geography_id,
                    state_fips,
                    county_fips
                ORDER BY geography_id
            """)
        ).mappings().all()

        # ----------------------------------------------------
        # PLACES
        # ----------------------------------------------------

            places_rows = conn.execute(
            text("""
                SELECT
                    location_id,
                    MIN(state_abbr) AS state_abbr,
                    COUNT(*) AS source_rows,
                    MIN(location_name) AS location_name,
                    MIN(latitude) AS latitude,
                    MIN(longitude) AS longitude,

                    COUNT(DISTINCT state_abbr)
                        AS state_variants,

                    COUNT(DISTINCT location_name)
                        AS name_variants,

                    COUNT(DISTINCT latitude)
                        FILTER (WHERE latitude IS NOT NULL)
                        AS latitude_variants,

                    COUNT(DISTINCT longitude)
                        FILTER (WHERE longitude IS NOT NULL)
                        AS longitude_variants,

                    COUNT(*) FILTER (
                        WHERE state_abbr IS NULL
                           OR BTRIM(state_abbr) = ''
                    ) AS missing_state_abbr,

                    COUNT(*) FILTER (
                        WHERE location_name IS NULL
                           OR BTRIM(location_name) = ''
                    ) AS missing_location_name,

                    COUNT(*) FILTER (
                        WHERE (latitude IS NULL AND longitude IS NOT NULL)
                           OR (latitude IS NOT NULL AND longitude IS NULL)
                    ) AS incomplete_coordinates

                FROM public.silver_places_2023
                GROUP BY location_id
                ORDER BY location_id
            """)
        ).mappings().all()

    if not acs_rows:
        raise ValueError(
            "silver_acs_county_2024 no contiene registros."
        )

    if not places_rows:
        raise ValueError(
            "silver_places_2023 no contiene registros."
        )

    source_rows = (
        sum(int(row["variable_rows"]) for row in acs_rows)
        + sum(int(row["source_rows"]) for row in places_rows)
    )

    
    # ========================================================
    # NORMALIZE ACS GEOGRAPHIES
    # ========================================================

    acs_by_fips: dict[str, dict] = {}
    state_names: dict[str, str] = {}

    for row in acs_rows:

        fips = str(row["geography_id"]).strip()
        state_fips = str(row["state_fips"]).strip()
        county_fips = str(row["county_fips"]).strip()
        full_name = str(row["geography_name"]).strip()

        if not re.fullmatch(r"\d{5}", fips):
            raise ValueError(
                f"FIPS de condado inválido en ACS: {fips!r}"
            )

        if not re.fullmatch(r"\d{2}", state_fips):
            raise ValueError(
                f"State FIPS inválido para {fips}: {state_fips!r}"
            )

        if not re.fullmatch(r"\d{3}", county_fips):
            raise ValueError(
                f"County FIPS inválido para {fips}: {county_fips!r}"
            )

        if fips != state_fips + county_fips:
            raise ValueError(
                f"Composición FIPS inconsistente para {fips}."
            )

        if row["variable_rows"] != 4:
            raise ValueError(
                f"Se esperaban 4 indicadores ACS para {fips}; "
                f"se encontraron {row['variable_rows']}."
            )

        if row["name_variants"] != 1:
            raise ValueError(
                f"Nombre ACS inconsistente para {fips}."
            )

        # ACS NAME tiene el formato:
        # Alameda County, California

        if "," not in full_name:
            raise ValueError(
                f"Nombre ACS no permite extraer el estado: "
                f"{full_name!r}"
            )

        county_name, state_name = (
            part.strip()
            for part in full_name.rsplit(",", 1)
        )

        if not county_name or not state_name:
            raise ValueError(
                f"Nombre geográfico ACS incompleto: {full_name!r}"
            )

        previous_state_name = state_names.setdefault(
            state_fips,
            state_name,
        )

        if previous_state_name != state_name:
            raise ValueError(
                f"FIPS estatal {state_fips} presenta nombres "
                f"inconsistentes: {previous_state_name!r} "
                f"y {state_name!r}."
            )

        if fips in acs_by_fips:
            raise ValueError(
                f"FIPS duplicado en ACS County: {fips}"
            )

        acs_by_fips[fips] = {
            "state_fips": state_fips,
            "county_fips": county_fips,
            "county_name": county_name,
        }

    # ========================================================
    # NORMALIZE PLACES GEOGRAPHIES
    # ========================================================

    places_by_fips: dict[str, dict] = {}
    state_abbr_by_fips: dict[str, str] = {}

    for row in places_rows:

        fips = str(row["location_id"]).strip()

        if (
            row["missing_state_abbr"] > 0
            or row["missing_location_name"] > 0
        ):
            raise ValueError(
                f"Hay abreviaturas o nombres faltantes en PLACES: {fips}"
            )

        if row["incomplete_coordinates"] > 0:
            raise ValueError(
                f"Hay coordenadas incompletas en PLACES: {fips}"
            )

        if not re.fullmatch(r"\d{5}", fips):
            raise ValueError(
                f"FIPS inválido en PLACES: {fips!r}"
            )

        if (
            row["state_variants"] != 1
            or row["name_variants"] != 1
        ):
            raise ValueError(
                f"Nombre o abreviatura estatal inconsistente "
                f"en PLACES: {fips}"
            )

        if (
            row["latitude_variants"] > 1
            or row["longitude_variants"] > 1
        ):
            raise ValueError(
                f"Coordenadas inconsistentes en PLACES: {fips}"
            )

        state_abbr = str(row["state_abbr"]).strip().upper()

        if not re.fullmatch(r"[A-Z]{2}", state_abbr):
            raise ValueError(
                f"Abreviatura estatal inválida para {fips}: "
                f"{state_abbr!r}"
            )

        latitude = row["latitude"]
        longitude = row["longitude"]

        if (
            latitude is not None
            and not -90 <= float(latitude) <= 90
        ):
            raise ValueError(
                f"Latitud fuera de rango para {fips}."
            )

        if (
            longitude is not None
            and not -180 <= float(longitude) <= 180
        ):
            raise ValueError(
                f"Longitud fuera de rango para {fips}."
            )

        state_fips = fips[:2]

        previous_abbr = state_abbr_by_fips.setdefault(
            state_fips,
            state_abbr,
        )

        if previous_abbr != state_abbr:
            raise ValueError(
                f"FIPS estatal {state_fips} tiene "
                f"abreviaturas inconsistentes."
            )

        places_by_fips[fips] = {
            "state_abbr": state_abbr,
            "location_name": str(
                row["location_name"]
            ).strip(),
            "latitude": latitude,
            "longitude": longitude,
        }

    # ========================================================
    # CROSS-SOURCE VALIDATION
    # ========================================================

    acs_fips = set(acs_by_fips)
    places_fips = set(places_by_fips)

    if acs_fips != places_fips:

        only_acs = sorted(acs_fips - places_fips)
        only_places = sorted(places_fips - acs_fips)

        raise ValueError(
            "Los universos geográficos ACS y PLACES no coinciden. "
            f"Solo ACS: {only_acs[:10]}; "
            f"solo PLACES: {only_places[:10]}."
        )

    if len(acs_by_fips) != EXPECTED_COUNTIES:
        raise ValueError(
            f"Se esperaban {EXPECTED_COUNTIES} condados; "
            f"se encontraron {len(acs_by_fips)}."
        )


    # ========================================================
    # BUILD COUNTY RECORDS
    # ========================================================

    counties = []

    for fips, acs in sorted(acs_by_fips.items()):

        places = places_by_fips[fips]

        # ACS es la fuente del nombre canónico.
        # PLACES se utiliza para contrastar el nombre.

        if (
            places["location_name"]
            and places["location_name"].casefold()
            != acs["county_name"].casefold()
        ):
            log.warning(
                "Nombre diferente para %s | ACS=%r | PLACES=%r. "
                "Se conserva el nombre ACS.",
                fips,
                acs["county_name"],
                places["location_name"],
            )

        counties.append({
            "geography_code": fips,
            "geography_name": acs["county_name"],
            "state_fips": acs["state_fips"],
            "county_fips": acs["county_fips"],
            "state_abbr": places["state_abbr"],
            "latitude": places["latitude"],
            "longitude": places["longitude"],
        })

    # ========================================================
    # BUILD STATE RECORDS
    # ========================================================

    states_by_fips = {}

    for county in counties:

        state_fips = county["state_fips"]
        state_name = state_names[state_fips]
        state_abbr = county["state_abbr"]

        previous = states_by_fips.get(state_fips)

        if previous and (
            previous["geography_name"] != state_name
            or previous["state_abbr"] != state_abbr
        ):
            raise ValueError(
                f"Datos estatales inconsistentes "
                f"para FIPS {state_fips}."
            )

        states_by_fips[state_fips] = {
            "geography_code": state_fips,
            "geography_name": state_name,
            "state_abbr": state_abbr,
        }

    states = list(states_by_fips.values())

    log.info(
        "Geografías preparadas: %s estados y %s condados.",
        len(states),
        len(counties),
    )

    return states, counties, source_rows
# ============================================================
# GEOGRAPHY LOADING
# ============================================================

def load_geographies(
    engine: Engine,
    states: list[dict],
    counties: list[dict],
) -> int:
    """
    Carga idempotente del catálogo geográfico.

    Orden:
        1. Estados.
        2. Resolución de sus geography_id.
        3. Condados con parent_geography_id.
        4. Dimensión silver_county.
        5. Validaciones.

    No elimina registros de las tablas de datos existentes.
    """

    with engine.begin() as conn:

        # Serializa la operación con las demás cargas Silver.
        lock_silver_budget(conn)

        # ----------------------------------------------------
        # 1. INSERT / UPDATE states
        # ----------------------------------------------------

        for state in states:

            conn.execute(
                text("""
                    INSERT INTO public.silver_geography (
                        geography_level,
                        geography_code,
                        geography_name,
                        parent_geography_id,
                        latitude,
                        longitude
                    )
                    VALUES (
                        'STATE',
                        :code,
                        :name,
                        NULL,
                        NULL,
                        NULL
                    )
                    ON CONFLICT (
                        geography_level,
                        geography_code
                    )
                    DO UPDATE SET
                        geography_name = EXCLUDED.geography_name
                """),
                {
                    "code": state["geography_code"],
                    "name": state["geography_name"],
                },
            )

        # ----------------------------------------------------
        # 2. resuelve ids de STATE
        # ----------------------------------------------------

        state_ids = dict(
            conn.execute(
                text("""
                    SELECT
                        geography_code,
                        geography_id
                    FROM public.silver_geography
                    WHERE geography_level = 'STATE'
                      AND geography_code = ANY(:codes)
                """),
                {
                    "codes": [
                        state["geography_code"]
                        for state in states
                    ]
                },
            ).all()
        )

        if len(state_ids) != len(states):
            raise RuntimeError(
                "No se pudieron resolver todos los geography_id "
                "de los estados."
            )

        # ----------------------------------------------------
        # 3. INSERT / UPDATE counties
        # ----------------------------------------------------

        for county in counties:

            parent_id = state_ids[county["state_fips"]]

            conn.execute(
                text("""
                    INSERT INTO public.silver_geography (
                        geography_level,
                        geography_code,
                        geography_name,
                        parent_geography_id,
                        latitude,
                        longitude
                    )
                    VALUES (
                        'COUNTY',
                        :code,
                        :name,
                        :parent,
                        :latitude,
                        :longitude
                    )
                    ON CONFLICT (
                        geography_level,
                        geography_code
                    )
                    DO UPDATE SET
                        geography_name = EXCLUDED.geography_name,
                        parent_geography_id =
                            EXCLUDED.parent_geography_id,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude
                """),
                {
                    "code": county["geography_code"],
                    "name": county["geography_name"],
                    "parent": parent_id,
                    "latitude": county["latitude"],
                    "longitude": county["longitude"],
                },
            )

        # ----------------------------------------------------
        # 4. resuelve countiesIDs
        # ----------------------------------------------------

        county_ids = dict(
            conn.execute(
                text("""
                    SELECT
                        geography_code,
                        geography_id
                    FROM public.silver_geography
                    WHERE geography_level = 'COUNTY'
                      AND geography_code = ANY(:codes)
                """),
                {
                    "codes": [
                        county["geography_code"]
                        for county in counties
                    ]
                },
            ).all()
        )

        if len(county_ids) != len(counties):
            raise RuntimeError(
                "No se pudieron resolver todos los geography_id "
                "de los condados."
            )

        # ----------------------------------------------------
        # 5. INSERT / UPDATE SILVER_COUNTY
        # ----------------------------------------------------

        for county in counties:

            conn.execute(
                text("""
                    INSERT INTO public.silver_county (
                        geography_id,
                        state_fips,
                        county_fips,
                        state_abbr
                    )
                    VALUES (
                        :geography_id,
                        :state_fips,
                        :county_fips,
                        :state_abbr
                    )
                    ON CONFLICT (geography_id)
                    DO UPDATE SET
                        state_fips = EXCLUDED.state_fips,
                        county_fips = EXCLUDED.county_fips,
                        state_abbr = EXCLUDED.state_abbr
                """),
                {
                    "geography_id": county_ids[
                        county["geography_code"]
                    ],
                    "state_fips": county["state_fips"],
                    "county_fips": county["county_fips"],
                    "state_abbr": county["state_abbr"],
                },
            )

        # ----------------------------------------------------
        # 6. geografia
        # ----------------------------------------------------

        all_codes = (
            [state["geography_code"] for state in states]
            + [county["geography_code"] for county in counties]
        )

        counts = conn.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE sg.geography_level = 'STATE'
                    ) AS states,

                    COUNT(*) FILTER (
                        WHERE sg.geography_level = 'COUNTY'
                    ) AS counties,

                    COUNT(*) FILTER (
                        WHERE sg.geography_level = 'COUNTY'
                          AND sg.parent_geography_id IS NULL
                    ) AS counties_without_parent,

                    COUNT(*) FILTER (
                        WHERE sg.geography_level = 'STATE'
                          AND sg.parent_geography_id IS NOT NULL
                    ) AS states_with_parent,

                    COUNT(*) FILTER (
                        WHERE sg.geography_level = 'COUNTY'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM public.silver_geography parent
                              WHERE parent.geography_id =
                                    sg.parent_geography_id
                                AND parent.geography_level = 'STATE'
                                AND parent.geography_code =
                                    LEFT(sg.geography_code, 2)
                          )
                    ) AS counties_with_invalid_parent

                FROM public.silver_geography sg
                WHERE sg.geography_code = ANY(:codes)
            """),
            {
                "codes": all_codes,
            },
        ).mappings().one()

        # ----------------------------------------------------
        # 7. dimension de county
        # ----------------------------------------------------

        dimension_rows = int(
            conn.execute(
                text("""
                    SELECT COUNT(*)
                    FROM public.silver_county sc
                    INNER JOIN public.silver_geography sg
                        ON sc.geography_id = sg.geography_id
                    WHERE sg.geography_level = 'COUNTY'
                      AND sg.geography_code = ANY(:codes)
                """),
                {
                    "codes": [
                        county["geography_code"]
                        for county in counties
                    ]
                },
            ).scalar_one()
        )

        if (
            counts["states"] != len(states)
            or counts["counties"] != len(counties)
            or counts["counties_without_parent"] != 0
            or counts["states_with_parent"] != 0
            or counts["counties_with_invalid_parent"] != 0
            or dimension_rows != len(counties)
        ):
            raise RuntimeError(
                "Validación geográfica fallida: "
                f"{dict(counts)}, "
                f"silver_county={dimension_rows}."
            )

        log.info(
            "Validación correcta: %s estados, %s condados, "
            "%s registros en silver_county.",
            len(states),
            len(counties),
            dimension_rows,
        )

    # Las dimensiones geográficas son tablas de referencia.
    # No se incluyen en el presupuesto de celdas analíticas.
    return len(states) + 2 * len(counties)


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    parser = argparse.ArgumentParser(
        description=(
            "Construye silver_geography y silver_county "
            "desde Silver PLACES y ACS County."
        )
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Valida y prepara el catálogo sin modificar "
            "las tablas geográficas."
        ),
    )

    args = parser.parse_args()

    engine = create_db_engine()

    run_id = None
    source_rows = 0
    loaded_rows = 0

    try:

        validate_target_tables(engine)

        run_id = start_run(engine)

        states, counties, source_rows = collect_geographies(engine)

        # ----------------------------------------------------
        # DRY RUN
        # ----------------------------------------------------

        if args.dry_run:

            finish_run(
                engine,
                run_id,
                "succeeded",
                source_rows,
                0,
            )

            log.info(
                "Dry-run correcto: %s estados y %s condados. "
                "Silver no fue modificada.",
                len(states),
                len(counties),
            )

            return 0

        # ----------------------------------------------------
        # DEFINITIVE LOAD
        # ----------------------------------------------------

        loaded_rows = load_geographies(
            engine,
            states,
            counties,
        )

        finish_run(
            engine,
            run_id,
            "succeeded",
            source_rows,
            loaded_rows,
        )

        log.info(
            "ETL geográfico completado | run_id=%s | "
            "estados=%s | condados=%s",
            run_id,
            len(states),
            len(counties),
        )

        return 0

    except Exception as exc:

        log.exception("Falló el ETL geográfico.")

        if run_id is not None:
            try:
                finish_run(
                    engine,
                    run_id,
                    "failed",
                    source_rows,
                    loaded_rows,
                    str(exc)[:4000],
                )
            except Exception:
                log.exception(
                    "No fue posible actualizar la auditoría ETL."
                )

        return 1

    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
