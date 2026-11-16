import argparse
import json
import logging

from sqlalchemy import text
from decimal import Decimal, InvalidOperation

from scripts.gold.db_connection import create_gold_engine


EXPECTED_DATASET = "geography_catalog"
EXPECTED_TARGET = "silver_geography+silver_county"

EXPECTED_STATES = 49
EXPECTED_COUNTIES = 83
EXPECTED_COUNTY_ATTRIBUTES = 83
EXPECTED_LOADED_ROWS = 215
EXPECTED_GOLD_STATES = 52

logger = logging.getLogger(__name__)


def validate_source_run(conn, source_run_id: int) -> None:
    """Valida la ejecución auditada que produjo el catálogo geográfico."""

    run = conn.execute(
        text("""
            SELECT
                run_id,
                dataset_name,
                target_table,
                status,
                source_rows,
                loaded_rows,
                finished_at
            FROM public.etl_runs
            WHERE run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).mappings().one_or_none()

    if run is None:
        raise RuntimeError(
            f"No existe la ejecución geográfica run_id={source_run_id}."
        )

    expected = {
        "dataset_name": EXPECTED_DATASET,
        "target_table": EXPECTED_TARGET,
        "status": "succeeded",
        "loaded_rows": EXPECTED_LOADED_ROWS,
    }

    for field, value in expected.items():
        if run[field] != value:
            raise RuntimeError(
                f"Registro geográfico inválido: {field}="
                f"{run[field]!r}; esperado={value!r}."
            )

    source_rows = run["source_rows"]
    if source_rows is None or int(source_rows) <= 0:
        raise RuntimeError(
            "La ejecución geográfica debe registrar source_rows positivo; "
            f"valor recibido={source_rows!r}."
        )

    if run["finished_at"] is None:
        raise RuntimeError(
            "La ejecución geográfica no tiene finished_at."
        )

    logger.info(
        "Ejecución Silver validada | run_id=%s | filas=%s",
        source_run_id,
        run["loaded_rows"],
    )


def read_and_validate_catalog(conn) -> list[dict]:
    """Lee el catálogo Silver y comprueba su integridad."""

    # Evita modificaciones concurrentes mientras se consume Silver.
    conn.execute(text("""
        LOCK TABLE
            public.silver_geography,
            public.silver_county
        IN SHARE MODE
    """))

    counts = conn.execute(text("""
        SELECT
            COUNT(*) FILTER (
                WHERE sg.geography_level = 'STATE'
            ) AS states,

            COUNT(*) FILTER (
                WHERE sg.geography_level = 'COUNTY'
            ) AS counties,

            COUNT(sc.geography_id) FILTER (
                WHERE sg.geography_level = 'COUNTY'
            ) AS county_attributes,

            COUNT(*) FILTER (
                WHERE sg.geography_level = 'COUNTY'
                  AND (
                      parent.geography_id IS NULL
                      OR parent.geography_level <> 'STATE'
                      OR parent.geography_code <> sc.state_fips
                      OR sg.geography_code <>
                         sc.state_fips || sc.county_fips
                  )
            ) AS invalid_counties,

            COUNT(*) FILTER (
                WHERE sg.geography_level = 'STATE'
                  AND sg.parent_geography_id IS NOT NULL
            ) AS states_with_parent

        FROM public.silver_geography sg

        LEFT JOIN public.silver_county sc
            ON sc.geography_id = sg.geography_id

        LEFT JOIN public.silver_geography parent
            ON parent.geography_id = sg.parent_geography_id
    """)).mappings().one()

    expected_counts = {
        "states": EXPECTED_STATES,
        "counties": EXPECTED_COUNTIES,
        "county_attributes": EXPECTED_COUNTY_ATTRIBUTES,
        "invalid_counties": 0,
        "states_with_parent": 0,
    }

    for field, expected in expected_counts.items():
        actual = int(counts[field])

        if actual != expected:
            raise RuntimeError(
                f"Catálogo Silver inválido: {field}={actual}; "
                f"esperado={expected}."
            )

    # Lee y prepara los 83 condados.
    records = conn.execute(text("""
        SELECT
            sg.geography_code AS location_id,
            sg.geography_name AS location_name,
            sg.latitude,
            sg.longitude,
            sc.state_fips,
            sc.county_fips,
            sc.state_abbr,
            parent.geography_code AS parent_state_fips

        FROM public.silver_geography sg

        INNER JOIN public.silver_county sc
            ON sc.geography_id = sg.geography_id

        INNER JOIN public.silver_geography parent
            ON parent.geography_id = sg.parent_geography_id

        WHERE sg.geography_level = 'COUNTY'

        ORDER BY sg.geography_code
    """)).mappings().all()

    if len(records) != EXPECTED_COUNTIES:
        raise RuntimeError(
            f"Se esperaban {EXPECTED_COUNTIES} condados; "
            f"se encontraron {len(records)}."
        )

    payload = [dict(row) for row in records]

    # Comprueba que cada condado tenga su estado correspondiente
    # y que la abreviatura coincida con el catálogo Gold.
    state_validation = conn.execute(text("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (
                WHERE gold_state.location_id IS NULL
            ) AS missing_states,
            COUNT(*) FILTER (
                WHERE gold_state.location_name <> silver_state.geography_name
            ) AS name_mismatches,
            COUNT(*) FILTER (
                WHERE gold_state.state_abbr <> county.state_abbr
            ) AS abbreviation_mismatches

        FROM public.silver_county county

        INNER JOIN public.silver_geography county_geo
            ON county_geo.geography_id = county.geography_id

        INNER JOIN public.silver_geography silver_state
            ON silver_state.geography_code = county.state_fips
           AND silver_state.geography_level = 'STATE'

        LEFT JOIN gold.dim_geography gold_state
            ON gold_state.location_id = county.state_fips
           AND gold_state.geography_level = 'STATE'
    """)).mappings().one()

    if (
        state_validation["total"] != EXPECTED_COUNTIES
        or state_validation["missing_states"] != 0
        or state_validation["name_mismatches"] != 0
        or state_validation["abbreviation_mismatches"] != 0
    ):
        raise RuntimeError(
            "Los estados padres de los condados no coinciden "
            f"con Gold: {dict(state_validation)}"
        )

    logger.info(
        "Catálogo Silver validado | estados=%s | condados=%s",
        counts["states"],
        counts["counties"],
    )

    return payload


def validate_gold_state_catalog(conn) -> None:
    """Comprueba los 49 estados Silver y las 3 jurisdicciones adicionales."""

    counts = conn.execute(text("""
        SELECT
            COUNT(*) AS total_states,

            COUNT(*) FILTER (
                WHERE location_id IN ('66', '72', '78')
            ) AS additional_jurisdictions

        FROM gold.dim_geography
        WHERE geography_level = 'STATE'
    """)).mappings().one()

    if counts["total_states"] != EXPECTED_GOLD_STATES:
        raise RuntimeError(
            "El catálogo Gold debe conservar exactamente "
            f"{EXPECTED_GOLD_STATES} jurisdicciones; "
            f"encontradas={counts['total_states']}."
        )

    if counts["additional_jurisdictions"] != 3:
        raise RuntimeError(
            "Gold debe conservar Guam, Puerto Rico y "
            "U.S. Virgin Islands (FIPS 66, 72 y 78)."
        )

    mismatches = conn.execute(text("""
        SELECT
            silver.geography_code,
            silver.geography_name,
            gold.location_name,
            gold.state_abbr

        FROM public.silver_geography silver

        LEFT JOIN gold.dim_geography gold
            ON gold.location_id = silver.geography_code
           AND gold.geography_level = 'STATE'

        WHERE silver.geography_level = 'STATE'
          AND (
              gold.location_id IS NULL
              OR gold.state_fips IS DISTINCT FROM silver.geography_code
              OR gold.location_name IS DISTINCT FROM silver.geography_name
          )

        ORDER BY silver.geography_code
    """)).mappings().all()

    if mismatches:
        raise RuntimeError(
            "Existen diferencias entre los estados Silver y Gold: "
            f"{[dict(row) for row in mismatches]}"
        )

    logger.info(
        "Catálogo Gold validado | 52 jurisdicciones | "
        "49 estados Silver coincidentes | 3 jurisdicciones adicionales"
    )


def same_numeric_value(left, right) -> bool:
    """Compara valores numéricos sin depender de su tipo Python."""

    if left is None or right is None:
        return left is right

    try:
        return Decimal(str(left)) == Decimal(str(right))
    except (InvalidOperation, ValueError, TypeError):
        return False
    

def validate_existing_counties(conn, records: list[dict]) -> int:
    """Detecta condados existentes y rechaza atributos incompatibles."""

    existing = conn.execute(text("""
        SELECT
            location_id,
            state_fips,
            county_fips,
            state_abbr,
            location_name,
            latitude,
            longitude,
            parent_geography_key,
            geography_key,
            country_code

        FROM gold.dim_geography

        WHERE geography_level = 'COUNTY'
    """)).mappings().all()

    existing_by_id = {
        row["location_id"]: dict(row)
        for row in existing
    }

    existing_by_fips = {
        (row["state_fips"], row["county_fips"]): dict(row)
        for row in existing
    }

    for record in records:
        location_id = record["location_id"]
        fips_key = (
            record["state_fips"],
            record["county_fips"],
        )

        by_id = existing_by_id.get(location_id)
        by_fips = existing_by_fips.get(fips_key)

        if by_id is not None and by_fips is not None:
            if by_id["geography_key"] != by_fips["geography_key"]:
                raise RuntimeError(
                    f"Conflicto de identidad geográfica: {location_id}."
                )

        existing_row = by_id or by_fips

        if existing_row is None:
            continue

        expected = {
            "location_id": location_id,
            "state_fips": record["state_fips"],
            "county_fips": record["county_fips"],
            "state_abbr": record["state_abbr"],
            "location_name": record["location_name"],
            "country_code": "US",
        }

        for field, value in expected.items():
            if existing_row[field] != value:
                raise RuntimeError(
                    f"Condado existente incompatible: {location_id}; "
                    f"{field}: Gold={existing_row[field]!r}, "
                    f"Silver={value!r}."
                )

        if (
                existing_row["parent_geography_key"] is None
                or not same_numeric_value(
                    existing_row["latitude"],
                    record["latitude"],
                )
                or not same_numeric_value(
                    existing_row["longitude"],
                    record["longitude"],
                )
            ):
                
            raise RuntimeError(
                f"El condado {location_id} tiene coordenadas "
                "o jerarquía incompatibles con Silver."
            )

        parent_state = conn.execute(text("""
            SELECT state_fips
            FROM gold.dim_geography
            WHERE geography_key = :parent_key
              AND geography_level = 'STATE'
        """), {
            "parent_key": existing_row["parent_geography_key"],
        }).scalar_one_or_none()

        if parent_state != record["state_fips"]:
            raise RuntimeError(
                f"Estado padre incorrecto para el condado {location_id}."
            )

    # Cuenta únicamente los condados de este lote que ya existen en Gold.
    # No debe influir la presencia de otros condados ajenos al catálogo Silver.
    return sum(
        1
        for record in records
        if (
            record["location_id"] in existing_by_id
            or (
                record["state_fips"],
                record["county_fips"],
            ) in existing_by_fips
        )
    )


def load_counties(conn, records: list[dict]) -> int:
    """Inserta los condados faltantes sin sobrescribir registros existentes."""

    conn.execute(text("""
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
        SELECT
            source.location_id,
            source.state_abbr,
            source.location_name,
            source.latitude,
            source.longitude,
            'COUNTY',
            source.state_fips,
            source.county_fips,
            parent.geography_key,
            'US'

        FROM jsonb_to_recordset(CAST(:records AS JSONB)) AS source(
            location_id TEXT,
            location_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            state_fips VARCHAR(2),
            county_fips VARCHAR(3),
            state_abbr VARCHAR(2)
        )

        INNER JOIN gold.dim_geography parent
            ON parent.location_id = source.state_fips
           AND parent.geography_level = 'STATE'

        ON CONFLICT (location_id) DO NOTHING
    """), {
        "records": json.dumps(records, default=str),
    })

    return int(conn.execute(text("""
        SELECT COUNT(*)
        FROM gold.dim_geography
        WHERE geography_level = 'COUNTY'
          AND location_id = ANY(:locations)
    """), {
        "locations": [record["location_id"] for record in records],
    }).scalar_one())


def validate_loaded_counties(conn, records: list[dict]) -> None:
    """Comprueba que los 83 condados quedaron correctamente persistidos."""

    validation = conn.execute(text("""
        SELECT
            COUNT(*) AS total,

            COUNT(*) FILTER (
                WHERE gold.location_name IS DISTINCT FROM silver.location_name
                   OR gold.state_fips IS DISTINCT FROM silver.state_fips
                   OR gold.county_fips IS DISTINCT FROM silver.county_fips
                   OR gold.state_abbr IS DISTINCT FROM silver.state_abbr
                   OR gold.latitude IS DISTINCT FROM silver.latitude
                   OR gold.longitude IS DISTINCT FROM silver.longitude
                   OR gold.country_code IS DISTINCT FROM 'US'
            ) AS attribute_mismatches,

            COUNT(*) FILTER (
                WHERE parent.state_fips IS DISTINCT FROM silver.state_fips
                   OR parent.geography_level IS DISTINCT FROM 'STATE'
            ) AS parent_mismatches

        FROM jsonb_to_recordset(CAST(:records AS JSONB)) AS silver(
            location_id TEXT,
            location_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            state_fips VARCHAR(2),
            county_fips VARCHAR(3),
            state_abbr VARCHAR(2)
        )

        INNER JOIN gold.dim_geography gold
            ON gold.location_id = silver.location_id
           AND gold.geography_level = 'COUNTY'

        LEFT JOIN gold.dim_geography parent
            ON parent.geography_key = gold.parent_geography_key
    """), {
        "records": json.dumps(records, default=str),
    }).mappings().one()

    if (
        validation["total"] != EXPECTED_COUNTIES
        or validation["attribute_mismatches"] != 0
        or validation["parent_mismatches"] != 0
    ):
        raise RuntimeError(
            "La validación posterior a la carga falló: "
            f"{dict(validation)}"
        )

    logger.info(
        "Condados Gold validados | total=%s | diferencias=0",
        validation["total"],
    )


def run_etl(source_run_id: int, dry_run: bool = False) -> None:
    engine = create_gold_engine()

    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(741, 2027)"))

            validate_source_run(conn, source_run_id)
            validate_gold_state_catalog(conn)

            records = read_and_validate_catalog(conn)

            existing_count = validate_existing_counties(
                conn,
                records,
            )

            missing_count = EXPECTED_COUNTIES - existing_count

            logger.info(
                "Estado previo | condados existentes=%s | "
                "condados pendientes=%s",
                existing_count,
                missing_count,
            )

            if dry_run:
                logger.info(
                    "DRY-RUN correcto. No se modificó Gold. "
                    "Se insertarían %s condados.",
                    missing_count,
                )
                return

            loaded_count = load_counties(conn, records)

            if loaded_count != EXPECTED_COUNTIES:
                raise RuntimeError(
                    f"Se esperaban {EXPECTED_COUNTIES} condados "
                    f"en Gold; encontrados={loaded_count}."
                )

            validate_loaded_counties(conn, records)

            logger.info(
                "ETL geográfico Gold completado | "
                "condados=%s | insertados=%s",
                loaded_count,
                missing_count,
            )

    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga la dimensión geográfica Gold desde Silver."
    )

    parser.add_argument(
        "--source-run-id",
        type=int,
        required=True,
        help=(
            "run_id de la ejecución Silver exitosa del catálogo geográfico. "
            "El orquestador Gold lo selecciona automáticamente."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta las validaciones sin modificar Gold.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        run_etl(
            source_run_id=args.source_run_id,
            dry_run=args.dry_run,
        )
        return 0

    except Exception:
        logger.exception("Falló el ETL geográfico Gold.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
