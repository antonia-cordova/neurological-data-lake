from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
import logging
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

from scripts.database_connection import create_database_engine
from scripts.database_config import get_target_database
from scripts.gold.silver_run_validator import resolve_single_silver_run_id
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]

SOURCE_TABLE = "silver_acs_2024"
DATASET_NAME = "acs_2024"

EXPECTED_SOURCE_ROWS = 113
EXPECTED_FACT_ROWS = 108
EXPECTED_SECTION_ROWS = 5
EXPECTED_ROOT_SOURCE_ROW_ID = 109

INSERT_BATCH_SIZE = 100

# Debe coincidir con el bloqueo global de scripts/run_gold_pipeline.py.
GOLD_PIPELINE_LOCK_KEY = 78_115_004_452_421
DATABASE_ALLOWED = get_target_database()

INDENT_CHARS = " \t\u00a0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("etl_gold_acs")


def resolve_source_run_id(
    conn,
    requested_run_id: int | None = None,
) -> int:
    """Selecciona y valida una ejecución Silver de ACS nacional."""

    run_id = resolve_single_silver_run_id(
        conn,
        SOURCE_TABLE,
        requested_run_id=requested_run_id,
        expected_rows=EXPECTED_SOURCE_ROWS,
        dataset_name=DATASET_NAME,
        target_table=SOURCE_TABLE,
    )

    logger.info(
        "Run ID ACS nacional seleccionado y validado | run_id=%s",
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
            FROM datalake_control.etl_runs
            WHERE run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).mappings().one_or_none()

    if run is None:
        raise ValueError(
            f"No existe la ejecución run_id={source_run_id}."
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
            "La ejecución de Silver no terminó correctamente."
        )

    if run["source_rows"] != EXPECTED_SOURCE_ROWS:
        raise ValueError(
            f"etl_runs.source_rows informa {run['source_rows']}; "
            f"se esperaban {EXPECTED_SOURCE_ROWS}."
        )

    if run["loaded_rows"] != EXPECTED_SOURCE_ROWS:
        raise ValueError(
            f"etl_runs.loaded_rows informa {run['loaded_rows']}; "
            f"se esperaban {EXPECTED_SOURCE_ROWS}."
        )

    return dict(run)


def parse_numeric(raw_value: str | None) -> Decimal | None:
    """
    Convierte valores numéricos ACS sin alterar la columna raw.

    Admite separadores de miles correctamente agrupados, el prefijo ±
    utilizado en márgenes de error y el sufijo % utilizado en porcentajes.
    Los marcadores estadísticos se conservan en raw y se representan como NULL.
    """
    if raw_value is None or raw_value.strip() == "":
        return None

    value = raw_value.strip()

    if value in {"*****", "(X)"}:
        return None

    if value.startswith("\u00b1"):
        value = value[1:].strip()

    if value.endswith("%"):
        value = value[:-1].strip()

    # No eliminar símbolos de forma indiscriminada: una cadena mal formada
    # debe rechazarse, no convertirse silenciosamente en otro número.
    numeric_pattern = r"-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?"
    if not re.fullmatch(numeric_pattern, value):
        raise ValueError(
            f"Formato numérico ACS no reconocido: {raw_value!r}"
        )

    try:
        return Decimal(value.replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError(
            f"No se pudo convertir el valor: {raw_value!r}"
        ) from exc

def indentation_and_label(label: str) -> tuple[int, str]:
    """
    Conserva el número original de caracteres de indentación.
    No utiliza strip() sobre el valor almacenado.
    """
    indentation = len(label) - len(label.lstrip(INDENT_CHARS))
    normalized = label.lstrip(INDENT_CHARS).rstrip(INDENT_CHARS)

    return indentation, normalized


def classify_percent(raw_value: str | None) -> tuple:
    """
    Devuelve:
    (tipo, porcentaje_numérico, conteo_numérico)
    """
    if raw_value is None or raw_value.strip() == "":
        return "NULL", None, None

    value = raw_value.strip()

    if value == "(X)":
        return "NOT_AVAILABLE", None, None

    if value.endswith("%"):
        percentage = parse_numeric(value)
        if percentage is None or not (Decimal("0") <= percentage <= Decimal("100")):
            raise ValueError(
                f"Porcentaje ACS fuera del rango 0-100: {raw_value!r}"
            )
        return "PERCENTAGE", percentage, None

    count_value = parse_numeric(value)
    if count_value is None or count_value < 0:
        raise ValueError(
            f"Conteo ACS negativo o ausente: {raw_value!r}"
        )
    if count_value != count_value.to_integral_value():
        raise ValueError(
            f"Se esperaba un conteo entero en percent: {raw_value!r}"
        )

    return "COUNT_VALUE", None, count_value


def read_and_validate_silver(conn, source_run: dict) -> list[dict]:
    run_id = source_run["run_id"]

    # Evitar cambios concurrentes en Silver durante la lectura.
    conn.execute(text("""
        LOCK TABLE silver.silver_acs_2024 IN SHARE MODE
    """))

    
    lineage = conn.execute(
        text("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(run_id) AS rows_with_run_id,
                COUNT(DISTINCT run_id) AS distinct_runs,
                MIN(run_id) AS min_run_id,
                MAX(run_id) AS max_run_id,
                COUNT(*) FILTER (
                    WHERE estimate IS NOT NULL
                      AND BTRIM(estimate) <> ''
                ) AS rows_with_estimate
            FROM silver.silver_acs_2024
            WHERE run_id = :run_id
        """),
        {"run_id": run_id},
    ).mappings().one()

    if (
        lineage["total_rows"] != EXPECTED_SOURCE_ROWS
        or lineage["rows_with_run_id"] != EXPECTED_SOURCE_ROWS
        or lineage["distinct_runs"] != 1
        or lineage["min_run_id"] != run_id
        or lineage["max_run_id"] != run_id
        or lineage["rows_with_estimate"] != EXPECTED_FACT_ROWS
    ):
        raise ValueError(
            f"Validación de Silver incorrecta para run_id={run_id}: "
            f"{dict(lineage)}"
        )

    records = conn.execute(
        text("""
            SELECT
                acs_row_id,
                label_grouping,
                estimate,
                margin_of_error,
                percent,
                percent_margin_of_error,
                run_id
            FROM silver.silver_acs_2024
            WHERE run_id = :run_id
            ORDER BY acs_row_id
        """),
        {"run_id": run_id},
    ).mappings().all()

    if len(records) != EXPECTED_SOURCE_ROWS:
        raise ValueError(
            f"Se leyeron {len(records)} filas; "
            f"se esperaban {EXPECTED_SOURCE_ROWS}."
        )

    if len({row["acs_row_id"] for row in records}) != EXPECTED_SOURCE_ROWS:
        raise ValueError("Existen identificadores ACS duplicados.")

    section_count = sum(
        1
        for row in records
        if row["estimate"] is None or row["estimate"].strip() == ""
    )

    if section_count != EXPECTED_SECTION_ROWS:
        raise ValueError(
            f"Se encontraron {section_count} encabezados; "
            f"se esperaban {EXPECTED_SECTION_ROWS}."
        )

    logger.info(
        "Silver validado | filas=%s | indicadores=%s | secciones=%s",
        len(records),
        EXPECTED_FACT_ROWS,
        section_count,
    )

    return [dict(row) for row in records]



def load_dimensions(conn, records: list[dict], source_run_id: int):

    prepared = []

    for row in records:
        original = row["label_grouping"]

        if original is None:
            raise ValueError(
                f"Etiqueta nula en acs_row_id={row['acs_row_id']}."
            )

        indentation, normalized = indentation_and_label(original)

        if not normalized:
            raise ValueError(
                f"Etiqueta vacía en acs_row_id={row['acs_row_id']}."
            )

        is_section = (
            row["estimate"] is None
            or row["estimate"].strip() == ""
        )

        prepared.append({
            "source_run_id": source_run_id,
            "source_row_id": row["acs_row_id"],
            "label_original": original,
            "label_normalized": normalized,
            "indentation_level": indentation,
            "ordinal_position": row["acs_row_id"],
            "is_section": is_section,
        })

    # No sobrescribir dimensiones históricas
    existing = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.dim_acs_indicator
            WHERE source_run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).scalar_one()

    if existing:
        raise RuntimeError(
            f"Ya existen {existing} indicadores para run_id={source_run_id}. "
            "No se sobrescriben versiones históricas."
        )

    conn.execute(
        text("""
            INSERT INTO gold.dim_acs_indicator (
                source_run_id,
                source_row_id,
                label_original,
                label_normalized,
                indentation_level,
                ordinal_position,
                is_section
            )
            VALUES (
                :source_run_id,
                :source_row_id,
                :label_original,
                :label_normalized,
                :indentation_level,
                :ordinal_position,
                :is_section
            )
        """),
        prepared,
    )

    dimension_rows = conn.execute(
        text("""
            SELECT
                indicator_key,
                source_row_id,
                label_normalized,
                indentation_level,
                is_section
            FROM gold.dim_acs_indicator
            WHERE source_run_id = :run_id
            ORDER BY ordinal_position
        """),
        {"run_id": source_run_id},
    ).mappings().all()

    if len(dimension_rows) != EXPECTED_SOURCE_ROWS:
        raise RuntimeError(
            f"Dimensión ACS incompleta: {len(dimension_rows)} filas; "
            f"se esperaban {EXPECTED_SOURCE_ROWS}."
        )

    dimension_section_count = sum(
        1 for row in dimension_rows if row["is_section"]
    )

    if dimension_section_count != EXPECTED_SECTION_ROWS:
        raise RuntimeError(
            f"La dimensión contiene {dimension_section_count} secciones; "
            f"se esperaban {EXPECTED_SECTION_ROWS}."
        )

    indicator_map = {
        row["source_row_id"]: row["indicator_key"]
        for row in dimension_rows
    }

    if len(indicator_map) != EXPECTED_SOURCE_ROWS:
        raise RuntimeError(
            "El mapa de indicadores ACS contiene duplicados."
        )

    stack = []
    parent_updates = []

    for row in dimension_rows:
        current_indent = row["indentation_level"]

        while stack and stack[-1][0] >= current_indent:
            stack.pop()

        parent_key = stack[-1][1] if stack else None

        if row["is_section"] and current_indent != 0:
            raise ValueError(
                "Una sección ACS debe encontrarse en el nivel raíz: "
                f"source_row_id={row['source_row_id']}."
            )

        # Un indicador puede ser raíz y no tener padre.
        # Se conserva la jerarquía cuando existe un padre.

        parent_updates.append({
            "indicator_key": row["indicator_key"],
            "parent_indicator_key": parent_key,
        })

        stack.append((current_indent, row["indicator_key"]))

    conn.execute(
        text("""
            UPDATE gold.dim_acs_indicator
            SET parent_indicator_key = :parent_indicator_key
            WHERE indicator_key = :indicator_key
        """),
        parent_updates,
    )

    hierarchy_validation = conn.execute(
    text("""
        SELECT
            COUNT(*) AS total_rows,

            COUNT(*) FILTER (
                WHERE child.is_section
            ) AS section_rows,

            COUNT(*) FILTER (
                WHERE child.is_section
                  AND child.parent_indicator_key IS NOT NULL
            ) AS sections_with_parent,

            COUNT(*) FILTER (
                WHERE NOT child.is_section
                  AND child.parent_indicator_key IS NULL
            ) AS root_indicators,

            COUNT(*) FILTER (
                WHERE NOT child.is_section
                  AND child.parent_indicator_key IS NULL
                  AND child.source_row_id <> :expected_root_source_row_id
            ) AS unexpected_root_indicators,

            COUNT(*) FILTER (
                WHERE child.parent_indicator_key IS NOT NULL
                  AND parent.source_run_id <> child.source_run_id
            ) AS cross_run_parents,

            COUNT(*) FILTER (
                WHERE child.parent_indicator_key IS NOT NULL
                  AND parent.indentation_level >= child.indentation_level
            ) AS invalid_parent_depth

        FROM gold.dim_acs_indicator child

        LEFT JOIN gold.dim_acs_indicator parent
            ON parent.indicator_key = child.parent_indicator_key

        WHERE child.source_run_id = :run_id
    """),
    {
        "run_id": source_run_id,
        "expected_root_source_row_id": EXPECTED_ROOT_SOURCE_ROW_ID,
    },
).mappings().one()

    expected_hierarchy = {
        "total_rows": EXPECTED_SOURCE_ROWS,
        "section_rows": EXPECTED_SECTION_ROWS,
        "sections_with_parent": 0,
        "root_indicators": 1,
        "unexpected_root_indicators": 0,
        "cross_run_parents": 0,
        "invalid_parent_depth": 0,
    }

    if any(
        hierarchy_validation[key] != expected
        for key, expected in expected_hierarchy.items()
    ):
        raise RuntimeError(
            "Jerarquía ACS inválida: "
            f"{dict(hierarchy_validation)}"
        )

    logger.info(
        "Dimensión ACS validada | indicadores=%s | secciones=%s",
        hierarchy_validation["total_rows"],
        hierarchy_validation["section_rows"],
    )

    return indicator_map


def load_facts(
    conn,
    records: list[dict],
    source_run_id: int,
    indicator_map: dict,
) -> int:

    source_key = conn.execute(
        text("""
            SELECT source_key
            FROM gold.dim_source
            WHERE source_code = 'ACS'
              AND dataset_name = :dataset_name
        """),
        {"dataset_name": DATASET_NAME},
    ).scalar_one_or_none()

    if source_key is None:
        raise ValueError(
            "No se encontró ACS en gold.dim_source."
        )

    existing = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_acs_indicator
            WHERE source_run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).scalar_one()

    if existing:
        raise RuntimeError(
            f"Ya existen {existing} hechos para run_id={source_run_id}."
        )

    insert_stmt = text("""
        INSERT INTO gold.fact_acs_indicator (
            source_run_id,
            source_key,
            indicator_key,
            estimate_raw,
            estimate_numeric,
            margin_of_error_raw,
            margin_of_error_numeric,
            percent_raw,
            percent_numeric,
            percent_count_numeric,
            percent_value_type,
            percent_margin_of_error_raw,
            percent_margin_of_error_numeric
        )
        VALUES (
            :source_run_id,
            :source_key,
            :indicator_key,
            :estimate_raw,
            :estimate_numeric,
            :margin_of_error_raw,
            :margin_of_error_numeric,
            :percent_raw,
            :percent_numeric,
            :percent_count_numeric,
            :percent_value_type,
            :percent_margin_of_error_raw,
            :percent_margin_of_error_numeric
        )
    """)

    batch = []
    loaded = 0

    for row in records:
        estimate_raw = row["estimate"]

        # Las cinco filas de encabezados se conservan en la dimensión, no generan hechos estadísticos
        if estimate_raw is None or estimate_raw.strip() == "":
            continue

        estimate_numeric = parse_numeric(estimate_raw)
        margin_of_error_numeric = parse_numeric(row["margin_of_error"])
        percent_type, percent_numeric, percent_count = classify_percent(
            row["percent"]
        )
        percent_margin_of_error_numeric = parse_numeric(
            row["percent_margin_of_error"]
        )

        if estimate_numeric is not None and estimate_numeric < 0:
            raise ValueError(
                "Estimación ACS negativa en DP05: "
                f"acs_row_id={row['acs_row_id']}, valor={estimate_raw!r}."
            )

        if margin_of_error_numeric is not None and margin_of_error_numeric < 0:
            raise ValueError(
                "Margen de error ACS negativo: "
                f"acs_row_id={row['acs_row_id']}, "
                f"valor={row['margin_of_error']!r}."
            )

        if (
            percent_margin_of_error_numeric is not None
            and percent_margin_of_error_numeric < 0
        ):
            raise ValueError(
                "Margen de error porcentual ACS negativo: "
                f"acs_row_id={row['acs_row_id']}, "
                f"valor={row['percent_margin_of_error']!r}."
            )

        batch.append({
            "source_run_id": source_run_id,
            "source_key": source_key,
            "indicator_key": indicator_map[row["acs_row_id"]],
            "estimate_raw": estimate_raw,
            "estimate_numeric": estimate_numeric,
            "margin_of_error_raw": row["margin_of_error"],
            "margin_of_error_numeric": margin_of_error_numeric,
            "percent_raw": row["percent"],
            "percent_numeric": percent_numeric,
            "percent_count_numeric": percent_count,
            "percent_value_type": percent_type,
            "percent_margin_of_error_raw": row["percent_margin_of_error"],
            "percent_margin_of_error_numeric": percent_margin_of_error_numeric,
        })

        if len(batch) >= INSERT_BATCH_SIZE:
            conn.execute(insert_stmt, batch)
            loaded += len(batch)
            batch.clear()

    if batch:
        conn.execute(insert_stmt, batch)
        loaded += len(batch)

    persisted = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_acs_indicator
            WHERE source_run_id = :run_id
        """),
        {"run_id": source_run_id},
    ).scalar_one()

    if loaded != EXPECTED_FACT_ROWS or persisted != EXPECTED_FACT_ROWS:
        raise RuntimeError(
            f"Validación fallida: insertados={loaded}, "
            f"persistidos={persisted}, esperados={EXPECTED_FACT_ROWS}."
        )

    return loaded




def validate_orchestrator_lock(engine) -> None:
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
            "Gold del proceso padre. Se cancela para evitar una ejecución "
            "sin exclusión mutua."
        )


@contextmanager
def acquire_acs_execution_lock():
    """Serializa ACS independiente frente al pipeline Gold completo."""
    engine = create_database_engine()
    conn = None
    acquired = False

    try:
        conn = engine.connect()
        database = conn.execute(
            text("SELECT current_database()")
        ).scalar_one()

        if database != DATABASE_ALLOWED:
            raise RuntimeError(
                f"ETL ACS bloqueado: base de datos no autorizada ({database})."
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
                "Existe un pipeline Gold u otra ejecución ACS en curso. "
                "No se iniciará una ejecución concurrente."
            )

        logger.info("Bloqueo compartido de ejecución Gold adquirido por ACS.")
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
                    logger.exception(
                        "No fue posible liberar el bloqueo; "
                        "se invalidará la conexión."
                    )
                    conn.invalidate()
            conn.close()
        engine.dispose()


def run_etl(
    source_run_id: int | None = None,
    orchestrated: bool = False,
):
    engine = create_database_engine()

    try:
        if orchestrated:
            validate_orchestrator_lock(engine)
            logger.info(
                "Bloqueo global del orquestador verificado; "
                "ACS no adquiere un segundo lock."
            )
            lock_context = nullcontext()
        else:
            lock_context = acquire_acs_execution_lock()

        with lock_context, engine.begin() as conn:

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

            indicator_map = load_dimensions(
                conn,
                records,
                source_run_id,
            )

            loaded = load_facts(
                conn,
                records,
                source_run_id,
                indicator_map,
            )

            logger.info(
                "ETL Gold ACS completado | run_id=%s | "
                "indicadores=%s | hechos=%s",
                source_run_id,
                len(indicator_map),
                loaded,
            )

    finally:
        engine.dispose()



def main() -> int:
    parser = argparse.ArgumentParser(
        description="Carga ACS 2024 desde Silver hacia Gold."
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
        logger.exception("Falló el ETL Gold ACS.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())