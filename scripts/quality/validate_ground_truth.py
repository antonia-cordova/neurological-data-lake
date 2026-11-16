
from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from dotenv import load_dotenv
from scripts.database_config import database_for_scenario

ROOT = Path(__file__).resolve().parents[2]
GT_DIR = ROOT / "quality" / "ground_truth"
REPORT_DIR = ROOT / "quality" / "reports"


REQUIRED = {
    "database", "level", "schema", "table", "primary_key",
    "column", "original_value", "altered_value", "rule",
    "dimension", "seed", "changed_at_utc",
}


def connect(database: str):
    load_dotenv(ROOT / ".env")

    names = (
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
    )

    missing = [name for name in names if not os.getenv(name)]

    if missing:
        raise RuntimeError(
            "Faltan variables en .env: " + ", ".join(missing)
        )

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=database,
        connect_timeout=10,
    )


def values_equal(left: Any, right: Any) -> bool:
    """
    Compara valores considerando equivalentes las representaciones
    numéricas como 57.6 y 57.60.

    No utiliza tolerancias ni redondeos.
    """

    if left is None or right is None:
        return left is right

    try:
        left_decimal = Decimal(str(left))
        right_decimal = Decimal(str(right))

        if left_decimal.is_finite() and right_decimal.is_finite():
            return left_decimal == right_decimal

    except (InvalidOperation, ValueError, TypeError):
        pass

    return str(left) == str(right)


def norm(value: Any) -> str | None:
    return None if value is None else str(value)


def load_gt(level: str, test_mode: bool = False) -> list[dict[str, Any]]:
    suffix = "_test" if test_mode else ""
    target_db = database_for_scenario(level, test_mode)
    path = GT_DIR / f"ground_truth_{level}{suffix}.jsonl"

    records = []

    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):

            if not line.strip():
                continue

            record = json.loads(line)

            missing = REQUIRED - record.keys()

            if missing:
                raise ValueError(
                    f"{path.name}, línea {line_no}: "
                    f"faltan {sorted(missing)}"
                )

            if (
                record["level"] != level
                or record["database"] != target_db
            ):
                raise ValueError(
                    f"{path.name}, línea {line_no}: "
                    "nivel/base no corresponden"
                )

            if not isinstance(record["primary_key"], str):
                raise ValueError(
                    f"{path.name}, línea {line_no}: "
                    "primary_key no es texto JSON"
                )

            record["_pk"] = json.loads(record["primary_key"])

            if not isinstance(record["_pk"], dict) or not record["_pk"]:
                raise ValueError(
                    f"{path.name}, línea {line_no}: "
                    "primary_key inválida"
                )

            record["_line"] = line_no

            records.append(record)

    return records


def table_metadata(conn, schema: str, table: str):

    columns = {
        row[0]
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        ).fetchall()
    }

    pk = [
        row[0]
        for row in conn.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_class t
                ON t.oid = i.indrelid
            JOIN pg_namespace n
                ON n.oid = t.relnamespace
            JOIN LATERAL unnest(i.indkey)
                WITH ORDINALITY k(attnum, ord)
                ON true
            JOIN pg_attribute a
                ON a.attrelid = t.oid
               AND a.attnum = k.attnum
            WHERE i.indisprimary
              AND n.nspname = %s
              AND t.relname = %s
            ORDER BY k.ord
            """,
            (schema, table),
        ).fetchall()
    ]

    return columns, pk


def read_cell(
    conn,
    schema: str,
    table: str,
    column: str,
    pk: dict[str, Any],
):

    predicates = [
        sql.SQL("{} IS NOT DISTINCT FROM %s").format(
            sql.Identifier(key)
        )
        for key in pk
    ]

    query = sql.SQL(
        "SELECT {} FROM {} WHERE {}"
    ).format(
        sql.Identifier(column),
        sql.Identifier(schema, table),
        sql.SQL(" AND ").join(predicates),
    )

    rows = conn.execute(
        query,
        list(pk.values()),
    ).fetchall()

    if not rows:
        return "ROW_NOT_FOUND", None

    if len(rows) != 1:
        return "MULTIPLE_ROWS", None

    return "FOUND", rows[0][0]


def audit(level: str, records: list[dict[str, Any]], test_mode: bool = False):

    source_db = database_for_scenario("perfect", test_mode)
    target_db = database_for_scenario(level, test_mode)

    results = []
    first_seen = {}
    duplicate_lines = set()

    for record in records:

        cell = (
            record["schema"],
            record["table"],
            tuple(sorted(record["_pk"].items())),
            record["column"],
        )

        if cell in first_seen:
            duplicate_lines.add(record["_line"])
        else:
            first_seen[cell] = record["_line"]

    with connect(source_db) as source, connect(target_db) as target:

        cache = {}

        for record in records:

            schema = record["schema"]
            table = record["table"]
            column = record["column"]
            pk = record["_pk"]
            line = record["_line"]

            status = "MATCH"
            detail = ""

            key = (schema, table)

            if line in duplicate_lines:

                status = "DUPLICATE_LOG"
                detail = "Celda repetida en Ground Truth."

            elif record["dimension"] not in {
                "accuracy",
                "completeness",
                "semantic_consistency",
            }:

                status = "INVALID_DIMENSION"
                detail = "Dimensión desconocida."

            else:

                if key not in cache:
                    cache[key] = table_metadata(
                        target,
                        schema,
                        table,
                    )

                columns, actual_pk = cache[key]

                if not columns:

                    status = "TABLE_NOT_FOUND"
                    detail = f"No existe {schema}.{table}."

                elif column not in columns:

                    status = "COLUMN_NOT_FOUND"
                    detail = f"No existe {column}."

                elif set(pk) != set(actual_pk):

                    status = "PRIMARY_KEY_MISMATCH"
                    detail = (
                        f"PK del log={list(pk)}; "
                        f"PK real={actual_pk}."
                    )

                else:

                    src_status, src_value = read_cell(
                        source,
                        schema,
                        table,
                        column,
                        pk,
                    )

                    dst_status, dst_value = read_cell(
                        target,
                        schema,
                        table,
                        column,
                        pk,
                    )

                    if src_status != "FOUND":

                        status = src_status
                        detail = "Fila no encontrada en Perfect."

                    elif dst_status != "FOUND":

                        status = dst_status
                        detail = (
                            f"Fila no encontrada en {target_db}."
                        )

                    elif not values_equal(
                        src_value,
                        record["original_value"],
                    ):

                        status = "ORIGINAL_MISMATCH"
                        detail = (
                            f"Perfect={norm(src_value)!r}; "
                            f"GT original="
                            f"{record['original_value']!r}."
                        )

                    elif record["dimension"] == "completeness":

                        if record["altered_value"] is not None:

                            status = "INVALID_COMPLETENESS_LOG"
                            detail = (
                                "Completeness debe registrar "
                                "altered_value null."
                            )

                        elif src_value is None:

                            status = "INVALID_COMPLETENESS_SOURCE"
                            detail = (
                                "El valor original en Perfect "
                                "ya era NULL."
                            )

                        elif dst_value is not None:

                            status = "ALTERED_MISMATCH"
                            detail = (
                                "Se esperaba NULL; "
                                f"se encontró {norm(dst_value)!r}."
                            )

                    elif record["altered_value"] is None:

                        status = "INVALID_ALTERED_VALUE"
                        detail = (
                            "altered_value null fuera "
                            "de completeness."
                        )

                    elif not values_equal(
                        dst_value,
                        record["altered_value"],
                    ):

                        status = "ALTERED_MISMATCH"
                        detail = (
                            f"{target_db}={norm(dst_value)!r}; "
                            f"GT alterado="
                            f"{record['altered_value']!r}."
                        )

            results.append({
                "level": level,
                "ground_truth_line": line,
                "database": target_db,
                "schema": schema,
                "table": table,
                "primary_key": record["primary_key"],
                "column": column,
                "dimension": record["dimension"],
                "rule": record["rule"],
                "status": status,
                "detail": detail,
            })

    return results


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Valida Ground Truth contra Perfect "
            "y las bases degradadas del mismo entorno."
        )
    )

    parser.add_argument(
        "--level",
        choices=("medium", "poor", "all"),
        default="all",
    )

    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Audita las bases *_test y el Ground Truth aislado de pruebas.",
    )
    args = parser.parse_args()

    levels = (
        ("medium", "poor")
        if args.level == "all"
        else (args.level,)
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []

    for level in levels:

        records = load_gt(level, args.test_mode)
        results = audit(level, records, args.test_mode)

        all_results.extend(results)

        counts = Counter(
            row["status"]
            for row in results
        )

        print(f"\n========== {level.upper()} ==========")
        print(f"Registros Ground Truth: {len(records):,}")

        for status, count in sorted(counts.items()):
            print(f"{status:30} {count:>8,}")

    report = REPORT_DIR / ("ground_truth_validation_test.csv" if args.test_mode else "ground_truth_validation.csv")

    fields = [
        "level",
        "ground_truth_line",
        "database",
        "schema",
        "table",
        "primary_key",
        "column",
        "dimension",
        "rule",
        "status",
        "detail",
    ]

    with report.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )

        writer.writeheader()
        writer.writerows(all_results)

    matches = sum(
        row["status"] == "MATCH"
        for row in all_results
    )

    print(f"\nTOTAL AUDITADO: {len(all_results):,}")
    print(f"MATCH: {matches:,}")
    print(
        f"NO MATCH: {len(all_results) - matches:,}"
    )
    print(f"INFORME: {report.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
