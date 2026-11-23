
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



def caesar_encrypt(value: str, shift: int = 3) -> str:
    """Replica exacta de la transformación César del Quality Generator."""
    if value is None:
        raise TypeError("El cifrado César no admite valores NULL.")
    if not isinstance(value, str):
        raise TypeError("El cifrado César solo admite valores de texto.")

    offset = shift % 26
    result = []

    for char in value:
        if "A" <= char <= "Z":
            result.append(chr((ord(char) - ord("A") + offset) % 26 + ord("A")))
        elif "a" <= char <= "z":
            result.append(chr((ord(char) - ord("a") + offset) % 26 + ord("a")))
        else:
            result.append(char)

    return "".join(result)


PAIR_SWAP_RULE = "fk_valid_semantic_measure_pair_swap"
CAESAR_RULE = "deterministic_caesar_cipher"
VALID_DIMENSIONS = {
    "accuracy",
    "completeness",
    "semantic_consistency",
    "encryption",
}


def pk_identity(schema: str, table: str, pk: dict[str, Any]) -> tuple:
    """Identidad estable de una PK, independiente del orden de sus claves."""
    return (
        schema,
        table,
        json.dumps(pk, sort_keys=True, ensure_ascii=False, default=str),
    )


def build_pair_swap_map(records: list[dict[str, Any]]) -> dict[tuple, dict[str, Any]]:
    """
    Construye el mapa PK original -> PK final generado por PAIR-SWAP.

    Quality Generator registra cada lado del intercambio como un cambio
    independiente. La traducción se aplica a todos los cambios de esa fila,
    no solo al registro que modifica health_measure_key.
    """
    pair_map: dict[tuple, dict[str, Any]] = {}
    destinations: dict[tuple, tuple] = {}
    pair_records = []

    for record in records:
        if record["rule"] != PAIR_SWAP_RULE:
            continue

        schema = record["schema"]
        table = record["table"]
        column = record["column"]
        original_pk = record["_pk"]

        if (
            schema != "gold"
            or table != "fact_area_health"
            or column != "health_measure_key"
        ):
            raise ValueError(
                "PAIR-SWAP fuera de gold.fact_area_health.health_measure_key "
                f"(línea {record['_line']})."
            )

        if "health_measure_key" not in original_pk:
            raise ValueError(
                f"PAIR-SWAP sin health_measure_key en PK (línea {record['_line']})."
            )

        try:
            original_key = int(original_pk["health_measure_key"])
            altered_key = int(record["altered_value"])
            logged_original = int(record["original_value"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Claves PAIR-SWAP no numéricas (línea {record['_line']})."
            ) from exc

        if original_key != logged_original:
            raise ValueError(
                f"El original_value no coincide con la PK (línea {record['_line']})."
            )

        if original_key == altered_key:
            raise ValueError(
                f"PAIR-SWAP no modifica la clave (línea {record['_line']})."
            )

        final_pk = dict(original_pk)
        final_pk["health_measure_key"] = altered_key
        identity = pk_identity(schema, table, original_pk)
        destination = pk_identity(schema, table, final_pk)

        if identity in pair_map and pair_map[identity] != final_pk:
            raise ValueError(
                f"PK original con destinos contradictorios (línea {record['_line']})."
            )

        if destination in destinations and destinations[destination] != identity:
            raise ValueError(
                f"Dos filas PAIR-SWAP apuntan a la misma PK final "
                f"(línea {record['_line']})."
            )

        pair_map[identity] = final_pk
        destinations[destination] = identity
        pair_records.append((record, original_key, altered_key, original_pk))

    # Cada asociación debe tener su contraparte recíproca.
    by_group_and_direction = {}
    for record, original_key, altered_key, pk in pair_records:
        group = (
            pk.get("source_run_id"),
            pk.get("geography_key"),
            pk.get("prevalence_type_key"),
        )
        by_group_and_direction[(group, original_key, altered_key)] = record

    for record, original_key, altered_key, pk in pair_records:
        group = (
            pk.get("source_run_id"),
            pk.get("geography_key"),
            pk.get("prevalence_type_key"),
        )
        if (group, altered_key, original_key) not in by_group_and_direction:
            raise ValueError(
                "PAIR-SWAP sin contraparte recíproca "
                f"(línea {record['_line']})."
            )

    return pair_map


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

    # Igual que recover_ground_truth(), se construye el mapa antes de auditar
    # cualquier registro. Así todas las columnas de una fila movida usan su PK
    # original en Perfect y su PK final en la base degradada.
    pair_key_map = build_pair_swap_map(records)

    with connect(source_db) as source, connect(target_db) as target:
        cache = {}

        for record in records:
            schema = record["schema"]
            table = record["table"]
            column = record["column"]
            original_pk = record["_pk"]
            line = record["_line"]
            dimension = record["dimension"]
            rule = record["rule"]

            status = "MATCH"
            detail = ""
            key = (schema, table)

            final_pk = pair_key_map.get(
                pk_identity(schema, table, original_pk),
                original_pk,
            )

            if line in duplicate_lines:
                status = "DUPLICATE_LOG"
                detail = "Celda repetida en Ground Truth."

            elif dimension not in VALID_DIMENSIONS:
                status = "INVALID_DIMENSION"
                detail = f"Dimensión desconocida: {dimension!r}."

            elif dimension == "encryption" and rule != CAESAR_RULE:
                status = "INVALID_ENCRYPTION_RULE"
                detail = (
                    "La dimensión encryption solo se admite para "
                    f"{CAESAR_RULE!r}."
                )

            elif dimension != "encryption" and rule == CAESAR_RULE:
                status = "INVALID_ENCRYPTION_DIMENSION"
                detail = "La regla César debe registrar dimension='encryption'."

            else:
                if key not in cache:
                    cache[key] = table_metadata(target, schema, table)

                columns, actual_pk = cache[key]

                if not columns:
                    status = "TABLE_NOT_FOUND"
                    detail = f"No existe {schema}.{table}."

                elif column not in columns:
                    status = "COLUMN_NOT_FOUND"
                    detail = f"No existe {column}."

                elif set(original_pk) != set(actual_pk):
                    status = "PRIMARY_KEY_MISMATCH"
                    detail = (
                        f"PK del log={list(original_pk)}; "
                        f"PK real={actual_pk}."
                    )

                else:
                    src_status, src_value = read_cell(
                        source, schema, table, column, original_pk
                    )
                    dst_status, dst_value = read_cell(
                        target, schema, table, column, final_pk
                    )

                    if src_status != "FOUND":
                        status = src_status
                        detail = "Fila original no encontrada en Perfect."

                    elif dst_status != "FOUND":
                        status = dst_status
                        detail = (
                            "Fila no encontrada en la base degradada usando "
                            f"la PK final {json.dumps(final_pk, ensure_ascii=False, default=str)}."
                        )

                    elif not values_equal(src_value, record["original_value"]):
                        status = "ORIGINAL_MISMATCH"
                        detail = (
                            f"Perfect={norm(src_value)!r}; "
                            f"GT original={record['original_value']!r}."
                        )

                    elif dimension == "completeness":
                        if record["altered_value"] is not None:
                            status = "INVALID_COMPLETENESS_LOG"
                            detail = "Completeness debe registrar altered_value null."
                        elif src_value is None:
                            status = "INVALID_COMPLETENESS_SOURCE"
                            detail = "El valor original en Perfect ya era NULL."
                        elif dst_value is not None:
                            status = "ALTERED_MISMATCH"
                            detail = (
                                "Se esperaba NULL; "
                                f"se encontró {norm(dst_value)!r}."
                            )

                    elif dimension == "encryption":
                        if src_value is None:
                            status = "INVALID_ENCRYPTION_SOURCE"
                            detail = "No se puede cifrar un valor original NULL."
                        elif not isinstance(src_value, str):
                            status = "INVALID_ENCRYPTION_SOURCE"
                            detail = "La regla César requiere un valor de texto."
                        else:
                            expected_cipher = caesar_encrypt(src_value, 3)

                            if not values_equal(
                                expected_cipher,
                                record["altered_value"],
                            ):
                                status = "INVALID_ENCRYPTION_TRANSFORM"
                                detail = (
                                    f"César(+3) esperado={expected_cipher!r}; "
                                    f"GT alterado={record['altered_value']!r}."
                                )
                            elif not values_equal(dst_value, expected_cipher):
                                status = "ALTERED_MISMATCH"
                                detail = (
                                    f"{target_db}={norm(dst_value)!r}; "
                                    f"César(+3) esperado={expected_cipher!r}."
                                )

                    elif record["altered_value"] is None:
                        status = "INVALID_ALTERED_VALUE"
                        detail = "altered_value null fuera de completeness."

                    elif not values_equal(dst_value, record["altered_value"]):
                        status = "ALTERED_MISMATCH"
                        detail = (
                            f"{target_db}={norm(dst_value)!r}; "
                            f"GT alterado={record['altered_value']!r}."
                        )

            results.append({
                "level": level,
                "ground_truth_line": line,
                "database": target_db,
                "schema": schema,
                "table": table,
                "primary_key": record["primary_key"],
                "final_primary_key": json.dumps(
                    final_pk, ensure_ascii=False, default=str
                ),
                "column": column,
                "dimension": dimension,
                "rule": rule,
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
        "final_primary_key",
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
