from __future__ import annotations
import re
from sqlalchemy import text

def resolve_single_silver_run_id(
    conn,
    table_name: str,
    requested_run_id: int | None = None,
    expected_rows: int | None = None,
    dataset_name: str | None = None,
    target_table: str | None = None,
) -> int:
    """
    Selecciona y valida una ejecución Silver.

    Si se proporciona requested_run_id, valida esa ejecución.
    Si se omite, selecciona la última ejecución exitosa del dataset.
    """

    if not re.fullmatch(
        r"(?:public\.)?[a-zA-Z_][a-zA-Z0-9_]*",
        table_name,
    ):
        raise ValueError(f"Nombre de tabla no permitido: {table_name!r}")

    qualified_table = (
        table_name
        if "." in table_name
        else f"public.{table_name}"
    )

    expected_table = target_table or qualified_table.split(".")[-1]
    expected_dataset = dataset_name

    conn.execute(
        text(f"LOCK TABLE {qualified_table} IN SHARE MODE")
    )

    if requested_run_id is None:
        if not expected_dataset:
            raise ValueError(
                "dataset_name es obligatorio para seleccionar "
                "automáticamente una ejecución."
            )

        run = conn.execute(
            text("""
                SELECT
                    run_id,
                    dataset_name,
                    target_table,
                    loaded_rows
                FROM public.etl_runs
                WHERE dataset_name = :dataset_name
                  AND target_table = :target_table
                  AND status = 'succeeded'
                  AND finished_at IS NOT NULL
                ORDER BY finished_at DESC, run_id DESC
                LIMIT 1
            """),
            {
                "dataset_name": expected_dataset,
                "target_table": expected_table,
            },
        ).mappings().one_or_none()

        if run is None:
            raise ValueError(
                f"No existe una ejecución exitosa para "
                f"{expected_dataset} en {expected_table}."
            )

        requested_run_id = int(run["run_id"])

    else:
        run = conn.execute(
            text("""
                SELECT
                    run_id,
                    dataset_name,
                    target_table,
                    loaded_rows,
                    status,
                    finished_at
                FROM public.etl_runs
                WHERE run_id = :run_id
            """),
            {"run_id": requested_run_id},
        ).mappings().one_or_none()

        if run is None:
            raise ValueError(
                f"No existe etl_runs.run_id={requested_run_id}."
            )

        if (
            run["status"] != "succeeded"
            or run["finished_at"] is None
        ):
            raise ValueError(
                f"La ejecución run_id={requested_run_id} "
                f"no finalizó correctamente."
            )

        if (
            expected_dataset
            and run["dataset_name"] != expected_dataset
        ):
            raise ValueError(
                f"Dataset incorrecto para run_id={requested_run_id}: "
                f"{run['dataset_name']}."
            )

        if run["target_table"] != expected_table:
            raise ValueError(
                f"Tabla de destino incorrecta para "
                f"run_id={requested_run_id}: {run['target_table']}."
            )

    lineage = conn.execute(
        text(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(run_id) AS rows_with_run_id,
                COUNT(DISTINCT run_id) AS distinct_runs
            FROM {qualified_table}
            WHERE run_id = :run_id
        """),
        {"run_id": requested_run_id},
    ).mappings().one()

    total_rows = int(lineage["total_rows"])

    if total_rows == 0:
        raise ValueError(
            f"{qualified_table} no contiene registros "
            f"para run_id={requested_run_id}."
        )

    if (
        lineage["rows_with_run_id"] != total_rows
        or lineage["distinct_runs"] != 1
    ):
        raise ValueError(
            f"Trazabilidad inconsistente en {qualified_table}: "
            f"{dict(lineage)}"
        )

    if expected_rows is not None and total_rows != expected_rows:
        raise ValueError(
            f"{qualified_table}: filas={total_rows}; "
            f"esperadas={expected_rows} para run_id={requested_run_id}."
        )

    if total_rows != int(run["loaded_rows"]):
        raise ValueError(
            f"Registros Silver ({total_rows}) no coinciden con "
            f"etl_runs.loaded_rows ({run['loaded_rows']}) "
            f"para run_id={requested_run_id}."
        )

    return int(requested_run_id)