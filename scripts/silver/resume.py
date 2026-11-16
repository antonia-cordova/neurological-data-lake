from __future__ import annotations


def classify_audited_stage(
    latest_run,
    actual_counts: tuple[int, ...],
    expected_rows: int,
    stage_name: str,
) -> str:
    """Clasifica una etapa auditada sin reconstruir estados parciales."""
    if latest_run is None:
        if all(count == 0 for count in actual_counts):
            return "pending"
        raise RuntimeError(
            f"{stage_name} contiene datos, pero no existe una ejecución ETL auditable."
        )
    if (
        latest_run[1] == "succeeded"
        and latest_run[3] is not None
        and int(latest_run[2]) == expected_rows
        and all(count == expected_rows for count in actual_counts)
    ):
        return "complete"
    raise RuntimeError(
        f"{stage_name} está incompleta o inconsistente: última ejecución={latest_run}, "
        f"conteos={actual_counts}, esperado={expected_rows}."
    )
