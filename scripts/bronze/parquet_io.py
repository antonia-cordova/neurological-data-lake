from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = ROOT / "data" / "bronze"
VALID_FREQUENCIES = {"daily", "every_3_days", "every_6_days"}


def selected_frequency() -> str | None:
    value = os.getenv("BRONZE_INGESTION_FREQUENCY")
    if value is None or not value.strip():
        return None
    value = value.strip()
    if value not in VALID_FREQUENCIES:
        raise ValueError(
            f"BRONZE_INGESTION_FREQUENCY inválida: {value!r}. "
            f"Valores: {sorted(VALID_FREQUENCIES)}."
        )
    return value


def dataset_path(dataset: str) -> Path:
    frequency = selected_frequency()
    if frequency is None:
        raise RuntimeError(
            "No se seleccionó una frecuencia Bronze. "
            "Define BRONZE_INGESTION_FREQUENCY para consumir Parquet."
        )
    path = BRONZE_ROOT / frequency / dataset
    if not path.is_dir():
        raise FileNotFoundError(
            f"No existe Bronze Parquet para {dataset}/{frequency}: {path}"
        )
    if not any(path.rglob("*.parquet")):
        raise FileNotFoundError(f"No hay archivos Parquet en {path}.")
    return path



def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_dataset_manifest(dataset: str) -> dict:
    frequency = selected_frequency()
    if frequency is None:
        raise RuntimeError("No se seleccionó frecuencia para validar Bronze.")
    scenario_root = (BRONZE_ROOT / frequency).resolve()
    manifest_path = scenario_root / "metadata" / "bronze_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"No existe el manifiesto Bronze: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("contract_version") != 1 or manifest.get("frequency") != frequency:
        raise ValueError("Versión o frecuencia inválida en el manifiesto Bronze.")

    entry = manifest.get("datasets", {}).get(dataset)
    if not isinstance(entry, dict):
        raise ValueError(f"El manifiesto Bronze no contiene {dataset}.")

    files = entry.get("parquet_files")
    if not isinstance(files, list) or not files:
        raise ValueError(f"El manifiesto de {dataset} no contiene archivos Parquet.")

    declared_paths: set[str] = set()
    total_rows = 0
    expected_schema = None
    for item in files:
        if not isinstance(item, dict):
            raise ValueError(f"Entrada de archivo inválida en manifiesto: {item!r}")
        relative_raw = item.get("path")
        if not isinstance(relative_raw, str) or not relative_raw.strip():
            raise ValueError("El manifiesto contiene una ruta Parquet vacía.")
        relative = Path(relative_raw)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"Ruta relativa insegura en manifiesto: {relative_raw}")
        normalized = relative.as_posix()
        if normalized in declared_paths:
            raise ValueError(f"Archivo Parquet duplicado en manifiesto: {normalized}")
        declared_paths.add(normalized)
        target = (scenario_root / relative).resolve()
        if scenario_root not in target.parents:
            raise ValueError(f"Ruta fuera del escenario Bronze: {relative_raw}")
        if not target.is_file():
            raise FileNotFoundError(f"Falta archivo Parquet declarado: {target}")

        expected_bytes = int(item.get("bytes", -1))
        if target.stat().st_size != expected_bytes:
            raise ValueError(f"Tamaño Parquet no coincide con manifiesto: {target}")

        expected_sha = item.get("sha256")
        if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha):
            raise ValueError(f"SHA-256 ausente o inválido en manifiesto: {target}")
        actual_sha = _sha256_file(target)
        if actual_sha.lower() != expected_sha.lower():
            raise ValueError(f"SHA-256 Parquet no coincide con manifiesto: {target}")

        parquet_file = pq.ParquetFile(target)
        physical_rows = parquet_file.metadata.num_rows
        expected_rows = int(item.get("rows", -1))
        if physical_rows != expected_rows:
            raise ValueError(
                f"Filas Parquet no coinciden con manifiesto en {target}: "
                f"archivo={physical_rows}, manifiesto={expected_rows}."
            )
        schema = parquet_file.schema_arrow
        if expected_schema is None:
            expected_schema = schema
        elif not schema.equals(expected_schema, check_metadata=False):
            raise ValueError(f"Esquema Parquet inconsistente: {target}")
        total_rows += physical_rows

    dataset_dir = (scenario_root / dataset).resolve()
    actual_paths = {
        path.relative_to(scenario_root).as_posix()
        for path in dataset_dir.rglob("*.parquet")
    }
    if actual_paths != declared_paths:
        missing = sorted(declared_paths - actual_paths)
        undeclared = sorted(actual_paths - declared_paths)
        raise ValueError(
            f"Inventario Parquet no coincide para {dataset}. "
            f"Faltantes={missing}; no declarados={undeclared}."
        )

    declared_parquet_rows = int(entry.get("parquet_rows", -1))
    declared_source_rows = int(entry.get("source_rows", -1))
    if total_rows != declared_parquet_rows or total_rows != declared_source_rows:
        raise ValueError(
            f"Conteo de manifiesto inconsistente para {dataset}: "
            f"archivos={total_rows}, parquet={declared_parquet_rows}, "
            f"fuente={declared_source_rows}."
        )
    return entry

def parquet_dataset(path: Path) -> ds.Dataset:
    if not path.is_dir():
        raise FileNotFoundError(f"No existe el directorio Parquet: {path}")
    return ds.dataset(path, format="parquet")


def read_parquet_frame(
    path: Path,
    columns: list[str] | None = None,
) -> pa.Table:
    dataset = parquet_dataset(path)
    available = set(dataset.schema.names)
    if columns is not None:
        missing = set(columns) - available
        if missing:
            raise ValueError(
                f"Faltan columnas en Bronze Parquet {path}: {sorted(missing)}"
            )
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No hay archivos Parquet en {path}.")
    tables = [pq.read_table(file, columns=columns) for file in files]
    return pa.concat_tables(tables)


def iter_parquet_frames(
    path: Path,
    batch_size: int,
    columns: list[str] | None = None,
) -> Iterator:
    dataset = parquet_dataset(path)
    available = set(dataset.schema.names)
    if columns is not None:
        missing = set(columns) - available
        if missing:
            raise ValueError(
                f"Faltan columnas en Bronze Parquet {path}: {sorted(missing)}"
            )
    # Orden lexicográfico = orden de fecha y número de parte. Bronze asigna
    # secuencias monótonas a esos directorios, por lo que se preserva el orden fuente.
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No hay archivos Parquet en {path}.")
    pending_batches = []
    pending_rows = 0
    for file in files:
        parquet_file = pq.ParquetFile(file)
        for batch in parquet_file.iter_batches(
            batch_size=batch_size,
            columns=columns,
            use_threads=True,
        ):
            pending_batches.append(batch)
            pending_rows += batch.num_rows
            if pending_rows >= batch_size:
                yield pa.Table.from_batches(pending_batches).to_pandas()
                pending_batches = []
                pending_rows = 0

    if pending_batches:
        yield pa.Table.from_batches(pending_batches).to_pandas()
