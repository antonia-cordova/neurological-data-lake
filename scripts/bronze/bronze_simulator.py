from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import uuid
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyreadstat

ROOT = Path(__file__).resolve().parents[2]
LANDING = ROOT / "data" / "landing"
BRONZE = ROOT / "data" / "bronze"

CALENDAR_START = "2023-01-01"
CALENDAR_END = "2024-12-31"
FREQUENCIES = {
    "daily": 1,
    "every_3_days": 3,
    "every_6_days": 6,
}
CSV_CHUNK_SIZE = 50_000
BRFSS_CHUNK_SIZE = 10_000

DATASETS = {
    "acs_2024": {
        "source_dir": "acs_2024",
        "source_pattern": "ACSDP5Y2024.DP05*.csv",
        "format": "csv",
        "expected_rows": 113,
        "chunk_size": 10_000,
    },
    "brfss_2023": {
        "source_dir": "brfss_2023",
        "source_pattern": "LLCP2023.XPT",
        "format": "xpt",
        "expected_rows": 433_323,
        "chunk_size": BRFSS_CHUNK_SIZE,
    },
    "places_2025": {
        "source_dir": "places_2025",
        "source_pattern": "*.csv",
        "format": "csv",
        "expected_rows": None,
        "chunk_size": CSV_CHUNK_SIZE,
    },
    "acs_2024_county_dp05": {
        "source_dir": "acs_2024_county",
        "source_pattern": "acs_dp05_county_run_*Z.json",
        "format": "json",
        "expected_rows": 3_222,
        "chunk_size": 10_000,
    },
}

METADATA_COLUMNS = [
    "source_record_id",
    "source_dataset",
    "source_file",
    "simulated_received_date",
    "ingestion_sequence",
    "ingestion_batch_id",
    "ingestion_scenario",
    "is_simulated_ingestion",
    "source_row_hash",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("bronze_simulator")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def landing_source_digest(path: Path) -> str:
    manifest_path = LANDING / "metadata" / "landing_manifest.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        relative = str(path.relative_to(ROOT)).replace("\\", "/")
        for item in manifest.get("files", []):
            if item.get("file") == relative:
                if int(item.get("bytes", -1)) != path.stat().st_size:
                    raise ValueError(f"Tamaño de fuente Landing no coincide: {relative}")
                digest = item.get("sha256")
                if digest:
                    return str(digest)
    return sha256_file(path)


def locate_source(dataset: str, config: dict) -> Path:
    folder = LANDING / config["source_dir"]
    matches = sorted(folder.glob(config["source_pattern"]))
    if len(matches) != 1:
        raise RuntimeError(
            f"{dataset}: se esperaba un archivo {config['source_pattern']} "
            f"en {folder}; encontrados={len(matches)}."
        )
    return matches[0]


def validate_county_landing_manifest(source: Path) -> None:
    manifest_path = source.with_name(f"{source.stem}.manifest.json")
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"ACS County requiere el manifiesto original de Landing: {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("raw_file") != source.name:
        raise ValueError("El manifiesto ACS County no corresponde al JSON de Landing.")
    if int(manifest.get("raw_file_bytes", -1)) != source.stat().st_size:
        raise ValueError("El tamaño del JSON ACS County no coincide con su manifiesto.")
    if manifest.get("sha256") != landing_source_digest(source):
        raise ValueError("El SHA-256 del JSON ACS County no coincide con su manifiesto.")


def count_csv_rows(path: Path, chunk_size: int) -> int:
    total = 0
    for frame in pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        chunksize=chunk_size,
    ):
        total += len(frame)
    return total


def count_xpt_rows(path: Path) -> int:
    total = 0

    def reader(file_path, **kwargs):
        return pyreadstat.read_xport(
            file_path, encoding="windows-1252", **kwargs
        )

    for frame, _metadata in pyreadstat.read_file_in_chunks(
        reader,
        str(path),
        chunksize=BRFSS_CHUNK_SIZE,
        output_format="pandas",
    ):
        total += len(frame)
    return total


def source_chunks(path: Path, config: dict) -> Iterable[pd.DataFrame]:
    kind = config["format"]
    if kind == "csv":
        yield from pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
            chunksize=config["chunk_size"],
        )
        return

    if kind == "xpt":
        def reader(file_path, **kwargs):
            return pyreadstat.read_xport(
                file_path, encoding="windows-1252", **kwargs
            )

        for frame, _metadata in pyreadstat.read_file_in_chunks(
            reader,
            str(path),
            chunksize=config["chunk_size"],
            output_format="pandas",
        ):
            yield frame
        return

    if kind == "json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        headers = payload.get("headers")
        rows = payload.get("data")
        if not isinstance(headers, list) or not isinstance(rows, list):
            raise ValueError(f"Estructura JSON ACS County inválida: {path}")
        frame = pd.DataFrame(rows, columns=headers)
        yield frame
        return

    raise ValueError(f"Formato de fuente no soportado: {kind}")


def count_source_rows(path: Path, config: dict) -> int:
    kind = config["format"]
    if kind == "csv":
        return count_csv_rows(path, config["chunk_size"])
    if kind == "xpt":
        return count_xpt_rows(path)
    if kind == "json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise ValueError(f"El JSON ACS County no contiene data[]: {path}")
        return len(rows)
    raise ValueError(f"Formato no soportado: {kind}")


def date_calendar(frequency: str) -> pd.DatetimeIndex:
    step = FREQUENCIES[frequency]
    dates = pd.date_range(CALENDAR_START, CALENDAR_END, freq=f"{step}D")
    if len(dates) == 0:
        raise RuntimeError("El calendario simulado está vacío.")
    return dates


def stable_row_hash(frame: pd.DataFrame) -> pd.Series:
    # Hash determinista de 64 bits sobre los valores originales y su orden de columnas.
    values = pd.util.hash_pandas_object(frame, index=False, categorize=True)
    return values.map(lambda value: f"{int(value):016x}")


def add_ingestion_metadata(
    frame: pd.DataFrame,
    dataset: str,
    source_file: str,
    frequency: str,
    start_sequence: int,
    total_rows: int,
    dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    original = frame.copy()
    count = len(original)
    sequence = np.arange(
        start_sequence + 1,
        start_sequence + count + 1,
        dtype=np.int64,
    )
    bucket = np.minimum(
        ((sequence - 1) * len(dates)) // total_rows,
        len(dates) - 1,
    )
    received_dates = dates.take(bucket).strftime("%Y-%m-%d")

    metadata = pd.DataFrame({
        "source_record_id": [
            f"{dataset}:{number:012d}" for number in sequence
        ],
        "source_dataset": dataset,
        "source_file": source_file,
        "simulated_received_date": received_dates,
        "ingestion_sequence": sequence,
        "ingestion_batch_id": [
            f"{dataset}:{frequency}:{date}" for date in received_dates
        ],
        "ingestion_scenario": frequency,
        "is_simulated_ingestion": True,
        "source_row_hash": stable_row_hash(original).to_numpy(),
    }, index=original.index)

    collisions = set(original.columns).intersection(METADATA_COLUMNS)
    if collisions:
        raise ValueError(
            f"Las columnas fuente colisionan con metadatos de ingesta: {sorted(collisions)}"
        )
    return pd.concat([original, metadata], axis=1)


def write_parquet_parts(
    frame: pd.DataFrame,
    output_dir: Path,
    dataset: str,
    frequency: str,
    source_file: str,
    start_sequence: int,
    total_rows: int,
    dates: pd.DatetimeIndex,
    part_numbers: dict[str, int],
) -> list[dict]:
    enriched = add_ingestion_metadata(
        frame=frame,
        dataset=dataset,
        source_file=source_file,
        frequency=frequency,
        start_sequence=start_sequence,
        total_rows=total_rows,
        dates=dates,
    )

    outputs = []
    date_values = enriched["simulated_received_date"]
    for date_value in date_values.drop_duplicates().tolist():
        part = enriched.loc[date_values.eq(date_value)].copy()
        part_dir = output_dir / dataset / date_value
        part_dir.mkdir(parents=True, exist_ok=True)
        part_number = part_numbers.get(date_value, 0)
        part_numbers[date_value] = part_number + 1
        target = part_dir / f"part-{part_number:05d}.parquet"
        part.to_parquet(
            target,
            index=False,
            engine="pyarrow",
            compression="snappy",
        )
        outputs.append({
            "path": str(target.relative_to(output_dir)).replace("\\", "/"),
            "rows": len(part),
            "bytes": target.stat().st_size,
            "sha256": sha256_file(target),
            "simulated_received_date": date_value,
        })
    return outputs


def simulate_dataset(
    dataset: str,
    config: dict,
    frequency: str,
    dates: pd.DatetimeIndex,
    staging_root: Path,
) -> dict:
    source = locate_source(dataset, config)
    if dataset == "acs_2024_county_dp05":
        validate_county_landing_manifest(source)
    expected = config["expected_rows"]
    # BRFSS tiene un conteo contractual conocido; se evita una pasada adicional
    # completa sobre el XPT de 1,2 GB. El total real se valida al terminar la lectura.
    source_rows = (
        int(expected)
        if config["format"] == "xpt" and expected is not None
        else count_source_rows(source, config)
    )
    if expected is not None and config["format"] != "xpt" and source_rows != expected:
        raise ValueError(
            f"{dataset}: filas de origen={source_rows}; se esperaban {expected}."
        )
    if source_rows <= 0:
        raise ValueError(f"{dataset}: la fuente no contiene registros.")

    source_digest = landing_source_digest(source)
    part_numbers: dict[str, int] = {}
    files = []
    processed = 0

    for frame in source_chunks(source, config):
        if frame.empty:
            continue
        files.extend(
            write_parquet_parts(
                frame=frame,
                output_dir=staging_root,
                dataset=dataset,
                frequency=frequency,
                source_file=source.name,
                start_sequence=processed,
                total_rows=source_rows,
                dates=dates,
                part_numbers=part_numbers,
            )
        )
        processed += len(frame)
        logger.info(
            "%s | procesados=%s/%s",
            dataset,
            processed,
            source_rows,
        )

    if processed != source_rows:
        raise RuntimeError(
            f"{dataset}: registros escritos={processed}; origen={source_rows}."
        )

    assigned_rows = sum(item["rows"] for item in files)
    if assigned_rows != source_rows:
        raise RuntimeError(
            f"{dataset}: filas distribuidas={assigned_rows}; origen={source_rows}."
        )

    return {
        "dataset": dataset,
        "source_file": source.name,
        "source_file_sha256": source_digest,
        "source_rows": source_rows,
        "parquet_rows": assigned_rows,
        "frequency": frequency,
        "calendar_start": CALENDAR_START,
        "calendar_end": CALENDAR_END,
        "calendar_step_days": FREQUENCIES[frequency],
        "calendar_dates": len(dates),
        "parquet_files": sorted(files, key=lambda item: item["path"]),
        "metadata_columns": METADATA_COLUMNS,
        "row_hash_algorithm": "pandas.util.hash_pandas_object (uint64)",
    }


def replace_output(staging_root: Path, final_root: Path) -> None:
    backup = final_root.with_name(f".{final_root.name}.previous")
    if backup.exists() and not final_root.exists():
        os.replace(backup, final_root)
    elif backup.exists():
        shutil.rmtree(backup)
    if final_root.exists():
        os.replace(final_root, backup)
    try:
        os.replace(staging_root, final_root)
    except Exception:
        if backup.exists() and not final_root.exists():
            os.replace(backup, final_root)
        raise
    if backup.exists():
        shutil.rmtree(backup)


def run(frequency: str) -> None:
    if frequency not in FREQUENCIES:
        raise ValueError(f"Frecuencia no válida: {frequency}")

    dates = date_calendar(frequency)
    BRONZE.mkdir(parents=True, exist_ok=True)
    staging_root = BRONZE / f".staging_{frequency}_{uuid.uuid4().hex}"
    final_root = BRONZE / frequency
    staging_root.mkdir(parents=True, exist_ok=False)

    try:
        manifests = {}
        for dataset, config in DATASETS.items():
            logger.info(
                "Simulando %s | frecuencia=%s | fechas=%s",
                dataset,
                frequency,
                len(dates),
            )
            manifests[dataset] = simulate_dataset(
                dataset,
                config,
                frequency,
                dates,
                staging_root,
            )

        metadata_dir = staging_root / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "contract_version": 1,
            "frequency": frequency,
            "calendar_start": CALENDAR_START,
            "calendar_end": CALENDAR_END,
            "calendar_step_days": FREQUENCIES[frequency],
            "calendar_dates": len(dates),
            "datasets": manifests,
        }
        (metadata_dir / "bronze_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        replace_output(staging_root, final_root)
        logger.info(
            "Bronze Simulator completado | frecuencia=%s | datasets=%s",
            frequency,
            len(manifests),
        )
    except Exception:
        if staging_root.exists():
            shutil.rmtree(staging_root, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Genera lotes Parquet deterministas con fechas simuladas de recepción. "
            "No modifica los valores originales."
        )
    )
    parser.add_argument(
        "--frequency",
        choices=sorted(FREQUENCIES),
        default=os.getenv("BRONZE_INGESTION_FREQUENCY", "daily"),
    )
    args = parser.parse_args()
    try:
        run(args.frequency)
        return 0
    except Exception:
        logger.exception("Falló Bronze Simulator.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
