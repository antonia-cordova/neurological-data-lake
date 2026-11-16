from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LEGACY_BRONZE = ROOT / "data" / "bronze"
LANDING = ROOT / "data" / "landing"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("landing")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def move_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        if not source.exists():
            return
        if source.stat().st_size != target.stat().st_size:
            raise RuntimeError(
                f"Existen dos versiones distintas de {target.name}: "
                f"{source} y {target}."
            )
        # Mismo tamaño no demuestra igualdad: comparar SHA-256.
        if sha256_file(source) != sha256_file(target):
            raise RuntimeError(
                f"Conflicto de contenido entre la fuente heredada y Landing: "
                f"{source} / {target}."
            )
        source.unlink()
        logger.info("Duplicado idéntico retirado de la ubicación heredada: %s", source)
        return

    if not source.is_file():
        raise FileNotFoundError(f"No se encontró el archivo fuente: {source}")

    os.replace(source, target)
    logger.info("Landing incorporado: %s", target.relative_to(ROOT))


def prepare() -> dict:
    mapping = {
        "acs_2024": [
            ("ACSDP*Y2024.DP05*.csv", None),
            ("DP05.json", "DP05.json"),
        ],
        "brfss_2023": [
            ("LLCP2023.XPT", "LLCP2023.XPT"),
            ("USCODE23_LLCP_021924.HTML", "USCODE23_LLCP_021924.HTML"),
        ],
        "places_2025": [
            ("*.csv", None),
        ],
        "acs_2024_county": [
            ("acs_dp05_county_run_*Z.json", None),
            ("acs_dp05_county_run_*.manifest.json", None),
        ],
    }

    manifest_path = LANDING / "metadata" / "landing_manifest.json"
    previous_files = {}
    if manifest_path.is_file():
        previous = json.loads(manifest_path.read_text(encoding="utf-8"))
        previous_files = {
            item["file"]: item
            for item in previous.get("files", [])
        }

    records = []
    for dataset, items in mapping.items():
        legacy_dir = LEGACY_BRONZE / dataset
        landing_dir = LANDING / dataset
        landing_dir.mkdir(parents=True, exist_ok=True)

        for pattern, fixed_name in items:
            legacy_matches = sorted(legacy_dir.glob(pattern))
            landing_matches = sorted(landing_dir.glob(pattern))

            if len(legacy_matches) > 1 or len(landing_matches) > 1:
                raise RuntimeError(
                    f"{dataset}: versiones ambiguas para {pattern!r}; "
                    f"legacy={len(legacy_matches)}, landing={len(landing_matches)}."
                )

            if legacy_matches:
                source = legacy_matches[0]
                target_name = fixed_name or source.name
                target = landing_dir / target_name
                move_file(source, target)
            elif landing_matches:
                target = landing_matches[0]
            else:
                raise FileNotFoundError(
                    f"{dataset}: no se encontró {pattern!r} ni en "
                    f"{legacy_dir} ni en {landing_dir}."
                )

            relative_path = str(target.relative_to(ROOT)).replace("\\", "/")
            file_size = target.stat().st_size
            file_digest = sha256_file(target)
            previous_record = previous_files.get(relative_path)
            if (
                previous_record
                and previous_record.get("sha256")
                and previous_record["sha256"] != file_digest
            ):
                raise RuntimeError(
                    f"Landing es inmutable: cambió el contenido de {relative_path}."
                )
            records.append({
                "dataset": dataset,
                "file": relative_path,
                "bytes": file_size,
                "sha256": file_digest,
            })

    manifest_dir = LANDING / "metadata"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "contract_version": 1,
        "datasets": sorted({item["dataset"] for item in records}),
        "files": sorted(records, key=lambda item: (item["dataset"], item["file"])),
    }
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "Landing validado: %s archivos originales en %s.",
        len(records),
        LANDING.relative_to(ROOT),
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Prepara Landing trasladando, sin transformar, los archivos fuente "
            "desde la estructura Bronze heredada."
        )
    )
    parser.parse_args()
    try:
        prepare()
        return 0
    except Exception:
        logger.exception("Falló la preparación de Landing.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
