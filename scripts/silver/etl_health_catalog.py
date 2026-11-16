
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from scripts.bronze.parquet_io import (
    dataset_path,
    read_parquet_frame,
    selected_frequency,
    validate_dataset_manifest,
)

from scripts.database_config import get_target_database

ROOT = Path(__file__).resolve().parents[2]
LANDING_ACS_DIR = ROOT / "data" / "landing" / "acs_2024"
LEGACY_ACS_DIR = ROOT / "data" / "bronze" / "acs_2024"
ACS_DIR = (
    LANDING_ACS_DIR
    if (LANDING_ACS_DIR / "DP05.json").is_file()
    else LEGACY_ACS_DIR
)
BRFSS_DIR = ROOT / "data" / "landing" / "brfss_2023"
LANDING_PLACES_DIR = ROOT / "data" / "landing" / "places_2025"
LEGACY_PLACES_DIR = ROOT / "data" / "bronze" / "places_2025"
ACS_METADATA_FILE = ACS_DIR / "DP05.json"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("etl_health_catalog")


SOURCES = [
    {
        "source_code": "ACS_DP05_NATIONAL_2024",
        "source_name": "ACS 2024 5-Year Data Profiles - DP05 - United States",
        "provider": "U.S. Census Bureau",
        "dataset_name": "acs_2024",
        "release_year": 2024,
        "reference_period_start": 2020,
        "reference_period_end": 2024,
    },
    {
        "source_code": "ACS_DP05_COUNTY_2024",
        "source_name": "ACS 2024 5-Year Data Profiles - DP05 - County",
        "provider": "U.S. Census Bureau",
        "dataset_name": "acs_2024_county_dp05",
        "release_year": 2024,
        "reference_period_start": 2020,
        "reference_period_end": 2024,
    },
    {
        "source_code": "BRFSS_2023",
        "source_name": "Behavioral Risk Factor Surveillance System 2023",
        "provider": "Centers for Disease Control and Prevention",
        "dataset_name": "brfss_2023",
        "release_year": 2023,
        "reference_period_start": 2023,
        "reference_period_end": 2023,
    },
    {
        "source_code": "PLACES_2025",
        "source_name": "PLACES 2025 release - 2023 data",
        "provider": "Centers for Disease Control and Prevention",
        "dataset_name": "places_2025",
        "release_year": 2025,
        "reference_period_start": 2023,
        "reference_period_end": 2023,
    },
]


BRFSS_META = {
    "_AGEG5YR": ("Age category", "demographic", "category", "ordinal"),
    "_SEX": ("Sex", "demographic", "category", "nominal"),
    "_RACEGR3": ("Race/ethnicity", "demographic", "category", "nominal"),
    "EDUCA": ("Education level", "socioeconomic", "category", "ordinal"),
    "INCOME3": ("Household income category", "socioeconomic", "category", "ordinal"),
    "GENHLTH": ("General health status", "health", "category", "ordinal"),
    "PHYSHLTH": ("Physical health: number of days not good", "health", "days", "count"),
    "MENTHLTH": ("Mental health: number of days not good", "health", "days", "count"),
    "POORHLTH": (
        "Activity limitation due to poor physical or mental health",
        "health",
        "days",
        "count",
    ),
    "DIABETE4": ("Ever diagnosed with diabetes", "health", "category", "nominal"),
    "CVDCRHD4": ("Ever diagnosed with coronary heart disease", "health", "category", "nominal"),
    "CVDSTRK3": ("Ever diagnosed with stroke", "health", "category", "nominal"),
    "HAVARTH4": ("Ever diagnosed with arthritis", "health", "category", "nominal"),
    "ADDEPEV3": ("Ever diagnosed with depressive disorder", "health", "category", "nominal"),
    "DIFFWALK": ("Difficulty walking or climbing stairs", "health", "category", "nominal"),
    "DIFFALON": ("Difficulty doing errands alone", "health", "category", "nominal"),
    "_BMI5": ("Body mass index", "health_risk", "BMI", "continuous"),
    "_SMOKER3": ("Smoking status", "health_risk", "category", "ordinal"),
    "DRNKANY6": ("Any alcohol consumption", "health_risk", "category", "nominal"),
    "_TOTINDA": ("Physical activity status", "health_risk", "category", "nominal"),
}


ACS_COUNTY_META = {
    "DP05_0001": ("Total population", "demographic", "persons", "count"),
    "DP05_0002": ("Male population", "demographic", "persons", "count"),
    "DP05_0003": ("Female population", "demographic", "persons", "count"),
    "DP05_0018": ("Median age", "demographic", "years", "continuous"),
}


PLACES_CATEGORY = {
    "COGNITION": ("Cognitive disability", "health"),
    "STROKE": ("Stroke prevalence", "health"),
    "DISABILITY": ("Any disability", "health"),
    "MOBILITY": ("Mobility disability", "health"),
    "SELFCARE": ("Self-care disability", "health"),
    "INDEPLIVE": ("Independent living disability", "health"),
    "HEARING": ("Hearing disability", "health"),
    "VISION": ("Vision disability", "health"),
    "DEPRESSION": ("Depression prevalence", "health"),
    "MHLTH": ("Frequent mental distress", "health"),
    "PHLTH": ("Frequent physical distress", "health"),
    "ARTHRITIS": ("Arthritis prevalence", "health"),
    "CHD": ("Coronary heart disease prevalence", "health"),
    "DIABETES": ("Diabetes prevalence", "health"),
    "GHLTH": ("Fair or poor general health", "health"),
}


def db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    target_database = get_target_database()
    if os.getenv("POSTGRES_DB") != target_database:
        raise RuntimeError(
            f"Conexión rechazada: POSTGRES_DB debe ser {target_database}."
        )

    if os.getenv("POSTGRES_HOST") != "localhost":
        raise RuntimeError(
            "Conexión rechazada: POSTGRES_HOST debe ser localhost."
        )

    if int(os.getenv("POSTGRES_PORT", "0")) != 5432:
        raise RuntimeError(
            "Conexión rechazada: POSTGRES_PORT debe ser 5432."
        )

    required = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]

    if missing:
        raise RuntimeError(
            f"Faltan variables PostgreSQL: {', '.join(missing)}"
        )

    url = URL.create(
        "postgresql+psycopg",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="localhost",
        port=5432,
        database=target_database,
    )

    engine = create_engine(url, pool_pre_ping=True)

    try:
        with engine.connect() as conn:
            actual_database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if actual_database != target_database:
                raise RuntimeError(
                    f"Base de datos inesperada: {actual_database!r}"
                )

        return engine

    except Exception:
        engine.dispose()
        raise


def clean_label(value: str) -> str:
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def classify_acs(label: str, concept: str) -> str:
    s = f"{label} {concept}".lower()

    if any(
        k in s
        for k in (
            "income", "poverty", "education", "employment",
            "occupation", "insurance", "computer", "internet",
            "housing", "tenure", "rent", "owner", "language",
            "marital",
        )
    ):
        return "socioeconomic"

    if any(
        k in s
        for k in (
            "population", "sex", "age", "race", "hispanic",
            "veteran", "citizenship", "nativity", "household",
            "family",
        )
    ):
        return "demographic"

    if any(
        k in s
        for k in (
            "disability", "health", "ambulatory", "cognitive",
            "hearing", "vision", "self-care",
        )
    ):
        return "health"

    return "demographic"


def infer_acs_unit(label: str, concept: str) -> str:
    s = f"{label} {concept}".lower()

    if "median age" in s or "years" in s:
        return "years"

    if "percent" in s or "percentage" in s:
        return "%"

    if "population" in s or "persons" in s:
        return "persons"

    return "category"


def read_acs_national_metadata() -> list[dict[str, Any]]:
    candidates = sorted(
        ACS_DIR.glob("ACSDP5Y2024.DP05*.csv")
    )

    if not candidates:
        raise FileNotFoundError(
            f"No se encontró CSV DP05 en {ACS_DIR}"
        )

    required = [
        "Label (Grouping)",
        "United States!!Estimate",
        "United States!!Margin of Error",
        "United States!!Percent",
        "United States!!Percent Margin of Error",
    ]

    if selected_frequency() is not None:
        manifest = validate_dataset_manifest("acs_2024")
        frame = read_parquet_frame(
            dataset_path("acs_2024"),
            columns=required,
        ).to_pandas()
        frame = frame.fillna("").astype(str)
        if len(frame) != int(manifest["source_rows"]):
            raise ValueError("El catálogo ACS no coincide con el conteo Bronze.")
    else:
        frame = pd.read_csv(
            candidates[0],
            dtype=str,
            keep_default_na=False,
        )

    if list(frame.columns) != required or len(frame) != 113:
        raise ValueError(
            "El CSV ACS nacional no coincide con la estructura esperada (113 x 5)."
        )

    if not ACS_METADATA_FILE.is_file():
        raise FileNotFoundError(
            f"No se encontró metadata oficial ACS: {ACS_METADATA_FILE}"
        )

    with ACS_METADATA_FILE.open("r", encoding="utf-8") as file:
        variables = json.load(file)["variables"]

    metadata_by_path: dict[
        tuple[str, ...], list[dict[str, Any]]
    ] = {}

    for code, item in variables.items():
        label = item.get("label", "")

        if (
            not code.startswith("DP05_")
            or not code.endswith("E")
            or code.endswith(("PE", "EA", "PEA"))
        ):
            continue

        if "!!" not in label:
            continue

        parts = tuple(
            clean_label(part) for part in label.split("!!")
        )

        if parts and parts[0] == "Estimate":
            parts = parts[1:]

        if parts:
            metadata_by_path.setdefault(parts, []).append(
                {
                    "code": code[:-1],
                    "label": parts[-1],
                    "concept": item.get("concept", ""),
                }
            )

    for items in metadata_by_path.values():
        items.sort(key=lambda item: item["code"])

    stack: list[tuple[int, str]] = []
    records: list[dict[str, Any]] = []
    path_occurrences: dict[tuple[str, ...], int] = {}

    for raw in frame["Label (Grouping)"].tolist():
        raw_nbsp = raw.replace("\xa0", " ")
        label = clean_label(raw)

        if not label:
            continue

        indent = len(raw_nbsp) - len(raw_nbsp.lstrip(" "))

        if indent == 0:
            stack = [(0, label)]
        else:
            depth = max(1, indent // 4)

            while stack and stack[-1][0] >= depth:
                stack.pop()

            stack.append((depth, label))

        path = tuple(x[1] for x in stack)
        candidates_for_path = metadata_by_path.get(path, [])

        item = None

        if candidates_for_path:
            occurrence = path_occurrences.get(path, 0)

            if occurrence < len(candidates_for_path):
                item = candidates_for_path[occurrence]

            path_occurrences[path] = occurrence + 1

        else:
            matches = [
                items
                for key, items in metadata_by_path.items()
                if key[-1] == label
            ]

            flattened = [
                candidate
                for items in matches
                for candidate in items
            ]

            if len(flattened) == 1:
                item = flattened[0]

        if item is None:
            continue

        records.append(
            {
                "source_code": item["code"],
                "indicator_name": item["label"],
                "indicator_category": classify_acs(
                    item["label"], item["concept"]
                ),
                "unit": infer_acs_unit(
                    item["label"], item["concept"]
                ),
                "value_type": "estimate_and_percent",
                "source_id_code": "ACS_DP05_NATIONAL_2024",
            }
        )

    expected_codes = {
        code[:-1]
        for code in variables
        if (
            code.startswith("DP05_")
            and code.endswith("E")
            and not code.endswith(("PE", "EA", "PEA"))
            and "!!" in variables[code].get("label", "")
        )
    }

    codes = {
        record["source_code"]
        for record in records
    }

    missing = expected_codes - codes
    unexpected = codes - expected_codes

    if (
        missing
        or unexpected
        or len(codes) != len(expected_codes)
    ):
        raise RuntimeError(
            "Mapeo ACS DP05 no íntegro. "
            f"Esperados={len(expected_codes)}, "
            f"resueltos={len(codes)}, "
            f"faltantes={sorted(missing)}, "
            f"inesperados={sorted(unexpected)}. "
            "No se cargó el catálogo."
        )

    return records


def build_indicator_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    records.extend(read_acs_national_metadata())

    for code, (
        name, category, unit, value_type
    ) in ACS_COUNTY_META.items():
        records.append(
            {
                "source_code": code,
                "indicator_name": name,
                "indicator_category": category,
                "unit": unit,
                "value_type": value_type,
                "source_id_code": "ACS_DP05_COUNTY_2024",
            }
        )

    for code, (
        name, category, unit, value_type
    ) in BRFSS_META.items():
        records.append(
            {
                "source_code": code,
                "indicator_name": name,
                "indicator_category": category,
                "unit": unit,
                "value_type": value_type,
                "source_id_code": "BRFSS_2023",
            }
        )

    if selected_frequency() is not None:
        manifest = validate_dataset_manifest("places_2025")
        places = read_parquet_frame(
            dataset_path("places_2025"),
            columns=["Measure", "MeasureId"],
        ).to_pandas()
        if len(places) != int(manifest["source_rows"]):
            raise ValueError("El catálogo PLACES no coincide con el conteo Bronze.")
        places = places.replace("", pd.NA).astype("string")
    else:
        bronze_candidates = sorted(LANDING_PLACES_DIR.glob("*.csv"))
        if not bronze_candidates:
            bronze_candidates = sorted(LEGACY_PLACES_DIR.glob("*.csv"))
        if len(bronze_candidates) != 1:
            raise FileNotFoundError(
                "Se esperaba un CSV PLACES en Landing o Bronze heredado; "
                f"encontrados={len(bronze_candidates)}."
            )
        places = pd.read_csv(
            bronze_candidates[0],
            usecols=["Measure", "MeasureId"],
            dtype=str,
        )

    places = places.dropna(subset=["MeasureId"]).copy()
    places["MeasureId"] = places["MeasureId"].str.strip()

    # Bronze conserva el catálogo completo de PLACES.
    # Silver solo utiliza las 15 medidas seleccionadas.
    found_in_bronze = set(places["MeasureId"])

    missing_selected = set(PLACES_CATEGORY) - found_in_bronze

    if missing_selected:
        raise ValueError(
            "Faltan MeasureId seleccionados en Bronze PLACES: "
            f"{sorted(missing_selected)}"
        )

    places = (
        places.loc[
            places["MeasureId"].isin(PLACES_CATEGORY)
        ]
        .drop_duplicates("MeasureId")
        .sort_values("MeasureId")
    )

    selected_found = set(places["MeasureId"])

    if selected_found != set(PLACES_CATEGORY):
        raise ValueError(
            "La selección PLACES no coincide con las 15 medidas esperadas. "
            f"Faltan={sorted(set(PLACES_CATEGORY)-selected_found)}; "
            f"extra={sorted(selected_found-set(PLACES_CATEGORY))}"
        )

    for row in places.to_dict("records"):
        code = row["MeasureId"].strip()
        name, category = PLACES_CATEGORY[code]

        records.append(
            {
                "source_code": code,
                "indicator_name": name,
                "indicator_category": category,
                "unit": "%",
                "value_type": "prevalence",
                "source_id_code": "PLACES_2025",
            }
        )

    return records


def upsert_catalog(
    engine: Engine,
    indicators: list[dict[str, Any]],
) -> tuple[int, int]:

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.silver_source
                    (
                        source_code,
                        source_name,
                        provider,
                        dataset_name,
                        release_year,
                        reference_period_start,
                        reference_period_end
                    )
                VALUES
                    (
                        :source_code,
                        :source_name,
                        :provider,
                        :dataset_name,
                        :release_year,
                        :reference_period_start,
                        :reference_period_end
                    )
                ON CONFLICT (source_code) DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    provider = EXCLUDED.provider,
                    dataset_name = EXCLUDED.dataset_name,
                    release_year = EXCLUDED.release_year,
                    reference_period_start = EXCLUDED.reference_period_start,
                    reference_period_end = EXCLUDED.reference_period_end
                """
            ),
            SOURCES,
        )

        source_ids = dict(
            conn.execute(
                text(
                    """
                    SELECT source_code, source_id
                    FROM public.silver_source
                    """
                )
            ).all()
        )

        rows = []

        for item in indicators:
            rows.append(
                {
                    **item,
                    "source_id": source_ids[item["source_id_code"]],
                }
            )

        conn.execute(
            text(
                """
                INSERT INTO public.silver_health_indicator
                    (
                        source_code,
                        indicator_name,
                        indicator_category,
                        unit,
                        value_type,
                        source_id
                    )
                VALUES
                    (
                        :source_code,
                        :indicator_name,
                        :indicator_category,
                        :unit,
                        :value_type,
                        :source_id
                    )
                ON CONFLICT (source_id, source_code) DO UPDATE SET
                    indicator_name = EXCLUDED.indicator_name,
                    indicator_category = EXCLUDED.indicator_category,
                    unit = EXCLUDED.unit,
                    value_type = EXCLUDED.value_type
                """
            ),
            rows,
        )

        source_count = conn.execute(
            text("SELECT COUNT(*) FROM public.silver_source")
        ).scalar_one()

        indicator_count = conn.execute(
            text(
                "SELECT COUNT(*) FROM public.silver_health_indicator"
            )
        ).scalar_one()

        if source_count != 4:
            raise RuntimeError(
                f"Se esperaban 4 fuentes; hay {source_count}."
            )

        if indicator_count < 100:
            raise RuntimeError(
                f"Catálogo insuficiente: {indicator_count} indicadores."
            )

        return int(source_count), int(indicator_count)


def main() -> int:
    indicators = build_indicator_records()

    keys = [
        (item["source_id_code"], item["source_code"])
        for item in indicators
    ]

    if len(keys) != len(set(keys)):
        raise RuntimeError(
            "Hay códigos duplicados dentro del catálogo preparado."
        )

    log.info(
        "Indicadores preparados para carga: %s",
        len(indicators),
    )

    engine = db_engine()

    try:
        source_count, indicator_count = upsert_catalog(
            engine,
            indicators,
        )

        log.info(
            "Catálogos Silver actualizados | fuentes=%s | indicadores=%s",
            source_count,
            indicator_count,
        )

    finally:
        engine.dispose()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
