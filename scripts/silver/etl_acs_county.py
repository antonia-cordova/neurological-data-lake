from __future__ import annotations
import argparse
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import requests
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from scripts.silver.silver_budget import (
    enforce_silver_budget,
    lock_silver_budget,
)

ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = ROOT / "data" / "bronze" / "acs_2024_county"
API_URL = "https://api.census.gov/data/2024/acs/acs5/profile"
DATASET_NAME = "acs_2024_county_dp05"
TARGET_TABLE = "silver_acs_county_2024"
VARIABLES = {
    "DP05_0001": "Total population",
    "DP05_0002": "Male population",
    "DP05_0003": "Female population",
    "DP05_0018": "Median age",
}
# E estimate, M margin of error, PE percent estimate, PM percent margin of error.
FIELDS = ["NAME"]
for code in VARIABLES:
    FIELDS.extend([f"{code}E", f"{code}M"])
    if code != "DP05_0018":  # La edad mediana no tiene porcentaje.
        FIELDS.extend([f"{code}PE", f"{code}PM"])
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("etl_acs_county")



def db_engine() -> Engine:
    load_dotenv(ROOT / ".env", override=False)

    allowed_database = "health_datalake"
    allowed_host = "localhost"
    allowed_port = 5432

    database = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST")
    port_raw = os.getenv("POSTGRES_PORT")

    if database != allowed_database:
        raise RuntimeError(
            f"Conexión rechazada: base no permitida: {database!r}"
        )

    if host != allowed_host:
        raise RuntimeError(
            f"Conexión rechazada: host no permitido: {host!r}"
        )

    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        raise RuntimeError(
            f"Puerto PostgreSQL inválido: {port_raw!r}"
        )

    if port != allowed_port:
        raise RuntimeError(
            f"Conexión rechazada: solo se permite el puerto "
            f"{allowed_port}. Configurado: {port}"
        )

    required = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]

    missing = [
        key for key in required if not os.getenv(key)
    ]

    if missing:
        raise RuntimeError(
            f"Faltan variables PostgreSQL: {', '.join(missing)}"
        )

    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=allowed_host,
        port=allowed_port,
        database=allowed_database,
    )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )

    try:
        with engine.connect() as conn:
            actual_database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if actual_database != allowed_database:
                raise RuntimeError(
                    f"Conexión rechazada: PostgreSQL respondió "
                    f"con la base {actual_database!r}"
                )

        return engine

    except Exception:
        engine.dispose()
        raise


def start_run(engine: Engine) -> int:
    with engine.begin() as conn:
        return int(conn.execute(text("""
            INSERT INTO public.etl_runs
                (dataset_name, target_table, started_at, status, source_rows, loaded_rows)
            VALUES
                (:dataset, :target, clock_timestamp(), 'running', 0, 0)
            RETURNING run_id
        """), {
            "dataset": DATASET_NAME,
            "target": TARGET_TABLE,
        }).scalar_one())


def finish_run(
    engine: Engine,
    run_id: int,
    status: str,
    source_rows: int,
    loaded_rows: int,
    error: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE public.etl_runs
            SET finished_at = GREATEST(clock_timestamp(), started_at),
                status = :status,
                source_rows = :source_rows,
                loaded_rows = :loaded_rows,
                error_message = :error
            WHERE run_id = :run_id
        """), {
            "run_id": run_id,
            "status": status,
            "source_rows": source_rows,
            "loaded_rows": loaded_rows,
            "error": error,
        })


def target_geographies(engine: Engine) -> dict[str, dict[str, str]]:
    """
    Obtiene los 83 condados del proyecto desde Silver PLACES.

    La muestra geográfica se define durante la carga Silver PLACES.
    ACS County no debe depender de dimensiones Gold todavía no cargadas.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                location_id,
                MIN(state_abbr) AS state_abbr,
                MIN(location_name) AS location_name,
                COUNT(DISTINCT state_abbr) AS state_variants,
                COUNT(DISTINCT location_name) AS name_variants
            FROM public.silver_places_2023
            GROUP BY location_id
            ORDER BY location_id
        """)).mappings().all()

    result = {}

    for row in rows:
        fips = str(row["location_id"]).strip()

        if len(fips) != 5 or not fips.isdigit():
            raise ValueError(
                f"FIPS inválido en Silver PLACES: {fips!r}"
            )

        if row["state_variants"] != 1:
            raise ValueError(
                f"Estado inconsistente para FIPS {fips}"
            )

        if row["name_variants"] != 1:
            raise ValueError(
                f"Nombre geográfico inconsistente para FIPS {fips}"
            )

        result[fips] = {
            "state": fips[:2],
            "county": fips[2:],
        }

    if len(result) != 83:
        raise ValueError(
            "Se esperaban 83 condados definidos por Silver PLACES; "
            f"encontrados {len(result)}."
        )

    log.info(
        "Universo geográfico validado desde Silver PLACES: %s condados.",
        len(result),
    )

    return result


def fetch(api_key: str) -> tuple[list[dict[str, str]], dict[str, Any]]:
    params = {"get": ",".join(FIELDS), "for":"county:*", "in":"state:*", "key":api_key}
    response = None
    for attempt in range(1, 5):
        try:
            response = requests.get(API_URL, params=params, timeout=60)
            response.raise_for_status()
            break
        except requests.RequestException as exc:
            if attempt == 4:
                raise RuntimeError(f"Error Census API: {exc}") from exc
            delay = min(2 ** attempt, 30)
            log.warning("Fallo API, reintento %s/4 en %ss", attempt, delay)
            time.sleep(delay)
    payload = response.json()
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError(f"Respuesta Census API inesperada: {str(payload)[:500]}")
    headers = payload[0]
    required = set(FIELDS + ["state", "county"])
    absent = required - set(headers)
    if absent:
        raise ValueError(f"La API no devolviÃ³ campos requeridos: {sorted(absent)}")
    rows = [dict(zip(headers, values, strict=True)) for values in payload[1:]]
    metadata = {"api_url":API_URL,"dataset":"2024 ACS 5-Year Data Profiles",
        "profile":"DP05","geography":"county:* within state:*",
        "requested_variables":VARIABLES,"requested_fields":FIELDS,
        "api_rows_excluding_header":len(rows),"http_status":response.status_code,
        "retrieved_at_utc":datetime.now(timezone.utc).isoformat(),"api_key_persisted":False}
    return rows, metadata


def read_bronze(path: Path) -> tuple[list[dict[str, str]], dict[str, Any], str]:
    """Lee Bronze y verifica su integridad mediante el manifiesto SHA-256."""
    path = path.resolve()

    if not path.is_file():
        raise FileNotFoundError(f"No existe el archivo Bronze: {path}")

    raw = path.read_bytes()
    checksum = hashlib.sha256(raw).hexdigest()

    # Validar el manifiesto asociado.
    manifest_path = path.with_name(f"{path.stem}.manifest.json")

    if not manifest_path.is_file():
        raise ValueError(
            f"No existe el manifiesto asociado al Bronze: {manifest_path.name}"
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(
            f"Manifiesto inválido: {manifest_path.name}"
        ) from exc

    if not isinstance(manifest, dict):
        raise ValueError("El manifiesto Bronze debe contener un objeto JSON.")

    if manifest.get("raw_file") != path.name:
        raise ValueError(
            f"El manifiesto no corresponde al archivo {path.name}."
        )

    expected_checksum = manifest.get("sha256")

    if not expected_checksum or expected_checksum != checksum:
        raise ValueError(
            f"SHA-256 no coincide para {path.name}. "
            "El archivo puede estar incompleto o modificado."
        )

    if manifest.get("raw_file_bytes") != len(raw):
        raise ValueError(
            f"Tamaño del archivo no coincide con el manifiesto: {path.name}"
        )

    # Validar estructura JSON y campos requeridos.
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"JSON Bronze inválido: {path.name}") from exc

    if not isinstance(payload, dict):
        raise ValueError("El Bronze debe contener un objeto JSON.")

    rows = payload.get("data")

    if not isinstance(rows, list) or not rows:
        raise ValueError("Archivo Bronze vacío o sin una lista 'data' válida.")

    required = set(FIELDS + ["state", "county"])

    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not required.issubset(row):
            raise ValueError(
                f"Estructura inválida en la fila {index} del archivo {path.name}."
            )

    metadata = {
        "dataset": "2024 ACS 5-Year Data Profiles",
        "profile": "DP05",
        "source": "existing_bronze",
        "bronze_file": str(path),
        "manifest_file": str(manifest_path),
        "manifest_run_id": manifest.get("run_id"),
    }

    return rows, metadata, checksum


def discover_bronze() -> Path | None:
    """
    Busca archivos Bronze válidos en BRONZE_DIR.

    Selecciona automáticamente solo cuando existe exactamente uno.
    Si hay varios, exige selección explícita mediante --bronze.
    """
    if not BRONZE_DIR.is_dir():
        log.info("No existe todavía la carpeta Bronze: %s", BRONZE_DIR)
        return None

    candidates = sorted(
        path for path in BRONZE_DIR.glob("*.json")
        if not path.name.endswith(".manifest.json")
    )

    if not candidates:
        log.info("No hay archivos Bronze disponibles en %s", BRONZE_DIR)
        return None

    valid_files = []

    for candidate in candidates:
        try:
            read_bronze(candidate)
            valid_files.append(candidate)
            log.info("Bronze válido: %s", candidate.name)

        except (OSError, ValueError, json.JSONDecodeError) as exc:
            log.warning(
                "Bronze descartado: %s | Motivo: %s",
                candidate.name,
                exc,
            )

    if len(valid_files) == 0:
        log.warning("No se encontró ningún archivo Bronze válido.")
        return None

    if len(valid_files) > 1:
        names = "\n".join(
            f"  - {path.name}" for path in valid_files
        )
        raise RuntimeError(
            "Se encontraron varias versiones Bronze válidas. "
            "Para evitar una selección ambigua, indica una mediante --bronze:\n"
            f"{names}"
        )

    selected = valid_files[0]

    log.info(
        "Selección automática de Bronze: %s",
        selected.name,
    )

    return selected


def save_bronze(rows: list[dict[str,str]], metadata: dict[str,Any], run_id: int) -> str:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = f"acs_dp05_county_run_{run_id}_{stamp}"
    raw = json.dumps({"headers": FIELDS + ["state", "county"], "data": rows},
                     ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    (BRONZE_DIR / f"{stem}.json").write_bytes(raw)
    manifest = {**metadata,"run_id":run_id,"raw_file":f"{stem}.json",
                "sha256":digest,"raw_file_bytes":len(raw),"software":"etl_acs_county.py"}
    (BRONZE_DIR / f"{stem}.manifest.json").write_text(
        json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")
    return digest


def clean_stat_value(value: Any) -> str | None:
    """Convierte códigos especiales ACS en NULL, conservando valores válidos."""
    if value is None or str(value).strip() == "":
        return None

    special_codes = {
        -555555555,
        -666666666,
        -888888888,
        -999999999,
    }

    try:
        if float(value) in special_codes:
            return None
    except (ValueError, TypeError):
        pass

    return str(value)


def normalize(
    api_rows: list[dict[str, str]],
    geos: dict[str, dict[str, str]]
) -> list[dict[str, Any]]:

    api_by_fips = {}

    for row in api_rows:
        fips = str(row["state"]).zfill(2) + str(row["county"]).zfill(3)

        if fips in api_by_fips:
            raise ValueError(f"FIPS duplicado en API: {fips}")

        api_by_fips[fips] = row

    missing = sorted(set(geos) - set(api_by_fips))

    if missing:
        raise ValueError(
            f"La API no devolvió {len(missing)} condados del proyecto: {missing}"
        )

    result = []

    for fips, geo in geos.items():
        row = api_by_fips[fips]

        for code in VARIABLES:
            result.append({
                "state_fips": geo["state"],
                "county_fips": geo["county"],
                "geography_id": fips,
                "geography_name": row["NAME"],
                "variable_code": code,
                "estimate": clean_stat_value(row.get(code + "E")),
                "margin_of_error": clean_stat_value(row.get(code + "M")),
                "percent": clean_stat_value(row.get(code + "PE")),
                "percent_margin_of_error": clean_stat_value(row.get(code + "PM")),
            })

    expected = len(geos) * len(VARIABLES)

    if len(result) != expected:
        raise ValueError(
            f"Cantidad inesperada: {len(result)}; esperada: {expected}"
        )

    return result



def load_silver(
    engine: Engine,
    rows: list[dict[str, Any]],
    run_id: int,
    replace_existing: bool = False,
) -> int:
    """
    Carga ACS County en Silver con protección contra duplicaciones.

    Por defecto, impide cargar si ya existen registros.
    La sustitución requiere autorización explícita mediante
    --replace-existing y una carga existente íntegra.
    """

    expected_rows = 332
    expected_geographies = 83
    expected_variables = 4

    # 1. Validaciones de los datos antes de acceder a la base.
    if len(rows) != expected_rows:
        raise ValueError(
            f"Se esperaban {expected_rows} registros; "
            f"recibidos {len(rows)}."
        )

    natural_keys = [
        (row["geography_id"], row["variable_code"])
        for row in rows
    ]

    if len(natural_keys) != len(set(natural_keys)):
        raise ValueError(
            "Se detectaron combinaciones duplicadas "
            "(geography_id, variable_code) en los datos de entrada."
        )

    # 2. Preparar sentencia de inserción.
    insert = text("""
        INSERT INTO public.silver_acs_county_2024 (
            state_fips,
            county_fips,
            geography_id,
            geography_name,
            variable_code,
            estimate,
            margin_of_error,
            percent,
            percent_margin_of_error,
            run_id
        )
        VALUES (
            :state_fips,
            :county_fips,
            :geography_id,
            :geography_name,
            :variable_code,
            :estimate,
            :margin_of_error,
            :percent,
            :percent_margin_of_error,
            :run_id
        )
    """)

    # 3. Ejecutar la carga dentro de una transacción.
    with engine.begin() as conn:

        # Serializar la operación con las demás cargas Silver.
        lock_silver_budget(conn)

        # Comprobar si ya existen registros ACS County.
        existing_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM public.silver_acs_county_2024
        """)).scalar_one()

        if existing_count > 0:

            # Sin autorización explícita, no modificar la tabla.
            if not replace_existing:
                raise RuntimeError(
                    "Carga ACS County bloqueada: "
                    f"la tabla ya contiene {existing_count} registros. "
                    "Para reemplazar la carga existente utiliza "
                    "--replace-existing."
                )

            # Verificar que la carga existente esté íntegra.
            existing_runs = conn.execute(text("""
                SELECT COUNT(DISTINCT run_id)
                FROM public.silver_acs_county_2024
            """)).scalar_one()

            if (
                existing_count != expected_rows
                or existing_runs != 1
            ):
                raise RuntimeError(
                    "No se permite reemplazar ACS County: "
                    f"se encontraron {existing_count} registros "
                    f"distribuidos en {existing_runs} ejecuciones. "
                    "Se requiere revisión manual."
                )

            log.warning(
                "Se reemplazará la carga ACS County existente: "
                "%s registros de una ejecución.",
                existing_count,
            )

            # Eliminar la carga anterior dentro de la transacción.
            conn.execute(text("""
                DELETE FROM public.silver_acs_county_2024
            """))

        # 4. Insertar los registros de la nueva ejecución.
        conn.execute(
            insert,
            [
                {**row, "run_id": run_id}
                for row in rows
            ],
        )

        # 5. Validar los registros realmente persistidos.
        validation = conn.execute(text("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT geography_id) AS total_geographies,
                COUNT(DISTINCT variable_code) AS total_variables,
                COUNT(DISTINCT (geography_id, variable_code))
                    AS unique_combinations
            FROM public.silver_acs_county_2024
            WHERE run_id = :run_id
        """), {"run_id": run_id}).mappings().one()

        persisted = int(validation["total_rows"])

        if (
            persisted != expected_rows
            or validation["total_geographies"] != expected_geographies
            or validation["total_variables"] != expected_variables
            or validation["unique_combinations"] != expected_rows
        ):
            raise ValueError(
                "Validación ACS County fallida: "
                f"registros={persisted}, "
                f"condados={validation['total_geographies']}, "
                f"indicadores={validation['total_variables']}, "
                f"combinaciones únicas={validation['unique_combinations']}."
            )

        # 6. Validar el presupuesto global Silver.
        total = enforce_silver_budget(conn)

        log.info(
            "Presupuesto Silver validado: %s / 100000",
            total,
        )

        log.info(
            "ACS County validado: %s registros, "
            "%s condados, %s indicadores.",
            persisted,
            expected_geographies,
            expected_variables,
        )

    return persisted


def calculate_budget(conn) -> int:
    from .silver_budget import calculate_silver_cells
    return calculate_silver_cells(conn)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ETL ACS 2024 5-Year DP05 para condados del proyecto"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Descarga y valida, sin cargar Silver",
    )

    parser.add_argument(
        "--bronze",
        type=Path,
        help="Lee un archivo JSON Bronze existente",
    )

    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Reemplaza explícitamente los registros ACS County existentes",
    )

    args = parser.parse_args()

    engine = db_engine()
    run_id = None
    source_rows = loaded_rows = 0

    try:
        run_id = start_run(engine)
        geos = target_geographies(engine)

        # Selección de la fuente Bronze.
        if args.bronze:
            # El usuario selecciona expresamente el archivo.
            api_rows, metadata, checksum = read_bronze(args.bronze)

            log.info(
                "Reconstrucción desde Bronze explícito: %s",
                args.bronze,
            )

        else:
            # Buscar automáticamente una única versión Bronze válida.
            selected_bronze = discover_bronze()

            if selected_bronze is not None:
                api_rows, metadata, checksum = read_bronze(
                    selected_bronze
                )

                log.info(
                    "Reconstrucción desde Bronze automático: %s",
                    selected_bronze.name,
                )

            else:
                # Si no existe Bronze válido, utilizar Census API.
                api_key = os.getenv("CENSUS_API_KEY")

                if not api_key:
                    raise RuntimeError(
                        "No existe un archivo Bronze válido en "
                        f"{BRONZE_DIR} y tampoco está configurada "
                        "CENSUS_API_KEY. Descarga el Bronze o configura la API."
                    )

                log.info(
                    "No hay Bronze válido. Se utilizará Census API."
                )

                api_rows, metadata = fetch(api_key)
                checksum = save_bronze(
                    api_rows,
                    metadata,
                    run_id,
                )

        # Normalización y validaciones.
        source_rows = len(api_rows)
        normalized = normalize(api_rows, geos)

        log.info(
            "Fuente=%s filas; proyecto=%s condados; "
            "objetivo=%s registros; SHA256=%s",
            source_rows,
            len(geos),
            len(normalized),
            checksum,
        )

        if args.dry_run:
            finish_run(
            engine,
            run_id,
            "succeeded",
            source_rows,
            0,
            "dry-run: validaciones completadas; Silver no modificada",
        )

            log.info(
                "Dry-run finalizado correctamente. Silver no fue modificada."
            )

            return 0

        
# Carga definitiva en Silver.
        loaded_rows = load_silver(
            engine,
            normalized,
            run_id,
            replace_existing=args.replace_existing,
        )


        finish_run(
            engine,
            run_id,
            "succeeded",
            source_rows,
            loaded_rows,
        )

        log.info(
            "ETL ACS County completado | run_id=%s | cargados=%s",
            run_id,
            loaded_rows,
        )

        return 0

    except Exception as exc:
        log.exception("Falló ETL ACS County")

        if run_id is not None:
            try:
                finish_run(
                    engine,
                    run_id,
                    "failed",
                    source_rows,
                    loaded_rows,
                    str(exc)[:4000],
                )

            except Exception:
                log.exception(
                    "No se pudo actualizar etl_runs"
                )

        return 1

    finally:
        engine.dispose()

if __name__ == "__main__":
    raise SystemExit(main())