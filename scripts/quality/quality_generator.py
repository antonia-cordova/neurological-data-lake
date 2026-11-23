from __future__ import annotations
import argparse, csv, hashlib, json, os, random, sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
import psycopg
from psycopg import sql
from dotenv import load_dotenv
from scripts.database_config import SCENARIOS, get_target_database, resolve_scenario
from scripts.quality.ground_truth import same_exported_value

ROOT = Path(__file__).resolve().parents[2]
EXPECTED = {
("gold","bridge_brfss_geography"),("gold","bridge_health_indicator_source"),
("gold","dim_acs_county_indicator"),("gold","dim_acs_indicator"),
("gold","dim_brfss_jurisdiction"),("gold","dim_brfss_response_code"),
("gold","dim_brfss_variable"),("gold","dim_geography"),
("gold","dim_health_integrated_indicator"),("gold","dim_health_measure"),
("gold","dim_prevalence_type"),("gold","dim_source"),
("gold","fact_acs_county_indicator"),("gold","fact_acs_indicator"),
("gold","fact_area_health"),("gold","fact_brfss_response"),
("gold","fact_health_integrated"),("datalake_control","etl_runs"),
("silver","silver_acs_2024"),("silver","silver_acs_county_2024"),
("silver","silver_brfss_2023"),("silver","silver_places_2023")}

NUMERIC_RULES = [
("gold","fact_area_health","data_value","percent"),
("gold","fact_health_integrated","value_numeric","percent"),
("silver","silver_places_2023","data_value","percent"),
("gold","fact_acs_indicator","estimate_numeric","positive"),
("gold","fact_acs_indicator","percent_numeric","percent"),
("gold","fact_acs_indicator","margin_of_error_numeric","positive"),
("gold","fact_acs_indicator","percent_margin_of_error_numeric","positive"),
("gold","fact_acs_county_indicator","estimate_numeric","positive"),
("gold","fact_acs_county_indicator","percent_numeric","percent"),
("gold","fact_acs_county_indicator","margin_of_error_numeric","positive"),
("gold","fact_acs_county_indicator","percent_margin_of_error_numeric","positive"),
("gold","fact_brfss_response","numeric_value","brfss"),
("gold","dim_geography","latitude","latitude"),
("gold","dim_geography","longitude","longitude"),
("silver","silver_places_2023","latitude","latitude"),
("silver","silver_places_2023","longitude","longitude")]

TEXT_RULES = [
("gold","bridge_health_indicator_source","relation_type",["DIRECT","CONCEPTUAL","PARTIAL"]),
("gold","bridge_health_indicator_source","comparability",["COMPARABLE_WITH_CAVEATS","NOT_DIRECTLY_COMPARABLE","CONTEXTUAL_ONLY"]),
("gold","dim_brfss_variable","value_type",["CATEGORICAL","NUMERIC"]),
("gold","dim_health_integrated_indicator","unit",["PERCENTAGE","PERCENTAGE_OR_DAYS","PERCENTAGE_OR_CATEGORY"])]

NULL_RULES = [
("gold","fact_acs_indicator","estimate_numeric"),
("gold","fact_acs_indicator","margin_of_error_numeric"),
("gold","fact_acs_indicator","percent_numeric"),
("gold","fact_acs_indicator","percent_count_numeric"),
("gold","fact_acs_indicator","percent_margin_of_error_numeric"),
("gold","fact_acs_county_indicator","estimate_numeric"),
("gold","fact_acs_county_indicator","margin_of_error_numeric"),
("gold","fact_acs_county_indicator","percent_numeric"),
("gold","fact_acs_county_indicator","percent_margin_of_error_numeric"),
("gold","fact_brfss_response","numeric_value"),
("gold","fact_brfss_response","raw_value"),
("gold","fact_health_integrated","value_numeric"),
("gold","fact_health_integrated","value_raw"),
("silver","silver_acs_2024","estimate"),
("silver","silver_acs_county_2024","estimate"),
("silver","silver_places_2023","data_value")]

CAESAR_SHIFT = 3
CAESAR_TARGETS = {
    "medium": {
        "gold.dim_health_measure.measure": 5,
        "gold.dim_prevalence_type.data_value_type": 1,
    },
    "poor": {
        "gold.dim_health_measure.measure": 9,
        "gold.dim_prevalence_type.data_value_type": 2,
    },
}
CAESAR_EXPECTED_VALUES = {
    "gold.dim_health_measure.measure": 15,
    "gold.dim_prevalence_type.data_value_type": 2,
}
CAESAR_RULE = "deterministic_caesar_cipher"

PLACES_REFERENCE_YEAR = 2023
REFERENCE_YEAR_MIN = 1900
REFERENCE_YEAR_MAX = 2100

INDICATOR_TO_MEASURE = {
"ARTHRITIS":"ARTHRITIS",
"CORONARY_HEART_DISEASE":"CHD",
"DEPRESSION":"DEPRESSION",
"DIABETES":"DIABETES","GENERAL_HEALTH":"GHLTH","MENTAL_HEALTH":"MHLTH",
"PHYSICAL_HEALTH":"PHLTH","STROKE":"STROKE","MOBILITY":"MOBILITY",
"DISABILITY":"DISABILITY","COGNITION":"COGNITION","HEARING":"HEARING",
"INDEPLIVE":"INDEPLIVE","SELFCARE":"SELFCARE","VISION":"VISION"}

@dataclass
class Change:
    database: str
    level: str
    schema: str
    table: str
    primary_key: dict[str, Any]
    column: str
    original_value: Any
    altered_value: Any
    rule: str
    dimension: str
    seed: int
    changed_at_utc: str
    def row(self):
        d=asdict(self)
        d["primary_key"]=json.dumps(d["primary_key"],default=str,ensure_ascii=False)
        d["original_value"]=None if self.original_value is None else str(self.original_value)
        d["altered_value"]=None if self.altered_value is None else str(self.altered_value)
        return d

def load_config(level):
    p=ROOT/"scripts"/"quality"/"configurations"/f"{level}.json"
    if not p.is_file(): raise FileNotFoundError(f"No existe {p}")
    c=json.loads(p.read_text(encoding="utf-8"))
    expected={"accuracy","completeness","semantic_consistency"}
    if set(c.get("rates",{}))!=expected: raise ValueError(f"{p.name}: rates inválidas")
    if any(not isinstance(v,(int,float)) or not 0<=v<=1 for v in c["rates"].values()):
        raise ValueError(f"{p.name}: las tasas deben estar entre 0 y 1")
    if "seed" not in c: raise ValueError(f"{p.name}: falta seed")
    encryption = c.get("encryption")
    if not isinstance(encryption, dict):
        raise ValueError(f"{p.name}: falta el objeto encryption.")
    if encryption.get("enabled") is not True:
        raise ValueError(f"{p.name}: encryption.enabled debe ser true.")
    targets = encryption.get("targets")
    expected_targets = CAESAR_TARGETS[level]
    if not isinstance(targets, dict) or targets != expected_targets:
        raise ValueError(
            f"{p.name}: encryption.targets debe ser exactamente "
            f"{expected_targets!r}; recibido: {targets!r}."
        )
    if encryption.get("shift") != CAESAR_SHIFT:
        raise ValueError(f"{p.name}: encryption.shift debe ser {CAESAR_SHIFT}.")
    scenario = resolve_scenario()
    if c.get("scenario") != level or c.get("frequency") != SCENARIOS[level]["frequency"]:
        raise RuntimeError(f"{p.name}: escenario/frecuencia no corresponden a {level}.")
    expected_rate = 0.05 if level == "medium" else 0.25
    if any(float(value) != expected_rate for value in c["rates"].values()):
        raise RuntimeError(
            f"{p.name}: las tres dimensiones de {level} deben degradarse "
            f"exactamente en {expected_rate:.0%}."
        )
    if scenario != level:
        raise RuntimeError(
            f"Quality Generator {level} no puede operar en el escenario {scenario}."
        )
    c["database"] = get_target_database()
    c["frequency"] = SCENARIOS[scenario]["frequency"]
    return c

def dbcfg(db):
    load_dotenv(ROOT/".env")
    missing=[x for x in ("POSTGRES_USER","POSTGRES_PASSWORD","POSTGRES_HOST") if not os.getenv(x)]
    if missing: raise RuntimeError("Faltan variables en .env: "+", ".join(missing))
    return dict(host=os.getenv("POSTGRES_HOST"),port=int(os.getenv("POSTGRES_PORT","5432")),
                user=os.getenv("POSTGRES_USER"),password=os.getenv("POSTGRES_PASSWORD"),dbname=db)
def connect(db,autocommit=False): return psycopg.connect(**dbcfg(db),autocommit=autocommit)

def verify_scenario_metadata(c, level, lock=False):
    expected = SCENARIOS[level]
    suffix = " FOR UPDATE" if lock else ""
    row = c.execute(
        "SELECT scenario, frequency, quality_level, quality_status "
        "FROM datalake_control.scenario_metadata WHERE singleton=TRUE" + suffix
    ).fetchone()
    if row is None:
        raise RuntimeError("La base no tiene metadatos de escenario; se rechaza degradarla.")
    scenario, frequency, quality_level, quality_status = row
    if (scenario, frequency, quality_level) != (
        level, expected["frequency"], expected["quality_level"]
    ):
        raise RuntimeError(
            "Los metadatos de la base no corresponden al nivel solicitado: "
            f"{row}."
        )
    if quality_status != "pristine":
        raise RuntimeError(
            f"La base {get_target_database()} ya está marcada como {quality_status}; "
            "no se permite degradarla nuevamente."
        )
    return row
def qt(s,t): return sql.SQL(".").join([sql.Identifier(s),sql.Identifier(t)])
def inventory(c):
    return set(c.execute("""SELECT table_schema,table_name FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema IN ('silver', 'gold', 'datalake_control')""").fetchall())

def colinfo(c,s,t):
    return {r[0]:r[1] for r in c.execute("""SELECT column_name,is_nullable FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s""",(s,t)).fetchall()}

def pkcols(c,s,t):
    return [r[0] for r in c.execute("""SELECT a.attname FROM pg_index i
    JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace
    JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord) ON TRUE
    JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum
    WHERE i.indisprimary AND n.nspname=%s AND t.relname=%s ORDER BY k.ord""",(s,t)).fetchall()]
def protections(c,s,t):
    pk=set(pkcols(c,s,t))
    fk={r[0] for r in c.execute("""SELECT DISTINCT a.attname FROM pg_constraint x
    JOIN pg_class t ON t.oid=x.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
    JOIN LATERAL unnest(x.conkey) k(attnum) ON TRUE
    JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum
    WHERE x.contype='f' AND n.nspname=%s AND t.relname=%s""",(s,t)).fetchall()}
    uq={r[0] for r in c.execute("""SELECT DISTINCT a.attname FROM pg_index i
    JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace
    JOIN LATERAL unnest(i.indkey) k(attnum) ON TRUE
    JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum
    WHERE i.indisunique AND n.nspname=%s AND t.relname=%s AND k.attnum>0""",(s,t)).fetchall()}
    return pk,fk,uq
def rowcount(c,s,t): return c.execute(sql.SQL("SELECT count(*) FROM {}").format(qt(s,t))).fetchone()[0]
def rate_count(n,rate,minimum): return 0 if n==0 or rate==0 else min(n,max(minimum,round(n*rate)))
def dimension(kind):
    if kind=="null": return "completeness"
    if kind in ("percent","positive","brfss","latitude","longitude"): return "accuracy"
    return "semantic_consistency"
def num_new(old,kind,level,rng):
    v=Decimal(str(old))
    if kind in ("latitude","longitude"):
        lim=Decimal(90 if kind=="latitude" else 180)
        shift=Decimal(str(rng.uniform(.1,1 if level=="medium" else 8)))*rng.choice([-1,1])
        new=max(-lim,min(lim,v+shift))
    else:
        factor=rng.uniform(.85,1.15) if level=="medium" else rng.uniform(.4,1.6)
        new=v*Decimal(str(factor))
        if kind in ("percent","brfss"): new=max(Decimal(0),min(Decimal(100),new))
        else: new=max(Decimal(0),new)
    new=new.quantize(Decimal(".01"))
    return None if new==v else new
def caesar_encrypt(value: str, shift: int = CAESAR_SHIFT) -> str:
    """Cifra letras ASCII con César; conserva el resto de caracteres."""
    if value is None:
        return None
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

def caesar_decrypt(value: str, shift: int = CAESAR_SHIFT) -> str:
    """Revierte caesar_encrypt usando el desplazamiento inverso."""
    return caesar_encrypt(value, -shift)


def plan_caesar_column(c, cfg, level, reserved, schema_name, table_name, column_name):
    """Planifica cifrado César determinista para una columna configurada."""
    encryption_cfg = cfg["encryption"]
    target_key = f"{schema_name}.{table_name}.{column_name}"
    target_count = encryption_cfg["targets"][target_key]
    shift = encryption_cfg["shift"]
    info = colinfo(c, schema_name, table_name)
    report = dict(
        table=f"{schema_name}.{table_name}", column=column_name,
        dimension="encryption", eligible=0, target=target_count, planned=0,
        status="PLANNED", reason="", shift=shift,
    )

    if column_name not in info:
        report.update(status="COLUMN_MISSING", reason="Columna inexistente")
        return [], report

    pk, fks, uniques = protections(c, schema_name, table_name)
    pk_order = pkcols(c, schema_name, table_name)
    if not pk_order:
        report.update(status="NO_PRIMARY_KEY", reason="Sin PK")
        return [], report
    if column_name in pk or column_name in fks:
        report.update(status="PROTECTED_COLUMN", reason="La columna participa en PK o FK")
        return [], report

    selected = pk_order + [column_name]
    rows = c.execute(sql.SQL("SELECT {} FROM {} WHERE {} IS NOT NULL ORDER BY {}").format(
        sql.SQL(",").join(map(sql.Identifier, selected)),
        qt(schema_name, table_name), sql.Identifier(column_name),
        sql.SQL(",").join(map(sql.Identifier, pk_order)),
    )).fetchall()

    expected_count = CAESAR_EXPECTED_VALUES[target_key]
    if len(rows) != expected_count:
        raise RuntimeError(
            f"Se esperaban {expected_count} valores no nulos en "
            f"{target_key}, pero se encontraron {len(rows)}."
        )

    candidates = []
    for row in rows:
        key_values = tuple(row[:len(pk_order)])
        original = row[len(pk_order)]
        identity = cell_id(schema_name, table_name, pk_order, key_values, column_name)
        if identity in reserved:
            continue
        altered = caesar_encrypt(original, shift)
        if altered == original:
            continue
        key_payload = json.dumps(
            dict(zip(pk_order, key_values)),
            sort_keys=True, ensure_ascii=False, default=str,
        )
        # ranking no depende del nivel. Poor conserva la selección de Medium
        rank = hashlib.sha256(
            ("caesar-selection-v1:" + target_key + ":" + key_payload).encode("utf-8")
        ).hexdigest()
        candidates.append((rank, key_values, original, altered, identity))

    candidates.sort(key=lambda item: item[0])
    if len(candidates) < target_count:
        raise RuntimeError(
            f"No se puede cumplir el objetivo de cifrado para {target_key}: "
            f"se requieren {target_count} valores y solo hay {len(candidates)} elegibles."
        )

    selected_candidates = candidates[:target_count]

    if column_name in uniques:
        selected_originals = {item[2] for item in selected_candidates}
        altered_values = [item[3] for item in selected_candidates]
        unselected_values = [
            row[len(pk_order)] for row in rows
            if row[len(pk_order)] not in selected_originals
        ]
        final_values = unselected_values + altered_values
        if len(final_values) != len(set(final_values)):
            raise RuntimeError(
                f"El cifrado de {target_key} produciría una colisión UNIQUE; "
                "no se planificarán cambios para esta columna."
            )

    changes = []
    for _rank, key_values, original, altered, identity in selected_candidates:
        changes.append(mkchange(
            cfg, level, schema_name, table_name, pk_order, key_values,
            column_name, original, altered, CAESAR_RULE, "encryption",
        ))
        reserved.add(identity)

    report.update(eligible=len(candidates), target=target_count, planned=len(changes))
    return changes, report


def mkchange(cfg,level,s,t,keys,vals,col,old,new,rule,dim):
    return Change(cfg["database"],level,s,t,dict(zip(keys,vals)),col,old,new,rule,dim,
                  int(cfg["seed"]),datetime.now(timezone.utc).isoformat())
def cell_id(s,t,keys,vals,col):
    return (s,t,tuple(vals),col)


def _stable_payload(value):
    """Serialización canónica para identidades deterministas."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))


def stable_rank(seed, namespace, identity):
    """Ranking estable: no depende del nivel ni del orden de consulta SQL."""
    payload = f"quality-selection-v2:{int(seed)}:{namespace}:{_stable_payload(identity)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def stable_rng(seed, namespace, identity):
    """RNG local a una celda/registro; no consume estado de otras reglas."""
    payload = f"quality-transform-v2:{int(seed)}:{namespace}:{_stable_payload(identity)}"
    digest = hashlib.sha256(payload.encode("utf-8")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))

def eligible(c,s,t,col,keys,reserved):
    names=keys+[col]
    rows=c.execute(sql.SQL("SELECT {} FROM {} WHERE {} IS NOT NULL").format(
        sql.SQL(",").join(map(sql.Identifier,names)),qt(s,t),sql.Identifier(col))).fetchall()
    return [r for r in rows if cell_id(s,t,keys,r[:len(keys)],col) not in reserved]


def plan_rule(c, cfg, level, s, t, col, kind, rate, minimum, rng, reserved):
    """
    Asigna propiedad de celdas de forma común a Medium y Poor.

    La selección se calcula siempre sobre la severidad máxima (25 %).
    Cada escenario aplica un prefijo de esa misma selección: Medium 5 %,
    Poor 25 %. Las celdas reservadas para Poor no quedan disponibles para
    que otra regla las reclame en Medium.

    Para reglas numéricas, un candidato solo se admite si la transformación
    produce un valor distinto del original en ambos niveles. 
    """
    name = f"{s}.{t}"
    info = colinfo(c, s, t)

    if col not in info:
        return [], dict(table=name, column=col, dimension=dimension(kind),
                        eligible=0, target=0, planned=0,
                        status="COLUMN_MISSING", reason="Columna inexistente")

    keys, fks, uniques = protections(c, s, t)
    key_order = pkcols(c, s, t)

    if not key_order:
        return [], dict(table=name, column=col, dimension=dimension(kind),
                        eligible=0, target=0, planned=0,
                        status="NO_PRIMARY_KEY", reason="Sin PK")

    if col in keys or col in uniques:
        return [], dict(table=name, column=col, dimension=dimension(kind),
                        eligible=0, target=0, planned=0,
                        status="UNIQUE_PROTECTED", reason="Participa en PK/índice UNIQUE")

    if col in fks:
        return [], dict(table=name, column=col, dimension=dimension(kind),
                        eligible=0, target=0, planned=0,
                        status="FK_PROTECTED", reason="Participa en FK")

    if kind == "null" and info[col] != "YES":
        return [], dict(table=name, column=col, dimension="completeness",
                        eligible=0, target=0, planned=0,
                        status="NOT_NULL_PROTECTED", reason="Columna NOT NULL")

    rows = eligible(c, s, t, col, key_order, reserved)
    n = len(rows)
    target = rate_count(n, rate, minimum)
    max_target = rate_count(n, 0.25, minimum)

    if not target and not max_target:
        return [], dict(table=name, column=col, dimension=dimension(kind),
                        eligible=n, target=0, planned=0,
                        status="NO_ELIGIBLE_ROWS", reason="Sin valores elegibles")

    namespace = f"{s}.{t}.{col}"
    candidates = []
    for row in rows:
        vals = tuple(row[:len(key_order)])
        old = row[len(key_order)]
        identity = dict(zip(key_order, vals))
        rank = stable_rank(cfg["seed"], namespace, identity)

        if kind in ("percent", "positive", "brfss", "latitude", "longitude"):
            # La selección no depende del nivel y debe ser ejecutable en ambos.
            medium_rng = stable_rng(cfg["seed"], f"medium:{namespace}", identity)
            poor_rng = stable_rng(cfg["seed"], f"poor:{namespace}", identity)
            medium_value = num_new(old, kind, "medium", medium_rng)
            poor_value = num_new(old, kind, "poor", poor_rng)
            if medium_value is None or poor_value is None:
                continue
            transformed = medium_value if level == "medium" else poor_value
            rule = f"numeric_{kind}"
        elif kind == "null":
            transformed = None
            rule = "null_injection"
        else:
            opts = [x for x in kind if x is not None and str(x) != str(old)]
            if not opts:
                continue
            # la alternativa se elige de manera idéntica en ambos escenarios
            cell_rng = stable_rng(cfg["seed"], namespace, identity)
            transformed = cell_rng.choice(opts)
            rule = "semantic_category_swap"

        candidates.append((rank, vals, old, transformed, identity, rule))

    candidates.sort(key=lambda item: item[0])
    if len(candidates) < max_target:
        raise RuntimeError(
            f"No se puede mantener la selección anidada para {name}.{col}: "
            f"se requieren {max_target} celdas comunes (Poor), "
            f"pero solo hay {len(candidates)} candidatas válidas en ambos niveles."
        )

    # oor define la propiedad máxima de la regla mientras que medium usa su prefijo
    selected_max = candidates[:max_target]
    selected_current = selected_max[:target]
    changes = []
    for _rank, vals, old, new, identity, rule in selected_current:
        changes.append(mkchange(
            cfg, level, s, t, key_order, vals, col, old, new,
            rule, dimension(kind),
        ))

    for _rank, vals, _old, _new, _identity, _rule in selected_max:
        reserved.add(cell_id(s, t, key_order, vals, col))

    planned = len(changes)
    status = "PLANNED" if planned == target else "PLANNED_PARTIAL"
    reason = "" if status == "PLANNED" else (
        f"Objetivo: {target}; cambios válidos: {planned}; "
        f"candidatos elegibles revisados: {n}"
    )
    return changes, dict(table=name, column=col, dimension=dimension(kind),
                         eligible=n, target=target, planned=planned,
                         status=status, reason=reason)


def plan_places_reference_year(c, cfg, level, rate, minimum, rng, reserved):
    """Planifica años inconsistentes con selección anidada y transformaciones aisladas."""
    rows = c.execute("""
        SELECT f.source_run_id, f.geography_key, f.health_measure_key,
               f.prevalence_type_key, f.reference_year
        FROM gold.fact_area_health f
        JOIN gold.dim_source s ON s.source_key = f.source_key
        WHERE s.source_code = 'PLACES'
          AND f.reference_year = %s
    """, (PLACES_REFERENCE_YEAR,)).fetchall()

    rows = [row for row in rows if (
        "gold", "fact_area_health", (row[0], row[1], row[2], row[3]), "reference_year"
    ) not in reserved]
    target = rate_count(len(rows), rate, minimum)
    rows = sorted(rows, key=lambda row: stable_rank(
        cfg["seed"], "gold.fact_area_health.reference_year",
        {"source_run_id": row[0], "geography_key": row[1],
         "health_measure_key": row[2], "prevalence_type_key": row[3]},
    ))

    valid_years = [year for year in range(REFERENCE_YEAR_MIN, REFERENCE_YEAR_MAX + 1)
                   if year != PLACES_REFERENCE_YEAR]
    changes = []
    for row in rows[:target]:
        run_id, geography_key, measure_key, prevalence_key, old_year = row
        identity = {"source_run_id": run_id, "geography_key": geography_key,
                    "health_measure_key": measure_key, "prevalence_type_key": prevalence_key}
        cell_rng = stable_rng(cfg["seed"], "reference_year", identity)
        new_year = cell_rng.choice(valid_years)
        changes.append(mkchange(
            cfg, level, "gold", "fact_area_health",
            ["source_run_id", "geography_key", "health_measure_key", "prevalence_type_key"],
            [run_id, geography_key, measure_key, prevalence_key],
            "reference_year", old_year, new_year,
            "contextual_reference_year_mismatch", "semantic_consistency",
        ))
        reserved.add(("gold", "fact_area_health",
                      (run_id, geography_key, measure_key, prevalence_key), "reference_year"))

    planned = len(changes)
    status = "PLANNED" if planned == target else "PLANNED_PARTIAL"
    reason = "" if status == "PLANNED" else (
        f"Objetivo: {target}; cambios válidos: {planned}; registros PLACES elegibles: {len(rows)}"
    )
    return changes, dict(table="gold.fact_area_health", column="reference_year",
                         dimension="semantic_consistency", eligible=len(rows),
                         target=target, planned=planned, status=status, reason=reason)


def plan_places_health_measure_semantics(c, cfg, level, rate, minimum, rng, reserved):
    """Planifica parejas recíprocas con orden estable y prefijo anidado."""
    rows = c.execute("""
        SELECT f.source_run_id, f.geography_key, f.health_measure_key,
               f.prevalence_type_key
        FROM gold.fact_area_health f
        JOIN gold.dim_source s ON s.source_key = f.source_key
        WHERE s.source_code = 'PLACES'
          AND f.health_measure_key IS NOT NULL
        ORDER BY f.source_run_id, f.geography_key,
                 f.prevalence_type_key, f.health_measure_key
    """).fetchall()
    measures = c.execute("""
        SELECT health_measure_key FROM gold.dim_health_measure
        ORDER BY health_measure_key
    """).fetchall()
    valid_key_set = {row[0] for row in measures}
    invalid_existing = sorted({row[2] for row in rows if row[2] not in valid_key_set})
    if invalid_existing:
        raise RuntimeError(
            "fact_area_health contiene health_measure_key fuera del diccionario: "
            f"{invalid_existing}"
        )

    groups = {}
    for run_id, geography_key, measure_key, prevalence_key in rows:
        pk = (run_id, geography_key, measure_key, prevalence_key)
        if ("gold", "fact_area_health", pk, "health_measure_key") in reserved:
            continue
        groups.setdefault((run_id, geography_key, prevalence_key), []).append(measure_key)

    eligible_rows = sum(len(keys) for keys in groups.values())
    target = rate_count(eligible_rows, rate, minimum)
    if target % 2:
        target += 1
    target = min(target, eligible_rows - (eligible_rows % 2))

    # construir un matching determinista dentro de cada grupo. El ranking de
    # parejas es independiente del nivel. Poor amplía el prefijo de parejas.
    pair_candidates = []
    for group, measure_keys in groups.items():
        run_id, geography_key, prevalence_key = group
        ordered_keys = sorted(
            measure_keys,
            key=lambda key: stable_rank(
                cfg["seed"], "places-pair-key",
                {"source_run_id": run_id, "geography_key": geography_key,
                 "prevalence_type_key": prevalence_key, "health_measure_key": key},
            ),
        )
        for index in range(0, len(ordered_keys) - 1, 2):
            key_a, key_b = ordered_keys[index:index + 2]
            identity = {"source_run_id": run_id, "geography_key": geography_key,
                        "prevalence_type_key": prevalence_key,
                        "measure_keys": sorted((key_a, key_b))}
            rank = stable_rank(cfg["seed"], "places-pair", identity)
            pair_candidates.append((rank, run_id, geography_key, prevalence_key, key_a, key_b))

    pair_candidates.sort(key=lambda item: item[0])
    target_pairs = target // 2
    selected_pairs = pair_candidates[:target_pairs]
    changes = []
    for _rank, run_id, geography_key, prevalence_key, key_a, key_b in selected_pairs:
        pk_a = (run_id, geography_key, key_a, prevalence_key)
        pk_b = (run_id, geography_key, key_b, prevalence_key)
        changes.append(mkchange(
            cfg, level, "gold", "fact_area_health",
            ["source_run_id", "geography_key", "health_measure_key", "prevalence_type_key"],
            list(pk_a), "health_measure_key", key_a, key_b,
            "fk_valid_semantic_measure_pair_swap", "semantic_consistency",
        ))
        changes.append(mkchange(
            cfg, level, "gold", "fact_area_health",
            ["source_run_id", "geography_key", "health_measure_key", "prevalence_type_key"],
            list(pk_b), "health_measure_key", key_b, key_a,
            "fk_valid_semantic_measure_pair_swap", "semantic_consistency",
        ))
        reserved.add(("gold", "fact_area_health", pk_a, "health_measure_key"))
        reserved.add(("gold", "fact_area_health", pk_b, "health_measure_key"))

    planned = len(changes)
    status = "PLANNED" if planned == target else "PLANNED_PARTIAL"
    reason = "" if status == "PLANNED" else (
        f"Objetivo: {target} asociaciones ({target // 2} pares); "
        f"planificadas: {planned} asociaciones ({planned // 2} pares); "
        "no hubo suficientes parejas disjuntas elegibles"
    )
    return changes, dict(table="gold.fact_area_health", column="health_measure_key",
                         dimension="semantic_consistency", eligible=eligible_rows,
                         target=target, planned=planned, pairs_planned=planned // 2,
                         status=status, reason=reason)


def plan_fk_semantics(c, cfg, level, rate, minimum, rng, reserved):
    rows = c.execute("""SELECT f.integrated_fact_key,f.source_run_id,f.geography_key,
    f.indicator_key,f.health_measure_key,f.prevalence_type_key,i.indicator_code,m.measure_id
    FROM gold.fact_health_integrated f
    JOIN gold.dim_health_integrated_indicator i ON i.indicator_key=f.indicator_key
    JOIN gold.dim_health_measure m ON m.health_measure_key=f.health_measure_key
    WHERE f.health_measure_key IS NOT NULL""").fetchall()
    rows = [r for r in rows if (
        "gold", "fact_health_integrated", (r[0],), "health_measure_key"
    ) not in reserved]
    target = rate_count(len(rows), rate, minimum)
    rows.sort(key=lambda r: stable_rank(
        cfg["seed"], "gold.fact_health_integrated.health_measure_key",
        {"integrated_fact_key": r[0]},
    ))

    occupied = {tuple(r) for r in c.execute("""
        SELECT source_run_id,geography_key,indicator_key,health_measure_key,prevalence_type_key
        FROM gold.fact_health_integrated WHERE health_measure_key IS NOT NULL
          AND source_run_id IS NOT NULL AND geography_key IS NOT NULL
          AND indicator_key IS NOT NULL AND prevalence_type_key IS NOT NULL
    """).fetchall()}
    measures = c.execute("SELECT health_measure_key,measure_id FROM gold.dim_health_measure").fetchall()
    changes = []

    for r in rows:
        if len(changes) >= target:
            break
        fact, run_id, geo, indicator, old, prevalence, code, measure_id = r
        if INDICATOR_TO_MEASURE.get(code) != measure_id:
            continue

        identity = {"integrated_fact_key": fact}
        options = list(measures)
        options.sort(key=lambda item: stable_rank(
            cfg["seed"], "fk-semantic-option",
            {"integrated_fact_key": fact, "health_measure_key": item[0]},
        ))
        # El orden estable de opciones evita que el orden de consulta o el
        # escenario cambien la alternativa de un registro común.
        selected = None
        for new_key, new_measure_id in options:
            if new_key == old:
                continue
            candidate = (run_id, geo, indicator, new_key, prevalence)
            if candidate not in occupied:
                selected = (new_key, candidate)
                break
        if selected is None:
            continue

        new_key, new_tuple = selected
        occupied.discard((run_id, geo, indicator, old, prevalence))
        occupied.add(new_tuple)
        changes.append(mkchange(
            cfg, level, "gold", "fact_health_integrated", ["integrated_fact_key"],
            [fact], "health_measure_key", old, new_key,
            "fk_valid_semantic_association_swap", "semantic_consistency",
        ))
        reserved.add(("gold", "fact_health_integrated", (fact,), "health_measure_key"))

    status = "PLANNED" if len(changes) == target else "PLANNED_PARTIAL"
    return changes, dict(table="gold.fact_health_integrated", column="health_measure_key",
                         dimension="semantic_consistency", eligible=len(rows),
                         target=target, planned=len(changes), status=status,
                         reason="" if status == "PLANNED" else
                         "Alternativas insuficientes sin colisión UNIQUE")


def build_plan(c,level,cfg):
    rng = random.Random(int(cfg["seed"]))  # Compatibilidad de firma; selección usa stable_rank().
    rates=cfg["rates"]; minimum=int(cfg.get("minimum_changes",{}).get("minimum_per_nonempty_table",1)) if cfg.get("minimum_changes",{}).get("enabled",True) else 0
    changes=[]; reports=[]; reserved=set()
    for s,t,col,kind in NUMERIC_RULES:
        a,b=plan_rule(c,cfg,level,s,t,col,kind,rates["accuracy"],minimum,rng,reserved); changes+=a; reports.append(b)
    for s,t,col,allowed in TEXT_RULES:
        a,b=plan_rule(c,cfg,level,s,t,col,allowed,rates["semantic_consistency"],minimum,rng,reserved); changes+=a; reports.append(b)
    a,b=plan_fk_semantics(c,cfg,level,rates["semantic_consistency"],minimum,rng,reserved); changes+=a; reports.append(b)

    # flujos RNG independientes
    year_rng = random.Random(int(cfg["seed"]) + 101)
    measure_rng = random.Random(int(cfg["seed"]) + 202)

    a,b=plan_places_reference_year(
        c,cfg,level,rates["semantic_consistency"],minimum,year_rng,reserved
    ); changes+=a; reports.append(b)
    a,b=plan_places_health_measure_semantics(
        c,cfg,level,rates["semantic_consistency"],minimum,measure_rng,reserved
    ); changes+=a; reports.append(b)

    # cifrado determinista 
    for schema_name, table_name, column_name in (
        ("gold", "dim_health_measure", "measure"),
        ("gold", "dim_prevalence_type", "data_value_type"),
    ):
        a, b = plan_caesar_column(
            c, cfg, level, reserved, schema_name, table_name, column_name
        )
        changes += a
        reports.append(b)

    for s,t,col in NULL_RULES:
        a,b=plan_rule(c,cfg,level,s,t,col,"null",rates["completeness"],minimum,rng,reserved); changes+=a; reports.append(b)
    ids=[cell_id(x.schema,x.table,list(x.primary_key),[x.primary_key[k] for k in x.primary_key],x.column) for x in changes]
    if len(ids)!=len(set(ids)):
        raise RuntimeError("El plan contiene alteraciones duplicadas sobre una misma celda.")
    return changes,reports

def write_reports(level,cfg,changes,reports,counts):
    out=ROOT/"quality"/"reports"; out.mkdir(parents=True,exist_ok=True)
    suffix = "_test" if cfg["database"].endswith("_test") else ""
    rulepath=out/f"quality_dry_run_rules_{level}{suffix}.csv"
    fields=["table","column","dimension","eligible","target","planned","pairs_planned","status","reason","shift"]
    with rulepath.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
        for r in reports: w.writerow({k:r.get(k,"") for k in fields})
    changepath=out/f"quality_dry_run_changes_{level}{suffix}.csv"
    fields2=["database","level","schema","table","primary_key","column","original_value","altered_value","rule","dimension","seed","changed_at_utc"]
    with changepath.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=fields2); w.writeheader()
        for x in changes:
            d=x.row(); w.writerow({k:d.get(k,"") for k in fields2})
    summary=out/f"quality_dry_run_summary_{level}{suffix}.csv"
    with summary.open("w",newline="",encoding="utf-8-sig") as f:
        fields3=["level","database","rate_accuracy","rate_completeness","rate_semantic_consistency","tables_in_scope","source_rows_total","planned_operations","unique_cells","planned_changes_total","dimension","rules","eligible_cells","planned_changes","partial_or_skipped_rules"]
        w=csv.DictWriter(f,fieldnames=fields3); w.writeheader()
        for dim in ("accuracy","completeness","semantic_consistency","encryption"):
            subset=[r for r in reports if r["dimension"]==dim]
            w.writerow(dict(level=level,database=cfg["database"],rate_accuracy=cfg["rates"]["accuracy"],
                rate_completeness=cfg["rates"]["completeness"],rate_semantic_consistency=cfg["rates"]["semantic_consistency"],
                tables_in_scope=len(counts),source_rows_total=sum(counts.values()),
                planned_operations=len(changes),unique_cells=len({(x.schema,x.table,tuple(x.primary_key.items()),x.column) for x in changes}),
                planned_changes_total=len(changes),
                dimension=dim,rules=len(subset),eligible_cells=sum(r["eligible"] for r in subset),
                planned_changes=sum(r["planned"] for r in subset),
                partial_or_skipped_rules=sum(r["status"]!="PLANNED" for r in subset)))
    return rulepath,changepath,summary

def _where_pk(keys, vals):
    return sql.SQL(" AND ").join(
        sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(k))
        for k in keys
    )


def _apply_fact_area_health_pair(c, pair):
    """Aplica un par con delete/insert transaccional, sin desactivar constraints."""
    if len(pair) != 2:
        raise RuntimeError("Un pair-swap debe contener exactamente dos cambios.")
    a, b = pair
    if a.schema != "gold" or a.table != "fact_area_health" or a.column != "health_measure_key":
        raise RuntimeError("Pair-swap recibido para una tabla/columna no permitida.")
    keys_a, keys_b = list(a.primary_key), list(b.primary_key)
    if keys_a != keys_b or set(keys_a) != {
        "source_run_id", "geography_key", "health_measure_key", "prevalence_type_key"
    }:
        raise RuntimeError("La PK del pair-swap no coincide con la PK compuesta esperada.")
    if (a.original_value != a.primary_key["health_measure_key"] or
        b.original_value != b.primary_key["health_measure_key"] or
        a.altered_value != b.original_value or b.altered_value != a.original_value):
        raise RuntimeError("El pair-swap no es recíproco o sus valores originales no coinciden.")
    group_a = (a.primary_key["source_run_id"], a.primary_key["geography_key"], a.primary_key["prevalence_type_key"])
    group_b = (b.primary_key["source_run_id"], b.primary_key["geography_key"], b.primary_key["prevalence_type_key"])
    if group_a != group_b:
        raise RuntimeError("Las dos filas del pair-swap no pertenecen al mismo grupo.")

    table = qt("gold", "fact_area_health")
    fetched = []
    for change in pair:
        keys = list(change.primary_key)
        vals = tuple(change.primary_key[k] for k in keys)
        query = sql.SQL("SELECT * FROM {} WHERE {} FOR UPDATE").format(
            table, _where_pk(keys, vals)
        )
        cur = c.execute(query, vals)
        rows = cur.fetchall()
        if len(rows) != 1:
            raise RuntimeError(f"Pair-swap: se esperaba una fila, se encontraron {len(rows)}.")
        column_names = [d.name for d in cur.description]
        record = dict(zip(column_names, rows[0]))
        if record.get("health_measure_key") != change.original_value:
            raise RuntimeError("Pair-swap: el valor original cambió desde la planificación.")
        fetched.append((change, record))

    # no se omiten ni deshabilitan FK/CHECK/UNIQUE/NOT NULL. Si existen FK
    # entrantes restrictivas, la eliminación falla y la transacción revierte
    for change, _record in fetched:
        keys = list(change.primary_key)
        vals = tuple(change.primary_key[k] for k in keys)
        result = c.execute(
            sql.SQL("DELETE FROM {} WHERE {}").format(table, _where_pk(keys, vals)), vals
        )
        if result.rowcount != 1:
            raise RuntimeError("Pair-swap: no se pudo retirar exactamente una fila.")

    for change, record in fetched:
        record["health_measure_key"] = change.altered_value
        generated = c.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='gold' AND table_name='fact_area_health'
              AND is_generated='ALWAYS'
        """).fetchall()
        generated_names = {r[0] for r in generated}
        insert_cols = [name for name in record if name not in generated_names]
        identity_always = c.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='gold' AND table_name='fact_area_health'
              AND is_identity='YES' AND identity_generation='ALWAYS'
        """).fetchall()
        identity_names = {r[0] for r in identity_always}
        prefix = sql.SQL(" OVERRIDING SYSTEM VALUE") if identity_names.intersection(insert_cols) else sql.SQL("")
        query = sql.SQL("INSERT INTO {} ({}){} VALUES ({})").format(
            table,
            sql.SQL(",").join(map(sql.Identifier, insert_cols)),
            prefix,
            sql.SQL(",").join(sql.Placeholder() for _ in insert_cols)
        )
        c.execute(query, [record[name] for name in insert_cols])


def apply_changes(c, changes):
    """Aplica cambios dentro de la transacción activa; pair-swaps como unidad."""
    pair_groups = {}
    ordinary = []
    for change in changes:
        if change.rule == "fk_valid_semantic_measure_pair_swap":
            key = (change.schema, change.table, change.rule,
                   change.primary_key["source_run_id"],
                   change.primary_key["geography_key"],
                   change.primary_key["prevalence_type_key"],
                   tuple(sorted((change.original_value, change.altered_value))))
            pair_groups.setdefault(key, []).append(change)
        else:
            ordinary.append(change)

    for x in ordinary:
        keys = list(x.primary_key)
        vals = tuple(x.primary_key[k] for k in keys)
        where = sql.SQL(" AND ").join([
            _where_pk(keys, vals),
            sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(x.column))
        ])
        result = c.execute(
            sql.SQL("UPDATE {} SET {}=%s WHERE {}").format(
                qt(x.schema, x.table), sql.Identifier(x.column), where
            ),
            [x.altered_value, *vals, x.original_value]
        )
        if result.rowcount != 1:
            raise RuntimeError(
                f"UPDATE {x.schema}.{x.table}.{x.column}: fila inexistente, "
                f"valor original distinto o PK no única (rowcount={result.rowcount}, esperado=1)"
            )

    # se ejecutan los pair-swaps al final, después de modificar las demás columnas
    # identificadas mediante la PK original. Cada pareja sigue siendo atómica
    # dentro de la transacción global y conserva sus restricciones
    for pair in pair_groups.values():
        _apply_fact_area_health_pair(c, pair)


def constraint_report(c):
    """Devuelve las restricciones declarativas de silver y gold."""
    return c.execute("""
        SELECT n.nspname,t.relname,con.conname,con.contype,
               pg_get_constraintdef(con.oid,true),con.convalidated
        FROM pg_constraint con
        JOIN pg_class t ON t.oid=con.conrelid
        JOIN pg_namespace n ON n.oid=t.relnamespace
        WHERE n.nspname IN ('silver', 'gold', 'datalake_control')
        ORDER BY n.nspname,t.relname,con.contype,con.conname
    """).fetchall()

def validate_constraints(c):
    # las restricciones inmediatas se comprueban en cada UPDATE
    c.execute("SET CONSTRAINTS ALL IMMEDIATE")
    constraints=constraint_report(c)
    unvalidated=[r for r in constraints if not r[5]]
    if unvalidated:
        raise RuntimeError(f"Restricciones no validadas: {unvalidated}")
    not_null=c.execute("""
        SELECT table_schema,table_name,column_name
        FROM information_schema.columns
        WHERE table_schema IN ('silver', 'gold', 'datalake_control')
        AND is_nullable = 'NO'
        ORDER BY table_schema,table_name,ordinal_position
    """).fetchall()
    return len(constraints),len(not_null)

def preflight_changes(source,changes):
    """Ejecuta los UPDATE reales sobre la fuente y revierte siempre."""
    with connect(source) as c:
        try:
            apply_changes(c,changes)
            constraint_count,not_null_count=validate_constraints(c)
        except Exception:
            c.rollback()
            raise
        else:
            c.rollback()
    return constraint_count,not_null_count

def recover_ground_truth(level):
    """
    Reconstruye el JSONL desde el CSV previo a la transacción.

    Solo exporta si la base ya está marcada como degraded y cada valor actual
    coincide con el valor alterado registrado en el CSV. No ejecuta UPDATE.

    Las PK originales de las filas afectadas por Pair Swap se traducen a sus
    PK finales antes de verificar cualquier celda. Esto permite verificar
    también los cambios ordinarios aplicados a esas mismas filas.
    """
    cfg = load_config(level)
    target = cfg["database"]
    suffix = "_test" if target.endswith("_test") else ""
    report = ROOT / "quality" / "reports" / f"quality_dry_run_changes_{level}{suffix}.csv"
    output = ROOT / "quality" / "ground_truth" / f"ground_truth_{level}{suffix}.jsonl"

    if not report.is_file():
        raise FileNotFoundError(
            "No se puede recuperar Ground Truth: falta el informe de cambios "
            f"previo a la degradación ({report})."
        )

    summary_path = ROOT / "quality" / "reports" / f"quality_dry_run_summary_{level}{suffix}.csv"
    if not summary_path.is_file():
        raise FileNotFoundError(
            "No se puede verificar la integridad del Ground Truth: "
            f"falta el resumen de cambios ({summary_path})."
        )

    with summary_path.open("r", newline="", encoding="utf-8-sig") as stream:
        summary_rows = list(csv.DictReader(stream))
    if not summary_rows:
        raise RuntimeError("El resumen de cambios está vacío.")
    try:
        expected_change_count = int(summary_rows[0]["planned_changes_total"])
        expected_unique_cells = int(summary_rows[0]["unique_cells"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("El resumen de cambios no contiene conteos válidos.") from exc

    with report.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        required = {
            "database", "level", "schema", "table", "primary_key",
            "column", "original_value", "altered_value", "rule",
            "dimension", "seed", "changed_at_utc",
        }
        if not required.issubset(set(reader.fieldnames or [])):
            raise RuntimeError(
                f"El CSV Ground Truth no contiene las columnas requeridas: "
                f"{sorted(required - set(reader.fieldnames or []))}."
            )
        report_rows = list(reader)

    
    pair_key_map = {}
    pair_destinations = set()
    for line_number, row in enumerate(report_rows, start=2):
        if row["rule"] != "fk_valid_semantic_measure_pair_swap":
            continue
        try:
            original_pk = json.loads(row["primary_key"])
            altered_key = int(row["altered_value"])
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Registro Pair Swap inválido en CSV, línea {line_number}."
            ) from exc
        if (
            row["schema"] != "gold"
            or row["table"] != "fact_area_health"
            or row["column"] != "health_measure_key"
            or not isinstance(original_pk, dict)
            or original_pk.get("health_measure_key") is None
        ):
            raise RuntimeError(
                f"Pair Swap fuera de la tabla/columna permitida, línea {line_number}."
            )
        try:
            original_key = int(original_pk["health_measure_key"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Clave original inválida para Pair Swap, línea {line_number}."
            ) from exc
        if original_key != int(row["original_value"]):
            raise RuntimeError(
                f"El valor original del Pair Swap no coincide con su PK, línea {line_number}."
            )

        final_pk = dict(original_pk)
        final_pk["health_measure_key"] = altered_key
        identity = (
            row["schema"], row["table"],
            json.dumps(original_pk, sort_keys=True, default=str),
        )
        destination_identity = (
            row["schema"], row["table"],
            json.dumps(final_pk, sort_keys=True, default=str),
        )
        if identity in pair_key_map and pair_key_map[identity] != final_pk:
            raise RuntimeError(
                f"Una PK original tiene destinos Pair Swap contradictorios, línea {line_number}."
            )
        if destination_identity in pair_destinations and pair_key_map.get(identity) != final_pk:
            raise RuntimeError(
                f"Dos filas Pair Swap apuntan a la misma PK final, línea {line_number}."
            )
        pair_key_map[identity] = final_pk
        pair_destinations.add(destination_identity)

    with connect(target) as conn:
        metadata = conn.execute(
            """
            SELECT scenario, frequency, quality_level, quality_status
            FROM datalake_control.scenario_metadata
            WHERE singleton=TRUE
            """
        ).fetchone()
        if metadata is None or metadata != (
            level, SCENARIOS[level]["frequency"], level, "degraded"
        ):
            raise RuntimeError(
                "La base no está marcada como degradada para el escenario solicitado; "
                f"metadatos={metadata!r}."
            )

        exported = []
        seen = set()
        for line_number, row in enumerate(report_rows, start=2):
            if row["database"] != target or row["level"] != level:
                raise RuntimeError(
                    f"El CSV no corresponde a {target}/{level}, línea {line_number}."
                )
            schema_name = row["schema"]
            table_name = row["table"]
            column_name = row["column"]
            if schema_name not in {"silver", "gold", "datalake_control"}:
                raise RuntimeError(
                    f"Esquema no permitido en CSV, línea {line_number}: {schema_name!r}."
                )
            try:
                primary_key = json.loads(row["primary_key"])
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Clave primaria inválida en CSV, línea {line_number}."
                ) from exc
            if not isinstance(primary_key, dict) or not primary_key:
                raise RuntimeError(
                    f"Clave primaria vacía o inválida en CSV, línea {line_number}."
                )

            identity = (
                schema_name, table_name,
                json.dumps(primary_key, sort_keys=True, default=str),
                column_name,
            )
            if identity in seen:
                raise RuntimeError(
                    f"Cambio duplicado en CSV, línea {line_number}: {identity}."
                )
            seen.add(identity)

            row_identity = (
                schema_name, table_name,
                json.dumps(primary_key, sort_keys=True, default=str),
            )
            final_pk = pair_key_map.get(row_identity, primary_key)

            where = sql.SQL(" AND ").join(
                sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(key))
                for key in final_pk
            )
            query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
                sql.Identifier(column_name),
                qt(schema_name, table_name),
                where,
            )
            matches = conn.execute(query, list(final_pk.values())).fetchall()
            if len(matches) != 1:
                raise RuntimeError(
                    f"No se pudo identificar una única celda para Ground Truth, "
                    f"línea {line_number}: filas={len(matches)}."
                )
            if not same_exported_value(matches[0][0], row["altered_value"]):
                raise RuntimeError(
                    f"El valor actual no coincide con el valor alterado del CSV, "
                    f"línea {line_number}: {schema_name}.{table_name}.{column_name}."
                )

            try:
                seed = int(row["seed"])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Seed inválida en CSV, línea {line_number}."
                ) from exc

            exported.append({
                "database": row["database"],
                "level": row["level"],
                "schema": schema_name,
                "table": table_name,
                "primary_key": row["primary_key"],
                "column": column_name,
                "original_value": row["original_value"] or None,
                "altered_value": row["altered_value"] or None,
                "rule": row["rule"],
                "dimension": row["dimension"],
                "seed": seed,
                "changed_at_utc": row["changed_at_utc"],
            })

    if not exported:
        raise RuntimeError("El CSV de cambios está vacío; no se puede recuperar Ground Truth.")
    if len(exported) != expected_change_count or len(seen) != expected_unique_cells:
        raise RuntimeError(
            "El CSV de cambios no coincide con el resumen previo a la degradación: "
            f"cambios={len(exported)}/{expected_change_count}, "
            f"celdas_únicas={len(seen)}/{expected_unique_cells}."
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as stream:
        for row in exported:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(
        f"GROUND TRUTH RECUPERADO: {output.relative_to(ROOT)} | "
        f"cambios verificados={len(exported):,} | base={target}. "
        "No se modificaron datos en PostgreSQL."
    )



def _change_identity(change):
    return (
        change.schema,
        change.table,
        tuple(change.primary_key.items()),
        change.column,
    )


def validate_plan_against_target(target_conn, changes):
    """
    Conserva la propiedad de regla definida en el universo canónico y valida
    qué operaciones pueden aplicarse sobre la base objetivo.

    Las operaciones ausentes o cuyo valor original no coincide se omiten,
    nunca se reasignan a otra regla. Los pair-swaps se conservan únicamente
    cuando las dos filas del par están disponibles y coinciden.
    """
    pair_groups = {}
    ordinary = []
    for change in changes:
        if change.rule == "fk_valid_semantic_measure_pair_swap":
            pair_key = (
                change.schema,
                change.table,
                change.rule,
                change.primary_key["source_run_id"],
                change.primary_key["geography_key"],
                change.primary_key["prevalence_type_key"],
                tuple(sorted((change.original_value, change.altered_value))),
            )
            pair_groups.setdefault(pair_key, []).append(change)
        else:
            ordinary.append(change)

    accepted = []
    skipped_by_rule = {}

    def target_value_matches(change):
        keys = list(change.primary_key)
        values = [change.primary_key[k] for k in keys]
        where = sql.SQL(" AND ").join(
            sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(k))
            for k in keys
        )
        query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
            sql.Identifier(change.column),
            qt(change.schema, change.table),
            where,
        )
        rows = target_conn.execute(query, values).fetchall()
        if len(rows) != 1:
            return False, f"fila_destino={len(rows)}"
        if not same_exported_value(rows[0][0], change.original_value):
            return False, "valor_original_distinto"
        return True, ""

    for change in ordinary:
        valid, reason = target_value_matches(change)
        if valid:
            accepted.append(change)
        else:
            skipped_by_rule[change.rule] = skipped_by_rule.get(change.rule, 0) + 1

    for _key, pair in pair_groups.items():
        valid_pair = len(pair) == 2
        if valid_pair:
            checks = [target_value_matches(item) for item in pair]
            valid_pair = all(item[0] for item in checks)
        if valid_pair:
            accepted.extend(pair)
        else:
            skipped_by_rule["fk_valid_semantic_measure_pair_swap"] = (
                skipped_by_rule.get("fk_valid_semantic_measure_pair_swap", 0)
                + len(pair)
            )

    return accepted, skipped_by_rule


def update_reports_for_target(reports, accepted_changes, skipped_by_rule):
    """Actualiza los totales del plan sin cambiar la propiedad de las celdas."""
    accepted_by_rule = {}
    for change in accepted_changes:
        accepted_by_rule[change.rule] = accepted_by_rule.get(change.rule, 0) + 1

    for report in reports:
        original_planned = report.get("planned", 0)
        # cada regla puede aparecer en más de una tabla, el vínculo se realiza
        # mediante tabla/columna y reglas presentes en las operaciones aceptadas
        table = report.get("table")
        column = report.get("column")
        matching = [
            change for change in accepted_changes
            if f"{change.schema}.{change.table}" == table
            and change.column == column
            and change.dimension == report.get("dimension")
        ]
        report["planned"] = len(matching)
        if report["planned"] < original_planned:
            report["status"] = "PLANNED_PARTIAL"
            report["reason"] = (
                (report.get("reason", "") + " | ") if report.get("reason") else ""
            ) + (
                f"Universo canónico: {original_planned}; "
                f"disponibles en base objetivo: {report['planned']}. "
                "No se reasignaron celdas."
            )


def run(level, execute, preflight_only=False):
    cfg=load_config(level)
    target=cfg["database"]
    reference_db = os.getenv("DATALAKE_REFERENCE_DB", "health_datalake")
    if reference_db != "health_datalake":
        raise RuntimeError(
            "El universo canónico debe ser health_datalake (Perfect). "
            f"DATALAKE_REFERENCE_DB recibido: {reference_db!r}."
        )

    with connect(target) as target_conn:
        verify_scenario_metadata(target_conn, level)
        inv = inventory(target_conn)
        missing = EXPECTED - inv
        if missing:
            raise RuntimeError(f"Faltan tablas en la base objetivo: {sorted(missing)}")
        counts = {f"{s}.{t}": rowcount(target_conn, s, t)
                  for s, t in sorted(EXPECTED)}

        # la selección completa se construye siempre desde Perfect 
        with connect(reference_db) as reference_conn:
            verify_scenario_metadata(reference_conn, "perfect")
            reference_inventory = inventory(reference_conn)
            missing_reference = EXPECTED - reference_inventory
            if missing_reference:
                raise RuntimeError(
                    "Faltan tablas en el universo canónico Perfect: "
                    f"{sorted(missing_reference)}"
                )
            changes, reports = build_plan(reference_conn, level, cfg)

    
        canonical_planned = len(changes)
        changes, unavailable = validate_plan_against_target(target_conn, changes)
        update_reports_for_target(reports, changes, unavailable)
        if unavailable:
            print(
                "OPERACIONES NO DISPONIBLES EN LA BASE OBJETIVO "
                "(sin reasignación): "
                + ", ".join(f"{rule}={count}" for rule, count in sorted(unavailable.items()))
            )

    print(f"\n{'='*72}\n{level.upper()} | base={target} | frecuencia={cfg['frequency']}")
    print(f"Tasas: accuracy {cfg['rates']['accuracy']:.0%} | completeness {cfg['rates']['completeness']:.0%} | semantic consistency {cfg['rates']['semantic_consistency']:.0%}")
    print(f"Universo de selección: {reference_db} (Perfect)")
    print(f"Tablas objetivo: {len(inv)} | Filas objetivo: {sum(counts.values()):,}")
    print(f"Operaciones canónicas seleccionadas: {canonical_planned:,}")
    print(f"Cifrado César: +{cfg['encryption']['shift']}")
    for target_key, target_count in cfg["encryption"]["targets"].items():
        print(f"  {target_key}: objetivo={target_count}")
    print(f"{'DIMENSIÓN':24} {'REGLAS':>7} {'ELEGIBLES':>12} {'CAMBIOS':>10} {'OMITIDAS/PARCIALES':>20}")
    for dim in ("accuracy","completeness","semantic_consistency","encryption"):
        subset=[r for r in reports if r["dimension"]==dim]
        print(f"{dim:24} {len(subset):7d} {sum(r['eligible'] for r in subset):12,d} {sum(r['planned'] for r in subset):10,d} {sum(r['status']!='PLANNED' for r in subset):20d}")
    unique_cells=len({(x.schema,x.table,tuple(x.primary_key.items()),x.column) for x in changes})
    print(f"OPERACIONES PLANIFICADAS: {len(changes):,}")
    print(f"CELDAS ÚNICAS A ALTERAR: {unique_cells:,}")
    for r in reports:
        if r.get("pairs_planned") is not None:
            print(f"  PAIR-SWAP {r['table']}.{r['column']}: {r['pairs_planned']} pares / {r['planned']} asociaciones")
        if r["status"]!="PLANNED": print(f"  {r['status']}: {r['table']}.{r['column']} — {r['reason']}")
    paths=write_reports(level,cfg,changes,reports,counts)
    print("Informes:")
    for p in paths: print(f"  {p.relative_to(ROOT)}")
    
    if not execute and not preflight_only:
        print("DRY RUN: solo lectura. La base permanece intacta.")
        return

    if not changes:
        raise RuntimeError(
            "El plan no contiene cambios; se cancela la ejecución."
        )

    # preflight sobre la misma base objetivo. Se revierte.
    constraint_count, not_null_count = preflight_changes(target, changes)

    print(
        f"PREFLIGHT APROBADO: {len(changes):,} operaciones; "
        f"{constraint_count} constraints; "
        f"{not_null_count} columnas NOT NULL. "
        "Transacción de prueba revertida."
    )

    # modo de prueba: no continúa hacia la degradación
    if preflight_only:
        print(
            f"PREFLIGHT ONLY COMPLETADO: {target}. "
            "La base permanece intacta; no se generó Ground Truth definitivo."
        )
        return
    
    # La fila de metadatos se bloquea y se vuelve a validar para evitar doble ejecución
    with connect(target) as c:
        with c.transaction():
            verify_scenario_metadata(c, level, lock=True)
            apply_changes(c,changes)
            constraint_count_target,not_null_count_target=validate_constraints(c)
            updated=c.execute(
                "UPDATE datalake_control.scenario_metadata "
                "SET quality_status='degraded', degraded_at=clock_timestamp() "
                "WHERE singleton=TRUE AND quality_status='pristine'"
            )
            if updated.rowcount != 1:
                raise RuntimeError("No se pudo marcar la degradación de forma atómica.")
            print(
                f"VALIDACIÓN POSTERIOR: {constraint_count_target} constraints; "
                f"{not_null_count_target} columnas NOT NULL."
            )

    print(f"BASE DEGRADADA Y VALIDADA: {target}")
    recover_ground_truth(level)

def main():
    p = argparse.ArgumentParser(
        description=(
            "Quality Generator: degradación transaccional in-place "
            "y recuperación del Ground Truth."
        )
    )

    p.add_argument(
        "--level",
        choices=["medium", "poor"],
        required=True
    )

    execution_group = p.add_mutually_exclusive_group()

    execution_group.add_argument(
        "--execute",
        action="store_true",
        help="Aplica definitivamente el plan de degradación."
    )

    execution_group.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Ejecuta las modificaciones dentro de una transacción "
            "y revierte siempre. No degrada la base."
        )
    )

    p.add_argument(
        "--recover-ground-truth",
        action="store_true",
        help=(
            "Reconstruye el JSONL desde el CSV y verifica "
            "los valores actuales; no modifica la base."
        )
    )

    a = p.parse_args()

    if a.recover_ground_truth and (
        a.execute or a.preflight_only
    ):
        p.error(
            "--recover-ground-truth es incompatible con "
            "--execute y --preflight-only."
        )

    if a.recover_ground_truth:
        recover_ground_truth(a.level)
    else:
        run(
            a.level,
            execute=a.execute,
            preflight_only=a.preflight_only
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)