from __future__ import annotations
import argparse, csv, json, os, random, sys
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

INDICATOR_TO_MEASURE = {
"ARTHRITIS":"ARTHRITIS","CORONARY_HEART_DISEASE":"CHD","DEPRESSION":"DEPRESSION",
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
def mkchange(cfg,level,s,t,keys,vals,col,old,new,rule,dim):
    return Change(cfg["database"],level,s,t,dict(zip(keys,vals)),col,old,new,rule,dim,
                  int(cfg["seed"]),datetime.now(timezone.utc).isoformat())
def cell_id(s,t,keys,vals,col):
    return (s,t,tuple(vals),col)

def eligible(c,s,t,col,keys,reserved):
    names=keys+[col]
    rows=c.execute(sql.SQL("SELECT {} FROM {} WHERE {} IS NOT NULL").format(
        sql.SQL(",").join(map(sql.Identifier,names)),qt(s,t),sql.Identifier(col))).fetchall()
    return [r for r in rows if cell_id(s,t,keys,r[:len(keys)],col) not in reserved]


def plan_rule(c, cfg, level, s, t, col, kind, rate, minimum, rng, reserved):
    name = f"{s}.{t}"
    info = colinfo(c, s, t)

    if col not in info:
        return [], dict(
            table=name,
            column=col,
            dimension=dimension(kind),
            eligible=0,
            target=0,
            planned=0,
            status="COLUMN_MISSING",
            reason="Columna inexistente"
        )

    keys, fks, uniques = protections(c, s, t)
    key_order = pkcols(c, s, t)

    if not key_order:
        return [], dict(
            table=name,
            column=col,
            dimension=dimension(kind),
            eligible=0,
            target=0,
            planned=0,
            status="NO_PRIMARY_KEY",
            reason="Sin PK"
        )

    if col in keys or col in uniques:
        return [], dict(
            table=name,
            column=col,
            dimension=dimension(kind),
            eligible=0,
            target=0,
            planned=0,
            status="UNIQUE_PROTECTED",
            reason="Participa en PK/índice UNIQUE"
        )

    if col in fks:
        return [], dict(
            table=name,
            column=col,
            dimension=dimension(kind),
            eligible=0,
            target=0,
            planned=0,
            status="FK_PROTECTED",
            reason="Participa en FK"
        )

    if kind == "null" and info[col] != "YES":
        return [], dict(
            table=name,
            column=col,
            dimension="completeness",
            eligible=0,
            target=0,
            planned=0,
            status="NOT_NULL_PROTECTED",
            reason="Columna NOT NULL"
        )

    # Obtener todas las filas elegibles, excluyendo celdas
    # que ya fueron reservadas por otra regla.
    rows = eligible(c, s, t, col, key_order, reserved)

    n = len(rows)
    target = rate_count(n, rate, minimum)

    if not target:
        return [], dict(
            table=name,
            column=col,
            dimension=dimension(kind),
            eligible=n,
            target=0,
            planned=0,
            status="NO_ELIGIBLE_ROWS",
            reason="Sin valores elegibles"
        )

    # CAMBIO PRINCIPAL:
    # Se genera una permutación aleatoria de todas las filas.
    # No se limita la selección inicial al número objetivo.
    candidates = rng.sample(rows, len(rows))

    changes = []

    for r in candidates:

        # Detenerse solamente cuando se alcanza el objetivo.
        if len(changes) >= target:
            break

        vals = tuple(r[:len(key_order)])
        old = r[len(key_order)]

        if kind == "null":
            new = None
            rule = "null_injection"

        elif kind in (
            "percent",
            "positive",
            "brfss",
            "latitude",
            "longitude"
        ):
            new = num_new(old, kind, level, rng)

            # Si no existe un cambio válido, continuar
            # buscando otra fila elegible.
            if new is None:
                continue

            rule = f"numeric_{kind}"

        else:
            opts = [
                x for x in kind
                if x is not None and str(x) != str(old)
            ]

            if not opts:
                continue

            new = rng.choice(opts)
            rule = "semantic_category_swap"

        changes.append(
            mkchange(
                cfg,
                level,
                s,
                t,
                key_order,
                vals,
                col,
                old,
                new,
                rule,
                dimension(kind)
            )
        )

        # Reservar únicamente las celdas efectivamente modificadas.
        reserved.add(
            cell_id(s, t, key_order, vals, col)
        )

    planned = len(changes)

    if planned == target:
        status = "PLANNED"
        reason = ""
    else:
        status = "PLANNED_PARTIAL"
        reason = (
            f"Objetivo: {target}; cambios válidos: {planned}; "
            f"candidatos elegibles revisados: {n}"
        )

    return changes, dict(
        table=name,
        column=col,
        dimension=dimension(kind),
        eligible=n,
        target=target,
        planned=planned,
        status=status,
        reason=reason
    )

def plan_fk_semantics(c,cfg,level,rate,minimum,rng,reserved):
    rows=c.execute("""SELECT f.integrated_fact_key,f.source_run_id,f.geography_key,
    f.indicator_key,f.health_measure_key,f.prevalence_type_key,i.indicator_code,m.measure_id
    FROM gold.fact_health_integrated f
    JOIN gold.dim_health_integrated_indicator i ON i.indicator_key=f.indicator_key
    JOIN gold.dim_health_measure m ON m.health_measure_key=f.health_measure_key
    WHERE f.health_measure_key IS NOT NULL""").fetchall()
    rows=[r for r in rows if ("gold","fact_health_integrated",(r[0],),"health_measure_key") not in reserved]
    target=rate_count(len(rows),rate,minimum); rng.shuffle(rows); changes=[]
    # The partial UNIQUE key is (source_run_id, geography_key, indicator_key,
    # health_measure_key, prevalence_type_key); do not include the PK in this tuple.
    occupied={tuple(r) for r in c.execute("""
    SELECT source_run_id,geography_key,indicator_key,health_measure_key,prevalence_type_key
    FROM gold.fact_health_integrated WHERE health_measure_key IS NOT NULL
    AND source_run_id IS NOT NULL AND geography_key IS NOT NULL
    AND indicator_key IS NOT NULL AND prevalence_type_key IS NOT NULL""").fetchall()}
    measures=c.execute("SELECT health_measure_key,measure_id FROM gold.dim_health_measure").fetchall()
    for r in rows:
        if len(changes)>=target: break
        fact,run,geo,ind,old,prev,code,measure_id=r
        if INDICATOR_TO_MEASURE.get(code)!=measure_id: continue
        options=measures[:]; rng.shuffle(options); selected=None
        for new,new_id in options:
            if new==old: continue
            tup=(run,geo,ind,new,prev)
            if tup not in occupied: selected=(new,new_id,tup); break
        if selected is None: continue
        new,new_id,tup=selected
        occupied.discard((run,geo,ind,old,prev)); occupied.add(tup)
        changes.append(mkchange(cfg,level,"gold","fact_health_integrated",
            ["integrated_fact_key"],[fact],"health_measure_key",old,new,
            "fk_valid_semantic_association_swap","semantic_consistency"))
        reserved.add(("gold","fact_health_integrated",(fact,),"health_measure_key"))
    status="PLANNED" if len(changes)==target else "PLANNED_PARTIAL"
    return changes,dict(table="gold.fact_health_integrated",column="health_measure_key",
        dimension="semantic_consistency",eligible=len(rows),target=target,planned=len(changes),
        status=status,reason="" if status=="PLANNED" else "Alternativas insuficientes sin colisión UNIQUE")

def build_plan(c,level,cfg):
    rng=random.Random(int(cfg["seed"])+(1 if level=="medium" else 2))
    rates=cfg["rates"]; minimum=int(cfg.get("minimum_changes",{}).get("minimum_per_nonempty_table",1)) if cfg.get("minimum_changes",{}).get("enabled",True) else 0
    changes=[]; reports=[]; reserved=set()
    for s,t,col,kind in NUMERIC_RULES:
        a,b=plan_rule(c,cfg,level,s,t,col,kind,rates["accuracy"],minimum,rng,reserved); changes+=a; reports.append(b)
    for s,t,col,allowed in TEXT_RULES:
        a,b=plan_rule(c,cfg,level,s,t,col,allowed,rates["semantic_consistency"],minimum,rng,reserved); changes+=a; reports.append(b)
    a,b=plan_fk_semantics(c,cfg,level,rates["semantic_consistency"],minimum,rng,reserved); changes+=a; reports.append(b)
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
    fields=["table","column","dimension","eligible","target","planned","status","reason"]
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
        for dim in ("accuracy","completeness","semantic_consistency"):
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

def apply_changes(c,changes):
    """Aplica el plan dentro de la transacción activa y verifica el valor de origen."""
    for x in changes:
        keys=list(x.primary_key)
        vals=tuple(x.primary_key[k] for k in keys)
        where=sql.SQL(" AND ").join(
            sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(k))
            for k in keys
        )
        # Evita aplicar un plan obsoleto si la celda cambió desde el dry run.
        where=sql.SQL(" AND ").join([
            where,
            sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(x.column))
        ])
        result=c.execute(
            sql.SQL("UPDATE {} SET {}=%s WHERE {}").format(
                qt(x.schema,x.table),sql.Identifier(x.column),where
            ),
            [x.altered_value,*vals,x.original_value]
        )
        if result.rowcount!=1:
            raise RuntimeError(
                f"UPDATE {x.schema}.{x.table}.{x.column}: "
                f"fila inexistente, valor original distinto o PK no única "
                f"(rowcount={result.rowcount}, esperado=1)"
            )

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
    # Las restricciones inmediatas se comprueban en cada UPDATE; esto fuerza
    # además las restricciones DEFERRABLE pendientes.
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

            for line_number, row in enumerate(reader, start=2):
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

                where = sql.SQL(" AND ").join(
                    sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(key))
                    for key in primary_key
                )
                query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
                    sql.Identifier(column_name),
                    qt(schema_name, table_name),
                    where,
                )
                values = list(primary_key.values())
                matches = conn.execute(query, values).fetchall()
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


def run(level,execute):
    cfg=load_config(level)
    target=cfg["database"]
    with connect(target) as c:
        verify_scenario_metadata(c, level)
        inv=inventory(c)
        missing=EXPECTED-inv
        if missing:
            raise RuntimeError(f"Faltan tablas: {sorted(missing)}")
        counts={f"{s}.{t}":rowcount(c,s,t) for s,t in sorted(EXPECTED)}
        changes,reports=build_plan(c,level,cfg)

    print(f"\n{'='*72}\n{level.upper()} | base={target} | frecuencia={cfg['frequency']}")
    print(f"Tasas: accuracy {cfg['rates']['accuracy']:.0%} | completeness {cfg['rates']['completeness']:.0%} | semantic consistency {cfg['rates']['semantic_consistency']:.0%}")
    print(f"Tablas: {len(inv)} | Filas fuente: {sum(counts.values()):,}")
    print(f"{'DIMENSIÓN':24} {'REGLAS':>7} {'ELEGIBLES':>12} {'CAMBIOS':>10} {'OMITIDAS/PARCIALES':>20}")
    for dim in ("accuracy","completeness","semantic_consistency"):
        subset=[r for r in reports if r["dimension"]==dim]
        print(f"{dim:24} {len(subset):7d} {sum(r['eligible'] for r in subset):12,d} {sum(r['planned'] for r in subset):10,d} {sum(r['status']!='PLANNED' for r in subset):20d}")
    unique_cells=len({(x.schema,x.table,tuple(x.primary_key.items()),x.column) for x in changes})
    print(f"OPERACIONES PLANIFICADAS: {len(changes):,}")
    print(f"CELDAS ÚNICAS A ALTERAR: {unique_cells:,}")
    for r in reports:
        if r["status"]!="PLANNED": print(f"  {r['status']}: {r['table']}.{r['column']} — {r['reason']}")
    paths=write_reports(level,cfg,changes,reports,counts)
    print("Informes:")
    for p in paths: print(f"  {p.relative_to(ROOT)}")
    if not execute:
        print("DRY RUN: solo lectura. La base permanece intacta.")
        return
    if not changes:
        raise RuntimeError("El plan no contiene cambios; se cancela para no marcar la base como degradada.")

    # Preflight transaccional sobre la misma base objetivo; siempre se revierte.
    constraint_count,not_null_count=preflight_changes(target,changes)
    print(
        f"PREFLIGHT APROBADO: {len(changes):,} operaciones; "
        f"{constraint_count} constraints; {not_null_count} columnas NOT NULL. "
        "Transacción de prueba revertida."
    )

    # Aplicación atómica: datos y estado de calidad se confirman juntos.
    # La fila de metadatos se bloquea y se vuelve a validar para evitar doble ejecución.
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

    gt=ROOT/"quality"/"ground_truth"
    gt.mkdir(parents=True,exist_ok=True)
    suffix = "_test" if target.endswith("_test") else ""
    with (gt/f"ground_truth_{level}{suffix}.jsonl").open("w",encoding="utf-8") as f:
        for x in changes:
            f.write(json.dumps(x.row(),ensure_ascii=False)+"\n")
    print(f"BASE DEGRADADA Y VALIDADA: {target}")

def main():
    p=argparse.ArgumentParser(description="Quality Generator: degradación transaccional in-place y recuperación del Ground Truth.")
    p.add_argument("--level",choices=["medium","poor"],required=True)
    p.add_argument("--execute",action="store_true",help="Aplica el plan de degradación a la base del escenario seleccionado.")
    p.add_argument("--recover-ground-truth",action="store_true",help="Reconstruye el JSONL desde el CSV y verifica los valores actuales; no modifica la base.")
    a=p.parse_args()
    if a.execute and a.recover_ground_truth:
        p.error("--execute y --recover-ground-truth son opciones incompatibles.")
    if a.recover_ground_truth:
        recover_ground_truth(a.level)
    else:
        run(a.level,a.execute)
if __name__=="__main__":
    try: main()
    except Exception as e:
        print(f"ERROR: {e}",file=sys.stderr); sys.exit(1)
