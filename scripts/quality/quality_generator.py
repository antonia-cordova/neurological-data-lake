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
("gold","fact_health_integrated"),("public","etl_runs"),
("public","silver_acs_2024"),("public","silver_acs_county_2024"),
("public","silver_brfss_2023"),("public","silver_places_2023")}

NUMERIC_RULES = [
("gold","fact_area_health","data_value","percent"),
("gold","fact_health_integrated","value_numeric","percent"),
("public","silver_places_2023","data_value","percent"),
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
("public","silver_places_2023","latitude","latitude"),
("public","silver_places_2023","longitude","longitude")]

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
("public","silver_acs_2024","estimate"),
("public","silver_acs_county_2024","estimate"),
("public","silver_places_2023","data_value")]

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
    for k in ("database","source_database","seed"):
        if k not in c: raise ValueError(f"{p.name}: falta {k}")
    return c

def dbcfg(db):
    load_dotenv(ROOT/".env")
    missing=[x for x in ("POSTGRES_USER","POSTGRES_PASSWORD","POSTGRES_HOST") if not os.getenv(x)]
    if missing: raise RuntimeError("Faltan variables en .env: "+", ".join(missing))
    return dict(host=os.getenv("POSTGRES_HOST"),port=int(os.getenv("POSTGRES_PORT","5432")),
                user=os.getenv("POSTGRES_USER"),password=os.getenv("POSTGRES_PASSWORD"),dbname=db)
def connect(db,autocommit=False): return psycopg.connect(**dbcfg(db),autocommit=autocommit)
def qt(s,t): return sql.SQL(".").join([sql.Identifier(s),sql.Identifier(t)])
def inventory(c):
    return set(c.execute("""SELECT table_schema,table_name FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_schema IN ('public','gold')""").fetchall())
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
    rulepath=out/f"quality_dry_run_rules_{level}.csv"
    fields=["table","column","dimension","eligible","target","planned","status","reason"]
    with rulepath.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
        for r in reports: w.writerow({k:r.get(k,"") for k in fields})
    changepath=out/f"quality_dry_run_changes_{level}.csv"
    fields2=["database","level","schema","table","primary_key","column","original_value","altered_value","rule","dimension","seed","changed_at_utc"]
    with changepath.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=fields2); w.writeheader()
        for x in changes:
            d=x.row(); w.writerow({k:d.get(k,"") for k in fields2})
    summary=out/f"quality_dry_run_summary_{level}.csv"
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
    """Devuelve las restricciones declarativas de public y gold."""
    return c.execute("""
        SELECT n.nspname,t.relname,con.conname,con.contype,
               pg_get_constraintdef(con.oid,true),con.convalidated
        FROM pg_constraint con
        JOIN pg_class t ON t.oid=con.conrelid
        JOIN pg_namespace n ON n.oid=t.relnamespace
        WHERE n.nspname IN ('public','gold')
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
        WHERE table_schema IN ('public','gold') AND is_nullable='NO'
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

def table_data_digest(c,s,t,keys):
    """Huella determinista del contenido completo de una tabla, ordenado por PK."""
    order=sql.SQL(",").join(sql.Identifier(k) for k in keys)
    query=sql.SQL("""
        SELECT md5(COALESCE(string_agg(md5(to_jsonb(x)::text), '' ORDER BY {}), ''))
        FROM {} AS x
    """).format(order,qt(s,t))
    return c.execute(query).fetchone()[0]

def structural_signature(c):
    """Firma de columnas, tipos, nulabilidad, defaults, constraints e índices."""
    columns=c.execute("""
        SELECT table_schema,table_name,ordinal_position,column_name,
               data_type,udt_schema,udt_name,is_nullable,column_default
        FROM information_schema.columns
        WHERE table_schema IN ('public','gold')
        ORDER BY table_schema,table_name,ordinal_position
    """).fetchall()
    constraints=c.execute("""
        SELECT n.nspname,t.relname,con.conname,con.contype,
               pg_get_constraintdef(con.oid,true),con.convalidated
        FROM pg_constraint con
        JOIN pg_class t ON t.oid=con.conrelid
        JOIN pg_namespace n ON n.oid=t.relnamespace
        WHERE n.nspname IN ('public','gold')
        ORDER BY n.nspname,t.relname,con.conname
    """).fetchall()
    indexes=c.execute("""
        SELECT schemaname,tablename,indexname,indexdef
        FROM pg_indexes
        WHERE schemaname IN ('public','gold')
        ORDER BY schemaname,tablename,indexname
    """).fetchall()
    payload=json.dumps(
        {"columns":columns,"constraints":constraints,"indexes":indexes},
        default=str,ensure_ascii=False,separators=(",",":")
    )
    import hashlib
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def source_snapshot(source):
    """Captura inventario, conteos, estructura y huellas de datos."""
    with connect(source) as c:
        inv=inventory(c)
        missing=EXPECTED-inv
        if missing:
            raise RuntimeError(f"Faltan tablas esperadas: {sorted(missing)}")
        counts={f"{s}.{t}":rowcount(c,s,t) for s,t in sorted(EXPECTED)}
        digests={}
        for s,t in sorted(EXPECTED):
            keys=pkcols(c,s,t)
            if not keys:
                raise RuntimeError(f"{s}.{t} no tiene PK; no se puede verificar la copia íntegra.")
            digests[f"{s}.{t}"]=table_data_digest(c,s,t,keys)
        return inv,counts,structural_signature(c),digests

def verify_clone(snapshot,target):
    inv0,counts0,structure0,digests0=snapshot
    with connect(target) as c:
        if inventory(c)!=inv0:
            raise RuntimeError("El clon no conserva exactamente el inventario de tablas.")
        counts1={f"{s}.{t}":rowcount(c,s,t) for s,t in sorted(EXPECTED)}
        if counts1!=counts0:
            raise RuntimeError("El clon no conserva los conteos de filas originales.")
        if structural_signature(c)!=structure0:
            raise RuntimeError("El clon no conserva columnas, tipos, constraints o índices.")
        digests1={}
        for s,t in sorted(EXPECTED):
            digests1[f"{s}.{t}"]=table_data_digest(c,s,t,pkcols(c,s,t))
        if digests1!=digests0:
            mismatches=[k for k in digests0 if digests0[k]!=digests1.get(k)]
            raise RuntimeError(f"El clon no conserva íntegramente los datos originales: {mismatches}")
    return True

def clone(source,target):
    with connect("postgres",True) as c:
        if c.execute("SELECT 1 FROM pg_database WHERE datname=%s",(target,)).fetchone():
            raise RuntimeError(f"{target} ya existe. No se sobrescribirá.")
        active=c.execute("SELECT count(*) FROM pg_stat_activity WHERE datname=%s AND pid<>pg_backend_pid()",(source,)).fetchone()[0]
        if active: raise RuntimeError(f"{active} conexión(es) activa(s) en {source}")
        c.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(sql.Identifier(target),sql.Identifier(source)))

def run(level,execute):
    cfg=load_config(level); source=cfg["source_database"]; target=cfg["database"]
    with connect(source) as c:
        inv=inventory(c); missing=EXPECTED-inv
        if missing: raise RuntimeError(f"Faltan tablas: {sorted(missing)}")
        counts={f"{s}.{t}":rowcount(c,s,t) for s,t in sorted(EXPECTED)}
        changes,reports=build_plan(c,level,cfg)
    print(f"\n{'='*72}\n{level.upper()} | {source} -> {target}")
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
        print("DRY RUN: solo lectura. No se creó ni modificó ninguna base.")
        return

    # 1) Captura el estado íntegro de la fuente antes de la clonación.
    snapshot=source_snapshot(source)

    # 2) Preflight real dentro de una transacción que SIEMPRE se revierte.
    # PostgreSQL valida CHECK, NOT NULL, FK, UNIQUE y tipos al ejecutar UPDATE.
    constraint_count,not_null_count=preflight_changes(source,changes)
    print(
        f"PREFLIGHT APROBADO: {len(changes):,} operaciones; "
        f"{constraint_count} constraints; {not_null_count} columnas NOT NULL. "
        "Transacción de prueba revertida."
    )

    # 3) CREATE DATABASE ... TEMPLATE.
    clone(source,target)

    # 4) Verifica estructura y contenido exacto antes de degradar.
    verify_clone(snapshot,target)
    print("CLON VERIFICADO: estructura y datos idénticos a la fuente.")

    # 5) Todas las alteraciones se aplican en una sola transacción.
    # Si falla un UPDATE o una restricción, el context manager hace ROLLBACK.
    try:
        with connect(target) as c:
            apply_changes(c,changes)
            constraint_count_target,not_null_count_target=validate_constraints(c)
            print(
                f"VALIDACIÓN POSTERIOR: {constraint_count_target} constraints; "
                f"{not_null_count_target} columnas NOT NULL."
            )
    except Exception:
        # No se borra automáticamente ninguna base. La transacción fallida
        # queda revertida y el target permanece como clon Perfect intacto.
        print(
            f"ERROR AL APLICAR. La transacción fue revertida. "
            f"{target} permanece como clon intacto de {source}; "
            "no se ejecutó DROP DATABASE."
        )
        raise

    gt=ROOT/"quality"/"ground_truth"
    gt.mkdir(parents=True,exist_ok=True)
    with (gt/f"ground_truth_{level}.jsonl").open("w",encoding="utf-8") as f:
        for x in changes:
            f.write(json.dumps(x.row(),ensure_ascii=False)+"\n")
    print(f"BASE CREADA Y VALIDADA: {target}")

def main():
    p=argparse.ArgumentParser(description="Quality Generator V5: preflight transaccional, verificación de clon y aplicación atómica.")
    p.add_argument("--level",choices=["medium","poor","both"],default="both")
    p.add_argument("--execute",action="store_true",help="Clona y aplica exactamente el plan calculado.")
    a=p.parse_args()
    for level in (["medium","poor"] if a.level=="both" else [a.level]): run(level,a.execute)
if __name__=="__main__":
    try: main()
    except Exception as e:
        print(f"ERROR: {e}",file=sys.stderr); sys.exit(1)
