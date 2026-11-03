"""Independently reconcile every mart row with the original CSV and record evidence."""
import csv
import json
import math
import os
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
import yaml
from common.clients import client_identity,open_registry
from scripts.clickhouse import connect,identifier

ROOT=Path(__file__).resolve().parents[1]

def stats():
    return {"n":0,"sum":Decimal(0),"frauds":0,"fraud_sum":Decimal(0),"clients":set(),"merchants":set(),"values":[],"large":0,"first":None,"last":None}

def record(group,amount,target,client_id,merchant,time,large,keep_values=False):
    group["n"]+=1
    group["sum"]+=amount
    group["frauds"]+=target
    if target: group["fraud_sum"]+=amount
    group["clients"].add(client_id)
    group["merchants"].add(merchant)
    group["large"]+=int(amount>=large)
    if keep_values: group["values"].append(amount)
    group["first"]=time if group["first"] is None else min(group["first"],time)
    group["last"]=time if group["last"] is None else max(group["last"],time)

def expect_equal(actual,expected,context):
    if isinstance(expected,float):
        okay=math.isclose(float(actual),expected,rel_tol=1e-9,abs_tol=1e-6)
    else:
        okay=actual==expected
    if not okay:
        raise AssertionError(f"{context}: got {actual!r}, expected {expected!r}")

def test_summary(path):
    data=json.loads(path.read_text(encoding="utf-8"))
    statuses={}
    tests=[item for item in data["results"] if item["unique_id"].startswith("test.")]
    if not tests:
        raise AssertionError(f"No tests recorded in {path}")
    for result in tests:
        name=result["status"]
        statuses[name]=statuses.get(name,0)+1
    if any(name not in ("pass","success") for name in statuses):
        raise AssertionError(f"Unsuccessful tests in {path}: {statuses}")
    return {"total":len(tests),"statuses":statuses}

def main():
    base=identifier(os.getenv("ANALYTICS_DATABASE","analytics"))
    raw=identifier(os.getenv("RAW_DATABASE","transactions_db"))
    settings=yaml.safe_load((ROOT/"dbt/dbt_project.yml").read_text(encoding="utf-8"))["vars"]
    threshold=Decimal(str(settings["large_amount_threshold"]))
    registry=open_registry(os.getenv("CLIENT_REGISTRY","/state/clients.sqlite"))
    mapping={tuple(row[1:]):row[0] for row in registry.execute("SELECT * FROM clients")}
    registry.close()
    daily=defaultdict(stats)
    categories=defaultdict(stats)
    states=defaultdict(stats)
    customers=defaultdict(stats)
    total=stats()
    with Path(os.getenv("TRAIN_DATA_CSV","/data/train.csv")).open(encoding="utf-8-sig",newline="") as stream:
        for row in csv.DictReader(stream):
            amount=Decimal(row["amount"])
            target=int(row["target"])
            time=datetime.fromisoformat(row["transaction_time"])
            state=row["us_state"].strip().upper()
            customer=mapping[client_identity(row)]
            merchant=row["merch"].strip()
            for group,values in ((total,False),(daily[(time.date(),state)],True),
                                 (categories[row["cat_id"].strip()],False),(states[state],False),(customers[customer],False)):
                record(group,amount,target,customer,merchant,time,threshold,values)
    client=connect()
    try:
        actual=tuple(client.query(f"SELECT count(),sum(toUInt64(target)),sum(amount) FROM {raw}.transactions").first_row)
        expect_equal(actual,(total["n"],total["frauds"],total["sum"]),"raw totals")
        report={"status":"success","source":{"rows":total["n"],"frauds":total["frauds"],
                "fraud_rate_pct":100*total["frauds"]/total["n"],"total_amount":str(total["sum"]),
                "clients":len(total["clients"]),"merchants":len(total["merchants"]),"regions":len(states)},
                "marts":{},"versions":{name:version(name) for name in ("dbt-core","dbt-clickhouse","clickhouse-connect")}}
        specifications=[
            ("mart_daily_state_metrics",daily,lambda r:(r["transaction_date"],r["state_code"])),
            ("mart_fraud_by_category",categories,lambda r:r["category"]),
            ("mart_fraud_by_state",states,lambda r:r["state_code"]),
            ("mart_customer_risk_profile",customers,lambda r:r["client_id"])]
        for name,expected,key in specifications:
            seen=set()
            for row in client.query(f"SELECT * FROM {base}_marts.{name}").named_results():
                identity=key(row)
                if identity in seen or identity not in expected:
                    raise AssertionError(f"Unexpected or duplicate key in {name}: {identity}")
                seen.add(identity)
                group=expected[identity]
                checks={"transaction_count":group["n"],"total_amount":group["sum"]}
                if name=="mart_daily_state_metrics":
                    values=sorted(group["values"])
                    checks.update(average_amount=float(group["sum"]/group["n"]),
                                  p95_amount=values[min(int(0.95*len(values)),len(values)-1)],
                                  large_transaction_share=group["large"]/group["n"])
                else:
                    rate=100*group["frauds"]/group["n"]
                    checks.update(fraud_count=group["frauds"],fraud_amount=group["fraud_sum"],fraud_rate=rate)
                    if name=="mart_fraud_by_state":
                        checks.update(unique_clients=len(group["clients"]),unique_merchants=len(group["merchants"]))
                    if name=="mart_customer_risk_profile":
                        checks.update(average_amount=float(group["sum"]/group["n"]),
                                      risk_level="HIGH" if rate>=settings["risk_high_pct"] else "MEDIUM" if rate>=settings["risk_medium_pct"] else "LOW")
                        expect_equal(row["first_transaction_at"].replace(tzinfo=None),group["first"],name+" first timestamp")
                        expect_equal(row["last_transaction_at"].replace(tzinfo=None),group["last"],name+" last timestamp")
                for field,value in checks.items():
                    expect_equal(row[field],value,f"{name} {identity} {field}")
            expect_equal(seen,set(expected),name+" keys")
            report["marts"][name]={"rows":len(seen),"all_rows_reconciled":True}
        report["dbt_tests"]=test_summary(ROOT/"reports/dbt_test.json")
        report["project_evaluator_tests"]=test_summary(ROOT/"reports/project_evaluator_test.json")
        manifest=json.loads((ROOT/"dbt/target/manifest.json").read_text(encoding="utf-8"))
        report["singular_tests"]=[node["name"] for node in manifest["nodes"].values()
                                  if node["resource_type"]=="test" and node["package_name"]=="transaction_analytics"
                                  and node["original_file_path"].startswith("tests/")]
        for filename in ("index.html","manifest.json","catalog.json"):
            if not (ROOT/"dbt/target"/filename).is_file():
                raise AssertionError(f"Missing docs artifact: {filename}")
        report["docs_generated"]=True
        report["loader_tests"]=json.loads((ROOT/"reports/loader_tests.json").read_text(encoding="utf-8"))
        if report["loader_tests"]["failures"] or report["loader_tests"]["errors"] or report["loader_tests"]["skipped"]:
            raise AssertionError("Loader test results are incomplete")
        screenshot=ROOT/"docs/dbt_docs.png"
        report["documentation_screenshot"]="docs/dbt_docs.png" if screenshot.is_file() else "not_provided"
        report["lineage_svg"]="docs/lineage.svg"
        report["client_key"]="SQLite INTEGER PRIMARY KEY AUTOINCREMENT; UNIQUE identity tuple"
        report["risk_distribution"]=[dict(row) for row in client.query(
            f"SELECT risk_level,count() AS clients FROM {base}_marts.mart_customer_risk_profile GROUP BY risk_level ORDER BY risk_level").named_results()]
        report["source_receipt"]=[{key:str(value) if isinstance(value,Decimal) else value for key,value in row.items()}
                                  for row in client.query(f"SELECT * FROM {raw}.ingestion_meta").named_results()]
        destination=ROOT/"reports/validation.json"
        destination.parent.mkdir(parents=True,exist_ok=True)
        destination.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print(json.dumps(report,ensure_ascii=False,indent=2))
    finally:
        client.close()

if __name__=="__main__":
    main()
