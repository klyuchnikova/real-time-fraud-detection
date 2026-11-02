"""Repeatable acceptance check against running services and real Airflow manual runs."""
import argparse
import base64
import json
import os
import time
import uuid
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
import psycopg
from common.registry import active_version, load_bundle, atomic_json
from scripts.smoke_stack import check
from pipeline.config import models_root, artifacts_root

BASE = os.getenv("AIRFLOW_URL","http://airflow-webserver:8080")

def api(path, method="GET", payload=None):
    token=base64.b64encode(f"{os.getenv('AIRFLOW_ADMIN_USER','admin')}:{os.getenv('AIRFLOW_ADMIN_PASSWORD','admin')}".encode()).decode()
    request=urllib.request.Request(BASE+"/api/v1"+path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Authorization":"Basic "+token,"Content-Type":"application/json"},method=method)
    with urllib.request.urlopen(request,timeout=30) as response:
        return json.load(response)

def manual(dag_id, timeout):
    run_id="acceptance-"+uuid.uuid4().hex
    api(f"/dags/{dag_id}/dagRuns","POST",{"dag_run_id":run_id})
    print(f"Triggered {dag_id}: {run_id}",flush=True)
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        result=api(f"/dags/{dag_id}/dagRuns/{run_id}")
        if result["state"] in ("success","failed"):
            tasks=api(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]
            evidence={"dag_id":dag_id,"run_id":run_id,"state":result["state"],
                      "tasks":[{"task_id":task["task_id"],"state":task["state"]} for task in tasks]}
            if result["state"]!="success" or any(task["state"]!="success" for task in tasks):
                raise AssertionError(f"Manual DAG did not succeed: {evidence}")
            print(f"Completed {dag_id}: success",flush=True)
            return evidence
        time.sleep(2)
    raise TimeoutError(f"Airflow did not finish {dag_id}: {run_id}")

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",default="reports/hw3_validation.json")
    parser.add_argument("--timeout",type=int,default=900)
    args=parser.parse_args()
    report={"checked_at":datetime.now(timezone.utc).isoformat(),"status":"running"}
    try:
        dags=api("/dags?only_active=true&limit=100")["dags"]
        names={dag["dag_id"] for dag in dags}
        required={"retrain_model":7,"supply_labeled_batch":4,"data_quality":4}
        if not set(required)<=names:
            raise AssertionError(f"Missing DAGs: {names}")
        report["dags"]={}
        for name,count in required.items():
            tasks=api(f"/dags/{name}/tasks")["tasks"]
            if len(tasks)!=count:
                raise AssertionError(f"Wrong task count: {name}")
            report["dags"][name]={"tasks":len(tasks)}
        before=check()
        report["score_before"]=before
        report["manual_runs"]=[manual("data_quality",args.timeout),manual("supply_labeled_batch",args.timeout)]
        for _ in range(5):
            report["manual_runs"].append(manual("retrain_model",args.timeout))
            after=check()
            report["score_after"]=after
            if after["model_version"]!=before["model_version"]:
                break
        else:
            raise AssertionError("Five tested windows produced no active version change; inspect gate decisions")
        version=report["score_after"]["model_version"]
        _,_,metadata=load_bundle(models_root(),version)
        if not metadata["decision"]["accepted"]:
            raise AssertionError("Rejected version became active")
        report["active_version"]=active_version(models_root())
        report["new_version_metadata"]=metadata
        with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
            report["labeled_rows"]=connection.execute("SELECT count(*) FROM labeled_transactions").fetchone()[0]
            report["sources"]=[{"sha256":row[0],"rows":row[1]} for row in connection.execute("SELECT sha256,rows_loaded FROM dataset_sources")]
            report["supply_batches"]=[{"batch_id":row[0],"status":row[1],"rows":row[2]} for row in connection.execute("SELECT batch_id,status,row_count FROM supply_runs ORDER BY target_month")]
        reports=list((artifacts_root()/"dq_reports").glob("*.html"))
        if not reports:
            raise AssertionError("No HTML quality reports")
        report["html_reports"]=[path.name for path in reports]
        report["status"]="success"
    except Exception as error:
        report["status"]="failed"
        report["error"]=str(error)
        raise
    finally:
        atomic_json(Path(args.output),report)
    print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=="__main__":
    main()
