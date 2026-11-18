"""Host-side acceptance: real metrics, provisioned panels, retraining and alert recovery."""
import argparse
import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def setting(name, default):
    if name in os.environ:
        return os.environ[name]
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8-sig").splitlines():
            if line.startswith(name+"="):
                return line.split("=",1)[1].strip().strip('"').strip("'")
    return default

GRAFANA = "http://127.0.0.1:"+setting("GRAFANA_PORT","3000")
PROMETHEUS = "http://127.0.0.1:"+setting("PROMETHEUS_PORT","9090")
AIRFLOW = "http://127.0.0.1:"+setting("AIRFLOW_WEB_PORT","8081")

def request(url, data=None, auth=False):
    headers={}
    if auth:
        token=base64.b64encode((setting("GRAFANA_ADMIN_USER","admin")+":"+setting("GRAFANA_ADMIN_PASSWORD","admin")).encode()).decode()
        headers["Authorization"]="Basic "+token
    if data is not None:
        headers["Content-Type"]="application/json"
    req=urllib.request.Request(url,data=None if data is None else json.dumps(data).encode(),headers=headers)
    try:
        with urllib.request.urlopen(req,timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"{url}: {error.code} {error.read().decode()[:1500]}") from error

def compose(*args, capture=False):
    result=subprocess.run(["docker","compose",*args],cwd=ROOT,text=True,encoding="utf-8",errors="replace",
                          stdout=subprocess.PIPE if capture else None,check=True)
    return result.stdout.strip() if capture else None

def prom(query):
    result=request(PROMETHEUS+"/api/v1/query?"+urllib.parse.urlencode({"query":query}))
    if result["status"]!="success":
        raise AssertionError(result)
    return result["data"]["result"]

def value(query):
    values=prom(query)
    return float(values[0]["value"][1]) if values else 0

def wait_for(check, title, timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            result=check()
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            result=False
        if result:
            print(title,flush=True)
            return result
        time.sleep(5)
    raise TimeoutError(title)

def pg_query(query):
    now=int(time.time()*1000)
    result=request(GRAFANA+"/api/ds/query",{"from":str(now-900000),"to":str(now),
        "queries":[{"refId":"A","datasource":{"type":"postgres","uid":"postgres"},
                    "rawSql":query,"format":"table","rawQuery":True,"intervalMs":10000,"maxDataPoints":1000}]},True)["results"]["A"]
    if result.get("error"):
        raise AssertionError(result["error"])
    return result.get("frames",[])

def columns(frames):
    return frames[0]["data"]["values"] if frames else []

def inspect_scorer():
    ident=compose("ps","-q","scoring-service",capture=True)
    record=json.loads(subprocess.check_output(["docker","inspect",ident],text=True))[0]
    return {"id":record["Id"],"started":record["State"]["StartedAt"],"restarts":record["RestartCount"]}

def alert_state(uid):
    rules=request(GRAFANA+"/api/prometheus/grafana/api/v1/rules",auth=True)["data"]["groups"]
    matches=[r for g in rules for r in g["rules"] if r.get("labels",{}).get("rule_uid")==uid or r.get("uid")==uid]
    if not matches:
        # Grafana's Prometheus-compatible endpoint exposes the title as rule name.
        titles={"stream-stopped":"Поток скоринга остановился","invalid-stream":"Некорректный входной поток"}
        matches=[r for g in rules for r in g["rules"] if r.get("name")==titles[uid]]
    if len(matches)!=1:
        raise AssertionError(f"Cannot find alert {uid}: {rules}")
    return matches[0]["state"]

def check_panels():
    states=columns(pg_query("SELECT DISTINCT us_state FROM monitoring_scores WHERE has_context ORDER BY 1"))[0]
    merchants=columns(pg_query("SELECT DISTINCT merch FROM monitoring_scores WHERE has_context ORDER BY 1"))[0]
    def quoted(items):
        return ",".join("'"+item.replace("'","''")+"'" for item in items)
    results={}
    for path in sorted((ROOT/"monitoring/grafana/dashboards").glob("*.json")):
        dashboard=json.loads(path.read_text(encoding="utf-8"))
        loaded=request(GRAFANA+"/api/dashboards/uid/"+dashboard["uid"],auth=True)
        assert len(loaded["dashboard"]["panels"])==len(dashboard["panels"])
        for panel in dashboard["panels"]:
            for target in panel["targets"]:
                key=dashboard["uid"]+"/"+str(panel["id"])+"/"+target["refId"]
                if "expr" in target:
                    assert prom(target["expr"]),key+" has no time series"
                else:
                    query=target["rawSql"].replace("${state:sqlstring}",quoted(states)).replace("${merchant:sqlstring}",quoted(merchants))
                    assert columns(pg_query(query)),key+" has no frame"
                results[key]="ok"
    return results

def check_alerts():
    evidence={}
    try:
        compose("stop","transaction-simulator")
        wait_for(lambda: alert_state("stream-stopped")=="firing","Stopped-flow alert fired",240)
        evidence["stream_stopped"]="firing"
    finally:
        compose("up","-d","--no-deps","transaction-simulator")
    wait_for(lambda: alert_state("stream-stopped")=="inactive","Stopped-flow alert recovered",180)
    evidence["stream_recovered"]="inactive"
    process=subprocess.Popen(["docker","compose","run","--rm","--no-deps","-T","transaction-simulator",
        "python","-m","transaction_simulator.app","--rate","10","--limit","900","--invalid-ratio","0.1"],cwd=ROOT)
    try:
        wait_for(lambda: alert_state("invalid-stream")=="firing","Invalid-stream alert fired",150)
        evidence["invalid_stream"]="firing"
    finally:
        if process.wait(timeout=150)!=0:
            raise RuntimeError("Invalid stream generator failed")
    wait_for(lambda: alert_state("invalid-stream")=="inactive","Invalid-stream alert recovered",180)
    evidence["invalid_recovered"]="inactive"
    return evidence

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--skip-retrain",action="store_true")
    parser.add_argument("--skip-alerts",action="store_true")
    args=parser.parse_args()
    report={"checked_at":datetime.now(timezone.utc).isoformat(),"status":"running"}
    out=ROOT/"reports/monitoring_validation.json"
    out.parent.mkdir(exist_ok=True)
    try:
        def ready_targets():
            found=request(PROMETHEUS+"/api/v1/targets")["data"]["activeTargets"]
            return found if len(found)==3 and all(t["health"]=="up" for t in found) else None
        targets=wait_for(ready_targets,"Three Prometheus targets are UP")
        wait_for(lambda: request(GRAFANA+"/api/health").get("database")=="ok","Grafana is ready")
        report["targets"]=[t["labels"]["job"] for t in targets]
        before=value("fraud_processed_total")
        written_before=value("sink_scores_written_total")
        wait_for(lambda: value("fraud_processed_total")>before+20,"Live scoring counters increased")
        wait_for(lambda: value("sink_scores_written_total")>written_before+20,"Live storage counters increased")
        report["panels"]=check_panels()
        rules=request(GRAFANA+"/api/v1/provisioning/alert-rules",auth=True)
        assert {r["uid"] for r in rules} >= {"stream-stopped","invalid-stream"}
        report["provisioned_alerts"]=[r["uid"] for r in rules]
        if not args.skip_retrain:
            wait_for(lambda: request(AIRFLOW+"/health").get("metadatabase",{}).get("status")=="healthy",
                     "Airflow API is ready",300)
            started=time.time()
            before_container=inspect_scorer()
            compose("run","--rm","--no-deps","-T","tools","python","-m","scripts.validate_stack","--monitoring-only","--output","reports/retraining_validation.json")
            assert inspect_scorer()==before_container,"Scorer restarted during reload"
            reloaded=prom("fraud_model_reload_timestamp_seconds")
            assert any(float(x["value"][1])>=started for x in reloaded),reloaded
            report["reload"]={"container":before_container,"events":reloaded,
                              "training":json.loads((ROOT/"reports/retraining_validation.json").read_text(encoding="utf-8"))}
        if not args.skip_alerts:
            report["alerts"]=check_alerts()
        report["status"]="partial" if args.skip_retrain or args.skip_alerts else "success"
    except Exception as error:
        report["status"]="failed"
        report["error"]=str(error)
        raise
    finally:
        out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print(f"Report: {out}",flush=True)

if __name__=="__main__":
    main()
