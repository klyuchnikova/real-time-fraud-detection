"""Only assemble snapshot, checks, report publication and assertion."""
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from pipeline.quality import snapshot_dataset, evaluate_dataset, write_report, assert_report

@dag(dag_id="data_quality", schedule="*/5 * * * *", start_date=pendulum.datetime(2026,1,1,tz="UTC"),
     catchup=False, max_active_runs=1, default_args={"retries":1,"retry_delay":timedelta(seconds=15)},
     tags=["hw3","quality"])
def quality_graph():
    snapshot = task(snapshot_dataset)()
    evaluated = task(evaluate_dataset)(snapshot)
    report = task(write_report)(evaluated)
    task(assert_report)(report)

quality_graph()
