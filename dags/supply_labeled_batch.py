"""Only assemble the monthly supply task graph."""
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from pipeline.supply import reserve_batch, prepare_batch, insert_batch, prune_history

@dag(dag_id="supply_labeled_batch", schedule="*/5 * * * *", start_date=pendulum.datetime(2026,1,1,tz="UTC"),
     catchup=False, max_active_runs=1, default_args={"retries":2,"retry_delay":timedelta(seconds=15)},
     tags=["hw3","data"])
def supply_graph():
    reserved = task(reserve_batch)()
    shifted = task(prepare_batch)(reserved)
    saved = task(insert_batch)(shifted)
    task(prune_history)(saved)

supply_graph()
