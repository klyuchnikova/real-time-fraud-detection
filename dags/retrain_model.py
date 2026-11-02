"""Only assemble the restartable retraining task graph."""
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from pipeline.retrain import (reserve_window, prepare_dataset, train_candidate,
                              evaluate_candidate, save_candidate, publish_candidate, finish_month)

@dag(dag_id="retrain_model", schedule="*/5 * * * *", start_date=pendulum.datetime(2026,1,1,tz="UTC"),
     catchup=False, max_active_runs=1, default_args={"retries":2,"retry_delay":timedelta(seconds=15)},
     tags=["hw3","training"])
def retrain_graph():
    window = task(reserve_window)()
    data = task(prepare_dataset)(window)
    candidate = task(train_candidate)(data)
    decision = task(evaluate_candidate)(candidate)
    saved = task(save_candidate)(decision)
    published = task(publish_candidate)(saved)
    task(finish_month)(published)

retrain_graph()
