"""Inspect actual Airflow DAG objects when running in the Airflow image."""
import importlib.util
import unittest
from pathlib import Path

@unittest.skipUnless((importlib.util.find_spec("airflow") and importlib.util.find_spec("airflow.models")),"Run this test in the Airflow image")
class DagTest(unittest.TestCase):
    def test_three_dags_import_and_have_task_chains(self):
        from airflow.models import DagBag
        bag=DagBag(str(Path("/opt/airflow/dags")),include_examples=False)
        self.assertEqual(bag.import_errors,{})
        expected={"retrain_model":7,"supply_labeled_batch":4,"data_quality":4}
        self.assertEqual(set(bag.dags),set(expected))
        for name,count in expected.items():
            dag=bag.dags[name]
            self.assertFalse(dag.catchup)
            self.assertEqual(dag.max_active_runs,1)
            self.assertEqual(len(dag.tasks),count)
            self.assertEqual(len(dag.roots),1)
            self.assertEqual(len(dag.leaves),1)
            self.assertEqual(dag.schedule_interval,"*/5 * * * *")
