import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from pipeline.data import split_window
from pipeline.time_utils import add_months, shift_to_month
from pipeline.quality import check_quality, evaluate_dataset, write_report, assert_report
from pipeline.retrain import is_acceptable, metrics
from pipeline.data import save_frame
from tests.support import frame

class LifecycleTest(unittest.TestCase):
    def test_calendar_clamps_and_preserves_clock(self):
        self.assertEqual(shift_to_month("2020-01-31T23:59:59","2020-02-01"),datetime(2020,2,29,23,59,59))
        self.assertEqual(shift_to_month("2019-01-31T23:59:59","2019-02-01"),datetime(2019,2,28,23,59,59))
        self.assertEqual(add_months("2019-12-01",1),datetime(2020,1,1))

    def test_split_is_by_time_with_no_overlap(self):
        source = frame(3).sample(frac=1,random_state=5)
        train,test = split_window(source,"2019-01-01","2019-03-01")
        self.assertEqual(len(train),60)
        self.assertEqual(len(test),30)
        self.assertLess(pd.to_datetime(train.transaction_time).max(),pd.to_datetime(test.transaction_time).min())
        with self.assertRaises(ValueError):
            split_window(frame(4),"2019-01-01","2019-03-01")

    def test_gate_equal_better_and_each_metric_worse(self):
        base = {"average_precision":0.7,"roc_auc":0.9}
        self.assertTrue(is_acceptable(base,None))
        self.assertTrue(is_acceptable(base,base))
        self.assertTrue(is_acceptable({"average_precision":0.8,"roc_auc":0.95},base))
        self.assertFalse(is_acceptable({"average_precision":0.6,"roc_auc":0.95},base))
        self.assertFalse(is_acceptable({"average_precision":0.8,"roc_auc":0.85},base))
        with self.assertRaises(ValueError):
            metrics([0,0],[0.1,0.2])

    def test_quality_seven_rules_and_individual_corruptions(self):
        source = frame(3)
        self.assertTrue(check_quality(source,"2019-01-01","2019-03-01")["passed"])
        variants = []
        variants.append(source.drop(columns=["merch"]))
        variants.append(pd.concat([source,source.iloc[:1]]))
        for name,value in (("transaction_time","bad"),("target",2),("amount",-1),("lat",91)):
            copy = source.copy()
            copy.loc[0,name] = value
            variants.append(copy)
        copy = source.copy()
        copy["target"] = 0
        variants.append(copy)
        for bad in variants:
            result = check_quality(bad,"2019-01-01","2019-03-01")
            self.assertEqual(len(result["rules"]),7)
            self.assertFalse(result["passed"])

    def test_failed_report_is_written_before_error(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict("os.environ",{"ARTIFACTS_ROOT":temporary}):
            path = Path(temporary)/"bad.jsonl"
            source = frame(3)
            source["target"] = 0
            save_frame(path,source)
            snapshot = {"key":"dq-test","path":str(path),"start":"2019-01-01","test":"2019-03-01"}
            report = write_report(evaluate_dataset(snapshot))
            self.assertTrue(Path(report["report"]).is_file())
            self.assertIn("FAIL",Path(report["report"]).read_text(encoding="utf-8"))
            with self.assertRaises(ValueError):
                assert_report(report)
            self.assertEqual(len(list((Path(temporary)/"dq_reports").glob("*.html"))),1)
