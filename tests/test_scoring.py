import csv
import math
import unittest
from pathlib import Path

from common.schema import INPUT_COLUMNS
from scoring_service.preprocessing import prepare_features
from scoring_service.scorer import Scorer


ROOT = Path(__file__).resolve().parents[1]


class ScoringTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scorer = Scorer(
            str(ROOT / "artifacts" / "my_catboost.cbm"),
            str(ROOT / "artifacts" / "preprocessor.json"),
        )
        with (ROOT / "examples" / "demo.csv").open(newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            cls.columns = reader.fieldnames
            cls.rows = list(reader)

    def test_demo_schema_and_scores(self):
        self.assertEqual(self.columns, list(INPUT_COLUMNS))
        self.assertEqual(len(self.rows), 3)
        expected_scores = (0.0021094796892489193, 0.0009171333159702732, 0.001080082764109121)
        for index, row in enumerate(self.rows):
            features = prepare_features(row, self.scorer.artifact)
            self.assertEqual(features.columns.tolist(), self.scorer.model.feature_names_)
            result = self.scorer.score({"transaction_id": f"demo-{index}", "data": row})
            self.assertEqual(set(result), {"transaction_id", "score", "fraud_flag"})
            self.assertTrue(math.isfinite(result["score"]))
            self.assertGreaterEqual(result["score"], 0)
            self.assertLessEqual(result["score"], 1)
            self.assertIn(result["fraud_flag"], (0, 1))
            self.assertAlmostEqual(result["score"], expected_scores[index], places=8)

    def test_missing_field_is_rejected(self):
        row = self.rows[0].copy()
        row.pop("amount")
        with self.assertRaisesRegex(ValueError, "Missing fields: amount"):
            self.scorer.score({"transaction_id": "bad", "data": row})

    def test_invalid_time_is_rejected(self):
        row = self.rows[0].copy()
        row["transaction_time"] = "wrong-time"
        with self.assertRaises(ValueError):
            self.scorer.score({"transaction_id": "bad-time", "data": row})


if __name__ == "__main__":
    unittest.main()
