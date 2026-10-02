import json
import unittest

from scores_sink.app import validate_score


class ScoreMessageTest(unittest.TestCase):
    def test_valid_message(self):
        payload = {"transaction_id": "abc", "score": 0.7, "fraud_flag": 1}
        self.assertEqual(validate_score(json.dumps(payload).encode()), ("abc", 0.7, 1))

    def test_extra_field_is_rejected(self):
        payload = {"transaction_id": "abc", "score": 0.7, "fraud_flag": 1, "extra": 1}
        with self.assertRaises(ValueError):
            validate_score(json.dumps(payload).encode())

    def test_non_finite_score_is_rejected(self):
        payload = {"transaction_id": "abc", "score": float("nan"), "fraud_flag": 1}
        with self.assertRaises(ValueError):
            validate_score(json.dumps(payload).encode())


if __name__ == "__main__":
    unittest.main()
