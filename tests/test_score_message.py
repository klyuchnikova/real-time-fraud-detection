import json
import unittest
from scores_sink.app import validate_score

class ScoreMessageTest(unittest.TestCase):
    def test_valid_message(self):
        result = validate_score(json.dumps({"transaction_id":"a","score":0.7,"fraud_flag":1,"model_version":"v-1"}).encode())
        self.assertEqual(result,("a",0.7,1,"v-1"))

    def test_invalid_fields(self):
        base = {"transaction_id":"a","score":0.7,"fraud_flag":1,"model_version":"v-1"}
        for field,value in (("score",float("nan")),("score",True),("score",1.1),
                            ("fraud_flag",True),("fraud_flag",2),("model_version",""),("model_version",None)):
            with self.subTest(field=field,value=value):
                with self.assertRaises(ValueError):
                    validate_score(json.dumps({**base,field:value}).encode())
        with self.assertRaises(ValueError):
            validate_score(json.dumps({**base,"extra":1}).encode())
        del base["model_version"]
        with self.assertRaises(ValueError):
            validate_score(json.dumps(base).encode())
