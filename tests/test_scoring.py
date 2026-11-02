import json
import tempfile
import unittest
from pathlib import Path
from common.registry import publish, atomic_json, save_bundle, load_bundle
from scoring_service.scorer import Scorer, ModelNotReady
from tests.support import bundle, frame

class ScoringTest(unittest.TestCase):
    def test_hot_reload_and_corruption_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            scorer = Scorer(root)
            with self.assertRaises(ModelNotReady):
                scorer.refresh()
            bundle(root,"v-one")
            publish(root,"v-one")
            payload = {"transaction_id":"one","data":frame().iloc[0].to_dict()}
            result = scorer.score(payload)
            self.assertEqual(result["model_version"],"v-one")
            self.assertEqual(set(result),{"transaction_id","score","fraud_flag","model_version"})
            self.assertTrue(0 <= result["score"] <= 1)
            bundle(root,"v-two")
            publish(root,"v-two")
            self.assertEqual(scorer.score(payload)["model_version"],"v-two")
            bundle(root,"v-broken")
            (root/"versions/v-broken/model.bin").write_bytes(b"corrupt")
            with self.assertRaises(ValueError):
                publish(root,"v-broken")
            atomic_json(root/"active.json",{"model_version":"v-broken"})
            self.assertEqual(scorer.score(payload)["model_version"],"v-two")
            (root/"active.json").write_text("broken",encoding="utf-8")
            self.assertEqual(scorer.score(payload)["model_version"],"v-two")

    def test_bad_payload_and_threshold(self):
        with self.assertRaises(ValueError):
            Scorer("/unused",1.1)
        scorer = Scorer("/unused")
        for value in (None,[],{},{"transaction_id":"","data":{}},{"transaction_id":"a","data":[]}):
            with self.assertRaises(ValueError):
                scorer.score(value)

    def test_bundle_retry_and_path_safety(self):
        with tempfile.TemporaryDirectory() as temporary:
            bundle(temporary,"v-one")
            load_bundle(temporary,"v-one")
            with self.assertRaises(ValueError):
                load_bundle(temporary,"../outside")
            with self.assertRaises(ValueError):
                publish(temporary,"../outside")
