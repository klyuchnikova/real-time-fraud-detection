import json
import tempfile
import unittest
from pathlib import Path
from prometheus_client import CollectorRegistry
from scoring_service.metrics import ScoringMetrics
from scores_sink.metrics import SinkMetrics
from scoring_service.scorer import Scorer
from common.registry import publish, atomic_json
from tests.support import bundle, frame
from scores_sink.app import validate_context
from transaction_simulator.app import rows, payload
from scoring_service.app import publish as publish_score

class MonitoringTest(unittest.TestCase):
    def test_pending_kafka_delivery_is_not_a_failed_publication(self):
        class PendingProducer:
            def __init__(self, error=None):
                self.pending = 2
                self.calls = 0
                self.error = error
            def produce(self, *args, on_delivery, **kwargs):
                self.calls += 1
                self.callback = on_delivery
            def flush(self, timeout):
                if self.pending:
                    self.pending -= 1
                    return 1
                self.callback(self.error, None)
                return 0
        producer = PendingProducer()
        publish_score(producer, "scores", {"transaction_id":"one"})
        self.assertEqual(producer.calls, 1)
        with self.assertRaises(RuntimeError):
            publish_score(PendingProducer("Delivery failed"), "scores", {"transaction_id":"two"})

    def test_only_successful_replacement_creates_event(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = CollectorRegistry()
            metrics = ScoringMetrics(registry)
            scorer = Scorer(directory, metrics=metrics)
            bundle(directory, "one")
            publish(directory, "one")
            scorer.refresh()
            self.assertIsNone(registry.get_sample_value("fraud_model_reloads_total", {"version":"one"}))
            bundle(directory, "two")
            publish(directory, "two")
            scorer.refresh()
            scorer.refresh()
            self.assertEqual(registry.get_sample_value("fraud_model_reloads_total", {"version":"two"}), 1)
            atomic_json(Path(directory)/"active.json", {"model_version":"broken"})
            scorer.refresh()
            self.assertEqual(scorer.version, "two")
            self.assertIsNone(registry.get_sample_value("fraud_model_reloads_total", {"version":"broken"}))
            self.assertEqual(registry.get_sample_value("fraud_model_active", {"version":"two"}),1)

    def test_metric_values_and_duplicate_semantics(self):
        registry = CollectorRegistry()
        metrics = SinkMetrics(registry)
        metrics.saved("score", True, ("a", .7, 1, "one"), .02)
        metrics.saved("score", False, ("a", .7, 1, "one"), .03)
        self.assertEqual(registry.get_sample_value("sink_scores_written_total"),1)
        self.assertEqual(registry.get_sample_value("sink_score_count"),1)
        self.assertEqual(registry.get_sample_value("sink_duplicates_total",{"kind":"score"}),1)
        scoring = ScoringMetrics(registry)
        scoring.success({"score":.3,"fraud_flag":0}, .01)
        self.assertEqual(registry.get_sample_value("fraud_processed_total"),1)
        self.assertEqual(registry.get_sample_value("fraud_flagged_total"),0)

    def test_replay_has_new_ids_original_time_no_target(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"train.csv"
            source=frame().iloc[:2].drop(columns=["transaction_id"])
            source.to_csv(path,index=False)
            iterator=rows(path)
            events=[payload(next(iterator)) for _ in range(4)]
            self.assertEqual(len({x["transaction_id"] for x in events}),4)
            self.assertNotIn("target",events[0]["data"])
            self.assertEqual(events[0]["data"],events[2]["data"])
            self.assertIn("transaction_time",events[0]["data"])
            validate_context(json.dumps(events[0]).encode())
            with self.assertRaises(ValueError):
                validate_context(json.dumps(payload(events[0]["data"],True)).encode())

