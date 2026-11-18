"""Per-process metrics. Callbacks run only after an atomic model activation."""
import time
from prometheus_client import Counter, Gauge, Histogram, REGISTRY
from common.monitoring import SCORE_BUCKETS, LATENCY_BUCKETS

class ScoringMetrics:
    def __init__(self, registry=REGISTRY):
        self.processed = Counter("fraud_processed_total", "Confirmed Kafka score publications", registry=registry)
        self.flagged = Counter("fraud_flagged_total", "Published scores with fraud_flag=1", registry=registry)
        self.invalid = Counter("fraud_invalid_total", "Rejected input messages", registry=registry)
        self.errors = Counter("fraud_processing_errors_total", "Unexpected scoring or publication failures", registry=registry)
        self.duration = Histogram("fraud_processing_seconds", "Decode through confirmed score publication", buckets=LATENCY_BUCKETS, registry=registry)
        self.scores = Histogram("fraud_score", "Published fraud scores", buckets=SCORE_BUCKETS, registry=registry)
        self.last = Gauge("fraud_last_processed_timestamp_seconds", "Last confirmed score publication, Unix seconds", registry=registry)
        self.started = Gauge("fraud_started_timestamp_seconds", "Scoring process start", registry=registry)
        self.started.set(time.time())
        self.reloads = Counter("fraud_model_reloads_total", "Successful replacements, excludes initial load", ["version"], registry=registry)
        self.reload_time = Gauge("fraud_model_reload_timestamp_seconds", "Last successful replacement by version", ["version"], registry=registry)
        self.active = Gauge("fraud_model_active", "One for the active version", ["version"], registry=registry)
        self.reload_errors = Counter("fraud_model_reload_errors_total", "Failed load attempts; old model stays active", registry=registry)

    def activated(self, previous, version):
        if previous:
            self.active.labels(previous).set(0)
        self.active.labels(version).set(1)
        if previous is not None and previous != version:
            self.reloads.labels(version).inc()
            self.reload_time.labels(version).set(time.time())

    def success(self, result, elapsed):
        self.processed.inc()
        self.flagged.inc(result["fraud_flag"])
        self.scores.observe(result["score"])
        self.duration.observe(elapsed)
        self.last.set(time.time())

