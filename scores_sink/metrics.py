"""Count durable new score rows, never Kafka redeliveries."""
from prometheus_client import Counter, Histogram, REGISTRY
from common.monitoring import SCORE_BUCKETS, LATENCY_BUCKETS

class SinkMetrics:
    def __init__(self, registry=REGISTRY):
        self.written = Counter("sink_scores_written_total", "New score rows committed", registry=registry)
        self.flagged = Counter("sink_flagged_total", "New flagged score rows committed", registry=registry)
        self.duration = Histogram("sink_write_seconds", "Score INSERT and commit duration", buckets=LATENCY_BUCKETS, registry=registry)
        self.scores = Histogram("sink_score", "New committed score distribution", buckets=SCORE_BUCKETS, registry=registry)
        self.duplicates = Counter("sink_duplicates_total", "Duplicate messages ignored", ["kind"], registry=registry)
        self.invalid = Counter("sink_invalid_total", "Invalid messages skipped", ["kind"], registry=registry)
        self.errors = Counter("sink_write_errors_total", "Database write failures", registry=registry)
        self.contexts = Counter("sink_contexts_written_total", "New transaction contexts committed", registry=registry)
        for kind in ("score", "context"):
            self.duplicates.labels(kind)
            self.invalid.labels(kind)

    def saved(self, kind, inserted, row, elapsed):
        if not inserted:
            self.duplicates.labels(kind).inc()
        elif kind == "score":
            self.written.inc()
            self.flagged.inc(row[2])
            self.scores.observe(row[1])
            self.duration.observe(elapsed)
        else:
            self.contexts.inc()

