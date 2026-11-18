"""Read transactions, hot-reload published bundles, and emit versioned Kafka scores."""
import json
import logging
import os
import time
from confluent_kafka import Consumer, KafkaException, Producer
from scoring_service.scorer import Scorer, ModelNotReady
from scoring_service.metrics import ScoringMetrics
from prometheus_client import start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def publish(producer, topic, result):
    errors = []
    def delivered(error, _message):
        if error:
            errors.append(str(error))
    producer.produce(topic, key=result["transaction_id"].encode(),
                     value=json.dumps(result, allow_nan=False).encode(), on_delivery=delivered)
    # A flush timeout only means that delivery is still pending. librdkafka
    # retries the same queued record until its delivery timeout; do not enqueue
    # another copy or terminate the process while that confirmation is pending.
    while producer.flush(5):
        logger.warning("Waiting for Kafka to confirm the queued score")
    if errors:
        raise RuntimeError(f"Could not publish score: {errors}")

def committed(error, _partitions):
    if error:
        logger.warning("Kafka offset commit failed; durable scores may be replayed: %s", error)

def main():
    metrics = ScoringMetrics()
    start_http_server(int(os.getenv("METRICS_PORT", "8000")))
    scorer = Scorer(os.getenv("MODEL_DIR","/shared/models"), float(os.getenv("FRAUD_THRESHOLD","0.98")), metrics)
    # Do not consume/ack the backlog until the first valid version is available.
    while True:
        try:
            scorer.refresh()
            break
        except ModelNotReady:
            time.sleep(2)
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:19092")
    consumer = Consumer({"bootstrap.servers": broker, "group.id":"fraud-scorer",
                         "auto.offset.reset":"earliest", "enable.auto.commit":False,
                         "max.poll.interval.ms":600000, "on_commit":committed})
    producer = Producer({"bootstrap.servers":broker,"acks":"all","enable.idempotence":True})
    consumer.subscribe([os.getenv("KAFKA_TRANSACTIONS_TOPIC","transactions")])
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            started = time.perf_counter()
            try:
                result = scorer.score(json.loads(message.value()))
            except (ValueError, TypeError, KeyError, UnicodeError) as error:
                logger.warning("Invalid transaction offset=%s: %s", message.offset(), error)
                metrics.invalid.inc()
                consumer.commit(message=message, asynchronous=True)
                continue
            except Exception:
                metrics.errors.inc()
                raise
            try:
                publish(producer, os.getenv("KAFKA_SCORES_TOPIC","scores"), result)
            except Exception:
                metrics.errors.inc()
                raise
            metrics.success(result, time.perf_counter() - started)
            # Only durable results reach this point. A failed offset commit
            # causes a safe replay; it must not kill a healthy scoring process.
            consumer.commit(message=message, asynchronous=True)
            logger.info("Scored transaction=%s model_version=%s", result["transaction_id"], result["model_version"])
    finally:
        producer.flush(10)
        consumer.close()

if __name__ == "__main__":
    main()
