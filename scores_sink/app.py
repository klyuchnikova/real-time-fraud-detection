"""Save scored transactions from Kafka to PostgreSQL."""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from confluent_kafka import Consumer, KafkaException
import psycopg
from prometheus_client import start_http_server
from scores_sink.metrics import SinkMetrics
from common.schema import INPUT_COLUMNS


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INSERT_SQL = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag, model_version, scored_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
RETURNING transaction_id
"""

CONTEXT_SQL = """
INSERT INTO transaction_context (transaction_id, us_state, merch, cat_id, amount, emitted_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
RETURNING transaction_id
"""

def validate_context(raw):
    event = json.loads(raw)
    if not isinstance(event, dict) or not isinstance(event.get("transaction_id"), str) or not event["transaction_id"].strip():
        raise ValueError("Invalid transaction_id")
    data = event.get("data")
    if not isinstance(data, dict) or not set(INPUT_COLUMNS) <= data.keys():
        raise ValueError("Missing input fields")
    fields = []
    for name in ("us_state", "merch", "cat_id"):
        value = data[name]
        fields.append(str(value).strip() if value is not None and str(value).strip() else "__MISSING__")
    try:
        amount = Decimal(str(data["amount"]))
    except InvalidOperation as error:
        raise ValueError("Invalid amount") from error
    if not amount.is_finite() or amount < 0:
        raise ValueError("Invalid amount")
    return event["transaction_id"], *fields, amount

def kafka_time(message):
    _, milliseconds = message.timestamp()
    if milliseconds is None or milliseconds < 0:
        raise ValueError("Kafka publication timestamp is missing")
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)

def save_message(connection, kind, row, published_at, metrics):
    """Commit before counting; retry of either topic cannot duplicate a row."""
    started = time.perf_counter()
    try:
        inserted = connection.execute(
            INSERT_SQL if kind == "score" else CONTEXT_SQL, (*row, published_at)
        ).fetchone() is not None
        connection.commit()
    except psycopg.Error:
        connection.rollback()
        metrics.errors.inc()
        raise
    metrics.saved(kind, inserted, row, time.perf_counter() - started)
    return inserted


def validate_score(raw: bytes) -> tuple[str, float, int, str]:
    data = json.loads(raw)
    if not isinstance(data, dict) or set(data) != {"transaction_id", "score", "fraud_flag", "model_version"}:
        raise ValueError("Expected transaction_id, score, fraud_flag, model_version")
    transaction_id = data["transaction_id"]
    if not isinstance(transaction_id, str) or not transaction_id:
        raise ValueError("Invalid transaction_id")
    if isinstance(data["score"], bool) or not isinstance(data["score"], (int, float)):
        raise ValueError("Invalid score")
    score = float(data["score"])
    if not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError("Score is out of range")
    fraud_flag = data["fraud_flag"]
    if type(fraud_flag) is not int or fraud_flag not in (0, 1):
        raise ValueError("Invalid fraud_flag")
    version = data["model_version"]
    if not isinstance(version, str) or not version.strip():
        raise ValueError("Invalid model_version")
    return transaction_id, score, fraud_flag, version


def connect(database_url: str):
    while True:
        try:
            return psycopg.connect(database_url)
        except psycopg.OperationalError:
            logger.warning("PostgreSQL is unavailable; retrying")
            time.sleep(3)


def committed(error, _partitions):
    if error:
        logger.warning("Kafka offset commit failed; committed rows may be replayed: %s", error)


def main() -> None:
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("KAFKA_SCORES_TOPIC", "scores")
    input_topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
    metrics = SinkMetrics()
    start_http_server(int(os.getenv("METRICS_PORT", "8001")))
    database_url = os.environ["DATABASE_URL"]
    connection = connect(database_url)
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": "monitoring-to-postgres",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "on_commit": committed,
    })
    consumer.subscribe([topic, input_topic])
    logger.info("Saving scores from %s", topic)
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            kind = "score" if message.topic() == topic else "context"
            try:
                row = validate_score(message.value()) if kind == "score" else validate_context(message.value())
                published_at = kafka_time(message)
            except (ValueError, TypeError, UnicodeError) as error:
                logger.warning("Skipping invalid score at offset %s: %s", message.offset(), error)
                metrics.invalid.labels(kind).inc()
                consumer.commit(message=message, asynchronous=True)
                continue
            save_message(connection, kind, row, published_at, metrics)
            consumer.commit(message=message, asynchronous=True)
            logger.info("Saved score for %s", row[0])
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
