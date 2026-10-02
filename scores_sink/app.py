"""Save scored transactions from Kafka to PostgreSQL."""

import json
import logging
import math
import os
import time

from confluent_kafka import Consumer, KafkaException
import psycopg


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INSERT_SQL = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""


def validate_score(raw: bytes) -> tuple[str, float, int]:
    data = json.loads(raw)
    if not isinstance(data, dict) or set(data) != {"transaction_id", "score", "fraud_flag"}:
        raise ValueError("Expected transaction_id, score, fraud_flag")
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
    return transaction_id, score, fraud_flag


def connect(database_url: str):
    while True:
        try:
            return psycopg.connect(database_url)
        except psycopg.OperationalError:
            logger.warning("PostgreSQL is unavailable; retrying")
            time.sleep(3)


def main() -> None:
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("KAFKA_SCORES_TOPIC", "scores")
    database_url = os.environ["DATABASE_URL"]
    connection = connect(database_url)
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": "scores-to-postgres",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    logger.info("Saving scores from %s", topic)
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                row = validate_score(message.value())
            except (ValueError, TypeError, UnicodeError) as error:
                logger.warning("Skipping invalid score at offset %s: %s", message.offset(), error)
                consumer.commit(message=message, asynchronous=False)
                continue
            with connection.cursor() as cursor:
                cursor.execute(INSERT_SQL, row)
            connection.commit()
            consumer.commit(message=message, asynchronous=False)
            logger.info("Saved score for %s", row[0])
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
