"""Check Kafka → model → Kafka → PostgreSQL with one demo row."""

import csv
import json
import os
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer
import psycopg


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    with (ROOT / "examples" / "demo.csv").open(newline="", encoding="utf-8") as file:
        row = next(csv.DictReader(file))
    transaction_id = str(uuid.uuid4())
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
    producer = Producer({"bootstrap.servers": broker, "acks": "all"})
    producer.produce(
        topic,
        key=transaction_id.encode(),
        value=json.dumps({"transaction_id": transaction_id, "data": row}).encode(),
    )
    if producer.flush(10):
        raise RuntimeError("Kafka did not accept the demo transaction")

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT score, fraud_flag FROM transaction_scores WHERE transaction_id = %s",
                    (transaction_id,),
                )
                result = cursor.fetchone()
        if result is not None:
            print(f"OK transaction_id={transaction_id} score={result[0]:.6f} fraud_flag={result[1]}")
            return
        time.sleep(1)
    raise TimeoutError(f"No score saved within 120 seconds for transaction_id={transaction_id}")


if __name__ == "__main__":
    main()
