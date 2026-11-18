"""Replay CSV continuously with fresh IDs and original model features."""
import argparse
import json
import os
import random
import time
import uuid
from confluent_kafka import Producer
from common.schema import read_transactions_csv

def rows(path):
    while True:
        count = 0
        with open(path, encoding="utf-8-sig", newline="") as stream:
            for row in read_transactions_csv(stream, allow_target=True):
                row.pop("target", None)
                if None in row or any(value is None for value in row.values()):
                    raise ValueError("Malformed CSV row")
                count += 1
                yield row
        if not count:
            raise ValueError("Empty input CSV")

def payload(row, invalid=False):
    result = {"transaction_id": str(uuid.uuid4()), "data": dict(row)}
    if invalid:
        result["data"].pop("amount", None)
    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=os.getenv("TRAIN_DATA_CSV", "/data/train.csv"))
    parser.add_argument("--rate", type=float, default=float(os.getenv("STREAM_RATE", "10")))
    parser.add_argument("--limit", type=int, default=0, help="0 means continuous")
    parser.add_argument("--invalid-ratio", type=float, default=0)
    args = parser.parse_args()
    if not 0 < args.rate <= 10000 or not 0 <= args.invalid_ratio <= 1 or args.limit < 0:
        parser.error("rate must be (0,10000], invalid-ratio [0,1], limit >=0")
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:19092"),
                         "acks":"all", "enable.idempotence":True})
    errors = []
    def delivered(error, _message):
        if error:
            errors.append(str(error))
    topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC","transactions")
    deadline = time.monotonic()
    try:
        for count, row in enumerate(rows(args.csv), 1):
            if errors:
                raise RuntimeError(errors.pop())
            event = payload(row, random.random() < args.invalid_ratio)
            producer.produce(topic, key=event["transaction_id"].encode(),
                             value=json.dumps(event, allow_nan=False).encode(), on_delivery=delivered)
            producer.poll(0)
            if args.limit and count >= args.limit:
                break
            deadline = max(deadline + 1 / args.rate, time.monotonic())
            time.sleep(max(0, deadline - time.monotonic()))
    finally:
        if producer.flush(30) or errors:
            raise RuntimeError("Kafka did not confirm all generated transactions")

if __name__ == "__main__":
    main()

