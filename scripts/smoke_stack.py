"""Verify one fresh transaction in Kafka output AND PostgreSQL with the expected version."""
import argparse
import csv
import json
import os
import time
import uuid
from pathlib import Path
from confluent_kafka import Producer, Consumer
import psycopg
from common.registry import active_version

def check(expected_version=None, timeout=120):
    with (Path(__file__).resolve().parents[1]/"examples/demo.csv").open(encoding="utf-8",newline="") as stream:
        row=next(csv.DictReader(stream))
    transaction_id=str(uuid.uuid4())
    broker=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:19092")
    consumer=Consumer({"bootstrap.servers":broker,"group.id":"smoke-"+transaction_id,
                       "auto.offset.reset":"earliest","enable.auto.commit":False})
    consumer.subscribe(["scores"])
    producer=Producer({"bootstrap.servers":broker,"acks":"all"})
    errors=[]
    producer.produce("transactions",key=transaction_id.encode(),
                     value=json.dumps({"transaction_id":transaction_id,"data":row}).encode(),
                     on_delivery=lambda error,message: errors.append(str(error)) if error else None)
    if producer.flush(15) or errors:
        raise RuntimeError(f"Kafka did not confirm delivery: {errors}")
    kafka_result=None
    saved=None
    deadline=time.monotonic()+timeout
    try:
        while time.monotonic()<deadline:
            message=consumer.poll(0.5)
            if message and not message.error():
                result=json.loads(message.value())
                if result["transaction_id"]==transaction_id:
                    kafka_result=result
            with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
                saved=connection.execute("SELECT score,fraud_flag,model_version FROM transaction_scores WHERE transaction_id=%s",(transaction_id,)).fetchone()
            if saved and kafka_result:
                if (saved[0],saved[1],saved[2]) != (kafka_result["score"],kafka_result["fraud_flag"],kafka_result["model_version"]):
                    raise AssertionError("Kafka result differs from PostgreSQL")
                if expected_version and saved[2]!=expected_version:
                    raise AssertionError(f"Expected {expected_version}, got {saved[2]}")
                return kafka_result
        raise TimeoutError(f"No matching Kafka and PostgreSQL result for {transaction_id}")
    finally:
        consumer.close()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--expect-version")
    args=parser.parse_args()
    print(json.dumps(check(args.expect_version),ensure_ascii=False))

if __name__=="__main__":
    main()
