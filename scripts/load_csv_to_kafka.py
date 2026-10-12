"""Stream a validated CSV to Kafka; fail on unconfirmed delivery."""
import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from common.analytics import file_hash, inspect_source, iter_csv, parse_row, save_json, transaction_id


def load_file(path, *, broker, topic, manifest_path, limit=None, timeout=60, producer_factory=Producer):
    manifest = {"status": "validating", "source_file": str(path), "confirmed_messages": 0}
    delivered, errors = 0, []
    try:
        manifest.update(inspect_source(path, limit))
        manifest.update(status="publishing", broker=broker, topic=topic,
                        started_at=datetime.now(timezone.utc).isoformat())
        save_json(manifest_path, manifest)
        producer = producer_factory({
            "bootstrap.servers": broker, "acks": "all", "enable.idempotence": True,
            "compression.type": "zstd", "linger.ms": 10, "batch.size": 65536,
            "queue.buffering.max.messages": 50000, "message.timeout.ms": int(timeout * 1000),
            "allow.auto.create.topics": False,
        })

        def on_delivery(error, message):
            nonlocal delivered
            if error:
                if len(errors) < 5:
                    errors.append(str(error))
            else:
                delivered += 1

        for index, row in iter_csv(path):
            if index > manifest["selected_rows"]:
                break
            payload = parse_row(row, index)
            payload["dataset_id"] = manifest["dataset_id"]
            payload["transaction_id"] = transaction_id(manifest["dataset_id"], index)
            encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
            deadline = time.monotonic() + timeout
            while True:
                try:
                    producer.produce(topic, key=payload["transaction_id"].encode(),
                                     value=encoded, on_delivery=on_delivery)
                    break
                except BufferError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Producer queue remained full")
                    producer.poll(0.1)
            producer.poll(0)
            if errors:
                raise RuntimeError(f"Kafka delivery failed: {errors}")
        remaining = producer.flush(timeout)
        manifest["confirmed_messages"] = delivered
        if remaining or errors or delivered != manifest["selected_rows"]:
            raise RuntimeError(f"Unconfirmed delivery: expected={manifest['selected_rows']}, confirmed={delivered}, pending={remaining}, errors={errors}")
        # Extra binary integrity pass: detects modifications during publishing.
        if file_hash(path) != manifest["source_sha256"]:
            raise RuntimeError("CSV changed during publishing; partial data may exist")
        manifest.update(status="success", completed_at=datetime.now(timezone.utc).isoformat())
        save_json(manifest_path, manifest)
        return manifest
    except Exception as error:
        manifest.update(status="failed", confirmed_messages=delivered, error=str(error))
        save_json(manifest_path, manifest)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--broker", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_ANALYTICS_TOPIC", "transactions_analytics"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--manifest", type=Path, default=Path("reports/load_manifest.json"))
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    manifest = load_file(args.csv, broker=args.broker, topic=args.topic,
                         manifest_path=args.manifest, limit=args.limit, timeout=args.timeout)
    print(json.dumps({key: manifest[key] for key in ("status", "dataset_id", "selected_rows", "confirmed_messages")}, indent=2))


if __name__ == "__main__":
    main()
